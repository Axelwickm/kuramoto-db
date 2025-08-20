use async_trait::async_trait;
use bincode::{Decode, Encode};
use redb::{TableHandle, WriteTransaction};
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{OnceLock, Weak};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc;

use crate::database::{IndexPutRequest, WriteRequest};
use crate::plugins::communication::transports::PeerId;
use crate::plugins::harmonizer::child_set::{Child, DigestChunk};
use crate::plugins::harmonizer::optimizer::{Action as OptAction, AvailabilityDraft};
use crate::plugins::harmonizer::optimizer::{BasicOptimizer, Optimizer, PeerContext};
use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::plugins::harmonizer::scorers::Scorer;
use crate::plugins::harmonizer::scorers::server_scorer::{ServerScorer, ServerScorerParams};
use crate::plugins::harmonizer::{
    child_set::{ChildSet, Sym},
    protocol::{
        HarmonizerMsg, HarmonizerResp, PROTO_HARMONIZER, ProtoCommand, ReconcileAsk,
        UpdateResponse, UpdateWithAttestation, register_harmonizer_protocol,
    },
};
use crate::plugins::{
    communication::router::Router,
    harmonizer::availability::{AVAILABILITIES_META_TABLE, AVAILABILITIES_TABLE, Availability},
};
use crate::tables::TableHash;
use crate::{
    KuramotoDb, StaticTableDef, WriteBatch, plugins::Plugin, storage_entity::StorageEntity,
    storage_error::StorageError, uuid_bytes::UuidBytes,
};

/*──────────────────────── Outbox (network side-effects) ───────────────────*/

enum OutboxItem {
    Notify { peer: PeerId, msg: HarmonizerMsg },
}

/*────────────────────────────────────────────────────────────────────────────*/

pub struct Harmonizer {
    db: OnceLock<Weak<KuramotoDb>>,
    scorer: Box<dyn Scorer>,
    watched_tables: HashSet<&'static str>,
    router: Arc<Router>,
    inbox_tx: mpsc::Sender<ProtoCommand>,
    outbox_tx: mpsc::Sender<OutboxItem>,
    peers: tokio::sync::RwLock<Vec<PeerId>>, // dynamic peers
}

impl Harmonizer {
    pub fn new(
        scorer: Box<dyn Scorer>,
        router: Arc<Router>,
        watched_tables: HashSet<&'static str>,
    ) -> Arc<Self> {
        let (inbox_tx, inbox_rx) = mpsc::channel::<ProtoCommand>(256);
        let (outbox_tx, outbox_rx) = mpsc::channel::<OutboxItem>(256);

        register_harmonizer_protocol(router.clone(), inbox_tx.clone());

        let hz = Arc::new(Self {
            scorer,
            watched_tables: watched_tables,
            router: router.clone(),
            inbox_tx,
            outbox_tx,
            peers: tokio::sync::RwLock::new(Vec::new()),
            db: OnceLock::new(),
        });

        hz.start_inbox_worker(inbox_rx);
        hz.start_outbox_worker(outbox_rx);

        hz
    }

    pub async fn add_peer(&self, p: PeerId) {
        self.peers.write().await.push(p);
    }

    fn start_inbox_worker(self: &Arc<Self>, mut inbox_rx: mpsc::Receiver<ProtoCommand>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(cmd) = inbox_rx.recv().await {
                match cmd {
                    ProtoCommand::HandleRequest { respond, msg, .. } => {
                        // (unchanged placeholder response)
                        let resp = match msg {
                            HarmonizerMsg::GetChildrenByRange(_)
                            | HarmonizerMsg::GetChildrenByAvailability(_) => {
                                HarmonizerResp::Children(
                                    crate::plugins::harmonizer::protocol::ChildrenResponse {
                                        items: vec![],
                                        next: None,
                                        headers: vec![],
                                    },
                                )
                            }
                            HarmonizerMsg::GetSymbolsByAvailability(_) => HarmonizerResp::Symbols(
                                crate::plugins::harmonizer::protocol::SymbolsResponse {
                                    cells: vec![],
                                    next: None,
                                    header: None,
                                },
                            ),
                            HarmonizerMsg::UpdateWithAttestation(_) => {
                                HarmonizerResp::Update(UpdateResponse {
                                    accepted: true,
                                    need: None::<ReconcileAsk>,
                                    headers: vec![],
                                })
                            }
                        };
                        let _ = respond.send(Ok(resp));
                    }

                    ProtoCommand::HandleNotify { peer: _peer, msg } => {
                        match msg {
                            HarmonizerMsg::UpdateWithAttestation(req) => {
                                // upgrade DB handle
                                let db = match this.db.get().and_then(|w| w.upgrade()) {
                                    Some(db) => db,
                                    None => {
                                        tracing::warn!(
                                            "harmonizer: no DB attached; dropping update"
                                        );
                                        continue;
                                    }
                                };

                                // apply raw bytes to the named table (this will trigger our own before_update)
                                if let Err(e) =
                                    db.put_by_table_bytes(&req.table, &req.entity_bytes).await
                                {
                                    tracing::warn!(error = %e, "harmonizer: put_by_table_bytes failed");
                                }
                                // v0: do nothing else (no drift check, no reply, no rebroadcast)
                            }
                            _ => {
                                // ignore other notify types in v0
                            }
                        }
                    }
                }
            }
        });
    }

    fn start_outbox_worker(&self, mut outbox_rx: mpsc::Receiver<OutboxItem>) {
        let router = self.router.clone();
        tokio::spawn(async move {
            while let Some(item) = outbox_rx.recv().await {
                if let OutboxItem::Notify { peer, msg } = item {
                    let _ = router.notify_on(PROTO_HARMONIZER, peer, &msg).await;
                }
            }
        });
    }

    fn is_watched(&self, tbl: StaticTableDef) -> bool {
        self.watched_tables.contains(tbl.name())
    }

    /// Build a multi-dim leaf: one dim for the PK axis + one per index table present in `index_puts`.
    /// Each dim is the tightest half-open interval that contains *all* keys emitted for that index.
    fn leaf_range_from_pk_and_indexes(
        data_table: StaticTableDef,
        pk: &[u8],
        index_puts: &[IndexPutRequest],
    ) -> RangeCube {
        // By-dimension accumulator, sorted by hash for deterministic output.
        // Value = (TableHash, raw_min_inclusive, raw_max_inclusive)
        let mut by_dim: BTreeMap<u64, (TableHash, Vec<u8>, Vec<u8>)> = BTreeMap::new();

        // 1) PK axis (always present)
        let pk_dim = TableHash::from(data_table);
        by_dim.insert(pk_dim.hash(), (pk_dim, pk.to_vec(), pk.to_vec()));

        // 2) Every index row → fold into that index’s min/max
        for ip in index_puts {
            let dim = TableHash::from(ip.table);
            let key_raw = Self::strip_pk_from_index_key(&ip.key, pk);

            let h = dim.hash();
            match by_dim.get_mut(&h) {
                Some((_d, lo, hi)) => {
                    if key_raw < *lo {
                        *lo = key_raw.clone();
                    }
                    if key_raw > *hi {
                        *hi = key_raw.clone();
                    }
                }
                None => {
                    by_dim.insert(h, (dim, key_raw.clone(), key_raw));
                }
            }
        }

        // 3) Materialize sorted dims/mins/maxs; make ranges half-open by bumping hi
        let mut dims = smallvec![];
        let mut mins = smallvec![];
        let mut maxs = smallvec![];
        for (_h, (dim, lo, mut hi_incl)) in by_dim.into_iter() {
            // Ensure non-empty: [lo, hi_incl + 0x01)
            if hi_incl <= lo {
                // Equal is fine; bump hi to keep non-empty.
                hi_incl = lo.clone();
            }
            let mut hi_excl = hi_incl.clone();
            hi_excl.push(0x01);

            dims.push(dim);
            mins.push(lo);
            maxs.push(hi_excl);
        }

        RangeCube { dims, mins, maxs }
    }

    /// Returns **owned** bytes (no lifetimes). If your index key encodes PK as suffix/prefix,
    /// strip it so the coordinate lives only in index-key space.
    fn strip_pk_from_index_key(stored: &[u8], pk: &[u8]) -> Vec<u8> {
        if stored.len() >= pk.len() && stored.ends_with(pk) {
            stored[..stored.len() - pk.len()].to_vec()
        } else if stored.len() >= pk.len() && stored.starts_with(pk) {
            stored[pk.len()..].to_vec()
        } else {
            stored.to_vec()
        }
    }

    #[inline]
    fn splitmix64(mut x: u64) -> u64 {
        // TODO: We already have hashes in table.rs, so maybe remove
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    fn child_sym(&self, data_table: StaticTableDef, key: &[u8]) -> Sym {
        let mut acc: u64 = 0xcbf2_9ce4_8422_2325;
        for b in data_table
            .name()
            .as_bytes()
            .iter()
            .copied()
            .chain([0u8])
            .chain(key.iter().copied())
        {
            acc ^= b as u64;
            acc = acc.wrapping_mul(0x1000_001B3);
        }
        Self::splitmix64(acc)
    }

    fn small_digest_single(&self, sym: Sym) -> u64 {
        let bytes = sym.to_le_bytes();
        let mut v = 0u64;
        for (i, b) in bytes.iter().enumerate() {
            v ^= (*b as u64) << ((i as u64 % 8) * 8);
            v = Self::splitmix64(v);
        }
        v
    }

    /// Find a small (containment-minimal) set of Availability nodes that
    /// together *touch* `target`. The `peer_filter` lets you restrict to
    /// local or to a set of gossip peers. `level_limit` currently acts as
    /// a simple filter: keep nodes with `level <= limit`.
    ///
    /// Returns:
    ///   - Vec<Availability>: an antichain by containment (no element is
    ///     strictly contained by another in the result). Sorted by
    ///     descending level, then by an approximate "volume".
    ///   - bool: `true` iff **no candidates overlapped** `target`
    ///           (kept for API compatibility).
    pub async fn range_cover<F>(
        &self,
        db: &KuramotoDb,
        target: &RangeCube,
        _root_ids: &[UuidBytes],
        peer_filter: F,
        level_limit: Option<usize>,
    ) -> Result<(Vec<Availability>, bool), StorageError>
    where
        F: Fn(&Availability) -> bool + Sync,
    {
        // 1) Load all availabilities (MVP: table scan; we can introduce indexes later)
        let mut all: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;

        // 2) Filter by (peer, overlap, level_limit)
        all.retain(|a| {
            if let Some(max_level) = level_limit {
                if a.level as usize > max_level {
                    return false;
                }
            }
            peer_filter(a) && a.range.overlaps(target)
        });

        println!(
            "range_cover: target mins={:?} maxs={:?}",
            target.mins, target.maxs
        );
        println!("range_cover: candidates after filter = {}", all.len());
        for (i, a) in all.iter().enumerate() {
            let contains = a.range.contains(target);
            let overlaps = a.range.overlaps(target);
            println!(
                "  [{}] lvl={} peer={:?} mins={:?} maxs={:?} contains={} overlaps={}",
                i, a.level, a.peer_id, a.range.mins, a.range.maxs, contains, overlaps
            );
        }
        std::io::stdout().flush().unwrap();

        // Early out: nothing touches the target
        if all.is_empty() {
            return Ok((Vec::new(), true)); // second == "no overlap"
        }

        // 3) Optional tightening: prefer candidates fully inside target.
        //    If at least one Availability *contains* the target, keep only
        //    the "smallest" such (min-approx-volume). That’s the strongest
        //    single-node cover we can get right now.
        let mut inside: Vec<&Availability> =
            all.iter().filter(|a| a.range.contains(target)).collect();
        if !inside.is_empty() {
            inside.sort_by_key(|a| (a.level, a.range.approx_volume()));
            // Pick the smallest-by-volume on the lowest level that still contains target
            let pick = inside[0].clone();
            return Ok((vec![pick.clone()], false));
        }

        println!("range_cover: inside.len()={}", inside.len());
        for (i, a) in inside.iter().enumerate() {
            println!(
                "  inside[{}]: lvl={} mins={:?} maxs={:?}",
                i, a.level, a.range.mins, a.range.maxs
            );
        }

        // 4) Build a containment-minimal antichain among overlapping candidates:
        //    drop any candidate whose range is strictly contained by another.
        //    (This keeps results small without geometry subtraction.)
        //    O(N^2) for now; fine for MVP.
        let mut keep = vec![true; all.len()];
        for i in 0..all.len() {
            if !keep[i] {
                continue;
            }
            for j in 0..all.len() {
                if i == j || !keep[i] {
                    continue;
                }
                // If all[j] contains all[i], drop i
                if all[j].range.contains(&all[i].range) {
                    keep[i] = false;
                }
            }
        }
        let mut result: Vec<Availability> = all
            .into_iter()
            .zip(keep.into_iter())
            .filter_map(|(a, k)| if k { Some(a) } else { None })
            .collect();

        // 5) Sort for determinism: prefer higher level (coarser) then larger volume
        result.sort_by(|a, b| {
            b.level
                .cmp(&a.level)
                .then(b.range.approx_volume().cmp(&a.range.approx_volume()))
        });

        Ok((result, false))
    }

    /// Count how many distinct peers fully cover `target` with at least
    /// one complete availability. Filtering (local vs peers) is entirely
    /// driven by `peer_filter`. If `level_limit` is set, only nodes with
    /// `level <= level_limit` are considered.
    ///
    /// Notes:
    /// - “Fully cover” means `a.range.contains(target)` (not just overlap).
    /// - Multiple availabilities from the same peer count as 1.
    /// - This is a full-table scan MVP. We can index later if needed.
    pub async fn count_replications<F>(
        &self,
        db: &KuramotoDb,
        target: &RangeCube,
        peer_filter: F,
        level_limit: Option<usize>,
    ) -> Result<usize, StorageError>
    where
        F: Fn(&Availability) -> bool + Sync,
    {
        let mut peers = HashSet::<UuidBytes>::new();

        // MVP: scan all availabilities (local store), then filter
        let all: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;

        for a in all {
            if !a.complete {
                continue;
            }
            if let Some(max_lvl) = level_limit {
                if a.level as usize > max_lvl {
                    continue;
                }
            }
            if !peer_filter(&a) {
                continue;
            }
            // strict 100% coverage for now (no partial-credit mode yet)
            if a.range.contains(target) {
                peers.insert(a.peer_id);
            }
        }

        Ok(peers.len())
    }
}

#[async_trait]
impl Plugin for Harmonizer {
    async fn before_update(
        &self,
        db: &KuramotoDb,
        _txn: &WriteTransaction,
        batch: &mut WriteBatch,
    ) -> Result<(), StorageError> {
        let Some(first) = batch.get(0) else {
            return Ok(());
        };

        println!("harm.before_update: incoming batch_len={}", batch.len());

        // Only act on writes to watched data tables
        let (data_table, key_owned, entity_bytes, index_puts) = match first {
            WriteRequest::Put {
                data_table,
                key,
                value,
                index_puts,
                ..
            } if self.is_watched(*data_table) => {
                println!(
                    "before_update: first table={} watched={}",
                    data_table.name(),
                    self.is_watched(*data_table)
                );
                (*data_table, key.clone(), value.clone(), index_puts)
            }
            _ => return Ok(()),
        };

        let now = db.get_clock().now();
        let sym = self.child_sym(data_table, &key_owned);

        // TODO: wire real peer id
        let peer_id = UuidBytes::new();

        // --- 1) materialize the leaf availability (level 0) ---
        let avail_id = UuidBytes::new();
        let leaf_range = Self::leaf_range_from_pk_and_indexes(data_table, &key_owned, &index_puts);

        let avail = Availability {
            key: avail_id,
            peer_id,
            range: leaf_range,
            level: 0,
            children: ChildSet {
                parent: avail_id,
                children: vec![sym],
            },
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };

        let value = avail.to_bytes();

        // lightweight meta for Ava
        #[derive(Encode, Decode)]
        struct AvailMetaV0 {
            version: u32,
            updated_at: u64,
        }
        let meta = bincode::encode_to_vec(
            AvailMetaV0 {
                version: 0,
                updated_at: now,
            },
            bincode::config::standard(),
        )
        .map_err(|e| StorageError::Bincode(e.to_string()))?;

        println!(
            "before_update: enqueue avail id={:?} level={} mins={:?} maxs={:?}",
            avail_id, avail.level, avail.range.mins, avail.range.maxs
        );

        batch.push(WriteRequest::Put {
            data_table: AVAILABILITIES_TABLE,
            meta_table: AVAILABILITIES_META_TABLE,
            key: avail.primary_key(),
            value,
            meta,
            index_removes: Vec::new(),
            index_puts: Vec::new(),
        });

        println!("before_update: batch size after enqueue={}", batch.len());

        // --- 2) broadcast inline attestation (v0) to known peers ---
        let peers = { self.peers.read().await.clone() }; // drop lock before awaits
        if !peers.is_empty() {
            let att = UpdateWithAttestation {
                table: data_table.name().to_string(),
                pk: key_owned.clone(),
                range: avail.range.clone(),
                local_availability_id: avail_id,
                level: 0,
                child_count: 1,
                small_digest: self.small_digest_single(sym),
                entity_bytes,
            };
            let msg = HarmonizerMsg::UpdateWithAttestation(att);

            for &peer in &peers {
                let _ = self
                    .outbox_tx
                    .send(OutboxItem::Notify {
                        peer,
                        msg: msg.clone(),
                    })
                    .await;
            }
        }

        // --- 3) run optimizer and apply safe parent inserts / deletes ---
        // TODO: gotta use the self.scorer function instead
        let scorer =
            ServerScorer::from_db_snapshot(db, peer_id, ServerScorerParams::default()).await?;
        let opt = BasicOptimizer::new(Box::new(scorer), PeerContext { peer_id });

        let seed = AvailabilityDraft {
            level: 0,
            range: avail.range.clone(),
            complete: true,
        };
        let inserts = vec![seed];

        println!("before_update: running optimizer.propose()");

        if let Some(plan) = opt.propose(db, &inserts).await? {
            println!("before_update: optimizer returned {} actions", plan.len());

            for act in plan {
                match act {
                    // Only materialize parents here; leaf (level 0) already inserted above
                    OptAction::Insert(d) => {
                        if d.level == 0 {
                            continue;
                        }
                        let id = UuidBytes::new();
                        let now2 = db.get_clock().now();
                        let parent = Availability {
                            key: id,
                            peer_id,
                            range: d.range.clone(),
                            level: d.level,
                            children: ChildSet {
                                parent: id,
                                children: vec![],
                            },
                            schema_hash: 0,
                            version: 0,
                            updated_at: now2,
                            complete: true,
                        };

                        let meta2 = bincode::encode_to_vec(
                            AvailMetaV0 {
                                version: 0,
                                updated_at: now2,
                            },
                            bincode::config::standard(),
                        )
                        .map_err(|e| StorageError::Bincode(e.to_string()))?;

                        batch.push(WriteRequest::Put {
                            data_table: AVAILABILITIES_TABLE,
                            meta_table: AVAILABILITIES_META_TABLE,
                            key: parent.primary_key(),
                            value: parent.to_bytes(),
                            meta: meta2,
                            index_removes: Vec::new(),
                            index_puts: Vec::new(),
                        });
                    }

                    OptAction::Delete(id) => {
                        if let Ok(existing) = db.get_data::<Availability>(id.as_bytes()).await {
                            if existing.level == 0 {
                                continue;
                            }
                        }
                        let now2 = db.get_clock().now();
                        let meta2 = bincode::encode_to_vec(
                            AvailMetaV0 {
                                version: 1,
                                updated_at: now2,
                            },
                            bincode::config::standard(),
                        )
                        .map_err(|e| StorageError::Bincode(e.to_string()))?;

                        batch.push(WriteRequest::Delete {
                            data_table: AVAILABILITIES_TABLE,
                            meta_table: AVAILABILITIES_META_TABLE,
                            key: id.as_bytes().to_vec(),
                            meta: meta2,
                            index_removes: Vec::new(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn attach_db(&self, db: Arc<KuramotoDb>) {
        db.create_table_and_indexes::<Availability>().unwrap();
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<DigestChunk>().unwrap();
        self.db.set(Arc::downgrade(&db)).unwrap();
    }

    // async fn try_complete_incomplete_nodes(&self, db: &KuramotoDb) -> Result<(), StorageError> {
    //     let self_peer = /* TODO: real peer id */;
    //     let locals: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;
    //     for a in locals.into_iter().filter(|x| x.peer_id == self_peer && !x.complete) {
    //         let peer_filter = |av: &Availability| av.peer_id != self_peer;
    //         let (helpers, _no_overlap) = self.range_cover(db, &a.range, &[], peer_filter, None).await?;
    //         if helpers.is_empty() {
    //             // nobody to ask; keep incomplete for now
    //             continue;
    //         }
    //         // TODO: issue RPCs to helpers to fetch children/symbols/data and reconcile.
    //         // Once local fill is done:
    //         // mark_complete(db, &a.key).await?;
    //     }
    //     Ok(())
    // }
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::plugins::communication::router::{Handler, Router, RouterConfig};
    use crate::plugins::communication::transports::{
        Connector, PeerId, PeerResolver,
        inmem::{InMemConnector, InMemResolver},
    };
    use crate::plugins::harmonizer::optimizer::AvailabilityDraft;
    use crate::storage_entity::*;
    use crate::tables::TableHash;

    use bincode::{Decode, Encode};
    use rand::Rng;
    use redb::TableDefinition;
    use smallvec::smallvec;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::task::yield_now;
    use tokio::time::{Duration as TDuration, advance};

    /* ---------------------- Test Entity ---------------------- */

    #[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
    struct Foo {
        id: u32,
    }
    impl StorageEntity for Foo {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_le_bytes().to_vec()
        }
        fn table_def() -> StaticTableDef {
            static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("foo");
            &TBL
        }
        fn meta_table_def() -> StaticTableDef {
            static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("foo_meta");
            &TBL
        }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            if data.is_empty() {
                return Err(StorageError::Bincode("empty input".into()));
            }
            match data[0] {
                0 => bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| StorageError::Bincode(e.to_string())),
                n => Err(StorageError::Bincode(format!("unknown version {n}"))),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] {
            &[]
        }
    }

    struct NullScore;
    impl Scorer for NullScore {
        fn score(&self, _: &PeerContext, _: &AvailabilityDraft) -> f32 {
            0.0
        }
    }

    /* ---------------------- Helpers ---------------------- */

    fn pid(x: u8) -> PeerId {
        let mut b = [0u8; 16];
        b[0] = x;
        PeerId::from_bytes(b)
    }

    async fn spin() {
        // drive router tasks deterministically (start_paused tests)
        yield_now().await;
        advance(TDuration::from_millis(1)).await;
        yield_now().await;
    }

    /* ---------------------- Tests ---------------------- */

    /// Local effect: inserting a watched entity appends exactly one leaf Availability.
    #[tokio::test(start_paused = true)]
    async fn inserts_one_availability_per_put() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());

        let hz = Harmonizer::new(
            Box::new(NullScore),
            router,
            HashSet::from([Foo::table_def().name()]),
        );

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("t.redb").to_str().unwrap(),
            clock,
            vec![hz.clone()],
        )
        .await;

        db.create_table_and_indexes::<Foo>().unwrap();

        db.put(Foo { id: 1 }).await.unwrap();

        let all: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        assert_eq!(
            all.len(),
            1,
            "should materialize a single leaf availability"
        );
        assert_eq!(
            all[0].children.count(),
            1,
            "leaf should carry one child symbol"
        );
        assert!(all[0].complete);
        assert_eq!(all[0].level, 0);
    }

    /// Wiring test: outbox emits UpdateWithAttestation to a peer without a Harmonizer.
    /// We attach a tiny probe handler on the peer to capture the notify.
    #[tokio::test(start_paused = true)]
    async fn outbox_emits_update_notify_to_peer() {
        // shared clock
        let clock = Arc::new(MockClock::new(0));

        // routers
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // peer IDs + inmem transport
        let ns: u64 = rand::rng().random();
        let pid_a = pid(1);
        let pid_b = pid(2);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;

        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();

        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Build Harmonizer on A (will register its protocol on A’s router)
        let hz_a = Harmonizer::new(
            Box::new(NullScore),
            rtr_a,
            HashSet::from([Foo::table_def().name()]),
        );
        hz_a.add_peer(pid_b).await; // tell A to notify B

        // DB A
        let dir_a = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        db_a.create_table_and_indexes::<Foo>().unwrap();

        // On B, install a probe handler (NO Harmonizer on B to avoid proto clash)
        #[derive(Clone)]
        struct Probe {
            tx: mpsc::Sender<UpdateWithAttestation>,
        }

        #[async_trait::async_trait]
        impl Handler for Probe {
            async fn on_request(&self, _peer: PeerId, _body: &[u8]) -> Result<Vec<u8>, String> {
                // Not used in this test
                Ok(Vec::new())
            }

            async fn on_notify(&self, _peer: PeerId, body: &[u8]) -> Result<(), String> {
                // decode and forward just UpdateWithAttestation
                if let Ok((msg, _)) = bincode::decode_from_slice::<HarmonizerMsg, _>(
                    body,
                    bincode::config::standard(),
                ) {
                    if let HarmonizerMsg::UpdateWithAttestation(att) = msg {
                        let _ = self.tx.send(att).await;
                    }
                }
                Ok(())
            }
        }

        // Register probe on B with the same proto id
        let (probe_tx, mut probe_rx) = mpsc::channel::<UpdateWithAttestation>(8);
        rtr_b.set_handler(PROTO_HARMONIZER, Arc::new(Probe { tx: probe_tx }));

        // Trigger: put Foo on A → Harmonizer emits UpdateWithAttestation → Probe should see it
        db_a.put(Foo { id: 42 }).await.unwrap();
        spin().await;

        let got = probe_rx.try_recv().ok();
        assert!(
            got.is_some(),
            "peer B should receive an UpdateWithAttestation notify"
        );

        let att = got.unwrap();
        assert_eq!(att.table, Foo::table_def().name().to_string());
        assert_eq!(att.pk, 42u32.to_le_bytes().to_vec());
        assert_eq!(att.level, 0);
        assert_eq!(att.child_count, 1);
        assert!(
            !att.entity_bytes.is_empty(),
            "v0 pushes entity bytes inline"
        );
    }

    /// End-to-end: A inserts, Harmonizer sends UpdateWithAttestation,
    /// B ingests via proto handler and writes to its DB.
    #[tokio::test(start_paused = true)]
    async fn empty_to_sync_single_insert() {
        // shared clock + two routers
        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // connect peers via in-mem transport
        let ns: u64 = rand::rng().random();
        let pid_a = pid(1);
        let pid_b = pid(2);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;

        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();
        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Harmonizers on both sides (register their protocol with their routers)
        let hz_a = Harmonizer::new(
            Box::new(NullScore),
            rtr_a.clone(),
            HashSet::from([Foo::table_def().name()]),
        );
        let hz_b = Harmonizer::new(
            Box::new(NullScore),
            rtr_b.clone(),
            HashSet::from([Foo::table_def().name()]),
        );

        // Tell A to notify B for this v0 flow (hacky bootstrap)
        hz_a.add_peer(pid_b).await;

        // DBs with harmonizer plugin installed
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        let db_b = KuramotoDb::new(
            dir_b.path().join("b.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_b.clone()],
        )
        .await;

        db_a.create_table_and_indexes::<Foo>().unwrap();
        db_b.create_table_and_indexes::<Foo>().unwrap();

        // Insert on A → should arrive on B through UpdateWithAttestation path
        db_a.put(Foo { id: 7 }).await.unwrap();

        // Drive the async machinery deterministically
        spin().await;

        // Assert Foo exists on B
        let got_b: Foo = db_b.get_data::<Foo>(&7u32.to_le_bytes()).await.unwrap();
        assert_eq!(got_b, Foo { id: 7 });

        // (Optional) sanity: B produced at least one leaf availability
        let av_b: Vec<Availability> = db_b
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        assert!(
            !av_b.is_empty(),
            "receiver should materialize a leaf availability for the ingested entity"
        );
        assert!(av_b.iter().any(|a| a.level == 0 && a.children.count() >= 1));
    }

    // --- New cover/replication tests ---

    #[tokio::test]
    async fn range_cover_antichain_and_single_container_pick() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());
        let hz = Harmonizer::new(Box::new(NullScore), router, HashSet::new());

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("cover.redb").to_str().unwrap(),
            clock,
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();

        let dim = TableHash { hash: 1 };

        // Helper to build a cube on one axis
        let cube = |min: &[u8], max: &[u8]| RangeCube {
            dims: smallvec![dim],
            mins: smallvec![min.to_vec()],
            maxs: smallvec![max.to_vec()],
        };

        // Seed 3 overlapping parents (same level)
        let peer = UuidBytes::new();
        let a_big = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(b"a", b"z"), // contains target
            level: 2,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let a_left = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(b"a", b"h"), // overlap only
            level: 2,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let a_right = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(b"p", b"z"), // overlap only
            level: 2,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(a_big.clone()).await.unwrap();
        db.put(a_left).await.unwrap();
        db.put(a_right).await.unwrap();

        // Target is inside [g, p]
        let target = cube(b"g", b"p");

        // Filter: allow everyone (local-only view is fine here)
        let allow_all = |_a: &Availability| true;

        let (cover, no_overlap) = hz
            .range_cover(&db, &target, &[], allow_all, None)
            .await
            .unwrap();
        assert!(!no_overlap, "we seeded overlaps; should be false");

        // Because there is at least one container, result should collapse to a single best container
        assert_eq!(
            cover.len(),
            1,
            "should prefer single tight container when present"
        );
        assert!(cover[0].range.contains(&target));
    }

    #[tokio::test]
    async fn range_cover_builds_containment_antichain() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());
        let hz = Harmonizer::new(Box::new(NullScore), router, HashSet::new());

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("antichain.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();

        let dim = TableHash { hash: 1 };
        let cube = |min: &[u8], max: &[u8]| RangeCube {
            dims: smallvec![dim],
            mins: smallvec![min.to_vec()],
            maxs: smallvec![max.to_vec()],
        };

        // B1 covers [a..m], B3 is strictly inside B1 → B3 should be pruned
        let peer = UuidBytes::new();
        let b1 = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(b"a", b"m"),
            level: 1,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let b2 = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(b"h", b"t"),
            level: 1,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let b3 = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(b"b", b"l"),
            level: 1, // contained in b1
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(b1.clone()).await.unwrap();
        db.put(b2.clone()).await.unwrap();
        db.put(b3).await.unwrap();

        let target = cube(b"g", b"p");
        let (cover, _) = hz
            .range_cover(&db, &target, &[], |_a| true, None)
            .await
            .unwrap();

        // Expect antichain {b1, b2} (order not guaranteed; we'll check set membership)
        let ids: HashSet<UuidBytes> = cover.into_iter().map(|a| a.key).collect();
        assert!(
            ids.contains(&b1.key) && ids.contains(&b2.key),
            "b1 and b2 should remain"
        );
    }

    #[tokio::test]
    async fn count_replications_counts_unique_peers_that_contain_target() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());
        let hz = Harmonizer::new(Box::new(NullScore), router, HashSet::new());

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("rep.redb").to_str().unwrap(),
            clock,
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();

        let dim = TableHash { hash: 1 };
        let cube = |min: &[u8], max: &[u8]| RangeCube {
            dims: smallvec![dim],
            mins: smallvec![min.to_vec()],
            maxs: smallvec![max.to_vec()],
        };
        let target = cube(b"g", b"p");

        // Two distinct peers that fully contain target
        let p1 = UuidBytes::new();
        let p2 = UuidBytes::new();
        db.put(Availability {
            key: UuidBytes::new(),
            peer_id: p1,
            range: cube(b"a", b"z"),
            level: 3,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        })
        .await
        .unwrap();
        db.put(Availability {
            key: UuidBytes::new(),
            peer_id: p2,
            range: cube(b"f", b"q"),
            level: 2,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        })
        .await
        .unwrap();

        // One peer that only overlaps (should NOT count)
        let p3 = UuidBytes::new();
        db.put(Availability {
            key: UuidBytes::new(),
            peer_id: p3,
            range: cube(b"p", b"z"),
            level: 2, // starts at p == target.max → not containing
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        })
        .await
        .unwrap();

        let cnt = hz
            .count_replications(&db, &target, |_a| true, None)
            .await
            .unwrap();
        assert_eq!(
            cnt, 2,
            "should count unique peers with full containment only"
        );
    }
}
