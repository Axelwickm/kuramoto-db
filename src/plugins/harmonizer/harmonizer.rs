use async_trait::async_trait;
use bincode::{Decode, Encode};
use redb::{ReadTransaction, TableHandle};
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::sync::{OnceLock, Weak};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc;

use crate::plugins::harmonizer::child_set::{Child, DigestChunk, Enc, CELLS_PER_CHUNK, STORED_CHUNKS};
use crate::plugins::harmonizer::optimizer::{Action as OptAction, AvailabilityDraft};
use crate::plugins::harmonizer::optimizer::{BasicOptimizer, Optimizer};
use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::plugins::harmonizer::{
    child_set::ChildSet,
    protocol::{
        HarmonizerMsg, HarmonizerResp, PROTO_HARMONIZER, ProtoCommand, ReconcileAsk,
        UpdateResponse, UpdateWithAttestation, register_harmonizer_protocol,
    },
};
use crate::plugins::harmonizer::availability::{roots_for_peer, AVAILABILITY_BY_PEER};
use crate::plugins::harmonizer::availability_queries::range_cover;
use crate::plugins::{
    communication::router::Router,
    harmonizer::availability::{AVAILABILITIES_META_TABLE, AVAILABILITIES_TABLE, Availability},
};
use crate::tables::TableHash;
use crate::{
    KuramotoDb, StaticTableDef, WriteBatch, plugins::Plugin, storage_entity::StorageEntity,
    storage_error::StorageError, uuid_bytes::UuidBytes,
};
use crate::{
    database::{IndexPutRequest, WriteRequest},
    plugins::communication::transports::PeerId,
};

#[derive(Clone, Debug)]
pub struct PeerContext {
    pub peer_id: UuidBytes,
}

/*──────────────────────── Outbox (network side-effects) ───────────────────*/

enum OutboxItem {
    Notify { peer: PeerId, msg: HarmonizerMsg },
}

/*────────────────────────────────────────────────────────────────────────────*/

pub struct Harmonizer {
    db: OnceLock<Weak<KuramotoDb>>,
    watched_tables: HashSet<&'static str>,
    router: Arc<Router>,
    optimizer: Arc<BasicOptimizer>,
    inbox_tx: mpsc::Sender<ProtoCommand>,
    outbox_tx: mpsc::Sender<OutboxItem>,
    peers: tokio::sync::RwLock<Vec<PeerId>>, // dynamic peers
    peer_ctx: PeerContext,
}

impl Harmonizer {
    pub fn new(
        router: Arc<Router>,
        optimizer: Arc<BasicOptimizer>,
        watched_tables: HashSet<&'static str>,
        peer_ctx: PeerContext,
    ) -> Arc<Self> {
        let (inbox_tx, inbox_rx) = mpsc::channel::<ProtoCommand>(256);
        let (outbox_tx, outbox_rx) = mpsc::channel::<OutboxItem>(256);

        register_harmonizer_protocol(router.clone(), inbox_tx.clone());

        let hz = Arc::new(Self {
            watched_tables: watched_tables,
            router: router.clone(),
            optimizer: optimizer.clone(),
            inbox_tx,
            outbox_tx,
            peers: tokio::sync::RwLock::new(Vec::new()),
            db: OnceLock::new(),
            peer_ctx,
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
                let OutboxItem::Notify { peer, msg } = item;
                let _ = router.notify_on(PROTO_HARMONIZER, peer, &msg).await;
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
}

#[async_trait]
impl Plugin for Harmonizer {
    async fn before_update(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
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

        let peer_id = self.peer_ctx.peer_id;

        // --- 1) materialize the leaf availability (level 0) ---
        let avail_id = UuidBytes::new();
        let leaf_range = Self::leaf_range_from_pk_and_indexes(data_table, &key_owned, &index_puts);

        let avail = Availability {
            key: avail_id,
            peer_id,
            range: leaf_range,
            level: 0,
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

        let avail_pk = avail.primary_key();
        let mut by_peer_key = peer_id.as_bytes().to_vec();
        by_peer_key.push(0);
        by_peer_key.extend_from_slice(&avail_pk);
        batch.push(WriteRequest::Put {
            data_table: AVAILABILITIES_TABLE,
            meta_table: AVAILABILITIES_META_TABLE,
            key: avail_pk.clone(),
            value,
            meta,
            index_removes: Vec::new(),
            index_puts: vec![crate::database::IndexPutRequest {
                table: AVAILABILITY_BY_PEER,
                key: by_peer_key,
                value: avail_pk,
                unique: false,
            }],
        });

        println!("before_update: batch size after enqueue={}", batch.len());

        // --- 2) broadcast inline attestation (v0) to known peers ---
        let peers = { self.peers.read().await.clone() }; // drop lock before awaits
        if !peers.is_empty() {
            let b = avail_id.as_bytes();
            debug_assert_eq!(b.len(), 16, "UuidBytes must be 16 bytes");
            let hi = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let lo = u64::from_le_bytes(b[8..16].try_into().unwrap());
            let digest = hi ^ lo;
            let att = UpdateWithAttestation {
                table: data_table.name().to_string(),
                pk: key_owned.clone(),
                range: avail.range.clone(),
                local_availability_id: avail_id,
                level: 0,
                child_count: 1,
                small_digest: digest,
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

        let seed = AvailabilityDraft {
            level: 0,
            range: avail.range.clone(),
            complete: true,
        };
        let inserts = vec![seed];

        println!("before_update: running optimizer.propose()");

        if let Some(plan) = self.optimizer.propose(db, txn, &inserts).await? {
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

                        let parent_pk = parent.primary_key();
                        let mut by_peer_key = peer_id.as_bytes().to_vec();
                        by_peer_key.push(0);
                        by_peer_key.extend_from_slice(&parent_pk);
                        batch.push(WriteRequest::Put {
                            data_table: AVAILABILITIES_TABLE,
                            meta_table: AVAILABILITIES_META_TABLE,
                            key: parent_pk.clone(),
                            value: parent.to_bytes(),
                            meta: meta2,
                            index_removes: Vec::new(),
                            index_puts: vec![crate::database::IndexPutRequest {
                                table: AVAILABILITY_BY_PEER,
                                key: by_peer_key,
                                value: parent_pk,
                                unique: false,
                            }],
                        });

                        // Determine adopted children (local, complete, contained) using snapshot
                        let roots = roots_for_peer(db, Some(txn), &peer_id).await?;
                        let root_ids: Vec<_> = roots.into_iter().map(|r| r.key).collect();
                        let (frontier, no_overlap) =
                            range_cover(db, Some(txn), &d.range, &root_ids, None).await?;
                        // Start with the freshly created leaf if it falls within the parent range
                        let mut child_ids: Vec<UuidBytes> = Vec::new();
                        if d.range.contains(&avail.range) {
                            child_ids.push(avail_id);
                        }
                        if !no_overlap {
                            child_ids.extend(
                                frontier
                                    .into_iter()
                                    .filter(|a| a.peer_id == peer_id && a.complete && d.range.contains(&a.range))
                                    .map(|a| a.key),
                            );
                        }
                        child_ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

                        let now_meta = bincode::encode_to_vec(
                            crate::meta::BlobMeta {
                                version: 0,
                                created_at: now2,
                                updated_at: now2,
                                deleted_at: None,
                                region_lock: crate::region_lock::RegionLock::None,
                            },
                            bincode::config::standard(),
                        )
                        .unwrap();

                        // Enqueue Child rows + by_child index entries
                        for (i, cid) in child_ids.iter().enumerate() {
                            let child = Child {
                                parent: id,
                                ordinal: i as u32,
                                child_id: *cid,
                            };
                            let pk = child.primary_key();
                            // non-unique stored index key = child_id • 0x00 • pk
                            let mut idx_key = cid.as_bytes().to_vec();
                            idx_key.push(0);
                            idx_key.extend_from_slice(&pk);

                            batch.push(WriteRequest::Put {
                                data_table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_TBL,
                                meta_table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_META_TBL,
                                key: pk,
                                value: child.to_bytes(),
                                meta: now_meta.clone(),
                                index_puts: vec![crate::database::IndexPutRequest {
                                    table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_BY_CHILD_TBL,
                                    key: idx_key,
                                    value: child.primary_key(),
                                    unique: false,
                                }],
                                index_removes: Vec::new(),
                            });
                        }

                        // Enqueue digest chunks for the parent (first STORED_CHUNKS)
                        if !child_ids.is_empty() {
                            let mut enc = Enc::new(&child_ids);
                            for chunk_no in 0..STORED_CHUNKS {
                                let mut buf = Vec::with_capacity(CELLS_PER_CHUNK);
                                for _ in 0..CELLS_PER_CHUNK {
                                    buf.push(enc.next_coded());
                                }
                                let bytes = bincode::encode_to_vec(&buf, bincode::config::standard()).unwrap();
                                let chunk = DigestChunk {
                                    parent: id,
                                    chunk_no: chunk_no as u32,
                                    bytes,
                                };
                                batch.push(WriteRequest::Put {
                                    data_table: crate::plugins::harmonizer::child_set::AVAIL_DIG_CHUNK_TBL,
                                    meta_table: crate::plugins::harmonizer::child_set::AVAIL_DIG_CHUNK_META_TBL,
                                    key: chunk.primary_key(),
                                    value: chunk.to_bytes(),
                                    meta: now_meta.clone(),
                                    index_puts: Vec::new(),
                                    index_removes: Vec::new(),
                                });
                            }
                        }
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
    use crate::plugins::harmonizer::optimizer::ActionSet;
    use crate::plugins::harmonizer::optimizer::AvailabilityDraft;
    use crate::plugins::harmonizer::scorers::Scorer;
    use crate::plugins::harmonizer::availability;
    use crate::storage_entity::*;

    use bincode::{Decode, Encode};
    use rand::Rng;
    use redb::TableDefinition;
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
    #[async_trait]
    impl Scorer for NullScore {
        async fn score(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _ctx: &PeerContext,
            _cand: &AvailabilityDraft,
            _overlay: &ActionSet,
        ) -> f32 {
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

        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let optimizer = Arc::new(BasicOptimizer::new(Box::new(NullScore), local_peer.clone()));
        let hz = Harmonizer::new(
            router,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
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
        // Child relations now live in a separate table; leaf completeness and level remain.
        assert!(all[0].complete);
        assert_eq!(all[0].level, 0);
    }

    /// When the optimizer proposes a parent, Harmonizer enqueues the parent and links children
    /// via the child table in the same batch; digest chunks are also written.
    #[tokio::test(start_paused = true)]
    async fn enqueues_parent_and_children_in_batch() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());

        // A simple scorer that strongly prefers larger ranges and higher levels
        struct GrowingScorer;
        #[async_trait]
        impl Scorer for GrowingScorer {
            async fn score(
                &self,
                _db: &KuramotoDb,
                _txn: &ReadTransaction,
                _ctx: &PeerContext,
                cand: &AvailabilityDraft,
                _overlay: &ActionSet,
            ) -> f32 {
                cand.range.approx_volume() as f32 + (cand.level as f32) * 10.0
            }
        }
        let scorer = GrowingScorer;

        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let optimizer = Arc::new(BasicOptimizer::new(Box::new(scorer), local_peer.clone()));
        let hz = Harmonizer::new(
            router,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("t.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();

        // Insert two rows → two leaves. Optimizer should propose a parent covering both.
        db.put(Foo { id: 10 }).await.unwrap();
        db.put(Foo { id: 11 }).await.unwrap();

        // Query all availabilities
        let avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        let leaves: Vec<_> = avs.iter().filter(|a| a.level == 0).collect();
        let parents: Vec<_> = avs.iter().filter(|a| a.level >= 1).collect();
        assert!(leaves.len() >= 2, "expected at least two leaves");
        assert!(parents.len() >= 1, "expected at least one parent");

        // Pick a parent and verify child rows exist and match local leaves
        let mut found_any = false;
        for p in &parents {
            let mut lo = p.key.as_bytes().to_vec();
            let mut hi = lo.clone();
            hi.push(0xFF);
            let kids: Vec<Child> = db.range_by_pk::<Child>(&lo, &hi, None).await.unwrap();
            if !kids.is_empty() {
                found_any = true;
                for c in &kids {
                    assert_eq!(c.parent, p.key);
                    let child: Availability = db.get_data::<Availability>(c.child_id.as_bytes()).await.unwrap();
                    assert_eq!(child.peer_id, p.peer_id);
                }
                // Digest chunk for parent must exist (chunk 0)
                let mut pk = p.key.as_bytes().to_vec();
                pk.extend_from_slice(&0u32.to_le_bytes());
                let _chunk: DigestChunk = db.get_data::<DigestChunk>(&pk).await.unwrap();
                break;
            }
        }
        assert!(found_any, "at least one parent should have linked children");

        // Roots: leaves adopted by this parent should not be roots; the parent should be a root
        let txn = db.begin_read_txn().unwrap();
        let roots = availability::roots_for_peer(&db, Some(&txn), &parents[0].peer_id)
            .await
            .unwrap();
        let root_ids: std::collections::HashSet<_> = roots.into_iter().map(|a| a.key).collect();
        // At least one parent should be a root, and its children should not be roots
        let mut asserted = false;
        for p in &parents {
            let mut lo = p.key.as_bytes().to_vec();
            let mut hi = lo.clone();
            hi.push(0xFF);
            let kids: Vec<Child> = db.range_by_pk::<Child>(&lo, &hi, None).await.unwrap();
            if !kids.is_empty() {
                assert!(root_ids.contains(&p.key), "parent should be a root");
                for c in &kids {
                    assert!(
                        !root_ids.contains(&c.child_id),
                        "adopted child should no longer be a root"
                    );
                }
                asserted = true;
                break;
            }
        }
        assert!(asserted, "no parent with children to assert root properties");
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
        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let optimizer = Arc::new(BasicOptimizer::new(Box::new(NullScore), local_peer.clone()));
        let hz_a = Harmonizer::new(
            rtr_a,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
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
        let peer_a = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let opt_a = Arc::new(BasicOptimizer::new(Box::new(NullScore), peer_a.clone()));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            opt_a,
            HashSet::from([Foo::table_def().name()]),
            peer_a,
        );
        let peer_b = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let opt_b = Arc::new(BasicOptimizer::new(Box::new(NullScore), peer_b.clone()));
        let hz_b = Harmonizer::new(
            rtr_b.clone(),
            opt_b,
            HashSet::from([Foo::table_def().name()]),
            peer_b,
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
        assert!(av_b.iter().any(|a| a.level == 0));
    }
}
