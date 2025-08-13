use async_recursion::async_recursion;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use rand::Rng;
use redb::{TableHandle, WriteTransaction};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc;

use smallvec::smallvec;

use crate::plugins::communication::transports::PeerId;
use crate::plugins::harmonizer::range_cube::RangeCube;
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
use crate::{
    KuramotoDb, StaticTableDef, WriteBatch, database::WriteRequest, plugins::Plugin,
    storage_entity::StorageEntity, storage_error::StorageError, uuid_bytes::UuidBytes,
};

/*────────────────────────────────────────────────────────────────────────────*/

pub struct PeerContext {
    pub peer_id: UuidBytes,
}

pub trait Scorer: Send + Sync {
    fn score(&self, ctx: &PeerContext, avail: &Availability) -> f32;
}

/*──────────────────────── Outbox (network side-effects) ───────────────────*/

enum OutboxItem {
    Notify { peer: PeerId, msg: HarmonizerMsg },
    // You can add request/response items later if needed
}

/*────────────────────────────────────────────────────────────────────────────*/

pub struct Harmonizer {
    scorer: Box<dyn Scorer>,
    watched: HashSet<&'static str>,
    router: Arc<Router>,

    // inbox for protocol requests/notifications
    inbox_tx: mpsc::Sender<ProtoCommand>,
    // outbox for network sends driven by Harmonizer logic
    outbox_tx: mpsc::Sender<OutboxItem>,

    // test-only: simple list of peers to notify for v0 flow
    bootstrap_peers: Vec<PeerId>,
}

impl Harmonizer {
    pub fn new(scorer: Box<dyn Scorer>, router: Arc<Router>) -> Self {
        // queues
        let (inbox_tx, mut inbox_rx) = mpsc::channel::<ProtoCommand>(256);
        let (outbox_tx, mut outbox_rx) = mpsc::channel::<OutboxItem>(256);

        // register protocol handler with a clone of inbox sender
        register_harmonizer_protocol(router.clone(), inbox_tx.clone());

        let this_router = router.clone();

        // spawn: outbox network worker
        tokio::spawn(async move {
            while let Some(item) = outbox_rx.recv().await {
                match item {
                    OutboxItem::Notify { peer, msg } => {
                        // fire-and-forget; errors are OK for v0
                        let _ = this_router.notify_on(PROTO_HARMONIZER, peer, &msg).await;
                    }
                }
            }
        });

        // spawn: inbox command worker (serialize Harmonizer protocol handling)
        tokio::spawn(async move {
            while let Some(cmd) = inbox_rx.recv().await {
                // For now, we don’t have access to DB here; the actual Harmonizer
                // instance will spawn its own loop that replaces this placeholder.
                // This placeholder simply responds minimally so wiring compiles.
                match cmd {
                    ProtoCommand::HandleRequest { respond, msg, .. } => {
                        // Minimal, empty responses. Replace with real logic later.
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
                    ProtoCommand::HandleNotify { .. } => {
                        // swallow for now
                    }
                }
            }
        });

        Self {
            scorer,
            watched: HashSet::new(),
            router,
            inbox_tx,
            outbox_tx,
            bootstrap_peers: Vec::new(),
        }
    }

    /// test-only: tell the harmonizer who to notify
    pub fn add_peer(&mut self, p: crate::plugins::communication::transports::PeerId) {
        self.bootstrap_peers.push(p);
    }

    pub fn watch<E: StorageEntity>(&mut self) {
        self.watched.insert(E::table_def().name());
    }

    fn is_watched(&self, tbl: StaticTableDef) -> bool {
        self.watched.contains(tbl.name())
    }

    /*──────── stable symbol & small digest helpers ───────*/

    #[inline]
    fn splitmix64(mut x: u64) -> u64 {
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

    /*──────────────────── range cover (async DFS) ─────────────────────────*/

    pub async fn range_cover<F>(
        &self,
        db: &KuramotoDb,
        target: &RangeCube,
        root_ids: &[UuidBytes],
        peer_filter: F,
        level_limit: Option<usize>,
    ) -> Result<(Vec<Availability>, bool), StorageError>
    where
        F: Fn(&Availability) -> bool + Sync,
    {
        async fn load_avail(
            db: &KuramotoDb,
            id: &UuidBytes,
        ) -> Result<Option<Availability>, StorageError> {
            match db.get_data::<Availability>(id.as_bytes()).await {
                Ok(a) => Ok(Some(a)),
                Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(e),
            }
        }

        async fn resolve_child_avail(
            _db: &KuramotoDb,
            _sym: Sym,
        ) -> Result<Option<Availability>, StorageError> {
            Ok(None)
        }

        #[async_recursion]
        async fn dfs<F>(
            db: &KuramotoDb,
            acc: &mut Vec<Availability>,
            cube: &RangeCube,
            id: UuidBytes,
            peer_filter: &F,
            level_left: Option<usize>,
            any_overlap: &mut bool,
        ) -> Result<(), StorageError>
        where
            F: Fn(&Availability) -> bool + Sync,
        {
            if level_left == Some(0) {
                return Ok(());
            }
            let level_next = level_left.map(|d| d - 1);

            let Some(av) = load_avail(db, &id).await? else {
                return Ok(());
            };
            if !peer_filter(&av) {
                return Ok(());
            }
            if av.range.intersect(cube).is_none() {
                return Ok(());
            }
            *any_overlap = true;

            let is_leaf = av.children.count() == 0;
            if !av.complete || is_leaf {
                acc.push(av);
                return Ok(());
            }

            for sym in &av.children.children {
                if let Some(child_av) = resolve_child_avail(db, *sym).await? {
                    dfs(
                        db,
                        acc,
                        cube,
                        child_av.key,
                        peer_filter,
                        level_next,
                        any_overlap,
                    )
                    .await?;
                } else {
                    acc.push(av.clone());
                    return Ok(());
                }
            }
            Ok(())
        }

        let mut results = Vec::<Availability>::new();
        let mut any_overlap = false;
        for rid in root_ids {
            dfs(
                db,
                &mut results,
                target,
                *rid,
                &peer_filter,
                level_limit,
                &mut any_overlap,
            )
            .await?;
        }

        let mut seen = std::collections::HashSet::<UuidBytes>::new();
        results.retain(|a| seen.insert(a.key));

        Ok((results, !any_overlap))
    }
}

/*──────────────────────── Plugin: append leaf + emit v0 update ───────────*/

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

        // copy out what we need, then drop the borrow on `batch`
        let (data_table, key_owned, entity_bytes) = match first {
            WriteRequest::Put {
                data_table,
                key,
                value,
                ..
            } if self.is_watched(*data_table) => (*data_table, key.clone(), value.clone()),
            _ => return Ok(()),
        };

        let now = db.get_clock().now();
        let sym = self.child_sym(data_table, &key_owned);

        let avail_id = UuidBytes::new();
        let peer_id = UuidBytes::new(); // TODO: supply real peer id

        let avail = Availability {
            key: avail_id,
            peer_id,
            range: RangeCube {
                dims: smallvec![],
                mins: smallvec![],
                maxs: smallvec![],
            },
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

        // serialize availability + meta
        let value = avail.to_bytes();

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

        // append availability write
        batch.push(WriteRequest::Put {
            data_table: AVAILABILITIES_TABLE,
            meta_table: AVAILABILITIES_META_TABLE,
            key: avail.primary_key(),
            value,
            meta,
            index_removes: Vec::new(),
            index_puts: Vec::new(),
        });

        // v0: broadcast direct UpdateWithAttestation to any bootstrapped peers
        if !self.bootstrap_peers.is_empty() {
            let att = UpdateWithAttestation {
                table: data_table.name().to_string(),
                pk: key_owned.clone(),
                range: RangeCube {
                    dims: smallvec![],
                    mins: smallvec![],
                    maxs: smallvec![],
                },
                local_availability_id: avail_id,
                level: 0,
                child_count: 1,
                small_digest: self.small_digest_single(sym),
                entity_bytes,
            };
            let msg = HarmonizerMsg::UpdateWithAttestation(att);

            for &peer in &self.bootstrap_peers {
                let _ = self
                    .outbox_tx
                    .send(OutboxItem::Notify {
                        peer,
                        msg: msg.clone(),
                    })
                    .await;
            }
        }

        Ok(())
    }
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
    use crate::plugins::fnv1a_16;
    use crate::storage_entity::*;
    use bincode::{Decode, Encode};
    use redb::TableDefinition;
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
        struct NullScore;
        impl Scorer for NullScore {
            fn score(&self, _: &PeerContext, _: &Availability) -> f32 {
                0.0
            }
        }

        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());

        let mut hz = Harmonizer::new(Box::new(NullScore), router);
        hz.watch::<Foo>();
        let hz = Arc::new(hz);

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("t.redb").to_str().unwrap(),
            clock,
            vec![hz.clone()],
        )
        .await;

        db.create_table_and_indexes::<Foo>().unwrap();
        db.create_table_and_indexes::<Availability>().unwrap();

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
        struct NullScore;
        impl Scorer for NullScore {
            fn score(&self, _: &PeerContext, _: &Availability) -> f32 {
                0.0
            }
        }

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
        let mut hz_a = Harmonizer::new(Box::new(NullScore), rtr_a.clone());
        hz_a.watch::<Foo>();
        hz_a.add_peer(pid_b); // tell A to notify B
        let hz_a = Arc::new(hz_a);

        // DB A
        let dir_a = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        db_a.create_table_and_indexes::<Foo>().unwrap();
        db_a.create_table_and_indexes::<Availability>().unwrap();

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
}
