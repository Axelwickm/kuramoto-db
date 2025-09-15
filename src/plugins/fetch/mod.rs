use async_trait::async_trait;
use bincode::{Decode, Encode};
use std::sync::{Arc, OnceLock, Weak};

use crate::{
    meta::BlobMeta,
    plugins::{
        communication::router::{Handler, Router},
        fnv1a_16, Plugin,
    },
    storage_error::StorageError,
    KuramotoDb, WriteOrigin,
};

use crate::plugins::communication::transports::PeerId;

pub const PROTO_REMOTE_FETCH: u16 = fnv1a_16("kuramoto.middleware.fetch.v1");

/// Query mode for exports.
#[derive(Clone, Debug, Encode, Decode)]
pub enum FetchMode {
    /// Scan by primary key range using `start`..`end`.
    ByPkRange,
    /// Scan by index range using `index_table` and `start_idx`..`end_idx`.
    ByIndexRange {
        index_table: String,
        start_idx: Vec<u8>,
        end_idx: Vec<u8>,
    },
}

/// Request rows for a table over a PK or index range. Symmetric between peers.
#[derive(Clone, Debug, Encode, Decode)]
pub struct ExportRequest {
    /// Logical table name (as registered by StorageEntity::table_def().name())
    pub table: String,
    /// Half-open PK range: [start, end) — used when `mode` is `ByPkRange`.
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    /// Query mode
    pub mode: FetchMode,
    /// Maximum rows per batch. If 0, defaults to 10,000,000.
    pub limit: u32,
    /// Resume token: last PK when ByPkRange, or last index key when ByIndexRange; exclusive resume.
    pub cursor: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct RowEnvelope {
    pub pk: Vec<u8>,
    /// If tombstone=true, `data` may be empty; `meta` must still be present.
    pub tombstone: bool,
    pub data: Vec<u8>,
    pub meta: Vec<u8>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct ExportBatch {
    pub table: String,
    pub rows: Vec<RowEnvelope>,
    pub next: Option<Vec<u8>>, // next cursor (PK) if more are available
    pub done: bool,
}

/*──────── Protocol handler: decode/encode and serve from local DB ───────*/
struct FetchProto {
    db: OnceLock<Weak<KuramotoDb>>,
    default_limit: u32,
}

impl FetchProto {
    fn new(db: Weak<KuramotoDb>) -> Arc<Self> {
        let this = Arc::new(Self {
            db: OnceLock::new(),
            default_limit: 10_000_000,
        });
        this.db.set(db).ok();
        this
    }
}

#[async_trait]
impl Handler for FetchProto {
    async fn on_request(&self, _peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String> {
        let (req, _) = bincode::decode_from_slice::<ExportRequest, _>(
            body,
            bincode::config::standard(),
        )
        .map_err(|e| e.to_string())?;

        let db = self
            .db
            .get()
            .and_then(|w| w.upgrade())
            .ok_or_else(|| "no db attached".to_string())?;

        let limit = if req.limit == 0 { self.default_limit } else { req.limit } as usize;

        // Mode-specific scanning
        let scan_limit = if req.cursor.is_some() { limit.saturating_add(1) } else { limit };
        let (out_rows, next, done) = match &req.mode {
            FetchMode::ByPkRange => {
                let scan_start = req.cursor.as_ref().unwrap_or(&req.start);
                let rows = db
                    .range_raw_with_meta_by_pk_table(&req.table, scan_start, &req.end, Some(scan_limit))
                    .await
                    .map_err(|e| e.to_string())?;
                let n_read = rows.len();
                let mut out_rows: Vec<RowEnvelope> = Vec::with_capacity(n_read);
                let mut next: Option<Vec<u8>> = None;
                for (pk, data_opt, meta_raw) in rows.into_iter() {
                    if let Some(c) = &req.cursor { if &pk == c { continue; } }
                    let tombstone = data_opt.is_none();
                    out_rows.push(RowEnvelope { pk: pk.clone(), tombstone, data: data_opt.unwrap_or_default(), meta: meta_raw });
                    next = Some(pk);
                    if out_rows.len() >= limit { break; }
                }
                (out_rows, next, n_read < scan_limit)
            }
            FetchMode::ByIndexRange { index_table, start_idx, end_idx } => {
                let scan_start = req.cursor.as_ref().unwrap_or(start_idx);
                let rows = db
                    .range_raw_with_meta_by_index_table(index_table, scan_start, end_idx, Some(scan_limit))
                    .await
                    .map_err(|e| e.to_string())?;
                let n_read = rows.len();
                let mut out_rows: Vec<RowEnvelope> = Vec::with_capacity(n_read);
                let mut next: Option<Vec<u8>> = None;
                for (ik, pk, data_opt, meta_raw) in rows.into_iter() {
                    if let Some(c) = &req.cursor { if &ik == c { continue; } }
                    let tombstone = data_opt.is_none();
                    out_rows.push(RowEnvelope { pk: pk.clone(), tombstone, data: data_opt.unwrap_or_default(), meta: meta_raw });
                    next = Some(ik);
                    if out_rows.len() >= limit { break; }
                }
                (out_rows, next, n_read < scan_limit)
            }
        };

        let resp = ExportBatch { table: req.table, rows: out_rows, next, done };
        bincode::encode_to_vec(resp, bincode::config::standard()).map_err(|e| e.to_string())
    }

    async fn on_notify(&self, _peer: PeerId, _body: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/*──────── Public plugin: register protocol and provide fetch/apply API ───────*/
pub struct RemoteFetch {
    router: Arc<Router>,
    db: OnceLock<Weak<KuramotoDb>>,
    default_limit: u32,
}

impl RemoteFetch {
    pub fn new(router: Arc<Router>) -> Arc<Self> {
        Arc::new(Self { router, db: OnceLock::new(), default_limit: 10_000_000 })
    }

    /// Register protocol handler with the router. Call after DB is attached.
    fn register_protocol(&self) {
        if let Some(dbw) = self.db.get() {
            let handler = FetchProto::new(dbw.clone());
            self.router.set_handler(PROTO_REMOTE_FETCH, handler);
        }
    }

    /// Fetch rows from a peer and apply into local DB. Snapshot only.
    /// - Default limit per request: 10,000,000 unless overridden in `req`.
    pub async fn fetch_from(&self, peer: PeerId, mut req: ExportRequest) -> Result<FetchStats, StorageError> {
        let db = self.db.get().and_then(|w| w.upgrade()).ok_or(StorageError::Other("no db attached".into()))?;
        if req.limit == 0 { req.limit = self.default_limit; }

        let mut stats = FetchStats::default();
        let timeout = self.router_config_timeout();

        loop {
            let batch: ExportBatch = self
                .router
                .request_on(PROTO_REMOTE_FETCH, peer, &req, timeout)
                .await
                .map_err(|e| StorageError::Other(format!("router: {e}")))?;

            for row in &batch.rows {
                let applied = self.apply_row(&db, &batch.table, row).await?;
                if applied { stats.applied += 1; } else { stats.skipped += 1; }
            }
            stats.fetched += batch.rows.len() as u64;

            if batch.done || batch.next.is_none() { break; }
            req.cursor = batch.next;
        }
        Ok(stats)
    }

    fn router_config_timeout(&self) -> std::time::Duration {
        // Reuse router default timeout; RouterConfig is private, so copy default.
        // Tests often use small durations; this keeps behavior consistent.
        std::time::Duration::from_secs(5)
    }

    /// Apply one row: compare meta.updated_at, prefer newer.
    async fn apply_row(&self, db: &KuramotoDb, table: &str, row: &RowEnvelope) -> Result<bool, StorageError> {
        let remote_meta: BlobMeta = bincode::decode_from_slice(&row.meta, bincode::config::standard())
            .map_err(|e| StorageError::Bincode(e.to_string()))?
            .0;

        // Fetch local meta if present; if missing treat as older.
        let local_meta_raw = db
            .get_meta_raw_by_table_pk(table, &row.pk)
            .await
            .ok();
        if let Some(lr) = local_meta_raw {
            if let Ok((lm, _)) = bincode::decode_from_slice::<BlobMeta, _>(&lr, bincode::config::standard()) {
                // Skip if local is as new or newer by updated_at (best-effort idempotency)
                if lm.updated_at >= remote_meta.updated_at {
                    return Ok(false);
                }
            }
        }

        // Apply: put or delete. Meta will be synthesized locally, so we rely on updated_at compare for idempotency.
        if row.tombstone {
            db
                .delete_by_table_pk_with_origin(table, &row.pk, WriteOrigin::Plugin(fnv1a_16("remote_fetch")))
                .await?;
        } else {
            db
                .put_by_table_bytes_with_origin(table, &row.data, WriteOrigin::Plugin(fnv1a_16("remote_fetch")))
                .await?;
        }
        Ok(true)
    }
}

#[derive(Default, Clone, Debug)]
pub struct FetchStats {
    pub fetched: u64,
    pub applied: u64,
    pub skipped: u64,
}

#[async_trait]
impl Plugin for RemoteFetch {
    fn attach_db(&self, db: Arc<KuramotoDb>) {
        let _ = self.db.set(Arc::downgrade(&db));
        // Register protocol handler now that we have a DB reference.
        self.register_protocol();
    }

    async fn before_update(
        &self,
        _db: &KuramotoDb,
        _txn: &redb::ReadTransaction,
        _batch: &mut crate::WriteBatch,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{Decode, Encode};
    use redb::{TableDefinition, TableHandle};
    use std::sync::Arc;
    use tokio::task::yield_now;
    use tempfile::tempdir;

    use crate::{
        clock::MockClock,
        storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
        storage_error::StorageError,
        KuramotoDb, StaticTableDef,
    };
    use crate::plugins::communication::router::{Router, RouterConfig};
    use crate::plugins::communication::transports::{
        Connector, PeerResolver,
        inmem::{InMemConnector, InMemResolver},
    };

    // ----- Test entity -----
    #[derive(Clone, Debug, PartialEq, Encode, Decode)]
    struct TEnt { id: u64, name: String, tag: u8 }

    static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("f_test");
    static META: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("f_test_meta");
    static IDX_NAME: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("f_test_name_idx"); // UNIQUE
    static IDX_TAG: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("f_test_tag_idx"); // NON-UNIQUE

    static INDEXES: &[IndexSpec<TEnt>] = &[
        IndexSpec { name: "name", key_fn: |e: &TEnt| e.name.as_bytes().to_vec(), table_def: &IDX_NAME, cardinality: IndexCardinality::Unique },
        IndexSpec { name: "tag", key_fn: |e: &TEnt| e.tag.to_be_bytes().to_vec(), table_def: &IDX_TAG, cardinality: IndexCardinality::NonUnique },
    ];

    impl StorageEntity for TEnt {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> { self.id.to_be_bytes().to_vec() }
        fn table_def() -> StaticTableDef { &TBL }
        fn meta_table_def() -> StaticTableDef { &META }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            match data.first().copied() {
                Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard()).map(|(v, _)| v).map_err(|e| StorageError::Bincode(e.to_string())),
                _ => Err(StorageError::Bincode("bad version".into())),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] { INDEXES }
    }

    fn pid(x: u8) -> crate::plugins::communication::transports::PeerId {
        let mut b = [0u8; 16];
        b[0] = x;
        crate::plugins::communication::transports::PeerId::from_bytes(b)
    }

    async fn setup_pair(ns: u64, a_clock: u64, b_clock: u64) -> (
        Arc<KuramotoDb>, Arc<RemoteFetch>, Arc<Router>, crate::plugins::communication::transports::PeerId,
        Arc<KuramotoDb>, Arc<RemoteFetch>, Arc<Router>, crate::plugins::communication::transports::PeerId,
    ) {
        // Connectors/Resolver
        let a_conn = InMemConnector::with_namespace(pid(1), ns);
        let b_conn = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a_conn.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b_conn.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // Routers
        let clock_a = Arc::new(MockClock::new(a_clock));
        let clock_b = Arc::new(MockClock::new(b_clock));
        let router_a = Router::new(RouterConfig::default(), clock_a.clone());
        let router_b = Router::new(RouterConfig::default(), clock_b.clone());

        // Fetch plugins
        let fetch_a = RemoteFetch::new(router_a.clone());
        let fetch_b = RemoteFetch::new(router_b.clone());

        // DBs with Versioning + Fetch (use temp dirs to avoid collisions)
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let path_a = dir_a.path().join("db.redb");
        let path_b = dir_b.path().join("db.redb");
        let db_a = KuramotoDb::new(path_a.to_str().unwrap(), clock_a.clone(), vec![
            crate::plugins::versioning::VersioningPlugin::new(),
            fetch_a.clone(),
        ]).await;
        let db_b = KuramotoDb::new(path_b.to_str().unwrap(), clock_b.clone(), vec![
            crate::plugins::versioning::VersioningPlugin::new(),
            fetch_b.clone(),
        ]).await;

        db_a.create_table_and_indexes::<TEnt>().unwrap();
        db_b.create_table_and_indexes::<TEnt>().unwrap();

        // Connect peers
        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await; // let tasks spin up

        (db_a, fetch_a, router_a, pid(1), db_b, fetch_b, router_b, pid(2))
    }

    #[tokio::test]
    async fn snapshot_import_basic() {
        let ns = 9001u64;
        let (db_a, _fa, _ra, pa, db_b, fb, _rb, _pb) = setup_pair(ns, 100, 200).await;

        // Seed A
        for (id, name, tag) in [(1u64, "A", 1u8), (2, "B", 1), (3, "C", 2)] {
            db_a.put(TEnt { id, name: name.into(), tag }).await.unwrap();
        }

        // Fetch into B
        let req = ExportRequest { table: TBL.name().into(), start: vec![], end: vec![0xFF], mode: FetchMode::ByPkRange, limit: 0, cursor: None };
        let stats = fb.fetch_from(pa, req).await.unwrap();
        assert_eq!(stats.applied, 3);
        assert_eq!(stats.fetched, 3);

        // Verify data and indexes present on B
        let got = db_b.range_by_pk::<TEnt>(&[], &[0xFF], None).await.unwrap();
        assert_eq!(got.len(), 3);
        let main = db_b.get_index(&IDX_NAME, b"B").await.unwrap().unwrap();
        assert_eq!(u64::from_be_bytes(main.as_slice().try_into().unwrap()), 2);
        let by_tag = db_b.get_by_index_all::<TEnt>(&IDX_TAG, &1u8.to_be_bytes()).await.unwrap();
        assert_eq!(by_tag.len(), 2);
    }

    #[tokio::test]
    async fn idempotent_second_fetch_skips() {
        // B clock much higher than A → local updated_at > remote after first apply
        let ns = 9002u64;
        let (db_a, _fa, _ra, pa, _db_b, fb, _rb, _pb) = setup_pair(ns, 100, 1_000_000).await;
        for id in 1..=5u64 {
            db_a.put(TEnt { id, name: format!("N{id}"), tag: 7 }).await.unwrap();
        }

        let req = ExportRequest { table: TBL.name().into(), start: vec![], end: vec![0xFF], mode: FetchMode::ByPkRange, limit: 0, cursor: None };
        let _stats1 = fb.fetch_from(pa, req.clone()).await.unwrap();
        let stats2 = fb.fetch_from(pa, req).await.unwrap();
        assert_eq!(stats2.applied, 0);
        assert!(stats2.skipped >= 5);
    }

    #[tokio::test]
    async fn tombstone_propagates_when_remote_newer() {
        // A clock higher → deletion meta.newer than B local
        let ns = 9003u64;
        let (db_a, _fa, _ra, pa, db_b, fb, _rb, _pb) = setup_pair(ns, 10_000, 10).await;
        db_a.put(TEnt { id: 42, name: "Z".into(), tag: 9 }).await.unwrap();

        let req = ExportRequest { table: TBL.name().into(), start: vec![], end: vec![0xFF], mode: FetchMode::ByPkRange, limit: 0, cursor: None };
        fb.fetch_from(pa, req.clone()).await.unwrap();
        // Delete on A (meta.updated_at increases on A)
        db_a.delete::<TEnt>(&42u64.to_be_bytes()).await.unwrap();
        // Fetch again → should delete on B
        fb.fetch_from(pa, req).await.unwrap();
        let got = db_b.get_data::<TEnt>(&42u64.to_be_bytes()).await.ok();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn pagination_cursor_over_multiple_batches() {
        let ns = 9004u64;
        let (db_a, _fa, _ra, pa, _db_b, fb, _rb, _pb) = setup_pair(ns, 100, 200).await;
        for id in 1..=5u64 {
            db_a.put(TEnt { id, name: format!("N{id}"), tag: (id % 2) as u8 }).await.unwrap();
        }

        let req = ExportRequest { table: TBL.name().into(), start: 1u64.to_be_bytes().to_vec(), end: 6u64.to_be_bytes().to_vec(), mode: FetchMode::ByPkRange, limit: 2, cursor: None };
        let stats = fb.fetch_from(pa, req).await.unwrap();
        assert_eq!(stats.applied, 5);
    }

    #[tokio::test]
    async fn error_on_unknown_table() {
        let ns = 9005u64;
        let (_db_a, _fa, _ra, pa, _db_b, fb, _rb, _pb) = setup_pair(ns, 100, 200).await;
        let req = ExportRequest { table: "nope".into(), start: vec![], end: vec![0xFF], mode: FetchMode::ByPkRange, limit: 0, cursor: None };
        let err = fb.fetch_from(pa, req).await.err().expect("should error");
        match err { StorageError::Other(msg) => assert!(msg.contains("router")), _ => panic!("unexpected err: {err:?}") }
    }

    #[tokio::test]
    async fn index_unique_range_imports_subset() {
        let ns = 9006u64;
        let (db_a, _fa, _ra, pa, db_b, fb, _rb, _pb) = setup_pair(ns, 100, 200).await;
        for (id, name, tag) in [(1u64, "A", 0u8), (2, "B", 1), (3, "C", 1), (4, "Z", 2)] {
            db_a.put(TEnt { id, name: name.into(), tag }).await.unwrap();
        }
        let req = ExportRequest {
            table: TBL.name().into(),
            start: vec![],
            end: vec![],
            mode: FetchMode::ByIndexRange { index_table: IDX_NAME.name().into(), start_idx: b"B".to_vec(), end_idx: b"Z".to_vec() },
            limit: 0,
            cursor: None,
        };
        let stats = fb.fetch_from(pa, req).await.unwrap();
        assert_eq!(stats.applied, 2, "should import B and C by unique index range");
        // Verify those two present
        let got = db_b.range_by_pk::<TEnt>(&[], &[0xFF], None).await.unwrap();
        let names: Vec<String> = got.into_iter().map(|e| e.name).collect();
        assert!(names.contains(&"B".to_string()) && names.contains(&"C".to_string()));
    }

    #[tokio::test]
    async fn index_nonunique_range_imports_all_matches() {
        let ns = 9007u64;
        let (db_a, _fa, _ra, pa, db_b, fb, _rb, _pb) = setup_pair(ns, 100, 200).await;
        for (id, tag) in [(1u64, 1u8), (2, 1), (3, 2), (4, 1)] {
            db_a.put(TEnt { id, name: format!("U{id}"), tag }).await.unwrap();
        }
        // Fetch all with tag == 1: range [1,2)
        let req = ExportRequest {
            table: TBL.name().into(),
            start: vec![],
            end: vec![],
            mode: FetchMode::ByIndexRange { index_table: IDX_TAG.name().into(), start_idx: 1u8.to_be_bytes().to_vec(), end_idx: 2u8.to_be_bytes().to_vec() },
            limit: 0,
            cursor: None,
        };
        let stats = fb.fetch_from(pa, req).await.unwrap();
        assert_eq!(stats.applied, 3, "three rows have tag=1");
        let got = db_b.get_by_index_all::<TEnt>(&IDX_TAG, &1u8.to_be_bytes()).await.unwrap();
        assert_eq!(got.len(), 3);
    }
}
