use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use async_trait::async_trait;
use bincode::{Encode, Decode};
use redb::{ReadTransaction, TableHandle};

use crate::{
    KuramotoDb, WriteBatch, WriteOrigin, WriteRequest,
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
    tables::TableHash,
    StaticTableDef,
};

use crate::plugins::{Plugin, fnv1a_16};
use redb::TableDefinition;

// ============ Log schema ============

#[derive(Clone, Debug, Encode, Decode)]
pub struct LogIndexPut {
    table_hash: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    unique: bool,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct LogIndexRemove {
    table_hash: u64,
    key: Vec<u8>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub enum LogWriteRequest {
    Put {
        data_table_hash: u64,
        meta_table_hash: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        meta: Vec<u8>,
        index_removes: Vec<LogIndexRemove>,
        index_puts: Vec<LogIndexPut>,
    },
    Delete {
        data_table_hash: u64,
        meta_table_hash: u64,
        key: Vec<u8>,
        meta: Vec<u8>,
        index_removes: Vec<LogIndexRemove>,
    },
    // Periodic database stats snapshot for replay browsing
    StatsSnapshot {
        stats: DatabaseStats,
    },
    // Mapping of table hash -> human-readable name
    TableNames {
        mappings: Vec<(u64, String)>,
    },
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct DatabaseStats {
    pub data_table_count: usize,
    pub index_table_count: usize,
    pub total_rows_data: u64,
    pub total_rows_index: u64,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct ReplayEvent {
    pub id: u64,        // monotonically increasing per-process
    pub ts: u64,        // clock timestamp
    pub origin: u16,    // plugin id or other origin encoded
    pub peer_id: [u8; 16], // emitter peer id
    pub batch: Vec<LogWriteRequest>,
}

// ============ StorageEntity impl ============

static REPLAY_TABLE: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("replay_events");
static REPLAY_META: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("replay_events_meta");
static REPLAY_INDEXES: &[IndexSpec<ReplayEvent>] = &[];

impl StorageEntity for ReplayEvent {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> { self.id.to_be_bytes().to_vec() }
    fn table_def() -> StaticTableDef { &REPLAY_TABLE }
    fn meta_table_def() -> StaticTableDef { &REPLAY_META }
    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        match data.first().copied() {
            Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| StorageError::Bincode(e.to_string())),
            _ => Err(StorageError::Bincode("bad version".into())),
        }
    }
    fn indexes() -> &'static [IndexSpec<Self>] { REPLAY_INDEXES }
}

// ============ Plugin ============

const REPLAY_PLUGIN_ID: u16 = fnv1a_16("replay");

#[derive(Default)]
pub struct ReplayPlugin {
    seq: AtomicU64,
}

impl ReplayPlugin {
    pub fn new() -> Self { Self { seq: AtomicU64::new(0) } }

    pub fn map_batch_to_log(batch: &WriteBatch) -> Vec<LogWriteRequest> {
        batch
            .iter()
            .map(|req| match req {
                WriteRequest::Put { data_table, meta_table, key, value, meta, index_removes, index_puts } => {
                    let dth = TableHash::from(*data_table).hash();
                    let mth = TableHash::from(*meta_table).hash();
                    let irs = index_removes.iter().map(|ir| LogIndexRemove { table_hash: TableHash::from(ir.table).hash(), key: ir.key.clone() }).collect();
                    let ips = index_puts.iter().map(|ip| LogIndexPut { table_hash: TableHash::from(ip.table).hash(), key: ip.key.clone(), value: ip.value.clone(), unique: ip.unique }).collect();
                    LogWriteRequest::Put {
                        data_table_hash: dth,
                        meta_table_hash: mth,
                        key: key.clone(),
                        value: value.clone(),
                        meta: meta.clone(),
                        index_removes: irs,
                        index_puts: ips,
                    }
                }
                WriteRequest::Delete { data_table, meta_table, key, meta, index_removes } => {
                    let dth = TableHash::from(*data_table).hash();
                    let mth = TableHash::from(*meta_table).hash();
                    let irs = index_removes.iter().map(|ir| LogIndexRemove { table_hash: TableHash::from(ir.table).hash(), key: ir.key.clone() }).collect();
                    LogWriteRequest::Delete {
                        data_table_hash: dth,
                        meta_table_hash: mth,
                        key: key.clone(),
                        meta: meta.clone(),
                        index_removes: irs,
                    }
                }
            })
            .collect()
    }

    fn compute_db_stats(db: &KuramotoDb) -> DatabaseStats {
        let (data_tables, index_tables) = db.list_registered_tables();
        let mut total_rows_data: u64 = 0;
        let mut total_rows_index: u64 = 0;
        for t in &data_tables {
            if let Ok(c) = db.count_rows_in_table(*t) { total_rows_data += c; }
        }
        for t in &index_tables {
            if let Ok(c) = db.count_rows_in_table(*t) { total_rows_index += c; }
        }
        DatabaseStats {
            data_table_count: data_tables.len(),
            index_table_count: index_tables.len(),
            total_rows_data,
            total_rows_index,
        }
    }

    fn compute_table_names(db: &KuramotoDb) -> Vec<(u64, String)> {
        let (data_tables, _index_tables) = db.list_registered_tables();
        let mut out = Vec::new();
        for t in &data_tables {
            let h = crate::tables::TableHash::from(*t).hash();
            if let Some(def) = db.resolve_data_table_by_hash(h) {
                out.push((h, def.name().to_string()));
            }
        }
        out
    }
}

impl ReplayEvent {
    pub fn items_count(&self) -> usize { self.batch.len() }
}

#[async_trait]
impl Plugin for ReplayPlugin {
    fn attach_db(&self, db: Arc<KuramotoDb>) {
        // Ensure tables exist
        let _ = db.create_table_and_indexes::<ReplayEvent>();
    }

    async fn before_update(
        &self,
        _db: &KuramotoDb,
        _txn: &ReadTransaction,
        _batch: &mut WriteBatch,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn after_write(
        &self,
        db: &KuramotoDb,
        applied: &Vec<WriteRequest>,
        origin: WriteOrigin,
    ) -> Result<(), StorageError> {
        // Avoid logging ourselves
        if matches!(origin, WriteOrigin::Plugin(id) if id == REPLAY_PLUGIN_ID) {
            return Ok(());
        }

        let now = db.get_clock().now();
        let id = self.seq.fetch_add(1, Ordering::Relaxed);
        let id_bytes = crate::plugins::self_identity::SelfIdentity::get_peer_id(db).await?;
        let mut peer_id = [0u8; 16];
        peer_id.copy_from_slice(id_bytes.as_bytes());
        // Map batch and maybe append periodic snapshots/metadata
        let mut batch = Self::map_batch_to_log(applied);
        const SNAPSHOT_EVERY: u64 = 100;
        if id % SNAPSHOT_EVERY == 0 {
            let stats = Self::compute_db_stats(db);
            batch.push(LogWriteRequest::StatsSnapshot { stats });
            let mappings = Self::compute_table_names(db);
            if !mappings.is_empty() {
                batch.push(LogWriteRequest::TableNames { mappings });
            }
        }

        let event = ReplayEvent {
            id,
            ts: now,
            origin: match origin { WriteOrigin::Plugin(id) => id, WriteOrigin::LocalCommit => 1, WriteOrigin::Completer => 2, WriteOrigin::RemoteIngest => 3 },
            peer_id,
            batch,
        };

        // Persist the event using the write queue, tagging origin as this plugin
        db.put_with_origin::<ReplayEvent>(event, WriteOrigin::Plugin(REPLAY_PLUGIN_ID)).await
    }
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KuramotoDb, storage_entity::{IndexSpec, StorageEntity}, storage_error::StorageError};
    use bincode::{Encode, Decode};
    use redb::TableDefinition;
    use std::sync::Arc;

    #[derive(Clone, Debug, PartialEq, Encode, Decode)]
    struct Foo { id: u64, name: String }

    static FOO: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("foo");
    static FOO_META: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("foo_meta");
    static FOO_INDEXES: &[IndexSpec<Foo>] = &[];

    impl StorageEntity for Foo {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> { self.id.to_be_bytes().to_vec() }
        fn table_def() -> StaticTableDef { &FOO }
        fn meta_table_def() -> StaticTableDef { &FOO_META }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            match data.first().copied() {
                Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard()).map(|(v, _)| v).map_err(|e| StorageError::Bincode(e.to_string())),
                _ => Err(StorageError::Bincode("bad version".into())),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] { FOO_INDEXES }
    }

    #[tokio::test]
    async fn replay_persists_events() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(crate::clock::MockClock::new(1));
        let plugin = Arc::new(ReplayPlugin::new());
        let db = KuramotoDb::new(dir.path().join("replay_test.redb").to_str().unwrap(), clock, vec![plugin]).await;
        db.create_table_and_indexes::<Foo>().unwrap();

        // Put one row
        db.put(Foo { id: 1, name: "A".into() }).await.unwrap();

        // Give the async after_write some time to enqueue and commit its event
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Read events
        let events: Vec<ReplayEvent> = db
            .range_by_pk::<ReplayEvent>(&0u64.to_be_bytes(), &u64::MAX.to_be_bytes(), None)
            .await
            .unwrap();

        assert!(!events.is_empty(), "expected at least one replay event");
        // The latest should contain a Put in its batch
        let has_put = events.iter().any(|ev| ev.batch.iter().any(|b| matches!(b, LogWriteRequest::Put { .. })));
        assert!(has_put, "expected a Put in replay batch");
    }
}
