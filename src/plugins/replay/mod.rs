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
    // Lightweight metadata to tag a batch for easier filtering
    HarmonizerContext {
        // 0 = own-tree delta, 1 = mirror-tree delta, 2 = snapshot
        kind: u8,
        peer_id: [u8; 16],
        first_dim_hash: u64,
    },
    // Snapshot envelope for harmonizer trees (chunked to handle large states)
    HarmonizerSnapshotStart {
        peer_id: [u8; 16],
        first_dim_hash: u64,
        total_avails: u64,
        total_edges: u64,
        version: u32,
    },
    HarmonizerSnapshotChunk {
        chunk_no: u32,
        availabilities: Vec<SnapshotAvailability>,
        children: Vec<SnapshotChild>,
    },
    HarmonizerSnapshotEnd {
        chunks: u32,
        checksum: u64,
    },
    // Harmonizer topology hints: a peer was added to the local node
    PeerAdded {
        peer_id: [u8; 16],
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
    // Last day (UTC, ms/86400000) when we emitted TableNames mapping
    last_names_day: AtomicU64,
}

impl ReplayPlugin {
    pub fn new() -> Self { Self { seq: AtomicU64::new(0), last_names_day: AtomicU64::new(0) } }

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

// ============ Snapshot row shapes (plugin-agnostic) ============

#[derive(Clone, Debug, Encode, Decode)]
pub struct SnapshotAvailability {
    pub availability_id: [u8; 16],
    pub peer_id: [u8; 16],
    pub level: u16,
    pub complete: bool,
    pub dims: Vec<u64>,
    pub mins: Vec<Vec<u8>>,
    pub maxs: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct SnapshotChild {
    pub parent: [u8; 16],
    pub ordinal: u32,
    pub child: [u8; 16],
}

// ============ Public helper to emit custom replay events ============

static CUSTOM_SEQ: AtomicU64 = AtomicU64::new(0);

/// Emit a custom replay event composed of the provided log items. This writes a single
/// `ReplayEvent` row to the replay table; it does not mutate any domain tables.
///
/// `plugin_tag` should be a 16-bit identifier (e.g., `fnv1a_16("harmonizer")`).
pub async fn emit_custom(
    db: &KuramotoDb,
    plugin_tag: u16,
    items: Vec<LogWriteRequest>,
) -> Result<(), StorageError> {
    // Build a reasonably-unique id from clock + local seq (monotonic across this process).
    let now = db.get_clock().now();
    let seq = CUSTOM_SEQ.fetch_add(1, Ordering::Relaxed) & 0xFFFF;
    let id = (now << 16) | seq;

    // Resolve peer id once
    let id_bytes = crate::plugins::self_identity::SelfIdentity::get_peer_id(db).await?;
    let mut peer_id = [0u8; 16];
    peer_id.copy_from_slice(id_bytes.as_bytes());

    // Persist event under the Replay plugin origin so the normal after_write doesn’t re-log it.
    let ev = ReplayEvent {
        id,
        ts: now,
        origin: plugin_tag,
        peer_id,
        batch: items,
    };
    // Best-effort visibility for debugging: print when a custom replay event is emitted.
    // Keep this lightweight and unconditional; users can filter logs by origin code.
    tracing::info!(
        target: "replay",
        "emit_custom: origin=0x{:04x} items={}",
        plugin_tag,
        ev.batch.len()
    );
    // Ensure table exists and write directly
    let _ = db.create_table_and_indexes::<ReplayEvent>();
    db.put_with_origin::<ReplayEvent>(ev, WriteOrigin::Plugin(REPLAY_PLUGIN_ID)).await
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
        // Map batch and append periodic metadata
        let mut batch = Self::map_batch_to_log(applied);
        const SNAPSHOT_EVERY: u64 = 100;
        if id % SNAPSHOT_EVERY == 0 {
            let stats = Self::compute_db_stats(db);
            batch.push(LogWriteRequest::StatsSnapshot { stats });
        }
        // Emit TableNames at startup (id==0) and at most once per day thereafter.
        const DAY_MS: u64 = 86_400_000;
        let day = now / DAY_MS;
        let mut need_names = id == 0;
        let prev = self.last_names_day.load(Ordering::Relaxed);
        if prev != day {
            need_names = true;
            self.last_names_day.store(day, Ordering::Relaxed);
        }
        if need_names {
            use std::collections::HashMap;
            let mut map: HashMap<u64, String> = HashMap::new();
            // 1) From DB registry (tables explicitly created)
            for (h, name) in Self::compute_table_names(db).into_iter() {
                map.entry(h).or_insert(name);
            }
            // 2) From this batch's table defs (covers lazily-created tables like self_identity)
            for req in applied.iter() {
                match req {
                    WriteRequest::Put { data_table, .. } | WriteRequest::Delete { data_table, .. } => {
                        let h = crate::tables::TableHash::from(*data_table).hash();
                        map.entry(h).or_insert_with(|| data_table.name().to_string());
                    }
                }
            }
            if !map.is_empty() {
                let mappings: Vec<(u64, String)> = map.into_iter().collect();
                batch.push(LogWriteRequest::TableNames { mappings });
            }
        }

        // Optional debug: summarize harmonizer-related writes captured in this event
        #[cfg(feature = "harmonizer")]
        {
            use crate::tables::TableHash as TH;
            let avail_h = TH::from(crate::plugins::harmonizer::AVAILABILITIES_TABLE).hash();
            let child_h = TH::from(crate::plugins::harmonizer::AVAIL_CHILDREN_TBL).hash();
            let mut puts_avail = 0usize;
            let mut puts_child = 0usize;
            let mut dels_avail = 0usize;
            let mut dels_child = 0usize;
            for it in applied.iter() {
                match it {
                    WriteRequest::Put { data_table, .. } => {
                        let h = TH::from(*data_table).hash();
                        if h == avail_h { puts_avail += 1; }
                        if h == child_h { puts_child += 1; }
                    }
                    WriteRequest::Delete { data_table, .. } => {
                        let h = TH::from(*data_table).hash();
                        if h == avail_h { dels_avail += 1; }
                        if h == child_h { dels_child += 1; }
                    }
                }
            }
            if puts_avail + puts_child + dels_avail + dels_child > 0 {
                println!(
                    "[replay] captured harmonizer writes: avail(+{}) child(+{}) avail(-{}) child(-{}) origin={}",
                    puts_avail, puts_child, dels_avail, dels_child,
                    match origin { WriteOrigin::Plugin(id) => format!("Plugin(0x{:04x})", id), WriteOrigin::LocalCommit => "LocalCommit".into(), WriteOrigin::Completer => "Completer".into(), WriteOrigin::RemoteIngest => "RemoteIngest".into() }
                );
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
