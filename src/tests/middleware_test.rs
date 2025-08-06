use std::sync::{Arc, Mutex};

use tempfile::tempdir;

use crate::{
    StaticTableDef,
    clock::Clock,
    database::{KuramotoDb, WriteRequest},
    middlewares::Middleware,
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
};

use bincode::{Decode, Encode};
use redb::TableDefinition;

/* ───── Mock clock ───── */
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> u64 {
        0
    }
}

/* ───── Middleware that counts calls ───── */
struct CounterMiddleware(Arc<Mutex<u32>>);

impl Middleware for CounterMiddleware {
    fn before_write(&self, _req: &mut WriteRequest) -> Result<(), StorageError> {
        *self.0.lock().unwrap() += 1;
        Ok(())
    }
}

/* ───── Minimal entity (no indexes) ───── */
#[derive(Clone, Debug, PartialEq, Encode, Decode)]
struct Foo {
    id: u64,
}

static FOO_TABLE: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("foo");
static FOO_META: TableDefinition<'static, &'static [u8], Vec<u8>> =
    TableDefinition::new("foo_meta");
static FOO_INDEXES: &[IndexSpec<Foo>] = &[];

impl StorageEntity for Foo {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }
    fn table_def() -> StaticTableDef {
        &FOO_TABLE
    }
    fn meta_table_def() -> StaticTableDef {
        &FOO_META
    }
    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        match data.first().copied() {
            Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| StorageError::Bincode(e.to_string())),
            _ => Err(StorageError::Bincode("bad version".into())),
        }
    }
    fn indexes() -> &'static [IndexSpec<Self>] {
        FOO_INDEXES
    }
}

/* ───── The test ───── */
#[tokio::test]
async fn middleware_runs_in_order() {
    // temp dir + db path
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("mw.redb");

    // counter shared with middleware
    let counter = Arc::new(Mutex::new(0u32));
    let mw = Arc::new(CounterMiddleware(counter.clone()));

    // build DB with middleware
    let db = KuramotoDb::new(db_path.to_str().unwrap(), Arc::new(ZeroClock), vec![mw]).await;

    // create tables for Foo and insert one row
    db.create_table_and_indexes::<Foo>().unwrap();
    db.put(Foo { id: 1 }).await.unwrap();

    // middleware must have run exactly once
    assert_eq!(*counter.lock().unwrap(), 1);
}
