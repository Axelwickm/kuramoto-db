use bincode::{Decode, Encode};
use redb::TableDefinition;
use std::sync::Arc;
use tempfile::tempdir;

use crate::{
    clock::Clock,
    region_lock::RegionLock,
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
    {KuramotoDb, StaticTableDef, WriteRequest},
};

// ============== Mock clock ==============
struct MockClock {
    ts: std::sync::Mutex<u64>,
}

impl MockClock {
    fn new(start: u64) -> Self {
        Self {
            ts: std::sync::Mutex::new(start),
        }
    }
    fn advance(&self, delta: u64) {
        *self.ts.lock().unwrap() += delta;
    }
}

impl Clock for MockClock {
    fn now(&self) -> u64 {
        *self.ts.lock().unwrap()
    }
}

// ============== TestEntity ==============
#[derive(Clone, Debug, PartialEq, Encode, Decode)]
struct TestEntity {
    id: u64,
    name: String,
    value: i32,
}

static TEST_TABLE: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test");

static TEST_META: TableDefinition<'static, &'static [u8], Vec<u8>> =
    TableDefinition::new("test_meta");

static TEST_NAME_INDEX: TableDefinition<'static, &'static [u8], Vec<u8>> =
    TableDefinition::new("test_name_idx");

// ---------- static slice of specs ----------
static TEST_INDEXES: &[IndexSpec<TestEntity>] = &[IndexSpec {
    name: "name",
    key_fn: |e: &TestEntity| e.name.as_bytes().to_vec(), // index key = name
    table_def: &TEST_NAME_INDEX,
}];

impl StorageEntity for TestEntity {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }

    fn table_def() -> StaticTableDef {
        &TEST_TABLE
    }

    fn meta_table_def() -> StaticTableDef {
        &TEST_META
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
        TEST_INDEXES
    }
}

// ---------- ASSERT HELPERS ----------
async fn assert_row<E: StorageEntity + PartialEq + std::fmt::Debug>(
    sys: &KuramotoDb,
    key: &[u8],
    expect_data: Option<&E>,
    expect_version: u32,

    expect_created: u64,
    expect_updated: u64,
    expect_deleted: bool,
) {
    match expect_data {
        Some(d) => assert_eq!(sys.get_data::<E>(key).await.unwrap(), *d),
        None => assert!(sys.get_data::<E>(key).await.is_err()),
    };
    let meta = sys.get_meta::<E>(key).await.unwrap();
    assert_eq!(meta.created_at, expect_created);
    assert_eq!(meta.updated_at, expect_updated);
    assert_eq!(meta.version, expect_version);

    assert_eq!(meta.deleted_at.is_some(), expect_deleted);
    assert_eq!(meta.region_lock, RegionLock::None);
}

async fn assert_index_row(
    sys: &KuramotoDb,
    idx_table: StaticTableDef,
    idx_key: &[u8],
    expect_main_key: Option<&[u8]>,
) {
    let got = sys.get_index(idx_table, idx_key).await.unwrap();
    match expect_main_key {
        Some(main) => {
            let v = got.expect("index row missing");
            assert_eq!(v, main);
        }
        None => assert!(got.is_none(), "index row should be absent"),
    }
}

/// Assert that get_by_index returns the expected entity (or None).
async fn assert_get_by_index<E: StorageEntity + PartialEq + std::fmt::Debug>(
    sys: &KuramotoDb,
    idx_table: StaticTableDef,
    idx_key: &[u8],
    expect: Option<&E>,
) {
    let got = sys.get_by_index::<E>(idx_table, idx_key).await.unwrap();
    match expect {
        Some(e) => assert_eq!(got.as_ref(), Some(e)),
        None => assert!(got.is_none(), "entity should be absent"),
    }
}

// ============== CRUD TESTS ==============

#[tokio::test]
async fn insert_and_read() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(1_000));
    let sys = KuramotoDb::new(dir.path().join("db.redb").to_str().unwrap(), clock.clone()).await;

    let e = TestEntity {
        id: 1,
        name: "Alice".into(),
        value: 42,
    };
    sys.put(e.clone()).await.unwrap();

    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 0, 1_000, 1_000, false).await;
}

#[tokio::test]
async fn overwrite_updates_meta() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(10));
    let sys = KuramotoDb::new(dir.path().join("db.redb").to_str().unwrap(), clock.clone()).await;

    let mut e = TestEntity {
        id: 99,
        name: "X".into(),
        value: 1,
    };
    sys.put(e.clone()).await.unwrap(); // initial
    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 0, 10, 10, false).await;
    clock.advance(5);
    e.value = 2;
    sys.put(e.clone()).await.unwrap(); // overwrite

    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 1, 10, 15, false).await;
}

#[tokio::test]
async fn delete_and_undelete() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(500));
    let sys = KuramotoDb::new(dir.path().join("db.redb").to_str().unwrap(), clock.clone()).await;

    let e = TestEntity {
        id: 7,
        name: "Y".into(),
        value: 0,
    };
    sys.put(e.clone()).await.unwrap(); // insert
    clock.advance(10);
    sys.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap(); // delete
    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), None, 0, 500, 510, true).await;

    clock.advance(5);
    sys.put(e.clone()).await.unwrap(); // undelete / re-insert
    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 1, 500, 515, false).await;
}

#[tokio::test]
async fn stale_version_rejected() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(50));
    let sys = KuramotoDb::new(
        dir.path().join("stale.redb").to_str().unwrap(),
        clock.clone(),
    )
    .await;

    // --- insert normally ---
    let e = TestEntity {
        id: 5,
        name: "S".into(),
        value: 5,
    };
    sys.put(e.clone()).await.unwrap();

    // --- fetch existing meta so we can craft a stale write ---
    let meta = sys
        .get_meta::<TestEntity>(&e.id.to_be_bytes())
        .await
        .unwrap();

    // Build a stale WriteRequest by hand (version not bumped)
    let stale_wr = WriteRequest::Put {
        data_table: TestEntity::table_def(),
        meta_table: TestEntity::meta_table_def(),
        key: e.primary_key(),
        value: e.to_bytes(),
        meta: bincode::encode_to_vec(meta, bincode::config::standard()).unwrap(), // same version
        index_puts: vec![],
        index_removes: vec![],
        respond_to: tokio::sync::oneshot::channel().0, // will never be read
    };

    // Push manually and expect StaleVersion
    let outcome = sys.raw_write(stale_wr).await;
    matches!(outcome, Err(StorageError::PutButNoVersionIncrease));
}

// ============== INDEX TESTS ==============
#[tokio::test]
async fn index_insert_update_delete() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let sys = KuramotoDb::new(dir.path().join("idx.redb").to_str().unwrap(), clock.clone()).await;

    // ---- insert ----
    let mut e = TestEntity {
        id: 1,
        name: "A".into(),
        value: 1,
    };
    sys.put(e.clone()).await.unwrap();
    assert_index_row(&sys, &TEST_NAME_INDEX, b"A", Some(&e.id.to_be_bytes())).await;
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"A", Some(&e)).await;

    // ---- update (name changes) ----
    clock.advance(1);
    e.name = "B".into();
    sys.put(e.clone()).await.unwrap();
    assert_index_row(&sys, &TEST_NAME_INDEX, b"A", None).await; // old gone
    assert_index_row(&sys, &TEST_NAME_INDEX, b"B", Some(&e.id.to_be_bytes())).await; // new present
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"A", None).await;
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"B", Some(&e)).await;

    // ---- delete ----
    clock.advance(1);
    sys.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap();
    assert_index_row(&sys, &TEST_NAME_INDEX, b"B", None).await; // index row removed
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"B", None).await;
}

#[tokio::test]
async fn duplicate_index_rejected() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let sys = KuramotoDb::new(dir.path().join("dup.redb").to_str().unwrap(), clock.clone()).await;

    let a = TestEntity {
        id: 1,
        name: "X".into(),
        value: 0,
    };
    let b = TestEntity {
        id: 2,
        name: "X".into(),
        value: 1,
    }; // same name key

    sys.put(a).await.unwrap();
    let err = sys.put(b).await.err().expect("should error");
    matches!(err, StorageError::DuplicateIndexKey { .. });
}
