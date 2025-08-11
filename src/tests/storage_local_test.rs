use async_trait::async_trait;
use bincode::{Decode, Encode};
use redb::TableDefinition;
use redb::WriteTransaction;
use std::sync::Arc;
use tempfile::tempdir;

use crate::clock::MockClock;
use crate::communication::router::Router;
use crate::{
    KuramotoDb, StaticTableDef, WriteBatch, WriteRequest,
    meta::BlobMeta,
    middlewares::Middleware,
    region_lock::RegionLock,
    storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
    storage_error::StorageError,
};

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
    TableDefinition::new("test_name_idx"); // UNIQUE

static TEST_VALUE_INDEX: TableDefinition<'static, &'static [u8], Vec<u8>> =
    TableDefinition::new("test_value_idx"); // NON-UNIQUE

static TEST_INDEXES: &[IndexSpec<TestEntity>] = &[
    IndexSpec {
        name: "name",
        key_fn: |e: &TestEntity| e.name.as_bytes().to_vec(),
        table_def: &TEST_NAME_INDEX,
        cardinality: IndexCardinality::Unique,
    },
    IndexSpec {
        name: "value",
        key_fn: |e: &TestEntity| e.value.to_be_bytes().to_vec(), // many can share same value
        table_def: &TEST_VALUE_INDEX,
        cardinality: IndexCardinality::NonUnique,
    },
];

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
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("db.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;

    // Should fail to put before table is created
    let e = TestEntity {
        id: 1,
        name: "Alice".into(),
        value: 42,
    };

    // Now create table and indexes
    sys.create_table_and_indexes::<TestEntity>().unwrap();

    // Now put should succeed
    sys.put(e.clone()).await.unwrap();

    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 0, 1_000, 1_000, false).await;
}

#[tokio::test(start_paused = true)]
async fn overwrite_updates_meta() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(10));
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("db.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;

    sys.create_table_and_indexes::<TestEntity>().unwrap();

    let mut e = TestEntity {
        id: 99,
        name: "X".into(),
        value: 1,
    };
    sys.put(e.clone()).await.unwrap(); // initial
    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 0, 10, 10, false).await;
    clock.advance(5).await;
    e.value = 2;
    sys.put(e.clone()).await.unwrap(); // overwrite

    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 1, 10, 15, false).await;
}

#[tokio::test(start_paused = true)]
async fn delete_and_undelete() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(500));
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("db.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;

    sys.create_table_and_indexes::<TestEntity>().unwrap();

    let e = TestEntity {
        id: 7,
        name: "Y".into(),
        value: 0,
    };
    sys.put(e.clone()).await.unwrap(); // insert
    clock.advance(10).await;
    sys.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap(); // delete
    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), None, 0, 500, 510, true).await;

    clock.advance(5).await;
    sys.put(e.clone()).await.unwrap(); // undelete / re-insert
    assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 1, 500, 515, false).await;
}

#[tokio::test]
async fn stale_version_rejected() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(50));
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("stale.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;

    sys.create_table_and_indexes::<TestEntity>().unwrap();

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
    };

    // Push manually and expect StaleVersion
    let outcome = sys.raw_write(stale_wr).await;
    matches!(outcome, Err(StorageError::PutButNoVersionIncrease));
}

// ============== INDEX TESTS ==============
#[tokio::test(start_paused = true)]
async fn index_insert_update_delete() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("idx.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;

    sys.create_table_and_indexes::<TestEntity>().unwrap();

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
    clock.advance(1).await;
    e.name = "B".into();
    sys.put(e.clone()).await.unwrap();
    assert_index_row(&sys, &TEST_NAME_INDEX, b"A", None).await; // old gone
    assert_index_row(&sys, &TEST_NAME_INDEX, b"B", Some(&e.id.to_be_bytes())).await; // new present
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"A", None).await;
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"B", Some(&e)).await;

    // ---- delete ----
    clock.advance(1).await;
    sys.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap();
    assert_index_row(&sys, &TEST_NAME_INDEX, b"B", None).await; // index row removed
    assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"B", None).await;
}

#[tokio::test]
async fn duplicate_index_rejected() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("dup.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;

    sys.create_table_and_indexes::<TestEntity>().unwrap();

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

#[tokio::test]
async fn duplicate_index_allowed_nonunique() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let router = Router::new(Default::default(), clock.clone());
    let sys = KuramotoDb::new(
        dir.path().join("dup_ok.redb").to_str().unwrap(),
        clock.clone(),
        vec![],
        router,
    )
    .await;
    sys.create_table_and_indexes::<TestEntity>().unwrap();

    // Two different entities share the same "value"
    let a = TestEntity {
        id: 10,
        name: "A".into(),
        value: 7,
    };
    let b = TestEntity {
        id: 11,
        name: "B".into(),
        value: 7,
    };

    sys.put(a.clone()).await.unwrap();
    sys.put(b.clone()).await.unwrap(); // should NOT error

    // Range-based lookup should yield both ids
    let all_main_keys = sys
        .get_index_all(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
        .await
        .unwrap();
    assert_eq!(all_main_keys.len(), 2);
    assert!(all_main_keys.contains(&a.id.to_be_bytes().to_vec()));
    assert!(all_main_keys.contains(&b.id.to_be_bytes().to_vec()));

    // Fetch entities by non-unique index
    let got = sys
        .get_by_index_all::<TestEntity>(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
        .await
        .unwrap();
    assert_eq!(got.len(), 2);
    assert!(got.contains(&a));
    assert!(got.contains(&b));

    // Deleting one removes only its (idx_key, pk) mapping
    sys.delete::<TestEntity>(&a.id.to_be_bytes()).await.unwrap();
    let rest = sys
        .get_by_index_all::<TestEntity>(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
        .await
        .unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0], b);
}

#[tokio::test]
async fn middleware_applied_in_order() {
    // ───── two middle-wares with observable, order-dependent effects ─────
    struct Add100; // updated_at += 100
    struct Double; // updated_at *= 2

    #[async_trait]
    impl Middleware for Add100 {
        async fn before_write(
            &self,
            _db: &KuramotoDb,
            _txn: &WriteTransaction,
            batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            for req in batch {
                if let WriteRequest::Put { meta, .. } = req {
                    let (mut m, _): (BlobMeta, _) =
                        bincode::decode_from_slice(meta, bincode::config::standard())
                            .map_err(|o| StorageError::Bincode(o.to_string()))?;
                    m.updated_at += 100;
                    *meta = bincode::encode_to_vec(m, bincode::config::standard())
                        .map_err(|o| StorageError::Bincode(o.to_string()))?;
                }
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Middleware for Double {
        async fn before_write(
            &self,
            _db: &KuramotoDb,
            _txn: &WriteTransaction,
            batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            for req in batch {
                if let WriteRequest::Put { meta, .. } = req {
                    let (mut m, _): (BlobMeta, _) =
                        bincode::decode_from_slice(meta, bincode::config::standard())
                            .map_err(|o| StorageError::Bincode(o.to_string()))?;
                    m.updated_at *= 2;
                    *meta = bincode::encode_to_vec(m, bincode::config::standard())
                        .map_err(|o| StorageError::Bincode(o.to_string()))?;
                }
            }
            Ok(())
        }
    }

    // ───── assemble DB with middle-wares in a specific order ─────
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(MockClock::new(100)); // initial now = 100
    let router = Router::new(Default::default(), clock.clone());

    let db = KuramotoDb::new(
        dir.path().join("order.redb").to_str().unwrap(),
        clock.clone(),
        vec![Arc::new(Add100), Arc::new(Double)], // <- order!
        router,
    )
    .await;
    db.create_table_and_indexes::<TestEntity>().unwrap();

    // ───── put one entity ─────
    let e = TestEntity {
        id: 1,
        name: "O".into(),
        value: 9,
    };
    db.put(e.clone()).await.unwrap();

    // ───── meta.updated_at should be (100 + 100) * 2 = 400 ─────
    let meta = db
        .get_meta::<TestEntity>(&1u64.to_be_bytes())
        .await
        .unwrap();
    assert_eq!(meta.updated_at, 400);
}

// ───────── RANGE HELPERS: one test per public function ──────────────────────

#[tokio::test]
async fn range_by_pk_basic() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let router = Router::new(Default::default(), clock.clone());
    let db = KuramotoDb::new(
        dir.path().join("pk.redb").to_str().unwrap(),
        clock,
        vec![],
        router,
    )
    .await;
    db.create_table_and_indexes::<TestEntity>().unwrap();

    // ids 1-3
    for i in 1..=3 {
        db.put(TestEntity {
            id: i,
            name: (b'A' + i as u8 - 1).to_string(),
            value: i as i32,
        })
        .await
        .unwrap();
    }

    let got = db
        .range_by_pk::<TestEntity>(&2u64.to_be_bytes(), &4u64.to_be_bytes(), None)
        .await
        .unwrap();
    assert_eq!(got.iter().map(|e| e.id).collect::<Vec<_>>(), vec![2, 3]);
}

#[tokio::test]
async fn range_by_index_basic() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let router = Router::new(Default::default(), clock.clone());
    let db = KuramotoDb::new(
        dir.path().join("idx.redb").to_str().unwrap(),
        clock,
        vec![],
        router,
    )
    .await;
    db.create_table_and_indexes::<TestEntity>().unwrap();

    for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
        db.put(TestEntity {
            id,
            name: name.into(),
            value: 0,
        })
        .await
        .unwrap();
    }

    let got = db
        .range_by_index::<TestEntity>(&TEST_NAME_INDEX, b"B", b"D", None)
        .await
        .unwrap();
    assert_eq!(
        got.iter().map(|e| e.name.clone()).collect::<Vec<_>>(),
        vec!["B", "C"]
    );
}

#[tokio::test]
async fn range_with_meta_by_pk_basic() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(123)); // initial now = 100
    let router = Router::new(Default::default(), clock.clone());
    let db = KuramotoDb::new(
        dir.path().join("pk_meta.redb").to_str().unwrap(),
        clock,
        vec![],
        router,
    )
    .await;
    db.create_table_and_indexes::<TestEntity>().unwrap();

    db.put(TestEntity {
        id: 10,
        name: "X".into(),
        value: 1,
    })
    .await
    .unwrap();
    db.put(TestEntity {
        id: 11,
        name: "Y".into(),
        value: 1,
    })
    .await
    .unwrap();

    let got = db
        .range_with_meta_by_pk::<TestEntity>(&10u64.to_be_bytes(), &12u64.to_be_bytes(), None)
        .await
        .unwrap();
    assert_eq!(got.len(), 2);
    assert!(
        got.iter()
            .all(|(_, m)| m.version == 0 && m.created_at == 123)
    );
}

#[tokio::test]
async fn range_with_meta_by_index_basic() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(999));
    let router = Router::new(Default::default(), clock.clone());
    let db = KuramotoDb::new(
        dir.path().join("idx_meta.redb").to_str().unwrap(),
        clock,
        vec![],
        router,
    )
    .await;
    db.create_table_and_indexes::<TestEntity>().unwrap();

    let rows = [
        TestEntity {
            id: 5,
            name: "K".into(),
            value: 2,
        },
        TestEntity {
            id: 6,
            name: "L".into(),
            value: 2,
        },
    ];
    for r in &rows {
        db.put(r.clone()).await.unwrap();
    }

    let got = db
        .range_with_meta_by_index::<TestEntity>(&TEST_NAME_INDEX, b"K", b"M", None)
        .await
        .unwrap();
    assert_eq!(got.len(), 2);
    for ((e, meta), expected) in got.iter().zip(rows.iter()) {
        assert_eq!(e, expected);
        assert_eq!(meta.created_at, 999);
    }
}
