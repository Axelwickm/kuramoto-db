use std::sync::Arc;

use async_trait::async_trait;
use redb::ReadTransaction;

pub mod communication;
pub mod versioning;

// NOTE: disabled
// Harmonizer is optional; gate entire module behind feature.
// #[cfg(feature = "harmonizer")]
// pub mod harmonizer;

// Web admin plugin (optional)
#[cfg(feature = "web_admin")]
pub mod web_admin;

// Replay plugin: persists write batches for later inspection/replay.
pub mod replay;

// Self-identity plugin: manages local persistent peer id (unsyncable)
pub mod self_identity;

// Remote fetch plugin: simple symmetric fetch/export over communication router.
pub mod fetch;

use crate::database::WriteRequest;
use crate::{KuramotoDb, WriteBatch, WriteOrigin, storage_error::StorageError};

// Just to hash the name into something nice and stable.
pub const fn fnv1a_16(s: &str) -> u16 {
    let mut hash: u32 = 0x811C_9DC5; // 32-bit offset basis
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    (hash & 0xFFFF) as u16
}

#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn attach_db(&self, db: Arc<KuramotoDb>);

    async fn before_update(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        batch: &mut WriteBatch,
    ) -> Result<(), StorageError>;

    /// Origin-aware hook. Default forwards to `before_update` for backwards compatibility.
    async fn before_update_with_origin(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        batch: &mut WriteBatch,
        origin: WriteOrigin,
    ) -> Result<(), StorageError> {
        let _ = origin; // default impl ignores origin
        self.before_update(db, txn, batch).await
    }

    /// Called after a write batch has been committed successfully.
    /// Implementations should be lightweight; if they enqueue new writes,
    /// they should avoid synchronous deadlocks with the write loop.
    async fn after_write(
        &self,
        _db: &KuramotoDb,
        _applied: &Vec<WriteRequest>,
        _origin: WriteOrigin,
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
    use redb::TableDefinition;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tempfile::tempdir;

    use crate::storage_error::StorageError;
    use crate::{
        KuramotoDb, StaticTableDef,
        clock::MockClock,
        storage_entity::{IndexSpec, StorageEntity},
    };

    /* ───── Hook that increments a counter ───── */
    struct CounterHook {
        count: Arc<AtomicU32>,
    }

    #[async_trait]
    impl Plugin for Arc<CounterHook> {
        async fn before_update(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }

    /* ───── Plugin that registers the counter hook ───── */
    struct CounterPlugin {
        count: Arc<AtomicU32>,
    }

    #[async_trait]
    impl Plugin for CounterPlugin {
        async fn before_update(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            Ok(())
        }

        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }

    /* ───── Minimal entity (no indexes) ───── */
    #[derive(Clone, Debug, PartialEq, Encode, Decode)]
    struct Foo {
        id: u64,
    }

    static FOO_TABLE: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("foo");
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
    async fn plugin_before_update_runs() {
        // temp dir + db path
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("plugins.redb");

        // counter shared with hook
        let counter = Arc::new(AtomicU32::new(0));
        let plugin = Arc::new(CounterPlugin {
            count: counter.clone(),
        });

        // build DB with plugin (build a Router even if unused)
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(db_path.to_str().unwrap(), clock, vec![plugin]).await;

        // create tables for Foo and insert one row
        db.create_table_and_indexes::<Foo>().unwrap();
        db.put(Foo { id: 1 }).await.unwrap();

        // Hook must have run exactly once
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    /* ───── Test after_write hook ───── */
    struct AfterWriteCounter {
        count: Arc<AtomicU32>,
    }

    #[async_trait]
    impl Plugin for AfterWriteCounter {
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
            _db: &KuramotoDb,
            applied: &Vec<crate::database::WriteRequest>,
            _origin: crate::WriteOrigin,
        ) -> Result<(), StorageError> {
            // Increment by number of items to ensure we saw the batch
            self.count
                .fetch_add(applied.len() as u32, Ordering::Relaxed);
            Ok(())
        }

        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }

    #[tokio::test]
    async fn plugin_after_write_runs() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("plugins_after.redb");

        let counter = Arc::new(AtomicU32::new(0));
        let plugin = Arc::new(AfterWriteCounter {
            count: counter.clone(),
        });

        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(db_path.to_str().unwrap(), clock, vec![plugin]).await;

        db.create_table_and_indexes::<Foo>().unwrap();
        db.put(Foo { id: 1 }).await.unwrap();

        // Allow async after_write to run
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(counter.load(Ordering::Relaxed) >= 1);
    }
}
