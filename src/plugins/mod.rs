use std::sync::Arc;

use async_trait::async_trait;
use redb::ReadTransaction;

pub mod communication;
pub mod harmonizer;

use crate::{KuramotoDb, WriteBatch, storage_error::StorageError};

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
}
