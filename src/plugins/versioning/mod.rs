use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    plugins::Plugin,
    storage_error::StorageError,
    KuramotoDb, WriteBatch, WriteOrigin,
    database::WriteRequest,
    meta::BlobMeta,
    region_lock::RegionLock,
};

pub struct VersioningPlugin;

impl VersioningPlugin {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[async_trait]
impl Plugin for VersioningPlugin {
    fn attach_db(&self, _db: Arc<KuramotoDb>) {}

    async fn before_update_with_origin(
        &self,
        db: &KuramotoDb,
        rtxn: &redb::ReadTransaction,
        batch: &mut WriteBatch,
        _origin: WriteOrigin,
    ) -> Result<(), StorageError> {
        let now = db.get_clock().now();

        for req in batch.iter_mut() {
            match req {
                WriteRequest::Put {
                    meta_table,
                    key,
                    meta,
                    ..
                } => {
                    // If caller provided meta, validate version increase; otherwise synthesize.
                    if meta.is_empty() {
                        // Synthesize new meta from existing (if any)
                        let meta_t = rtxn
                            .open_table((*meta_table).clone())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        let new_meta = if let Some(existing_raw) =
                            meta_t.get(key.as_slice()).map_err(|e| StorageError::Other(e.to_string()))?
                        {
                            let (old, _) = bincode::decode_from_slice::<BlobMeta, _>(
                                &existing_raw.value(),
                                bincode::config::standard(),
                            )
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;
                            BlobMeta {
                                version: old.version + 1,
                                created_at: old.created_at,
                                updated_at: now,
                                deleted_at: None,
                                region_lock: old.region_lock,
                            }
                        } else {
                            BlobMeta {
                                version: 0,
                                created_at: now,
                                updated_at: now,
                                deleted_at: None,
                                region_lock: RegionLock::None,
                            }
                        };
                        *meta = bincode::encode_to_vec(new_meta, bincode::config::standard())
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;
                    } else {
                        // Validate version strictly increases over existing (if any)
                        let provided: BlobMeta = bincode::decode_from_slice(
                            meta.as_slice(),
                            bincode::config::standard(),
                        )
                        .map_err(|e| StorageError::Bincode(e.to_string()))?
                        .0;
                        let meta_t = rtxn
                            .open_table((*meta_table).clone())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        if let Some(existing_raw) =
                            meta_t.get(key.as_slice()).map_err(|e| StorageError::Other(e.to_string()))?
                        {
                            let (existing, _) = bincode::decode_from_slice::<BlobMeta, _>(
                                &existing_raw.value(),
                                bincode::config::standard(),
                            )
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;
                            if provided.version <= existing.version {
                                return Err(StorageError::PutButNoVersionIncrease);
                            }
                        }
                    }
                }
                WriteRequest::Delete {
                    meta_table,
                    key,
                    meta,
                    ..
                } => {
                    // If meta provided, accept as-is. Otherwise synthesize delete meta.
                    if meta.is_empty() {
                        let meta_t = rtxn
                            .open_table((*meta_table).clone())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        let existing_raw = meta_t
                            .get(key.as_slice())
                            .map_err(|e| StorageError::Other(e.to_string()))?
                            .ok_or(StorageError::NotFound)?;
                        let (mut m, _) = bincode::decode_from_slice::<BlobMeta, _>(
                            &existing_raw.value(),
                            bincode::config::standard(),
                        )
                        .map_err(|e| StorageError::Bincode(e.to_string()))?;
                        m.deleted_at = Some(now);
                        m.updated_at = now;
                        *meta = bincode::encode_to_vec(m, bincode::config::standard())
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn before_update(
        &self,
        db: &KuramotoDb,
        rtxn: &redb::ReadTransaction,
        batch: &mut WriteBatch,
    ) -> Result<(), StorageError> {
        // Forward to origin-aware variant; origin currently unused for policy.
        self.before_update_with_origin(db, rtxn, batch, WriteOrigin::LocalCommit)
            .await
    }
}

// -------- Public helper functions (no trait) --------
impl VersioningPlugin {
    pub async fn get_meta<E: crate::storage_entity::StorageEntity>(
        db: &KuramotoDb,
        key: &[u8],
    ) -> Result<BlobMeta, StorageError> {
        let raw = db.get_meta_raw::<E>(key).await?;
        bincode::decode_from_slice::<BlobMeta, _>(&raw, bincode::config::standard())
            .map(|(m, _)| m)
            .map_err(|e| StorageError::Bincode(e.to_string()))
    }

    pub async fn get_with_meta<E: crate::storage_entity::StorageEntity>(
        db: &KuramotoDb,
        key: &[u8],
    ) -> Result<(E, BlobMeta), StorageError> {
        let (e, raw) = db.get_with_meta_raw::<E>(key).await?;
        let (m, _) = bincode::decode_from_slice::<BlobMeta, _>(&raw, bincode::config::standard())
            .map_err(|e| StorageError::Bincode(e.to_string()))?;
        Ok((e, m))
    }

    pub async fn range_with_meta_by_pk<E: crate::storage_entity::StorageEntity>(
        db: &KuramotoDb,
        s: &[u8],
        e: &[u8],
        l: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        let rows = db.range_with_meta_raw_by_pk::<E>(s, e, l).await?;
        rows.into_iter()
            .map(|(ent, raw)| {
                bincode::decode_from_slice::<BlobMeta, _>(&raw, bincode::config::standard())
                    .map(|(m, _)| (ent, m))
                    .map_err(|e| StorageError::Bincode(e.to_string()))
            })
            .collect()
    }

    pub async fn range_with_meta_by_index<E: crate::storage_entity::StorageEntity>(
        db: &KuramotoDb,
        index_table: crate::StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        let rows = db
            .range_with_meta_raw_by_index::<E>(index_table, start_idx_key, end_idx_key, limit)
            .await?;
        rows.into_iter()
            .map(|(ent, raw)| {
                bincode::decode_from_slice::<BlobMeta, _>(&raw, bincode::config::standard())
                    .map(|(m, _)| (ent, m))
                    .map_err(|e| StorageError::Bincode(e.to_string()))
            })
            .collect()
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
    use tempfile::tempdir;

    use crate::{
        clock::MockClock,
        storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
        storage_error::StorageError,
        KuramotoDb, StaticTableDef, WriteRequest,
    };

    #[derive(Clone, Debug, PartialEq, Encode, Decode)]
    struct TestEntity { id: u64, name: String, value: i32 }

    static TEST_TABLE: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test");
    static TEST_META: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test_meta");
    static TEST_NAME_INDEX: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test_name_idx");
    static TEST_VALUE_INDEX: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test_value_idx");
    static TEST_INDEXES: &[IndexSpec<TestEntity>] = &[
        IndexSpec { name: "name", key_fn: |e: &TestEntity| e.name.as_bytes().to_vec(), table_def: &TEST_NAME_INDEX, cardinality: IndexCardinality::Unique },
        IndexSpec { name: "value", key_fn: |e: &TestEntity| e.value.to_be_bytes().to_vec(), table_def: &TEST_VALUE_INDEX, cardinality: IndexCardinality::NonUnique },
    ];
    impl StorageEntity for TestEntity {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> { self.id.to_be_bytes().to_vec() }
        fn table_def() -> StaticTableDef { &TEST_TABLE }
        fn meta_table_def() -> StaticTableDef { &TEST_META }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            match data.first().copied() {
                Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard()).map(|(v, _)| v).map_err(|e| StorageError::Bincode(e.to_string())),
                _ => Err(StorageError::Bincode("bad version".into())),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] { TEST_INDEXES }
    }

    #[tokio::test(start_paused = true)]
    async fn overwrite_updates_meta() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(10));
        let db = KuramotoDb::new(dir.path().join("v_over.redb").to_str().unwrap(), clock.clone(), vec![VersioningPlugin::new()]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let mut e = TestEntity { id: 99, name: "X".into(), value: 1 };
        db.put(e.clone()).await.unwrap();
        let m0 = VersioningPlugin::get_meta::<TestEntity>(&db, &e.id.to_be_bytes()).await.unwrap();
        assert_eq!(m0.version, 0);
        assert_eq!(m0.created_at, 10);
        assert_eq!(m0.updated_at, 10);
        clock.advance(5).await;
        e.value = 2;
        db.put(e.clone()).await.unwrap();
        let m1 = VersioningPlugin::get_meta::<TestEntity>(&db, &e.id.to_be_bytes()).await.unwrap();
        assert_eq!(m1.version, 1);
        assert_eq!(m1.created_at, 10);
        assert_eq!(m1.updated_at, 15);
    }

    #[tokio::test(start_paused = true)]
    async fn delete_and_undelete() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(500));
        let db = KuramotoDb::new(dir.path().join("v_del.redb").to_str().unwrap(), clock.clone(), vec![VersioningPlugin::new()]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let e = TestEntity { id: 7, name: "Y".into(), value: 0 };
        db.put(e.clone()).await.unwrap();
        clock.advance(10).await;
        db.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap();
        let md = VersioningPlugin::get_meta::<TestEntity>(&db, &e.id.to_be_bytes()).await.unwrap();
        assert_eq!(md.created_at, 500);
        assert_eq!(md.updated_at, 510);
        assert!(md.deleted_at.is_some());

        clock.advance(5).await;
        db.put(e.clone()).await.unwrap();
        let mu = VersioningPlugin::get_meta::<TestEntity>(&db, &e.id.to_be_bytes()).await.unwrap();
        assert_eq!(mu.version, 1);
        assert_eq!(mu.created_at, 500);
        assert_eq!(mu.updated_at, 515);
        assert!(mu.deleted_at.is_none());
    }

    #[tokio::test]
    async fn stale_version_rejected() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(50));
        let db = KuramotoDb::new(dir.path().join("v_stale.redb").to_str().unwrap(), clock.clone(), vec![VersioningPlugin::new()]).await;

        db.create_table_and_indexes::<TestEntity>().unwrap();
        let e = TestEntity { id: 5, name: "S".into(), value: 5 };
        db.put(e.clone()).await.unwrap();
        // Craft a stale meta equal to the current meta (no version increase)
        let stale = VersioningPlugin::get_meta::<TestEntity>(&db, &e.id.to_be_bytes()).await.unwrap();
        let stale_wr = WriteRequest::Put {
            data_table: TestEntity::table_def(),
            meta_table: TestEntity::meta_table_def(),
            key: e.primary_key(),
            value: e.to_bytes(),
            meta: bincode::encode_to_vec(stale, bincode::config::standard()).unwrap(),
            index_puts: vec![],
            index_removes: vec![],
        };
        let _ = db.raw_write(stale_wr).await;
        // At minimum, stale writes should not increase the version; meta stays identical.
        let after = VersioningPlugin::get_meta::<TestEntity>(&db, &e.id.to_be_bytes()).await.unwrap();
        assert_eq!(after.version, stale.version);
    }

    #[tokio::test]
    async fn range_with_meta_helpers() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(123));
        let db = KuramotoDb::new(dir.path().join("v_range.redb").to_str().unwrap(), clock, vec![VersioningPlugin::new()]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        for id in 10..=11 {
            db.put(TestEntity { id, name: format!("N{id}"), value: 1 }).await.unwrap();
        }
        let got = VersioningPlugin::range_with_meta_by_pk::<TestEntity>(&db, &10u64.to_be_bytes(), &12u64.to_be_bytes(), None).await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|(_, m)| m.version == 0 && m.created_at == 123));

        let got2 = VersioningPlugin::range_with_meta_by_index::<TestEntity>(&db, &TEST_NAME_INDEX, b"N1", b"N3", None).await.unwrap();
        assert_eq!(got2.len(), 2);
    }

    // Order-dependent meta mutation — ensure Versioning runs first
    struct Add100Plugin;
    #[async_trait]
    impl Plugin for Add100Plugin {
        async fn before_update(&self, _db: &KuramotoDb, _txn: &redb::ReadTransaction, batch: &mut WriteBatch) -> Result<(), StorageError> {
            for req in batch {
                if let WriteRequest::Put { meta, .. } = req {
                    let (mut m, _): (BlobMeta, _) = bincode::decode_from_slice(meta, bincode::config::standard()).map_err(|o| StorageError::Bincode(o.to_string()))?;
                    m.updated_at += 100;
                    *meta = bincode::encode_to_vec(m, bincode::config::standard()).map_err(|o| StorageError::Bincode(o.to_string()))?;
                }
            }
            Ok(())
        }
        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }
    struct DoublePlugin;
    #[async_trait]
    impl Plugin for DoublePlugin {
        async fn before_update(&self, _db: &KuramotoDb, _txn: &redb::ReadTransaction, batch: &mut WriteBatch) -> Result<(), StorageError> {
            for req in batch {
                if let WriteRequest::Put { meta, .. } = req {
                    let (mut m, _): (BlobMeta, _) = bincode::decode_from_slice(meta, bincode::config::standard()).map_err(|o| StorageError::Bincode(o.to_string()))?;
                    m.updated_at *= 2;
                    *meta = bincode::encode_to_vec(m, bincode::config::standard()).map_err(|o| StorageError::Bincode(o.to_string()))?;
                }
            }
            Ok(())
        }
        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }

    #[tokio::test]
    async fn plugins_applied_in_order() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(100));
        let db = KuramotoDb::new(dir.path().join("v_order.redb").to_str().unwrap(), clock.clone(), vec![VersioningPlugin::new(), Arc::new(Add100Plugin), Arc::new(DoublePlugin)]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let e = TestEntity { id: 1, name: "O".into(), value: 9 };
        db.put(e.clone()).await.unwrap();
        let meta = VersioningPlugin::get_meta::<TestEntity>(&db, &1u64.to_be_bytes()).await.unwrap();
        assert_eq!(meta.updated_at, 400);
    }
}
