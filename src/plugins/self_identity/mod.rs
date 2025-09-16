use std::sync::Arc;

use async_trait::async_trait;
use bincode::{Encode, Decode};
use redb::ReadTransaction;
use redb::TableDefinition;

use crate::{
    KuramotoDb, WriteBatch, WriteOrigin, StaticTableDef, storage_error::StorageError,
    storage_entity::{AuxTableSpec, IndexSpec, StorageEntity},
};
use crate::plugins::{versioning::VERSIONING_AUX_ROLE, Plugin};
use crate::uuid_bytes::UuidBytes;

// Simple single-row table holding a persistent local peer id
#[derive(Clone, Debug, Encode, Decode)]
struct SelfIdRow {
    key: u8,            // always 0
    peer_id: [u8; 16],  // stable id
}

static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("self_identity");
static META: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("self_identity_meta");
static AUX_TABLES: &[AuxTableSpec] = &[AuxTableSpec { role: VERSIONING_AUX_ROLE, table: &META }];
static INDEXES: &[IndexSpec<SelfIdRow>] = &[];

impl StorageEntity for SelfIdRow {
    const STRUCT_VERSION: u8 = 0;
    fn primary_key(&self) -> Vec<u8> { vec![self.key] }
    fn table_def() -> StaticTableDef { &TBL }
    fn aux_tables() -> &'static [AuxTableSpec] { AUX_TABLES }
    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        match data.first().copied() {
            Some(0) => bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| StorageError::Bincode(e.to_string())),
            _ => Err(StorageError::Bincode("bad version".into())),
        }
    }
    fn indexes() -> &'static [IndexSpec<Self>] { INDEXES }
}

#[derive(Default)]
pub struct SelfIdentity;

impl SelfIdentity {
    pub fn new() -> Arc<Self> { Arc::new(SelfIdentity) }

    pub async fn get_peer_id(db: &KuramotoDb) -> Result<UuidBytes, StorageError> {
        // Try read
        if let Ok(row) = db.get_data::<SelfIdRow>(&[0u8]).await {
            return Ok(UuidBytes::from_bytes(row.peer_id));
        }
        // Create if missing
        let id = UuidBytes::new();
        let mut arr = [0u8; 16];
        arr.copy_from_slice(id.as_bytes());
        db.put_with_origin::<SelfIdRow>(SelfIdRow { key: 0, peer_id: arr }, WriteOrigin::Plugin(crate::plugins::fnv1a_16("self_identity"))).await?;
        Ok(id)
    }
}

#[async_trait]
impl Plugin for SelfIdentity {
    fn attach_db(&self, db: Arc<KuramotoDb>) {
        // Ensure table exists and row is present
        let db2 = db.clone();
        let plugin_id = crate::plugins::fnv1a_16("self_identity");
        let _ = db.create_table_and_indexes::<SelfIdRow>();
        tokio::spawn(async move {
            let _ = SelfIdentity::get_peer_id(&db2).await;
            let _ = plugin_id; // silence
        });
    }

    async fn before_update(&self, _db: &KuramotoDb, _txn: &ReadTransaction, _batch: &mut WriteBatch) -> Result<(), StorageError> { Ok(()) }
}

/* tests */
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use tempfile::tempdir;

    #[tokio::test]
    async fn returns_stable_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("selfid.redb");
        let clock = Arc::new(MockClock::new(0));
        // Build DB without registering the plugin to avoid concurrent spawn in attach_db.
        let db = KuramotoDb::new(path.to_str().unwrap(), clock.clone(), vec![]).await;
        let a = SelfIdentity::get_peer_id(&db).await.unwrap();
        let b = SelfIdentity::get_peer_id(&db).await.unwrap();
        assert_eq!(a, b, "self id should be stable within process");
    }
}
