use bincode::{Decode, Encode};
use redb::TableDefinition;

use crate::{
    StaticTableDef,
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
    tables::TableHash,
};

pub static AVAILABILITIES_TABLE: StaticTableDef = &TableDefinition::new("availabilities");

pub static AVAILABILITIES_META_TABLE: StaticTableDef = &TableDefinition::new("availabilities_meta");

pub static AVAILABILITY_INDEX_BY_ENTITY: StaticTableDef =
    &TableDefinition::new("availabilities_by_entity");

pub static AVAILABILITES_INDEXES: &[IndexSpec<Availability>] = &[IndexSpec::<Availability> {
    name: "by_entity",
    key_fn: |a| a.entity_id.clone(),
    table_def: &AVAILABILITY_INDEX_BY_ENTITY,
}];

#[derive(Clone, Debug, Encode, Decode)]
pub struct Availability {
    pub table_hash: TableHash,
    pub entity_id: Vec<u8>,
    pub peer_id: String,
    pub latency_ms: u32,
    pub last_seen: u64,
}

impl StorageEntity for Availability {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.entity_id.len() + self.peer_id.len());
        out.extend_from_slice(&self.table_hash.hash.to_be_bytes()); // 8 bytes
        out.extend_from_slice(&self.entity_id); // N bytes
        out.extend_from_slice(self.peer_id.as_bytes()); // M bytes
        out
    }

    fn table_def() -> StaticTableDef {
        AVAILABILITIES_TABLE
    }

    fn meta_table_def() -> StaticTableDef {
        AVAILABILITIES_META_TABLE
    }

    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        if data.is_empty() {
            return Err(StorageError::Bincode("empty input".into()));
        }
        match data[0] {
            0 => bincode::decode_from_slice(&data[1..], bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| StorageError::Bincode(e.to_string())),
            n => Err(StorageError::Bincode(format!("unknown version {n}"))),
        }
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        &AVAILABILITES_INDEXES
    }
}
