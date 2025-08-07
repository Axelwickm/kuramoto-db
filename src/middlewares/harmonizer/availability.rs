use bincode::{Decode, Encode};

use redb::TableDefinition;

use crate::{
    StaticTableDef,
    middlewares::harmonizer::{child_set::ChildSet, range_cube::RangeCube},
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/// Main storage & meta tables
pub static AVAILABILITIES_TABLE: StaticTableDef = &TableDefinition::new("availabilities");
pub static AVAILABILITIES_META_TABLE: StaticTableDef = &TableDefinition::new("availabilities_meta");

/// Index: by (index_name, range_id) enables quick replica counting
pub static AVAILABILITY_BY_RANGE: StaticTableDef = &TableDefinition::new("availability_by_range");

#[derive(Clone, Debug, Encode, Decode)]
pub struct AvailabilityV0 {
    /* ─ identification ─ */
    pub key: UuidBytes,
    pub peer_id: UuidBytes,

    pub range: RangeCube,
    pub children: ChildSet,

    pub schema_hash: u64, // hash(dataset + struct_version + index_layout)

    /* ─ bookkeeping ─ */
    pub version: u32,
    pub updated_at: u64,
}

pub type Availability = AvailabilityV0;

pub static AVAILABILITY_INDEXES: &[IndexSpec<AvailabilityV0>] = &[
//     IndexSpec::<AvailabilityV0> {
//     name: "by_range",
//     key_fn: |a| {
//         let mut k = a.range.dataset.to_le_bytes().to_vec();
//         k.extend_from_slice(a.peer_id.as_bytes());
//         k
//     },
//     table_def: &AVAILABILITY_BY_RANGE,
//     cardinality: IndexCardinality::NonUnique,
// }
];

impl StorageEntity for AvailabilityV0 {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        self.key.into_vec()
    }

    fn table_def() -> StaticTableDef {
        AVAILABILITIES_TABLE
    }
    fn meta_table_def() -> StaticTableDef {
        AVAILABILITIES_META_TABLE
    }

    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        // with only one version we can ditch the leading‐byte tag
        bincode::decode_from_slice::<Self, _>(data, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|e| StorageError::Bincode(e.to_string()))
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        AVAILABILITY_INDEXES
    }
}
