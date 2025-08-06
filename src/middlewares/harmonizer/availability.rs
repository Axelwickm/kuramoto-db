use bincode::{Decode, Encode};
use uuid::Uuid;

use redb::TableDefinition;

use crate::{
    StaticTableDef,
    middlewares::harmonizer::{child_set::ChildSet, range_cube::RangeCube},
    storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
    storage_error::StorageError,
    tables::stable_hash,
};

/// Main storage & meta tables
pub static AVAILABILITIES_TABLE: StaticTableDef = &TableDefinition::new("availabilities");
pub static AVAILABILITIES_META_TABLE: StaticTableDef = &TableDefinition::new("availabilities_meta");

/// Index: by (index_name, range_id) enables quick replica counting
pub static AVAILABILITY_BY_RANGE: StaticTableDef = &TableDefinition::new("availability_by_range");

pub static AVAILABILITY_INDEXES: &[IndexSpec<Availability>] = &[IndexSpec::<Availability> {
    name: "by_range",
    key_fn: |a| {
        let mut k = a.index_name.as_bytes().to_vec();
        k.extend_from_slice(a.range_id.as_bytes());
        k
    },
    table_def: &AVAILABILITY_BY_RANGE,
    cardinality: IndexCardinality::NonUnique,
}];

#[derive(Clone, Debug, Encode, Decode)]
pub struct AvailabilityV0 {
    /* ─ identification ─ */
    pub peer_id: String,

    pub range: RangeCube,
    pub children: ChildSet,

    pub schema_hash: u64, // hash(dataset + struct_version + index_layout)

    /* ─ bookkeeping ─ */
    pub version: u32,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Encode, Decode)]
enum Availability {
    V0(AvailabilityV0),
}

impl StorageEntity for Availability {
    const STRUCT_VERSION: u8 = 0; // bump if needed

    fn primary_key(&self) -> Vec<u8> {
        match self {
            Availability::V0(v0) => v0.pk(),
        }
    }

    fn table_def() -> StaticTableDef {
        AVAILABILITIES_TABLE
    }
    fn meta_table_def() -> StaticTableDef {
        AVAILABILITIES_META_TABLE
    }

    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        if data.is_empty() {
            return Err(StorageError::Bincode("empty".into()));
        }
        match data[0] {
            0 => {
                // old row
                let (old, _) = bincode::decode_from_slice::<AvailabilityV0, _>(
                    &data[1..],
                    bincode::config::standard(),
                )
                .map_err(|e| StorageError::Bincode(e.to_string()))?;
                Ok(Availability::V1(old.upgrade_to_v1()))
            }
            n => Err(StorageError::Bincode(format!("unknown version {n}"))),
        }
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        AVAILABILITY_INDEXES
    }
}
