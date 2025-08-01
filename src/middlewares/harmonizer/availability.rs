use bincode::{Decode, Encode};
use uuid::Uuid;

use redb::TableDefinition;

use crate::{
    StaticTableDef,
    storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
    storage_error::StorageError,
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

/// A *single* Availability row – either meta (children) or leaf (entity IDs)
#[derive(Clone, Debug, Encode, Decode)]
pub struct Availability {
    /* ───── Identification ───── */
    pub range_id: Uuid,     // unique ID for this range claim
    pub index_name: String, // e.g. "user_id", "time_idx"
    pub peer_id: String,    // who claims it

    /* ───── Range covered ───── */
    pub min: Vec<u8>, // inclusive start (raw index bytes)
    pub max: Vec<u8>, // inclusive end

    /* ───── Payload ───── */
    pub rlblt: Vec<u8>, // fixed-size RIBLT summary
    pub is_meta: bool,  // true = rlblt encodes child range_ids

    /* ───── Book-keeping ───── */
    pub version: u32,    // bump every mutation
    pub updated_at: u64, // logical clock / unix millis
}

impl Availability {
    /// Helper: primary-key bytes = range_id (16) + peer_id
    fn pk(&self) -> Vec<u8> {
        let mut out = self.range_id.as_bytes().to_vec();
        out.extend_from_slice(self.peer_id.as_bytes());
        out
    }
}

impl StorageEntity for Availability {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        self.pk()
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
            1 => bincode::decode_from_slice(&data[1..], bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| StorageError::Bincode(e.to_string())),
            n => Err(StorageError::Bincode(format!("unknown version {n}"))),
        }
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        AVAILABILITY_INDEXES
    }
}
