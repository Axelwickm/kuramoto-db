use bincode::{Decode, Encode};

use crate::StaticTableDef;

use super::storage_error::StorageError;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IndexCardinality {
    Unique,
    NonUnique, // allows duplicates
}

pub struct IndexSpec<T: StorageEntity> {
    pub name: &'static str,
    pub key_fn: fn(&T) -> Vec<u8>,
    pub table_def: StaticTableDef,
    pub cardinality: IndexCardinality,
}

pub trait StorageEntity: Encode + Decode<()> + Sized + Send + Sync + 'static {
    const STRUCT_VERSION: u8;

    fn primary_key(&self) -> Vec<u8>;

    fn table_def() -> StaticTableDef;
    fn meta_table_def() -> StaticTableDef;

    fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::encode_to_vec(self, bincode::config::standard()).unwrap();
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(Self::STRUCT_VERSION);
        buf.extend(payload);
        buf
    }

    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError>;

    fn indexes() -> &'static [IndexSpec<Self>];
}
