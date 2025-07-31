use bincode::{Decode, Encode};
use redb::TableHandle;

use crate::StaticTableDef;

pub fn stable_hash(s: &str) -> u64 {
    // 64-bit FNV-1a params
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in s.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[derive(Copy, Clone, Debug, Encode, Decode)]
pub struct TableHash {
    pub hash: u64,
}

impl TableHash {
    pub fn new(table: StaticTableDef) -> Self {
        let hash = stable_hash(table.name());
        Self { hash }
    }
    pub fn hash(&self) -> u64 {
        self.hash
    }
}

impl From<StaticTableDef> for TableHash {
    fn from(table: StaticTableDef) -> Self {
        TableHash::new(table)
    }
}
