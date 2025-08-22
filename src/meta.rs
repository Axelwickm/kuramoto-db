use bincode::{Decode, Encode};

use super::region_lock::RegionLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Encode, Decode)]
pub struct BlobMetaV0 {
    pub version: u32,
    pub created_at: u64,
    pub updated_at: u64,
    pub deleted_at: Option<u64>,
    pub region_lock: RegionLock,
}

pub type BlobMeta = BlobMetaV0;
