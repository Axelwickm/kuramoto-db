use bincode::{Decode, Encode};

use super::region_lock::RegionLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Encode, Decode)]
pub struct BlobMetaV0 {
    pub version: u32,
    pub created_at: u64,
    pub updated_at: u64,
    pub deleted_at: Option<u64>,
    // TODO: not convinced this should be so deeply integrated into database...
    // We should probably break this out into a versioning plugin, and then let the region_lock
    // maybe belong to a secondary metadata? If the logic between this new metadata manager, and
    // the versioning one would be very similar, then we might want some shared logic to it, maybe
    // a third plugin, or common helper functions...
    pub region_lock: RegionLock,
}

pub type BlobMeta = BlobMetaV0;
