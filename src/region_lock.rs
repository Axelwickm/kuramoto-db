use bincode::{Decode, Encode};

// --- RegionLock ---
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug, Encode, Decode)]
pub enum RegionLock {
    None = 0,
    GDPR = 1,
    Brazil = 2,
    China = 3, // Extend as needed; never reorder!
}
