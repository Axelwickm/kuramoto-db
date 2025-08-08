use bincode::{Decode, Encode};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Encode, Decode)]
#[repr(transparent)]
pub struct UuidBytes([u8; 16]);

impl UuidBytes {
    pub fn new() -> Self {
        UuidBytes(*Uuid::new_v4().as_bytes())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        UuidBytes(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl From<Uuid> for UuidBytes {
    fn from(u: Uuid) -> Self {
        UuidBytes(*u.as_bytes())
    }
}
impl From<UuidBytes> for Uuid {
    fn from(b: UuidBytes) -> Self {
        Uuid::from_bytes(b.0)
    }
}

/// Optional: for APIs expecting &[u8]
impl AsRef<[u8]> for UuidBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
