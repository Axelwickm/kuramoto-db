//pub mod harmonizer;

use crate::{WriteRequest, storage_error::StorageError};

pub trait Middleware: Send + Sync {
    /// Inspect / mutate / veto a write
    fn before_write(&self, req: &mut WriteRequest) -> Result<(), StorageError>;
}
