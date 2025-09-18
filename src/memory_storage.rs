// src/tree_store/memory_backend.rs
use std::io::{Error, ErrorKind, Result};
use std::sync::Mutex;

use redb::StorageBackend;

/// A growable, zero-filled, in-memory StorageBackend.
/// Sync is a no-op; useful for tests and ephemeral DBs.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    buf: Mutex<Vec<u8>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            buf: Mutex::new(Vec::new()),
        }
    }
}

impl StorageBackend for MemoryBackend {
    fn len(&self) -> Result<u64> {
        let b = self.buf.lock().unwrap();
        Ok(b.len() as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(len);
        let b = self.buf.lock().unwrap();

        let off = usize::try_from(offset)
            .map_err(|_| Error::new(ErrorKind::Other, "offset too large"))?;
        let end = off
            .checked_add(len)
            .ok_or_else(|| Error::new(ErrorKind::Other, "length overflow"))?;

        if end > b.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "read past end"));
        }
        out.extend_from_slice(&b[off..end]);
        Ok(out)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        let mut b = self.buf.lock().unwrap();
        let new_len =
            usize::try_from(len).map_err(|_| Error::new(ErrorKind::Other, "len too large"))?;
        if new_len >= b.len() {
            b.resize(new_len, 0);
        } else {
            b.truncate(new_len);
        }
        Ok(())
    }

    fn sync_data(&self, _eventual: bool) -> Result<()> {
        // In-memory: nothing to do
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut b = self.buf.lock().unwrap();

        let off = usize::try_from(offset)
            .map_err(|_| Error::new(ErrorKind::Other, "offset too large"))?;
        let end = off
            .checked_add(data.len())
            .ok_or_else(|| Error::new(ErrorKind::Other, "length overflow"))?;

        // Zero-fill any gap and extend as needed
        if b.len() < off {
            b.resize(off, 0);
        }
        if b.len() < end {
            b.resize(end, 0);
        }
        b[off..end].copy_from_slice(data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn write_and_read_round_trip() {
        let backend = MemoryBackend::new();
        backend.write(0, &[1, 2, 3, 4]).unwrap();

        assert_eq!(backend.len().unwrap(), 4);

        let data = backend.read(0, 4).unwrap();
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn resizes_and_truncates() {
        let backend = MemoryBackend::new();
        backend.write(2, &[9, 9]).unwrap();

        // Zero-filled gap before write
        assert_eq!(backend.read(0, 4).unwrap(), vec![0, 0, 9, 9]);

        backend.set_len(2).unwrap();
        assert_eq!(backend.len().unwrap(), 2);

        let truncated = backend.read(0, 2).unwrap();
        assert_eq!(truncated, vec![0, 0]);
        assert!(backend.read(0, 3).is_err());
    }

    #[test]
    fn read_past_end_errors() {
        let backend = MemoryBackend::new();
        backend.write(0, &[1, 2]).unwrap();

        let err = backend.read(0, 3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }
}
