#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Bincode(String),
    NotFound,

    PutButNoVersionIncrease,
    DuplicateIndexKey {
        // same index key already mapped elsewhere
        index_name: &'static str,
    },

    Other(String),
}

impl Clone for StorageError {
    fn clone(&self) -> Self {
        match self {
            StorageError::Io(e) => StorageError::Io(std::io::Error::new(e.kind(), e.to_string())),
            StorageError::NotFound => StorageError::NotFound,
            StorageError::Bincode(s) => StorageError::Bincode(s.clone()),
            StorageError::DuplicateIndexKey { index_name } => {
                StorageError::DuplicateIndexKey { index_name }
            }
            StorageError::PutButNoVersionIncrease => StorageError::PutButNoVersionIncrease,
            StorageError::Other(s) => StorageError::Other(s.clone()),
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "IO error: {}", e),
            StorageError::Bincode(e) => write!(f, "Serialization error: {}", e),
            StorageError::NotFound => write!(f, "Entity not found"),
            StorageError::PutButNoVersionIncrease => write!(f, "Put without version increase"),
            StorageError::DuplicateIndexKey { index_name } => {
                write!(f, "Duplicate index key in: {index_name}")
            }
            StorageError::Other(e) => write!(f, "Other: {}", e),
        }
    }
}
impl std::error::Error for StorageError {}
impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}
