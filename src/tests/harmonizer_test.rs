use std::sync::{Arc, Mutex};

use tempfile::tempdir;

use crate::{
    StaticTableDef,
    clock::Clock,
    database::{KuramotoDb, WriteRequest},
    middlewares::{Middleware, harmonizer::harmonizer::Harmonizer},
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
};

use bincode::{Decode, Encode};
use redb::TableDefinition;

#[tokio::test]
async fn middleware_runs_in_order() {
    let harmonizer = Harmonizer();
}
