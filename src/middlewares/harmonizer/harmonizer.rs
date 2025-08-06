//! Background engine that
//!   * (later) optimises local Availability tree
//!   * (later) keeps K-replica via gossip
//!   * right now: is a no-op Middleware so it can be wired in.

use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    database::{KuramotoDb, WriteRequest},
    middlewares::Middleware,
    storage_entity::StorageEntity,
    storage_error::StorageError,
};

/* ───── tuning constants ───── */
const REPLICA_K: usize = 3;

/* ───── struct ───── */
pub struct Harmonizer {
    db: Arc<KuramotoDb>,
    /// quick in-mem cache: (range_id, peer_id) → score
    local_scores: RwLock<std::collections::HashMap<(Uuid, String), f32>>,
}

impl Harmonizer {
    pub fn new(db: Arc<KuramotoDb>) -> Self {
        Self {
            db,
            local_scores: RwLock::new(Default::default()),
        }
    }

    /* ----- scaffolding for future work ----- */
    async fn score_local(&self, avail: &Availability) -> f32 {
        // placeholder heuristic
        let replicas = self.count_replicas(avail).await as f32;
        1.0 - (replicas - REPLICA_K as f32).max(0.0)
    }

    async fn count_replicas(&self, avail: &Availability) -> usize {
        let key = {
            let mut k = avail.index_name.as_bytes().to_vec();
            k.extend_from_slice(avail.range_id.as_bytes());
            k
        };
        match self
            .db
            .get_by_index::<Availability>(AVAILABILITY_BY_RANGE, &key)
            .await
        {
            Ok(Some(_)) => {
                // cheap count via primary-key prefix scan
                let mut tx = self.db.begin_read_txn()?;
                let tbl = tx.open_table(Availability::table_def().clone())?;
                let mut it = tbl.range(key.as_slice()..)?;
                let mut peers = std::collections::HashSet::new();
                while let Some(Ok((_, v))) = it.next() {
                    let (row, _) = bincode::decode_from_slice::<Availability, _>(
                        v.value(),
                        bincode::config::standard(),
                    )
                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
                    peers.insert(row.peer_id);
                }
                Ok(peers.len())
            }
            _ => Ok(1),
        }
        .unwrap_or(1)
    }
}

/* ───── implement Middleware ───── */
impl Middleware for Harmonizer {
    /// For now we’re a *no-op*; later we’ll inject / mutate Availability rows.
    fn before_write(&self, _req: &mut WriteRequest) -> Result<(), StorageError> {
        // placeholder: do nothing, succeed
        Ok(())
    }
}
