//! High-level background engine that
//!   * optimises local Availability tree (split/merge/prune)
//!   * maintains K-replica invariant via peer gossip
//!   * services incoming sync / query requests
//!
//! The heavy lifting (RIBLT diff, network IO) is *not* included here –
//! this is only the skeleton that plugs into `KuramotoDb`.

use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    availability::Availability, database::KuramotoDb, storage_entity::StorageEntity,
    storage_error::StorageError,
};

use super::availability::AVAILABILITY_BY_RANGE;

/// Tuning constants – pull these from config later
const TARGET_LEAF_SIZE: usize = 10; // ≈ N entities per leaf Availability
const REPLICA_K: usize = 3; // desired replication factor
const SCORE_EVICT: f32 = -0.1; // below → schedule eviction
const SCORE_ADOPT: f32 = 0.1; // above → consider adopting

pub struct Harmonizer {
    db: Arc<KuramotoDb>,
    /// quick in-mem lookup: (range_id, peer_id) → score
    local_scores: RwLock<std::collections::HashMap<(Uuid, String), f32>>,
}

impl Harmonizer {
    pub fn new(db: Arc<KuramotoDb>) -> Self {
        Self {
            db,
            local_scores: RwLock::new(Default::default()),
        }
    }

    /* ───── Public API stubs ───── */

    /// Called periodically by a tokio task.
    pub async fn optimise_local_tree(&self) -> Result<(), StorageError> {
        // 1. Walk local leaf Availabilities for each index.
        // 2. Merge adjacent leaves if combined score ↑ and total ≤ TARGET_LEAF_SIZE.
        // 3. Split oversized leaves stochastically.
        // 4. Update / insert meta nodes upward.
        Ok(())
    }

    /// Handle a peer sync request (root meta comparison).
    pub async fn handle_sync_request(
        &self,
        peer_root: Availability,
    ) -> Result<Vec<Availability>, StorageError> {
        // Compare peer_root.rlblt with our root for that (index_name, range)
        // Return children that differ.
        Ok(vec![])
    }

    /// Evaluate (or recompute) the fuzzy score for an Availability we own.
    async fn score_local(&self, avail: &Availability) -> f32 {
        // Example: simple heuristic
        let novelty = 1.0; // TODO: compute via interval tree
        let replicas = self.count_replicas(avail).await as f32;
        let storage_pressure = 0.0; // TODO
        novelty - (replicas - REPLICA_K as f32).max(0.0) - storage_pressure
    }

    /// Count distinct peer_ids for the same (range_id) locally cached from gossip
    async fn count_replicas(&self, avail: &Availability) -> usize {
        let range_key = {
            let mut k = avail.index_name.as_bytes().to_vec();
            k.extend_from_slice(avail.range_id.as_bytes());
            k
        };
        match self
            .db
            .get_by_index::<Availability>(AVAILABILITY_BY_RANGE, &range_key)
            .await
        {
            Ok(Some(a)) => {
                // a is *one* row; scan siblings by primary-key prefix
                // Simplified: we assume small replica count
                let mut tx = self.db.begin_read_txn()?;
                let tbl = tx.open_table(Availability::table_def().clone())?;
                let mut it = tbl.range(range_key.as_slice()..)?;
                let mut set = std::collections::HashSet::new();
                while let Some(Ok((_, v))) = it.next() {
                    let (row, _) = bincode::decode_from_slice::<Availability, _>(
                        v.value(),
                        bincode::config::standard(),
                    )
                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
                    set.insert(row.peer_id);
                }
                Ok(set.len())
            }
            _ => Ok(1),
        }
        .unwrap_or(1)
    }
}
