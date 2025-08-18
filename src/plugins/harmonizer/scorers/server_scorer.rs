use std::collections::HashSet;

use crate::plugins::harmonizer::availability::Availability;
use crate::plugins::harmonizer::optimizer::AvailabilityDraft;
use crate::plugins::harmonizer::optimizer::PeerContext;
use crate::plugins::harmonizer::scorers::Scorer;
use crate::uuid_bytes::UuidBytes;
use crate::{KuramotoDb, storage_error::StorageError};

// ====== Params ======
#[derive(Clone, Debug)]
pub struct ServerScorerParams {
    /// Baseline cost per node (positive number). Score subtracts this.
    pub rent: f32,
    /// Desired number of children per parent (soft target).
    pub child_target: usize,
    /// Weight for the child-target shaping term.
    pub child_weight: f32,
    /// Weight per overlapping same-level neighbor (simple anti-overlap).
    pub overlap_weight: f32,
    // To make sure we don't delete data
    pub replication_target: usize, // e.g. 3
    pub replication_weight: f32,   // how strong the nudge should be
}

impl Default for ServerScorerParams {
    fn default() -> Self {
        Self {
            rent: 1.0,
            child_target: 6,
            child_weight: 0.5,
            overlap_weight: 0.25,
            replication_target: 3,
            replication_weight: 1.0,
        }
    }
}

// ====== Scorer implementation (snapshot-based, local only) ======
pub struct ServerScorer {
    params: ServerScorerParams,
    self_peer: UuidBytes,
    /// Local (and any gossiped) availabilities snapshotted at construction time.
    view: Vec<Availability>,
}

impl ServerScorer {
    /// Build a scorer from a DB snapshot (no async work inside score()).
    pub async fn from_db_snapshot(
        db: &KuramotoDb,
        self_peer: UuidBytes,
        params: ServerScorerParams,
    ) -> Result<Self, StorageError> {
        // MVP: scan everything; later we can pre-index by level.
        let view: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;
        Ok(Self {
            params,
            self_peer,
            view,
        })
    }

    /// Estimate how many immediate children this parent would adopt if inserted.
    /// Heuristic: count **local** complete nodes at level = cand.level - 1
    /// whose range is fully contained by the candidate.
    fn estimate_children_local(&self, cand: &AvailabilityDraft) -> usize {
        if cand.level == 0 {
            // leaf → one symbol (the new entity) by construction
            return 1;
        }
        let want_child_level = cand.level.saturating_sub(1);
        self.view
            .iter()
            .filter(|a| a.complete)
            .filter(|a| a.peer_id == self.self_peer) // local-only for now
            .filter(|a| a.level == want_child_level)
            .filter(|a| cand.range.contains(&a.range))
            .count()
    }

    /// Count same-level overlaps with local nodes (cheap anti-overlap pressure).
    fn overlap_count_same_level(&self, cand: &AvailabilityDraft) -> usize {
        self.view
            .iter()
            .filter(|a| a.level == cand.level)
            .filter(|a| a.range.overlaps(&cand.range))
            .count()
    }

    fn replication_count(&self, cand: &AvailabilityDraft) -> usize {
        let mut peers = HashSet::new();
        for a in &self.view {
            if !a.complete {
                continue;
            }
            if a.range.contains(&cand.range) {
                peers.insert(a.peer_id);
            }
        }
        peers.len()
    }

    /// Convert child deviation into a bounded utility: max at target; negative if far.
    fn child_shape_score(&self, child_count: usize) -> f32 {
        let t = self.params.child_target as i32;
        let dev = (child_count as i32 - t).abs() as f32;
        // Linear “peak” shaping around the target: +W*t at dev=0, then declining.
        self.params.child_weight * ((t as f32) - dev)
    }
}

impl Scorer for ServerScorer {
    fn score(&self, _ctx: &PeerContext, cand: &AvailabilityDraft) -> f32 {
        // 1) rent
        let mut s = -self.params.rent;

        // 2) branching / child target
        let est_children = self.estimate_children_local(cand);
        s += self.child_shape_score(est_children);

        // 3) anti-overlap (same level)
        let overlaps = self.overlap_count_same_level(cand) as f32;
        s -= self.params.overlap_weight * overlaps;

        // 4) replication nudge (soft): + if replicas < K, 0 at K, - if > K
        let replicas = self.replication_count(cand) as i32;
        let k = self.params.replication_target as i32;
        s += self.params.replication_weight * ((k - replicas) as f32);

        // Coverage safety: leaves are required; never rate them negative.
        if cand.level == 0 && s < 0.0 {
            s = 0.0;
        }
        s
    }
}
