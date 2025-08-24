use async_recursion::async_recursion;
use async_trait::async_trait;
use redb::ReadTransaction;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{
        availability::{Availability, roots_for_peer}, // roots via (peer_id,is_root) index
        availability_queries::{count_replications, resolve_child_avail}, // txn-aware helpers
        harmonizer::PeerContext,
        optimizer::{ActionSet, AvailabilityDraft},
        scorers::Scorer,
    },
    storage_error::StorageError,
};

/// Tunables for the server-side scoring heuristic.
#[derive(Clone, Debug)]
pub struct ServerScorerParams {
    /// Baseline cost per node (positive number). Score subtracts this.
    pub rent: f32,
    /// Desired number of children per parent (soft target).
    pub child_target: usize,
    /// Weight for the child-target shaping term.
    pub child_weight: f32,
    /// Replication target (distinct peers fully containing the candidate).
    pub replication_target: usize,
    /// Nudge strength for replication shortfall when we **don’t** short-circuit.
    pub replication_weight: f32,
}

impl Default for ServerScorerParams {
    fn default() -> Self {
        Self {
            rent: 1.0,
            child_target: 6,
            child_weight: 0.5,
            replication_target: 3,
            replication_weight: 1.0,
        }
    }
}

/// Stateless, async scorer that queries the DB on demand.
/// Snapshot-aware (reads via provided `txn`), no whole-table scans for child counting.
pub struct ServerScorer {
    params: ServerScorerParams,
}

impl ServerScorer {
    pub fn new(params: ServerScorerParams) -> Self {
        Self { params }
    }

    /// Txn-aware replication count: distinct peers that **fully contain** the candidate range.
    /// NOTE: remains global & scan-based (as in availability_queries), since replication is
    /// a cross-peer property.
    async fn replication_count(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        cand: &AvailabilityDraft,
    ) -> Result<usize, StorageError> {
        count_replications(db, Some(txn), &cand.range, |_a| true, None).await
    }

    /// Count **local** children a parent would adopt at level-1 (i.e., `want_child_level`),
    /// using only your local roots and a txn-aware child resolver. No full-table scans.
    ///
    /// Rules:
    /// - We traverse only subtrees reachable from `(ctx.peer_id)` roots.
    /// - We **prune** traversal when a node does not intersect `cand.range`.
    /// - We **stop descending** once we reach at/below `want_child_level`.
    /// - We count a node iff:
    ///     * `node.level == want_child_level`
    ///     * `node.complete`
    ///     * `node.peer_id == ctx.peer_id`
    ///     * `cand.range.contains(node.range)`
    /// - Overlay deletions/additions can be folded in (TODOs noted inline).
    async fn child_count_local(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
        _overlay: &ActionSet, // TODO: subtract overlay deletions, add overlay additions
    ) -> Result<usize, StorageError> {
        if cand.level == 0 {
            // Leaves adopt exactly the written entity.
            return Ok(1);
        }

        let want_child_level = cand.level.saturating_sub(1) as u16;

        // Get **local** roots via the index (no scans).
        let roots: Vec<Availability> = roots_for_peer(db, Some(txn), &ctx.peer_id).await?;

        // Dedup to avoid double-count if multiple roots reach the same node.
        use std::collections::HashSet;
        let mut seen: HashSet<crate::uuid_bytes::UuidBytes> = HashSet::new();

        #[async_recursion]
        async fn walk(
            db: &KuramotoDb,
            txn: &ReadTransaction,
            ctx: &PeerContext,
            target: &crate::plugins::harmonizer::range_cube::RangeCube,
            want_level: u16,
            acc_seen: &mut HashSet<crate::uuid_bytes::UuidBytes>,
            node: &Availability,
            // _overlay: &ActionSet, // when you surface overlay APIs, add here
        ) -> Result<usize, StorageError> {
            // Prune if no intersection
            if node.range.intersect(target).is_none() {
                return Ok(0);
            }

            // If we're at/below want_level, either count (==) or stop (<).
            if node.level <= want_level {
                if node.level == want_level
                    && node.complete
                    && node.peer_id == ctx.peer_id
                    && target.contains(&node.range)
                {
                    // TODO(overlay): if overlay deletes this node, skip it.
                    if acc_seen.insert(node.key) {
                        return Ok(1);
                    }
                }
                return Ok(0);
            }

            // Otherwise, descend to children that resolve.
            let mut sum = 0usize;
            for cid in &node.children.children {
                // TODO(overlay): if overlay deletes this child id, skip resolution.
                if let Some(child) = resolve_child_avail(db, Some(txn), cid).await? {
                    sum += walk(db, txn, ctx, target, want_level, acc_seen, &child).await?;
                } else {
                    // Broken link → stop this branch (don't count node here, only want exact level)
                }
            }
            Ok(sum)
        }

        let mut total = 0usize;
        for r in &roots {
            total += walk(db, txn, ctx, &cand.range, want_child_level, &mut seen, r).await?;
        }

        // TODO(overlay):
        //  - Add any `AvailabilityDraft` inserts that:
        //      * level == want_child_level
        //      * peer_id == ctx.peer_id (or implicit)
        //      * cand.range.contains(draft.range)
        //  - Make sure not to double-count something that already exists.

        Ok(total)
    }

    /// Single “child count” shaping function: peak at `child_target`,
    /// linear drop-off as you deviate.
    fn child_count_score(&self, child_count: usize) -> f32 {
        let t = self.params.child_target as i32;
        let dev = (child_count as i32 - t).abs() as f32;
        self.params.child_weight * ((t as f32) - dev)
    }
}

#[async_trait]
impl Scorer for ServerScorer {
    async fn score(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
        overlay: &ActionSet,
    ) -> f32 {
        // 1) Safety valve: under-replicated? Force positive score and return.
        let replicas = match self.replication_count(db, txn, cand).await {
            Ok(n) => n as i32,
            Err(_) => 0, // on error, be conservative
        };
        let target = self.params.replication_target as i32;
        if replicas < target {
            // Always positive; bias proportional to shortfall.
            return 1.0 + self.params.replication_weight * ((target - replicas) as f32);
        }

        // 2) Otherwise: rent + child-count shaping (no overlap term).
        let mut s = -self.params.rent;

        let child_count = match self.child_count_local(db, txn, ctx, cand, overlay).await {
            Ok(c) => c,
            Err(_) => 0, // on error, fall back to 0 local children
        };
        s += self.child_count_score(child_count);

        s
    }
}
