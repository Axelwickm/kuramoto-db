// server_scorer.rs
// Stateless scorer that consults the DB (snapshot-aware) and respects an overlay for deletes.
// The score favors reaching a soft child-target per parent while charging a per-node rent,
// and it short-circuits to strongly encourage under-replicated ranges.

use async_trait::async_trait;
use redb::ReadTransaction;
use std::collections::HashSet;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{
        availability::Availability,
        availability_queries::{count_replications, range_cover},
        harmonizer::PeerContext,
        optimizer::{Action, ActionSet, AvailabilityDraft},
        scorers::Scorer,
    },
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
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

    /// Level-free, range-driven local child count under this peer, overlay-aware for deletes.
    ///
    /// Counts how many **local, complete** availabilities under this peer’s roots are
    /// **contained by** `cand.range`. We avoid global scans by:
    /// - fetching only this peer’s roots (`roots_for_peer`), and
    /// - descending from those roots using `range_cover`, which only walks branches that
    ///   intersect the target range and returns a frontier without over-traversal.
    ///
    /// Notes:
    /// - No dependency on `level`; the definition is purely geometric (containment).
    /// - Dedups by availability key since multiple roots may reach the same node.
    /// - Overlay handling:
    ///     • If an availability is marked `Delete(id)` in overlay, we skip counting it.
    ///     • We do **not** count overlay `Insert` phantoms here (they are typically parents).
    pub(crate) async fn child_count_local(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
        overlay: &ActionSet,
    ) -> Result<usize, StorageError> {
        use crate::plugins::harmonizer::availability::roots_for_peer;

        // 1) Fetch this peer's roots in the same snapshot.
        let roots: Vec<Availability> = roots_for_peer(db, Some(txn), &ctx.peer_id).await?;
        if roots.is_empty() {
            return Ok(0);
        }
        let root_ids: Vec<UuidBytes> = roots.iter().map(|r| r.key).collect();

        // 2) Compute the frontier that touches cand.range by descending only intersecting branches.
        let (frontier, no_overlap) =
            range_cover(db, Some(txn), &cand.range, &root_ids, None).await?;
        if no_overlap {
            return Ok(0);
        }

        // Precompute deletions in overlay for quick membership checks.
        let mut deleted: HashSet<UuidBytes> = HashSet::new();
        for a in overlay {
            if let Action::Delete(id) = a {
                deleted.insert(*id);
            }
        }

        // 3) Count local, complete nodes whose ranges are contained by the candidate’s range,
        //    skipping any that the overlay deletes.
        let mut seen = HashSet::<UuidBytes>::new();
        let mut count = 0usize;

        for a in frontier {
            if a.peer_id == ctx.peer_id && a.complete && cand.range.contains(&a.range) {
                if deleted.contains(&a.key) {
                    continue;
                }
                if seen.insert(a.key) {
                    count += 1;
                }
            }
        }

        Ok(count)
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

/*──────────────────────────── tests ─────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        KuramotoDb,
        clock::MockClock,
        plugins::harmonizer::{
            availability::{Availability, roots_for_peer},
            child_set::ChildSet,
            harmonizer::PeerContext,
            optimizer::{Action, ActionSet, AvailabilityDraft, BasicOptimizer, Caps, Optimizer},
            range_cube::RangeCube,
            scorers::Scorer,
        },
        tables::TableHash,
        uuid_bytes::UuidBytes,
    };
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    /* ---------- Helpers ---------- */

    fn cube(dim: TableHash, min: &[u8], max_excl: &[u8]) -> RangeCube {
        RangeCube {
            dims: smallvec![dim],
            mins: smallvec![min.to_vec()],
            maxs: smallvec![max_excl.to_vec()],
        }
    }

    async fn fresh_db() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("server_scorer.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db
    }

    /// Make N contiguous leaf availabilities [base+i, base+i+1) on one axis.
    /// Returns (root_id, leaf_ids).
    async fn build_linear_leaves_with_root(
        db: &KuramotoDb,
        peer: UuidBytes,
        dim: TableHash,
        n: u8,
        base: u8,
    ) -> (UuidBytes, Vec<UuidBytes>) {
        let mut leaf_ids = Vec::with_capacity(n as usize);
        for i in 0..n {
            let id = UuidBytes::new();
            let lo = vec![base + i];
            let mut hi = lo.clone();
            hi.push(1); // half-open bump
            let leaf = Availability {
                key: id,
                peer_id: peer,
                parent: Some(UuidBytes::new()), // arbitrary; roots index doesn't use this
                range: cube(dim, &lo, &hi),
                level: 0,
                children: ChildSet {
                    parent: id,
                    children: vec![], // leaf ⇒ 0 children for range_cover frontier
                },
                schema_hash: 0,
                version: 0,
                updated_at: 0,
                complete: true,
            };
            db.put(leaf).await.unwrap();
            leaf_ids.push(id);
        }

        // Single root spanning all leaves and pointing to them
        let rid = UuidBytes::new();
        let root = Availability {
            key: rid,
            peer_id: peer,
            parent: None,                           // root
            range: cube(dim, &[base], &[base + n]), // covers all
            level: 3,
            children: ChildSet {
                parent: rid,
                children: leaf_ids.clone(),
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root).await.unwrap();

        (rid, leaf_ids)
    }

    fn params(child_target: usize) -> ServerScorerParams {
        ServerScorerParams {
            rent: 0.25,
            child_target,
            child_weight: 1.0,
            replication_target: 0, // disable safety valve for these tests
            replication_weight: 0.0,
        }
    }

    fn draft(level: u16, r: &RangeCube) -> AvailabilityDraft {
        AvailabilityDraft {
            level,
            range: r.clone(),
            complete: true,
        }
    }

    /* ---------- Unit tests for child_count_local ---------- */

    #[tokio::test]
    async fn child_count_local_counts_only_local_complete_contained() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 11 };
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };
        // Build 6 local leaves [10..16)
        let (_root, _leaves) = build_linear_leaves_with_root(&db, ctx.peer_id, dim, 6, 10).await;

        // Add a foreign peer leaf that overlaps but must be ignored
        let foreign = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(), // different peer
            parent: None,
            range: cube(dim, &[12], &[13]),
            level: 0,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(foreign).await.unwrap();

        // Candidate covering [11..14) should see exactly 3 local leaves: 11,12,13
        let cand = draft(1, &cube(dim, &[11], &[14]));
        let scorer = ServerScorer::new(params(3));

        let txn = db.begin_read_txn().unwrap();
        let got = scorer
            .child_count_local(&db, &txn, &ctx, &cand, &vec![])
            .await
            .unwrap();
        assert_eq!(
            got, 3,
            "should count local, complete, contained leaves only"
        );
    }

    #[tokio::test]
    async fn child_count_local_returns_zero_when_no_overlap_from_roots() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 12 };
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let (_root, _leaves) = build_linear_leaves_with_root(&db, ctx.peer_id, dim, 4, 50).await;

        let cand = draft(2, &cube(dim, &[10], &[20])); // disjoint
        let scorer = ServerScorer::new(params(2));
        let txn = db.begin_read_txn().unwrap();
        let got = scorer
            .child_count_local(&db, &txn, &ctx, &cand, &vec![])
            .await
            .unwrap();
        assert_eq!(got, 0);
    }

    #[tokio::test]
    async fn child_count_local_dedupes_frontier_from_multiple_roots() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 13 };
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };
        // Build 3 leaves and a first root
        let (_r1, _leaves) = build_linear_leaves_with_root(&db, ctx.peer_id, dim, 3, 30).await;

        // Add a second root that also points to the same leaves (dup coverage)
        let all: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        let leaves: Vec<UuidBytes> = all
            .into_iter()
            .filter(|a| a.level == 0 && a.peer_id == ctx.peer_id)
            .map(|a| a.key)
            .collect();

        let r2 = Availability {
            key: UuidBytes::new(),
            peer_id: ctx.peer_id,
            parent: None,
            range: cube(dim, &[30], &[33]),
            level: 4,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: leaves.clone(),
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(r2).await.unwrap();

        // Sanity: two roots exist
        let txn = db.begin_read_txn().unwrap();
        let roots = roots_for_peer(&db, Some(&txn), &ctx.peer_id).await.unwrap();
        assert!(roots.len() >= 2);

        // Count should still be 3 (no double count)
        let cand = draft(1, &cube(dim, &[30], &[33]));
        let scorer = ServerScorer::new(params(3));
        let got = scorer
            .child_count_local(&db, &txn, &ctx, &cand, &vec![])
            .await
            .unwrap();
        assert_eq!(got, 3, "duplicate roots must not double count");
    }

    /* ---------- Score shaping sanity ---------- */

    #[tokio::test]
    async fn score_prefers_child_target_and_penalizes_deviation() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 14 };
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let (_root, _leaves) = build_linear_leaves_with_root(&db, ctx.peer_id, dim, 6, 100).await;

        let scorer = ServerScorer::new(ServerScorerParams {
            rent: 0.5,
            child_target: 3,
            child_weight: 1.0,
            replication_target: 0,
            replication_weight: 0.0,
        });

        let txn = db.begin_read_txn().unwrap();
        // Candidate windows with 2, 3, 4 leaves respectively
        let c2 = draft(1, &cube(dim, &[101], &[103])); // covers 101,102 -> 2
        let c3 = draft(1, &cube(dim, &[101], &[104])); // 101,102,103 -> 3
        let c4 = draft(1, &cube(dim, &[101], &[105])); // 4 leaves

        let s2 = scorer.score(&db, &txn, &ctx, &c2, &vec![]).await;
        let s3 = scorer.score(&db, &txn, &ctx, &c3, &vec![]).await;
        let s4 = scorer.score(&db, &txn, &ctx, &c4, &vec![]).await;

        assert!(
            s3 > s2 && s3 > s4,
            "score should peak at child_target; got s2={s2}, s3={s3}, s4={s4}"
        );
    }

    /* ---------- Emergence: optimizer + scorer build k-ary coverage ---------- */

    #[tokio::test]
    async fn optimizer_proposes_parents_that_group_exactly_k_leaves() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 15 };
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };

        // Build 9 leaves in [0..9), local root points to all
        let (_root, _leaves) = build_linear_leaves_with_root(&db, ctx.peer_id, dim, 9, 0).await;

        // Scorer tuned for k = 3 children per parent; replication disabled
        let scorer = ServerScorer::new(ServerScorerParams {
            rent: 0.25,
            child_target: 3,
            child_weight: 2.0,
            replication_target: 0,
            replication_weight: 0.0,
        });

        // Optimizer setup (using the beam-based optimizer you plugged in)
        let opt = BasicOptimizer::new(Box::new(scorer), ctx.clone()).with_caps(Caps {
            depth: 2,
            beam_width: 16,
            max_variants_per_draft: 128,
            eps: 0.0,
        });

        // Seed with a small leaf window; beam can promote and expand.
        let seed = AvailabilityDraft {
            level: 0,
            range: cube(dim, &[2], &[3]),
            complete: true,
        };

        let txn = db.begin_read_txn().unwrap();
        let plan = opt.propose(&db, &txn, &[seed]).await.unwrap().unwrap();

        // Keep only INSERTs at level >= 1; they are prospective parents
        let parents: Vec<_> = plan
            .iter()
            .filter_map(|a| match a {
                Action::Insert(d) if d.level >= 1 => Some(d.clone()),
                _ => None,
            })
            .collect();

        assert!(
            !parents.is_empty(),
            "expected the optimizer to propose parent inserts"
        );

        // Count underlying *real* leaves per proposed parent
        let sc = ServerScorer::new(params(3));
        let mut found_k = false;
        for p in parents {
            let cnt = sc
                .child_count_local(&db, &txn, &ctx, &p, &vec![])
                .await
                .unwrap();
            if cnt == 3 {
                found_k = true;
                break;
            }
        }
        assert!(found_k, "at least one parent must group exactly 3 leaves");
    }

    #[tokio::test]
    async fn iterative_planning_covers_all_leaves_in_k_sized_parents() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 16 };
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };

        // 12 leaves, expect ~4 parents of size 3 each in ideal grouping.
        let (_root, _leaves) = build_linear_leaves_with_root(&db, ctx.peer_id, dim, 12, 40).await;

        let scorer = ServerScorer::new(ServerScorerParams {
            rent: 0.25,
            child_target: 3,
            child_weight: 2.0,
            replication_target: 0,
            replication_weight: 0.0,
        });

        let opt = BasicOptimizer::new(Box::new(scorer), ctx.clone()).with_caps(Caps {
            depth: 2,
            beam_width: 16,
            max_variants_per_draft: 256,
            eps: 0.0,
        });

        let txn = db.begin_read_txn().unwrap();

        // Iterate seeds across the space to encourage coverage
        let mut seeds: Vec<AvailabilityDraft> = vec![
            draft(0, &cube(dim, &[40], &[41])),
            draft(0, &cube(dim, &[43], &[44])),
            draft(0, &cube(dim, &[46], &[47])),
            draft(0, &cube(dim, &[49], &[50])),
        ];

        let mut parents: Vec<AvailabilityDraft> = Vec::new();
        for _round in 0..3 {
            if let Some(plan) = opt.propose(&db, &txn, &seeds).await.unwrap() {
                // Accept only INSERT parents; record them as seeds for the next round (to allow promotions)
                for a in plan {
                    if let Action::Insert(d) = a {
                        if d.level >= 1 && !parents.iter().any(|p| p.range == d.range) {
                            parents.push(d.clone());
                        }
                    }
                }
            }
            // Next round: reseed around gaps (shift right)
            seeds = seeds
                .iter()
                .map(|s| {
                    let mut lo = s.range.mins[0].clone();
                    let mut hi = s.range.maxs[0].clone();
                    if !lo.is_empty() {
                        lo[0] = lo[0].saturating_add(3);
                    }
                    if !hi.is_empty() {
                        hi[0] = hi[0].saturating_add(3);
                    }
                    draft(0, &cube(dim, &lo, &hi))
                })
                .collect();
        }

        // For every parent found, count underlying leaves; accept either 2–4 as “near k”
        // (beam search may align boundaries conservatively), but require that **at least three**
        // parents hit exactly k=3.
        let sc_check = ServerScorer::new(params(3));
        let mut exact_k = 0usize;
        for p in &parents {
            let cnt = sc_check
                .child_count_local(&db, &txn, &ctx, p, &vec![])
                .await
                .unwrap();
            assert!(
                (2..=4).contains(&cnt),
                "parent should not be wildly off target; got {cnt}"
            );
            if cnt == 3 {
                exact_k += 1;
            }
        }
        assert!(
            exact_k >= 3,
            "expected at least 3 parents to capture exactly k=3 leaves; got {exact_k}"
        );
    }
}
