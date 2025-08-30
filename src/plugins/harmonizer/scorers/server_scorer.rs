// server_scorer.rs
// Stateless scorer that consults the DB (snapshot-aware) and respects an overlay for deletes.
// Now with simple memo caches for roots/frontiers/replication.

use async_trait::async_trait;
use redb::ReadTransaction;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{
        availability::Availability,
        availability::roots_for_peer,
        availability_queries::{count_replications, range_cover},
        harmonizer::PeerContext,
        optimizer::{Action, ActionSet, AvailabilityDraft},
        range_cube::RangeCube,
        scorers::Scorer,
    },
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/// Tunables for the server-side scoring heuristic.
#[derive(Clone, Debug)]
pub struct ServerScorerParams {
    pub rent: f32,
    pub child_target: usize,
    pub child_weight: f32,
    pub replication_target: usize,
    pub replication_weight: f32,
    pub level_weight: f32,
}

impl Default for ServerScorerParams {
    fn default() -> Self {
        Self {
            rent: 1.0,
            child_target: 6,
            child_weight: 0.5,
            replication_target: 3,
            replication_weight: 1.0,
            level_weight: 0.0001,
        }
    }
}

// ───── simple global caches keyed by txn ptr + peer/range ─────

#[derive(Default)]
struct ScorerCaches {
    roots: Mutex<HashMap<(usize, Vec<u8>), Vec<UuidBytes>>>,
    frontier: Mutex<HashMap<(usize, Vec<u8>, Vec<u8>), Vec<UuidBytes>>>,
    repl: Mutex<HashMap<(usize, Vec<u8>), usize>>,
}

fn txntag(txn: &ReadTransaction) -> usize {
    (txn as *const ReadTransaction) as usize
}
fn peer_key_bytes(p: &UuidBytes) -> Vec<u8> {
    p.as_bytes().to_vec()
}
fn range_key(r: &RangeCube) -> Vec<u8> {
    let mut k = Vec::with_capacity(64);
    for d in r.dims() {
        k.extend_from_slice(&d.hash.to_be_bytes());
        k.push(0xFE);
    }
    k.push(0xF0);
    for m in r.mins() {
        k.extend_from_slice(m);
        k.push(0xFD);
    }
    k.push(0xE0);
    for m in r.maxs() {
        k.extend_from_slice(m);
        k.push(0xFB);
    }
    k
}

/// Server-side scorer with lightweight memoization.
pub struct ServerScorer {
    params: ServerScorerParams,
    caches: ScorerCaches,
}

impl ServerScorer {
    pub fn new(params: ServerScorerParams) -> Self {
        Self {
            params,
            caches: ScorerCaches::default(),
        }
    }

    async fn replication_count(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        cand: &AvailabilityDraft,
    ) -> Result<usize, StorageError> {
        let tk = txntag(txn);
        let rk = range_key(&cand.range);
        {
            let repl = self.caches.repl.lock().unwrap();
            if let Some(v) = repl.get(&(tk, rk.clone())) {
                // println!("Repl cache hit!");
                return Ok(*v);
            }
        }
        let v = count_replications(db, Some(txn), &cand.range, |_a| true, None).await?;
        {
            let mut repl = self.caches.repl.lock().unwrap();
            if repl.len() > 4096 {
                repl.clear();
            }
            repl.insert((tk, rk), v);
        }
        Ok(v)
    }

    async fn roots_for_peer(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        peer: &UuidBytes,
    ) -> Result<Vec<UuidBytes>, StorageError> {
        let tk = txntag(txn);
        let pk = peer_key_bytes(peer);
        {
            let roots = self.caches.roots.lock().unwrap();
            if let Some(v) = roots.get(&(tk, pk.clone())) {
                // println!("Root cache hit");
                return Ok(v.clone());
            }
        }
        let roots = roots_for_peer(db, Some(txn), peer).await?;
        let ids: Vec<UuidBytes> = roots.into_iter().map(|r| r.key).collect();
        {
            let mut roots = self.caches.roots.lock().unwrap();
            if roots.len() > 1024 {
                roots.clear();
            }
            roots.insert((tk, pk), ids.clone());
        }
        Ok(ids)
    }

    async fn frontier_ids(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        range: &RangeCube,
    ) -> Result<Vec<UuidBytes>, StorageError> {
        let tk = txntag(txn);
        let pk = peer_key_bytes(&ctx.peer_id);
        let rk = range_key(range);
        {
            let fr = self.caches.frontier.lock().unwrap();
            if let Some(v) = fr.get(&(tk, pk.clone(), rk.clone())) {
                // println!("Fronteir cache hit");
                return Ok(v.clone());
            }
        }

        let roots_ids = self.roots_for_peer(db, txn, &ctx.peer_id).await?;
        let (frontier, no_overlap) = range_cover(db, Some(txn), range, &roots_ids, None).await?;
        let mut seen = HashSet::<UuidBytes>::new();
        if !no_overlap {
            for a in frontier {
                if a.peer_id == ctx.peer_id && a.complete && range.contains(&a.range) {
                    let _ = seen.insert(a.key);
                }
            }
        }
        let ids: Vec<UuidBytes> = seen.into_iter().collect();
        {
            let mut fr = self.caches.frontier.lock().unwrap();
            if fr.len() > 4 * 4096 {
                fr.clear();
            }
            fr.insert((tk, pk, rk), ids.clone());
        }
        Ok(ids)
    }

    fn child_count_score(&self, child_count: usize) -> f32 {
        let t = self.params.child_target as i32;
        let dev = (child_count as i32 - t).abs() as f32;
        self.params.child_weight * ((t as f32) - dev)
    }

    pub(crate) async fn child_count_local(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
        overlay: &ActionSet,
    ) -> Result<usize, StorageError> {
        let mut deleted = HashSet::<UuidBytes>::new();
        for a in overlay {
            if let Action::Delete(id) = a {
                deleted.insert(*id);
            }
        }
        let ids = self.frontier_ids(db, txn, ctx, &cand.range).await?;
        let count = ids.iter().filter(|id| !deleted.contains(id)).count();
        Ok(count)
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
        let replicas = match self.replication_count(db, txn, cand).await {
            Ok(n) => n as i32,
            Err(_) => 0,
        };
        let target = self.params.replication_target as i32;
        if replicas < target {
            return 1.0 + self.params.replication_weight * ((target - replicas) as f32);
        }

        let mut s = -self.params.rent;
        let child_count = match self.child_count_local(db, txn, ctx, cand, overlay).await {
            Ok(c) => c,
            Err(_) => 0,
        };
        s += self.child_count_score(child_count);
        s += (cand.level as f32) * self.params.level_weight;
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
            child_set::{ChildSet, Child, DigestChunk},
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

    fn cube(dim: TableHash, min: &[u8], max_excl: &[u8]) -> RangeCube {
        RangeCube::new(
            smallvec![dim],
            smallvec![min.to_vec()],
            smallvec![max_excl.to_vec()],
        )
        .unwrap()
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
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<DigestChunk>().unwrap();
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
                range: cube(dim, &lo, &hi),
                level: 0,
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
            range: cube(dim, &[base], &[base + n]), // covers all
            level: 3,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root).await.unwrap();
        // Link root -> leaves via child table
        let mut cs = ChildSet::open(db, rid).await.unwrap();
        for id in &leaf_ids {
            cs.add_child(db, *id).await.unwrap();
        }

        (rid, leaf_ids)
    }

    fn params(child_target: usize) -> ServerScorerParams {
        ServerScorerParams {
            rent: 0.25,
            child_target,
            child_weight: 1.0,
            replication_target: 0, // disable safety valve for these tests
            replication_weight: 0.0,
            ..Default::default()
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
            range: cube(dim, &[12], &[13]),
            level: 0,
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
            range: cube(dim, &[30], &[33]),
            level: 4,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let r2_id = r2.key;
        db.put(r2).await.unwrap();
        let mut cs2 = ChildSet::open(&db, r2_id).await.unwrap();
        for id in &leaves {
            cs2.add_child(&db, *id).await.unwrap();
        }

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
            ..Default::default()
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
            ..Default::default()
        });

        let opt = BasicOptimizer::new(Box::new(scorer), ctx.clone()).with_caps(Default::default());

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
            ..Default::default()
        });

        let opt = BasicOptimizer::new(Box::new(scorer), ctx.clone()).with_caps(Default::default());

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
                    let mut lo = s.range.mins()[0].clone();
                    let mut hi = s.range.maxs()[0].clone();
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
