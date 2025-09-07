// server_scorer.rs
// Stateless scorer that consults the DB (snapshot-aware) and respects an overlay for deletes.
// Now with simple memo caches for roots/frontiers/replication.

use async_trait::async_trait;
use redb::ReadTransaction;
use std::collections::HashMap;
use tokio::sync::Mutex;
use std::time::Instant;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{
        availability_queries::{AvailabilityQueryCache, child_count, peer_contains_range_local},
        harmonizer::PeerContext,
        optimizer::{ActionSet, AvailabilityDraft},
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
    repl: Mutex<HashMap<(usize, Vec<u8>), usize>>,
    child_counts: Mutex<HashMap<(usize, Vec<u8>, Vec<u8>), usize>>,
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
    query_cache: Mutex<Option<AvailabilityQueryCache>>, // shared per-txn cache
}

impl ServerScorer {
    pub fn new(params: ServerScorerParams) -> Self {
        Self {
            params,
            caches: ScorerCaches::default(),
            query_cache: Mutex::new(None),
        }
    }

    async fn ensure_cache_for_txn(&self, txn: &ReadTransaction) {
        let mut guard = self.query_cache.lock().await;
        let needs = match guard.as_ref() {
            Some(c) => !c.compatible_txn(Some(txn)),
            None => true,
        };
        if needs {
            *guard = Some(AvailabilityQueryCache::new(txn));
        }
    }

    async fn replication_count(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
        overlay: &ActionSet,
    ) -> Result<usize, StorageError> {
        self.ensure_cache_for_txn(txn).await;
        let tk = txntag(txn);
        let rk = range_key(&cand.range);
        {
            let repl = self.caches.repl.lock().await;
            if let Some(v) = repl.get(&(tk, rk.clone())) {
                return Ok(*v);
            }
        }

        // Global-ish replication count from our current snapshot:
        // count distinct peers that fully contain this range with a complete availability.
        // This leverages our ingested remote availabilities; no network probes here.
        let t0 = Instant::now();
        let replicas = crate::plugins::harmonizer::availability_queries::count_replications(
            db,
            Some(txn),
            &cand.range,
            |_| true,
            None,
        )
        .await?;
        let dt = t0.elapsed();
        // println!(
        //     "scorer.replication_count: level={} peers={} took={}ms",
        //     cand.level,
        //     replicas,
        //     dt.as_millis()
        // );
        //
        let mut repl = self.caches.repl.lock().await;
        if repl.len() > 4096 {
            repl.clear();
        }
        repl.insert((tk, rk), replicas);
        Ok(replicas)
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
        self.ensure_cache_for_txn(txn).await;
        let tk = txntag(txn);
        let pk = peer_key_bytes(&ctx.peer_id);
        let rk = range_key(&cand.range);

        if overlay.is_empty() {
            let cc_guard = self.caches.child_counts.lock().await;
            if let Some(cached) = cc_guard.get(&(tk, pk.clone(), rk.clone())).copied() {
                return Ok(cached);
            }
        }

        // Use level-aware child counting with cache shared via AvailabilityQueryCache
        let count = {
            // take the cache out to avoid holding lock across await
            let mut local_cache = {
                let mut guard = self.query_cache.lock().await;
                guard.take()
            };
            let mut opt_ref: Option<&mut AvailabilityQueryCache> =
                local_cache.as_mut().map(|c| c as _);
            let t0 = Instant::now();
            let res = child_count(
                db,
                Some(txn),
                &ctx.peer_id,
                &cand.range,
                cand.level,
                overlay,
                &mut opt_ref,
            )
            .await?;
            let dt = t0.elapsed();
            // println!(
            //     "scorer.child_count_local: level={} count={} overlay={} took={}ms",
            //     cand.level,
            //     res,
            //     overlay.len(),
            //     dt.as_millis()
            // );
            // restore cache
            {
                let mut guard = self.query_cache.lock().await;
                *guard = local_cache;
            }
            res
        };

        if overlay.is_empty() {
            let mut cc = self.caches.child_counts.lock().await;
            if cc.len() > 4 * 4096 {
                cc.clear();
            }
            cc.insert((tk, pk, rk), count);
        }
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
        let replicas = match self.replication_count(db, txn, ctx, cand, overlay).await {
            Ok(n) => n as i32,
            Err(_) => 0,
        };
        let target = self.params.replication_target as i32;

        // Symmetric replication shaping: penalize deviation from target on both sides.
        // Reducing |replicas - target| increases score; overshooting is discouraged too.
        let mut s = -self.params.rent;
        let repl_diff = (replicas - target).abs() as f32;
        s += -self.params.replication_weight * repl_diff;
        let child_count = match self.child_count_local(db, txn, ctx, cand, overlay).await {
            Ok(c) => c,
            Err(_) => 0,
        };
        s += self.child_count_score(child_count);
        s += (cand.level as f32) * self.params.level_weight;
        // Nudge toward parents to encourage grouping over flat leaves
        if cand.level == 0 {
            s -= 1.0;
        } else {
            s += 1.0;
        }
        s
    }
}

/*──────────────────────────── tests ─────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_entity::StorageEntity;
    use crate::{
        KuramotoDb,
        clock::MockClock,
        plugins::harmonizer::{
            availability::{Availability, roots_for_peer},
            child_set::{Child, ChildSet, DigestChunk},
            harmonizer::PeerContext,
            optimizer::{Action, ActionSet, AvailabilityDraft, BasicOptimizer, Caps, Optimizer},
            range_cube::RangeCube,
            scorers::Scorer,
        },
        tables::TableHash,
        uuid_bytes::UuidBytes,
    };
    use redb::TableHandle;
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
        // Use a real data-table-backed dim so level-0 growth can be scored via storage atoms.
        // Register a simple entity table and populate atoms spanning the test window.
        db.create_table_and_indexes::<TestEnt>().unwrap();
        for id in 30u32..60u32 {
            db.put(TestEnt { id }).await.unwrap();
        }
        let dim = TableHash::from(TestEnt::table_def());
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

    // ───── Entities + replication tests (3 peers, k=2) ─────

    #[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct TestEnt {
        id: u32,
    }
    impl StorageEntity for TestEnt {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_le_bytes().to_vec()
        }
        fn table_def() -> crate::StaticTableDef {
            static T: redb::TableDefinition<'static, &'static [u8], Vec<u8>> =
                redb::TableDefinition::new("test_ent");
            &T
        }
        fn meta_table_def() -> crate::StaticTableDef {
            static M: redb::TableDefinition<'static, &'static [u8], Vec<u8>> =
                redb::TableDefinition::new("test_ent_meta");
            &M
        }
        fn load_and_migrate(data: &[u8]) -> Result<Self, crate::storage_error::StorageError> {
            match data.first().copied() {
                Some(0) => {
                    bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                        .map(|(v, _)| v)
                        .map_err(|e| crate::storage_error::StorageError::Bincode(e.to_string()))
                }
                _ => Err(crate::storage_error::StorageError::Bincode(
                    "bad version".into(),
                )),
            }
        }
        fn indexes() -> &'static [crate::storage_entity::IndexSpec<Self>] {
            &[]
        }
    }

    #[tokio::test(start_paused = true)]
    async fn three_peers_replicate_k2_single_insert() {
        use crate::plugins::harmonizer::SyncTester;
        let peers = vec![UuidBytes::new(), UuidBytes::new(), UuidBytes::new()];
        let params = ServerScorerParams {
            replication_target: 2,
            replication_weight: 5.0,
            ..Default::default()
        };
        let mut t = SyncTester::new(&peers, &[TestEnt::table_def().name()], params).await;
        for n in t.peers_mut() {
            n.db.create_table_and_indexes::<TestEnt>().unwrap();
        }
        // Broadcast to both remotes
        for i in 0..t.peers().len() {
            for j in 0..t.peers().len() {
                if i != j {
                    t.peers()[i].harmonizer.add_peer(t.peers()[j].peer_id).await;
                }
            }
        }
        // Insert one row on origin
        let enc = |e: &TestEnt| {
            let mut out = Vec::new();
            out.push(0);
            out.extend(bincode::encode_to_vec(e, bincode::config::standard()).unwrap());
            out
        };
        t.insert_bytes(
            peers[0],
            TestEnt::table_def().name(),
            enc(&TestEnt { id: 1 }),
        )
        .await;
        // Wait until exactly k=2 peers have the row (origin + one remote)
        let origin = peers[0];
        let _rep = t
            .run_until_async(10_000, Some(std::time::Duration::from_secs(5)), |st| {
                Box::pin(async move {
                    let mut count = 0;
                    let mut origin_has = false;
                    for n in st.peers().iter() {
                        let ok = n.db.get_data::<TestEnt>(&1u32.to_le_bytes()).await.is_ok();
                        if ok {
                            count += 1;
                        }
                        if n.peer_id == origin && ok {
                            origin_has = true;
                        }
                    }
                    origin_has && count == 2
                })
            })
            .await;
        // Post-check
        let mut count = 0;
        let mut origin_has = false;
        for n in t.peers().iter() {
            let ok = n.db.get_data::<TestEnt>(&1u32.to_le_bytes()).await.is_ok();
            if ok {
                count += 1;
            }
            if n.peer_id == origin && ok {
                origin_has = true;
            }
        }
        assert!(origin_has, "origin should retain the row");
        assert_eq!(count, 2, "row should be present on exactly k=2 peers");
    }

    #[tokio::test(start_paused = true)]
    async fn three_peers_replicate_k2_spread_many_rows() {
        use crate::plugins::harmonizer::SyncTester;
        use crate::plugins::harmonizer::sync_tester::SyncTesterOptions;
        use std::fs;
        use std::path::Path;
        let peers = vec![UuidBytes::new(), UuidBytes::new(), UuidBytes::new()];
        let params = ServerScorerParams {
            replication_target: 2,
            replication_weight: 5.0,
            ..Default::default()
        };
        let opts = SyncTesterOptions { record_replay: true, export_dir: Some(std::path::PathBuf::from("exports")) };
        let mut t = SyncTester::new_with_options(&peers, &[TestEnt::table_def().name()], params, opts.clone()).await;
        for n in t.peers_mut() {
            n.db.create_table_and_indexes::<TestEnt>().unwrap();
        }
        for i in 0..t.peers().len() {
            for j in 0..t.peers().len() {
                if i != j {
                    t.peers()[i].harmonizer.add_peer(t.peers()[j].peer_id).await;
                }
            }
        }
        let enc = |e: &TestEnt| {
            let mut out = Vec::new();
            out.push(0);
            out.extend(bincode::encode_to_vec(e, bincode::config::standard()).unwrap());
            out
        };
        let total = 60u32;
        for id in 1..=total {
            t.insert_bytes(peers[0], TestEnt::table_def().name(), enc(&TestEnt { id }))
                .await;
        }
        // Wait until each row is present on exactly k=2 peers, and one of them is the origin
        let origin = peers[0];
        let _rep = t
            .run_until_async(20_000, Some(std::time::Duration::from_secs(8)), |st| {
                Box::pin(async move {
                    for id in 1..=total {
                        let mut count = 0;
                        let mut origin_has = false;
                        for n in st.peers().iter() {
                            let ok = n.db.get_data::<TestEnt>(&id.to_le_bytes()).await.is_ok();
                            if ok {
                                count += 1;
                            }
                            if n.peer_id == origin && ok {
                                origin_has = true;
                            }
                        }
                        if !(origin_has && count == 2) {
                            return false;
                        }
                    }
                    true
                })
            })
            .await;
        // Post-check: each row has exactly 2 replicas and origin holds all rows.
        for id in 1..=total {
            let mut count = 0;
            let mut origin_has = false;
            for n in t.peers().iter() {
                let ok = n.db.get_data::<TestEnt>(&id.to_le_bytes()).await.is_ok();
                if ok {
                    count += 1;
                }
                if n.peer_id == origin && ok {
                    origin_has = true;
                }
            }
            assert!(origin_has, "origin should retain row {id}");
            assert_eq!(
                count, 2,
                "row {id} should be replicated to exactly k=2 peers; got {count}"
            );
        }

        // Balance: origin has all rows; remotes split the rest roughly evenly.
        let mut origin_count = 0usize;
        let mut remote_counts = Vec::new();
        for n in t.peers().iter() {
            let mut ok = 0;
            for id in 1..=total {
                if n.db.get_data::<TestEnt>(&id.to_le_bytes()).await.is_ok() {
                    ok += 1;
                }
            }
            if n.peer_id == origin {
                origin_count = ok;
            } else {
                remote_counts.push(ok);
            }
        }
        assert_eq!(origin_count, total as usize, "origin should hold all rows");
        assert_eq!(remote_counts.len(), 2);
        let a = remote_counts[0] as i32;
        let b = remote_counts[1] as i32;
        let diff = (a - b).abs();
        assert!(
            diff as f32 <= (total as f32 * 0.2),
            "replicas should be roughly balanced across remotes: {a} vs {b}"
        );

        // Export replay logs under ./exports (one file per DB)
        if let Some(dir) = opts.export_dir {
            let _ = t.export_replay_files(&dir).await;
            // Basic assertion: directory exists and contains at least one replay file per peer
            let entries = fs::read_dir(&dir).unwrap();
            let mut count = 0usize;
            for e in entries {
                let p = e.unwrap().path();
                if p.is_file() && p.file_name().unwrap().to_string_lossy().starts_with("replay_") {
                    count += 1;
                }
            }
            assert!(count >= peers.len(), "expected at least {} replay files in {:?}", peers.len(), dir);
            // Leave exports in place for inspection
        }
    }
}
