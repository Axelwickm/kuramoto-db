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
        let want_child_level = 10; // TODO: don't hardcode
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

#[cfg(test)]
mod emergence_via_plugin_tests {
    use crate::clock::MockClock;
    use crate::plugins::communication::router::{Router, RouterConfig};
    use crate::plugins::harmonizer::availability::Availability;
    use crate::plugins::harmonizer::harmonizer::Harmonizer;
    use crate::plugins::harmonizer::optimizer::{
        AvailabilityDraft, BasicOptimizer, Optimizer, Overlay, PeerContext,
    };
    use crate::plugins::harmonizer::range_cube::RangeCube;
    use crate::plugins::harmonizer::scorers::Scorer;
    use crate::plugins::harmonizer::scorers::server_scorer::{ServerScorer, ServerScorerParams};
    use crate::storage_entity::{IndexSpec, StorageEntity};
    use crate::tables::TableHash;
    use crate::uuid_bytes::UuidBytes;
    use crate::{KuramotoDb, StaticTableDef, storage_error::StorageError};
    use redb::TableDefinition;
    use redb::TableHandle;
    use smallvec::smallvec;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tempfile::tempdir;

    // ---------- Minimal test entity ----------
    #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq, Eq)]
    struct Foo {
        id: u32,
    }
    impl StorageEntity for Foo {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_be_bytes().to_vec()
        }
        fn table_def() -> StaticTableDef {
            static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("foo");
            &TBL
        }
        fn meta_table_def() -> StaticTableDef {
            static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("foo_meta");
            &TBL
        }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            match data.first().copied() {
                Some(0) => {
                    bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                        .map(|(v, _)| v)
                        .map_err(|e| StorageError::Bincode(e.to_string()))
                }
                _ => Err(StorageError::Bincode("bad version".into())),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] {
            &[]
        }
    }

    // Dummy scorer just to satisfy Harmonizer::new signature (Harmonizer builds its own ServerScorer internally)
    struct Dummy;
    impl Scorer for Dummy {
        fn score(&self, _ctx: &PeerContext, _a: &AvailabilityDraft) -> f32 {
            0.0
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // EMERGENCE (real): scorer+optimizer propose parents that “adopt” many leaves
    // ─────────────────────────────────────────────────────────────────────
    //
    // This bypasses the plugin’s ephemeral peer-id issue and tests the actual
    // emergence logic: given a bed of local leaves, does the scorer+optimizer
    // propose level>0 parents that cover multiple children?
    #[tokio::test]
    async fn scorer_emergence_builds_parents_over_leaf_bed() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("emergence_core.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();

        // All leaves share the SAME local peer id → scorer can “see” children locally.
        let self_peer = UuidBytes::new();
        let dim = TableHash { hash: 1 };
        let cube = |min: &[u8], max: &[u8]| RangeCube {
            dims: smallvec![dim],
            mins: smallvec![min.to_vec()],
            maxs: smallvec![max.to_vec()],
        };

        // Seed ~30 leaves on one axis: [i, i+ε)
        for i in 0u8..30 {
            let id = UuidBytes::new();
            let leaf = Availability {
                key: id,
                peer_id: self_peer,
                range: cube(&[i], &[i, 1]),
                level: 0,
                children: crate::plugins::harmonizer::child_set::ChildSet {
                    parent: id,
                    children: vec![i as u64],
                },
                schema_hash: 0,
                version: 0,
                updated_at: 0,
                complete: true,
            };
            db.put(leaf).await.unwrap();
        }

        // Scorer tuned to actually form parents (target ~6 kids, modest rent).
        let params = ServerScorerParams {
            rent: 0.2,
            child_target: 6,
            child_weight: 2.0,
            overlap_weight: 0.1,
            replication_target: 1,
            replication_weight: 0.0,
        };
        let scorer = ServerScorer::from_db_snapshot(&db, self_peer, params.clone())
            .await
            .unwrap();
        let opt = BasicOptimizer::new(Box::new(scorer), PeerContext { peer_id: self_peer });

        // Seed a tiny draft around the middle; optimizer must promote+expand.
        let base = AvailabilityDraft {
            level: 0,
            range: cube(&[15u8], &[15u8, 1]),
            complete: true,
        };
        let binding = [base];
        let overlay = Overlay::with_score(&binding, &[], &*opt.scorer, &opt.ctx);

        let plan = opt
            .propose(&db, overlay)
            .await
            .unwrap()
            .expect("should propose parents");

        println!("plan.len()={}", plan.len());
        for (i, a) in plan.iter().enumerate() {
            match a {
                crate::plugins::harmonizer::optimizer::Action::Insert(d) => {
                    println!(
                        "  plan[{}] INSERT level={} mins={:?} maxs={:?}",
                        i, d.level, d.range.mins, d.range.maxs
                    );
                }
                crate::plugins::harmonizer::optimizer::Action::Delete(id) => {
                    println!("  plan[{}] DELETE id={:?}", i, id);
                }
            }
        }
        let leaves: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap()
            .into_iter()
            .filter(|a| a.level == 0)
            .collect();

        // Validate: at least one parent insert exists and it covers multiple leaves (~target).
        let mut best_adopt = 0usize;
        let mut saw_parent = false;
        for act in plan {
            if let crate::plugins::harmonizer::optimizer::Action::Insert(d) = act {
                if d.level >= 1 {
                    saw_parent = true;
                    let adopted = leaves
                        .iter()
                        .filter(|lv| d.range.contains(&lv.range))
                        .count();
                    best_adopt = best_adopt.max(adopted);
                }
            }
        }
        assert!(saw_parent, "expected at least one parent insert");
        assert!(
            best_adopt >= 4,
            "parent should cover multiple leaves (got {best_adopt})"
        );
        // Optional stronger claim if you want closeness to target:
        // assert!((best_adopt as isize - params.child_target as isize).abs() <= 2);
    }

    // ─────────────────────────────────────────────────────────────────────
    // SYSTEM (current): plugin forms leaves but NO parents yet (peer-id mismatch)
    // ─────────────────────────────────────────────────────────────────────
    //
    // This documents current end-to-end behavior: Harmonizer randomizes peer_id
    // per write, so the scorer sees 0 local children and doesn’t propose parents.
    // Flip the final assertion once the plugin uses a stable self_peer.
    #[tokio::test(start_paused = true)]
    async fn plugin_currently_only_forms_leaves_no_parents() {
        // Router + Harmonizer plugin (watching Foo)
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());
        let hz = Harmonizer::new(
            Box::new(Dummy),
            router,
            HashSet::from([Foo::table_def().name()]),
        );

        // DB with plugin installed
        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("emergence_plugin.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();

        // Seed rows → plugin creates leaves during before_update
        for i in 0..20u32 {
            db.put(Foo { id: i }).await.unwrap();
        }

        // Observe: leaves yes, parents no (for now)
        let avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        let leaves = avs.iter().filter(|a| a.level == 0).count();
        let parents = avs.iter().filter(|a| a.level >= 1).count();

        assert!(leaves >= 20, "expected at least 20 leaves");
        assert_eq!(
            parents, 0,
            "no parents should emerge yet (random peer_id per leaf)"
        );
    }
}
