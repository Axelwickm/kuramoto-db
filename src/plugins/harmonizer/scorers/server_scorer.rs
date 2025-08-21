use std::collections::HashSet;

use async_trait::async_trait;

use crate::plugins::harmonizer::availability::Availability;
use crate::plugins::harmonizer::harmonizer::PeerContext;
use crate::plugins::harmonizer::optimizer::AvailabilityDraft;
use crate::plugins::harmonizer::scorers::Scorer;
use crate::{KuramotoDb, storage_error::StorageError};

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
/// No snapshots, no overlap pressure.
pub struct ServerScorer {
    params: ServerScorerParams,
}

impl ServerScorer {
    pub fn new(params: ServerScorerParams) -> Self {
        Self { params }
    }

    /// Count distinct peers that **fully contain** the candidate range.
    async fn replication_count(
        &self,
        db: &KuramotoDb,
        cand: &AvailabilityDraft,
    ) -> Result<usize, StorageError> {
        let mut peers: HashSet<_> = HashSet::new();
        // MVP: full scan. Callers can later wrap this scorer with caching/indexes.
        let all: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;
        for a in all {
            if a.complete && a.range.contains(&cand.range) {
                peers.insert(a.peer_id);
            }
        }
        Ok(peers.len())
    }

    /// Estimate local children a parent would adopt (level-1 nodes contained).
    /// For leaves, this is 1 by construction (the written entity).
    async fn child_count_local(
        &self,
        db: &KuramotoDb,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
    ) -> Result<usize, StorageError> {
        if cand.level == 0 {
            return Ok(1);
        }
        let want_child_level = cand.level.saturating_sub(1);
        let all: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;
        Ok(all
            .into_iter()
            .filter(|a| a.complete)
            .filter(|a| a.peer_id == ctx.peer_id) // local-only
            .filter(|a| a.level == want_child_level)
            .filter(|a| cand.range.contains(&a.range))
            .count())
    }

    /// Single “child count” shaping function:
    /// peak at `child_target`, linear drop-off as you deviate.
    fn child_count_score(&self, child_count: usize) -> f32 {
        let t = self.params.child_target as i32;
        let dev = (child_count as i32 - t).abs() as f32;
        self.params.child_weight * ((t as f32) - dev)
    }
}

#[async_trait]
impl Scorer for ServerScorer {
    async fn score(&self, db: &KuramotoDb, ctx: &PeerContext, cand: &AvailabilityDraft) -> f32 {
        // 1) Strong safety valve: if under-replicated, force a positive score and return.
        //    This guarantees we keep/expand coverage until we hit the target.
        let replicas = match self.replication_count(db, cand).await {
            Ok(n) => n as i32,
            Err(_) => 0, // on error, be conservative
        };
        let target = self.params.replication_target as i32;
        if replicas < target {
            // Always positive; bias proportional to shortfall.
            // Caller can still rank among under-replicated candidates.
            return 1.0 + self.params.replication_weight * ((target - replicas) as f32);
        }

        // 2) Otherwise: rent + child-count shaping (no overlap term).
        let mut s = -self.params.rent;

        let child_count = match self.child_count_local(db, ctx, cand).await {
            Ok(c) => c,
            Err(_) => 0, // on error, fall back to 0 local children
        };
        s += self.child_count_score(child_count);

        s
    }
}

#[cfg(test)]
mod emergence_via_plugin_tests {
    use crate::clock::MockClock;
    use crate::plugins::communication::router::{Router, RouterConfig};
    use crate::plugins::harmonizer::availability::Availability;
    use crate::plugins::harmonizer::harmonizer::{Harmonizer, PeerContext};
    use crate::plugins::harmonizer::optimizer::AvailabilityDraft;
    use crate::plugins::harmonizer::scorers::Scorer;
    use crate::storage_entity::{IndexSpec, StorageEntity};
    use crate::uuid_bytes::UuidBytes;
    use crate::{KuramotoDb, StaticTableDef, storage_error::StorageError};

    use redb::{TableDefinition, TableHandle};
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

    // Dummy scorer just to satisfy Harmonizer::new signature (the plugin currently
    // builds its own ServerScorer internally for planning).
    struct Dummy;
    impl Scorer for Dummy {
        fn score(&self, _ctx: &PeerContext, _a: &AvailabilityDraft) -> f32 {
            0.0
        }
    }

    /// End-to-end: with the Harmonizer plugin enabled and many leaves present,
    /// we expect level>=1 parents to emerge that cover multiple leaves, i.e.
    /// the structure is a hierarchy rather than a flat list.
    ///
    /// NOTE: This asserts desired system behavior (hierarchy). It will begin to
    /// pass once the plugin uses a stable self-peer id (so the scorer “sees”
    /// local children) and parent formation is enabled by default.
    #[tokio::test]
    async fn end_to_end_emergence_builds_hierarchy() {
        // Router + Harmonizer plugin (watching Foo)
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());
        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };

        let hz = Harmonizer::new(
            Box::new(Dummy),
            router,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        // DB with plugin installed
        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path()
                .join("emergence_end_to_end.redb")
                .to_str()
                .unwrap(),
            clock.clone(),
            vec![hz.clone()],
        )
        .await;

        db.create_table_and_indexes::<Foo>().unwrap();
        db.create_table_and_indexes::<Availability>().unwrap();

        // Seed rows → plugin creates leaves during before_update and (once wired)
        // proposes parents in the same batch.
        for i in 0..30u32 {
            db.put(Foo { id: i }).await.unwrap();
        }

        // Observe the resulting availability graph.
        let avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();

        let leaves: Vec<&Availability> = avs.iter().filter(|a| a.level == 0).collect();
        let parents: Vec<&Availability> = avs.iter().filter(|a| a.level >= 1).collect();

        // Basic presence checks.
        assert!(
            leaves.len() >= 30,
            "expected at least 30 leaves; got {}",
            leaves.len()
        );
        assert!(
            !parents.is_empty(),
            "expected parent nodes to emerge (level>=1), but none were found"
        );

        // At least one parent should adopt multiple leaves (grouping effect).
        let mut best_adopt = 0usize;
        for p in &parents {
            let adopted = leaves
                .iter()
                .filter(|lv| p.range.contains(&lv.range))
                .count();
            best_adopt = best_adopt.max(adopted);
        }
        assert!(
            best_adopt >= 4,
            "expected a parent to cover multiple leaves; best adoption count was {best_adopt}"
        );

        // Optional shape sanity: if we have ≥2 parent levels, ensure nesting (tree-like).
        let max_parent_level = parents.iter().map(|a| a.level).max().unwrap_or(0);
        if max_parent_level >= 2 {
            // Find a level-2+ parent that contains some level-1 parent (hierarchical nesting)
            let has_nesting = parents.iter().any(|p_hi| {
                parents
                    .iter()
                    .any(|p_lo| p_lo.level + 1 == p_hi.level && p_hi.range.contains(&p_lo.range))
            });
            assert!(has_nesting, "expected some nesting among parent levels");
        }
    }
}
