//! Optimizer: reductionist, stateless, mutation-driven search
//! ---------------------------------------------------------
//! - Runs in `before_update` against a virtual overlay (pending inserts/deletes).
//! - Tries **primitive mutations** only: `Insert(draft)` and `Delete(id)`.
//! - Complex edits (merge/split/rebuild) emerge from sequences of these primitives.
//! - Returns the **greedy hill-climbed ActionSet**:
//!     keep taking the best improving step until there is no improvement
//!     or we hit `caps.max_steps` (default 512).
//! - **Hard rule:** empty ranges are discarded; existing empty parents can be pruned.

use async_trait::async_trait;
use std::sync::Arc;

use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::uuid_bytes::UuidBytes;
use crate::{KuramotoDb, storage_error::StorageError};

/*────────────────────────── Scorer ───────────────────────────*/

#[derive(Clone, Debug)]
pub struct PeerContext {
    pub peer_id: UuidBytes,
}

/// Keep the scorer *pure*. It should not read the DB or mutate anything.
/// The optimizer will construct candidates and ask the scorer to rank them.
pub trait Scorer: Send + Sync {
    /// Score a single availability draft. Higher is better.
    /// Negative means "prefer to remove" (subject to replication guards).
    fn score(&self, ctx: &PeerContext, avail: &AvailabilityDraft) -> f32;
}

/*──────────────────────────── Types ───────────────────────────*/

#[derive(Debug, Clone)]
pub struct AvailabilityDraft {
    pub level: u16,
    pub range: RangeCube,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub enum Action {
    Insert(AvailabilityDraft),
    Delete(UuidBytes),
}

/// Ordered list of atomic actions (execute in order within the batch).
pub type ActionSet = Vec<Action>;

#[derive(Debug)]
pub struct Overlay<'a> {
    pub pending_inserts: &'a [AvailabilityDraft],
    pub pending_deletes: &'a [UuidBytes],
    pub score: f32, // sum of draft scores (node-only for now)
}

impl<'a> Overlay<'a> {
    pub fn with_score(
        pending_inserts: &'a [AvailabilityDraft],
        pending_deletes: &'a [UuidBytes],
        scorer: &dyn Scorer,
        ctx: &PeerContext,
    ) -> Self {
        let score = pending_inserts
            .iter()
            .filter(|a| !range_is_empty(&a.range))
            .map(|a| scorer.score(ctx, a))
            .sum();
        Self {
            pending_inserts,
            pending_deletes,
            score,
        }
    }
}

/*──────────────────────── Caps (bounds + epsilon) ───────────*/

#[derive(Debug, Clone, Copy)]
pub struct Caps {
    /// candidate parents per focus (DB lookup cap)
    pub pmax_parents: usize,
    /// (kept for compat / future sibling exploration; unused in current reductionist pass)
    pub nmax_neighbors: usize,
    /// target children bias (scorer may use it; optimizer just carries it)
    pub b_target: usize,
    /// improvement threshold
    pub eps: f32,
    /// mutation step for range boundaries (kept for non-byte dims)
    pub step: i64,
    /// cap enumerated insert variants per draft
    pub max_variants_per_draft: usize,
    /// **hill-climb step cap** — keep taking best greedy steps until none or this cap
    pub max_steps: usize,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            pmax_parents: 8,
            nmax_neighbors: 8,
            b_target: 6,
            eps: 0.0,
            step: 1,
            max_variants_per_draft: 32,
            max_steps: 512,
        }
    }
}

/*────────────────────────── Trait ────────────────────────────*/

#[async_trait]
pub trait Optimizer: Send + Sync {
    /// Greedy hill-climb:
    ///   1) enumerate best step
    ///   2) if improvement > eps, **apply to overlay** and continue
    ///   3) else stop
    /// Returns the **full ActionSet** or `None` if no improvement at all.
    async fn propose(
        &self,
        db: &KuramotoDb,
        overlay: Overlay<'_>,
    ) -> Result<Option<ActionSet>, StorageError>;
}

/*──────────────────── Reductionist optimizer ──────────────────*/

pub struct BasicOptimizer {
    pub scorer: Box<dyn Scorer>,
    pub ctx: PeerContext,
    pub caps: Caps,
}

impl BasicOptimizer {
    pub fn new(scorer: Box<dyn Scorer>, ctx: PeerContext) -> Self {
        Self {
            scorer,
            ctx,
            caps: Caps::default(),
        }
    }
    pub fn with_caps(mut self, caps: Caps) -> Self {
        self.caps = caps;
        self
    }

    // Best single action for a given draft, using primitive mutations only.
    // Returns (gain, actions).
    async fn best_action_for_draft(
        &self,
        db: &KuramotoDb,
        _overlay: &Overlay<'_>,
        drafts: &[AvailabilityDraft],
        focus_idx: usize,
    ) -> Result<Option<(f32, ActionSet)>, StorageError> {
        let d = &drafts[focus_idx];

        let mut best_gain = 0.0f32;
        let mut best_actions: Option<ActionSet> = None;

        // 1) Primitive INSERT mutations derived from the focus draft (same level + level+1 promos)
        for cand in enumerate_insert_mutations(d, self.caps) {
            // Note: gain is absolute here; in practice the scorer should
            // encode "cost/size" so useless identity loses to a better variant.
            let gain = self.scorer.score(&self.ctx, &cand);
            if gain > best_gain + self.caps.eps {
                best_gain = gain;
                best_actions = Some(vec![Action::Insert(cand)]);
            }
        }

        // 2) Primitive DELETE and DELETE+INSERT for nearby existing parents
        let parents = find_covering_or_near_parents(db, d, self.caps.pmax_parents).await?;
        for a in parents {
            // Treat the existing parent as a draft for scoring deltas
            let draft_old = AvailabilityDraft {
                level: a.level,
                range: a.range.clone(),
                complete: true,
            };
            let s_old = if range_is_empty(&draft_old.range) {
                0.0
            } else {
                self.scorer.score(&self.ctx, &draft_old)
            };

            // 2a) Pure DELETE (prune) if beneficial and replication allows
            if s_old < 0.0 && replication_guard_ok(db, &a).await {
                let delta = -s_old;
                if delta > best_gain + self.caps.eps {
                    best_gain = delta;
                    best_actions = Some(vec![Action::Delete(a.id)]);
                }
            }

            // 2b) DELETE + INSERT (single mutated replacement)
            for mutated in enumerate_insert_mutations(&draft_old, self.caps) {
                if range_is_empty(&mutated.range) {
                    continue;
                }
                let s_new = self.scorer.score(&self.ctx, &mutated);
                let delta = s_new - s_old;
                if delta > best_gain + self.caps.eps {
                    best_gain = delta;
                    best_actions =
                        Some(vec![Action::Delete(a.id), Action::Insert(mutated.clone())]);
                }
            }

            // 2c) DELETE + INSERT + INSERT (two mutated variants = "split" from primitives)
            let variants: Vec<AvailabilityDraft> =
                enumerate_insert_mutations(&draft_old, self.caps)
                    .into_iter()
                    .collect();
            for i in 0..variants.len() {
                for j in (i + 1)..variants.len() {
                    let a1 = &variants[i];
                    let a2 = &variants[j];
                    if range_is_empty(&a1.range) || range_is_empty(&a2.range) {
                        continue;
                    }
                    let s_new = self.scorer.score(&self.ctx, a1) + self.scorer.score(&self.ctx, a2);
                    let delta = s_new - s_old;
                    if delta > best_gain + self.caps.eps {
                        best_gain = delta;
                        best_actions = Some(vec![
                            Action::Delete(a.id),
                            Action::Insert(a1.clone()),
                            Action::Insert(a2.clone()),
                        ]);
                    }
                }
            }
        }

        Ok(best_actions.map(|a| (best_gain, a)))
    }

    // Global, tiny prune sweep (bounded by pmax_parents per draft).
    // Returns (gain, actions).
    async fn best_global_action(
        &self,
        db: &KuramotoDb,
        drafts: &[AvailabilityDraft],
    ) -> Result<Option<(f32, ActionSet)>, StorageError> {
        let mut best_gain = 0.0f32;
        let mut best_actions: Option<ActionSet> = None;
        for d in drafts {
            let parents = find_covering_or_near_parents(db, d, self.caps.pmax_parents).await?;
            for a in parents {
                let draft_old = AvailabilityDraft {
                    level: a.level,
                    range: a.range.clone(),
                    complete: true,
                };
                let s_old = if range_is_empty(&draft_old.range) {
                    0.0
                } else {
                    self.scorer.score(&self.ctx, &draft_old)
                };
                if s_old < 0.0 && replication_guard_ok(db, &a).await {
                    let delta = -s_old;
                    if delta > best_gain + self.caps.eps {
                        best_gain = delta;
                        best_actions = Some(vec![Action::Delete(a.id)]);
                    }
                }
            }
        }
        Ok(best_actions.map(|a| (best_gain, a)))
    }
}

#[async_trait]
impl Optimizer for BasicOptimizer {
    /// **Greedy hill-climb** with cap `caps.max_steps` (default 512).
    /// Accumulates all improving actions into one `ActionSet` and returns it.
    async fn propose(
        &self,
        db: &KuramotoDb,
        overlay: Overlay<'_>,
    ) -> Result<Option<ActionSet>, StorageError> {
        // Normalize drafts: drop empty ranges
        let mut drafts: Vec<AvailabilityDraft> = overlay
            .pending_inserts
            .iter()
            .cloned()
            .filter(|d| !range_is_empty(&d.range))
            .collect();
        let mut deletes: Vec<UuidBytes> = overlay.pending_deletes.to_vec();

        let mut plan: ActionSet = Vec::new();

        // Greedy loop
        for _step in 0..self.caps.max_steps {
            // 1) Best per-draft local action
            let mut best: Option<(f32, ActionSet)> = None;
            for i in 0..drafts.len() {
                if let Some((gain, actions)) =
                    self.best_action_for_draft(db, &overlay, &drafts, i).await?
                {
                    if best
                        .as_ref()
                        .map_or(true, |(g0, _)| gain > *g0 + self.caps.eps)
                    {
                        best = Some((gain, actions));
                    }
                }
            }

            // 2) Best global prune
            if let Some((gain_g, actions_g)) = self.best_global_action(db, &drafts).await? {
                if best
                    .as_ref()
                    .map_or(true, |(g0, _)| gain_g > *g0 + self.caps.eps)
                {
                    best = Some((gain_g, actions_g));
                }
            }

            // 3) If no improvement → stop
            let Some((_gain, actions)) = best else {
                break;
            };

            // 4) Apply the chosen actions to our local overlay view
            for a in &actions {
                match a {
                    Action::Insert(d) => {
                        // Skip empties; avoid trivial duplicates
                        if !range_is_empty(&d.range) && !contains_draft(&drafts, d) {
                            drafts.push(d.clone());
                        }
                    }
                    Action::Delete(id) => {
                        if !deletes.iter().any(|x| x == id) {
                            deletes.push(*id);
                        }
                    }
                }
            }

            // 5) Record in the resulting plan
            plan.extend(actions);

            // 6) Continue; if next iteration finds no improvement, loop ends.
        }

        if plan.is_empty() {
            Ok(None)
        } else {
            Ok(Some(plan))
        }
    }
}

/*──────────────────────────── Helpers ────────────────────────*/

/// Minimal representation of an existing parent availability for neighborhood queries.
#[derive(Debug, Clone)]
struct ExistingParent {
    id: UuidBytes,
    level: u16,
    range: RangeCube,
}

/// Placeholder: find parents that cover or are near a draft (bounded, deterministic order).
async fn find_covering_or_near_parents(
    _db: &KuramotoDb,
    _d: &AvailabilityDraft,
    cap: usize,
) -> Result<Vec<ExistingParent>, StorageError> {
    let _ = cap; // TODO: wire real lookup (range index / cover ledger)
    Ok(Vec::new())
}

/// Placeholder replication guard; implement against your replica ledger.
async fn replication_guard_ok(_db: &KuramotoDb, _a: &ExistingParent) -> bool {
    true
}

/// Enumerate primitive INSERT mutations derived from a draft:
/// - Same-level boundary tweaks by ±step for each dim/min/max
/// - Optional level+1 variants ("promotions") for each produced range, plus an identity parent
fn enumerate_insert_mutations(base: &AvailabilityDraft, caps: Caps) -> Vec<AvailabilityDraft> {
    let mut out = Vec::new();
    let dims = base.mins_len();

    // identity same-level (optional): rely on scorer to reject if useless
    out.push(AvailabilityDraft {
        level: base.level,
        range: base.range.clone(),
        complete: true,
    });

    for dim in 0..dims {
        // four elementary mutations on boundaries
        if let Some(r) = mutate_range_step(&base.range, dim, Boundary::Min, Dir::Outward, caps.step)
        {
            out.push(AvailabilityDraft {
                level: base.level,
                range: r,
                complete: true,
            });
        }
        if let Some(r) = mutate_range_step(&base.range, dim, Boundary::Min, Dir::Inward, caps.step)
        {
            out.push(AvailabilityDraft {
                level: base.level,
                range: r,
                complete: true,
            });
        }
        if let Some(r) = mutate_range_step(&base.range, dim, Boundary::Max, Dir::Outward, caps.step)
        {
            out.push(AvailabilityDraft {
                level: base.level,
                range: r,
                complete: true,
            });
        }
        if let Some(r) = mutate_range_step(&base.range, dim, Boundary::Max, Dir::Inward, caps.step)
        {
            out.push(AvailabilityDraft {
                level: base.level,
                range: r,
                complete: true,
            });
        }
    }

    // level+1 promotions for each produced range (including identity)
    let mut promos = Vec::with_capacity(out.len());
    for d in &out {
        promos.push(AvailabilityDraft {
            level: d.level + 1,
            range: d.range.clone(),
            complete: true,
        });
    }
    out.extend(promos);

    // Cap total to avoid explosion; keep deterministic prefix
    if out.len() > caps.max_variants_per_draft {
        out.truncate(caps.max_variants_per_draft);
    }
    // Enforce non-empty rule; keep only unique ranges at the very end (optional)
    out.into_iter()
        .filter(|d| !range_is_empty(&d.range))
        .collect()
}

// Deduplicate drafts by (level, range bytes, complete)
fn contains_draft(haystack: &[AvailabilityDraft], needle: &AvailabilityDraft) -> bool {
    haystack.iter().any(|d| draft_eq(d, needle))
}
fn draft_eq(a: &AvailabilityDraft, b: &AvailabilityDraft) -> bool {
    a.level == b.level && a.complete == b.complete && ranges_equal(&a.range, &b.range)
}
fn ranges_equal(x: &RangeCube, y: &RangeCube) -> bool {
    if x.mins.len() != y.mins.len() || x.maxs.len() != y.maxs.len() {
        return false;
    }
    for i in 0..x.mins.len() {
        if x.mins[i] != y.mins[i] {
            return false;
        }
    }
    for i in 0..x.maxs.len() {
        if x.maxs[i] != y.maxs[i] {
            return false;
        }
    }
    true
}

/*──────────── Range utilities (local, deterministic) ───────────*/

// Small helpers on RangeCube to keep mutation code tidy
trait RangeExt {
    fn mins_len(&self) -> usize;
}
impl RangeExt for AvailabilityDraft {
    fn mins_len(&self) -> usize {
        self.range.mins.len()
    }
}
impl RangeExt for RangeCube {
    fn mins_len(&self) -> usize {
        self.mins.len()
    }
}

#[derive(Copy, Clone)]
enum Boundary {
    Min,
    Max,
}
#[derive(Copy, Clone)]
enum Dir {
    Inward,
    Outward,
}

/// Returns a new range with a single boundary moved by ±step if valid, else None.
/// Returns a new range with a single boundary moved in lexicographic byte-space.
/// For bytes, we avoid arithmetic and mutate lengths:
/// - Min Outward  (expand ↓): pop one trailing byte if any
/// - Min Inward   (shrink ↑): push 0x00 (minimal lex increase)
/// - Max Outward  (expand ↑): push 0xFF (maximal lex increase)
/// - Max Inward   (shrink ↓): pop one trailing byte if any
fn mutate_range_step(
    base: &RangeCube,
    dim: usize,
    which: Boundary,
    dir: Dir,
    _step: i64, // unused for bytes; kept for API consistency
) -> Option<RangeCube> {
    if dim >= base.mins_len() {
        return None;
    }
    let mut r = base.clone();
    match (which, dir) {
        (Boundary::Min, Dir::Outward) => {
            if !r.mins[dim].is_empty() {
                r.mins[dim].pop();
            }
        }
        (Boundary::Min, Dir::Inward) => {
            r.mins[dim].push(0x00);
        }
        (Boundary::Max, Dir::Outward) => {
            r.maxs[dim].push(0xFF);
        }
        (Boundary::Max, Dir::Inward) => {
            if !r.maxs[dim].is_empty() {
                r.maxs[dim].pop();
            }
        }
    }
    if range_is_empty(&r) { None } else { Some(r) }
}

/// A range is empty if, on any dimension, max <= min in lexicographic byte order.
fn range_is_empty(r: &RangeCube) -> bool {
    let d = r.mins.len().min(r.maxs.len());
    for i in 0..d {
        if r.maxs[i] <= r.mins[i] {
            return true;
        }
    }
    false
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::uuid_bytes::UuidBytes;
    use crate::{KuramotoDb, clock::MockClock};

    // ---------- small builders ----------
    fn draft_bytes(level: u16, mins: &[&[u8]], maxs: &[&[u8]]) -> AvailabilityDraft {
        AvailabilityDraft {
            level,
            complete: true,
            range: RangeCube {
                dims: smallvec![], // unused here
                mins: {
                    let mut s = smallvec![];
                    for m in mins {
                        s.push((*m).to_vec());
                    }
                    s
                },
                maxs: {
                    let mut s = smallvec![];
                    for m in maxs {
                        s.push((*m).to_vec());
                    }
                    s
                },
            },
        }
    }

    async fn fresh_db() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        KuramotoDb::new(dir.path().join("opt.redb").to_str().unwrap(), clock, vec![]).await
    }

    fn peer() -> PeerContext {
        PeerContext {
            peer_id: UuidBytes::new(),
        }
    }

    fn caps() -> Caps {
        Caps {
            pmax_parents: 8,
            nmax_neighbors: 8,
            b_target: 6,
            eps: 0.0,
            step: 1,
            max_variants_per_draft: 32,
            max_steps: 16, // make tests fast/deterministic
        }
    }

    // ---------- scorers ----------
    struct ZeroScorer;
    impl Scorer for ZeroScorer {
        fn score(&self, _ctx: &PeerContext, _a: &AvailabilityDraft) -> f32 {
            0.0
        }
    }

    /// Favors larger (len(max) - len(min)) across dims. Pushes outward expansions.
    struct ByteSpanScorer;
    impl Scorer for ByteSpanScorer {
        fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
            let mut s: i32 = 0;
            let d = a.range.mins.len().min(a.range.maxs.len());
            for i in 0..d {
                s += (a.range.maxs[i].len() as i32) - (a.range.mins[i].len() as i32);
            }
            s as f32
        }
    }

    /// Rewards higher levels; ignores geometry.
    struct LevelFavoringScorer;
    impl Scorer for LevelFavoringScorer {
        fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
            a.level as f32 * 10.0
        }
    }

    // ───────────────────── range utilities ─────────────────────

    #[test]
    fn empty_range_detects_lex_order() {
        // 1D: equal → empty
        let r1 = RangeCube {
            dims: smallvec![],
            mins: smallvec![vec![5u8]],
            maxs: smallvec![vec![5u8]],
        };
        assert!(super::range_is_empty(&r1));

        // 1D: max < min → empty
        let r2 = RangeCube {
            dims: smallvec![],
            mins: smallvec![vec![6u8]],
            maxs: smallvec![vec![5u8]],
        };
        assert!(super::range_is_empty(&r2));

        // 2D: one dim equal → empty
        let r3 = RangeCube {
            dims: smallvec![],
            mins: smallvec![vec![], vec![]],
            maxs: smallvec![vec![10u8], vec![]],
        };
        assert!(super::range_is_empty(&r3));

        // 2D: both dims valid → non-empty
        let r4 = RangeCube {
            dims: smallvec![],
            mins: smallvec![vec![], vec![]],
            maxs: smallvec![vec![10u8], vec![1u8]],
        };
        assert!(!super::range_is_empty(&r4));
    }

    #[test]
    fn mutate_step_operates_in_byte_space() {
        // Give dim0 extra slack so Max Inward won't collapse to equality
        let base = RangeCube {
            dims: smallvec![],
            mins: smallvec![b"a".to_vec(), b"a".to_vec()],
            // dim0 has two bytes of headroom ("a\x02\x00"); dim1 stays modest ("a\x01")
            maxs: smallvec![b"a\x02\x00".to_vec(), b"a\x01".to_vec()],
        };

        // Min Inward: append 0x00 → len increases
        let r = super::mutate_range_step(&base, 0, super::Boundary::Min, super::Dir::Inward, 1)
            .unwrap();
        assert_eq!(r.mins[0].len(), base.mins[0].len() + 1);
        assert!(!super::range_is_empty(&r));

        // Min Outward: pop → len decreases (if non-empty)
        let r = super::mutate_range_step(&base, 1, super::Boundary::Min, super::Dir::Outward, 1)
            .unwrap();
        assert_eq!(r.mins[1].len() + 1, base.mins[1].len());
        assert!(!super::range_is_empty(&r));

        // Max Inward: pop → len decreases
        let r = super::mutate_range_step(&base, 0, super::Boundary::Max, super::Dir::Inward, 1)
            .unwrap();
        assert_eq!(r.maxs[0].len() + 1, base.maxs[0].len());
        assert!(!super::range_is_empty(&r));

        // Max Outward: push 0xFF → len increases
        let r = super::mutate_range_step(&base, 1, super::Boundary::Max, super::Dir::Outward, 1)
            .unwrap();
        assert_eq!(r.maxs[1].len(), base.maxs[1].len() + 1);
        assert!(!super::range_is_empty(&r));
    }

    #[test]
    fn enumerate_insert_mutations_filters_empties_and_caps() {
        let d = draft_bytes(0, &[b"a"], &[b"a\x01"]);
        let mut c = caps();
        c.max_variants_per_draft = 6; // force a small cap
        let variants = super::enumerate_insert_mutations(&d, c);
        assert!(!variants.is_empty());
        assert!(variants.len() <= 6);
        assert!(variants.iter().all(|v| !super::range_is_empty(&v.range)));
    }

    // ───────────────────── optimizer behavior ──────────────────

    #[tokio::test]
    async fn propose_noop_when_scorer_gives_no_improvement() {
        let db = fresh_db().await;
        let opt = BasicOptimizer::new(Box::new(ZeroScorer), peer()).with_caps(caps());

        let base = draft_bytes(0, &[b"a"], &[b"a\x01"]);
        let inserts = vec![base];
        let deletes: Vec<UuidBytes> = vec![];

        let o = Overlay {
            pending_inserts: &inserts,
            pending_deletes: &deletes,
            score: 0.0,
        };

        let got = opt.propose(&db, o).await.unwrap();
        assert!(got.is_none(), "expected NoOp with zero scorer");
    }

    #[tokio::test]
    async fn propose_prefers_outward_expansion_with_byte_span_scorer() {
        let db = fresh_db().await;
        let mut c = caps();
        c.max_steps = 4; // keep it small
        let opt = BasicOptimizer::new(Box::new(ByteSpanScorer), peer()).with_caps(c);

        // Base span = len(max) - len(min) = 1
        let base = draft_bytes(0, &[b"a"], &[b"a\x01"]);
        let inserts = vec![base.clone()];
        let deletes: Vec<UuidBytes> = vec![];

        let o = Overlay {
            pending_inserts: &inserts,
            pending_deletes: &deletes,
            score: 1.0,
        };

        let got = opt.propose(&db, o).await.unwrap();
        let Some(actions) = got else {
            panic!("expected an improving Insert action");
        };

        assert!(
            actions.iter().any(|a| match a {
                Action::Insert(d) => {
                    let mut span: i32 = 0;
                    let dim = d.range.mins.len().min(d.range.maxs.len());
                    for i in 0..dim {
                        span += (d.range.maxs[i].len() as i32) - (d.range.mins[i].len() as i32);
                    }
                    span > 1
                }
                _ => false,
            }),
            "no improving Insert (span) found"
        );

        // With a monotone scorer, hill-climb should return multiple steps (capped)
        assert!(
            actions.len() >= 2,
            "expected multiple greedy steps under hill-climb"
        );
        assert!(
            actions.len() as usize <= c.max_steps,
            "should never exceed the step cap"
        );
    }

    #[tokio::test]
    async fn hill_climb_hits_step_cap_with_level_scorer() {
        let db = fresh_db().await;
        let mut c = caps();
        c.max_steps = 5; // tiny cap for test
        let opt = BasicOptimizer::new(Box::new(LevelFavoringScorer), peer()).with_caps(c);

        let base = draft_bytes(0, &[b"a"], &[b"a\x01"]);
        let inserts = vec![base.clone()];
        let deletes: Vec<UuidBytes> = vec![];

        let o = Overlay {
            pending_inserts: &inserts,
            pending_deletes: &deletes,
            score: 0.0,
        };

        let got = opt.propose(&db, o).await.unwrap();
        let actions = got.expect("should produce actions");
        assert_eq!(
            actions.len(),
            c.max_steps,
            "monotone scorer should drive to the step cap"
        );

        // levels should keep increasing among Insert actions
        let mut last_level = base.level;
        for a in actions {
            if let Action::Insert(d) = a {
                assert!(d.level >= last_level, "levels should not decrease");
                if d.level > last_level {
                    last_level = d.level;
                }
            }
        }
    }
}
