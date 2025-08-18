//! Optimizer: greedy hill-climb (bounded), no neighbor surgery
//! -----------------------------------------------------------
//! - Runs in `before_update` against a virtual overlay (pending inserts/deletes).
//! - **Search policy:** greedy hill-climb up to `caps.max_steps` (default 512):
//!     enumerate candidates → pick highest gain (> eps) → apply to overlay → repeat.
//! - **Candidate families per step:**
//!     (A) Insert-variants from the current drafts (identity, boundary tweaks, promotions)
//!         plus two small macros: Promote+One-Tweak, and Double-Outward per dim.
//!     (B) Prune negatives among a bounded **touched frontier** of existing parents
//!         that overlap any draft seen so far (Delete only; strictly gated by RF).
//! - **What we deliberately DO NOT do now:** no Replace/Split neighbor surgery.
//!   The “economics” (overlap, branching, size, replication bonus) live in the Scorer.
//!
//! Wire real neighbor discovery inside `find_covering_or_near_parents(..)` to unlock
//! touched-set pruning. Until then it’s safe (it returns empty).

use async_trait::async_trait;

use crate::plugins::harmonizer::availability::Availability;
use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::plugins::harmonizer::scorers::Scorer;
use crate::uuid_bytes::UuidBytes;
use crate::{KuramotoDb, storage_error::StorageError};

#[derive(Clone, Debug)]
pub struct PeerContext {
    pub peer_id: UuidBytes,
}

/*──────────────────────────── Types ───────────────────────────*/

#[derive(Debug, Clone)]
pub struct AvailabilityDraft {
    pub level: u16,
    pub range: RangeCube,
    pub complete: bool,
}

impl From<&Availability> for AvailabilityDraft {
    fn from(a: &Availability) -> Self {
        AvailabilityDraft {
            level: a.level,
            range: a.range.clone(),
            complete: a.complete,
        }
    }
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
    /// candidate parents per focus (DB lookup cap for touched frontier expansion)
    pub pmax_parents: usize,
    /// (kept for compat / future sibling exploration; not used here)
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

    /// Enumerate insert variants for a draft, including small “macro” moves.
    fn enumerate_inserts(&self, d: &AvailabilityDraft) -> Vec<AvailabilityDraft> {
        enumerate_insert_mutations(d, self.caps)
    }

    /// Expand the touched frontier by discovering existing parents that overlap `d`.
    async fn expand_touched(
        &self,
        db: &KuramotoDb,
        d: &AvailabilityDraft,
        touched: &mut Touched,
    ) -> Result<(), StorageError> {
        let mut neigh = find_covering_or_near_parents(db, d, self.caps.pmax_parents).await?;
        neigh.sort_by_key(|p| p.id);
        neigh.dedup_by(|a, b| a.id == b.id);
        for p in neigh {
            touched.insert(p);
        }
        Ok(())
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

        // Seed touched frontier from initial drafts
        let mut touched = Touched::new();
        for d in &drafts {
            self.expand_touched(db, d, &mut touched).await?;
        }

        let mut plan: ActionSet = Vec::new();

        // Greedy loop
        for _step in 0..self.caps.max_steps {
            let mut best_gain = f32::NEG_INFINITY;
            let mut best_actions: Option<ActionSet> = None;

            // A) Insert candidates from current drafts (+macros)
            for d in &drafts {
                for cand in self.enumerate_inserts(d) {
                    if range_is_empty(&cand.range) {
                        continue;
                    }
                    let gain = self.scorer.score(&self.ctx, &cand);
                    if gain > self.caps.eps && gain > best_gain + self.caps.eps {
                        best_gain = gain;
                        best_actions = Some(vec![Action::Insert(cand)]);
                    }
                }
            }

            // B) Prune negatives in the touched frontier (Delete only; coverage-safe)
            for p in touched.iter() {
                // Coverage constraint: never delete leaves until eviction is implemented
                if p.level == 0 {
                    continue;
                }

                let pd = AvailabilityDraft {
                    level: p.level,
                    range: p.range.clone(),
                    complete: true,
                };
                let s_old = if range_is_empty(&pd.range) {
                    0.0
                } else {
                    self.scorer.score(&self.ctx, &pd)
                };

                if s_old < 0.0 {
                    let gain = -s_old;
                    if gain > self.caps.eps && gain > best_gain + self.caps.eps {
                        best_gain = gain;
                        best_actions = Some(vec![Action::Delete(p.id)]);
                    }
                }
            }

            // Stop if we found nothing improving
            let Some(actions) = best_actions else { break };

            // Apply actions to overlay + frontier
            for a in &actions {
                match a {
                    Action::Insert(d) => {
                        // Skip empties; avoid trivial duplicates
                        if !range_is_empty(&d.range) && !contains_draft(&drafts, d) {
                            self.expand_touched(db, d, &mut touched).await?;
                            drafts.push(d.clone());
                        }
                    }
                    Action::Delete(id) => {
                        if !deletes.iter().any(|x| x == id) {
                            deletes.push(*id);
                        }
                        touched.remove(*id);
                    }
                }
            }

            // Record in final plan and continue
            plan.extend(actions);
        }

        if plan.is_empty() {
            Ok(None)
        } else {
            Ok(Some(plan))
        }
    }
}

/*──────────────────────────── Touched frontier ─────────────────*/

#[derive(Debug, Clone)]
struct ExistingParent {
    id: UuidBytes,
    level: u16,
    range: RangeCube,
    peer_id: UuidBytes,
}

#[derive(Default)]
struct Touched {
    parents: Vec<ExistingParent>,
}
impl Touched {
    fn new() -> Self {
        Self {
            parents: Vec::new(),
        }
    }
    fn insert(&mut self, p: ExistingParent) {
        if !self.parents.iter().any(|x| x.id == p.id) {
            self.parents.push(p);
        }
    }
    fn remove(&mut self, id: UuidBytes) {
        self.parents.retain(|p| p.id != id);
    }
    fn iter(&self) -> impl Iterator<Item = &ExistingParent> {
        self.parents.iter()
    }
}

/// Placeholder: find parents that cover or are near a draft (bounded, deterministic order).
async fn find_covering_or_near_parents(
    db: &KuramotoDb,
    d: &AvailabilityDraft,
    cap: usize,
) -> Result<Vec<ExistingParent>, StorageError> {
    let mut out = Vec::<ExistingParent>::new();
    let all: Vec<Availability> = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;
    for a in all {
        // Consider same-level or higher "parents" that overlap the draft
        if a.level >= d.level && a.range.overlaps(&d.range) {
            out.push(ExistingParent {
                id: a.key,
                level: a.level,
                range: a.range.clone(),
                peer_id: a.peer_id,
            });
        }
    }
    // Prefer coarser nodes first (so pruning considers bigger wins), then larger volume
    out.sort_by(|x, y| {
        y.level
            .cmp(&x.level)
            .then(y.range.approx_volume().cmp(&x.range.approx_volume()))
            .then(y.id.as_bytes().cmp(x.id.as_bytes())) // deterministic tie-break
    });
    if out.len() > cap {
        out.truncate(cap);
    }
    Ok(out)
}

/*──────────────── Insert enumeration (+ two macros) ───────────*/

/// Enumerate primitive INSERT mutations derived from a draft:
/// - Same-level boundary tweaks by ±step for each dim/min/max
/// - Promotions (level+1) for each produced range
/// - Macro 1: Promote+One-Tweak (applies one extra outward tweak to a promotion)
/// - Macro 2: Double-Outward per dim (min-outward + max-outward together)
fn enumerate_insert_mutations(base: &AvailabilityDraft, caps: Caps) -> Vec<AvailabilityDraft> {
    let dims = base.mins_len();

    let mut out: Vec<AvailabilityDraft> = Vec::with_capacity(1 + 4 * dims);

    // 0) identity same-level (rely on scorer to reject if useless)
    out.push(AvailabilityDraft {
        level: base.level,
        range: base.range.clone(),
        complete: true,
    });

    // 1) per-dim boundary tweaks (same level)
    for dim in 0..dims {
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

        // 2) Macro: Double-Outward for this dim (same level)
        if let Some(r1) =
            mutate_range_step(&base.range, dim, Boundary::Min, Dir::Outward, caps.step)
        {
            if let Some(r2) = mutate_range_step(&r1, dim, Boundary::Max, Dir::Outward, caps.step) {
                out.push(AvailabilityDraft {
                    level: base.level,
                    range: r2,
                    complete: true,
                });
            }
        }
    }

    // Collect the same set again as promotions (level+1)
    let mut promos: Vec<AvailabilityDraft> = Vec::with_capacity(out.len());
    for d in &out {
        promos.push(AvailabilityDraft {
            level: d.level + 1,
            range: d.range.clone(),
            complete: true,
        });
    }

    // Macro 1 on promotions: for each promo, apply a single outward max tweak per dim
    let mut promo_plus_one: Vec<AvailabilityDraft> = Vec::new();
    for p in &promos {
        for dim in 0..dims {
            if let Some(r) =
                mutate_range_step(&p.range, dim, Boundary::Max, Dir::Outward, caps.step)
            {
                promo_plus_one.push(AvailabilityDraft {
                    level: p.level,
                    range: r,
                    complete: true,
                });
            }
        }
    }

    out.extend(promos);
    out.extend(promo_plus_one);

    // Enforce non-empty rule; dedupe; cap
    let mut uniq: Vec<AvailabilityDraft> = Vec::with_capacity(out.len());
    for d in out.into_iter().filter(|d| !range_is_empty(&d.range)) {
        if !contains_draft(&uniq, &d) {
            uniq.push(d);
        }
        if uniq.len() >= caps.max_variants_per_draft {
            break;
        }
    }
    uniq
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
/// Operates in **lexicographic byte space**:
/// - Min Outward  (expand ↓): pop one trailing byte if any
/// - Min Inward   (shrink ↑): push 0x00
/// - Max Outward  (expand ↑): push 0xFF
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

/// A range is empty if, on any dimension, max ≤ min in lexicographic byte order.
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
        let db = KuramotoDb::new(
            dir.path().join("opt.redb").to_str().unwrap(),
            clock,
            vec![], // tests don't need plugins here
        )
        .await;

        // IMPORTANT: the optimizer scans Availability; make sure the table exists.
        db.create_table_and_indexes::<Availability>()
            .expect("create Availability tables");

        db
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
            max_steps: 16, // keep tests fast/deterministic
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

    /// Rewards **promotion+growth**, penalizes pure promotion or pure growth.
    struct PromoPlusGrowthScorer;
    impl Scorer for PromoPlusGrowthScorer {
        fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
            let span: i32 = {
                let d = a.range.mins.len().min(a.range.maxs.len());
                let mut v = 0;
                for i in 0..d {
                    v += (a.range.maxs[i].len() as i32) - (a.range.mins[i].len() as i32);
                }
                v
            };
            if a.level >= 1 && span >= 2 {
                10.0 // promote+grow is good
            } else if a.level >= 1 {
                -1.0 // promotion without growth is bad
            } else if span >= 2 {
                -1.0 // growth without promotion is bad
            } else {
                0.0
            }
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
    async fn greedy_climb_takes_multiple_steps_and_respects_cap() {
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
            panic!("expected an improving Insert action sequence");
        };

        // At least one Insert should increase span over base
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
    async fn macro_promotion_plus_tweak_is_reachable_without_lookahead() {
        let db = fresh_db().await;
        let mut c = caps();
        c.max_steps = 8;
        let opt = BasicOptimizer::new(Box::new(PromoPlusGrowthScorer), peer()).with_caps(c);

        // Base span ~ 1 (tiny)
        let base = draft_bytes(0, &[b"a"], &[b"a\x01"]);
        let inserts = vec![base.clone()];
        let deletes: Vec<UuidBytes> = vec![];

        let o = Overlay {
            pending_inserts: &inserts,
            pending_deletes: &deletes,
            score: 0.0,
        };

        let got = opt
            .propose(&db, o)
            .await
            .unwrap()
            .expect("should produce actions");

        // We should see at least one promoted + grown insert (level >=1 and span >=2)
        let mut ok = false;
        for a in got {
            if let Action::Insert(d) = a {
                let span: i32 = {
                    let dim = d.range.mins.len().min(d.range.maxs.len());
                    let mut v = 0;
                    for i in 0..dim {
                        v += (d.range.maxs[i].len() as i32) - (d.range.mins[i].len() as i32);
                    }
                    v
                };
                if d.level >= 1 && span >= 2 {
                    ok = true;
                    break;
                }
            }
        }
        assert!(
            ok,
            "promotion+growth should be reachable via macro variants"
        );
    }
}
