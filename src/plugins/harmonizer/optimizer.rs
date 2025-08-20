//! Optimizer: greedy hill-climb (bounded), no neighbor surgery
//! -----------------------------------------------------------
//! - Runs in `before_update` against a virtual overlay (pending inserts/deletes).
//! - **Search policy:** greedy hill-climb up to `caps.max_steps` (default 512):
//!     enumerate candidates (with adaptive momentum over prefix orders)
//!     → pick highest gain (> eps) → apply to overlay → repeat.
//! - **Candidate families per step:**
//!     (A) Insert-variants from the current drafts (identity + prefix-succ buckets,
//!         promotions).
//!     (B) Prune negatives among a bounded **touched frontier** of existing parents
//!         that overlap any draft seen so far (Delete only; coverage-gated).
//! - **No neighbor surgery yet** (no Replace/Split).
//!
//! Range mutation no longer relies on push/pop of 0x00/0xFF (which gets stuck in lexicographic
//! buckets). Instead we jump via **prefix successor**:
//!   [P, succ(P))  where P is a truncated prefix of the current min.
//! This allows coarse hops like  [ [0x0f], [0x10) )  (i.e., "a" → "b"), spanning siblings.

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
    /// mutation step (kept for API compat; unused in prefix mode)
    pub step: i64,
    /// cap enumerated insert variants per draft
    pub max_variants_per_draft: usize,
    /// hill-climb step cap — keep taking best greedy steps until none or this cap
    pub max_steps: usize,
    /// Max number of sibling buckets to span per dimension (≥1). 6–8 works well.
    pub max_bucket_span: usize,
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
            max_bucket_span: 6,
        }
    }
}

/*──────────────────────── Momentum (adaptive order) ──────────*/

#[derive(Clone, Debug)]
pub struct Momentum {
    /// 0 = micro (no truncation), 1 = hop at 1-byte prefix, 2 = hop at 2-byte prefix, ...
    pub order: usize,
    /// EMA smoothing for accepted actions (0..1]; higher = adapt faster.
    pub alpha: f32,
    /// Safety cap on order.
    pub max_order: usize,
}

impl Default for Momentum {
    fn default() -> Self {
        Self {
            order: 0,
            alpha: 0.35,
            max_order: 8,
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
    pub momentum: Momentum,
}

impl BasicOptimizer {
    pub fn new(scorer: Box<dyn Scorer>, ctx: PeerContext) -> Self {
        Self {
            scorer,
            ctx,
            caps: Caps::default(),
            momentum: Momentum::default(),
        }
    }
    pub fn with_caps(mut self, caps: Caps) -> Self {
        self.caps = caps;
        self
    }
    pub fn with_momentum(mut self, m: Momentum) -> Self {
        self.momentum = m;
        self
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

/*──────────────────────── prefix utilities ───────────────────*/

/// Return the length of the common prefix of two byte strings.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    for i in 0..n {
        if a[i] != b[i] {
            return i;
        }
    }
    n
}

/// The minimal byte vector strictly greater than any string starting with `p`.
/// Example: succ_prefix([0x0f]) = [0x10]; succ_prefix([0xff]) = None (overflow).
fn succ_prefix(p: &[u8]) -> Option<Vec<u8>> {
    if p.is_empty() {
        return Some(vec![0x01]); // [] → [0x01] (arbitrary but monotone)
    }
    let mut out = p.to_vec();
    for i in (0..out.len()).rev() {
        if out[i] != 0xff {
            out[i] = out[i].wrapping_add(1);
            out.truncate(i + 1); // drop lower-order suffix → minimal successor
            return Some(out);
        }
    }
    None // all 0xff → no finite successor
}

/// immediate predecessor of a prefix (mirror of succ_prefix)
fn pred_prefix(p: &[u8]) -> Option<Vec<u8>> {
    if p.is_empty() {
        return None;
    }
    let mut out = p.to_vec();
    for i in (0..out.len()).rev() {
        if out[i] != 0x00 {
            out[i] = out[i].wrapping_sub(1);
            out.truncate(i + 1);
            return Some(out);
        }
    }
    None
}

/// apply succ_prefix n times
fn succ_n(mut p: Vec<u8>, n: usize) -> Option<Vec<u8>> {
    for _ in 0..n {
        p = succ_prefix(&p)?;
    }
    Some(p)
}

/// apply pred_prefix n times
fn pred_n(mut p: Vec<u8>, n: usize) -> Option<Vec<u8>> {
    for _ in 0..n {
        p = pred_prefix(&p)?;
    }
    Some(p)
}

/// Truncate to the first `keep` bytes.
fn prefix_truncate(v: &[u8], keep: usize) -> Vec<u8> {
    if keep >= v.len() {
        v.to_vec()
    } else {
        v[..keep].to_vec()
    }
}

/*──────────────────── candidate enumeration ───────────────────*/

/// Enumerate INSERT mutations at a given **prefix order** (0 = micro).
/// For each dim:
///   - Build the coarse bucket: let P = min truncated by `ord` bytes; candidate = [P, succ(P)).
///     Also include a promoted version (level+1).
///   - Add a conservative shrink on max via truncation (for scorer to reject/accept).
fn enumerate_insert_mutations_with_order(
    base: &AvailabilityDraft,
    ord: usize,
    caps: Caps,
) -> Vec<AvailabilityDraft> {
    let dims = base.mins_len();
    let mut out: Vec<AvailabilityDraft> = Vec::with_capacity(1 + 16 * dims);

    // Identity (so we can early-exit if it's the argmax)
    out.push(AvailabilityDraft {
        level: base.level,
        range: base.range.clone(),
        complete: true,
    });

    let max_span = caps.max_bucket_span.max(1); // default 6 if you added the cap

    for dim in 0..dims {
        let min0 = &base.range.mins[dim];
        let max0 = &base.range.maxs[dim];

        // Truncate MIN by 'ord' to get the anchor prefix P
        let keep = min0.len().saturating_sub(ord);
        let p = prefix_truncate(min0, keep);

        // Basic coarse bucket [P, succ(P))
        if let Some(p_succ) = succ_prefix(&p) {
            let mut r = base.range.clone();
            r.mins[dim] = p.clone();
            r.maxs[dim] = p_succ.clone();
            if !range_is_empty(&r) {
                out.push(AvailabilityDraft {
                    level: base.level,
                    range: r.clone(),
                    complete: true,
                });
                out.push(AvailabilityDraft {
                    level: base.level + 1,
                    range: r,
                    complete: true,
                });
            }
        }

        // Immediate left neighbor [pred(P), P)
        if let Some(p_pred) = pred_prefix(&p) {
            let mut r = base.range.clone();
            r.mins[dim] = p_pred.clone();
            r.maxs[dim] = p.clone();
            if !range_is_empty(&r) {
                out.push(AvailabilityDraft {
                    level: base.level,
                    range: r.clone(),
                    complete: true,
                });
                out.push(AvailabilityDraft {
                    level: base.level + 1,
                    range: r,
                    complete: true,
                });
            }
        }

        // Multi-bucket spans:
        //  - right-wide:  [P, succ^w(P))              (covers P .. P+w-1)
        //  - left-wide:   [pred^(w-1)(P), P)          (covers P-w+1 .. P-1)
        //  - symmetric:   [pred^t(P), succ^t(P))      (covers P-t .. P+t-1)  (width = 2t)
        for w in 2..=max_span {
            // Right-wide
            if let Some(p_right) = succ_n(p.clone(), w) {
                let mut r = base.range.clone();
                r.mins[dim] = p.clone();
                r.maxs[dim] = p_right.clone();
                if !range_is_empty(&r) {
                    out.push(AvailabilityDraft {
                        level: base.level,
                        range: r.clone(),
                        complete: true,
                    });
                    out.push(AvailabilityDraft {
                        level: base.level + 1,
                        range: r,
                        complete: true,
                    });
                }
            }
            // Left-wide
            if let Some(p_left) = pred_n(p.clone(), w - 1) {
                let mut r = base.range.clone();
                r.mins[dim] = p_left.clone();
                r.maxs[dim] = p.clone();
                if !range_is_empty(&r) {
                    out.push(AvailabilityDraft {
                        level: base.level,
                        range: r.clone(),
                        complete: true,
                    });
                    out.push(AvailabilityDraft {
                        level: base.level + 1,
                        range: r,
                        complete: true,
                    });
                }
            }
        }

        // Symmetric widening (even widths 2,4,6...)
        for t in 1..=((max_span) / 2) {
            if let (Some(left), Some(right)) = (pred_n(p.clone(), t), succ_n(p.clone(), t)) {
                let mut r = base.range.clone();
                r.mins[dim] = left.clone();
                r.maxs[dim] = right.clone();
                if !range_is_empty(&r) {
                    out.push(AvailabilityDraft {
                        level: base.level,
                        range: r.clone(),
                        complete: true,
                    });
                    out.push(AvailabilityDraft {
                        level: base.level + 1,
                        range: r,
                        complete: true,
                    });
                }
            }
        }

        // (Optional) conservative shrink on max, as you had before
        let keep_max = max0.len().saturating_sub(ord);
        let max_trunc = prefix_truncate(max0, keep_max);
        if max_trunc > *min0 {
            let mut r = base.range.clone();
            r.maxs[dim] = max_trunc.clone();
            if !range_is_empty(&r) {
                out.push(AvailabilityDraft {
                    level: base.level,
                    range: r.clone(),
                    complete: true,
                });
                out.push(AvailabilityDraft {
                    level: base.level + 1,
                    range: r,
                    complete: true,
                });
            }
        }
    }

    // Dedupe & cap
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

#[async_trait]
impl Optimizer for BasicOptimizer {
    /// **Greedy hill-climb** with adaptive momentum over prefix order.
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
        eprintln!(
            "opt.start: drafts={} deletes={} max_steps={} ord0={}",
            drafts.len(),
            deletes.len(),
            self.caps.max_steps,
            self.momentum.order.min(self.momentum.max_order)
        );

        // Seed touched frontier from initial drafts
        let mut touched = Touched::new();
        for d in &drafts {
            self.expand_touched(db, d, &mut touched).await?;
        }

        let mut plan: ActionSet = Vec::new();

        // Momentum state (per climb)
        let mut ord_ema = self.momentum.order as f32;
        let alpha = self.momentum.alpha;
        let mut ord = self.momentum.order.min(self.momentum.max_order);
        let mut neighborhood_radius: usize = 1;

        // Greedy loop
        for _step in 0..self.caps.max_steps {
            let mut best_gain = f32::NEG_INFINITY;
            let mut best: Option<(ActionSet, usize)> = None; // (actions, order_used)

            // Try orders around current momentum (symmetric neighborhood)
            let mut orders_to_try = Vec::<usize>::new();
            for delta in 0..=neighborhood_radius {
                if ord >= delta {
                    orders_to_try.push(ord - delta);
                }
                if delta != 0 && ord + delta <= self.momentum.max_order {
                    orders_to_try.push(ord + delta);
                }
            }
            orders_to_try.sort_unstable();
            orders_to_try.dedup();

            for &ord_try in &orders_to_try {
                // A) Insert candidates from current drafts (+ promotions)
                for d in &drafts {
                    for cand in enumerate_insert_mutations_with_order(d, ord_try, self.caps) {
                        if range_is_empty(&cand.range) {
                            continue;
                        }
                        let gain = self.scorer.score(&self.ctx, &cand);
                        if gain > self.caps.eps && gain > best_gain + self.caps.eps {
                            best_gain = gain;
                            best = Some((vec![Action::Insert(cand)], ord_try));
                        }
                    }
                }

                // B) Prune negatives in touched frontier (Delete only; coverage-safe)
                for p in touched.iter() {
                    // Coverage constraint: never delete leaves until eviction is implemented
                    if p.level == 0 {
                        continue;
                    }
                    if p.peer_id != self.ctx.peer_id {
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
                            best = Some((vec![Action::Delete(p.id)], ord_try));
                        }
                    }
                }
            }

            // No improvement with current neighborhood → widen search a bit; if already wide, stop.
            let Some((actions, ord_used)) = best else {
                if neighborhood_radius < 3 && ord < self.momentum.max_order {
                    neighborhood_radius += 1;
                    continue;
                }
                break;
            };

            if actions.len() == 1 {
                if let Action::Insert(ref d) = actions[0] {
                    if is_identity(d, &drafts) {
                        // Nothing left to do; return whatever we’ve accumulated so far
                        return if plan.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(plan))
                        };
                    }
                }
            }

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

            // Update momentum (EMA) toward the order that produced the accepted step
            ord_ema = alpha * (ord_used as f32) + (1.0 - alpha) * ord_ema;
            ord = ord_ema.round().clamp(0.0, self.momentum.max_order as f32) as usize;
            neighborhood_radius = 1; // reset after a successful step

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

/*──────────────────── Helpers: equality/dedup ─────────────────*/

fn contains_draft(haystack: &[AvailabilityDraft], needle: &AvailabilityDraft) -> bool {
    haystack.iter().any(|d| draft_eq(d, needle))
}

fn draft_eq(a: &AvailabilityDraft, b: &AvailabilityDraft) -> bool {
    a.level == b.level && a.complete == b.complete && ranges_equal(&a.range, &b.range)
}

#[inline]
fn is_identity(cand: &AvailabilityDraft, drafts: &[AvailabilityDraft]) -> bool {
    // identity = same (level, range, complete) as something already in `drafts`
    contains_draft(drafts, cand)
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
            max_variants_per_draft: 64,
            max_steps: 16, // keep tests fast/deterministic
            max_bucket_span: 6,
        }
    }

    // ---------- test scorers ----------
    struct ZeroScorer;
    impl Scorer for ZeroScorer {
        fn score(&self, _ctx: &PeerContext, _a: &AvailabilityDraft) -> f32 {
            0.0
        }
    }

    /// Rewards **coarser buckets** (smaller common prefix between min and max) and promotions.
    struct CoarseBucketScorer;
    impl Scorer for CoarseBucketScorer {
        fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
            let d = a.range.mins.len().min(a.range.maxs.len());
            let mut s = 0.0f32;
            for i in 0..d {
                let cp = super::common_prefix_len(&a.range.mins[i], &a.range.maxs[i]);
                // Smaller cp → coarser bucket → higher reward
                s += (16_i32 - (cp as i32)).max(0) as f32;
            }
            // Prefer higher levels a bit
            s += (a.level as f32) * 0.5;
            s
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
    fn order_one_enumeration_spans_adjacent_bucket() {
        // Base leaf around 0x0f: [ [0x0f,0x00], [0x0f,0x01) )
        let base = draft_bytes(0, &[&[0x0f, 0x00]], &[&[0x0f, 0x01]]);
        let mut c = caps();
        c.max_variants_per_draft = 64;

        let variants = super::enumerate_insert_mutations_with_order(&base, 1, c);

        // Expect a candidate that hops to [ [0x0f], [0x10) )
        let wanted_min = vec![0x0f];
        let wanted_max = vec![0x10];
        assert!(
            variants
                .iter()
                .any(|d| d.range.mins[0] == wanted_min && d.range.maxs[0] == wanted_max),
            "expected a prefix-succ hop from [0x0f] to [0x10)"
        );
        // And at least one promoted version should exist too
        assert!(
            variants.iter().any(|d| d.level >= 1
                && d.range.mins[0] == wanted_min
                && d.range.maxs[0] == wanted_max),
            "expected a promoted variant of the coarse bucket"
        );
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
    async fn greedy_climb_uses_coarse_buckets_and_respects_cap() {
        let db = fresh_db().await;
        let mut c = caps();
        c.max_steps = 4; // keep it small
        let opt = BasicOptimizer::new(Box::new(CoarseBucketScorer), peer())
            .with_caps(c)
            .with_momentum(Momentum {
                order: 0,
                alpha: 0.5,
                max_order: 8,
            });

        // Base leaf ~ [15.00, 15.01)
        let base = draft_bytes(0, &[&[15u8, 0x00]], &[&[15u8, 0x01]]);
        let inserts = vec![base.clone()];
        let deletes: Vec<UuidBytes> = vec![];

        let o = Overlay {
            pending_inserts: &inserts,
            pending_deletes: &deletes,
            score: 0.0,
        };

        let got = opt.propose(&db, o).await.unwrap();
        let Some(actions) = got else {
            panic!("expected an improving Insert action sequence");
        };

        // We should see at least one insertion that spans the adjacent bucket [15]..[16)
        let mut saw_bucket_hop = false;
        for a in &actions {
            if let Action::Insert(d) = a {
                if d.range.mins[0] == vec![15u8] && d.range.maxs[0] == vec![16u8] {
                    saw_bucket_hop = true;
                }
            }
        }
        assert!(saw_bucket_hop, "expected a coarse bucket hop [15]..[16)");

        // Greedy climb should return multiple steps (capped)
        assert!(
            actions.len() >= 1,
            "expected at least one improving step under hill-climb"
        );
        assert!(
            actions.len() as usize <= c.max_steps,
            "should never exceed the step cap"
        );
    }

    #[tokio::test]
    async fn early_exit_on_identity_proposal() {
        let db = fresh_db().await;

        // Force the enumerator to offer only the identity variant
        let mut c = caps();
        c.max_steps = 8;
        c.max_variants_per_draft = 1;

        let opt = BasicOptimizer::new(Box::new(CoarseBucketScorer), peer()).with_caps(c);

        // A simple 1D leaf; identity exists and is "good" under ByteSpanScorer
        let base = draft_bytes(0, &[b"\x0f"], &[b"\x0f\x01"]); // [15]..[15,1)
        let inserts = vec![base];
        let deletes: Vec<UuidBytes> = vec![];

        let o = Overlay {
            pending_inserts: &inserts,
            pending_deletes: &deletes,
            score: 1.0,
        };

        // Before the fix this would return 512 identity inserts;
        // now it should early-exit with None (no change).
        let got = opt.propose(&db, o).await.unwrap();
        assert!(got.is_none(), "should early-exit on identity proposal");
    }
}
