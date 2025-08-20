//! Optimizer: greedy hill-climb (bounded), additive mutations only
//! ---------------------------------------------------------------
//! - No Overlay type. The API is propose(&db, &[AvailabilityDraft]) -> ActionSet.
//! - Candidate generation is *additive on bytes-as-big-endian integers*:
//!     min  ← min  - step        (expand left)
//!     max  ← max  + step        (expand right)
//!     both ← (min - step, max + step)
//!   with `step` tried from {1, EMA, 10*EMA} (deduped; EMA is a moving average of accepted steps).
//! - We always include **promotion** variants (level+1) of each candidate.
//! - Deletes: scan DB for same-peer parents (level ≥ 1) overlapping any current draft.
//!   If their *standalone* score is negative, deleting yields positive gain.
//! - Identity early-exit: if the best action is an Insert equal to an existing draft, we stop.

use async_trait::async_trait;

use crate::plugins::harmonizer::availability::Availability;
use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::plugins::harmonizer::scorers::Scorer;
use crate::uuid_bytes::UuidBytes;
use crate::{KuramotoDb, storage_error::StorageError};

/*──────────────────────── Context & core types ───────────────────────*/

#[derive(Clone, Debug)]
pub struct PeerContext {
    pub peer_id: UuidBytes,
}

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
pub type ActionSet = Vec<Action>;

/*──────────────────────── Config & Momentum ──────────────────────────*/

#[derive(Debug, Clone, Copy)]
pub struct Caps {
    /// improvement threshold (strictly greater than this is “better”)
    pub eps: f32,
    /// max greedy steps per call
    pub max_steps: usize,
    /// cap total variants per draft per iteration
    pub max_variants_per_draft: usize,
}
impl Default for Caps {
    fn default() -> Self {
        Self {
            eps: 0.0,
            max_steps: 512,
            max_variants_per_draft: 64,
        }
    }
}

/// Simple momentum over **step size** (EMA in “lex distance” units).
#[derive(Clone, Debug)]
pub struct Momentum {
    pub ema: f64,   // current moving average step
    pub alpha: f64, // smoothing factor (0,1]
}
impl Default for Momentum {
    fn default() -> Self {
        Self {
            ema: 1.0,
            alpha: 0.5,
        }
    }
}

/*──────────────────────── Optimizer trait ────────────────────────────*/

#[async_trait]
pub trait Optimizer: Send + Sync {
    async fn propose(
        &self,
        db: &KuramotoDb,
        drafts: &[AvailabilityDraft],
    ) -> Result<Option<ActionSet>, StorageError>;
}

/*──────────────────────── BasicOptimizer ─────────────────────────────*/

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
}

#[async_trait]
impl Optimizer for BasicOptimizer {
    async fn propose(
        &self,
        db: &KuramotoDb,
        initial_drafts: &[AvailabilityDraft],
    ) -> Result<Option<ActionSet>, StorageError> {
        // Normalize drafts: drop empties
        let mut drafts: Vec<AvailabilityDraft> = initial_drafts
            .iter()
            .cloned()
            .filter(|d| !range_is_empty(&d.range))
            .collect();

        println!(
            "opt.start: drafts={} max_steps={} ema={:.3} alpha={:.2}",
            drafts.len(),
            self.caps.max_steps,
            self.momentum.ema,
            self.momentum.alpha
        );

        let mut plan: ActionSet = Vec::new();
        let mut ema = self.momentum.ema;

        for step_idx in 0..self.caps.max_steps {
            let step_base = ema.max(1.0).round() as u128;
            let mut step_candidates: Vec<u128> = vec![1, step_base, step_base.saturating_mul(10)];
            step_candidates.sort_unstable();
            step_candidates.dedup();

            let mut best_gain = f32::NEG_INFINITY;
            let mut best_action: Option<(Action, Option<u128>)> = None; // (action, used_step for EMA)

            // A) INSERTS from current drafts with additive mutations (+ promotion)
            for d in &drafts {
                let variants = enumerate_additive_mutations(d, &step_candidates, self.caps);
                for cand in variants {
                    if range_is_empty(&cand.draft.range) {
                        continue;
                    }
                    let s = self.scorer.score(&self.ctx, &cand.draft);
                    if s > self.caps.eps && s > best_gain + self.caps.eps {
                        best_gain = s;
                        best_action = Some((Action::Insert(cand.draft), Some(cand.used_step)));
                    }
                }
            }

            // B) DELETEs: scan parents (same peer, level ≥ 1) that overlap any draft; delete negatives
            {
                let parents = db.range_by_pk::<Availability>(&[], &[0xFF], None).await?;
                for p in parents {
                    if p.peer_id != self.ctx.peer_id || p.level == 0 {
                        continue;
                    }
                    if !overlaps_any(&p.range, &drafts) {
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
                            best_action = Some((Action::Delete(p.key), None));
                        }
                    }
                }
            }

            // Stop if no improving action
            let Some((action, used_step)) = best_action else {
                println!("opt.stop: no improving action at step {}", step_idx);
                break;
            };

            // Early-exit on identity insert
            if let Action::Insert(ref d) = action {
                if is_identity(d, &drafts) {
                    println!("opt.exit: identity chosen at step {}", step_idx);
                    return if plan.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(plan))
                    };
                }
            }

            // Apply action locally and track momentum
            match &action {
                Action::Insert(d) => {
                    if !contains_draft(&drafts, d) && !range_is_empty(&d.range) {
                        println!(
                            "opt.accept: INSERT lvl={} mins0={:?} maxs0={:?}",
                            d.level,
                            d.range.mins.get(0).cloned().unwrap_or_default(),
                            d.range.maxs.get(0).cloned().unwrap_or_default()
                        );
                        drafts.push(d.clone());
                        if let Some(us) = used_step {
                            // EMA update
                            ema = self.momentum.alpha * (us as f64)
                                + (1.0 - self.momentum.alpha) * ema;
                        }
                    }
                }
                Action::Delete(id) => {
                    println!("opt.accept: DELETE id={:?}", id);
                    // We only *propose* deletions; the caller applies them atomically with inserts.
                }
            }

            plan.push(action);
        }

        if plan.is_empty() {
            Ok(None)
        } else {
            Ok(Some(plan))
        }
    }
}

/*──────────────────────── Additive mutation helpers ──────────────────*/

#[derive(Clone)]
struct Cand {
    draft: AvailabilityDraft,
    used_step: u128, // magnitude of the additive delta we applied
}

/// For each step in `steps`, produce additive variants:
///   - expand left  (min -= step)
///   - expand right (max += step)
///   - expand both  (min -= step, max += step)
/// For each produced range we also add a **promoted** copy (level+1).
fn enumerate_additive_mutations(base: &AvailabilityDraft, steps: &[u128], caps: Caps) -> Vec<Cand> {
    let dims = base.range.mins.len().min(base.range.maxs.len());
    let mut out: Vec<Cand> = Vec::with_capacity(1 + 6 * dims);

    // Identity (kept so we can early-exit if it's the argmax)
    out.push(Cand {
        draft: AvailabilityDraft {
            level: base.level,
            range: base.range.clone(),
            complete: true,
        },
        used_step: 0,
    });

    out.push(Cand {
        draft: AvailabilityDraft {
            level: base.level + 1,
            range: base.range.clone(),
            complete: true,
        },
        used_step: 0,
    });

    for &st in steps {
        if st == 0 {
            continue;
        }
        for dim in 0..dims {
            // min -= st
            if let Some(min2) = be_add_signed(&base.range.mins[dim], -(st as i128)) {
                let mut r = base.range.clone();
                r.mins[dim] = min2;
                if !range_is_empty(&r) {
                    push_both_levels(&mut out, &r, base.level, st);
                }
            }
            // max += st
            if let Some(max2) = be_add_signed(&base.range.maxs[dim], st as i128) {
                let mut r = base.range.clone();
                r.maxs[dim] = max2;
                if !range_is_empty(&r) {
                    push_both_levels(&mut out, &r, base.level, st);
                }
            }
            // both sides
            if let (Some(min2), Some(max2)) = (
                be_add_signed(&base.range.mins[dim], -(st as i128)),
                be_add_signed(&base.range.maxs[dim], st as i128),
            ) {
                let mut r = base.range.clone();
                r.mins[dim] = min2;
                r.maxs[dim] = max2;
                if !range_is_empty(&r) {
                    push_both_levels(&mut out, &r, base.level, st);
                }
            }
        }
    }

    // Dedupe and cap
    let mut uniq: Vec<Cand> = Vec::with_capacity(out.len());
    for c in out.into_iter() {
        if !range_is_empty(&c.draft.range) && !contains_draft_cand(&uniq, &c.draft) {
            uniq.push(c);
        }
        if uniq.len() >= caps.max_variants_per_draft {
            break;
        }
    }
    uniq
}

fn push_both_levels(out: &mut Vec<Cand>, r: &RangeCube, level: u16, used: u128) {
    out.push(Cand {
        draft: AvailabilityDraft {
            level,
            range: r.clone(),
            complete: true,
        },
        used_step: used,
    });
    out.push(Cand {
        draft: AvailabilityDraft {
            level: level + 1,
            range: r.clone(),
            complete: true,
        },
        used_step: used,
    });
}

/*──────────────────────── Equality / identity ───────────────────────*/

fn contains_draft(haystack: &[AvailabilityDraft], needle: &AvailabilityDraft) -> bool {
    haystack.iter().any(|d| draft_eq(d, needle))
}
fn contains_draft_cand(haystack: &[Cand], needle: &AvailabilityDraft) -> bool {
    haystack.iter().any(|c| draft_eq(&c.draft, needle))
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
fn is_identity(cand: &AvailabilityDraft, drafts: &[AvailabilityDraft]) -> bool {
    contains_draft(drafts, cand)
}

/*──────────────────────── Range utils (byte-lex) ────────────────────*/

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
fn overlaps_any(r: &RangeCube, drafts: &[AvailabilityDraft]) -> bool {
    drafts.iter().any(|d| r.overlaps(&d.range))
}

/// Add a signed integer `delta` to a **big-endian, variable-length** unsigned byte vector.
/// - Positive delta does carry-propagation, possibly increasing length.
/// - Negative delta borrows; if magnitude exceeds the value, returns **empty vec** (canonical zero).
/// - Leading zeros are trimmed so lex compare aligns with numeric compare.
fn be_add_signed(input: &[u8], delta: i128) -> Option<Vec<u8>> {
    if delta == 0 {
        return Some(trim_leading_zeros(input.to_vec()));
    }
    if delta > 0 {
        let mut a = input.to_vec();
        let mut b = u128_to_trimmed_be(delta as u128);
        be_add_in_place(&mut a, &mut b);
        return Some(trim_leading_zeros(a));
    } else {
        let mag = (-delta) as u128;
        match be_sub_value(input, mag) {
            Some(v) => Some(trim_leading_zeros(v)),
            None => Some(vec![]), // underflow → canonical zero (empty vec)
        }
    }
}
fn be_add_in_place(a: &mut Vec<u8>, b: &mut Vec<u8>) {
    // make both same length by pre-padding shorter with leading zeros
    let max_len = a.len().max(b.len());
    if a.len() < max_len {
        let mut pad = vec![0u8; max_len - a.len()];
        pad.extend_from_slice(a);
        *a = pad;
    }
    if b.len() < max_len {
        let mut pad = vec![0u8; max_len - b.len()];
        pad.extend_from_slice(b);
        *b = pad;
    }

    let mut carry = 0u16;
    for i in (0..max_len).rev() {
        let sum = a[i] as u16 + b[i] as u16 + carry;
        a[i] = (sum & 0xFF) as u8;
        carry = sum >> 8;
    }
    if carry != 0 {
        let mut out = Vec::with_capacity(max_len + 1);
        out.push((carry & 0xFF) as u8);
        out.extend_from_slice(a);
        *a = out;
    }
}
fn be_sub_value(a: &[u8], sub: u128) -> Option<Vec<u8>> {
    let aval = be_to_u128_sat(a);
    if sub > aval {
        return None;
    }
    let res = aval - sub;
    Some(u128_to_trimmed_be(res))
}
fn be_to_u128_sat(a: &[u8]) -> u128 {
    let mut v: u128 = 0;
    for &b in a {
        v = v.saturating_mul(256);
        v = v.saturating_add(b as u128);
    }
    v
}
fn u128_to_trimmed_be(mut v: u128) -> Vec<u8> {
    if v == 0 {
        return vec![];
    }
    let mut buf = [0u8; 16];
    let mut i = 16;
    while v > 0 {
        i -= 1;
        buf[i] = (v & 0xFF) as u8;
        v >>= 8;
    }
    buf[i..].to_vec()
}
fn trim_leading_zeros(mut v: Vec<u8>) -> Vec<u8> {
    while v.first().map(|b| *b == 0).unwrap_or(false) {
        v.remove(0);
    }
    v
}

/*──────────────────────────── tests ─────────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::{clock::MockClock, uuid_bytes::UuidBytes};

    // small builder
    fn draft_bytes(level: u16, mins: &[&[u8]], maxs: &[&[u8]]) -> AvailabilityDraft {
        AvailabilityDraft {
            level,
            complete: true,
            range: RangeCube {
                dims: smallvec![], // unused in these tests
                mins: mins.iter().map(|m| m.to_vec()).collect(),
                maxs: maxs.iter().map(|m| m.to_vec()).collect(),
            },
        }
    }

    async fn fresh_db() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("opt_add.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db
    }

    fn caps() -> Caps {
        Caps {
            eps: 0.0,
            max_steps: 16,
            max_variants_per_draft: 64,
        }
    }

    /*──────── score helpers ────────*/
    struct ZeroScorer;
    impl Scorer for ZeroScorer {
        fn score(&self, _ctx: &PeerContext, _a: &AvailabilityDraft) -> f32 {
            0.0
        }
    }

    /// Prefers larger span and higher level → pushes to parents and growth.
    struct SpanPromoteScorer;
    impl Scorer for SpanPromoteScorer {
        fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
            let d = a.range.mins.len().min(a.range.maxs.len());
            let mut span = 0i32;
            for i in 0..d {
                // span proxy: length(max) - length(min)
                span += (a.range.maxs[i].len() as i32) - (a.range.mins[i].len() as i32);
            }
            span as f32 + (a.level as f32) * 2.0
        }
    }

    /// Loves identity and dislikes change → triggers identity early-exit.
    struct IdentityLovesScorer;
    impl Scorer for IdentityLovesScorer {
        fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
            if a.level == 0 { 1.0 } else { 0.0 }
        }
    }

    /*──────── unit tests ───────────*/

    #[test]
    fn be_add_signed_basic() {
        // add within same len
        assert_eq!(be_add_signed(&[0x0f], 1).unwrap(), vec![0x10]);
        // carry extends length
        assert_eq!(be_add_signed(&[0xff], 1).unwrap(), vec![0x01, 0x00]);
        // subtract within value
        assert_eq!(be_add_signed(&[0x10], -1).unwrap(), vec![0x0f]);
        // subtract past zero → empty vec (canonical zero)
        assert_eq!(be_add_signed(&[0x00], -1).unwrap(), Vec::<u8>::new());
        // trim leading zeros
        assert_eq!(be_add_signed(&[0x00, 0x10], 0).unwrap(), vec![0x10]);
    }

    #[tokio::test]
    async fn propose_noop_when_scorer_is_zero() {
        let db = fresh_db().await;
        let opt = BasicOptimizer::new(
            Box::new(ZeroScorer),
            PeerContext {
                peer_id: UuidBytes::new(),
            },
        )
        .with_caps(caps());

        let base = draft_bytes(0, &[b"a"], &[b"a\x01"]);
        let got = opt.propose(&db, &[base]).await.unwrap();
        assert!(got.is_none(), "zero scorer should produce no plan");
    }

    #[tokio::test]
    async fn identity_early_exit() {
        let db = fresh_db().await;
        let opt = BasicOptimizer::new(
            Box::new(IdentityLovesScorer),
            PeerContext {
                peer_id: UuidBytes::new(),
            },
        )
        .with_caps(caps());

        let base = draft_bytes(0, &[b"\x0f"], &[b"\x0f\x01"]);
        let got = opt.propose(&db, &[base]).await.unwrap();
        assert!(got.is_none(), "should early-exit when identity is argmax");
    }

    #[tokio::test]
    async fn proposes_a_parent_insert_under_span_promote() {
        let db = fresh_db().await;
        let opt = BasicOptimizer::new(
            Box::new(SpanPromoteScorer),
            PeerContext {
                peer_id: UuidBytes::new(),
            },
        )
        .with_caps(caps())
        .with_momentum(Momentum {
            ema: 1.0,
            alpha: 0.5,
        });

        // small leaf
        let base = draft_bytes(0, &[&[15u8, 0x00]], &[&[15u8, 0x01]]);
        let plan = opt.propose(&db, &[base]).await.unwrap().expect("plan");
        assert!(
            plan.iter()
                .any(|a| matches!(a, Action::Insert(d) if d.level >= 1)),
            "expected at least one promoted (parent) insert"
        );
    }

    #[tokio::test]
    async fn proposes_delete_of_negative_parent() {
        // scorer that makes ANY parent (level ≥1) negative
        struct KillParents;
        impl Scorer for KillParents {
            fn score(&self, _ctx: &PeerContext, a: &AvailabilityDraft) -> f32 {
                if a.level >= 1 { -1.0 } else { 0.0 }
            }
        }

        let db = fresh_db().await;
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let opt = BasicOptimizer::new(Box::new(KillParents), ctx.clone()).with_caps(caps());

        // insert one negative parent directly into DB
        let parent_id = UuidBytes::new();
        let rc = RangeCube {
            dims: smallvec![],
            mins: smallvec![vec![10u8]],
            maxs: smallvec![vec![20u8]],
        };
        let parent = Availability {
            key: parent_id,
            peer_id: ctx.peer_id,
            range: rc,
            level: 1,
            children: crate::plugins::harmonizer::child_set::ChildSet {
                parent: parent_id,
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(parent).await.unwrap();

        // one tiny draft overlapping that parent
        let draft = draft_bytes(0, &[&[15u8]], &[&[15u8, 1]]);
        let plan = opt.propose(&db, &[draft]).await.unwrap().expect("plan");
        assert!(
            plan.iter().any(|a| matches!(a, Action::Delete(_))),
            "expected at least one Delete action"
        );
    }
}
