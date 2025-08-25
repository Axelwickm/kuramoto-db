//! optimizer.rs
//! Beam-search planner that mutates ranges and scores against an overlay view.
//!
//! Key points:
//! - Public API is unchanged: `propose(&db, &txn, &[AvailabilityDraft]) -> Option<ActionSet>`
//! - Planning **state** = `{ focus draft, overlay }`, where overlay is a de-duped list of
//!   `Action::{Insert,Delete}`. We *do not* write to the DB during planning.
//! - Candidate generation enumerates **range mutations** (±Δ on min/max per axis, and grow-both),
//!   plus a promotion variant (level+1). Δ comes from a fixed step ladder.
//! - Evaluator builds an **effective overlay** per state by conditionally including the focus
//!   insert only if it would actually adopt at least one local child (range->children via roots).
//! - Scoring is done by the provided `Scorer` against that effective overlay.
//! - No domain-y post passes: pruning empty nodes happens at commit time.
//!
//! This file intentionally relies on `RangeCube` operations (`overlaps`, `contains`, `intersect`)
//! instead of re-implementing geometry checks here.

use async_trait::async_trait;
use redb::ReadTransaction;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{
        availability::Availability,
        availability_queries::local_child_count_under_peer,
        harmonizer::PeerContext,
        range_cube::RangeCube,
        scorers::Scorer,
        search::{
            CandidateGen, Evaluator, SearchAlgorithm, State,
            beam::{BeamConfig, BeamSearch},
        },
    },
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/*──────────────────── drafts & actions (public) ───────────────────*/

#[derive(Debug, Clone)]
pub struct AvailabilityDraft {
    pub level: u16,
    pub range: RangeCube,
    pub complete: bool,
}
impl From<&Availability> for AvailabilityDraft {
    fn from(a: &Availability) -> Self {
        Self {
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

/*──────────────────────── config ──────────────────────────────*/

#[derive(Debug, Clone, Copy)]
pub struct Caps {
    /// beam search depth (look-ahead)
    pub depth: usize,
    /// beam width
    pub beam_width: usize,
    /// cap total variants per expansion
    pub max_variants_per_draft: usize,
    /// minimum improvement threshold (passed to beam)
    pub eps: f32,
}
impl Default for Caps {
    fn default() -> Self {
        Self {
            depth: 2,
            beam_width: 12,
            max_variants_per_draft: 96,
            eps: 0.0,
        }
    }
}

/*──────────────────────── Optimizer trait ─────────────────────*/

#[async_trait]
pub trait Optimizer: Send + Sync {
    async fn propose(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        seeds: &[AvailabilityDraft],
    ) -> Result<Option<ActionSet>, StorageError>;
}

/*──────────────────────── Planning state ──────────────────────*/

#[derive(Debug, Clone)]
struct PlanState {
    /// The node we are currently mutating (range & level only).
    focus: AvailabilityDraft,
    /// The **material plan** so far (deduped Inserts/Deletes).
    overlay: ActionSet,
}
impl PlanState {
    fn new(seed: AvailabilityDraft) -> Self {
        Self {
            focus: seed,
            overlay: Vec::new(),
        }
    }
}
impl State for PlanState {
    type Action = Action;

    fn apply(&self, a: &Self::Action) -> Self {
        let mut next = self.clone();
        match a {
            Action::Insert(d) => {
                next.focus = d.clone();
            }
            Action::Delete(id) => push_delete(&mut next.overlay, *id),
        }
        next
    }
}

/*──────────────────────── Basic optimizer ─────────────────────*/

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
}

#[async_trait]
impl Optimizer for BasicOptimizer {
    async fn propose(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        seeds: &[AvailabilityDraft],
    ) -> Result<Option<ActionSet>, StorageError> {
        // Filter obviously invalid seeds (empty range)
        let seeds: Vec<_> = seeds
            .iter()
            .cloned()
            .filter(|d| !range_is_empty(&d.range))
            .collect();
        if seeds.is_empty() {
            return Ok(None);
        }

        let cfg = BeamConfig {
            depth: self.caps.depth,
            beam_width: self.caps.beam_width,
            eps: self.caps.eps,
            max_evals: 0, // unlimited
            ..Default::default()
        };

        let mut merged: ActionSet = Vec::new();

        let mut search = BeamSearch::new(cfg);

        for seed in seeds {
            let start = PlanState::new(seed.clone());
            let g = PlannerGen { caps: self.caps };
            let eval = PlannerEval {
                db,
                txn,
                ctx: &self.ctx,
                scorer: &*self.scorer,
            };

            let Some(path) = search.propose_step(&start, &g, &eval).await else {
                continue;
            };

            // Build final state's overlay and merge deterministically.
            let mut st = start.clone();
            for a in &path {
                st = st.apply(a);
            }

            // 1) For scoring we used effective_overlay (adoption-aware).
            // 2) For the returned plan, always include the beam’s final focus draft.
            //    Commit-time pruning will drop empty/non-adopting parents later.
            push_insert(&mut merged, st.focus.clone());
            for a in st.overlay {
                if let Action::Delete(id) = a {
                    push_delete(&mut merged, id);
                }
            }
        }

        if merged.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merged))
        }
    }
}

/*──────────────────────── Candidate generator ─────────────────*/

struct PlannerGen {
    caps: Caps,
}
impl CandidateGen<PlanState> for PlannerGen {
    fn candidates(&self, s: &PlanState) -> Vec<Action> {
        let steps = step_ladder();
        println!("Step ladder steps {}", steps.len());
        let d = s.focus.range.mins.len().min(s.focus.range.maxs.len());
        let mut out: Vec<Action> = Vec::with_capacity(self.caps.max_variants_per_draft);

        // Identity
        out.push(Action::Insert(s.focus.clone()));
        // Promotion (same geometry, higher level)
        out.push(Action::Insert(AvailabilityDraft {
            level: s.focus.level.saturating_add(1),
            range: s.focus.range.clone(),
            complete: true,
        }));

        for &st in &steps {
            for ax in 0..d {
                // min -= Δ
                if let Some(min2) = be_add_signed(&s.focus.range.mins[ax], -(st as i128)) {
                    let mut r = s.focus.range.clone();
                    r.mins[ax] = min2;
                    if !range_is_empty(&r) {
                        out.push(Action::Insert(AvailabilityDraft {
                            level: s.focus.level,
                            range: r.clone(),
                            complete: true,
                        }));
                    }
                }
                // max += Δ
                if let Some(max2) = be_add_signed(&s.focus.range.maxs[ax], st as i128) {
                    let mut r = s.focus.range.clone();
                    r.maxs[ax] = max2;
                    if !range_is_empty(&r) {
                        out.push(Action::Insert(AvailabilityDraft {
                            level: s.focus.level,
                            range: r.clone(),
                            complete: true,
                        }));
                    }
                }
                // grow both
                if let (Some(min2), Some(max2)) = (
                    be_add_signed(&s.focus.range.mins[ax], -(st as i128)),
                    be_add_signed(&s.focus.range.maxs[ax], st as i128),
                ) {
                    let mut r = s.focus.range.clone();
                    r.mins[ax] = min2;
                    r.maxs[ax] = max2;
                    if !range_is_empty(&r) {
                        out.push(Action::Insert(AvailabilityDraft {
                            level: s.focus.level,
                            range: r.clone(),
                            complete: true,
                        }));
                    }
                }
            }
            if out.len() >= self.caps.max_variants_per_draft {
                break;
            }
        }

        // De-dup by (level, range, complete), capped
        let mut uniq: ActionSet = Vec::with_capacity(out.len());
        for a in out.into_iter() {
            if let Action::Insert(ref d) = a {
                if contains_insert(&uniq, d) {
                    continue;
                }
            }
            uniq.push(a);
            if uniq.len() >= self.caps.max_variants_per_draft {
                break;
            }
        }
        uniq
    }
}

/*──────────────────────── Evaluator (overlay-aware) ─────────────────*/

struct PlannerEval<'a> {
    db: &'a KuramotoDb,
    txn: &'a ReadTransaction,
    ctx: &'a PeerContext,
    scorer: &'a dyn Scorer,
}

#[async_trait]
impl Evaluator<PlanState> for PlannerEval<'_> {
    async fn score(&self, s: &PlanState) -> f32 {
        let overlay = match self.effective_overlay(s).await {
            Ok(v) => v,
            Err(_) => s.overlay.clone(),
        };
        self.scorer
            .score(self.db, self.txn, self.ctx, &s.focus, &overlay)
            .await
    }

    fn feasible(&self, s: &PlanState) -> bool {
        !range_is_empty(&s.focus.range)
    }
}

impl<'a> PlannerEval<'a> {
    /// Build what the scorer should see:
    /// - include the focus insert only if it would adopt ≥1 local child under this peer’s roots.
    async fn effective_overlay(&self, s: &PlanState) -> Result<ActionSet, StorageError> {
        let mut eff = s.overlay.clone();

        let cnt = local_child_count_under_peer(
            self.db,
            Some(self.txn),
            &self.ctx.peer_id,
            &s.focus.range,
        )
        .await?;
        if cnt > 0 {
            push_insert(&mut eff, s.focus.clone());
        }
        Ok(eff)
    }
}

/*──────────────────────── Overlay helpers ─────────────────────*/

fn push_insert(overlay: &mut ActionSet, d: AvailabilityDraft) {
    if let Some(pos) = overlay
        .iter()
        .position(|a| matches!(a, Action::Insert(x) if draft_eq(x, &d)))
    {
        overlay[pos] = Action::Insert(d);
    } else {
        overlay.push(Action::Insert(d));
    }
}
fn push_delete(overlay: &mut ActionSet, id: UuidBytes) {
    if !overlay
        .iter()
        .any(|a| matches!(a, Action::Delete(x) if *x == id))
    {
        overlay.push(Action::Delete(id));
    }
}

/*──────────────────── equality / de-dup helpers ───────────────────*/

fn contains_insert(haystack: &[Action], needle: &AvailabilityDraft) -> bool {
    haystack
        .iter()
        .any(|a| matches!(a, Action::Insert(d) if draft_eq(d, needle)))
}
fn draft_eq(a: &AvailabilityDraft, b: &AvailabilityDraft) -> bool {
    a.level == b.level && a.complete == b.complete && ranges_equal(&a.range, &b.range)
}
fn ranges_equal(x: &RangeCube, y: &RangeCube) -> bool {
    if x.dims.len() != y.dims.len() || x.mins.len() != y.mins.len() || x.maxs.len() != y.maxs.len()
    {
        return false;
    }
    // rely on RangeCube invariants (aligned dims) — compare mins/maxs bytewise
    for i in 0..x.mins.len() {
        if x.mins[i] != y.mins[i] || x.maxs[i] != y.maxs[i] {
            return false;
        }
    }
    true
}

/*──────────────────────── range / byte math ─────────────────────────*/

/// A range is **invalid/empty** if any axis has max ≤ min (lex byte order).
fn range_is_empty(r: &RangeCube) -> bool {
    let n = r.mins.len().min(r.maxs.len());
    (0..n).any(|i| r.maxs[i] <= r.mins[i])
}

/// Ladder of step magnitudes in "byte-lex distance".
fn step_ladder() -> Vec<u128> {
    let mut v: Vec<u128> = vec![
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, // powers of two
        10, 100, 1_000, 10_000, 100_000, // powers of ten
    ];
    v.sort_unstable();
    v.dedup();
    v
}

/// Add signed integer `delta` to a big-endian, variable-length unsigned byte vector.
fn be_add_signed(input: &[u8], delta: i128) -> Option<Vec<u8>> {
    if delta == 0 {
        return Some(trim_leading_zeros(input.to_vec()));
    }
    if delta > 0 {
        let mut a = input.to_vec();
        let mut b = u128_to_trimmed_be(delta as u128);
        be_add_in_place(&mut a, &mut b);
        Some(trim_leading_zeros(a))
    } else {
        let mag = (-delta) as u128;
        match be_sub_value(input, mag) {
            Some(v) => Some(trim_leading_zeros(v)),
            None => Some(vec![]), // underflow → canonical zero
        }
    }
}
fn be_add_in_place(a: &mut Vec<u8>, b: &mut Vec<u8>) {
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
    while v.first().is_some_and(|b| *b == 0) {
        v.remove(0);
    }
    v
}

/*──────────────────────────── tests ─────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::{
        clock::MockClock,
        plugins::harmonizer::{
            availability::Availability, child_set::ChildSet, harmonizer::PeerContext,
            range_cube::RangeCube, scorers::Scorer,
        },
        tables::TableHash,
        uuid_bytes::UuidBytes,
    };

    fn draft(level: u16, mins: &[&[u8]], maxs: &[&[u8]]) -> AvailabilityDraft {
        AvailabilityDraft {
            level,
            complete: true,
            range: RangeCube {
                dims: smallvec![], // tests don't depend on dims here
                mins: mins.iter().map(|m| m.to_vec()).collect(),
                maxs: maxs.iter().map(|m| m.to_vec()).collect(),
            },
        }
    }

    async fn fresh_db() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("opt_overlay_beam.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db
    }

    fn caps() -> Caps {
        Caps {
            depth: 2,
            beam_width: 8,
            max_variants_per_draft: 64,
            eps: 0.0,
        }
    }

    /*──────── dummy scorers ────────*/

    struct ZeroScorer;
    #[async_trait::async_trait]
    impl Scorer for ZeroScorer {
        async fn score(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _ctx: &PeerContext,
            _a: &AvailabilityDraft,
            _overlay: &ActionSet,
        ) -> f32 {
            0.0
        }
    }

    /// Prefers larger span and higher level → pushes to parents and growth.
    struct SpanPromoteScorer;
    #[async_trait::async_trait]
    impl Scorer for SpanPromoteScorer {
        async fn score(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _ctx: &PeerContext,
            a: &AvailabilityDraft,
            _overlay: &ActionSet,
        ) -> f32 {
            a.range.approx_volume() as f32 + (a.level as f32) * 2.0
        }
    }

    #[test]
    fn be_add_signed_basic() {
        assert_eq!(be_add_signed(&[0x0f], 1).unwrap(), vec![0x10]);
        assert_eq!(be_add_signed(&[0xff], 1).unwrap(), vec![0x01, 0x00]);
        assert_eq!(be_add_signed(&[0x10], -1).unwrap(), vec![0x0f]);
        assert_eq!(be_add_signed(&[0x00], -1).unwrap(), Vec::<u8>::new());
        assert_eq!(be_add_signed(&[0x00, 0x10], 0).unwrap(), vec![0x10]);
    }

    #[tokio::test]
    async fn zero_scorer_returns_none() {
        let db = fresh_db().await;
        let opt = BasicOptimizer::new(
            Box::new(ZeroScorer),
            PeerContext {
                peer_id: UuidBytes::new(),
            },
        )
        .with_caps(caps());

        let base = draft(0, &[b"a"], &[b"a\x01"]);
        let txn = db.begin_read_txn().unwrap();
        let got = opt.propose(&db, &txn, &[base]).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn span_promote_proposes_parent_insert() {
        let db = fresh_db().await;
        let opt = BasicOptimizer::new(
            Box::new(SpanPromoteScorer),
            PeerContext {
                peer_id: UuidBytes::new(),
            },
        )
        .with_caps(caps());

        // small leaf
        let base = draft(0, &[&[15u8, 0x00]], &[&[15u8, 0x01]]);
        let txn = db.begin_read_txn().unwrap();
        let plan = opt.propose(&db, &txn, &[base]).await.unwrap().unwrap();
        assert!(
            plan.iter()
                .any(|a| matches!(a, Action::Insert(d) if d.level >= 1)),
            "expected at least one promoted (parent) insert"
        );
        // No explicit deletes in this planner
        assert!(!plan.iter().any(|a| matches!(a, Action::Delete(_))));
    }
}
