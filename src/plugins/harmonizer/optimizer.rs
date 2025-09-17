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
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::time::Instant;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{
        availability::{
            Availability, AVAILABILITY_BY_PEER_AND_ENTITY, AVAILABILITY_INCOMPLETE_BY_PEER,
        },
        availability::roots_for_peer,
        availability_queries::{child_count, range_cover, resolve_child_avail},
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

fn debug_enabled() -> bool {
    static ONCE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ONCE.get_or_init(|| match std::env::var("KDB_DEBUG_RANGE") {
        Ok(v) => !v.is_empty() && v != "0" && v.to_lowercase() != "false",
        Err(_) => false,
    })
}

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
    /// Replace an existing availability (by id) with a new draft.
    /// Semantically equivalent to: Insert(draft) + Delete(id), but expressed atomically
    /// so the planner can compete fairly without sequence-length penalties.
    Replace { id: UuidBytes, draft: AvailabilityDraft },
    /// Overlay hint for a storage atom at a specific location (level=0 semantics).
    Atom(RangeCube),
}
pub type ActionSet = Vec<Action>;

/*──────────────────────── config ──────────────────────────────*/

#[derive(Debug, Clone, Copy)]
pub struct Caps {
    /// beam search depth (look-ahead)
    pub depth: usize,
    /// beam width
    pub beam_width: usize,
    /// minimum improvement threshold (passed to beam)
    pub eps: f32,
}
impl Default for Caps {
    fn default() -> Self {
        Self {
            depth: 1,
            beam_width: 12,
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
        seed_atom: Option<RangeCube>,
    ) -> Result<Option<ActionSet>, StorageError>;
}

/*──────────────────────── Planning state ──────────────────────*/

#[derive(Debug, Clone)]
struct PlanState {
    focus: AvailabilityDraft,
    overlay: ActionSet,
}
impl PlanState {
    fn new(seed: AvailabilityDraft, overlay: ActionSet) -> Self { Self { focus: seed, overlay } }
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
            Action::Replace { id, draft } => {
                // Replace updates focus to the new draft and stages a delete for the old id
                next.focus = draft.clone();
                push_delete(&mut next.overlay, *id);
            }
            Action::Atom(r) => push_atom(&mut next.overlay, r.clone()),
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
        seed_atom: Option<RangeCube>,
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
            max_evals: 0,
            // Depth-1 should be cheap: disable tie-rollout to avoid large plateaus.
            rollout_depth: if self.caps.depth == 1 { 0 } else { 2 },
            ..Default::default()
        };

        let mut merged: ActionSet = Vec::new();

        let mut search = BeamSearch::new(cfg);

        // Initial overlay: include seed atom if provided
        let mut initial_overlay: ActionSet = Vec::new();
        if let Some(r) = seed_atom.clone() { initial_overlay.push(Action::Atom(r)); }

        for seed in seeds {
            // Build delete candidates for this seed by walking the local frontier under the seed range.
            // For every touched availability, propose deleting it and its parent(s).
            let mut dels_set: std::collections::HashSet<UuidBytes> =
                std::collections::HashSet::new();

            let roots = if let Some(fd) = seed.range.dims().first() {
                crate::plugins::harmonizer::availability::roots_for_peer_and_entity(
                    db,
                    Some(txn),
                    &self.ctx.peer_id,
                    fd.hash(),
                )
                .await?
            } else {
                roots_for_peer(db, Some(txn), &self.ctx.peer_id).await?
            };
            let root_ids: Vec<_> = roots.into_iter().map(|r| r.key).collect();
            let mut qcache = None;
            if debug_enabled() {
                println!(
                    "opt.propose: range_cover for seed level={} dims={} roots={}",
                    seed.level,
                    seed.range.dims().len(),
                    root_ids.len()
                );
            }
            // If there are no roots visible in this snapshot/scope, print diagnostics.
            if root_ids.is_empty() && debug_enabled() {
                // Count peer-scoped availabilities (complete + incomplete) in this snapshot
                let mut key_incomplete = self.ctx.peer_id.as_bytes().to_vec();
                key_incomplete.push(0);
                let mut key_complete = self.ctx.peer_id.as_bytes().to_vec();
                key_complete.push(1);
                let peer_incomplete = db
                    .get_by_index_all_tx::<Availability>(
                        Some(txn),
                        AVAILABILITY_INCOMPLETE_BY_PEER,
                        &key_incomplete,
                    )
                    .await
                    .unwrap_or_default()
                    .len();
                let peer_complete = db
                    .get_by_index_all_tx::<Availability>(
                        Some(txn),
                        AVAILABILITY_INCOMPLETE_BY_PEER,
                        &key_complete,
                    )
                    .await
                    .unwrap_or_default()
                    .len();
                // If seed is entity-scoped, also count per-entity rows
                let entity_count = if let Some(fd) = seed.range.dims().first() {
                    let mut k = self.ctx.peer_id.as_bytes().to_vec();
                    k.extend_from_slice(&fd.hash().to_be_bytes());
                    db.get_by_index_all_tx::<Availability>(
                        Some(txn),
                        AVAILABILITY_BY_PEER_AND_ENTITY,
                        &k,
                    )
                    .await
                    .unwrap_or_default()
                    .len()
                } else {
                    0
                };
                println!(
                    "opt.propose: DIAG roots empty | peer_avails: complete={} incomplete={} | entity_avails={} | note: range_cover ignores overlay inserts",
                    peer_complete, peer_incomplete, entity_count
                );
                // Previous early-exit removed for investigation:
                // continue;
            }

            let (frontier, no_overlap) = range_cover(
                db,
                Some(txn),
                &seed.range,
                &root_ids,
                None,
                &vec![],
                &mut qcache,
            )
            .await?;
            if debug_enabled() {
                println!(
                    "opt.propose: range_cover frontier_len={} no_overlap={}",
                    frontier.len(),
                    no_overlap
                );
            }
            if !no_overlap {
                for a in frontier.into_iter() {
                    // local, complete nodes touched by this seed
                    if a.peer_id == self.ctx.peer_id && a.complete && a.range.overlaps(&seed.range)
                    {
                        dels_set.insert(a.key);
                        // Propose deleting immediate parents as well (if any)
                        let parents = crate::plugins::harmonizer::child_set::ChildSet::parents_of(
                            db,
                            Some(txn),
                            a.key,
                        )
                        .await
                        .unwrap_or_default();
                        for pid in parents {
                            if let Some(pav) = resolve_child_avail(db, Some(txn), &pid).await? {
                                if pav.peer_id == self.ctx.peer_id && pav.complete {
                                    dels_set.insert(pav.key);
                                }
                            }
                        }
                    }
                }
            }

            let mut dels: Vec<UuidBytes> = dels_set.into_iter().collect();

            // Resolve overlap targets to full records for Replace candidates
            let mut replace_targets: Vec<crate::plugins::harmonizer::availability::Availability> =
                Vec::new();
            for id in &dels {
                if let Some(av) = crate::plugins::harmonizer::availability_queries::resolve_child_avail(
                    db,
                    Some(txn),
                    id,
                )
                .await?
                {
                    // Only consider local, complete nodes as replacement targets
                    if av.peer_id == self.ctx.peer_id && av.complete {
                        replace_targets.push(av);
                    }
                }
            }

            let start = PlanState::new(seed.clone(), initial_overlay.clone());
            let g = PlannerGen {
                caps: self.caps,
                deletes: dels,
                replace_targets,
            };
            let eval = PlannerEval::new(db, txn, &self.ctx, &*self.scorer);

            let t_search = Instant::now();

            // Tiny-plateau lookahead: if caps.depth == 1 and the best immediate
            // action is tied with a very small plateau (e.g., 2), perform a
            // constrained depth-2 pass (no rollout) to break the tie.
            let mut path_opt = None;
            if self.caps.depth == 1 {
                // Score root once
                let s0 = eval.score(&start).await;
                // Score immediate children to detect tie plateau among feasible actions
                let kids = g.candidates(&start);
                let mut best_sc = f32::NEG_INFINITY;
                let mut scores: Vec<(f32, bool, Action)> = Vec::with_capacity(kids.len());
                for a in kids.into_iter() {
                    let st = start.apply(&a);
                    let sc = eval.score(&st).await;
                    let feas = eval.feasible(&st);
                    if feas && sc > best_sc {
                        best_sc = sc;
                    }
                    scores.push((sc, feas, a));
                }
                let mut ties = 0usize;
                let tie_margin = 1e-6f32;
                for (sc, feas, _a) in &scores {
                    if *feas && (best_sc - *sc).abs() <= tie_margin {
                        ties += 1;
                    }
                }
                let best_gain = best_sc - s0;
                // If tiny plateau (1-2 ties) with a positive gain, attempt limited depth-2
                if best_gain > self.caps.eps && ties > 1 && ties <= 2 {
                    let cfg2 = BeamConfig {
                        depth: 2,
                        beam_width: self.caps.beam_width.min(4),
                        eps: self.caps.eps,
                        max_evals: 10_000,
                        rollout_depth: 0, // no selective rollout for speed
                        beam_slack: 0.0,
                        prefer_longer_on_tie: false,
                        selective_rollout_top_r: 0,
                        tie_margin,
                    };
                    let mut bs2 = BeamSearch::new(cfg2);
                    path_opt = bs2.propose_step(&start, &g, &eval).await;
                }
            }

            let Some(path) = (match path_opt {
                Some(p) => Some(p),
                None => search.propose_step(&start, &g, &eval).await,
            }) else {
                let (calls, ns) = eval.timing_summary();
                #[cfg(feature = "harmonizer_debug")]
                if calls > 0 {
                    tracing::debug!(
                        scorer_time_ms = % (ns as f64 / 1e6),
                        calls = calls,
                        avg_us = %((ns / calls as u128) / 1000),
                        "opt.propose_step: no path"
                    );
                }
                // Surgical fallback: If the seed adds coverage that isn't already
                // provided by the current tree, return the seed insert. This preserves
                // the contract that propose() only returns None when the seed is
                // unnecessary (already covered or empty).
                // Conditions: level-0, cube has storage atoms, and not already
                // covered locally by this peer.
                if seed.level == 0 && !range_is_empty(&seed.range) {
                    // Fast atom existence check (overlay-aware)
                    if let Ok(Some(n)) = crate::plugins::harmonizer::availability_queries::storage_atom_count_in_cube_tx_overlay(
                        db,
                        Some(txn),
                        &seed.range,
                        &start.overlay,
                    ).await {
                        if n > 0 {
                            // Already covered check (snapshot-aware)
                            let mut qcache = None;
                            let covered = crate::plugins::harmonizer::availability_queries::peer_contains_range_local(
                                db,
                                Some(txn),
                                &self.ctx.peer_id,
                                &seed.range,
                                &vec![],
                                &mut qcache,
                            )
                            .await
                            .unwrap_or(false);
                            if !covered && !contains_insert(&merged, &seed) {
                                push_insert(&mut merged, seed.clone());
                            }
                        }
                    }
                }
                continue;
            };
            let dt_search = t_search.elapsed().as_millis();
            let (calls, ns) = eval.timing_summary();
            #[cfg(feature = "harmonizer_debug")]
            if calls > 0 {
                tracing::debug!(
                    took_ms = %dt_search,
                    scorer_time_ms = % (ns as f64 / 1e6),
                    calls = calls,
                    avg_us = %((ns / calls as u128) / 1000),
                    "opt.propose_step"
                );
            }

            // Build final state's overlay and merge deterministically.
            let mut st = start.clone();
            for a in &path {
                st = st.apply(a);
            }

            // Unconditionally stage the focus insert; adoption is handled by effective_overlay and scoring.
            push_insert(&mut merged, st.focus.clone());
            for a in st.overlay {
                if let Action::Delete(id) = a {
                    push_delete(&mut merged, id);
                }
            }
        }

        let out = if merged.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merged))
        };
        out
    }
}

/*──────────────────────── Candidate generator ─────────────────*/

struct PlannerGen {
    caps: Caps,
    deletes: Vec<UuidBytes>,
    replace_targets: Vec<crate::plugins::harmonizer::availability::Availability>,
}
impl CandidateGen<PlanState> for PlannerGen {
    fn candidates(&self, s: &PlanState) -> Vec<Action> {
        // Multi-magnitude ladder: include steps at 1,2,4,... across byte magnitudes
        fn step_ladder_multimag(coord_len: usize) -> Vec<usize> {
            let base: [usize; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
            let max_mag = coord_len.saturating_sub(1).min(3); // up to 3 byte-shifts (24 bits)
            let mut out = Vec::with_capacity(base.len() * (max_mag + 1));
            for mag in 0..=max_mag {
                let shift = 8 * mag;
                for &b in &base {
                    out.push(b << shift);
                }
            }
            out
        }
        let d = s.focus.range.mins().len().min(s.focus.range.maxs().len());
        let mut out: Vec<Action> = Vec::with_capacity(16);

        // Always include identity Insert for the seed focus and a promotion Insert (for cases
        // where no replacement target exists). These preserve previous behavior.
        out.push(Action::Insert(s.focus.clone()));
        out.push(Action::Insert(AvailabilityDraft {
            level: s.focus.level.saturating_add(1),
            range: s.focus.range.clone(),
            complete: true,
        }));

        // If we have overlapping local nodes, generate Replace candidates instead of
        // standalone Insert mutations for geometry tweaks. Promotion (level+1) is always
        // modeled as an Insert so children can be adopted (do not delete the child).
        if !self.replace_targets.is_empty() {
            for tgt in &self.replace_targets {
                let td = tgt
                    .range
                    .mins()
                    .len()
                    .min(tgt.range.maxs().len());
                // Promotion of target → Insert parent (keep child; adoption handled by evaluator)
                out.push(Action::Insert(AvailabilityDraft {
                    level: tgt.level.saturating_add(1),
                    range: tgt.range.clone(),
                    complete: true,
                }));
                for ax in 0..td {
                    let steps = step_ladder_multimag(tgt.range.mins()[ax].len());
                    for &st in &steps {
                        // min -= Δ
                        if let Some(min2) =
                            be_add_signed(&tgt.range.mins()[ax], -(st as i128))
                        {
                            let mut r = tgt.range.clone();
                            r.set_min(ax, min2);
                            if !range_is_empty(&r) {
                                out.push(Action::Replace {
                                    id: tgt.key,
                                    draft: AvailabilityDraft {
                                        level: tgt.level,
                                        range: r.clone(),
                                        complete: true,
                                    },
                                });
                            }
                        }
                        // max += Δ
                        if let Some(max2) = be_add_signed(&tgt.range.maxs()[ax], st as i128) {
                            let mut r = tgt.range.clone();
                            r.set_max(ax, max2);
                            if !range_is_empty(&r) {
                                out.push(Action::Replace {
                                    id: tgt.key,
                                    draft: AvailabilityDraft {
                                        level: tgt.level,
                                        range: r.clone(),
                                        complete: true,
                                    },
                                });
                            }
                        }
                        // grow both
                        if let (Some(min2), Some(max2)) = (
                            be_add_signed(&tgt.range.mins()[ax], -(st as i128)),
                            be_add_signed(&tgt.range.maxs()[ax], st as i128),
                        ) {
                            let mut r = tgt.range.clone();
                            r.set_min(ax, min2);
                            r.set_max(ax, max2);
                            if !range_is_empty(&r) {
                                out.push(Action::Replace {
                                    id: tgt.key,
                                    draft: AvailabilityDraft {
                                        level: tgt.level,
                                        range: r.clone(),
                                        complete: true,
                                    },
                                });
                            }
                        }
                    }
                }
            }
        } else {
            // No replacement targets → keep legacy Insert ladder mutations around the seed
            for ax in 0..d {
                let steps = step_ladder_multimag(s.focus.range.mins()[ax].len());
                for &st in &steps {
                    // min -= Δ
                    if let Some(min2) =
                        be_add_signed(&s.focus.range.mins()[ax], -(st as i128))
                    {
                        let mut r = s.focus.range.clone();
                        r.set_min(ax, min2);
                        if !range_is_empty(&r) {
                            out.push(Action::Insert(AvailabilityDraft {
                                level: s.focus.level,
                                range: r.clone(),
                                complete: true,
                            }));
                        }
                    }
                    // max += Δ
                    if let Some(max2) = be_add_signed(&s.focus.range.maxs()[ax], st as i128) {
                        let mut r = s.focus.range.clone();
                        r.set_max(ax, max2);
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
                        be_add_signed(&s.focus.range.mins()[ax], -(st as i128)),
                        be_add_signed(&s.focus.range.maxs()[ax], st as i128),
                    ) {
                        let mut r = s.focus.range.clone();
                        r.set_min(ax, min2);
                        r.set_max(ax, max2);
                        if !range_is_empty(&r) {
                            out.push(Action::Insert(AvailabilityDraft {
                                level: s.focus.level,
                                range: r.clone(),
                                complete: true,
                            }));
                        }
                    }
                }
            }
        }

        // Add Delete candidates from scorer-provided pool (attached to generator)
        for id in &self.deletes {
            out.push(Action::Delete(*id));
        }

        // De-dup by geometry and unique ids for Delete/Replace. Skip Replace that doesn't change geometry.
        let mut uniq: ActionSet = Vec::with_capacity(out.len());
        for a in out.into_iter() {
            if let Action::Insert(ref d) = a {
                if contains_insert(&uniq, d) {
                    continue;
                }
            }
            if let Action::Delete(id) = a {
                if uniq
                    .iter()
                    .any(|u| matches!(u, Action::Delete(x) if *x == id))
                {
                    continue;
                }
            }
            if let Action::Replace { id, ref draft } = a {
                // Skip if another Replace for same (id, geom) exists or if geometry is unchanged
                let same_geom = self
                    .replace_targets
                    .iter()
                    .find(|t| t.key == id)
                    .map(|t| ranges_equal(&t.range, &draft.range) && t.level == draft.level)
                    .unwrap_or(false);
                if same_geom {
                    continue;
                }
                if uniq.iter().any(|u| match u {
                    Action::Replace { id: id2, draft: d2 } => *id2 == id && draft_eq(d2, draft),
                    _ => false,
                }) {
                    continue;
                }
            }
            if let Action::Atom(ref r) = a {
                if contains_atom(&uniq, r) {
                    continue;
                }
            }
            uniq.push(a);
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

    // Caches
    roots_ids: Mutex<Option<Vec<UuidBytes>>>,
    frontier_cache: Mutex<HashMap<Vec<u8>, Vec<UuidBytes>>>,
    // Timing accumulators
    score_calls: AtomicUsize,
    score_time_ns: Mutex<u128>,
}
impl<'a> PlannerEval<'a> {
    fn new(
        db: &'a KuramotoDb,
        txn: &'a ReadTransaction,
        ctx: &'a PeerContext,
        scorer: &'a dyn Scorer,
    ) -> Self {
        Self {
            db,
            txn,
            ctx,
            scorer,
            roots_ids: Mutex::new(None),
            frontier_cache: Mutex::new(HashMap::new()),
            score_calls: AtomicUsize::new(0),
            score_time_ns: Mutex::new(0),
        }
    }

    fn note_score_time(&self, dt: std::time::Duration) {
        self.score_calls.fetch_add(1, Ordering::Relaxed);
        let mut acc = self.score_time_ns.lock().unwrap();
        *acc += dt.as_nanos() as u128;
    }

    pub fn timing_summary(&self) -> (usize, u128) {
        let calls = self.score_calls.load(Ordering::Relaxed);
        let ns = *self.score_time_ns.lock().unwrap();
        (calls, ns)
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

    async fn ensure_roots(&self) -> Result<Vec<UuidBytes>, StorageError> {
        {
            let guard = self.roots_ids.lock().unwrap();
            if let Some(v) = guard.as_ref() {
                return Ok(v.clone());
            }
        }
        let roots = roots_for_peer(self.db, Some(self.txn), &self.ctx.peer_id).await?;
        let ids: Vec<UuidBytes> = roots.into_iter().map(|r| r.key).collect();
        *self.roots_ids.lock().unwrap() = Some(ids.clone());
        Ok(ids)
    }

    async fn local_child_ids(&self, target: &RangeCube) -> Result<Vec<UuidBytes>, StorageError> {
        let key = Self::range_key(target);
        {
            let cache = self.frontier_cache.lock().unwrap();
            if let Some(v) = cache.get(&key) {
                return Ok(v.clone());
            }
        }
        // Discover local children via frontier DFS under our roots, then filter for contained nodes.
        let roots_ids = self.ensure_roots().await?;
        let mut qcache = None;
        if debug_enabled() {
            println!(
                "opt.eval: local_child_ids roots={} dims={}",
                roots_ids.len(),
                target.dims().len()
            );
        }
        let (frontier, no_overlap) = range_cover(
            self.db,
            Some(self.txn),
            target,
            &roots_ids,
            None,
            &vec![],
            &mut qcache,
        )
        .await?;
        if debug_enabled() {
            println!(
                "opt.eval: range_cover frontier_len={} no_overlap={} (for local_child_ids)",
                frontier.len(),
                no_overlap
            );
        }
        let mut seen = std::collections::HashSet::<UuidBytes>::new();
        if !no_overlap {
            for a in frontier {
                if a.peer_id == self.ctx.peer_id && a.complete && target.contains(&a.range) {
                    seen.insert(a.key);
                }
            }
        }
        let ids: Vec<UuidBytes> = seen.into_iter().collect();
        self.frontier_cache.lock().unwrap().insert(key, ids.clone());
        Ok(ids)
    }

    async fn effective_overlay(&self, s: &PlanState) -> Result<ActionSet, StorageError> {
        let mut eff = s.overlay.clone();

        let mut deleted = HashSet::<UuidBytes>::new();
        for a in &s.overlay {
            if let Action::Delete(id) = a {
                deleted.insert(*id);
            }
        }

        // Level-aware adoption: level 0 adopts if there are storage atoms in range;
        // for parents (level > 0), adopt only when there are at least two local, complete
        // contained availability children (bottom-up rule) and none are masked by deletes.
        let adopts = if s.focus.level == 0 {
            // Level 0 adopts if there are underlying storage atoms in range (overlay-aware)
            let n = crate::plugins::harmonizer::availability_queries::storage_atom_count_in_cube_tx_overlay(
                self.db,
                Some(self.txn),
                &s.focus.range,
                &s.overlay,
            )
            .await?;
            let ok = n.unwrap_or(0) > 0;
            if debug_enabled() {
                println!("opt.eval: adopts(level0)={} atoms?={:?}", ok, n);
            }
            ok
        } else {
            let ids = self.local_child_ids(&s.focus.range).await?;
            let kept = ids.iter().filter(|id| !deleted.contains(id)).count();
            // Also treat overlay atoms as provisional children for parent adoption.
            let overlay_atoms_in_range = s
                .overlay
                .iter()
                .filter(|a| matches!(a, Action::Atom(r) if s.focus.range.contains(r)))
                .count();
            let ok = (kept + overlay_atoms_in_range) >= 2;
            if debug_enabled() {
                println!(
                    "opt.eval: adopts(level{})={} kept_children={} overlay_atoms_in_range={} (raw_ids={})",
                    s.focus.level,
                    ok,
                    kept,
                    overlay_atoms_in_range,
                    ids.len()
                );
            }
            ok
        };
        if adopts {
            push_insert(&mut eff, s.focus.clone());
        }
        Ok(eff)
    }
}

#[async_trait]
impl Evaluator<PlanState> for PlannerEval<'_> {
    async fn score(&self, s: &PlanState) -> f32 {
        let overlay = match self.effective_overlay(s).await {
            Ok(v) => v,
            Err(_) => s.overlay.clone(),
        };
        let t0 = Instant::now();
        let out = self
            .scorer
            .score(self.db, self.txn, self.ctx, &s.focus, &overlay)
            .await;
        self.note_score_time(t0.elapsed());
        out
    }

    fn feasible(&self, s: &PlanState) -> bool {
        !range_is_empty(&s.focus.range)
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

fn push_atom(overlay: &mut ActionSet, r: RangeCube) {
    if !overlay.iter().any(|a| matches!(a, Action::Atom(x) if ranges_equal(x, &r))) {
        overlay.push(Action::Atom(r));
    }
}

/*──────────────────── equality / de-dup helpers ───────────────────*/

fn contains_insert(haystack: &[Action], needle: &AvailabilityDraft) -> bool {
    haystack
        .iter()
        .any(|a| matches!(a, Action::Insert(d) if draft_eq(d, needle)))
}
fn contains_atom(haystack: &[Action], needle: &RangeCube) -> bool {
    haystack
        .iter()
        .any(|a| matches!(a, Action::Atom(r) if ranges_equal(r, needle)))
}
fn draft_eq(a: &AvailabilityDraft, b: &AvailabilityDraft) -> bool {
    a.level == b.level && a.complete == b.complete && ranges_equal(&a.range, &b.range)
}
fn ranges_equal(x: &RangeCube, y: &RangeCube) -> bool {
    if x.dims().len() != y.dims().len()
        || x.mins().len() != y.mins().len()
        || x.maxs().len() != y.maxs().len()
    {
        return false;
    }
    for i in 0..x.mins().len() {
        if x.mins()[i] != y.mins()[i] || x.maxs()[i] != y.maxs()[i] {
            return false;
        }
    }
    true
}

/*──────────────────────── range / byte math ─────────────────────────*/

fn range_is_empty(r: &RangeCube) -> bool {
    let n = r.mins().len().min(r.maxs().len());
    (0..n).any(|i| r.maxs()[i] <= r.mins()[i])
}

pub fn step_ladder(n: usize) -> Vec<usize> {
    let mut v = Vec::with_capacity(n);
    let mut x = 1usize;
    while x <= n {
        v.push(x);
        x <<= 1; // multiply by 2
    }
    v
}

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

fn trim_leading_zeros(v: Vec<u8>) -> Vec<u8> {
    let i = v.iter().position(|&b| b != 0).unwrap_or(v.len());
    v[i..].to_vec()
}

/*──────────────────────────── tests ─────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::harmonizer::SyncTester;
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
        let len = mins.len().min(maxs.len());
        let dims: smallvec::SmallVec<[TableHash; 4]> =
            (0..len).map(|i| TableHash { hash: i as u64 }).collect();
        let mins_sv: smallvec::SmallVec<[Vec<u8>; 4]> = mins.iter().map(|m| m.to_vec()).collect();
        let maxs_sv: smallvec::SmallVec<[Vec<u8>; 4]> = maxs.iter().map(|m| m.to_vec()).collect();
        AvailabilityDraft {
            level,
            complete: true,
            range: RangeCube::new(dims, mins_sv, maxs_sv).unwrap(),
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
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::Child>()
            .unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::DigestChunk>()
            .unwrap();
        db
    }

    fn caps() -> Caps {
        Caps {
            depth: 2,
            beam_width: 8,
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

    /// Prefers having at least one delete in the overlay.
    struct DeleteFavorScorer;
    #[async_trait::async_trait]
    impl Scorer for DeleteFavorScorer {
        async fn score(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _ctx: &PeerContext,
            _a: &AvailabilityDraft,
            overlay: &ActionSet,
        ) -> f32 {
            overlay
                .iter()
                .filter(|x| matches!(x, Action::Delete(_)))
                .count() as f32
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
        let got = opt.propose(&db, &txn, &[base], None).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn sync_tester_optimizer_smoke() {
        // Basic smoke: ensure SyncTester can run alongside optimizer code paths without panics
        let peers = vec![UuidBytes::new(), UuidBytes::new()];
        let mut t = SyncTester::new(&peers, &[], Default::default()).await;
        // No watched tables or entities needed here; just step the system
        t.step(5).await;
        // Integrity check (no availability yet) should be clean
        for n in t.peers().iter() {
            let errs = crate::plugins::harmonizer::integrity_run_all(&n.db, None, n.peer_id, false)
                .await
                .unwrap();
            assert!(errs.is_empty());
        }
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
        let plan = opt.propose(&db, &txn, &[base], None).await.unwrap().unwrap();
        assert!(
            plan.iter()
                .any(|a| matches!(a, Action::Insert(d) if d.level >= 1)),
            "expected at least one promoted (parent) insert"
        );
        // No explicit deletes in this planner
        assert!(!plan.iter().any(|a| matches!(a, Action::Delete(_))));
    }

    #[tokio::test]
    async fn planner_generates_delete_candidates() {
        let db = fresh_db().await;
        let ctx = PeerContext {
            peer_id: UuidBytes::new(),
        };

        // Create a leaf and a parent that contains it; parent is root (no parent edge)
        use crate::tables::TableHash;
        let dim = TableHash { hash: 1 };
        let leaf = Availability {
            key: UuidBytes::new(),
            peer_id: ctx.peer_id,
            range: RangeCube::new(smallvec![dim], smallvec![vec![0x10]], smallvec![vec![0x11]])
                .unwrap(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(leaf.clone()).await.unwrap();
        let parent = Availability {
            key: UuidBytes::new(),
            peer_id: ctx.peer_id,
            range: RangeCube::new(smallvec![dim], smallvec![vec![0x10]], smallvec![vec![0x12]])
                .unwrap(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let pid = parent.key;
        db.put(parent.clone()).await.unwrap();
        // Link parent -> leaf
        let mut cs = ChildSet::open(&db, pid).await.unwrap();
        cs.add_child(&db, leaf.key).await.unwrap();

        let opt = BasicOptimizer::new(Box::new(DeleteFavorScorer), ctx.clone()).with_caps(caps());
        let seed = AvailabilityDraft {
            level: 0,
            range: leaf.range.clone(),
            complete: true,
        };
        let txn = db.begin_read_txn().unwrap();
        let plan = opt.propose(&db, &txn, &[seed], None).await.unwrap().unwrap();
        assert!(
            plan.iter().any(|a| matches!(a, Action::Delete(_))),
            "expected a delete candidate to be proposed"
        );
    }
}
