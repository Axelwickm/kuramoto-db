//! beam.rs
//! Deterministic N-step beam look-ahead planner (async).
//!
//! - Depth >= 1, beam width >= 1, no randomness.
//! - Explores top-K children at each level (ordered by child state's score).
//! - Returns the best sequence (up to length `depth`) whose *total gain*
//!   over the current state exceeds `eps`.
//! - `Evaluator::feasible` can rule out illegal intermediate/final states.
//!
//! Knobs:
//! - rollout_depth: lookahead scoring for tie-break ranking.
//! - beam_slack: keep near-top branches in addition to the strict beam.
//! - prefer_longer_on_tie: break equal-gain ties by preferring longer seqs.

use super::{CandidateGen, Evaluator, SearchAlgorithm, State};
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub struct BeamConfig {
    pub depth: usize,
    pub beam_width: usize,
    pub eps: f32,
    pub max_evals: usize,
    pub rollout_depth: usize,
    pub beam_slack: f32,
    pub prefer_longer_on_tie: bool,
}

impl Default for BeamConfig {
    fn default() -> Self {
        Self {
            depth: 2,
            beam_width: 8,
            eps: 0.0,
            max_evals: 0,
            rollout_depth: 2,
            beam_slack: 1.0,
            prefer_longer_on_tie: false,
        }
    }
}

pub struct BeamSearch {
    cfg: BeamConfig,
    evals: usize,
}

#[derive(Default)]
struct Stats {
    d1_total: usize,
    d1_kept: usize,
    d1_rollout_kept: usize,
    depth_frontiers: Vec<usize>, // size after beam cut at each depth
    depth_expanded: Vec<usize>,  // number of nodes expanded at each depth>=2
    scores_used: usize,
}

impl BeamSearch {
    pub fn new(cfg: BeamConfig) -> Self {
        Self { cfg, evals: 0 }
    }

    fn reset_budget(&mut self) {
        self.evals = 0;
    }

    fn budget_ok(&self) -> bool {
        self.cfg.max_evals == 0 || self.evals < self.cfg.max_evals
    }

    async fn score<S, E>(&mut self, eval: &E, s: &S) -> Option<f32>
    where
        S: State + Send + Sync,
        E: Evaluator<S> + Send + Sync,
    {
        if !self.budget_ok() {
            return None;
        }
        if !eval.feasible(s) {
            return None;
        }
        self.evals += 1;
        Some(eval.score(s).await)
    }

    // add: a raw score that ignores feasibility (used for heuristic ranking)
    async fn score_raw<S, E>(&mut self, eval: &E, s: &S) -> Option<f32>
    where
        S: State + Send + Sync,
        E: Evaluator<S> + Send + Sync,
    {
        if !self.budget_ok() {
            return None;
        }
        self.evals += 1;
        Some(eval.score(s).await)
    }

    // add: rollout priority that does NOT gate the root by feasibility;
    // feasibility is enforced when expanding, not when ranking.
    async fn rollout_priority_unsafe<S, C, E>(
        &mut self,
        cand: &C,
        eval: &E,
        start: &S,
    ) -> Option<f32>
    where
        S: State + Clone + Send + Sync,
        C: CandidateGen<S> + Send + Sync,
        E: Evaluator<S> + Send + Sync,
    {
        if self.cfg.rollout_depth == 0 {
            return self.score_raw(eval, start).await;
        }
        let mut best = self.score_raw(eval, start).await?;
        let mut frontier = vec![start.clone()];
        for _ in 0..self.cfg.rollout_depth {
            let mut next = Vec::new();
            for s in frontier {
                for a in cand.candidates(&s).into_iter() {
                    let st = s.apply(&a);
                    if let Some(sc) = self.score_raw(eval, &st).await {
                        if sc > best {
                            best = sc;
                        }
                        next.push(st);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Some(best)
    }
}

#[async_trait]
impl<S> SearchAlgorithm<S> for BeamSearch
where
    S: State + Clone + Send + Sync,
{
    async fn propose_step<G, E>(
        &mut self,
        current: &S,
        cand: &G,
        eval: &E,
    ) -> Option<Vec<S::Action>>
    where
        G: CandidateGen<S> + Send + Sync,
        E: Evaluator<S> + Send + Sync,
    {
        self.reset_budget();

        let mut stats = Stats::default();

        // Root must be feasible or we bail immediately.
        if !eval.feasible(current) {
            return None;
        }
        let s0 = self.score(eval, current).await?;

        println!(
            "search.step: depth={} beam={} eps={} s0={:.3}",
            self.cfg.depth, self.cfg.beam_width, self.cfg.eps, s0
        );

        #[derive(Clone)]
        struct Node<S: State> {
            state: S,
            score: f32, // immediate score of this state
            prio: f32,  // rollout-based priority for ranking
            seq: Vec<S::Action>,
        }

        // -------- Depth 1: rank by rollout priority (do not gate by feasibility here) --------
        let mut kids: Vec<(
            f32,  /*prio*/
            f32,  /*score*/
            bool, /*feas*/
            S::Action,
            S,
        )> = Vec::new();
        for a in cand.candidates(current).into_iter() {
            let next = current.apply(&a);
            // heuristic ranking (ignores feasibility at the root of rollout)
            let prio = match self.rollout_priority_unsafe(cand, eval, &next).await {
                Some(p) => p,
                None => return None, // budget exhausted
            };
            // immediate score for gains / logging
            let sc = match self.score_raw(eval, &next).await {
                Some(s) => s,
                None => return None, // budget exhausted
            };
            let feas = eval.feasible(&next);
            kids.push((prio, sc, feas, a, next));
        }
        stats.d1_total = kids.len();

        println!("kids: {}", kids.len());

        if kids.is_empty() {
            println!("search.best_single: none");
            println!(
                "search.best_seq: depth={} gain={:.3} len=0",
                self.cfg.depth, 0.0
            );
            return None;
        }

        // rank: prio desc; deterministic tie-break by action Debug
        kids.sort_by(|(pa, _sa, _fa, aa, _na), (pb, _sb, _fb, ab, _nb)| {
            pb.partial_cmp(pa)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| format!("{ab:?}").cmp(&format!("{aa:?}")))
        });

        // best single (for logging) = top-ranked immediate gain (even if infeasible)
        let best_single_gain = kids[0].1 - s0;
        println!(
            "search.best_single: gain={:.3} act={:?}",
            best_single_gain, kids[0].3
        );

        // beam+slack at depth-1: keep K + ceil(slack)
        let extra = if self.cfg.beam_slack > 0.0 {
            self.cfg.beam_slack.ceil() as usize
        } else {
            0
        };
        let keep = self.cfg.beam_width.saturating_add(extra);
        if kids.len() > keep {
            kids.truncate(keep);
        }
        stats.d1_kept = kids.len();
        stats.depth_frontiers.push(kids.len());

        // frontier may include infeasible nodes (on purpose) to let strict beams die as expected
        let mut frontier: Vec<Node<S>> = kids
            .iter()
            .map(|(prio, sc, _feas, act, st)| Node {
                state: st.clone(),
                score: *sc,
                prio: *prio,
                seq: vec![act.clone()],
            })
            .collect();

        // initialize best with the best **feasible** single-step candidate
        let mut best_gain = f32::NEG_INFINITY;
        let mut best_seq: Vec<S::Action> = Vec::new();
        for (_prio, sc, feas, act, _st) in &kids {
            if *feas && *sc - s0 > best_gain {
                best_gain = *sc - s0;
                best_seq = vec![act.clone()];
            }
        }

        // Depth==1 early exit: only return if a feasible 1-step beats eps
        if self.cfg.depth == 1 {
            return if best_gain > self.cfg.eps && !best_seq.is_empty() {
                Some(best_seq)
            } else {
                None
            };
        }
        println!(
            "search.stats: depth={} beam={} rollout_depth={} \
         d1={{total:{}, kept:{}}} per_depth={{expanded:{:?}, frontier:{:?}}} evals={}",
            self.cfg.depth,
            self.cfg.beam_width,
            self.cfg.rollout_depth,
            stats.d1_total,
            stats.d1_kept,
            stats.depth_expanded,
            stats.depth_frontiers,
            self.evals
        );

        // -------- Depth >= 2 --------
        for d in 2..=self.cfg.depth {
            let mut expanded: Vec<Node<S>> = Vec::new();
            let mut expanded_count_this_depth = 0;

            for node in &frontier {
                // only expand feasible frontier nodes
                if !eval.feasible(&node.state) {
                    continue;
                }
                for a in cand.candidates(&node.state).into_iter() {
                    let next = node.state.apply(&a);
                    // enforce feasibility for generated children
                    if !eval.feasible(&next) {
                        continue;
                    }
                    let sc = match self.score_raw(eval, &next).await {
                        Some(s) => s,
                        None => return None, // budget exhausted
                    };
                    let prio = if self.cfg.rollout_depth > 0 {
                        match self.rollout_priority_unsafe(cand, eval, &next).await {
                            Some(p) => p,
                            None => return None, // budget exhausted
                        }
                    } else {
                        sc
                    };
                    let mut seq2 = node.seq.clone();
                    seq2.push(a);
                    // update best (only from feasible nodes)
                    let gain = sc - s0;
                    if gain > best_gain
                        || (self.cfg.prefer_longer_on_tie
                            && (gain - best_gain).abs() <= f32::EPSILON
                            && seq2.len() > best_seq.len())
                    {
                        best_gain = gain;
                        best_seq = seq2.clone();
                    }
                    expanded.push(Node {
                        state: next,
                        score: sc,
                        prio,
                        seq: seq2,
                    });
                    expanded_count_this_depth += 1;
                }
            }

            if expanded.is_empty() {
                let strict = self.cfg.beam_slack <= 0.0 && self.cfg.beam_width == 1;
                if strict {
                    println!("search.dead_end: no feasible children at depth {d} (strict)");
                    return None;
                }
                println!("search.dead_end: no feasible children at depth {d} (slack present)");
                break;
            }

            // rank next frontier by rollout priority
            expanded.sort_by(|a, b| {
                b.prio
                    .partial_cmp(&a.prio)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| format!("{:?}", b.seq).cmp(&format!("{:?}", a.seq)))
            });
            if expanded.len() > self.cfg.beam_width {
                expanded.truncate(self.cfg.beam_width);
            }

            stats.depth_expanded.push(expanded_count_this_depth);
            stats.depth_frontiers.push(expanded.len());

            frontier = expanded;
        }

        println!(
            "search.best_seq: depth={} gain={:.3} len={}",
            self.cfg.depth,
            if best_gain.is_finite() {
                best_gain
            } else {
                0.0
            },
            best_seq.len()
        );

        if best_gain > self.cfg.eps && !best_seq.is_empty() {
            Some(best_seq)
        } else {
            None
        }
    }
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    // use crate::plugins::harmonizer::optimizer::search::{
    //     CandidateGen, Evaluator, SearchAlgorithm, State,
    // };
    // use async_trait::async_trait;

    /* ────────────────────────── Toy domains ────────────────────────── */

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct S {
        val: i32,
        staged: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum A {
        Inc(i32),
        Setup,
    }

    impl State for S {
        type Action = A;
        fn apply(&self, action: &Self::Action) -> Self {
            match *action {
                A::Inc(n) => S {
                    val: self.val + n,
                    staged: self.staged,
                },
                A::Setup => S {
                    val: self.val - 1,
                    staged: true,
                },
            }
        }
    }

    struct G;
    impl CandidateGen<S> for G {
        fn candidates(&self, s: &S) -> Vec<A> {
            if !s.staged {
                // Only +1 or Setup from the root
                vec![A::Inc(1), A::Setup]
            } else {
                vec![A::Inc(5)]
            }
        }
    }

    struct E;
    #[async_trait]
    impl Evaluator<S> for E
    where
        S: State + Send + Sync,
    {
        async fn score(&self, s: &S) -> f32 {
            s.val as f32
        }
        fn feasible(&self, _s: &S) -> bool {
            true
        }
    }

    /* Domain for rollout/slack/diversity checks */
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RS {
        val: i32,
        phase: u8, // 0=root, 1=after GoA, 2=after GoB
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum RA {
        GoA,
        GoB,
        Big,
        Small,
    }
    impl State for RS {
        type Action = RA;
        fn apply(&self, a: &Self::Action) -> Self {
            match *a {
                RA::GoA => RS {
                    val: self.val,
                    phase: 1,
                },
                RA::GoB => RS {
                    val: self.val,
                    phase: 2,
                },
                RA::Big => RS {
                    val: self.val + 10,
                    phase: 1,
                },
                RA::Small => RS {
                    val: self.val + 1,
                    phase: 2,
                },
            }
        }
    }
    struct RG; // candidate generator
    impl CandidateGen<RS> for RG {
        fn candidates(&self, s: &RS) -> Vec<RA> {
            match s.phase {
                0 => vec![RA::GoA, RA::GoB],
                1 => vec![RA::Big],
                2 => vec![RA::Small],
                _ => vec![],
            }
        }
    }
    struct RE {
        // evaluator (optionally with feasibility gating)
        forbid_phase: Option<u8>,
    }
    #[async_trait]
    impl Evaluator<RS> for RE
    where
        RS: State + Send + Sync,
    {
        async fn score(&self, s: &RS) -> f32 {
            s.val as f32
        }
        fn feasible(&self, s: &RS) -> bool {
            if let Some(p) = self.forbid_phase {
                s.phase != p
            } else {
                true
            }
        }
    }

    /* Domain for “prefer longer on tie” */
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TS {
        val: i32,
        step: u8,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum TA {
        X,
        Y,
        Z,
    }
    impl State for TS {
        type Action = TA;
        fn apply(&self, a: &Self::Action) -> Self {
            match *a {
                TA::X => TS {
                    val: self.val + 2,
                    step: 1,
                }, // one-step +2, then dead-end
                TA::Y => TS {
                    val: self.val + 1,
                    step: 1,
                }, // +1 then Z gives +1 more
                TA::Z => TS {
                    val: self.val + 1,
                    step: 2,
                },
            }
        }
    }
    struct TG;
    impl CandidateGen<TS> for TG {
        fn candidates(&self, s: &TS) -> Vec<TA> {
            match s.step {
                0 => vec![TA::X, TA::Y],
                1 => {
                    if s.val == 1 {
                        vec![TA::Z]
                    } else {
                        vec![]
                    }
                }
                _ => vec![],
            }
        }
    }
    struct TE;
    #[async_trait]
    impl Evaluator<TS> for TE
    where
        TS: State + Send + Sync,
    {
        async fn score(&self, s: &TS) -> f32 {
            s.val as f32
        }
        fn feasible(&self, _s: &TS) -> bool {
            true
        }
    }

    /* ────────────────────────── Tests ────────────────────────── */

    #[tokio::test]
    async fn depth1_picks_best_single() {
        let mut algo = BeamSearch::new(BeamConfig {
            depth: 1,
            beam_width: 8,
            eps: 0.0,
            max_evals: 0,
            ..Default::default()
        });
        let s0 = S {
            val: 0,
            staged: false,
        };
        let plan = algo.propose_step(&s0, &G, &E).await.expect("some plan");
        assert_eq!(plan, vec![A::Inc(1)]);
    }

    #[tokio::test]
    async fn depth2_escapes_local_minimum() {
        let mut algo = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 2,
            eps: 0.0,
            max_evals: 0,
            ..Default::default()
        });
        let s0 = S {
            val: 0,
            staged: false,
        };
        let plan = algo.propose_step(&s0, &G, &E).await.expect("some plan");
        assert_eq!(plan, vec![A::Setup, A::Inc(5)]);
    }

    #[tokio::test]
    async fn epsilon_blocks_small_improvements() {
        let mut algo = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 2,
            eps: 5.0,
            max_evals: 0,
            ..Default::default()
        });
        let s0 = S {
            val: 0,
            staged: false,
        };
        assert!(
            algo.propose_step(&s0, &G, &E).await.is_none(),
            "gain=4 should be rejected by eps=5"
        );
    }

    #[tokio::test]
    async fn rollout_with_beam1_changes_which_branch_survives() {
        // Two first actions tie immediately (score 0). Only GoA leads to Big (+10).
        let s0 = RS { val: 0, phase: 0 };

        // Without rollout: beam_width=1 keeps the lexicographically later debug (GoB),
        // so the best depth-2 sequence is [GoB, Small] (gain 1).
        let mut greedy = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 1,
            eps: 0.0,
            max_evals: 0,
            rollout_depth: 0,
            ..Default::default()
        });
        let plan_greedy = greedy
            .propose_step(&s0, &RG, &RE { forbid_phase: None })
            .await
            .expect("some plan");
        assert_eq!(plan_greedy, vec![RA::GoB, RA::Small]);

        // With rollout_depth=1: GoA gets higher priority and survives → [GoA, Big] (gain 10).
        let mut with_rollout = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 1,
            eps: 0.0,
            max_evals: 0,
            rollout_depth: 1,
            ..Default::default()
        });
        let plan_rollout = with_rollout
            .propose_step(&s0, &RG, &RE { forbid_phase: None })
            .await
            .expect("some plan");
        assert_eq!(plan_rollout, vec![RA::GoA, RA::Big]);
    }

    #[tokio::test]
    async fn beam_slack_recovers_when_top_branch_becomes_infeasible() {
        // Make the best first branch (GoA) infeasible at expansion; only GoB → Small is viable.
        let s0 = RS { val: 0, phase: 0 };

        // No slack + beam_width=1: we keep GoA only; its child is infeasible → no improvement (None).
        let mut no_slack = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 1,
            eps: 0.0,
            max_evals: 0,
            rollout_depth: 1,
            beam_slack: 0.0,
            ..Default::default()
        });
        let none = no_slack
            .propose_step(
                &s0,
                &RG,
                &RE {
                    forbid_phase: Some(1),
                },
            )
            .await;
        assert!(
            none.is_none(),
            "top branch infeasible at depth-2 should yield None without slack"
        );

        // With slack: keep near-top second branch (GoB) too → we can realize [GoB, Small].
        let mut slacky = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 1,
            eps: 0.0,
            max_evals: 0,
            rollout_depth: 1,
            beam_slack: 1.0,
            ..Default::default()
        });
        let plan = slacky
            .propose_step(
                &s0,
                &RG,
                &RE {
                    forbid_phase: Some(1),
                },
            )
            .await
            .expect("plan with slack");
        assert_eq!(plan, vec![RA::GoB, RA::Small]);
    }

    #[tokio::test]
    async fn prefer_longer_sequence_on_exact_gain_tie() {
        // X = +2 in one step; Y=+1 then Z=+1 → also +2.
        let s0 = TS { val: 0, step: 0 };

        // Without prefer_longer_on_tie: either is acceptable; deterministic tie-break favors X.
        let mut no_pref = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 4,
            eps: 0.0,
            max_evals: 0,
            prefer_longer_on_tie: false,
            ..Default::default()
        });
        let plan1 = no_pref.propose_step(&s0, &TG, &TE).await.expect("plan");
        assert_eq!(plan1, vec![TA::X]);

        // With prefer_longer_on_tie: choose [Y, Z] (length 2) over [X] (length 1) at equal gain.
        let mut prefer_long = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 4,
            eps: 0.0,
            max_evals: 0,
            prefer_longer_on_tie: true,
            ..Default::default()
        });
        let plan2 = prefer_long.propose_step(&s0, &TG, &TE).await.expect("plan");
        assert_eq!(plan2, vec![TA::Y, TA::Z]);
    }

    #[tokio::test]
    async fn max_evals_budget_can_prevent_a_plan() {
        // Need a few evals to discover Setup→Inc(5). With max_evals=1, we won't get there.
        let s0 = S {
            val: 0,
            staged: false,
        };

        let mut tight = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 2,
            eps: 0.0,
            max_evals: 1,
            ..Default::default()
        });
        assert!(
            tight.propose_step(&s0, &G, &E).await.is_none(),
            "budget too tight should return None"
        );

        let mut roomy = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 2,
            eps: 0.0,
            max_evals: 0,
            ..Default::default()
        });
        let plan = roomy
            .propose_step(&s0, &G, &E)
            .await
            .expect("plan under normal budget");
        assert_eq!(plan, vec![A::Setup, A::Inc(5)]);
    }

    #[tokio::test]
    async fn determinism_same_inputs_same_plan() {
        let s0 = RS { val: 0, phase: 0 };
        let cfg = BeamConfig {
            depth: 2,
            beam_width: 3,
            eps: 0.0,
            max_evals: 0,
            rollout_depth: 1,
            beam_slack: 0.25,
            prefer_longer_on_tie: true,
            ..Default::default()
        };
        let mut a1 = BeamSearch::new(cfg.clone());
        let mut a2 = BeamSearch::new(cfg);
        let p1 = a1.propose_step(&s0, &RG, &RE { forbid_phase: None }).await;
        let p2 = a2.propose_step(&s0, &RG, &RE { forbid_phase: None }).await;
        assert_eq!(p1, p2, "beam search should be deterministic");
    }

    #[tokio::test]
    async fn feasibility_false_at_root_yields_none() {
        struct EF;
        #[async_trait]
        impl Evaluator<S> for EF
        where
            S: State + Send + Sync,
        {
            async fn score(&self, _s: &S) -> f32 {
                0.0
            }
            fn feasible(&self, _s: &S) -> bool {
                false
            }
        }

        let mut algo = BeamSearch::new(BeamConfig::default());
        let s0 = S {
            val: 0,
            staged: false,
        };
        assert!(algo.propose_step(&s0, &G, &EF).await.is_none());
    }
}
