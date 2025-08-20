//! Deterministic N-step beam look-ahead planner.
//!
//! - Depth >= 1, beam width >= 1, no randomness.
//! - Explores top-K children at each level (ordered by child state's score).
//! - Returns the best sequence (up to length `depth`) whose *total gain*
//!   over the current state exceeds `eps`.
//! - `Evaluator::feasible` can rule out illegal intermediate/final states.
//!
//! Prints (minimal, deterministic):
//! - search.step: depth, beam, eps
//! - search.best_single and search.best_seq with gains

use super::{CandidateGen, Evaluator, SearchAlgorithm, State};

#[derive(Clone, Debug)]
pub struct BeamConfig {
    /// Maximum look-ahead depth per planning call (>= 1).
    pub depth: usize,
    /// Beam width per expansion (>= 1).
    pub beam_width: usize,
    /// Require strictly positive total gain above this epsilon to accept.
    pub eps: f32,
    /// Optional cap on total score evaluations per call (0 = unlimited).
    pub max_evals: usize,
}

impl Default for BeamConfig {
    fn default() -> Self {
        Self {
            depth: 2,
            beam_width: 8,
            eps: 0.0,
            max_evals: 0,
        }
    }
}

pub struct BeamSearch {
    cfg: BeamConfig,
    evals: usize,
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

    fn score<S: State, E: Evaluator<S>>(&mut self, eval: &E, s: &S) -> Option<f32> {
        if !self.budget_ok() {
            return None;
        }
        self.evals += 1;
        if !eval.feasible(s) {
            return None;
        }
        Some(eval.score(s))
    }

    /// Return (gain, seq) for best sequence up to `depth` from `state`,
    /// relative to `base_score`. Deterministic.
    fn best_seq<S, G, E>(
        &mut self,
        state: &S,
        cand: &G,
        eval: &E,
        depth: usize,
        base_score: f32,
    ) -> (f32, Vec<S::Action>)
    where
        S: State,
        G: CandidateGen<S>,
        E: Evaluator<S>,
    {
        if depth == 0 {
            return (0.0, Vec::new());
        }

        // Generate children and pre-score them to order the beam.
        let mut scored: Vec<(f32, S::Action, S)> = Vec::new();
        for a in cand.candidates(state).into_iter() {
            let next = state.apply(&a);
            if let Some(s1) = self.score(eval, &next) {
                scored.push((s1, a, next));
            }
        }

        // No feasible children.
        if scored.is_empty() {
            return (0.0, Vec::new());
        }

        // Deterministic order: sort by child score desc, then debug-printable action.
        scored.sort_by(|(s_a, a_act, _), (s_b, b_act, _)| {
            s_b.partial_cmp(s_a)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| format!("{b_act:?}").cmp(&format!("{a_act:?}")))
        });

        // Keep top-K.
        if scored.len() > self.cfg.beam_width {
            scored.truncate(self.cfg.beam_width);
        }

        // Explore each child recursively and keep the best (by total gain).
        let mut best_gain = f32::NEG_INFINITY;
        let mut best_seq: Vec<S::Action> = Vec::new();

        for (s1, a1, next1) in scored.into_iter() {
            let gain1 = s1 - base_score;

            // If we still have depth, recurse from next1 with base s1.
            let (sub_gain, mut sub_seq) =
                self.best_seq(&next1, cand, eval, depth.saturating_sub(1), s1);
            let total_gain = gain1 + sub_gain;

            if total_gain > best_gain {
                best_gain = total_gain;
                let mut seq = Vec::with_capacity(1 + sub_seq.len());
                seq.push(a1);
                seq.append(&mut sub_seq);
                best_seq = seq;
            }
        }

        (best_gain, best_seq)
    }
}

impl<S: State> SearchAlgorithm<S> for BeamSearch {
    fn propose_step<G: CandidateGen<S>, E: Evaluator<S>>(
        &mut self,
        current: &S,
        cand: &G,
        eval: &E,
    ) -> Option<Vec<S::Action>> {
        self.reset_budget();

        // Score the current state once.
        let s0 = self.score(eval, current)?;
        println!(
            "search.step: depth={} beam={} eps={} s0={:.3}",
            self.cfg.depth, self.cfg.beam_width, self.cfg.eps, s0
        );

        // Depth-1 “best single” for visibility
        let (single_gain, single_seq) = {
            let mut tmp = Self::new(BeamConfig {
                depth: 1,
                beam_width: self.cfg.beam_width,
                eps: self.cfg.eps,
                max_evals: self.cfg.max_evals,
            });
            tmp.best_seq(current, cand, eval, 1, s0)
        };
        if let Some(a) = single_seq.get(0) {
            println!("search.best_single: gain={:.3} act={:?}", single_gain, a);
        } else {
            println!("search.best_single: none");
        }

        // Full depth best sequence.
        let (best_gain, best_seq) = self.best_seq(current, cand, eval, self.cfg.depth, s0);
        println!(
            "search.best_seq: depth={} gain={:.3} len={}",
            self.cfg.depth,
            best_gain,
            best_seq.len()
        );

        if best_gain > self.cfg.eps && !best_seq.is_empty() {
            Some(best_seq)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple deterministic toy domain:
    /// - State(val, staged). score = val.
    /// - Actions: Inc(n) increases val; Setup reduces val by 1 and toggles staged=true.
    /// - When staged=true, a large payoff Inc(5) becomes available.
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
                // IMPORTANT: only +1 or Setup before staging.
                // This removes the alternate +4 path that bypassed the valley,
                // making the two-step “escape” unique and deterministic.
                vec![A::Inc(1), A::Setup]
            } else {
                vec![A::Inc(5)]
            }
        }
    }

    struct E;
    impl Evaluator<S> for E {
        fn score(&self, s: &S) -> f32 {
            s.val as f32
        }
        fn feasible(&self, _s: &S) -> bool {
            true
        }
    }

    #[test]
    fn depth1_picks_best_single() {
        let mut algo = BeamSearch::new(BeamConfig {
            depth: 1,
            beam_width: 8,
            eps: 0.0,
            max_evals: 0,
        });
        let s0 = S {
            val: 0,
            staged: false,
        };
        let plan = algo.propose_step(&s0, &G, &E).expect("some plan");
        // With only Inc(1) or Setup available, depth-1 should pick Inc(1).
        assert_eq!(plan, vec![A::Inc(1)]);
    }

    #[test]
    fn depth2_escapes_local_minimum() {
        // With depth=2 and beam_width=2, planner must choose Setup -> Inc(5) for total gain 4.
        let mut algo = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 2,
            eps: 0.0,
            max_evals: 0,
        });
        let s0 = S {
            val: 0,
            staged: false,
        };
        let plan = algo.propose_step(&s0, &G, &E).expect("some plan");
        assert_eq!(plan, vec![A::Setup, A::Inc(5)]);
    }

    #[test]
    fn epsilon_blocks_small_improvements() {
        // Even with the good two-step plan (gain 4), eps=5 should reject.
        let mut algo = BeamSearch::new(BeamConfig {
            depth: 2,
            beam_width: 2,
            eps: 5.0,
            max_evals: 0,
        });
        let s0 = S {
            val: 0,
            staged: false,
        };
        let plan = algo.propose_step(&s0, &G, &E);
        assert!(plan.is_none(), "gain=4 should be rejected by eps=5");
    }
}

