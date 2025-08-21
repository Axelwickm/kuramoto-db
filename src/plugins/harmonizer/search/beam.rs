//! Deterministic N-step beam look-ahead planner (async).
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
//!
//! Notes:
//! - This async version keeps the original semantics but avoids recursion so we
//!   can `.await` scoring at every expansion without additional deps.

use super::{CandidateGen, Evaluator, SearchAlgorithm, State};
use async_trait::async_trait;

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
}

#[async_trait]
impl<S> SearchAlgorithm<S> for BeamSearch
where
    S: State + Send + Sync,
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

        // Score the current state once.
        let s0 = self.score(eval, current).await?;
        println!(
            "search.step: depth={} beam={} eps={} s0={:.3}",
            self.cfg.depth, self.cfg.beam_width, self.cfg.eps, s0
        );

        // Helper record kept on the frontier.
        #[derive(Clone)]
        struct Node<S: State> {
            state: S,
            score: f32,          // absolute score of `state`
            seq: Vec<S::Action>, // actions taken from the root to reach `state`
        }

        // ----- Depth 1: evaluate children of `current` (also used for best_single print) -----
        let mut first_level: Vec<(f32, S::Action, S)> = Vec::new();
        for a in cand.candidates(current).into_iter() {
            let next = current.apply(&a);
            if let Some(s1) = self.score(eval, &next).await {
                first_level.push((s1, a, next));
            }
        }

        if first_level.is_empty() {
            println!("search.best_single: none");
            println!(
                "search.best_seq: depth={} gain={:.3} len=0",
                self.cfg.depth, 0.0
            );
            return None;
        }

        // Deterministic order: by score desc, tie-break by Debug(action)
        first_level.sort_by(|(s_a, a_act, _), (s_b, b_act, _)| {
            s_b.partial_cmp(s_a)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| format!("{b_act:?}").cmp(&format!("{a_act:?}")))
        });

        // Best single for visibility
        let best_single_gain = first_level[0].0 - s0;
        println!(
            "search.best_single: gain={:.3} act={:?}",
            best_single_gain, first_level[0].1
        );

        // Beam frontier after depth=1
        if first_level.len() > self.cfg.beam_width {
            first_level.truncate(self.cfg.beam_width);
        }
        let mut frontier: Vec<Node<S>> = first_level
            .into_iter()
            .map(|(sc, act, st)| Node {
                state: st,
                score: sc,
                seq: vec![act],
            })
            .collect();

        // Track the best sequence (any depth up to cfg.depth) by total gain over s0.
        let mut best_gain = best_single_gain;
        let mut best_seq = frontier.get(0).map(|n| n.seq.clone()).unwrap_or_default();

        // If the configured depth is 1, we're done.
        if self.cfg.depth == 1 {
            if best_gain > self.cfg.eps && !best_seq.is_empty() {
                return Some(best_seq);
            } else {
                return None;
            }
        }

        // ----- Depth >= 2: iteratively expand the frontier -----
        for _d in 2..=self.cfg.depth {
            let mut expanded: Vec<Node<S>> = Vec::new();

            for node in &frontier {
                for a in cand.candidates(&node.state).into_iter() {
                    let next = node.state.apply(&a);
                    if let Some(s2) = self.score(eval, &next).await {
                        let mut seq2 = node.seq.clone();
                        seq2.push(a);
                        // Absolute score ordering (same as original code)
                        expanded.push(Node {
                            state: next,
                            score: s2,
                            seq: seq2,
                        });
                        // Track best-so-far across *all* levels
                        let gain = s2 - s0;
                        if gain > best_gain {
                            best_gain = gain;
                            best_seq = expanded.last().unwrap().seq.clone();
                        }
                    }
                }
            }

            if expanded.is_empty() {
                break; // no more feasible children
            }

            // Deterministic order: by score desc, tie-break by the sequence's Debug (stable string)
            expanded.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| format!("{:?}", b.seq).cmp(&format!("{:?}", a.seq)))
            });
            if expanded.len() > self.cfg.beam_width {
                expanded.truncate(self.cfg.beam_width);
            }
            frontier = expanded;
        }

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
    use async_trait::async_trait;

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

    #[tokio::test]
    async fn depth1_picks_best_single() {
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
        let plan = algo.propose_step(&s0, &G, &E).await.expect("some plan");
        // With only Inc(1) or Setup available, depth-1 should pick Inc(1).
        assert_eq!(plan, vec![A::Inc(1)]);
    }

    #[tokio::test]
    async fn depth2_escapes_local_minimum() {
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
        let plan = algo.propose_step(&s0, &G, &E).await.expect("some plan");
        assert_eq!(plan, vec![A::Setup, A::Inc(5)]);
    }

    #[tokio::test]
    async fn epsilon_blocks_small_improvements() {
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
        let plan = algo.propose_step(&s0, &G, &E).await;
        assert!(plan.is_none(), "gain=4 should be rejected by eps=5");
    }
}
