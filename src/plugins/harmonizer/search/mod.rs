//! Generic, deterministic search interface for planning action sequences
//! over a hypothetical state (no DB side-effects).
//!
//! Plug different planners (beam, greedy, etc.) behind `SearchAlgorithm`.

use std::fmt::Debug;

pub mod beam;

/// A clonable, hypothetical state of the world.
/// Apply an action to obtain the next hypothetical state.
pub trait State: Clone {
    type Action: Clone + Debug;

    /// Pure transition (no side-effects).
    fn apply(&self, action: &Self::Action) -> Self;
}

/// Generates candidate actions for a given state.
/// Deterministic and domain-specific.
pub trait CandidateGen<S: State> {
    fn candidates(&self, state: &S) -> Vec<S::Action>;
}

/// Scores a *whole* hypothetical state (global objective).
/// Also provides a feasibility guard for hard constraints.
pub trait Evaluator<S: State> {
    /// Global objective. Larger is better.
    fn score(&self, state: &S) -> f32;

    /// Hard guard: return false to reject the state outright.
    /// Default is no guard.
    fn feasible(&self, _state: &S) -> bool {
        true
    }
}

/// One-step planner: from a given state, propose a (possibly multi-action)
/// batch to apply atomically now. Returns None if no improving batch.
pub trait SearchAlgorithm<S: State> {
    fn propose_step<G: CandidateGen<S>, E: Evaluator<S>>(
        &mut self,
        current: &S,
        cand: &G,
        eval: &E,
    ) -> Option<Vec<S::Action>>;
}
