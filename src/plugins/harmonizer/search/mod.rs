//! Generic, deterministic search interface for planning action sequences
//! over a hypothetical state (no DB side-effects).
//!
//! Plug different planners (beam, greedy, etc.) behind `SearchAlgorithm`.

use async_trait::async_trait;
use std::fmt::Debug;

pub mod beam;

/// A clonable, hypothetical state of the world.
/// Apply an action to obtain the next hypothetical state.
pub trait State: Clone {
    type Action: Clone + Debug + Send + Sync;

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
///
/// `score` is async to allow DB-backed evaluators/scorers.
/// `feasible` stays sync (pure guard) for cheap early rejections.
#[async_trait]
pub trait Evaluator<S>: Send + Sync
where
    S: State + Send + Sync,
{
    /// Global objective. Larger is better.
    async fn score(&self, state: &S) -> f32;

    /// Hard guard: return false to reject the state outright.
    /// Default is no guard.
    fn feasible(&self, _state: &S) -> bool {
        true
    }
}

/// One-step planner: from a given state, propose a (possibly multi-action)
/// batch to apply atomically now. Returns None if no improving batch.
#[async_trait]
pub trait SearchAlgorithm<S>: Send
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
        E: Evaluator<S> + Send + Sync;
}
