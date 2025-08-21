use crate::plugins::harmonizer::{harmonizer::PeerContext, optimizer::AvailabilityDraft};

use async_trait::async_trait;

use crate::KuramotoDb;

/// Async scorer so implementors can hit the DB (or a cached context) per evaluation.
/// Keep this minimal: one pure score method; callers decide how/when to cache.
#[async_trait]
pub trait Scorer: Send + Sync {
    async fn score(&self, db: &KuramotoDb, ctx: &PeerContext, cand: &AvailabilityDraft) -> f32;
}
