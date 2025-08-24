use crate::plugins::harmonizer::{
    harmonizer::PeerContext,
    optimizer::{ActionSet, AvailabilityDraft},
};
use redb::ReadTransaction;

use async_trait::async_trait;

pub mod server_scorer;

use crate::KuramotoDb;

/// Async scorer so implementors can hit the DB (or a cached context) per evaluation.
/// Keep this minimal: one pure score method; callers decide how/when to cache.
#[async_trait]
pub trait Scorer: Send + Sync {
    async fn score(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        ctx: &PeerContext,
        cand: &AvailabilityDraft,
        overlay: &ActionSet,
    ) -> f32;
}
