use crate::plugins::harmonizer::{
    harmonizer::PeerContext,
    optimizer::{ActionSet, AvailabilityDraft},
    range_cube::RangeCube,
};
use redb::ReadTransaction;

use async_trait::async_trait;
use std::any::Any;

pub mod server_scorer;

use crate::KuramotoDb;
use crate::uuid_bytes::UuidBytes;

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

    fn as_any(&self) -> &dyn Any
    where
        Self: 'static + Sized,
    {
        self
    }
}

#[async_trait]
pub trait PeerReplicationProbe: Send + Sync {
    async fn count_replicas_for(
        &self,
        target: &crate::plugins::harmonizer::range_cube::RangeCube,
    ) -> usize;
}
