use crate::plugins::harmonizer::optimizer::{AvailabilityDraft, PeerContext};

pub mod server_scorer;

pub trait Scorer: Send + Sync {
    fn score(&self, ctx: &PeerContext, avail: &AvailabilityDraft) -> f32;
}
