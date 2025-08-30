use std::sync::Arc;

use async_trait::async_trait;
use tokio::time::{Duration, timeout};

use crate::plugins::communication::router::{Router, RouterError};
use crate::plugins::communication::transports::PeerId;
use crate::plugins::harmonizer::protocol::{HarmonizerMsg, HarmonizerResp, GetChildrenByRange, PROTO_HARMONIZER};
use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::plugins::harmonizer::scorers::PeerReplicationProbe;

/// Router-backed replication probe that asks connected peers for headers-only coverage.
pub struct RouterProbe {
    pub router: Arc<Router>,
    pub peers: Arc<tokio::sync::RwLock<Vec<PeerId>>>,
    pub timeout: Duration,
}

impl RouterProbe {
    pub fn new(router: Arc<Router>, peers: Arc<tokio::sync::RwLock<Vec<PeerId>>>) -> Self {
        Self { router, peers, timeout: Duration::from_millis(500) }
    }
    pub fn with_timeout(mut self, d: Duration) -> Self { self.timeout = d; self }
}

#[async_trait]
impl PeerReplicationProbe for RouterProbe {
    async fn count_replicas_for(&self, target: &RangeCube) -> usize {
        let peers = self.peers.read().await.clone();
        let mut count = 0usize;
        for p in peers {
            let req = HarmonizerMsg::GetChildrenByRange(GetChildrenByRange { range: target.clone(), cursor: None, max: 1 });
            let fut = self.router.request_on::<HarmonizerMsg, HarmonizerResp>(PROTO_HARMONIZER, p, &req, self.timeout);
            match fut.await {
                Ok(HarmonizerResp::Children(resp)) => {
                    if !resp.headers.is_empty() {
                        count += 1;
                    }
                }
                _ => { /* ignore timeouts/errors */ }
            }
        }
        count
    }
}

