pub mod inmem;
// pub mod quic; // TODO

use crate::uuid_bytes::UuidBytes;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;

/*──────── types from your minimal API ───────*/
pub type PeerId = UuidBytes;

#[derive(Debug)]
pub enum TransportError {
    ConnectionClosed,
    Io(String),
    Backpressure,
}

/*──────── peer addressing ──────────────*/
pub trait PeerAddr: Send + Sync + Clone + Debug {}
impl<T: Send + Sync + Clone + Debug> PeerAddr for T {}

#[async_trait::async_trait]
pub trait PeerResolver<A: PeerAddr>: Send + Sync {
    async fn resolve(&self, peer: PeerId) -> Result<A, TransportError>;
}

#[async_trait::async_trait]
pub trait Connector<A: PeerAddr>: Send + Sync {
    async fn dial(&self, addr: &A) -> Result<Arc<dyn TransportConn>, TransportError>;
}

/*──────── per-peer connection ──────────*/
#[async_trait::async_trait]
pub trait TransportConn: Send + Sync {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError>;
    fn recv(&self) -> mpsc::Receiver<Vec<u8>>;
    async fn close(&self);
}
