pub mod inmem;
// QUIC transport is under construction. Re-enable when ready:
// pub mod quic;
// WebRTC transport intentionally disabled while pivoting to WebSockets:
// #[cfg(feature = "webrtc_transport")]
// pub mod webrtc;
// WebSockets transport (native + wasm backends via features)
#[cfg(feature = "ws_transport")]
pub mod websockets;

use crate::uuid_bytes::UuidBytes;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;

/*──────── types from your minimal API ───────*/
pub type PeerId = UuidBytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    ConnectionClosed,
    Io(String),
    Backpressure,
}

/*──────── peer addressing ──────────────*/
pub trait PeerAddr: Send + Sync + Clone + Debug {}
impl<T: Send + Sync + Clone + Debug> PeerAddr for T {}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait PeerResolver: Send + Sync {
    type Addr: PeerAddr;
    async fn resolve(&self, peer: PeerId) -> Result<Self::Addr, TransportError>;
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait Connector: Send + Sync {
    type Addr: PeerAddr;
    async fn dial(&self, addr: &Self::Addr) -> Result<Arc<dyn TransportConn>, TransportError>;
}

/*──────── per-peer connection ──────────*/
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait TransportConn: Send + Sync {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError>;
    fn recv(&self) -> mpsc::Receiver<Vec<u8>>;
    async fn close(&self);
}
