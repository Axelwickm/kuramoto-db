#![cfg(all(feature = "webrtc_transport", feature = "webrtc_native", not(target_arch = "wasm32")))]

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::plugins::communication::transports::{
    Connector, PeerId, PeerResolver, TransportConn, TransportError,
};

use webrtc::{
    api::media_engine::MediaEngine,
    api::APIBuilder,
    data_channel::data_channel_message::DataChannelMessage,
    ice_transport::ice_server::RTCIceServer,
    peer_connection::{
        configuration::RTCConfiguration,
        sdp::session_description::RTCSessionDescription,
    },
};

/*──────────────────────────────────────────────────────────────────────────────*/
/* Address + Resolvers                                                          */
/*──────────────────────────────────────────────────────────────────────────────*/

#[derive(Clone, Debug)]
pub struct RtcAddr {
    pub peer: PeerId,
    pub session: String,
    pub ice_servers: Vec<String>,
}

pub struct RtcResolver {
    table: Arc<RwLock<HashMap<PeerId, RtcAddr>>>,
}

impl RtcResolver {
    pub fn new() -> Self { Self { table: Arc::new(RwLock::new(HashMap::new())) } }
    pub async fn set(&self, peer: PeerId, addr: RtcAddr) { self.table.write().await.insert(peer, addr); }
}

#[async_trait::async_trait]
impl PeerResolver for RtcResolver {
    type Addr = RtcAddr;
    async fn resolve(&self, peer: PeerId) -> Result<Self::Addr, TransportError> {
        self.table.read().await.get(&peer).cloned().ok_or_else(|| TransportError::Io("webrtc: peer not found".into()))
    }
}

pub struct RtcUriResolver { table: Arc<RwLock<HashMap<PeerId, RtcAddr>>> }
impl RtcUriResolver {
    pub fn new() -> Self { Self { table: Arc::new(RwLock::new(HashMap::new())) } }
    pub async fn set_uri(&self, peer: PeerId, uri: &str) -> Result<(), TransportError> { let addr = parse_webrtc_uri(peer, uri)?; self.table.write().await.insert(peer, addr); Ok(()) }
}
#[async_trait::async_trait]
impl PeerResolver for RtcUriResolver {
    type Addr = RtcAddr;
    async fn resolve(&self, peer: PeerId) -> Result<Self::Addr, TransportError> {
        self.table.read().await.get(&peer).cloned().ok_or_else(|| TransportError::Io("webrtc: peer not found".into()))
    }
}

fn parse_webrtc_uri(peer: PeerId, uri: &str) -> Result<RtcAddr, TransportError> {
    let s = uri.trim();
    let prefix = "webrtc:";
    if !s.starts_with(prefix) { return Err(TransportError::Io("webrtc uri must start with 'webrtc:'".into())); }
    let rest = &s[prefix.len()..];
    let mut session = rest;
    let mut ice_servers: Vec<String> = Vec::new();
    if let Some(qidx) = rest.find('?') {
        session = &rest[..qidx];
        let query = &rest[qidx + 1..];
        for kv in query.split('&') {
            if let Some(eq) = kv.find('=') {
                let (k, v) = (&kv[..eq], &kv[eq + 1..]);
                if k == "ice" && !v.is_empty() { ice_servers = v.split(',').map(|s| s.to_string()).collect(); }
            }
        }
    }
    if session.is_empty() { return Err(TransportError::Io("webrtc uri missing session".into())); }
    Ok(RtcAddr { peer, session: session.to_string(), ice_servers })
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Signaling abstraction                                                        */
/*──────────────────────────────────────────────────────────────────────────────*/

#[async_trait::async_trait]
pub trait Signaling: Send + Sync {
    fn session(&self) -> &str;
    fn role_offer(&self) -> bool;
    async fn send_offer(&self, sdp: String);
    async fn recv_offer(&self) -> String;
    async fn send_answer(&self, sdp: String);
    async fn recv_answer(&self) -> String;
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Connector + Conn                                                              */
/*──────────────────────────────────────────────────────────────────────────────*/

pub struct RtcConnector<S: Signaling + 'static> { pub signaling: Arc<S> }
impl<S: Signaling + 'static> RtcConnector<S> { pub fn new(signaling: Arc<S>) -> Self { Self { signaling } } }

pub struct RtcConn {
    dc: Arc<webrtc::data_channel::RTCDataChannel>,
    rx_once: std::sync::Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl TransportConn for RtcConn {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        let data = bytes::Bytes::from(bytes);
        self.dc.send(&data).await.map(|_| ()).map_err(|e| TransportError::Io(format!("webrtc send: {e}")))
    }
    fn recv(&self) -> mpsc::Receiver<Vec<u8>> {
        let mut g = self.rx_once.lock().expect("poisoned");
        if let Some(rx) = g.take() { rx } else { let (_t, rx) = mpsc::channel(1); rx }
    }
    async fn close(&self) { let _ = self.dc.close().await; }
}

#[async_trait::async_trait]
impl<S: Signaling + 'static> Connector for RtcConnector<S> {
    type Addr = RtcAddr;
    async fn dial(&self, addr: &Self::Addr) -> Result<Arc<dyn TransportConn>, TransportError> {
        let m = MediaEngine::default();
        let api = APIBuilder::new().with_media_engine(m).build();
        let cfg = RTCConfiguration { ice_servers: vec![RTCIceServer { urls: addr.ice_servers.clone(), ..Default::default() }], ..Default::default() };
        let pc = api.new_peer_connection(cfg).await.map_err(|e| TransportError::Io(format!("pc: {e}")))?;

        let (tx_bytes, rx_bytes) = mpsc::channel::<Vec<u8>>(1024);

        if self.signaling.role_offer() {
            let dc = pc.create_data_channel("data", None).await.map_err(|e| TransportError::Io(format!("datachannel: {e}")))?;
            {
                let tx = tx_bytes.clone();
                dc.on_message(Box::new(move |msg: DataChannelMessage| {
                    let tx = tx.clone();
                    Box::pin(async move { let _ = tx.send(msg.data.to_vec()).await; })
                }));
            }
            let (opened_tx, opened_rx) = oneshot::channel::<()>();
            dc.on_open(Box::new(move || { let _ = opened_tx.send(()); Box::pin(async {}) }));

            let offer = pc.create_offer(None).await.map_err(|e| TransportError::Io(format!("offer: {e}")))?;
            pc.set_local_description(offer).await.map_err(|e| TransportError::Io(format!("set local: {e}")))?;
            let mut gather = pc.gathering_complete_promise().await; let _ = gather.recv().await;
            let local = pc.local_description().await.ok_or_else(|| TransportError::Io("missing local sdp".into()))?;
            self.signaling.send_offer(local.sdp).await;
            let ans_sdp = self.signaling.recv_answer().await;
            let answer = RTCSessionDescription::answer(ans_sdp).map_err(|e| TransportError::Io(format!("answer desc: {e}")))?;
            pc.set_remote_description(answer).await.map_err(|e| TransportError::Io(format!("set remote: {e}")))?;
            let _ = opened_rx.await;
            Ok(Arc::new(RtcConn { dc: Arc::clone(&dc), rx_once: std::sync::Mutex::new(Some(rx_bytes)) }))
        } else {
            let (dc_tx, dc_rx) = oneshot::channel::<Arc<webrtc::data_channel::RTCDataChannel>>();
            let (opened_tx, opened_rx) = oneshot::channel::<()>();
            let dc_tx_cell = Arc::new(std::sync::Mutex::new(Some(dc_tx)));
            let opened_tx_cell = Arc::new(std::sync::Mutex::new(Some(opened_tx)));

            pc.on_data_channel(Box::new(move |dc: Arc<webrtc::data_channel::RTCDataChannel>| {
                let dc_clone = dc.clone();
                let tx = tx_bytes.clone();
                dc.on_message(Box::new(move |msg: DataChannelMessage| {
                    let tx = tx.clone();
                    Box::pin(async move { let _ = tx.send(msg.data.to_vec()).await; })
                }));
                let opened_tx_cell2 = opened_tx_cell.clone();
                dc.on_open(Box::new(move || { if let Some(tx) = opened_tx_cell2.lock().unwrap().take() { let _ = tx.send(()); } Box::pin(async {}) }));
                if let Some(tx) = dc_tx_cell.lock().unwrap().take() { let _ = tx.send(dc_clone); }
                Box::pin(async {})
            }));

            let off_sdp = self.signaling.recv_offer().await;
            let offer = RTCSessionDescription::offer(off_sdp).map_err(|e| TransportError::Io(format!("offer desc: {e}")))?;
            pc.set_remote_description(offer).await.map_err(|e| TransportError::Io(format!("set remote: {e}")))?;
            let answer = pc.create_answer(None).await.map_err(|e| TransportError::Io(format!("answer: {e}")))?;
            pc.set_local_description(answer).await.map_err(|e| TransportError::Io(format!("set local: {e}")))?;
            let mut gather = pc.gathering_complete_promise().await; let _ = gather.recv().await;
            let local = pc.local_description().await.ok_or_else(|| TransportError::Io("missing local sdp".into()))?;
            self.signaling.send_answer(local.sdp).await;
            let _ = opened_rx.await;
            let dc = dc_rx.await.map_err(|_| TransportError::ConnectionClosed)?;
            Ok(Arc::new(RtcConn { dc, rx_once: std::sync::Mutex::new(Some(rx_bytes)) }))
        }
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Tests                                                                        */
/*──────────────────────────────────────────────────────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;

    struct MemSigSide {
        sess: String,
        offerer: bool,
        to_other_offer: mpsc::Sender<String>,
        from_other_offer: tokio::sync::Mutex<mpsc::Receiver<String>>,
        to_other_answer: mpsc::Sender<String>,
        from_other_answer: tokio::sync::Mutex<mpsc::Receiver<String>>,
    }

    #[async_trait::async_trait]
    impl Signaling for MemSigSide {
        fn session(&self) -> &str { &self.sess }
        fn role_offer(&self) -> bool { self.offerer }
        async fn send_offer(&self, sdp: String) { let _ = self.to_other_offer.send(sdp).await; }
        async fn recv_offer(&self) -> String { self.from_other_offer.lock().await.recv().await.unwrap() }
        async fn send_answer(&self, sdp: String) { let _ = self.to_other_answer.send(sdp).await; }
        async fn recv_answer(&self) -> String { self.from_other_answer.lock().await.recv().await.unwrap() }
    }

    struct MemSigPair;
    impl MemSigPair { fn new(session: &str) -> (Arc<MemSigSide>, Arc<MemSigSide>) {
        let (a_to_b_offer_tx, a_to_b_offer_rx) = mpsc::channel::<String>(1);
        let (b_to_a_offer_tx, b_to_a_offer_rx) = mpsc::channel::<String>(1);
        let (a_to_b_ans_tx, a_to_b_ans_rx) = mpsc::channel::<String>(1);
        let (b_to_a_ans_tx, b_to_a_ans_rx) = mpsc::channel::<String>(1);
        let a = Arc::new(MemSigSide { sess: session.to_string(), offerer: true, to_other_offer: a_to_b_offer_tx, from_other_offer: tokio::sync::Mutex::new(b_to_a_offer_rx), to_other_answer: b_to_a_ans_tx, from_other_answer: tokio::sync::Mutex::new(a_to_b_ans_rx) });
        let b = Arc::new(MemSigSide { sess: session.to_string(), offerer: false, to_other_offer: b_to_a_offer_tx, from_other_offer: tokio::sync::Mutex::new(a_to_b_offer_rx), to_other_answer: a_to_b_ans_tx, from_other_answer: tokio::sync::Mutex::new(b_to_a_ans_rx) });
        (a, b)
    }}

    #[tokio::test]
    async fn resolver_maps_peer() {
        let r = RtcResolver::new();
        let p = PeerId::from_bytes([1; 16]);
        r.set(p, RtcAddr { peer: p, session: "s".into(), ice_servers: vec![] }).await;
        let got = r.resolve(p).await.unwrap();
        assert_eq!(got.peer, p);
        assert_eq!(got.session, "s");
    }

    #[tokio::test]
    async fn uri_resolver_parses_basic_and_ice() {
        let p = PeerId::from_bytes([9; 16]);
        let r = RtcUriResolver::new();
        r.set_uri(p, "webrtc:room1").await.unwrap();
        let a = r.resolve(p).await.unwrap();
        assert_eq!(a.session, "room1");
        assert!(a.ice_servers.is_empty());

        let r2 = RtcUriResolver::new();
        r2.set_uri(p, "webrtc:room2?ice=stun:stun.example.org:3478,turn:turn.example.org:3478").await.unwrap();
        let a2 = r2.resolve(p).await.unwrap();
        assert_eq!(a2.session, "room2");
        assert_eq!(a2.ice_servers.len(), 2);
        assert!(a2.ice_servers[0].starts_with("stun:"));
        assert!(a2.ice_servers[1].starts_with("turn:"));
    }

    #[tokio::test]
    async fn webrtc_datachannel_roundtrip() {
        let p1 = PeerId::from_bytes([1; 16]);
        let p2 = PeerId::from_bytes([2; 16]);
        let (sig_a, sig_b) = MemSigPair::new("sess1");
        let a = RtcConnector::new(sig_a);
        let b = RtcConnector::new(sig_b);
        let addr_a = RtcAddr { peer: p1, session: "sess1".into(), ice_servers: vec![] };
        let addr_b = RtcAddr { peer: p2, session: "sess1".into(), ice_servers: vec![] };
        let (conn_a_res, conn_b_res) = tokio::join!(a.dial(&addr_b), b.dial(&addr_a));
        let conn_a = conn_a_res.expect("conn a");
        let conn_b = conn_b_res.expect("conn b");
        let mut rx_b = conn_b.recv();
        conn_a.send_bytes(b"hello".to_vec()).await.unwrap();
        let got = rx_b.recv().await.unwrap();
        assert_eq!(got, b"hello".to_vec());
    }
}

