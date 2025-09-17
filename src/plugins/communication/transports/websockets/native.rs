#![cfg(all(
    feature = "ws_transport",
    feature = "ws_native",
    not(target_arch = "wasm32")
))]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{RwLock, mpsc},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::plugins::communication::transports::{
    Connector, PeerId, PeerResolver, TransportConn, TransportError,
};

/*──────────── Address + Resolvers ───────────*/

#[derive(Clone, Debug)]
pub struct WsAddr {
    pub peer: PeerId,
    pub url: String,
}

pub struct WsResolver {
    table: Arc<RwLock<HashMap<PeerId, WsAddr>>>,
}
impl WsResolver {
    pub fn new() -> Self {
        Self {
            table: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn set(&self, peer: PeerId, addr: WsAddr) {
        self.table.write().await.insert(peer, addr);
    }
}
#[async_trait::async_trait]
impl PeerResolver for WsResolver {
    type Addr = WsAddr;
    async fn resolve(&self, peer: PeerId) -> Result<Self::Addr, TransportError> {
        self.table
            .read()
            .await
            .get(&peer)
            .cloned()
            .ok_or_else(|| TransportError::Io("ws: peer not found".into()))
    }
}

pub struct WsUriResolver {
    table: Arc<RwLock<HashMap<PeerId, WsAddr>>>,
}
impl WsUriResolver {
    pub fn new() -> Self {
        Self {
            table: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn set_uri(&self, peer: PeerId, uri: &str) -> Result<(), TransportError> {
        let addr = parse_ws_uri(peer, uri)?;
        self.table.write().await.insert(peer, addr);
        Ok(())
    }
}
#[async_trait::async_trait]
impl PeerResolver for WsUriResolver {
    type Addr = WsAddr;
    async fn resolve(&self, peer: PeerId) -> Result<Self::Addr, TransportError> {
        self.table
            .read()
            .await
            .get(&peer)
            .cloned()
            .ok_or_else(|| TransportError::Io("ws: peer not found".into()))
    }
}

fn parse_ws_uri(peer: PeerId, uri: &str) -> Result<WsAddr, TransportError> {
    let s = uri.trim();
    if !(s.starts_with("ws://") || s.starts_with("wss://")) {
        return Err(TransportError::Io(
            "ws uri must start with ws:// or wss://".into(),
        ));
    }
    Ok(WsAddr {
        peer,
        url: s.to_string(),
    })
}

/*──────────── Connector + Conn ─────────────*/

pub struct WsConnector;
impl WsConnector {
    pub fn new() -> Self {
        Self
    }
}

pub struct WsConn {
    writer: Arc<
        tokio::sync::Mutex<
            futures_util::stream::SplitSink<
                WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
                Message,
            >,
        >,
    >,
    rx_once: std::sync::Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl TransportConn for WsConn {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        let mut w = self.writer.lock().await;
        w.send(Message::Binary(bytes))
            .await
            .map_err(|e| TransportError::Io(format!("ws send: {e}")))
    }
    fn recv(&self) -> mpsc::Receiver<Vec<u8>> {
        let mut g = self.rx_once.lock().expect("poisoned");
        if let Some(rx) = g.take() {
            rx
        } else {
            let (_t, rx) = mpsc::channel(1);
            rx
        }
    }
    async fn close(&self) {
        let _ = self.writer.lock().await.send(Message::Close(None)).await;
    }
}

#[async_trait::async_trait]
impl Connector for WsConnector {
    type Addr = WsAddr;
    async fn dial(&self, addr: &Self::Addr) -> Result<Arc<dyn TransportConn>, TransportError> {
        let (ws, _resp) = connect_async(&addr.url)
            .await
            .map_err(|e| TransportError::Io(format!("ws connect: {e}")))?;
        let (writer, mut reader) = ws.split();

        let (tx_bytes, rx_bytes) = mpsc::channel::<Vec<u8>>(1024);
        tokio::spawn(async move {
            while let Some(msg) = reader.next().await {
                match msg {
                    Ok(Message::Binary(b)) => {
                        let _ = tx_bytes.send(b).await;
                    }
                    Ok(Message::Text(s)) => {
                        let _ = tx_bytes.send(s.into_bytes()).await;
                    }
                    Ok(Message::Close(_)) => break,
                    _ => {}
                }
            }
        });

        Ok(Arc::new(WsConn {
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            rx_once: std::sync::Mutex::new(Some(rx_bytes)),
        }))
    }
}

/*──────────── Test echo server helper ─────────────*/

async fn spawn_echo_server() -> Result<(SocketAddr, tokio::task::JoinHandle<()>), TransportError> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|e| TransportError::Io(format!("bind: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| TransportError::Io(format!("local_addr: {e}")))?;
    let h = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                    while let Some(m) = ws.next().await {
                        if let Ok(msg) = m {
                            let _ = ws.send(msg).await;
                        } else {
                            break;
                        }
                    }
                }
            });
        }
    });
    Ok((addr, h))
}

/*──────────── Tests ─────────────*/

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uri_resolver_parses_and_resolves() {
        let p = PeerId::from_bytes([1; 16]);
        let r = WsUriResolver::new();
        r.set_uri(p, "ws://localhost:1234").await.unwrap();
        let a = r.resolve(p).await.unwrap();
        assert_eq!(a.url, "ws://localhost:1234");
        assert_eq!(a.peer, p);
    }

    #[tokio::test]
    async fn websocket_roundtrip_against_echo_server() {
        let (addr, _h) = spawn_echo_server().await.unwrap();
        let url = format!("ws://{}", addr);
        let p = PeerId::from_bytes([2; 16]);
        let conn = WsConnector::new()
            .dial(&WsAddr { peer: p, url })
            .await
            .unwrap();
        let mut rx = conn.recv();
        conn.send_bytes(b"ping".to_vec()).await.unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got, b"ping".to_vec());
    }
}
