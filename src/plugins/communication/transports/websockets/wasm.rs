#![cfg(all(feature = "ws_transport", feature = "ws_wasm", target_arch = "wasm32"))]

use std::collections::HashMap;
use std::sync::Arc;

use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::{BinaryType, MessageEvent, WebSocket, js_sys};

use tokio::sync::{RwLock, mpsc, oneshot};

use crate::plugins::communication::transports::{
    Connector, PeerId, PeerResolver, TransportConn, TransportError,
};

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

pub struct WsConnector;
impl WsConnector {
    pub fn new() -> Self {
        Self
    }
}

enum WsCmd {
    Send {
        bytes: Vec<u8>,
        ack: oneshot::Sender<Result<(), TransportError>>,
    },
    Close,
}

pub struct WsConn {
    cmd_tx: mpsc::UnboundedSender<WsCmd>,
    rx_once: std::sync::Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl TransportConn for WsConn {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(WsCmd::Send { bytes, ack: ack_tx })
            .map_err(|_| TransportError::ConnectionClosed)?;
        ack_rx
            .await
            .unwrap_or_else(|_| Err(TransportError::ConnectionClosed))
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
        let _ = self.cmd_tx.send(WsCmd::Close);
    }
}

#[async_trait::async_trait]
impl Connector for WsConnector {
    type Addr = WsAddr;
    async fn dial(&self, addr: &Self::Addr) -> Result<Arc<dyn TransportConn>, TransportError> {
        let ws = WebSocket::new(&addr.url)
            .map_err(|e| TransportError::Io(format!("ws new: {:?}", e)))?;
        ws.set_binary_type(BinaryType::Arraybuffer);
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);
        let onmessage = {
            let tx = tx.clone();
            Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
                if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let array = js_sys::Uint8Array::new(&abuf);
                    let mut v = vec![0; array.length() as usize];
                    array.copy_to(&mut v[..]);
                    let _ = tx.blocking_send(v);
                } else if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
                    let s = String::from(txt);
                    let _ = tx.blocking_send(s.into_bytes());
                }
            })
        };
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WsCmd>();
        let ws_loop = ws.clone();
        spawn_local(async move {
            ws_loop.set_binary_type(BinaryType::Arraybuffer);
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    WsCmd::Send { bytes, ack } => {
                        let res = ws_loop
                            .send_with_u8_array(&bytes)
                            .map_err(|e| TransportError::Io(format!("ws send: {:?}", e)));
                        let _ = ack.send(res);
                    }
                    WsCmd::Close => {
                        let _ = ws_loop.close();
                        break;
                    }
                }
            }
        });

        Ok(Arc::new(WsConn {
            cmd_tx,
            rx_once: std::sync::Mutex::new(Some(rx)),
        }))
    }
}
