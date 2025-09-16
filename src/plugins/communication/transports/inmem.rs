use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::{RwLock, mpsc};

use crate::plugins::communication::transports::{
    Connector, PeerId, PeerResolver, TransportConn, TransportError,
};

/*──────────────────────────────────────────────────────────────────────────────*/
/* Address + Resolver                                                           */
/*──────────────────────────────────────────────────────────────────────────────*/

/// Address type for the in-memory transport (wraps logical PeerId).
#[derive(Clone, Debug)]
pub struct InMemAddr {
    pub peer: PeerId,
}

/// Resolves any PeerId to an InMemAddr with the same id.
pub struct InMemResolver;

#[async_trait::async_trait]
impl PeerResolver for InMemResolver {
    type Addr = InMemAddr;
    async fn resolve(&self, peer: PeerId) -> Result<Self::Addr, TransportError> {
        Ok(InMemAddr { peer })
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Global registry with per-test namespaces                                     */
/*──────────────────────────────────────────────────────────────────────────────*/

type ChanTx = mpsc::Sender<Vec<u8>>;
type ChanRx = mpsc::Receiver<Vec<u8>>;
type RxCell = Arc<Mutex<Option<ChanRx>>>;

// Keys are (namespace, src, dst)
type Key = (u64, PeerId, PeerId);

// Global maps, initialized on first use.
static SENDERS: OnceLock<RwLock<HashMap<Key, ChanTx>>> = OnceLock::new();
static RECEIVERS: OnceLock<RwLock<HashMap<Key, RxCell>>> = OnceLock::new();

#[inline]
fn senders() -> &'static RwLock<HashMap<Key, ChanTx>> {
    SENDERS.get_or_init(|| RwLock::new(HashMap::new()))
}
#[inline]
fn receivers() -> &'static RwLock<HashMap<Key, RxCell>> {
    RECEIVERS.get_or_init(|| RwLock::new(HashMap::new()))
}

async fn ensure_path(ns: u64, src: PeerId, dst: PeerId) {
    let key = (ns, src, dst);

    // Fast path: check under read lock.
    {
        let s_map = senders().read().await;
        if s_map.contains_key(&key) {
            return;
        }
    }

    // Slow path: upgrade to write and double-check (classic check-then-insert).
    let mut s_map = senders().write().await;
    if !s_map.contains_key(&key) {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);
        s_map.insert(key, tx);

        // Store the receiver under (ns, dst, src) so *dst* can later recv "from src".
        let mut r_map = receivers().write().await;
        r_map.insert((ns, dst, src), Arc::new(Mutex::new(Some(rx))));
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Per-peer connection                                                          */
/*──────────────────────────────────────────────────────────────────────────────*/

pub struct InMemConn {
    tx: ChanTx,
    // Handed out exactly once; further `recv()` calls return a closed channel.
    rx_once: Mutex<Option<ChanRx>>,
}

#[async_trait::async_trait]
impl TransportConn for InMemConn {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        self.tx
            .send(bytes)
            .await
            .map_err(|_| TransportError::ConnectionClosed)
    }

    fn recv(&self) -> ChanRx {
        let mut guard = self.rx_once.lock().expect("poisoned");
        if let Some(rx) = guard.take() {
            rx
        } else {
            // Return an already-closed channel on subsequent calls.
            let (_tx, rx) = mpsc::channel(1);
            rx
        }
    }

    async fn close(&self) {
        // No-op for in-mem; dropping tx/rx closes paths naturally.
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Connector (binds a local peer + namespace; dials remote peer)               */
/*──────────────────────────────────────────────────────────────────────────────*/

pub struct InMemConnector {
    ns: u64,
    local: PeerId,
}

impl InMemConnector {
    /// Create a connector with default namespace 0 (not isolated).
    pub fn new(local: PeerId) -> Self {
        Self { ns: 0, local }
    }

    /// Create a connector bound to a specific namespace (use a fresh one per test).
    pub fn with_namespace(local: PeerId, ns: u64) -> Self {
        Self { ns, local }
    }
}

#[async_trait::async_trait]
impl Connector for InMemConnector {
    type Addr = InMemAddr;
    async fn dial(&self, addr: &Self::Addr) -> Result<Arc<dyn TransportConn>, TransportError> {
        let remote = addr.peer;

        // Ensure both directed paths exist in this namespace.
        ensure_path(self.ns, self.local, remote).await;
        ensure_path(self.ns, remote, self.local).await;

        // Sender for local → remote
        let tx = {
            let s_map = senders().read().await;
            s_map
                .get(&(self.ns, self.local, remote))
                .ok_or_else(|| TransportError::Io("missing sender".into()))?
                .clone()
        };

        // Receiver for remote → local; take it exactly once.
        let rx_arc = {
            let r_map = receivers().read().await;
            r_map
                .get(&(self.ns, self.local, remote))
                .ok_or_else(|| TransportError::Io("missing receiver".into()))?
                .clone()
        };

        let mut guard = rx_arc.lock().expect("poisoned");
        let rx = guard.take().ok_or(TransportError::ConnectionClosed)?;

        Ok(Arc::new(InMemConn {
            tx,
            rx_once: Mutex::new(Some(rx)),
        }))
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Tests                                                                        */
/*──────────────────────────────────────────────────────────────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::task;

    // Unique namespace per test so they can run in parallel without global collisions.
    static NEXT_NS: AtomicU64 = AtomicU64::new(1);
    fn ns() -> u64 {
        NEXT_NS.fetch_add(1, Ordering::Relaxed)
    }

    fn pid(x: u8) -> PeerId {
        let mut b = [0u8; 16];
        b[0] = x;
        PeerId::from_bytes(b)
    }

    #[tokio::test]
    async fn basic_send_receive() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // B listens for messages from A
        let mut rx_b = conn_b_to_a.recv();

        // A sends bytes to B
        conn_a_to_b.send_bytes(b"hello".to_vec()).await.unwrap();
        assert_eq!(rx_b.recv().await.unwrap(), b"hello");

        // Round-trip back
        let mut rx_a = conn_a_to_b.recv();
        conn_b_to_a.send_bytes(b"pong".to_vec()).await.unwrap();
        assert_eq!(rx_a.recv().await.unwrap(), b"pong");
    }

    #[tokio::test]
    async fn multiple_messages_order_preserved() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let mut rx_b = conn_b.recv();

        for i in 0..10u8 {
            conn_a.send_bytes(vec![i]).await.unwrap();
        }
        for i in 0..10u8 {
            assert_eq!(rx_b.recv().await.unwrap(), vec![i]);
        }
    }

    #[tokio::test]
    async fn recv_only_once_returns_closed_after() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // First recv works
        let mut rx_b = conn_b.recv();

        // Second recv returns closed channel
        let mut rx_b2 = conn_b.recv();

        conn_a.send_bytes(b"one".to_vec()).await.unwrap();
        assert_eq!(rx_b.recv().await.unwrap(), b"one".to_vec());

        // The second receiver should be closed and yield None
        assert!(rx_b2.recv().await.is_none());
    }

    #[tokio::test]
    async fn send_before_remote_dials_is_buffered() {
        // A sends before B dials; messages queue and deliver once B dials.
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let addr_b = r.resolve(pid(2)).await.unwrap();
        let addr_a = r.resolve(pid(1)).await.unwrap();

        // A dials and sends early (creates local→remote path)
        let conn_a = a.dial(&addr_b).await.unwrap();
        conn_a.send_bytes(b"early".to_vec()).await.unwrap();

        // Now B dials and receives the queued message
        let conn_b = b.dial(&addr_a).await.unwrap();
        let mut rx_b = conn_b.recv();
        assert_eq!(rx_b.recv().await.unwrap(), b"early".to_vec());
    }

    #[tokio::test]
    async fn connection_closed_error_when_rx_taken_twice() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let r = InMemResolver;

        let _first = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let second = a.dial(&r.resolve(pid(2)).await.unwrap()).await;
        assert!(matches!(second, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn dropping_receiver_causes_sender_error() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // Take and drop B's receiver
        let _rx_b = conn_b.recv();
        drop(_rx_b);

        // Now A's send should error (no receiver)
        let err = conn_a.send_bytes(b"lost".to_vec()).await.unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }

    #[tokio::test]
    async fn concurrent_senders_all_deliver() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();
        let mut rx_b = conn_b.recv();

        // Fire 100 tasks sending concurrently
        let mut tasks = Vec::new();
        for i in 0..100u16 {
            let c = conn_a.clone();
            tasks.push(task::spawn(async move {
                c.send_bytes(i.to_le_bytes().to_vec()).await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        // Collect 100 messages
        let mut seen = Vec::new();
        for _ in 0..100 {
            let v = rx_b.recv().await.unwrap();
            seen.push(u16::from_le_bytes([v[0], v[1]]));
        }
        seen.sort_unstable();
        assert_eq!(seen.first(), Some(&0));
        assert_eq!(seen.last(), Some(&99));
    }

    #[tokio::test]
    async fn isolation_between_pairs() {
        // A↔B traffic shouldn't leak to C
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let c = InMemConnector::with_namespace(pid(3), ns);
        let r = InMemResolver;

        // A→B and B→A
        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // C→A and (important) A←C receiver
        let conn_c_to_a = c.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();
        let conn_a_from_c = a.dial(&r.resolve(pid(3)).await.unwrap()).await.unwrap();

        let mut rx_b = conn_b_to_a.recv(); // receive A→B on B side
        let mut rx_a_from_c = conn_a_from_c.recv(); // receive C→A on A side

        // Send on the two independent directions
        conn_a_to_b.send_bytes(b"to_b".to_vec()).await.unwrap();
        conn_c_to_a.send_bytes(b"to_a".to_vec()).await.unwrap();

        assert_eq!(rx_b.recv().await.unwrap(), b"to_b".to_vec());
        assert_eq!(rx_a_from_c.recv().await.unwrap(), b"to_a".to_vec());
    }
}
