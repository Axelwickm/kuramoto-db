use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bincode::{Decode, Encode};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    clock::Clock,
    communication::{
        rate_limiter::RateLimiter,
        transports::{PeerId, TransportConn, TransportError},
    },
};

/*──────────────────────── framing ───────────────────────*/

pub type MsgId = u64;

/// Verbs for the RPC envelope.
/// - `Request`: caller expects a `Response` or `Error`
/// - `Response`: success path containing protocol-specific bytes
/// - `Notify`: fire-and-forget
/// - `Error`: standardized failure envelope carrying `StdError`
#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum Verb {
    Request,
    Response,
    Notify,
    Error,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Envelope {
    pub id: MsgId,
    pub correl: Option<MsgId>,
    pub verb: Verb,

    /// Protocol namespace. Each middleware owns one (or more) proto ids.
    pub proto: u16,

    /// Wire version. `major` must match exactly; `minor` must be >= our `minor`.
    pub major: u16,
    pub minor: u16,

    pub payload: Vec<u8>,
}

/*──────────────────────── standardized error ───────────*/

#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
pub struct StdError {
    pub code: u16,
    pub msg: String,
}

/*──────────────────────── errors (no thiserror) ─────────*/

#[derive(Debug)]
pub enum RouterError {
    NoConnection,
    Transport(TransportError),
    Timeout,
    Encode(String),
    Decode(String),
    Denied(&'static str),
    Remote { code: u16, msg: String },
}
impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RouterError::*;
        match self {
            NoConnection => write!(f, "no connection"),
            Transport(e) => write!(f, "transport: {:?}", e),
            Timeout => write!(f, "timeout"),
            Encode(e) => write!(f, "encode: {e}"),
            Decode(e) => write!(f, "decode: {e}"),
            Denied(r) => write!(f, "denied: {r}"),
            Remote { code, msg } => write!(f, "remote error {code}: {msg}"),
        }
    }
}

/*──────────────────────── config / DoS guards ──────────*/

#[derive(Clone, Debug)]
pub struct RouterConfig {
    pub version_major: u16,
    pub version_minor: u16,
    pub default_timeout: Duration,
    pub max_frame_bytes: usize,
    pub per_peer_inflight: usize,
    pub per_peer_send_q: usize,
    pub rate_per_sec: u32, // used for both in/out buckets (for now)
    pub burst: u32,        // used for both in/out buckets (for now)
    pub strike_limit: u32,
}
impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            version_major: 1,
            version_minor: 0,
            default_timeout: Duration::from_secs(5),
            max_frame_bytes: 512 * 1024,
            per_peer_inflight: 256,
            per_peer_send_q: 1024,
            rate_per_sec: 200,
            burst: 400,
            strike_limit: 5,
        }
    }
}

/*──────────────────────── handler API ──────────────────*/

#[async_trait::async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Handle a request and return encoded response bytes.
    async fn on_request(&self, peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String>;
    /// Handle a notify (no response).
    async fn on_notify(&self, peer: PeerId, body: &[u8]) -> Result<(), String>;
}

/*──────────────────────── peer state ───────────────────*/

struct PeerState {
    // concurrency cap (safe against races)
    sem: Arc<Semaphore>,
    // simple strike counter
    strikes: u32,
    // rate limits
    in_bucket: RateLimiter,  // throttle inbound processing
    out_bucket: RateLimiter, // self-throttle outbound sends
}

/*──────────────────────── internal types ───────────────*/

#[derive(Copy, Clone, Eq, PartialEq)]
enum FrameClass {
    /// Subject to outbound rate limiting.
    Normal,
    /// Bypasses outbound rate limiting (responses/errors).
    Bypass,
}

struct Pending {
    peer: PeerId,
    tx: oneshot::Sender<Result<Envelope, RouterError>>,
}

/*──────────────────────── Router ───────────────────────*/

pub struct Router {
    cfg: RouterConfig,
    clock: Arc<dyn Clock>,
    next_id: AtomicU64,

    // connections & per-peer send workers
    conns: Mutex<HashMap<PeerId, Arc<dyn TransportConn>>>,
    workers: Mutex<HashMap<PeerId, mpsc::Sender<Vec<u8>>>>,
    state: Mutex<HashMap<PeerId, PeerState>>,

    // pending requests (id -> waiter with owning peer)
    pending: Mutex<HashMap<MsgId, Pending>>,

    // protocol handlers (keyed by `proto`)
    handlers: RwLock<HashMap<u16, Arc<dyn Handler>>>,
}

impl Router {
    pub fn new(cfg: RouterConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            clock,
            next_id: AtomicU64::new(1),
            conns: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            state: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            handlers: RwLock::new(HashMap::new()),
        })
    }

    #[inline]
    fn alloc_id(&self) -> MsgId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register a handler for a protocol id.
    pub fn set_handler(&self, proto: u16, h: Arc<dyn Handler>) {
        let mut g = self.handlers.write().unwrap();
        g.insert(proto, h);
    }

    /// Attach a peer connection and spawn its read & send tasks.
    pub fn connect_peer(self: &Arc<Self>, peer: PeerId, conn: Arc<dyn TransportConn>) {
        info!(%peer, "router: connect_peer");

        // set up send worker (pre-encoded bytes)
        let (tx_bytes, mut rx_bytes) = mpsc::channel::<Vec<u8>>(self.cfg.per_peer_send_q);

        {
            let mut w = self.workers.lock().unwrap();
            w.insert(peer, tx_bytes.clone());
        }
        {
            let mut c = self.conns.lock().unwrap();
            c.insert(peer, conn.clone());
        }
        {
            let mut s = self.state.lock().unwrap();
            s.insert(
                peer,
                PeerState {
                    sem: Arc::new(Semaphore::new(self.cfg.per_peer_inflight)),
                    strikes: 0,
                    in_bucket: RateLimiter::new(
                        self.cfg.burst,
                        self.cfg.rate_per_sec,
                        self.clock.clone(),
                    ),
                    out_bucket: RateLimiter::new(
                        self.cfg.burst,
                        self.cfg.rate_per_sec,
                        self.clock.clone(),
                    ),
                },
            );
        }

        // SEND WORKER (per-peer) — uses this conn directly
        let this = Arc::clone(self);
        let conn_for_send = conn.clone();
        tokio::spawn(async move {
            while let Some(bytes) = rx_bytes.recv().await {
                if let Err(_e) = conn_for_send.send_bytes(bytes).await {
                    warn!(%peer, "router: send failed, dropping peer");
                    this.drop_peer(peer);
                    break;
                }
            }
            debug!(%peer, "router: send worker exit");
        });

        // READ LOOP — move the receiver into the task (don’t store it in a map)
        let mut rx_frames = conn.recv();
        let this2 = Arc::clone(self);
        tokio::spawn(async move {
            this2.read_loop(peer, &mut rx_frames).await;
        });
    }

    fn strike(&self, peer: PeerId, why: &'static str) {
        let mut should_drop = false;
        if let Some(st) = self.state.lock().unwrap().get_mut(&peer) {
            st.strikes = st.strikes.saturating_add(1);
            warn!(%peer, strikes = st.strikes, why, "router: strike");
            if st.strikes >= self.cfg.strike_limit {
                should_drop = true;
            }
        }
        if should_drop {
            warn!(%peer, "router: strike limit reached, dropping");
            self.drop_peer(peer);
        }
    }

    fn fail_correl(&self, correl: Option<MsgId>, err: RouterError) {
        if let Some(cid) = correl {
            if let Some(p) = self.pending.lock().unwrap().remove(&cid) {
                let _ = p.tx.send(Err(err));
            }
        }
    }

    fn drop_peer(&self, peer: PeerId) {
        debug!(%peer, "router: drop_peer");
        self.workers.lock().unwrap().remove(&peer);
        self.conns.lock().unwrap().remove(&peer);
        self.state.lock().unwrap().remove(&peer);

        // Fail all pendings for this peer immediately (don't let them timeout)
        let mut pend = self.pending.lock().unwrap();
        let to_fail: Vec<MsgId> = pend
            .iter()
            .filter_map(|(id, p)| if p.peer == peer { Some(*id) } else { None })
            .collect();
        for id in to_fail {
            if let Some(p) = pend.remove(&id) {
                let _ = p.tx.send(Err(RouterError::NoConnection));
            }
        }
    }

    #[inline]
    fn inbound_version_ok(&self, major: u16, minor: u16) -> bool {
        major == self.cfg.version_major && minor >= self.cfg.version_minor
    }

    async fn send_error(
        &self,
        peer: PeerId,
        correl: MsgId,
        proto: u16,
        code: u16,
        msg: impl Into<String>,
    ) {
        // Encode StdError payload
        let payload = match bincode::encode_to_vec(
            StdError {
                code,
                msg: msg.into(),
            },
            bincode::config::standard(),
        ) {
            Ok(p) => p,
            Err(e) => {
                warn!(peer=?peer, ?e, "router: failed to encode StdError payload");
                return;
            }
        };

        // Build envelope
        let env = Envelope {
            id: self.alloc_id(),
            correl: Some(correl),
            verb: Verb::Error,
            proto,
            major: self.cfg.version_major,
            minor: self.cfg.version_minor,
            payload,
        };

        // Encode envelope
        let bytes = match bincode::encode_to_vec(&env, bincode::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                warn!(peer=?peer, ?e, "router: failed to encode error envelope");
                return;
            }
        };

        // Oversize guard
        if bytes.len() > self.cfg.max_frame_bytes {
            warn!(peer=?peer, len = bytes.len(), "router: error frame too large, dropping");
            return;
        }

        // Clone sender without holding lock across await
        let tx_opt = {
            let g = self.workers.lock().unwrap();
            g.get(&peer).cloned()
        };

        if let Some(tx) = tx_opt {
            if let Err(e) = tx.send(bytes).await {
                warn!(peer=?peer, ?e, "router: failed to send error frame");
            }
        } else {
            debug!(peer=?peer, "router: no worker found for peer when sending error");
        }
    }

    #[instrument(skip_all, fields(%peer))]
    async fn read_loop(&self, peer: PeerId, rx: &mut mpsc::Receiver<Vec<u8>>) {
        debug!(%peer, "router: read loop start");
        while let Some(bytes) = rx.recv().await {
            if bytes.len() > self.cfg.max_frame_bytes {
                warn!(%peer, len = bytes.len(), "router: inbound oversize");
                self.strike(peer, "oversize");
                continue;
            }
            let env: Envelope =
                match bincode::decode_from_slice(&bytes, bincode::config::standard())
                    .map(|(v, _)| v)
                {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%peer, ?e, "router: bad decode");
                        self.strike(peer, "bad decode");
                        continue;
                    }
                };

            match env.verb {
                Verb::Response => {
                    if !self.inbound_version_ok(env.major, env.minor) {
                        warn!(%peer, "router: resp version mismatch");
                        self.strike(peer, "resp version");
                        continue;
                    }
                    if let Some(cid) = env.correl {
                        if let Some(p) = self.pending.lock().unwrap().remove(&cid) {
                            let _ = p.tx.send(Ok(env));
                        }
                    }
                }
                Verb::Error => {
                    if let Some(cid) = env.correl {
                        let remote = match bincode::decode_from_slice::<StdError, _>(
                            &env.payload,
                            bincode::config::standard(),
                        ) {
                            Ok((se, _)) => RouterError::Remote {
                                code: se.code,
                                msg: se.msg,
                            },
                            Err(_) => RouterError::Remote {
                                code: 500,
                                msg: "malformed remote error".into(),
                            },
                        };
                        if let Some(p) = self.pending.lock().unwrap().remove(&cid) {
                            let _ = p.tx.send(Err(remote));
                        }
                    }
                }
                Verb::Request | Verb::Notify => {
                    // Version gate first
                    if !self.inbound_version_ok(env.major, env.minor) {
                        if matches!(env.verb, Verb::Request) {
                            self.send_error(peer, env.id, env.proto, 400, "incompatible version")
                                .await;
                        }
                        self.strike(peer, "req/notify version");
                        continue;
                    }

                    // inbound rate-limit via token bucket
                    let allowed = {
                        let mut g = self.state.lock().unwrap();
                        if let Some(st) = g.get_mut(&peer) {
                            st.in_bucket.try_acquire(1)
                        } else {
                            false
                        }
                    };
                    if !allowed {
                        // Soft drop and strike for requests; drop for notifies.
                        if matches!(env.verb, Verb::Request) {
                            self.send_error(peer, env.id, env.proto, 429, "rate limited")
                                .await;
                        }
                        self.strike(peer, "rate");
                        continue;
                    }

                    // lookup handler
                    let handler = {
                        let g = self.handlers.read().unwrap();
                        g.get(&env.proto).cloned()
                    };
                    let Some(h) = handler else {
                        if matches!(env.verb, Verb::Request) {
                            self.send_error(peer, env.id, env.proto, 404, "no handler")
                                .await;
                        }
                        continue;
                    };

                    let body = env.payload.as_slice();
                    if matches!(env.verb, Verb::Request) {
                        match h.on_request(peer, body).await {
                            Ok(resp_bytes) => {
                                let reply = Envelope {
                                    id: self.alloc_id(),
                                    correl: Some(env.id),
                                    verb: Verb::Response,
                                    proto: env.proto,
                                    major: self.cfg.version_major,
                                    minor: self.cfg.version_minor,
                                    payload: resp_bytes,
                                };
                                // Responses bypass outbound rate limiting to avoid spurious timeouts.
                                let _ = self.enqueue(peer, reply, FrameClass::Bypass).await;
                            }
                            Err(msg) => {
                                self.send_error(peer, env.id, env.proto, 500, msg).await;
                            }
                        }
                    } else {
                        let _ = h.on_notify(peer, body).await;
                    }
                }
            }
        }
        debug!(%peer, "router: read loop exit");
    }

    /// Pre-encode and send; enforce max-frame and outbound rate-limit (unless bypass).
    async fn enqueue(
        &self,
        peer: PeerId,
        env: Envelope,
        class: FrameClass,
    ) -> Result<(), RouterError> {
        // Pre-encode for error propagation and size check.
        let bytes = bincode::encode_to_vec(&env, bincode::config::standard())
            .map_err(|e| RouterError::Encode(e.to_string()))?;

        if bytes.len() > self.cfg.max_frame_bytes {
            warn!(%peer, len = bytes.len(), "router: outbound oversize");
            // If this was a response we owe the caller a small error frame.
            if matches!(env.verb, Verb::Response) {
                if let Some(correl) = env.correl {
                    self.send_error(peer, correl, env.proto, 413, "response too large")
                        .await;
                }
            }
            // Also notify our own waiter if any (only meaningful for locally-correlated sends).
            self.fail_correl(env.correl, RouterError::Denied("frame too large"));
            return Err(RouterError::Denied("frame too large"));
        }

        // Outbound rate limiting unless bypassed.
        if class == FrameClass::Normal {
            let allowed = {
                let mut g = self.state.lock().unwrap();
                if let Some(st) = g.get_mut(&peer) {
                    st.out_bucket.try_acquire(1)
                } else {
                    false
                }
            };
            if !allowed {
                return Err(RouterError::Denied("outbound rate"));
            }
        }

        // Send via per-peer worker
        let tx = {
            let g = self.workers.lock().unwrap();
            g.get(&peer).cloned()
        }
        .ok_or(RouterError::NoConnection)?;

        tx.send(bytes).await.map_err(|_| RouterError::NoConnection)
    }

    /*──────── public API ────────*/

    /// Fire-and-forget notify on a specific protocol.
    pub async fn notify_on<M: Encode>(
        &self,
        proto: u16,
        peer: PeerId,
        msg: &M,
    ) -> Result<(), RouterError> {
        let payload = bincode::encode_to_vec(msg, bincode::config::standard())
            .map_err(|e| RouterError::Encode(e.to_string()))?;
        let env = Envelope {
            id: self.alloc_id(),
            correl: None,
            verb: Verb::Notify,
            proto,
            major: self.cfg.version_major,
            minor: self.cfg.version_minor,
            payload,
        };
        self.enqueue(peer, env, FrameClass::Normal).await
    }

    /// Request/response on a specific protocol.
    pub async fn request_on<Req: Encode, Resp: Decode<()>>(
        &self,
        proto: u16,
        peer: PeerId,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, RouterError> {
        // acquire a permit; deny immediately if none
        let permit = {
            let g = self.state.lock().unwrap();
            let st = g.get(&peer).ok_or(RouterError::NoConnection)?;
            st.sem
                .clone()
                .try_acquire_owned()
                .map_err(|_| RouterError::Denied("peer inflight limit"))?
        };
        let _permit = permit; // RAII: releases when dropped

        let payload = bincode::encode_to_vec(req, bincode::config::standard())
            .map_err(|e| RouterError::Encode(e.to_string()))?;
        let id = self.alloc_id();
        let env = Envelope {
            id,
            correl: None,
            verb: Verb::Request,
            proto,
            major: self.cfg.version_major,
            minor: self.cfg.version_minor,
            payload,
        };

        let (tx, rx) = oneshot::channel();
        {
            self.pending
                .lock()
                .unwrap()
                .insert(id, Pending { peer, tx });
        }
        if let Err(e) = self.enqueue(peer, env, FrameClass::Normal).await {
            self.pending.lock().unwrap().remove(&id);
            return Err(e);
        }

        // await response with timeout
        let r = tokio::time::timeout(timeout, rx).await;
        if r.is_err() {
            // timeout
            self.pending.lock().unwrap().remove(&id);
            return Err(RouterError::Timeout); // `_permit` drops here
        }
        let env = r.unwrap().map_err(|_| RouterError::Timeout)??;

        // decode response body
        let (resp, _) =
            bincode::decode_from_slice::<Resp, _>(&env.payload, bincode::config::standard())
                .map_err(|e| RouterError::Decode(e.to_string()))?;
        Ok(resp) // `_permit` drops here
    }

    /*──────── optional convenience (legacy, proto 0) ────────*/

    pub async fn notify<M: Encode>(&self, peer: PeerId, msg: &M) -> Result<(), RouterError> {
        self.notify_on(0, peer, msg).await
    }

    pub async fn request<Req: Encode, Resp: Decode<()>>(
        &self,
        peer: PeerId,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, RouterError> {
        self.request_on(0, peer, req, timeout).await
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Tests (uses in-memory transport + MockClock)                                  */
/*──────────────────────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::transports::{
        Connector, PeerResolver,
        inmem::{InMemConnector, InMemResolver},
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::{
        sync::{mpsc, oneshot},
        task::yield_now,
        time::{Duration as TDuration, advance},
    };

    // Unique namespace per test for in-mem transport isolation
    static NEXT_NS: AtomicU64 = AtomicU64::new(100);
    fn ns() -> u64 {
        NEXT_NS.fetch_add(1, Ordering::Relaxed)
    }

    fn pid(x: u8) -> PeerId {
        let mut b = [0u8; 16];
        b[0] = x;
        PeerId::from_bytes(b)
    }

    // One protocol id for all tests
    const PROTO_TEST: u16 = 1;

    #[derive(Debug, Encode, Decode, PartialEq)]
    enum Msg {
        Ping(u32),
        Pong(u32),
        Add { a: i32, b: i32 },
        Sum(i32),
        Notify(String),
        SleepMs(u64),
        Error(u16, String),
    }

    struct TestHandler {
        notify_tx: mpsc::Sender<String>,
    }

    #[async_trait::async_trait]
    impl Handler for TestHandler {
        async fn on_request(&self, _peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String> {
            let (m, _) = bincode::decode_from_slice::<Msg, _>(body, bincode::config::standard())
                .map_err(|e| e.to_string())?;
            match m {
                Msg::Ping(x) => bincode::encode_to_vec(Msg::Pong(x), bincode::config::standard())
                    .map_err(|e| e.to_string()),
                Msg::Add { a, b } => {
                    bincode::encode_to_vec(Msg::Sum(a + b), bincode::config::standard())
                        .map_err(|e| e.to_string())
                }
                Msg::SleepMs(ms) => {
                    // Sleep uses Tokio time; tests advance virtual time.
                    tokio::time::sleep(TDuration::from_millis(ms)).await;
                    bincode::encode_to_vec(Msg::Pong(42), bincode::config::standard())
                        .map_err(|e| e.to_string())
                }
                Msg::Error(code, msg) => Err(format!("E{code}:{msg}")),
                _ => bincode::encode_to_vec(
                    Msg::Error(400, "bad req".into()),
                    bincode::config::standard(),
                )
                .map_err(|e| e.to_string()),
            }
        }

        async fn on_notify(&self, _peer: PeerId, body: &[u8]) -> Result<(), String> {
            let (m, _) = bincode::decode_from_slice::<Msg, _>(body, bincode::config::standard())
                .map_err(|e| e.to_string())?;
            if let Msg::Notify(s) = m {
                let _ = self.notify_tx.try_send(s);
            }
            Ok(())
        }
    }

    // GateHandler lets us deterministically hold the handler “open”
    // so we know the inflight semaphore is actually acquired.
    struct GateHandler {
        entered_tx: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
        release_rx: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait]
    impl Handler for GateHandler {
        async fn on_request(&self, _peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String> {
            // Signal we've entered handler (permit held)
            if let Some(tx) = self.entered_tx.lock().await.take() {
                let _ = tx.send(());
            }
            // Wait for release signal
            if let Some(rx) = self.release_rx.lock().await.take() {
                let _ = rx.await;
            }

            let (m, _) = bincode::decode_from_slice::<Msg, _>(body, bincode::config::standard())
                .map_err(|e| e.to_string())?;
            match m {
                Msg::Ping(x) => bincode::encode_to_vec(Msg::Pong(x), bincode::config::standard())
                    .map_err(|e| e.to_string()),
                _ => bincode::encode_to_vec(Msg::Pong(42), bincode::config::standard())
                    .map_err(|e| e.to_string()),
            }
        }

        async fn on_notify(&self, _peer: PeerId, _body: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn basic_request_response() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let clock = Arc::new(crate::clock::MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (ntx, _nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(PROTO_TEST, Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);

        yield_now().await; // let tasks spin

        let resp: Msg = router_a
            .request_on(
                PROTO_TEST,
                pid(2),
                &Msg::Ping(7),
                Duration::from_millis(200),
            )
            .await
            .unwrap();
        assert_eq!(resp, Msg::Pong(7));

        let resp2: Msg = router_a
            .request_on(
                PROTO_TEST,
                pid(2),
                &Msg::Add { a: 3, b: 5 },
                Duration::from_millis(200),
            )
            .await
            .unwrap();
        assert_eq!(resp2, Msg::Sum(8));
    }

    #[tokio::test(start_paused = true)]
    async fn notify_is_delivered() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let clock = Arc::new(crate::clock::MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (ntx, mut nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(PROTO_TEST, Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);

        yield_now().await;

        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("hi".into()))
            .await
            .unwrap();

        // drive the scheduler a tick so recv gets polled
        advance(TDuration::from_millis(1)).await;

        let got = nrx.recv().await.unwrap();
        assert_eq!(got, "hi");
    }

    #[tokio::test(start_paused = true)]
    async fn request_times_out() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let clock = Arc::new(crate::clock::MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (_ntx, _nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(PROTO_TEST, Arc::new(TestHandler { notify_tx: _ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        let fut = router_a.request_on::<Msg, Msg>(
            PROTO_TEST,
            pid(2),
            &Msg::SleepMs(200),
            Duration::from_millis(50),
        );

        // Advance Tokio time to trigger timeout deterministically
        advance(TDuration::from_millis(50)).await;

        let err = fut.await.unwrap_err();
        assert!(matches!(err, RouterError::Timeout));
    }

    #[tokio::test(start_paused = true)]
    async fn inflight_limit_enforced() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let mut cfg = RouterConfig::default();
        cfg.per_peer_inflight = 1;

        let clock = Arc::new(crate::clock::MockClock::new(0));
        let router_a = Router::new(cfg, clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (entered_tx, entered_rx) = oneshot::channel();
        let (tx_release, rx_release) = oneshot::channel();

        router_b.set_handler(
            PROTO_TEST,
            Arc::new(GateHandler {
                entered_tx: tokio::sync::Mutex::new(Some(entered_tx)),
                release_rx: tokio::sync::Mutex::new(Some(rx_release)),
            }),
        );

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        // Kick off first request so it acquires the permit
        let ra = router_a.clone();
        let fut1 = tokio::spawn(async move {
            ra.request_on::<Msg, Msg>(PROTO_TEST, pid(2), &Msg::Ping(123), Duration::from_secs(10))
                .await
        });

        // Wait until handler actually started (permit is held)
        entered_rx.await.unwrap();

        // Second should be denied immediately
        let err = router_a
            .request_on::<Msg, Msg>(
                PROTO_TEST,
                pid(2),
                &Msg::Ping(1),
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RouterError::Denied(_)));

        // Let the first finish
        let _ = tx_release.send(());
        advance(TDuration::from_millis(1)).await;
        let _ = fut1.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn handler_error_is_returned_as_remote_error() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let clock = Arc::new(crate::clock::MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (ntx, _nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(PROTO_TEST, Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        let err = router_a
            .request_on::<Msg, Msg>(
                PROTO_TEST,
                pid(2),
                &Msg::Error(418, "teapot".into()),
                Duration::from_millis(200),
            )
            .await
            .unwrap_err();

        match err {
            RouterError::Remote { code, msg } => {
                assert_eq!(code, 500, "server maps handler Err(...) to 500");
                assert_eq!(msg, "E418:teapot");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn inbound_rate_limiter_drops_excess_until_refill() {
        use tokio::sync::mpsc::error::TryRecvError;

        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // B has tight inbound limits (burst=1, 1 token/sec)
        let mut cfg_b = RouterConfig::default();
        cfg_b.burst = 1;
        cfg_b.rate_per_sec = 1;

        let clock = Arc::new(crate::clock::MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(cfg_b, clock.clone());

        let (ntx, mut nrx) = mpsc::channel::<String>(16);
        router_b.set_handler(PROTO_TEST, Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Send three quickly; B should accept only the first now
        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("a".into()))
            .await
            .unwrap();
        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("b".into()))
            .await
            .unwrap();
        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("c".into()))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // First should arrive — bounded spin, no timeouts
        let first = {
            let mut got = None;
            for _ in 0..50 {
                match nrx.try_recv() {
                    Ok(v) => {
                        got = Some(v);
                        break;
                    }
                    Err(TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(TryRecvError::Disconnected) => panic!("channel closed"),
                }
            }
            got.expect("deliver first notify within bounded spins")
        };
        assert_eq!(first, "a");

        // No second yet (bucket empty)
        match nrx.try_recv() {
            Err(TryRecvError::Empty) => (), // good
            Ok(v) => panic!("unexpected second notify: {:?}", v),
            Err(e) => panic!("channel error: {e:?}"),
        }

        // Refill inbound via MockClock, then send another
        clock.advance(1).await;
        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("d".into()))
            .await
            .unwrap();

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Drain again to get 'd' — bounded spin
        let d = {
            let mut got = None;
            for _ in 0..50 {
                match nrx.try_recv() {
                    Ok(v) => {
                        got = Some(v);
                        break;
                    }
                    Err(TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(TryRecvError::Disconnected) => panic!("channel closed"),
                }
            }
            got.expect("deliver 'd' notify within bounded spins")
        };
        assert_eq!(d, "d");
    }

    #[tokio::test(start_paused = true)]
    async fn oversize_response_maps_to_413() {
        use tokio::task::yield_now;

        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let clock = Arc::new(crate::clock::MockClock::new(0));

        // A can receive the error frame; B is tight so the response trips oversize.
        let mut cfg_a = RouterConfig::default();
        cfg_a.max_frame_bytes = 4096;

        let mut cfg_b = RouterConfig::default();
        cfg_b.max_frame_bytes = 256;

        let router_a = Router::new(cfg_a, clock.clone());
        let router_b = Router::new(cfg_b, clock.clone());

        struct BigResp;
        #[async_trait::async_trait]
        impl Handler for BigResp {
            async fn on_request(&self, _peer: PeerId, _body: &[u8]) -> Result<Vec<u8>, String> {
                Ok(vec![0u8; 32_000]) // guarantees oversize after envelope
            }
            async fn on_notify(&self, _peer: PeerId, _body: &[u8]) -> Result<(), String> {
                Ok(())
            }
        }
        router_b.set_handler(PROTO_TEST, Arc::new(BigResp));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);

        // Let workers spin up deterministically.
        yield_now().await;
        yield_now().await;

        let err = router_a
            .request_on::<Msg, Msg>(PROTO_TEST, pid(2), &Msg::Ping(1), Duration::from_secs(1))
            .await
            .unwrap_err();

        match err {
            RouterError::Remote { code, .. } => assert_eq!(code, 413, "should map oversize to 413"),
            other => panic!("expected Remote 413, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn outbound_self_throttle_denies_then_refills_with_mock_clock() {
        use tokio::sync::mpsc::error::TryRecvError;

        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        let clock = Arc::new(crate::clock::MockClock::new(0));

        // A is constrained on outbound
        let mut cfg_a = RouterConfig::default();
        cfg_a.burst = 1;
        cfg_a.rate_per_sec = 1;

        let router_a = Router::new(cfg_a, clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (ntx, mut nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(PROTO_TEST, Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // 1) First notify succeeds (bucket full)
        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("x".into()))
            .await
            .unwrap();

        // 2) Second notify is denied locally (bucket empty)
        let e = router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("y".into()))
            .await
            .unwrap_err();
        assert!(matches!(e, RouterError::Denied(r) if r == "outbound rate"));

        // Drain deliveries (bounded spin, deterministic)
        let first = {
            let mut got = None;
            for _ in 0..50 {
                match nrx.try_recv() {
                    Ok(v) => {
                        got = Some(v);
                        break;
                    }
                    Err(TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(TryRecvError::Disconnected) => panic!("channel closed"),
                }
            }
            got.expect("first notify should arrive")
        };
        assert_eq!(first, "x");
        assert!(matches!(nrx.try_recv(), Err(TryRecvError::Empty)));

        // 3) Refill bucket and send again
        clock.advance(1).await;
        router_a
            .notify_on(PROTO_TEST, pid(2), &Msg::Notify("z".into()))
            .await
            .unwrap();

        // Confirm only 'z' arrives next
        let z = {
            let mut got = None;
            for _ in 0..50 {
                match nrx.try_recv() {
                    Ok(v) => {
                        got = Some(v);
                        break;
                    }
                    Err(TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(TryRecvError::Disconnected) => panic!("channel closed"),
                }
            }
            got.expect("'z' should arrive after refill")
        };
        assert_eq!(z, "z");
    }
}
