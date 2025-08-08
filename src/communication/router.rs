use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bincode::{Decode, Encode};
use dashmap::DashMap;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::{
    clock::Clock,
    communication::{
        rate_limiter::RateLimiter,
        transports::{PeerId, TransportConn, TransportError},
    },
};

/*──────────────────────── framing ───────────────────────*/

pub type MsgId = u64;

#[derive(Clone, Copy, Debug, Encode, Decode)]
pub enum Verb {
    Request,
    Response,
    Notify,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Envelope {
    pub id: MsgId,
    pub correl: Option<MsgId>,
    pub verb: Verb,
    pub major: u16,
    pub minor: u16,
    pub payload: Vec<u8>,
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

/*──────────────────────── Router ───────────────────────*/

pub struct Router {
    cfg: RouterConfig,
    clock: Arc<dyn Clock>,
    next_id: AtomicU64,

    // connections & per-peer send workers
    conns: DashMap<PeerId, Arc<dyn TransportConn>>,
    workers: DashMap<PeerId, mpsc::Sender<Envelope>>,
    state: DashMap<PeerId, PeerState>,

    // pending requests (id -> waiter)
    pending: DashMap<MsgId, oneshot::Sender<Result<Envelope, RouterError>>>,

    // single-protocol handler for now
    handler: DashMap<(), Arc<dyn Handler>>,
}

impl Router {
    pub fn new(cfg: RouterConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            clock,
            next_id: AtomicU64::new(1),
            conns: DashMap::new(),
            workers: DashMap::new(),
            state: DashMap::new(),
            pending: DashMap::new(),
            handler: DashMap::new(),
        })
    }

    #[inline]
    fn alloc_id(&self) -> MsgId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn set_handler(&self, h: Arc<dyn Handler>) {
        self.handler.insert((), h);
    }

    /// Attach a peer connection and spawn its read & send tasks.
    pub fn connect_peer(self: &Arc<Self>, peer: PeerId, conn: Arc<dyn TransportConn>) {
        // set up send worker
        let (tx_env, mut rx_env) = mpsc::channel::<Envelope>(self.cfg.per_peer_send_q);
        self.workers.insert(peer, tx_env.clone());
        self.conns.insert(peer, conn.clone());
        self.state.insert(
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

        // SEND WORKER (per-peer)
        let this = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(env) = rx_env.recv().await {
                let bytes = match bincode::encode_to_vec(&env, bincode::config::standard()) {
                    Ok(b) if b.len() <= this.cfg.max_frame_bytes => b,
                    Ok(_) => {
                        this.strike(peer, "frame too large");
                        continue;
                    }
                    Err(e) => {
                        this.fail_correl(env.correl, RouterError::Encode(e.to_string()));
                        continue;
                    }
                };

                match this.conns.get(&peer) {
                    Some(c) => {
                        if let Err(_e) = c.send_bytes(bytes).await {
                            // transport died; drop peer (requests will timeout)
                            this.drop_peer(peer);
                            break;
                        }
                    }
                    None => {
                        this.drop_peer(peer);
                        break;
                    }
                }
            }
        });

        // READ LOOP — move the receiver into the task (don’t store it in a map)
        let mut rx_bytes = conn.recv();
        let this2 = Arc::clone(self);
        tokio::spawn(async move {
            this2.read_loop(peer, &mut rx_bytes).await;
        });
    }

    fn strike(&self, peer: PeerId, _why: &'static str) {
        if let Some(mut s) = self.state.get_mut(&peer) {
            s.strikes += 1;
            if s.strikes >= self.cfg.strike_limit {
                self.drop_peer(peer);
            }
        }
    }

    fn fail_correl(&self, correl: Option<MsgId>, err: RouterError) {
        if let Some(cid) = correl {
            if let Some((_, waiter)) = self.pending.remove(&cid) {
                let _ = waiter.send(Err(err));
            }
        }
    }

    fn drop_peer(&self, peer: PeerId) {
        self.workers.remove(&peer);
        self.conns.remove(&peer);
        self.state.remove(&peer);
        // pending entries are left to timeout
    }

    async fn read_loop(&self, peer: PeerId, rx: &mut mpsc::Receiver<Vec<u8>>) {
        while let Some(bytes) = rx.recv().await {
            if bytes.len() > self.cfg.max_frame_bytes {
                self.strike(peer, "oversize");
                continue;
            }
            let env: Envelope =
                match bincode::decode_from_slice(&bytes, bincode::config::standard())
                    .map(|(v, _)| v)
                {
                    Ok(v) => v,
                    Err(_) => {
                        self.strike(peer, "bad decode");
                        continue;
                    }
                };

            match env.verb {
                Verb::Response => {
                    if let Some(cid) = env.correl {
                        if let Some((_, waiter)) = self.pending.remove(&cid) {
                            let _ = waiter.send(Ok(env));
                        }
                    }
                }
                Verb::Request | Verb::Notify => {
                    // inbound rate-limit via token bucket
                    if let Some(mut st) = self.state.get_mut(&peer) {
                        if !st.in_bucket.try_acquire(1) {
                            self.strike(peer, "rate");
                            continue;
                        }
                    }

                    let Some(h) = self.handler.get(&()) else {
                        // no handler registered yet -> drop
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
                                    major: self.cfg.version_major,
                                    minor: self.cfg.version_minor,
                                    payload: resp_bytes,
                                };
                                let _ = self.enqueue(peer, reply).await;
                            }
                            Err(_msg) => {
                                // optional: send standardized error frames later
                            }
                        }
                    } else {
                        let _ = h.on_notify(peer, body).await;
                    }
                }
            }
        }
        // channel closed -> done
    }

    async fn enqueue(&self, peer: PeerId, env: Envelope) -> Result<(), RouterError> {
        // self-throttle outbound
        if let Some(mut st) = self.state.get_mut(&peer) {
            if !st.out_bucket.try_acquire(1) {
                return Err(RouterError::Denied("outbound rate"));
            }
        } else {
            return Err(RouterError::NoConnection);
        }

        let w = self.workers.get(&peer).ok_or(RouterError::NoConnection)?;
        w.send(env).await.map_err(|_| RouterError::NoConnection)
    }

    /*──────── public API ────────*/

    pub async fn notify<M: Encode>(&self, peer: PeerId, msg: &M) -> Result<(), RouterError> {
        let payload = bincode::encode_to_vec(msg, bincode::config::standard())
            .map_err(|e| RouterError::Encode(e.to_string()))?;
        let env = Envelope {
            id: self.alloc_id(),
            correl: None,
            verb: Verb::Notify,
            major: self.cfg.version_major,
            minor: self.cfg.version_minor,
            payload,
        };
        self.enqueue(peer, env).await
    }

    pub async fn request<Req: Encode, Resp: Decode<()>>(
        &self,
        peer: PeerId,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, RouterError> {
        // acquire a permit; deny immediately if none
        let permit = {
            let st = self.state.get(&peer).ok_or(RouterError::NoConnection)?;
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
            major: self.cfg.version_major,
            minor: self.cfg.version_minor,
            payload,
        };

        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        self.enqueue(peer, env).await?;

        // await response with timeout
        let r = tokio::time::timeout(timeout, rx).await;
        if r.is_err() {
            // timeout
            self.pending.remove(&id);
            return Err(RouterError::Timeout); // `_permit` drops here
        }
        let env = r.unwrap().map_err(|_| RouterError::Timeout)??;

        // decode response body
        let (resp, _) =
            bincode::decode_from_slice::<Resp, _>(&env.payload, bincode::config::standard())
                .map_err(|e| RouterError::Decode(e.to_string()))?;
        Ok(resp) // `_permit` drops here
    }
}

/*──────────────────────────────────────────────────────────────────────────────*/
/* Tests (uses in-memory transport + MockClock)                                  */
/*──────────────────────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::transports::inmem::{InMemConnector, InMemResolver};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::{
        sync::{Notify, mpsc, oneshot},
        task::yield_now,
        time::{Duration as TDuration, advance, timeout},
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
                let _ = self.notify_tx.send(s).await;
            }
            Ok(())
        }
    }

    // GateHandler lets us deterministically hold the handler “open”
    // so we know the inflight semaphore is actually acquired.
    struct GateHandler {
        started: Arc<Notify>,
        release_rx: oneshot::Receiver<()>,
    }

    #[async_trait::async_trait]
    impl Handler for GateHandler {
        async fn on_request(&self, _peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String> {
            // Signal we've entered handler (permit held, decoding done)
            self.started.notify_waiters();

            // Wait until test tells us to finish
            let _ = &self.release_rx.await;

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

        let clock = Arc::new(MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (ntx, _nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);

        yield_now().await; // let tasks spin

        let resp: Msg = router_a
            .request(pid(2), &Msg::Ping(7), Duration::from_millis(200))
            .await
            .unwrap();
        assert_eq!(resp, Msg::Pong(7));

        let resp2: Msg = router_a
            .request(pid(2), &Msg::Add { a: 3, b: 5 }, Duration::from_millis(200))
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

        let clock = Arc::new(MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (ntx, mut nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);

        yield_now().await;

        router_a
            .notify(pid(2), &Msg::Notify("hi".into()))
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

        let clock = Arc::new(MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let (_ntx, _nrx) = mpsc::channel::<String>(8);
        router_b.set_handler(Arc::new(TestHandler { notify_tx: _ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        let fut =
            router_a.request::<Msg, Msg>(pid(2), &Msg::SleepMs(200), Duration::from_millis(50));

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

        let clock = Arc::new(MockClock::new(0));
        let router_a = Router::new(cfg, clock.clone());
        let router_b = Router::new(RouterConfig::default(), clock.clone());

        let started = Arc::new(Notify::new());
        let (tx_release, rx_release) = oneshot::channel();
        router_b.set_handler(Arc::new(GateHandler {
            started: started.clone(),
            release_rx: rx_release,
        }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        // Kick off first request so it acquires the permit
        let fut1 = tokio::spawn(router_a.request::<Msg, Msg>(
            pid(2),
            &Msg::Ping(123),
            Duration::from_secs(10),
        ));

        // Wait until handler actually started (permit is held)
        started.notified().await;

        // Second should be denied immediately
        let err = router_a
            .request::<Msg, Msg>(pid(2), &Msg::Ping(1), Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(matches!(err, RouterError::Denied(_)));

        // Let the first finish
        let _ = tx_release.send(());
        advance(TDuration::from_millis(1)).await;
        let _ = fut1.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn outbound_self_throttle_denies_then_refills_with_mock_clock() {
        let ns = ns();
        let a = InMemConnector::with_namespace(pid(1), ns);
        let b = InMemConnector::with_namespace(pid(2), ns);
        let r = InMemResolver;

        let conn_a_to_b = a.dial(&r.resolve(pid(2)).await.unwrap()).await.unwrap();
        let conn_b_to_a = b.dial(&r.resolve(pid(1)).await.unwrap()).await.unwrap();

        // A: very tight outbound limit; B: default
        let mut cfg_a = RouterConfig::default();
        cfg_a.burst = 2;
        cfg_a.rate_per_sec = 1;
        let cfg_b = RouterConfig::default();

        let clock = Arc::new(MockClock::new(0));
        let router_a = Router::new(cfg_a, clock.clone());
        let router_b = Router::new(cfg_b, clock.clone());

        let (ntx, mut nrx) = mpsc::channel::<String>(16);
        router_b.set_handler(Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        // Two notifies should pass (burst=2)
        router_a
            .notify(pid(2), &Msg::Notify("x".into()))
            .await
            .unwrap();
        router_a
            .notify(pid(2), &Msg::Notify("y".into()))
            .await
            .unwrap();

        // Third should be denied by OUTBOUND bucket
        let err = router_a
            .notify(pid(2), &Msg::Notify("z".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, RouterError::Denied(_)));

        // Refill outbound via MockClock
        clock.advance(1);
        router_a
            .notify(pid(2), &Msg::Notify("w".into()))
            .await
            .unwrap();

        // Drive scheduler so deliveries happen
        advance(TDuration::from_millis(1)).await;

        // Collect delivered messages
        let mut got = vec![];
        for _ in 0..3 {
            if let Ok(s) = timeout(TDuration::from_millis(50), nrx.recv()).await {
                got.push(s.unwrap());
            }
        }
        assert!(got.contains(&"x".to_string()));
        assert!(got.contains(&"y".to_string()));
        assert!(got.contains(&"w".to_string()));
    }

    #[tokio::test(start_paused = true)]
    async fn inbound_rate_limiter_drops_excess_until_refill() {
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

        let clock = Arc::new(MockClock::new(0));
        let router_a = Router::new(RouterConfig::default(), clock.clone());
        let router_b = Router::new(cfg_b, clock.clone());

        let (ntx, mut nrx) = mpsc::channel::<String>(16);
        router_b.set_handler(Arc::new(TestHandler { notify_tx: ntx }));

        router_a.connect_peer(pid(2), conn_a_to_b);
        router_b.connect_peer(pid(1), conn_b_to_a);
        yield_now().await;

        // Send three quickly; B should accept only the first now
        router_a
            .notify(pid(2), &Msg::Notify("a".into()))
            .await
            .unwrap();
        router_a
            .notify(pid(2), &Msg::Notify("b".into()))
            .await
            .unwrap();
        router_a
            .notify(pid(2), &Msg::Notify("c".into()))
            .await
            .unwrap();

        // Drive scheduler
        advance(TDuration::from_millis(1)).await;

        // First should arrive
        let first = timeout(TDuration::from_millis(50), nrx.recv())
            .await
            .expect("deliver first")
            .expect("channel open");
        assert_eq!(first, "a");

        // No second yet (bucket empty)
        let second = timeout(TDuration::from_millis(50), nrx.recv()).await;
        assert!(second.is_err(), "unexpectedly received a second notify");

        // Refill inbound via MockClock, then send another
        clock.advance(1);
        router_a
            .notify(pid(2), &Msg::Notify("d".into()))
            .await
            .unwrap();

        advance(TDuration::from_millis(1)).await;

        let d = timeout(TDuration::from_millis(50), nrx.recv())
            .await
            .expect("deliver after refill")
            .expect("channel open");
        assert_eq!(d, "d");
    }
}
