use quinn::{ClientConfig, crypto::rustls::QuicClientConfig};
use rustls::{
    RootCertStore,
    client::danger::{ServerCertVerified, ServerCertVerifier},
};
use std::{net::SocketAddr, sync::Arc};
use tokio::{io::AsyncWriteExt, sync::mpsc};

use crate::communication::transports::{
    Connector, PeerId, PeerResolver, TransportConn, TransportError,
};

fn client_cfg_with_server_cert(
    server_cert: rustls::pki_types::CertificateDer<'static>,
) -> Result<QuicClientConfig, TransportError> {
    let mut roots = RootCertStore::empty();
    // rustls 0.23 accepts CertificateDer directly
    roots
        .add(server_cert)
        .map_err(|e| TransportError::Io(format!("add root: {e}")))?;
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut q = quinn::ClientConfig::new(Arc::new(cfg));
    q.transport_config(Arc::new(quinn::TransportConfig::default()));
    Ok(q)
}

fn server_cfg_self_signed() -> Result<
    (
        quinn::ServerConfig,
        rustls::pki_types::CertificateDer<'static>,
    ),
    TransportError,
> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| TransportError::Io(format!("rcgen: {e}")))?;

    // rcgen 0.13: fields are on `cert` (CertifiedKey)
    let key_der = cert.key_pair.serialize_der();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        key_der,
    ));
    let cert_der: rustls::pki_types::CertificateDer<'static> =
        rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());

    let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key)
        .map_err(|e| TransportError::Io(format!("server cfg: {e}")))?;
    server_config.transport_config(Arc::new(quinn::TransportConfig::default()));
    Ok((server_config, cert_der))
}

/// Where to dial or listen.
#[derive(Clone, Debug)]
pub struct QuicAddr {
    pub bind: SocketAddr,    // local bind (0.0.0.0:0 for client)
    pub remote: SocketAddr,  // remote server address
    pub server_name: String, // SNI, e.g., "localhost"
}

/// Simple resolver stub (usually you’ll have your own).
pub struct QuicResolver {
    pub bind: SocketAddr,
    pub server_name: String,
}
#[async_trait::async_trait]
impl PeerResolver<QuicAddr> for QuicResolver {
    async fn resolve(&self, _peer: PeerId) -> Result<QuicAddr, TransportError> {
        Err(TransportError::Io(
            "QuicResolver needs app-specific addressing".into(),
        ))
    }
}

/*──────────────────────── TLS helpers (dev only!) ─────────────────────*/

fn client_cfg_insecure(server_name: &str) -> Result<quinn::ClientConfig, TransportError> {
    // Create rustls client config that accepts ANY certificate (dev only).
    use rustls::{RootCertStore, client::ClientConfig, pki_types::ServerName};
    let mut cfg = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    // Dangerous: accept any cert
    struct NoVerify;
    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
    }
    cfg.dangerous().set_certificate_verifier(Arc::new(NoVerify));

    let mut q = quinn::ClientConfig::new(Arc::new(cfg))
        .transport_config(Arc::new(quinn::TransportConfig::default()))
        .map_err(|e| TransportError::Io(format!("quic client cfg: {e}")))?;
    // ALPN optional; leave default.
    Ok(q)
}

fn server_cfg_self_signed() -> Result<(quinn::ServerConfig, String), TransportError> {
    // Self-signed cert for dev localhost
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| TransportError::Io(format!("rcgen: {e}")))?;
    let key = quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair().serialize_der());
    let der = quinn::rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
    let mut server_config = quinn::ServerConfig::with_single_cert(vec![der], key)
        .map_err(|e| TransportError::Io(format!("server cfg: {e}")))?;
    let transport = quinn::TransportConfig::default();
    server_config.transport_config(Arc::new(transport));
    Ok((server_config, "localhost".to_string()))
}

/*──────────────────────── TransportConn impl ─────────────────────────*/

pub struct QuicConn {
    send: tokio::sync::Mutex<quinn::SendStream>,
    recv_task: tokio::task::JoinHandle<()>,
    rx_once: std::sync::Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    _conn: quinn::Connection, // keep alive
}

#[async_trait::async_trait]
impl TransportConn for QuicConn {
    async fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        use tokio::io::AsyncWriteExt;
        let mut s = self.send.lock().await;
        let len = (bytes.len() as u32).to_be_bytes();
        s.write_all(&len)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        s.write_all(&bytes)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        s.flush()
            .await
            .map_err(|_| TransportError::ConnectionClosed)
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
        self._conn.close(0u32.into(), b"bye");
        let _ = self.recv_task.abort();
    }
}

/*──────────────────────── connector (dial) ─────────────────────────*/

pub struct QuicConnector {
    // Provide the server certificate you trust (e.g., from discovery or config)
    pub server_cert: rustls::pki_types::CertificateDer<'static>,
}

#[async_trait::async_trait]
impl Connector<QuicAddr> for QuicConnector {
    async fn dial(&self, addr: &QuicAddr) -> Result<Arc<dyn TransportConn>, TransportError> {
        let client_cfg = client_cfg_with_server_cert(self.server_cert.clone())?;
        let mut endpoint = quinn::Endpoint::client(addr.bind)
            .map_err(|e| TransportError::Io(format!("endpoint client: {e}")))?;
        endpoint.set_default_client_config(client_cfg);

        let conn = endpoint
            .connect(addr.remote, &addr.server_name)
            .map_err(|e| TransportError::Io(format!("connect: {e}")))?
            .await
            .map_err(|e| TransportError::ConnectionClosed)?;

        // Open a single bidirectional stream for the session.
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        // Background reader → mpsc
        let (tx_bytes, rx_bytes) = mpsc::channel::<Vec<u8>>(1024);
        let recv_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            loop {
                let mut len_buf = [0u8; 4];
                if recv.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if recv.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if tx_bytes.send(buf).await.is_err() {
                    break;
                }
            }
        });

        Ok(Arc::new(QuicConn {
            send: tokio::sync::Mutex::new(send),
            recv_task,
            rx_once: std::sync::Mutex::new(Some(rx_bytes)),
            _conn: conn,
        }))
    }
}

/*──────────────────────── acceptor (server) ─────────────────────────
Run this on the peer you’re “dialing.” It accepts one incoming QUIC
connection and yields a `TransportConn` bound to that peer.
-------------------------------------------------------------------*/
pub struct QuicAcceptor {
    endpoint: quinn::Endpoint,
    pub server_cert: rustls::pki_types::CertificateDer<'static>, // expose for client
}

impl QuicAcceptor {
    pub async fn bind(listen: std::net::SocketAddr) -> Result<Self, TransportError> {
        let (server_cfg, cert_der) = server_cfg_self_signed()?;
        let endpoint = quinn::Endpoint::server(server_cfg, listen)
            .map_err(|e| TransportError::Io(format!("endpoint server: {e}")))?;
        Ok(Self {
            endpoint,
            server_cert: cert_der,
        })
    }

    pub async fn accept_one(&self) -> Result<Arc<dyn TransportConn>, TransportError> {
        let conn = self
            .endpoint
            .accept()
            .await
            .ok_or(TransportError::ConnectionClosed)?
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        // Prefer accepting a bi stream from the client
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(s) => s,
            Err(_) => conn
                .open_bi()
                .await
                .map_err(|_| TransportError::ConnectionClosed)?,
        };

        let (tx_bytes, rx_bytes) = mpsc::channel::<Vec<u8>>(1024);
        let recv_task = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            loop {
                let mut len_buf = [0u8; 4];
                if recv.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if recv.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if tx_bytes.send(buf).await.is_err() {
                    break;
                }
            }
        });

        Ok(Arc::new(QuicConn {
            send: tokio::sync::Mutex::new(send),
            recv_task,
            rx_once: std::sync::Mutex::new(Some(rx_bytes)),
            _conn: conn,
        }))
    }
}
