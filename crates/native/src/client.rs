use crate::ClientCredentials;
use crate::{
    ALPN, DatagramPipe, EXPORTER, MAX_RECORDS, MAX_UDP_PAYLOAD, dtls_config, invalid, tls_connector,
};
use bytes::Bytes;
use cds_relay_protocol::{OpenRequest, OpenStatus, Transport};
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::TcpStream, time::timeout};
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::sync::CancellationToken;
use webrtc_dtls::conn::DTLSConn;

pub type NativeTcpTunnel = TlsStream<TcpStream>;

#[derive(Clone)]
pub struct NativeClient {
    credentials: ClientCredentials,
    connector: TlsConnector,
    deadline: Duration,
}

impl NativeClient {
    pub async fn connect(credentials: &ClientCredentials) -> io::Result<Self> {
        Self::connect_with_timeout(credentials, Duration::from_secs(5)).await
    }
    /// Connections authenticate separately when opening a target; this validates local credentials.
    pub async fn connect_with_timeout(
        credentials: &ClientCredentials,
        deadline: Duration,
    ) -> io::Result<Self> {
        credentials.validate()?;
        if deadline.is_zero() || tokio::time::Instant::now().checked_add(deadline).is_none() {
            return Err(invalid("invalid open timeout"));
        }
        Ok(Self {
            credentials: credentials.clone(),
            connector: tls_connector(credentials)?,
            deadline,
        })
    }
    async fn open(&self, target: SocketAddr, transport: Transport) -> io::Result<NativeTcpTunnel> {
        let socket = TcpStream::connect(self.credentials.endpoint).await?;
        socket.set_nodelay(true)?;
        let name = rustls::pki_types::ServerName::try_from(self.credentials.server_name.clone())
            .map_err(invalid)?;
        let mut stream = self.connector.connect(name, socket).await?;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(invalid("native relay ALPN required"));
        }
        OpenRequest { target, transport }.write(&mut stream).await?;
        let status = OpenStatus::read(&mut stream).await?;
        if status != OpenStatus::Ok {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("relay target rejected: {status:?}"),
            ));
        }
        Ok(stream)
    }
    pub async fn open_tcp(&self, target: SocketAddr) -> io::Result<NativeTcpTunnel> {
        timeout(self.deadline, self.open(target, Transport::Tcp))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP open timed out"))?
    }
    pub async fn open_udp(&self, target: SocketAddr) -> io::Result<NativeUdpTunnel> {
        timeout(self.deadline, async {
            let mut control = self.open(target, Transport::Udp).await?;
            let mut id = [0; 16]; control.read_exact(&mut id).await?;
            let secret = control.get_ref().1.export_keying_material([0; 32], EXPORTER, Some(&id)).map_err(invalid)?;
            let pipe = DatagramPipe::client(self.credentials.endpoint, id).await?;
            let mut guard = CloseGuard(Some(pipe.stop.clone()));
            let dtls = Arc::new(DTLSConn::new(pipe.clone(), dtls_config(secret, id), true, None).await.map_err(invalid)?);
            if control.read_u8().await? != 0 { return Err(invalid("DTLS setup rejected")); }
            let stop = CancellationToken::new();
            let sent = Arc::new(AtomicU64::new(0));
            let session = Arc::new(Session { dtls: dtls.clone(), stop: stop.clone(), sent: sent.clone() });
            tokio::spawn(async move {
                let mut byte = [0];
                let mut keepalive = tokio::time::interval(Duration::from_secs(15));
                keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                keepalive.tick().await;
                loop {
                    tokio::select! { biased;
                        _ = stop.cancelled() => break,
                        _ = control.read(&mut byte) => break,
                        _ = keepalive.tick() => {
                            if sent.fetch_add(1, Ordering::Relaxed) >= MAX_RECORDS { break; }
                            if !matches!(timeout(Duration::from_secs(1), dtls.write(&[1], None)).await, Ok(Ok(_))) { break; }
                        },
                    }
                }
                // Let DTLS notify/stop its reader before closing the raw transport.
                stop.cancel();
                let _ = timeout(Duration::from_millis(100), dtls.close()).await;
                pipe.stop.cancel();
            });
            guard.0.take();
            Ok(NativeUdpTunnel { receiver: NativeUdpReceiver { session: session.clone(), buffer: vec![0; MAX_UDP_PAYLOAD + 1] }, sender: NativeUdpSender { session } })
        }).await.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "UDP open timed out"))?
    }
}

struct CloseGuard(Option<CancellationToken>);
impl Drop for CloseGuard {
    fn drop(&mut self) {
        if let Some(stop) = &self.0 {
            stop.cancel();
        }
    }
}
struct Session {
    dtls: Arc<DTLSConn>,
    stop: CancellationToken,
    sent: Arc<AtomicU64>,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

pub struct NativeUdpTunnel {
    receiver: NativeUdpReceiver,
    sender: NativeUdpSender,
}
impl NativeUdpTunnel {
    pub fn split(self) -> (NativeUdpReceiver, NativeUdpSender) {
        (self.receiver, self.sender)
    }
    pub fn max_payload(&self) -> Option<usize> {
        Some(MAX_UDP_PAYLOAD)
    }
    pub async fn send(&self, payload: &[u8]) -> io::Result<()> {
        self.sender.send(payload).await
    }
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        self.receiver.recv().await
    }
}
pub struct NativeUdpSender {
    session: Arc<Session>,
}
impl NativeUdpSender {
    pub fn max_payload(&self) -> Option<usize> {
        Some(MAX_UDP_PAYLOAD)
    }
    pub async fn send(&self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > MAX_UDP_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP payload exceeds path limit",
            ));
        }
        if self.session.sent.fetch_add(1, Ordering::Relaxed) >= MAX_RECORDS {
            self.session.stop.cancel();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "DTLS record budget exhausted; reopen association",
            ));
        }
        let mut packet = Vec::with_capacity(payload.len() + 1);
        packet.push(0);
        packet.extend_from_slice(payload);
        tokio::select! { biased;
            _ = self.session.stop.cancelled() => Err(io::Error::new(io::ErrorKind::BrokenPipe, "UDP association closed")),
            result = self.session.dtls.write(&packet, None) => { result.map_err(invalid)?; Ok(()) },
        }
    }
}
impl Drop for NativeUdpSender {
    fn drop(&mut self) {
        self.session.stop.cancel();
    }
}
pub struct NativeUdpReceiver {
    session: Arc<Session>,
    buffer: Vec<u8>,
}
impl NativeUdpReceiver {
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            let size = tokio::select! { biased;
                _ = self.session.stop.cancelled() => return Ok(None),
                result = self.session.dtls.read(&mut self.buffer, None) => result.map_err(invalid)?,
            };
            if size > 0 && self.buffer[0] == 0 {
                return Ok(Some(Bytes::copy_from_slice(&self.buffer[1..size])));
            }
        }
    }
}
impl Drop for NativeUdpReceiver {
    fn drop(&mut self) {
        self.session.stop.cancel();
    }
}
