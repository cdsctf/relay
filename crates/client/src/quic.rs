//! Multiplexed, mutually authenticated QUIC client.

use crate::OpenError;
use bytes::Bytes;
use cds_relay_protocol::{OpenRequest, OpenStatus, Transport};
use cds_relay_quic::{
    ClientCredentials, DatagramRouter, Registration, Stream, client_config, invalid,
    max_udp_payload, send_udp,
};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

struct Session {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    router: DatagramRouter,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.connection.close(0u32.into(), b"client closed");
        self.endpoint.close(0u32.into(), b"client closed");
    }
}

/// Clone to share a single QUIC connection. Tunnels retain its lifetime.
#[derive(Clone)]
pub struct QuicClient {
    session: Arc<Session>,
    handshake_timeout: Duration,
}

impl QuicClient {
    pub async fn connect(credentials: &ClientCredentials) -> io::Result<Self> {
        Self::connect_with_timeout(credentials, Duration::from_secs(10)).await
    }

    /// The timeout also covers waiting for stream capacity and target-open responses.
    pub async fn connect_with_timeout(
        credentials: &ClientCredentials,
        handshake_timeout: Duration,
    ) -> io::Result<Self> {
        if handshake_timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(handshake_timeout)
                .is_none()
        {
            return Err(invalid(
                "handshake timeout must be positive and within range",
            ));
        }
        let bind = if credentials.endpoint.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let mut endpoint = quinn::Endpoint::client(bind.parse().unwrap())?;
        endpoint.set_default_client_config(client_config(credentials)?);
        let connection = tokio::time::timeout(
            handshake_timeout,
            endpoint
                .connect(credentials.endpoint, &credentials.server_name)
                .map_err(invalid)?,
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "QUIC handshake timed out"))??;
        if max_udp_payload(&connection).is_none() {
            connection.close(1u32.into(), b"DATAGRAM required");
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "QUIC DATAGRAM required",
            ));
        }
        let router = DatagramRouter::new(connection.clone());
        Ok(Self {
            session: Arc::new(Session {
                endpoint,
                connection,
                router,
            }),
            handshake_timeout,
        })
    }

    pub async fn open_tcp(&self, target: SocketAddr) -> Result<QuicTcpTunnel, OpenError> {
        let stream = tokio::time::timeout(self.handshake_timeout, async {
            let (send, recv) = self
                .session
                .connection
                .open_bi()
                .await
                .map_err(io::Error::from)
                .map_err(OpenError::Io)?;
            open_stream(Stream::new(send, recv), Transport::Tcp, target).await
        })
        .await
        .map_err(|_| OpenError::Timeout)??;
        Ok(QuicTcpTunnel {
            stream,
            _session: self.session.clone(),
        })
    }

    pub async fn open_udp(&self, target: SocketAddr) -> Result<QuicUdpTunnel, OpenError> {
        let (stream, flow) = tokio::time::timeout(self.handshake_timeout, async {
            let (send, recv) = self
                .session
                .connection
                .open_bi()
                .await
                .map_err(io::Error::from)
                .map_err(OpenError::Io)?;
            let stream = Stream::new(send, recv);
            // Register before sending OPEN so immediate target replies cannot race registration.
            let flow = self
                .session
                .router
                .register(stream.id())
                .map_err(OpenError::Io)?;
            let stream = open_stream(stream, Transport::Udp, target).await?;
            Ok::<_, OpenError>((stream, flow))
        })
        .await
        .map_err(|_| OpenError::Timeout)??;
        let closed = Arc::new(AtomicBool::new(false));
        let id = stream.id();
        Ok(QuicUdpTunnel {
            sender: QuicUdpSender {
                session: self.session.clone(),
                id,
                control: stream.send,
                closed: closed.clone(),
            },
            receiver: QuicUdpReceiver {
                _session: self.session.clone(),
                flow,
                control: stream.recv,
                closed,
            },
        })
    }

    pub fn max_udp_payload(&self) -> Option<usize> {
        max_udp_payload(&self.session.connection)
    }

    /// Immediately close every tunnel on this connection, including those held by clones.
    pub fn close(&self) {
        self.session.connection.close(0u32.into(), b"client closed");
    }
}

async fn open_stream(
    mut stream: Stream,
    transport: Transport,
    target: SocketAddr,
) -> Result<Stream, OpenError> {
    OpenRequest { transport, target }
        .write(&mut stream)
        .await
        .map_err(OpenError::Io)?;
    let status = OpenStatus::read(&mut stream).await.map_err(OpenError::Io)?;
    if status != OpenStatus::Ok {
        return Err(OpenError::Rejected(status));
    }
    Ok(stream)
}

pub struct QuicTcpTunnel {
    stream: Stream,
    _session: Arc<Session>,
}

impl AsyncRead for QuicTcpTunnel {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for QuicTcpTunnel {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buffer)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

pub struct QuicUdpTunnel {
    sender: QuicUdpSender,
    receiver: QuicUdpReceiver,
}

impl QuicUdpTunnel {
    pub fn send(&self, payload: &[u8]) -> io::Result<()> {
        self.sender.send(payload)
    }
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        self.receiver.recv().await
    }
    pub fn max_payload(&self) -> Option<usize> {
        self.sender.max_payload()
    }
    pub fn split(self) -> (QuicUdpReceiver, QuicUdpSender) {
        (self.receiver, self.sender)
    }
}

pub struct QuicUdpSender {
    session: Arc<Session>,
    id: u64,
    control: quinn::SendStream,
    closed: Arc<AtomicBool>,
}

impl QuicUdpSender {
    /// Success means queued locally; QUIC DATAGRAM delivery is not guaranteed.
    pub fn send(&self, payload: &[u8]) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "UDP association closed",
            ));
        }
        send_udp(&self.session.connection, self.id, payload)
    }

    pub fn max_payload(&self) -> Option<usize> {
        max_udp_payload(&self.session.connection)
    }
}

impl Drop for QuicUdpSender {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.session.router.unregister(self.id);
        let _ = self.control.finish();
    }
}

pub struct QuicUdpReceiver {
    _session: Arc<Session>,
    flow: Registration,
    control: quinn::RecvStream,
    closed: Arc<AtomicBool>,
}

impl QuicUdpReceiver {
    /// Cancellation safe. FIN, reset, and idle expiry close the entire association.
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        if self.closed.load(Ordering::Acquire) {
            self.flow.close();
            return Ok(None);
        }
        let mut byte = [0];
        let result = tokio::select! {
            biased;
            result = AsyncReadExt::read(&mut self.control, &mut byte) => match result {
                Ok(0) => Ok(None),
                Ok(_) => Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected UDP control data")),
                Err(error) => Err(error),
            },
            packet = self.flow.recv() => Ok(packet),
        };
        if !matches!(&result, Ok(Some(_))) {
            self.closed.store(true, Ordering::Release);
            self.flow.close();
        }
        result
    }
}

impl Drop for QuicUdpReceiver {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.control.stop(0u32.into());
    }
}
