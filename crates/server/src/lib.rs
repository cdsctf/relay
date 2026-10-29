//! Bounded relay server. Kubernetes lifecycle and WebTransport live in the platform.

use cds_relay_protocol::{
    DatagramReader, DatagramWriter, MAX_DATAGRAM_SIZE, OpenRequest, OpenStatus, Transport,
};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{Semaphore, watch},
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct Config {
    pub max_connections: usize,
    pub handshake_timeout: Duration,
    pub connect_timeout: Duration,
    pub udp_idle_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 256,
            handshake_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            udp_idle_timeout: Duration::from_secs(60),
        }
    }
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if self.max_connections == 0
            || self.max_connections > Semaphore::MAX_PERMITS
            || [
                self.handshake_timeout,
                self.connect_timeout,
                self.udp_idle_timeout,
            ]
            .iter()
            .any(|duration| duration.is_zero() || Instant::now().checked_add(*duration).is_none())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection limit and timeouts must be positive and within range",
            ));
        }
        Ok(())
    }
}

/// On shutdown or listener failure, cancels and joins every active session.
/// Excess connections are closed immediately without creating a session task.
pub async fn serve(
    listener: TcpListener,
    config: Config,
    shutdown: CancellationToken,
) -> io::Result<()> {
    config.validate()?;
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let mut sessions = JoinSet::new();
    let outcome = loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break Ok(()),
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(error) = result { tracing::warn!(%error, "relay session task failed"); }
            }
            result = listener.accept() => {
                let (stream, peer) = match result { Ok(pair) => pair, Err(error) => break Err(error) };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    tracing::debug!(%peer, "relay connection limit reached");
                    continue;
                };
                let config = config.clone();
                sessions.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle(stream, &config).await {
                        tracing::debug!(%peer, %error, "relay session ended");
                    }
                });
            }
        }
    };
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
    outcome
}

async fn respond(stream: &mut TcpStream, status: OpenStatus, duration: Duration) -> io::Result<()> {
    timeout(duration, status.write(stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "relay response timed out"))?
}

async fn handle(mut stream: TcpStream, config: &Config) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let request = match timeout(config.handshake_timeout, OpenRequest::read(&mut stream)).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            let status = if error.kind() == io::ErrorKind::Unsupported {
                OpenStatus::UnsupportedVersion
            } else {
                OpenStatus::InvalidRequest
            };
            respond(&mut stream, status, config.handshake_timeout).await?;
            return Err(error);
        }
        Err(_) => {
            respond(&mut stream, OpenStatus::Timeout, config.handshake_timeout).await?;
            return Ok(());
        }
    };
    tracing::debug!(target = %request.target, transport = ?request.transport, "opening relay target");
    match request.transport {
        Transport::Tcp => {
            let mut target = match timeout(
                config.connect_timeout,
                TcpStream::connect(request.target),
            )
            .await
            {
                Ok(Ok(target)) => target,
                Ok(Err(error)) => {
                    tracing::debug!(target = %request.target, %error, "TCP target connection failed");
                    respond(
                        &mut stream,
                        OpenStatus::ConnectFailed,
                        config.handshake_timeout,
                    )
                    .await?;
                    return Ok(());
                }
                Err(_) => {
                    respond(&mut stream, OpenStatus::Timeout, config.handshake_timeout).await?;
                    return Ok(());
                }
            };
            target.set_nodelay(true)?;
            respond(&mut stream, OpenStatus::Ok, config.handshake_timeout).await?;
            // Tokio propagates FIN in each direction and drains the other direction.
            copy_bidirectional(&mut stream, &mut target).await?;
            Ok(())
        }
        Transport::Udp => {
            let socket = match timeout(config.connect_timeout, udp_socket(request.target)).await {
                Ok(Ok(socket)) => socket,
                Ok(Err(error)) => {
                    tracing::debug!(target = %request.target, %error, "UDP target socket setup failed");
                    respond(
                        &mut stream,
                        OpenStatus::ConnectFailed,
                        config.handshake_timeout,
                    )
                    .await?;
                    return Ok(());
                }
                Err(_) => {
                    respond(&mut stream, OpenStatus::Timeout, config.handshake_timeout).await?;
                    return Ok(());
                }
            };
            respond(&mut stream, OpenStatus::Ok, config.handshake_timeout).await?;
            relay_udp(stream, socket, config.udp_idle_timeout).await
        }
    }
}

async fn udp_socket(target: SocketAddr) -> io::Result<UdpSocket> {
    let bind = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind).await?;
    socket.connect(target).await?;
    Ok(socket)
}

async fn wait_idle(mut activity: watch::Receiver<Instant>, duration: Duration) {
    loop {
        let deadline = *activity.borrow_and_update() + duration;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                // Activity might race the timer; inspect the latest timestamp.
                if Instant::now().duration_since(*activity.borrow()) >= duration { return; }
            }
            result = activity.changed() => { if result.is_err() { return; } }
        }
    }
}

async fn relay_udp(stream: TcpStream, socket: UdpSocket, idle: Duration) -> io::Result<()> {
    let (read, write) = stream.into_split();
    let mut reader = DatagramReader::new(read);
    let mut writer = DatagramWriter::new(write);
    let (activity, receiver) = watch::channel(Instant::now());
    // Each loop owns at most one packet. No global receive loop or unbounded queues.
    let uplink = async {
        while let Some(packet) = reader.recv().await? {
            match socket.send(&packet).await {
                Ok(size) if size == packet.len() => {
                    activity.send_replace(Instant::now());
                }
                Ok(_) => return Err(io::Error::new(io::ErrorKind::WriteZero, "partial UDP send")),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    };
    let downlink = async {
        // One extra byte detects truncation/oversize rather than forwarding a partial packet.
        let mut buffer = vec![0; MAX_DATAGRAM_SIZE + 1];
        loop {
            let size = match socket.recv(&mut buffer).await {
                Ok(size) => size,
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(error) => return Err(error),
            };
            if size > MAX_DATAGRAM_SIZE {
                continue;
            }
            activity.send_replace(Instant::now());
            writer.send(&buffer[..size]).await?;
            activity.send_replace(Instant::now());
        }
    };
    tokio::select! {
        result = uplink => result,
        result = downlink => result,
        _ = wait_idle(receiver, idle) => Ok(()),
    }
}
