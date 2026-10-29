//! Bounded QUIC relay server. Kubernetes lifecycle and WebTransport live in the platform.

pub mod identity;
pub mod quic;
pub use quic::{bind_quic, serve_quic};

use std::{io, net::SocketAddr, time::Duration};
use tokio::{
    net::UdpSocket,
    sync::{Semaphore, watch},
    time::Instant,
};

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
