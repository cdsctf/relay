//! Bounded native TCP/UDP relay server. Kubernetes lifecycle and WebTransport live in the platform.

pub mod identity;
pub mod native;
mod traffic;

use std::{
    io,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{net::UdpSocket, sync::Semaphore, time::Instant};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_connections: usize,
    /// Aggregate TCP payload bytes/s in each direction across this server.
    /// None preserves unrestricted TCP forwarding.
    pub tcp_rate_limit: Option<u64>,
    pub udp_max_queue_delay: Option<Duration>,
    pub handshake_timeout: Duration,
    pub connect_timeout: Duration,
    pub udp_idle_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 256,
            tcp_rate_limit: None,
            udp_max_queue_delay: None,
            handshake_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            udp_idle_timeout: Duration::from_secs(60),
        }
    }
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if self
            .udp_max_queue_delay
            .is_some_and(|delay| delay.is_zero() || delay > Duration::from_secs(1))
            || self.max_connections == 0
            || self
                .tcp_rate_limit
                .is_some_and(|rate| rate == 0 || rate > 1_000_000_000_000)
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
                "connection limit, TCP rate, UDP queue expiry, and timeouts must be positive and within range",
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

/// Activity shares the target task with both forwarding directions. Updating it
/// does not notify/reset a timer on every packet; expiry rechecks the timestamp.
struct Activity {
    epoch: Instant,
    nanos: AtomicU64,
}

impl Activity {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            nanos: AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        self.nanos.store(
            self.epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    async fn wait_idle(&self, duration: Duration) {
        loop {
            let last = self.nanos.load(Ordering::Relaxed);
            tokio::time::sleep_until(self.epoch + Duration::from_nanos(last) + duration).await;
            if self.nanos.load(Ordering::Relaxed) == last {
                return;
            }
        }
    }
}
