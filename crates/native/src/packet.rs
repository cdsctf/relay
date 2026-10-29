//! Bounded, nonblocking UDP demultiplexing. Cryptography belongs to DTLS.
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, mpsc},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use webrtc_util::{Error, Result, conn::Conn};

const ID_BYTES: usize = 16;
const MAX_WIRE: usize = 1232;
const QUEUED_BYTES: usize = 1024 * 1024;

struct Packet {
    bytes: Bytes,
    received: Instant,
    _quota: OwnedSemaphorePermit,
}
struct Route {
    expected_ip: IpAddr,
    peer: Arc<Mutex<Option<SocketAddr>>>,
    tx: mpsc::Sender<Packet>,
}
#[derive(Default)]
struct Counters {
    unknown: AtomicU64,
    full: AtomicU64,
    budget: AtomicU64,
    expired: AtomicU64,
}
#[derive(Debug)]
pub struct DatagramStats {
    pub unknown: u64,
    pub queue_full: u64,
    pub budget_exhausted: u64,
    pub expired: u64,
}

#[derive(Clone)]
pub struct DatagramHub {
    socket: Arc<UdpSocket>,
    routes: Arc<Mutex<HashMap<[u8; ID_BYTES], Route>>>,
    quota: Arc<Semaphore>,
    counters: Arc<Counters>,
    expiry: Option<Duration>,
}
impl DatagramHub {
    pub async fn bind(address: SocketAddr, expiry: Option<Duration>) -> io::Result<Self> {
        Ok(Self {
            socket: Arc::new(UdpSocket::bind(address).await?),
            routes: Arc::default(),
            quota: Arc::new(Semaphore::new(QUEUED_BYTES)),
            counters: Arc::default(),
            expiry,
        })
    }
    pub fn register(&self, expected_ip: IpAddr) -> io::Result<DatagramRoute> {
        let mut id = [0; ID_BYTES];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut id)
            .map_err(super::invalid)?;
        let (tx, rx) = mpsc::channel(128);
        let peer: Arc<Mutex<Option<SocketAddr>>> = Arc::default();
        let stop = CancellationToken::new();
        let pipe = Arc::new(DatagramPipe {
            socket: self.socket.clone(),
            id,
            peer: peer.clone(),
            incoming: Some(AsyncMutex::new(rx)),
            stop,
            expiry: self.expiry,
            counters: self.counters.clone(),
        });
        let mut routes = self.routes.lock().unwrap();
        if routes.contains_key(&id) {
            return Err(super::invalid("association collision"));
        }
        routes.insert(
            id,
            Route {
                expected_ip,
                peer,
                tx,
            },
        );
        Ok(DatagramRoute {
            hub: self.clone(),
            id,
            pipe,
        })
    }
    pub fn stats(&self) -> DatagramStats {
        DatagramStats {
            unknown: self.counters.unknown.load(Ordering::Relaxed),
            queue_full: self.counters.full.load(Ordering::Relaxed),
            budget_exhausted: self.counters.budget.load(Ordering::Relaxed),
            expired: self.counters.expired.load(Ordering::Relaxed),
        }
    }
    pub async fn run(&self, stop: CancellationToken) -> io::Result<()> {
        let mut buffer = [0; MAX_WIRE + 1];
        loop {
            let (size, source) = tokio::select! { biased;
                _ = stop.cancelled() => return Ok(()),
                packet = self.socket.recv_from(&mut buffer) => packet?,
            };
            if size <= ID_BYTES || size > MAX_WIRE {
                continue;
            }
            let id: [u8; ID_BYTES] = buffer[..ID_BYTES].try_into().unwrap();
            let routes = self.routes.lock().unwrap();
            let Some(route) = routes.get(&id) else {
                self.counters.unknown.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if source.ip() != route.expected_ip {
                continue;
            }
            let mut peer = route.peer.lock().unwrap();
            if peer.is_some_and(|p| p != source) {
                continue;
            }
            *peer = Some(source);
            let Ok(quota) = self
                .quota
                .clone()
                .try_acquire_many_owned((size - ID_BYTES) as u32)
            else {
                self.counters.budget.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let packet = Packet {
                bytes: Bytes::copy_from_slice(&buffer[ID_BYTES..size]),
                received: Instant::now(),
                _quota: quota,
            };
            if route.tx.try_send(packet).is_err() {
                self.counters.full.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

pub struct DatagramRoute {
    hub: DatagramHub,
    pub id: [u8; ID_BYTES],
    pub pipe: Arc<DatagramPipe>,
}
impl Drop for DatagramRoute {
    fn drop(&mut self) {
        self.hub.routes.lock().unwrap().remove(&self.id);
        self.pipe.stop.cancel();
    }
}

pub struct DatagramPipe {
    socket: Arc<UdpSocket>,
    id: [u8; ID_BYTES],
    peer: Arc<Mutex<Option<SocketAddr>>>,
    incoming: Option<AsyncMutex<mpsc::Receiver<Packet>>>,
    pub stop: CancellationToken,
    expiry: Option<Duration>,
    counters: Arc<Counters>,
}
impl DatagramPipe {
    pub async fn client(endpoint: SocketAddr, id: [u8; ID_BYTES]) -> io::Result<Arc<Self>> {
        let bind = if endpoint.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind).await?;
        socket.connect(endpoint).await?;
        Ok(Arc::new(Self {
            socket: Arc::new(socket),
            id,
            peer: Arc::new(Mutex::new(Some(endpoint))),
            incoming: None,
            stop: CancellationToken::new(),
            expiry: None,
            counters: Arc::default(),
        }))
    }
}

#[async_trait]
impl Conn for DatagramPipe {
    async fn connect(&self, _: SocketAddr) -> Result<()> {
        Err(Error::ErrNoRemAddr)
    }
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        // DTLS owns its reader-close signal. After raw cancellation, park this
        // read until that signal cancels it: webrtc-dtls retries ordinary I/O
        // errors, so repeatedly returning an immediately-ready error would spin.
        if let Some(incoming) = &self.incoming {
            let mut incoming = incoming.lock().await;
            loop {
                let packet = tokio::select! { biased;
                    _ = self.stop.cancelled() => return std::future::pending().await,
                    packet = incoming.recv() => match packet { Some(packet) => packet, None => return std::future::pending().await },
                };
                if self.expiry.is_some_and(|d| packet.received.elapsed() > d) {
                    self.counters.expired.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if packet.bytes.len() > buf.len() {
                    return Err(Error::ErrBufferShort);
                }
                buf[..packet.bytes.len()].copy_from_slice(&packet.bytes);
                return Ok(packet.bytes.len());
            }
        }
        let mut packet = [0; MAX_WIRE + 1];
        loop {
            let size = tokio::select! { biased;
                _ = self.stop.cancelled() => return std::future::pending().await,
                size = self.socket.recv(&mut packet) => size?,
            };
            if size <= ID_BYTES || size > MAX_WIRE || packet[..ID_BYTES] != self.id {
                continue;
            }
            let size = size - ID_BYTES;
            if size > buf.len() {
                return Err(Error::ErrBufferShort);
            }
            buf[..size].copy_from_slice(&packet[ID_BYTES..ID_BYTES + size]);
            return Ok(size);
        }
    }
    async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let size = self.recv(buf).await?;
        Ok((size, self.remote_addr().ok_or(Error::ErrNoRemAddr)?))
    }
    async fn send(&self, buf: &[u8]) -> Result<usize> {
        if buf.len() + ID_BYTES > MAX_WIRE {
            return Err(Error::ErrPacketTooBig);
        }
        let peer = self.remote_addr().ok_or(Error::ErrNoRemAddr)?;
        let mut packet = Vec::with_capacity(buf.len() + ID_BYTES);
        packet.extend_from_slice(&self.id);
        packet.extend_from_slice(buf);
        tokio::select! { biased;
            _ = self.stop.cancelled() => return Err(Error::ErrUseClosedNetworkConn),
            sent = async { if self.incoming.is_none() { self.socket.send(&packet).await } else { self.socket.send_to(&packet, peer).await } } => { sent?; },
        }
        Ok(buf.len())
    }
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        if Some(target) != self.remote_addr() {
            return Err(Error::ErrNoRemAddr);
        }
        self.send(buf).await
    }
    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }
    fn remote_addr(&self) -> Option<SocketAddr> {
        *self.peer.lock().unwrap()
    }
    async fn close(&self) -> Result<()> {
        self.stop.cancel();
        Ok(())
    }
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}
