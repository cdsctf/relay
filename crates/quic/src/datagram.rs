use crate::{MAX_STREAMS, UDP_QUEUE_PACKETS, invalid};
use bytes::{Bytes, BytesMut};
use cds_relay_protocol::MAX_DATAGRAM_SIZE;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
};
use tokio::{sync::mpsc, task::JoinHandle};

const HEADER_SIZE: usize = 8;
type Routes = Arc<Mutex<HashMap<u64, mpsc::Sender<Bytes>>>>;

/// Exactly one receive dispatcher per QUIC connection. Slow flows drop packets.
pub struct DatagramRouter {
    routes: Routes,
    task: JoinHandle<()>,
}

pub struct Registration {
    routes: Routes,
    id: u64,
    receiver: mpsc::Receiver<Bytes>,
}

impl DatagramRouter {
    pub fn new(connection: quinn::Connection) -> Self {
        let routes: Routes = Arc::default();
        let task_routes = routes.clone();
        let task = tokio::spawn(async move {
            while let Ok(packet) = connection.read_datagram().await {
                if !(HEADER_SIZE..=HEADER_SIZE + MAX_DATAGRAM_SIZE).contains(&packet.len()) {
                    continue;
                }
                let id = u64::from_be_bytes(packet[..HEADER_SIZE].try_into().unwrap());
                if let Some(sender) = task_routes.lock().unwrap().get(&id) {
                    // Never await a flow queue in the shared dispatcher.
                    let _ = sender.try_send(packet.slice(HEADER_SIZE..));
                }
            }
            task_routes.lock().unwrap().clear();
        });
        Self { routes, task }
    }

    pub fn register(&self, id: u64) -> io::Result<Registration> {
        let mut routes = self.routes.lock().unwrap();
        if routes.len() >= MAX_STREAMS as usize || routes.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "UDP flow limit reached",
            ));
        }
        let (sender, receiver) = mpsc::channel(UDP_QUEUE_PACKETS);
        routes.insert(id, sender);
        Ok(Registration {
            routes: self.routes.clone(),
            id,
            receiver,
        })
    }

    pub fn unregister(&self, id: u64) {
        self.routes.lock().unwrap().remove(&id);
    }
}

impl Drop for DatagramRouter {
    fn drop(&mut self) {
        self.task.abort();
        self.routes.lock().unwrap().clear();
    }
}

impl Registration {
    /// Cancellation safe; a cancelled receive leaves the queued packet intact.
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.receiver.recv().await
    }

    pub fn close(&mut self) {
        self.routes.lock().unwrap().remove(&self.id);
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.close();
    }
}

/// The current payload limit can change as QUIC discovers the path MTU.
pub fn max_udp_payload(connection: &quinn::Connection) -> Option<usize> {
    connection
        .max_datagram_size()
        .and_then(|size| size.checked_sub(HEADER_SIZE))
        .map(|size| size.min(MAX_DATAGRAM_SIZE))
}

/// Enqueue one unreliable packet. Congestion may discard earlier queued packets.
/// Oversized packets are rejected as a whole and never use a reliable stream.
pub fn send_udp(connection: &quinn::Connection, id: u64, payload: &[u8]) -> io::Result<()> {
    let limit = max_udp_payload(connection).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "peer does not support QUIC DATAGRAM",
        )
    })?;
    if payload.len() > limit {
        return Err(invalid(format!(
            "UDP payload exceeds current QUIC limit of {limit} bytes"
        )));
    }
    let mut packet = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    packet.extend_from_slice(&id.to_be_bytes());
    packet.extend_from_slice(payload);
    connection
        .send_datagram(packet.freeze())
        .map_err(|error| match error {
            quinn::SendDatagramError::TooLarge => invalid("path MTU changed; UDP packet dropped"),
            quinn::SendDatagramError::UnsupportedByPeer | quinn::SendDatagramError::Disabled => {
                io::Error::new(io::ErrorKind::Unsupported, error)
            }
            quinn::SendDatagramError::ConnectionLost(error) => io::Error::from(error),
        })
}
