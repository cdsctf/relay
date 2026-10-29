//! Multiplexed TCP/UDP relay client over mutually authenticated QUIC.

pub mod quic;
pub use cds_relay_quic::ClientCredentials;
pub use quic::{QuicClient, QuicTcpTunnel, QuicUdpReceiver, QuicUdpSender, QuicUdpTunnel};

pub use cds_relay_protocol::{MAX_DATAGRAM_SIZE, OpenStatus};
use std::{error::Error, fmt, io};

#[derive(Debug)]
pub enum OpenError {
    Io(io::Error),
    Timeout,
    Rejected(OpenStatus),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "relay handshake: {error}"),
            Self::Timeout => f.write_str("relay handshake timed out"),
            Self::Rejected(status) => write!(f, "relay rejected connection: {status:?}"),
        }
    }
}
impl Error for OpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
