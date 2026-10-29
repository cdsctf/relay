//! Open relay sessions over any Tokio bidirectional stream, including port-forward.

pub use cds_relay_protocol::{DatagramReader, DatagramWriter, MAX_DATAGRAM_SIZE, OpenStatus};
use cds_relay_protocol::{OpenRequest, Transport};
use std::{error::Error, fmt, io, net::SocketAddr, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

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

#[derive(Clone, Copy, Debug)]
pub struct Client {
    pub handshake_timeout: Duration,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

impl Client {
    async fn open<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut stream: S,
        transport: Transport,
        target: SocketAddr,
    ) -> Result<S, OpenError> {
        // On timeout/error the owned stream is dropped; a partial handshake is never reused.
        let status = tokio::time::timeout(self.handshake_timeout, async {
            OpenRequest { transport, target }.write(&mut stream).await?;
            OpenStatus::read(&mut stream).await
        })
        .await
        .map_err(|_| OpenError::Timeout)?
        .map_err(OpenError::Io)?;
        if status != OpenStatus::Ok {
            return Err(OpenError::Rejected(status));
        }
        Ok(stream)
    }

    /// Returns the raw stream after the relay has connected to the target.
    pub async fn open_tcp<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
        target: SocketAddr,
    ) -> Result<S, OpenError> {
        self.open(stream, Transport::Tcp, target).await
    }

    /// Creates one UDP association with a stable target-side source socket.
    /// Successful open confirms socket setup, not UDP target reachability.
    pub async fn open_udp<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
        target: SocketAddr,
    ) -> Result<UdpTunnel<S>, OpenError> {
        Ok(UdpTunnel {
            stream: self.open(stream, Transport::Udp, target).await?,
        })
    }
}

pub struct UdpTunnel<S> {
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> UdpTunnel<S> {
    /// Read and write independently; no mutex is held while waiting for a reply.
    pub fn split(self) -> (DatagramReader<ReadHalf<S>>, DatagramWriter<WriteHalf<S>>) {
        let (reader, writer) = tokio::io::split(self.stream);
        (DatagramReader::new(reader), DatagramWriter::new(writer))
    }
}
