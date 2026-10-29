//! Versioned, IP-addressed opening requests for authenticated relay transports.
//!
//! One reliable stream carries one TCP connection or controls one UDP association.
//! Multi-byte integers are big endian. See `docs/protocol.md` for the wire format.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAGIC: [u8; 4] = *b"CDSR";
pub const VERSION: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Transport {
    Tcp = 1,
    Udp = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenRequest {
    pub transport: Transport,
    pub target: SocketAddr,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl OpenRequest {
    pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let mut prefix = [0; 7];
        reader.read_exact(&mut prefix).await?;
        if prefix[..4] != MAGIC {
            return Err(invalid("invalid relay magic"));
        }
        if prefix[4] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported relay version",
            ));
        }
        let transport = match prefix[5] {
            1 => Transport::Tcp,
            2 => Transport::Udp,
            _ => return Err(invalid("unknown transport")),
        };
        let ip = match prefix[6] {
            4 => {
                let mut bytes = [0; 4];
                reader.read_exact(&mut bytes).await?;
                IpAddr::V4(Ipv4Addr::from(bytes))
            }
            6 => {
                let mut bytes = [0; 16];
                reader.read_exact(&mut bytes).await?;
                IpAddr::V6(Ipv6Addr::from(bytes))
            }
            _ => return Err(invalid("unknown address family")),
        };
        let port = reader.read_u16().await?;
        if port == 0 {
            return Err(invalid("target port must be nonzero"));
        }
        Ok(Self {
            transport,
            target: SocketAddr::new(ip, port),
        })
    }

    pub async fn write<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        if self.target.port() == 0 {
            return Err(invalid("target port must be nonzero"));
        }
        if let SocketAddr::V6(addr) = self.target {
            if addr.scope_id() != 0 || addr.flowinfo() != 0 {
                return Err(invalid("scoped IPv6 addresses are unsupported"));
            }
        }
        let mut header = Vec::with_capacity(25);
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&[VERSION, self.transport as u8]);
        match self.target.ip() {
            IpAddr::V4(ip) => {
                header.push(4);
                header.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                header.push(6);
                header.extend_from_slice(&ip.octets());
            }
        }
        header.extend_from_slice(&self.target.port().to_be_bytes());
        writer.write_all(&header).await?;
        writer.flush().await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum OpenStatus {
    Ok = 0,
    InvalidRequest = 1,
    UnsupportedVersion = 2,
    ConnectFailed = 3,
    Timeout = 4,
}

impl OpenStatus {
    pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        match reader.read_u8().await? {
            0 => Ok(Self::Ok),
            1 => Ok(Self::InvalidRequest),
            2 => Ok(Self::UnsupportedVersion),
            3 => Ok(Self::ConnectFailed),
            4 => Ok(Self::Timeout),
            _ => Err(invalid("unknown open status")),
        }
    }

    pub async fn write<W: AsyncWrite + Unpin>(self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(self as u8).await?;
        writer.flush().await
    }
}
