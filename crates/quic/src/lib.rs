//! Shared QUIC transport, mutual TLS configuration, and bounded UDP dispatch.

mod credentials;
mod datagram;
mod stream;

pub use credentials::{ClientCredentials, ServerIdentity, client_config, server_config};
pub use datagram::{DatagramRouter, Registration, max_udp_payload, send_udp};
pub use stream::Stream;

pub const ALPN: &[u8] = b"cds-relay/2";
pub const MAX_STREAMS: u32 = 256;
pub const UDP_QUEUE_PACKETS: usize = 32;

pub fn invalid(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
}

pub fn transport(incoming_streams: u32) -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config
        .max_concurrent_bidi_streams(incoming_streams.into())
        .max_concurrent_uni_streams(0u32.into())
        .stream_receive_window((1024u32 * 1024).into())
        .receive_window((8u32 * 1024 * 1024).into())
        .send_window(8 * 1024 * 1024)
        .datagram_receive_buffer_size(Some(256 * 1024))
        .datagram_send_buffer_size(256 * 1024)
        .max_idle_timeout(Some(std::time::Duration::from_secs(60).try_into().unwrap()))
        .keep_alive_interval(Some(std::time::Duration::from_secs(15)));
    config
}
