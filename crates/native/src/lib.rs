//! Independent mTLS/TCP streams and TLS-exporter-bound DTLS/UDP associations.
mod client;
mod credentials;
pub use credentials::{ClientCredentials, ServerIdentity};
mod packet;
pub use client::{
    NativeClient, NativeTcpTunnel, NativeUdpReceiver, NativeUdpSender, NativeUdpTunnel,
};
pub use packet::{DatagramHub, DatagramPipe, DatagramRoute, DatagramStats};

use std::{io, sync::Arc};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use webrtc_dtls::{
    cipher_suite::CipherSuiteId,
    config::{Config, ExtendedMasterSecretType},
};

pub const ALPN: &[u8] = b"cds-relay-native/1";
pub const EXPORTER: &[u8] = b"EXPORTER-cds-relay-native-dtls-v1";
/// Fits one encrypted UDP packet below the IPv6 minimum MTU. No fragmentation.
pub const MAX_UDP_PAYLOAD: usize = 1152;
pub const SESSION_ID_BYTES: usize = 16;
/// Conservative per-association AES-GCM application-record ceiling.
pub const MAX_RECORDS: u64 = 1 << 24;

pub fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub fn tls_acceptor(identity: &ServerIdentity) -> io::Result<TlsAcceptor> {
    let config = credentials::server_tls_config(identity)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub fn tls_connector(credentials: &ClientCredentials) -> io::Result<TlsConnector> {
    let config = credentials::client_tls_config(credentials)?;
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Only an authenticated TLS peer can derive this association's DTLS PSK.
pub fn dtls_config(secret: [u8; 32], id: [u8; SESSION_ID_BYTES]) -> Config {
    Config {
        psk: Some(Arc::new(move |identity| {
            if identity != id {
                return Err(webrtc_dtls::Error::Other(
                    "association identity mismatch".into(),
                ));
            }
            Ok(secret.to_vec())
        })),
        psk_identity_hint: Some(id.to_vec()),
        cipher_suites: vec![CipherSuiteId::Tls_Psk_With_Aes_128_Gcm_Sha256],
        extended_master_secret: ExtendedMasterSecretType::Require,
        flight_interval: std::time::Duration::from_millis(200),
        mtu: 1200,
        replay_protection_window: 256,
        ..Config::default()
    }
}
