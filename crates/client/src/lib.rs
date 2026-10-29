//! Authenticated native TLS/TCP and DTLS/UDP client.

pub use cds_relay_native::{
    ClientCredentials, MAX_UDP_PAYLOAD, NativeClient, NativeTcpTunnel, NativeUdpReceiver,
    NativeUdpSender, NativeUdpTunnel,
};
pub type RelayClient = NativeClient;
