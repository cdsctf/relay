use cds_relay_protocol::{MAGIC, OpenRequest, OpenStatus, Transport, VERSION};
use std::{
    io,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
};
use tokio::io::duplex;

#[tokio::test]
async fn requests_roundtrip_both_address_families_and_transports() {
    for target in ["127.0.0.1:8080", "[::1]:5353"] {
        for transport in [Transport::Tcp, Transport::Udp] {
            let request = OpenRequest {
                transport,
                target: target.parse().unwrap(),
            };
            let (mut left, mut right) = duplex(3);
            let write = tokio::spawn(async move {
                request.write(&mut left).await.unwrap();
            });
            assert_eq!(OpenRequest::read(&mut right).await.unwrap(), request);
            write.await.unwrap();
        }
    }
}

#[tokio::test]
async fn opening_format_is_v2_and_rejects_other_versions() {
    let request = OpenRequest {
        transport: Transport::Udp,
        target: "127.0.0.1:5353".parse().unwrap(),
    };
    let mut bytes = Vec::new();
    request.write(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"CDSR\x02\x02\x04\x7f\x00\x00\x01\x14\xe9");
    for version in [0, 1, 3, 255] {
        bytes[4] = version;
        assert_eq!(
            OpenRequest::read(&mut bytes.as_slice())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}

#[tokio::test]
async fn invalid_and_truncated_targets_are_rejected() {
    let mut valid = MAGIC.to_vec();
    valid.extend_from_slice(&[VERSION, 1, 4, 127, 0, 0, 1, 0, 80]);
    for index in [0, 5, 6, 12] {
        let mut invalid = valid.clone();
        invalid[index] = 0;
        assert_eq!(
            OpenRequest::read(&mut invalid.as_slice())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
    for length in 0..valid.len() {
        assert_eq!(
            OpenRequest::read(&mut &valid[..length])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    for target in [
        "127.0.0.1:0".parse().unwrap(),
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 80, 0, 1)),
    ] {
        let mut bytes = Vec::new();
        assert!(
            OpenRequest {
                transport: Transport::Tcp,
                target
            }
            .write(&mut bytes)
            .await
            .is_err()
        );
        assert!(bytes.is_empty());
    }
}

#[tokio::test]
async fn statuses_roundtrip_and_unknown_status_is_rejected() {
    for status in [
        OpenStatus::Ok,
        OpenStatus::InvalidRequest,
        OpenStatus::UnsupportedVersion,
        OpenStatus::ConnectFailed,
        OpenStatus::Timeout,
    ] {
        let mut bytes = Vec::new();
        status.write(&mut bytes).await.unwrap();
        assert_eq!(
            OpenStatus::read(&mut bytes.as_slice()).await.unwrap(),
            status
        );
    }
    assert_eq!(
        OpenStatus::read(&mut &[255][..]).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}
