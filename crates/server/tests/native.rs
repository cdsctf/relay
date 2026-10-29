//! Production native transport authentication, packet boundaries, and lifecycle.
use cds_relay_client::NativeClient as Client;
use cds_relay_server::identity;
use cds_relay_server::{Config, native::NativeServer as Server};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn native_tcp_half_close_udp_empty_and_shutdown() {
    timeout(Duration::from_secs(5), async {
        let (identity, mut credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), &identity, Config::default())
            .await
            .unwrap();
        credentials.endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Client::connect(&credentials).await.unwrap();
        let mut stream = client.open_tcp(target.local_addr().unwrap()).await.unwrap();
        let (mut peer, _) = target.accept().await.unwrap();
        stream.write_all(b"request").await.unwrap();
        stream.shutdown().await.unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"request");
        peer.write_all(b"response").await.unwrap();
        peer.shutdown().await.unwrap();
        bytes.clear();
        stream.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"response");
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut tunnel = client.open_udp(udp.local_addr().unwrap()).await.unwrap();
        tunnel.send(b"").await.unwrap();
        let (size, address) = udp.recv_from(&mut [0; 16]).await.unwrap();
        assert_eq!(size, 0);
        udp.send_to(b"reply", address).await.unwrap();
        assert_eq!(&tunnel.recv().await.unwrap().unwrap()[..], b"reply");
        stop.cancel();
        task.await.unwrap().unwrap();
        assert!(tunnel.recv().await.unwrap().is_none());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn untrusted_native_identity_cannot_reach_target() -> io::Result<()> {
    let (identity, mut trusted) =
        identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into())?;
    let (_, stranger) = identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into())?;
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), &identity, Config::default()).await?;
    trusted.endpoint = server.local_addr()?;
    trusted.cert_pem = stranger.cert_pem;
    trusted.key_pem = stranger.key_pem;
    let stop = CancellationToken::new();
    let task = tokio::spawn(server.serve(stop.clone()));
    let target = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&trusted).await?;
    assert!(
        timeout(
            Duration::from_secs(2),
            client.open_tcp(target.local_addr()?)
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(
        timeout(Duration::from_millis(30), target.accept())
            .await
            .is_err()
    );
    stop.cancel();
    task.await.unwrap()?;
    Ok(())
}

#[tokio::test]
async fn native_udp_ipv6_isolation_cancelled_receive_and_payload_limit() {
    timeout(Duration::from_secs(5), async {
        let (identity, mut credentials) =
            identity::generate("[::1]:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind("[::1]:0".parse().unwrap(), &identity, Config::default())
            .await
            .unwrap();
        credentials.endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        let target = UdpSocket::bind("[::1]:0").await.unwrap();
        let client = Client::connect(&credentials).await.unwrap();
        let mut first = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        let mut second = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        assert!(
            first
                .send(&vec![0; cds_relay_native::MAX_UDP_PAYLOAD + 1])
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(20), first.recv())
                .await
                .is_err()
        );
        let payload = vec![0x5a; cds_relay_native::MAX_UDP_PAYLOAD];
        first.send(&payload).await.unwrap();
        let mut buffer = vec![0; 65536];
        let (size, a) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], payload);
        second.send(b"second").await.unwrap();
        let (_, b) = target.recv_from(&mut buffer).await.unwrap();
        assert_ne!(a, b);
        target.send_to(b"reply-b", b).await.unwrap();
        target.send_to(&payload, a).await.unwrap();
        assert_eq!(&second.recv().await.unwrap().unwrap()[..], b"reply-b");
        assert_eq!(&first.recv().await.unwrap().unwrap()[..], payload);
        stop.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn forged_datagrams_and_stalled_handshake_do_not_reach_targets_or_block_shutdown() {
    use cds_relay_protocol::{OpenRequest, OpenStatus, Transport};
    timeout(Duration::from_secs(5), async {
        let (identity, mut credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), &identity, Config::default())
            .await
            .unwrap();
        credentials.endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let connector = cds_relay_native::tls_connector(&credentials).unwrap();
        let socket = tokio::net::TcpStream::connect(credentials.endpoint)
            .await
            .unwrap();
        let mut control = connector
            .connect(
                rustls::pki_types::ServerName::try_from("cds-relay").unwrap(),
                socket,
            )
            .await
            .unwrap();
        OpenRequest {
            transport: Transport::Udp,
            target: target.local_addr().unwrap(),
        }
        .write(&mut control)
        .await
        .unwrap();
        assert_eq!(
            OpenStatus::read(&mut control).await.unwrap(),
            OpenStatus::Ok
        );
        let mut id = [0; 16];
        control.read_exact(&mut id).await.unwrap();
        let attacker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut forged = id.to_vec();
        forged.extend_from_slice(&[23, 254, 253, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16]);
        forged.extend_from_slice(&[0; 16]);
        attacker
            .send_to(&forged, credentials.endpoint)
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(30), target.recv_from(&mut [0; 64]))
                .await
                .is_err()
        );
        // Another authenticated association still forwards while this handshake stalls.
        let client = Client::connect(&credentials).await.unwrap();
        let tunnel = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        tunnel.send(b"trusted").await.unwrap();
        let mut buffer = [0; 64];
        let (size, _) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"trusted");
        stop.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!matches!(control.read(&mut buffer).await, Ok(size) if size > 0));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_idle_and_dropped_halves_reclaim_target_capacity() {
    timeout(Duration::from_secs(5), async {
        let (identity, mut credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            &identity,
            Config {
                max_connections: 1,
                udp_idle_timeout: Duration::from_millis(150),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        credentials.endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = Client::connect(&credentials).await.unwrap();
        let mut tunnel = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        assert!(
            timeout(Duration::from_secs(1), tunnel.recv())
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        let tunnel = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        let (mut receiver, sender) = tunnel.split();
        drop(sender);
        assert!(receiver.recv().await.unwrap().is_none());
        let tcp_target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // EOF on the control connection releases the one global slot asynchronously.
        timeout(Duration::from_secs(1), async {
            loop {
                if client
                    .open_tcp(tcp_target.local_addr().unwrap())
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        stop.cancel();
        task.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_wrong_server_name_and_ca_are_rejected_before_target_open() {
    timeout(Duration::from_secs(5), async {
        let (identity, mut credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let (_, stranger) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), &identity, Config::default())
            .await
            .unwrap();
        credentials.endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut wrong_name = credentials.clone();
        wrong_name.server_name = "wrong.invalid".into();
        let mut wrong_ca = credentials;
        wrong_ca.ca_cert_pem = stranger.ca_cert_pem;
        for credentials in [wrong_name, wrong_ca] {
            let client = Client::connect(&credentials).await.unwrap();
            assert!(client.open_tcp(target.local_addr().unwrap()).await.is_err());
        }
        assert!(
            timeout(Duration::from_millis(30), target.accept())
                .await
                .is_err()
        );
        stop.cancel();
        task.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_udp_setup_closes_control_and_releases_server_capacity() {
    timeout(Duration::from_secs(5), async {
        let (identity, mut credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            &identity,
            Config {
                max_connections: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        credentials.endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        // Forward only TCP through the proxy; no UDP listener exists at its port.
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut unreachable_udp = credentials.clone();
        unreachable_udp.endpoint = proxy.local_addr().unwrap();
        let endpoint = credentials.endpoint;
        let proxy_task = tokio::spawn(async move {
            let (mut client, _) = proxy.accept().await.unwrap();
            let mut upstream = tokio::net::TcpStream::connect(endpoint).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
        });
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let blocked = Client::connect_with_timeout(&unreachable_udp, Duration::from_millis(150))
            .await
            .unwrap();
        assert!(
            blocked
                .open_udp(target.local_addr().unwrap())
                .await
                .is_err()
        );
        timeout(Duration::from_secs(1), proxy_task)
            .await
            .unwrap()
            .unwrap();
        let client = Client::connect(&credentials).await.unwrap();
        let tunnel = timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(tunnel) = client.open_udp(target.local_addr().unwrap()).await {
                    break tunnel;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tunnel.send(b"reclaimed").await.unwrap();
        let mut buffer = [0; 32];
        let (size, _) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"reclaimed");
        stop.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_client_certificate_is_rejected_before_target_open() {
    use cds_relay_protocol::{OpenRequest, OpenStatus, Transport};
    use std::sync::Arc;
    timeout(Duration::from_secs(5), async {
        let (identity, credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), &identity, Config::default())
            .await
            .unwrap();
        let endpoint = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.serve(stop.clone()));
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut credentials.ca_cert_pem.as_bytes()) {
            roots.add(certificate.unwrap()).unwrap();
        }
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![cds_relay_native::ALPN.to_vec()];
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let socket = tokio::net::TcpStream::connect(endpoint).await.unwrap();
        let rejected = async {
            let mut stream = connector
                .connect(
                    rustls::pki_types::ServerName::try_from("cds-relay").unwrap(),
                    socket,
                )
                .await?;
            OpenRequest {
                transport: Transport::Tcp,
                target: target.local_addr()?,
            }
            .write(&mut stream)
            .await?;
            OpenStatus::read(&mut stream).await
        }
        .await;
        assert!(rejected.is_err());
        assert!(
            timeout(Duration::from_millis(30), target.accept())
                .await
                .is_err()
        );
        stop.cancel();
        task.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}
