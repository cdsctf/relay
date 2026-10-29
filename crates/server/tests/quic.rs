use cds_relay_client::{ClientCredentials, OpenError, QuicClient};
use cds_relay_protocol::{OpenRequest, OpenStatus, Transport};
use cds_relay_quic::{ALPN, DatagramRouter, Stream, UDP_QUEUE_PACKETS, client_config};
use cds_relay_server::{Config, bind_quic, identity, serve_quic};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const DEADLINE: Duration = Duration::from_secs(5);

struct Relay {
    credentials: ClientCredentials,
    cancel: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl Relay {
    async fn start(config: Config, ipv6: bool) -> Self {
        let address: SocketAddr = if ipv6 { "[::1]:0" } else { "127.0.0.1:0" }
            .parse()
            .unwrap();
        let (identity, mut credentials) =
            identity::generate("127.0.0.1:1".parse().unwrap(), "cds-relay".into()).unwrap();
        let endpoint = bind_quic(address, &identity).unwrap();
        credentials.endpoint = endpoint.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve_quic(endpoint, config, cancel.clone()));
        Self {
            credentials,
            cancel,
            task,
        }
    }

    async fn client(&self) -> QuicClient {
        QuicClient::connect_with_timeout(&self.credentials, DEADLINE)
            .await
            .unwrap()
    }

    async fn raw(&self) -> (quinn::Endpoint, quinn::Connection) {
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_config(&self.credentials).unwrap());
        let connection = endpoint
            .connect(self.credentials.endpoint, &self.credentials.server_name)
            .unwrap()
            .await
            .unwrap();
        (endpoint, connection)
    }

    async fn stop(self) {
        self.cancel.cancel();
        timeout(DEADLINE, self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn tcp_half_close_and_udp_share_one_quic_connection() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let client = relay.client().await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut tcp = client.open_tcp(target.local_addr().unwrap()).await.unwrap();
        let (mut peer, _) = target.accept().await.unwrap();
        let payload = vec![0x5a; 256 * 1024];
        let expected = payload.clone();
        let peer_task = tokio::spawn(async move {
            let mut request = Vec::new();
            peer.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, expected);
            peer.write_all(&request).await.unwrap();
            peer.shutdown().await.unwrap();
        });
        // A stalled TCP read must not block opening or forwarding a UDP flow.
        let mut datagrams = client.open_udp(udp.local_addr().unwrap()).await.unwrap();
        datagrams.send(b"udp").unwrap();
        let mut buffer = [0; 32];
        let (size, address) = udp.recv_from(&mut buffer).await.unwrap();
        udp.send_to(&buffer[..size], address).await.unwrap();
        assert_eq!(&datagrams.recv().await.unwrap().unwrap()[..], b"udp");
        tcp.write_all(&payload).await.unwrap();
        tcp.shutdown().await.unwrap();
        drop(client); // Tunnels retain the connection and endpoint.
        let mut reply = Vec::new();
        tcp.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, payload);
        peer_task.await.unwrap();
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn udp_isolated_flows_empty_packets_and_split_receive_cancellation() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let client = relay.client().await;
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (mut a_read, a_write) = client
            .open_udp(target.local_addr().unwrap())
            .await
            .unwrap()
            .split();
        let mut b = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        let mut buffer = [0; 64];
        a_write.send(b"a").unwrap();
        let (size, a_peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"a");
        b.send(b"b").unwrap();
        let (size, b_peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"b");
        assert_ne!(a_peer, b_peer);
        assert!(
            timeout(Duration::from_millis(20), a_read.recv())
                .await
                .is_err()
        );
        target.send_to(b"reply-b", b_peer).await.unwrap();
        target.send_to(b"reply-a", a_peer).await.unwrap();
        target.send_to(b"extra-a", a_peer).await.unwrap();
        assert_eq!(&b.recv().await.unwrap().unwrap()[..], b"reply-b");
        assert_eq!(&a_read.recv().await.unwrap().unwrap()[..], b"reply-a");
        assert_eq!(&a_read.recv().await.unwrap().unwrap()[..], b"extra-a");
        a_write.send(b"").unwrap();
        let (size, peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(size, 0);
        assert_eq!(peer, a_peer);
        target.send_to(b"", peer).await.unwrap();
        assert!(a_read.recv().await.unwrap().unwrap().is_empty());
        let stranger = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        stranger.send_to(b"wrong", a_peer).await.unwrap();
        target.send_to(b"right", a_peer).await.unwrap();
        assert_eq!(&a_read.recv().await.unwrap().unwrap()[..], b"right");
        drop(a_read);
        assert_eq!(
            a_write.send(b"closed").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        b.send(b"still-open").unwrap();
        let (size, peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(peer, b_peer);
        assert_eq!(&buffer[..size], b"still-open");
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn oversized_datagrams_are_rejected_or_dropped_without_breaking_flow() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let client = relay.client().await;
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut tunnel = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        assert!(tunnel.max_payload().unwrap() >= 1100);
        assert_eq!(
            tunnel.send(&vec![0; 65_508]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        tunnel.send(b"peer").unwrap();
        let (_, peer) = target.recv_from(&mut [0; 8]).await.unwrap();
        target.send_to(&vec![0; 8000], peer).await.unwrap();
        assert!(
            timeout(Duration::from_millis(30), tunnel.recv())
                .await
                .is_err()
        );
        target.send_to(b"after-oversize", peer).await.unwrap();
        assert_eq!(
            &tunnel.recv().await.unwrap().unwrap()[..],
            b"after-oversize"
        );
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn full_flow_queue_drops_packets_without_blocking_another_flow() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let (_endpoint, connection) = relay.raw().await;
        let router = DatagramRouter::new(connection.clone());
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut slow = Stream::new(send, recv);
        let mut slow_flow = router.register(slow.id()).unwrap();
        OpenRequest {
            transport: Transport::Udp,
            target: address,
        }
        .write(&mut slow)
        .await
        .unwrap();
        assert_eq!(OpenStatus::read(&mut slow).await.unwrap(), OpenStatus::Ok);
        cds_relay_quic::send_udp(&connection, slow.id(), b"discover").unwrap();
        let (_, peer) = target.recv_from(&mut [0; 16]).await.unwrap();
        for _ in 0..(UDP_QUEUE_PACKETS * 2) {
            target.send_to(b"slow", peer).await.unwrap();
            tokio::task::yield_now().await;
        }
        // Observe the received QUIC frames, not an assumed delay for queue saturation.
        while connection.stats().frame_rx.datagram < (UDP_QUEUE_PACKETS * 2) as u64 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut fast = Stream::new(send, recv);
        let mut fast_flow = router.register(fast.id()).unwrap();
        OpenRequest {
            transport: Transport::Udp,
            target: address,
        }
        .write(&mut fast)
        .await
        .unwrap();
        assert_eq!(OpenStatus::read(&mut fast).await.unwrap(), OpenStatus::Ok);
        cds_relay_quic::send_udp(&connection, fast.id(), b"fast").unwrap();
        let (_, peer) = target.recv_from(&mut [0; 16]).await.unwrap();
        target.send_to(b"fast-reply", peer).await.unwrap();
        assert_eq!(&fast_flow.recv().await.unwrap()[..], b"fast-reply");
        for _ in 0..UDP_QUEUE_PACKETS {
            assert_eq!(&slow_flow.recv().await.unwrap()[..], b"slow");
        }
        assert!(
            timeout(Duration::from_millis(20), slow_flow.recv())
                .await
                .is_err()
        );
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_unknown_and_closed_flow_ids_never_reach_a_target() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let (_endpoint, connection) = relay.raw().await;
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut control = Stream::new(send, recv);
        OpenRequest {
            transport: Transport::Udp,
            target: address,
        }
        .write(&mut control)
        .await
        .unwrap();
        assert_eq!(
            OpenStatus::read(&mut control).await.unwrap(),
            OpenStatus::Ok
        );
        let id = control.id();
        connection.send_datagram(vec![0; 7].into()).unwrap();
        cds_relay_quic::send_udp(&connection, id + 4, b"unknown").unwrap();
        assert!(
            timeout(Duration::from_millis(20), target.recv_from(&mut [0; 16]))
                .await
                .is_err()
        );
        cds_relay_quic::send_udp(&connection, id, b"valid").unwrap();
        let mut buffer = [0; 16];
        let (size, _) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"valid");
        control.shutdown().await.unwrap();
        assert_eq!(control.read(&mut [0]).await.unwrap(), 0);
        cds_relay_quic::send_udp(&connection, id, b"late").unwrap();
        assert!(
            timeout(Duration::from_millis(20), target.recv_from(&mut buffer))
                .await
                .is_err()
        );
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn udp_idle_expiry_and_drop_reclaim_global_target_capacity() {
    timeout(DEADLINE, async {
        let relay = Relay::start(
            Config {
                max_connections: 2,
                udp_idle_timeout: Duration::from_millis(100),
                ..Config::default()
            },
            false,
        )
        .await;
        let a = relay.client().await;
        let b = relay.client().await;
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let mut first = a.open_udp(address).await.unwrap();
        let mut second = a.open_udp(address).await.unwrap();
        assert!(b.open_udp(address).await.is_err()); // Global, not per-connection limit.
        assert!(first.recv().await.unwrap().is_none());
        assert!(second.recv().await.unwrap().is_none());
        let (mut reader, writer) = b.open_udp(address).await.unwrap().split();
        drop(writer);
        assert!(reader.recv().await.unwrap().is_none());
        // The other authenticated connection remains usable after the resets and expiries.
        let mut final_flow = a.open_udp(address).await.unwrap();
        final_flow.send(b"alive").unwrap();
        let mut buffer = [0; 16];
        let (size, peer) = target.recv_from(&mut buffer).await.unwrap();
        target.send_to(&buffer[..size], peer).await.unwrap();
        assert_eq!(&final_flow.recv().await.unwrap().unwrap()[..], b"alive");
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stalled_and_invalid_opens_do_not_block_other_streams() {
    timeout(DEADLINE, async {
        let relay = Relay::start(
            Config {
                handshake_timeout: Duration::from_millis(80),
                ..Config::default()
            },
            false,
        )
        .await;
        let (_endpoint, connection) = relay.raw().await;
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut stalled = Stream::new(send, recv);
        stalled.write_all(b"C").await.unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut invalid = Stream::new(send, recv);
        invalid.write_all(b"CDSR\x09\x01\x04").await.unwrap();
        assert_eq!(
            OpenStatus::read(&mut invalid).await.unwrap(),
            OpenStatus::UnsupportedVersion
        );
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (send, recv) = connection.open_bi().await.unwrap();
        let mut working = Stream::new(send, recv);
        OpenRequest {
            transport: Transport::Udp,
            target: target.local_addr().unwrap(),
        }
        .write(&mut working)
        .await
        .unwrap();
        assert_eq!(
            OpenStatus::read(&mut working).await.unwrap(),
            OpenStatus::Ok
        );
        assert_eq!(
            OpenStatus::read(&mut stalled).await.unwrap(),
            OpenStatus::Timeout
        );
        let client = relay.client().await;
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unused_address = unused.local_addr().unwrap();
        drop(unused);
        assert!(matches!(
            client.open_tcp(unused_address).await,
            Err(OpenError::Rejected(OpenStatus::ConnectFailed))
        ));
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn untrusted_client_wrong_ca_and_wrong_server_name_cannot_open_targets() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_, stranger) =
            identity::generate(relay.credentials.endpoint, "cds-relay".into()).unwrap();
        let mut credentials = relay.credentials.clone();
        credentials.cert_pem = stranger.cert_pem.clone();
        credentials.key_pem = stranger.key_pem.clone();
        // A client may finish its side of TLS before seeing the server's rejection.
        if let Ok(client) = QuicClient::connect_with_timeout(&credentials, DEADLINE).await {
            assert!(client.open_tcp(target.local_addr().unwrap()).await.is_err());
        }
        credentials = relay.credentials.clone();
        credentials.ca_cert_pem = stranger.ca_cert_pem;
        assert!(
            QuicClient::connect_with_timeout(&credentials, DEADLINE)
                .await
                .is_err()
        );
        credentials = relay.credentials.clone();
        credentials.server_name = "wrong-relay".into();
        assert!(
            QuicClient::connect_with_timeout(&credentials, DEADLINE)
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(30), target.accept())
                .await
                .is_err()
        );
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_client_certificate_is_rejected_before_target_open() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), false).await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut relay.credentials.ca_cert_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
        if let Ok(connection) = endpoint
            .connect(relay.credentials.endpoint, "cds-relay")
            .unwrap()
            .await
        {
            if let Ok((send, recv)) = connection.open_bi().await {
                let mut stream = Stream::new(send, recv);
                let result = async {
                    OpenRequest {
                        transport: Transport::Tcp,
                        target: target.local_addr().unwrap(),
                    }
                    .write(&mut stream)
                    .await?;
                    OpenStatus::read(&mut stream).await
                }
                .await;
                assert!(result.is_err());
            }
        }
        assert!(
            timeout(Duration::from_millis(30), target.accept())
                .await
                .is_err()
        );
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ipv6_transport_and_targets_and_shutdown_cleanup() {
    timeout(DEADLINE, async {
        let relay = Relay::start(Config::default(), true).await;
        let client = relay.client().await;
        let target = TcpListener::bind("[::1]:0").await.unwrap();
        let mut tcp = client.open_tcp(target.local_addr().unwrap()).await.unwrap();
        let (mut peer, _) = target.accept().await.unwrap();
        let target = UdpSocket::bind("[::1]:0").await.unwrap();
        let mut udp = client.open_udp(target.local_addr().unwrap()).await.unwrap();
        tcp.write_all(b"v6").await.unwrap();
        let mut data = [0; 2];
        peer.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"v6");
        udp.send(b"v6").unwrap();
        let (size, address) = target.recv_from(&mut data).await.unwrap();
        target.send_to(&data[..size], address).await.unwrap();
        assert_eq!(&udp.recv().await.unwrap().unwrap()[..], b"v6");
        relay.stop().await;
        assert_eq!(peer.read(&mut data).await.unwrap(), 0);
        assert!(matches!(tcp.read(&mut data).await, Ok(0) | Err(_)));
        assert!(matches!(udp.recv().await, Ok(None) | Err(_)));
    })
    .await
    .unwrap();
}

#[test]
fn generated_credentials_are_separate_private_and_never_overwritten() {
    let directory = std::env::temp_dir().join(format!("cds-relay-identity-{}", std::process::id()));
    let endpoint = "127.0.0.1:9527".parse().unwrap();
    identity::initialize(&directory, endpoint, "cds-relay".into()).unwrap();
    let server = cds_relay_quic::ServerIdentity::read(directory.join("server.json")).unwrap();
    let client = ClientCredentials::read(directory.join("platform.json")).unwrap();
    assert_ne!(server.key_pem, client.key_pem);
    assert_ne!(server.cert_pem, client.cert_pem);
    assert!(client_config(&client).is_ok());
    assert!(cds_relay_quic::server_config(&server).is_ok());
    assert!(identity::initialize(&directory, endpoint, "cds-relay".into()).is_err());
    assert!(client.write_new(directory.join("platform.json")).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for filename in ["platform.json", "server.json"] {
            assert_eq!(
                std::fs::metadata(directory.join(filename))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
}
