use cds_relay_client::{ClientCredentials, NativeClient};
use cds_relay_server::{Config, identity};
use std::{io, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
const DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn server_pacing_is_shared_across_connections_and_preserves_half_close_and_udp() {
    timeout(DEADLINE, async {
        let relay = Relay::start(
            Config {
                tcp_rate_limit: Some(128 * 1024),
                ..Config::default()
            },
            false,
        )
        .await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let first = relay.client().await;
        let second = relay.client().await;
        let mut sessions = Vec::new();
        let start = tokio::time::Instant::now();
        for (client, value) in [(&first, 0x5a), (&second, 0xa5)] {
            let mut tunnel = client.open_tcp(target.local_addr().unwrap()).await.unwrap();
            let (mut peer, _) = target.accept().await.unwrap();
            let expected = vec![value; 64 * 1024];
            let reply = expected.clone();
            sessions.push(tokio::spawn(async move {
                let server = async {
                    let mut data = Vec::new();
                    peer.read_to_end(&mut data).await.unwrap();
                    assert_eq!(data, expected);
                    let uploaded_at = start.elapsed();
                    peer.write_all(&data).await.unwrap();
                    peer.shutdown().await.unwrap();
                    uploaded_at
                };
                let client = async {
                    tunnel.write_all(&reply).await.unwrap();
                    tunnel.shutdown().await.unwrap();
                    let mut data = Vec::new();
                    tunnel.read_to_end(&mut data).await.unwrap();
                    assert_eq!(data, reply);
                };
                let (uploaded_at, ()) = tokio::join!(server, client);
                uploaded_at
            }));
        }
        // Target-open control and UDP traffic do not spend the TCP payload quota.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut datagrams = first.open_udp(udp.local_addr().unwrap()).await.unwrap();
        datagrams.send(b"during-pacing").await.unwrap();
        let (size, peer) = timeout(Duration::from_secs(1), udp.recv_from(&mut [0; 32]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(size, 13);
        udp.send_to(b"reply", peer).await.unwrap();
        assert_eq!(
            &timeout(Duration::from_secs(1), datagrams.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()[..],
            b"reply"
        );
        let mut last_upload = Duration::ZERO;
        for task in sessions {
            last_upload = last_upload.max(task.await.unwrap());
        }
        assert!(
            last_upload >= Duration::from_millis(850),
            "two connections bypassed the shared quota"
        );
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn server_shutdown_cancels_long_pacing_waits() {
    timeout(DEADLINE, async {
        let relay = Relay::start(
            Config {
                tcp_rate_limit: Some(1),
                ..Config::default()
            },
            false,
        )
        .await;
        let client = relay.client().await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut tunnel = client.open_tcp(target.local_addr().unwrap()).await.unwrap();
        let (mut peer, _) = target.accept().await.unwrap();
        let writer = tokio::spawn(async move { tunnel.write_all(&vec![0x5a; 16384]).await });
        let mut first = [0; 1];
        peer.read_exact(&mut first).await.unwrap();
        timeout(Duration::from_secs(1), relay.stop()).await.unwrap();
        let _ = writer.await.unwrap();
        let mut rest = Vec::new();
        peer.read_to_end(&mut rest).await.unwrap();
        assert!(rest.len() < 16383);
    })
    .await
    .unwrap();
}

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
        let endpoint = cds_relay_server::native::NativeServer::bind(address, &identity, config)
            .await
            .unwrap();
        credentials.endpoint = endpoint.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(endpoint.serve(cancel.clone()));
        Self {
            credentials,
            cancel,
            task,
        }
    }

    async fn client(&self) -> NativeClient {
        NativeClient::connect_with_timeout(&self.credentials, DEADLINE)
            .await
            .unwrap()
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
