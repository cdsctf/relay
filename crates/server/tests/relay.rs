use cds_relay_client::{Client, OpenError};
use cds_relay_protocol::{MAGIC, OpenRequest, OpenStatus, Transport, VERSION};
use cds_relay_server::{Config, serve};
use std::{io, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const DEADLINE: Duration = Duration::from_secs(3);

struct Relay {
    address: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}
impl Relay {
    async fn start(config: Config) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve(listener, config, cancel.clone()));
        Self {
            address,
            cancel,
            task,
        }
    }
    async fn stream(&self) -> TcpStream {
        TcpStream::connect(self.address).await.unwrap()
    }
    async fn stop(self) {
        self.cancel.cancel();
        tokio::time::timeout(DEADLINE, self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn tcp_half_close_drains_the_target_response() {
    tokio::time::timeout(DEADLINE, async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let payload = vec![0x5a; 256 * 1024];
        let expected = payload.clone();
        let target_task = tokio::spawn(async move {
            let (mut stream, _) = target.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, expected);
            stream.write_all(&request).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let relay = Relay::start(Config::default()).await;
        let mut stream = Client::default()
            .open_tcp(relay.stream().await, address)
            .await
            .unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, payload);
        target_task.await.unwrap();
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn udp_sessions_keep_distinct_stable_source_ports_and_empty_packets() {
    tokio::time::timeout(DEADLINE, async {
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let relay = Relay::start(Config::default()).await;
        let client = Client::default();
        let (mut a_read, mut a_write) = client
            .open_udp(relay.stream().await, address)
            .await
            .unwrap()
            .split();
        let (mut b_read, mut b_write) = client
            .open_udp(relay.stream().await, address)
            .await
            .unwrap()
            .split();
        let mut buffer = [0; 64];
        a_write.send(b"a").await.unwrap();
        let (size, a_peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"a");
        b_write.send(b"b").await.unwrap();
        let (size, b_peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"b");
        assert_ne!(a_peer, b_peer);
        // Reply in reverse order, then send an unsolicited second reply to A.
        target.send_to(b"reply-b", b_peer).await.unwrap();
        target.send_to(b"reply-a", a_peer).await.unwrap();
        target.send_to(b"extra-a", a_peer).await.unwrap();
        assert_eq!(b_read.recv().await.unwrap().unwrap(), b"reply-b");
        assert_eq!(a_read.recv().await.unwrap().unwrap(), b"reply-a");
        assert_eq!(a_read.recv().await.unwrap().unwrap(), b"extra-a");
        a_write.send(b"").await.unwrap();
        let (size, peer) = target.recv_from(&mut buffer).await.unwrap();
        assert_eq!(size, 0);
        assert_eq!(peer, a_peer);
        target.send_to(b"", peer).await.unwrap();
        assert_eq!(a_read.recv().await.unwrap().unwrap(), b"");
        // Connected sockets only accept replies from the configured target.
        let stranger = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        stranger.send_to(b"wrong-target", a_peer).await.unwrap();
        target.send_to(b"right-target", a_peer).await.unwrap();
        assert_eq!(a_read.recv().await.unwrap().unwrap(), b"right-target");
        a_write.shutdown().await.unwrap();
        assert!(a_read.recv().await.unwrap().is_none());
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn udp_idle_timeout_also_interrupts_partial_frames() {
    let relay = Relay::start(Config {
        udp_idle_timeout: Duration::from_millis(50),
        ..Config::default()
    })
    .await;
    let mut stream = relay.stream().await;
    let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    OpenRequest {
        transport: Transport::Udp,
        target: target.local_addr().unwrap(),
    }
    .write(&mut stream)
    .await
    .unwrap();
    assert_eq!(OpenStatus::read(&mut stream).await.unwrap(), OpenStatus::Ok);
    stream.write_all(&[0]).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(DEADLINE, stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(bytes.is_empty());
    relay.stop().await;
}

#[tokio::test]
async fn handshake_reports_bad_versions_invalid_targets_and_connection_failure() {
    tokio::time::timeout(DEADLINE, async {
        let relay = Relay::start(Config::default()).await;
        let mut stream = relay.stream().await;
        let mut prefix = MAGIC.to_vec();
        prefix.extend_from_slice(&[VERSION + 1, 1, 4]);
        stream.write_all(&prefix).await.unwrap();
        assert_eq!(
            OpenStatus::read(&mut stream).await.unwrap(),
            OpenStatus::UnsupportedVersion
        );
        let mut stream = relay.stream().await;
        let mut request = MAGIC.to_vec();
        request.extend_from_slice(&[VERSION, 1, 4, 127, 0, 0, 1, 0, 0]);
        stream.write_all(&request).await.unwrap();
        assert_eq!(
            OpenStatus::read(&mut stream).await.unwrap(),
            OpenStatus::InvalidRequest
        );
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unused_address = unused.local_addr().unwrap();
        drop(unused);
        let result = Client::default()
            .open_tcp(relay.stream().await, unused_address)
            .await;
        assert!(matches!(
            result,
            Err(OpenError::Rejected(OpenStatus::ConnectFailed))
        ));
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn incomplete_handshake_times_out_without_blocking_another_session() {
    tokio::time::timeout(DEADLINE, async {
        let relay = Relay::start(Config {
            handshake_timeout: Duration::from_millis(80),
            ..Config::default()
        })
        .await;
        let mut stalled = relay.stream().await;
        stalled.write_all(b"C").await.unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tunnel = Client::default()
            .open_udp(relay.stream().await, target.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(
            OpenStatus::read(&mut stalled).await.unwrap(),
            OpenStatus::Timeout
        );
        drop(tunnel);
        relay.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn connection_limit_and_shutdown_close_active_sessions() {
    tokio::time::timeout(DEADLINE, async {
        let relay = Relay::start(Config {
            max_connections: 1,
            ..Config::default()
        })
        .await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut stream = Client::default()
            .open_tcp(relay.stream().await, target.local_addr().unwrap())
            .await
            .unwrap();
        let (mut target_stream, _) = target.accept().await.unwrap();
        let mut excess = relay.stream().await;
        assert_eq!(excess.read(&mut [0]).await.unwrap(), 0);
        relay.stop().await;
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(target_stream.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ipv6_tcp_and_udp_targets_work() {
    tokio::time::timeout(DEADLINE, async {
        let target = TcpListener::bind("[::1]:0").await.unwrap();
        let udp = UdpSocket::bind("[::1]:0").await.unwrap();
        let relay = Relay::start(Config::default()).await;
        let mut stream = Client::default()
            .open_tcp(relay.stream().await, target.local_addr().unwrap())
            .await
            .unwrap();
        let (mut target_stream, _) = target.accept().await.unwrap();
        stream.write_all(b"v6").await.unwrap();
        let mut data = [0; 2];
        target_stream.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"v6");
        let (mut read, mut write) = Client::default()
            .open_udp(relay.stream().await, udp.local_addr().unwrap())
            .await
            .unwrap()
            .split();
        write.send(b"v6").await.unwrap();
        let (size, peer) = udp.recv_from(&mut data).await.unwrap();
        udp.send_to(&data[..size], peer).await.unwrap();
        assert_eq!(read.recv().await.unwrap().unwrap(), b"v6");
        relay.stop().await;
    })
    .await
    .unwrap();
}
