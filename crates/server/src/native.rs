//! Native TCP forwarding and independent, authenticated DTLS datagrams.
use crate::{Activity, Config, traffic::Traffic, udp_socket};
use cds_relay_native::ServerIdentity;
use cds_relay_native::{
    ALPN, DatagramHub, EXPORTER, MAX_RECORDS, MAX_UDP_PAYLOAD, dtls_config, invalid, tls_acceptor,
};
use cds_relay_protocol::{OpenRequest, OpenStatus, Transport};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use webrtc_dtls::conn::DTLSConn;

pub struct NativeServer {
    listener: TcpListener,
    hub: DatagramHub,
    acceptor: TlsAcceptor,
    config: Config,
}

impl NativeServer {
    /// Binds TCP and UDP at the same numerical port; zero chooses one common port.
    pub async fn bind(
        address: SocketAddr,
        identity: &ServerIdentity,
        config: Config,
    ) -> io::Result<Self> {
        config.validate()?;
        let acceptor = tls_acceptor(identity)?;
        let listener = TcpListener::bind(address).await?;
        let hub = DatagramHub::bind(listener.local_addr()?, config.udp_max_queue_delay).await?;
        Ok(Self {
            listener,
            hub,
            acceptor,
            config,
        })
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
    pub async fn serve(self, stop: CancellationToken) -> io::Result<()> {
        let capacity = Arc::new(Semaphore::new(self.config.max_connections));
        let traffic = Traffic::new(self.config.tcp_rate_limit);
        let mut tasks = JoinSet::new();
        let hub_stop = stop.child_token();
        let hub = self.hub.clone();
        let dispatcher = hub.run(hub_stop.clone());
        tokio::pin!(dispatcher);
        let result = loop {
            tokio::select! { biased;
                _ = stop.cancelled() => break Ok(()),
                result = &mut dispatcher => break result,
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(error) = result { tracing::warn!(%error, "native session task failed"); }
                },
                incoming = self.listener.accept() => {
                    let (socket, peer) = match incoming { Ok(pair) => pair, Err(error) => break Err(error) };
                    let Ok(permit) = capacity.clone().try_acquire_owned() else { continue; };
                    let acceptor = self.acceptor.clone(); let config = self.config.clone();
                    let hub = self.hub.clone(); let traffic = traffic.clone(); let session_stop = hub_stop.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        tokio::select! { biased;
                            _ = session_stop.cancelled() => {},
                            result = handle(socket, peer, acceptor, hub, config, traffic) => {
                                if let Err(error) = result { tracing::debug!(%error, "native session ended"); }
                            },
                        }
                    });
                },
            }
        };
        hub_stop.cancel();
        while tasks.join_next().await.is_some() {}
        traffic.log();
        tracing::debug!(stats = ?self.hub.stats(), "native UDP dispatcher stopped");
        result
    }
}

async fn handle(
    socket: TcpStream,
    peer: SocketAddr,
    acceptor: TlsAcceptor,
    hub: DatagramHub,
    config: Config,
    traffic: Traffic,
) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let (mut control, request) = timeout(config.handshake_timeout, async {
        let mut control = acceptor.accept(socket).await?;
        if control.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(invalid("native ALPN required"));
        }
        let request = match OpenRequest::read(&mut control).await {
            Ok(request) => request,
            Err(error) => {
                let status = if error.kind() == io::ErrorKind::Unsupported {
                    OpenStatus::UnsupportedVersion
                } else {
                    OpenStatus::InvalidRequest
                };
                status.write(&mut control).await?;
                return Err(error);
            }
        };
        Ok((control, request))
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "native handshake timed out"))??;
    if request.transport == Transport::Tcp {
        let mut target =
            match timeout(config.connect_timeout, TcpStream::connect(request.target)).await {
                Ok(Ok(target)) => target,
                result => {
                    let status = if result.is_err() {
                        OpenStatus::Timeout
                    } else {
                        OpenStatus::ConnectFailed
                    };
                    timeout(config.handshake_timeout, status.write(&mut control))
                        .await
                        .map_err(invalid)??;
                    return Ok(());
                }
            };
        target.set_nodelay(true)?;
        timeout(config.handshake_timeout, OpenStatus::Ok.write(&mut control))
            .await
            .map_err(invalid)??;
        return traffic.copy(&mut control, &mut target).await;
    }
    let target = match timeout(config.connect_timeout, udp_socket(request.target)).await {
        Ok(Ok(target)) => target,
        result => {
            let status = if result.is_err() {
                OpenStatus::Timeout
            } else {
                OpenStatus::ConnectFailed
            };
            timeout(config.handshake_timeout, status.write(&mut control))
                .await
                .map_err(invalid)??;
            return Ok(());
        }
    };
    let route = hub.register(peer.ip())?;
    let secret = control
        .get_ref()
        .1
        .export_keying_material([0; 32], EXPORTER, Some(&route.id))
        .map_err(invalid)?;
    let dtls = timeout(config.handshake_timeout, async {
        OpenStatus::Ok.write(&mut control).await?;
        control.write_all(&route.id).await?; control.flush().await?;
        let mut byte = [0];
        let dtls = tokio::select! { biased;
            _ = control.read(&mut byte) => return Err(invalid("control closed during DTLS handshake")),
            result = DTLSConn::new(route.pipe.clone(), dtls_config(secret, route.id), false, None) => result.map_err(invalid)?,
        };
        control.write_u8(0).await?; control.flush().await?;
        Ok::<_, io::Error>(dtls)
    }).await.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DTLS handshake timed out"))??;
    let activity = Activity::new();
    let upload = async {
        let mut buffer = vec![0; MAX_UDP_PAYLOAD + 1];
        let mut records = 0;
        loop {
            let size = dtls.read(&mut buffer, None).await.map_err(invalid)?;
            if size == 0 || buffer[0] != 0 {
                continue;
            }
            records += 1;
            if records > MAX_RECORDS {
                return Err(invalid("DTLS record budget exhausted"));
            }
            target.send(&buffer[1..size]).await?;
            activity.touch();
        }
        #[allow(unreachable_code)]
        Ok::<_, io::Error>(())
    };
    let download = async {
        let mut buffer = vec![0; 65536];
        let mut records = 0;
        loop {
            let size = match target.recv(&mut buffer[1..]).await {
                Ok(size) => size,
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(error) => return Err(error),
            };
            if size > MAX_UDP_PAYLOAD {
                continue;
            }
            records += 1;
            if records > MAX_RECORDS {
                return Err(invalid("DTLS record budget exhausted"));
            }
            buffer[0] = 0;
            dtls.write(&buffer[..size + 1], None)
                .await
                .map_err(invalid)?;
            activity.touch();
        }
        #[allow(unreachable_code)]
        Ok::<_, io::Error>(())
    };
    let mut byte = [0];
    tokio::select! { result = upload => result, result = download => result,
    _ = activity.wait_idle(config.udp_idle_timeout) => Ok(()), _ = control.read(&mut byte) => Ok(()), }
}
