//! Native QUIC backend: one reliable stream per target, unreliable UDP datagrams.

use crate::{Config, udp_socket, wait_idle};
use cds_relay_protocol::{MAX_DATAGRAM_SIZE, OpenRequest, OpenStatus, Transport};
use cds_relay_quic::{
    DatagramRouter, MAX_STREAMS, Registration, ServerIdentity, Stream, max_udp_payload, send_udp,
    server_config,
};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, copy_bidirectional},
    net::{TcpStream, UdpSocket},
    sync::{Semaphore, watch},
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

pub fn bind_quic(address: SocketAddr, identity: &ServerIdentity) -> io::Result<quinn::Endpoint> {
    quinn::Endpoint::server(server_config(identity)?, address)
}

/// Limits pending/established QUIC connections and target sessions independently.
/// The target-session limit is global across all authenticated connections.
pub async fn serve_quic(
    endpoint: quinn::Endpoint,
    config: Config,
    shutdown: CancellationToken,
) -> io::Result<()> {
    config.validate()?;
    let connections = Arc::new(Semaphore::new(config.max_connections));
    let targets = Arc::new(Semaphore::new(config.max_connections));
    let stop = shutdown.child_token();
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result { tracing::warn!(%error, "QUIC connection task failed"); }
            },
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break; };
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    incoming.refuse();
                    continue;
                };
                // Validate source addresses before spending state on a TLS handshake.
                if !incoming.remote_address_validated() {
                    let _ = incoming.retry();
                    continue;
                }
                let config = config.clone();
                let targets = targets.clone();
                let stop = stop.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    // Await the full mutual TLS handshake; never accept 0-RTT streams.
                    let authenticated = tokio::select! {
                        biased;
                        _ = stop.cancelled() => return,
                        result = timeout(config.handshake_timeout, incoming) => result,
                    };
                    let connection = match authenticated {
                        Ok(Ok(connection)) => connection,
                        result => {
                            tracing::debug!(?result, "QUIC authentication failed or timed out");
                            return;
                        },
                    };
                    if max_udp_payload(&connection).is_none() {
                        connection.close(1u32.into(), b"DATAGRAM required");
                        return;
                    }
                    connection.set_max_concurrent_bi_streams(
                        (config.max_connections.min(MAX_STREAMS as usize) as u32).into(),
                    );
                    handle_connection(connection, config, targets, stop).await;
                });
            }
        }
    }
    stop.cancel();
    endpoint.close(0u32.into(), b"relay shutting down");
    while tasks.join_next().await.is_some() {}
    Ok(())
}

async fn handle_connection(
    connection: quinn::Connection,
    config: Config,
    targets: Arc<Semaphore>,
    stop: CancellationToken,
) {
    let router = Arc::new(DatagramRouter::new(connection.clone()));
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(error) = result { tracing::warn!(%error, "QUIC target task failed"); }
            },
            stream = connection.accept_bi() => {
                let Ok((mut send, mut recv)) = stream else { break; };
                let Ok(permit) = targets.clone().try_acquire_owned() else {
                    let _ = send.reset(2u32.into());
                    let _ = recv.stop(2u32.into());
                    continue;
                };
                let config = config.clone();
                let connection = connection.clone();
                let router = router.clone();
                sessions.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle(Stream::new(send, recv), connection, router, config).await {
                        tracing::debug!(%error, "QUIC target session ended");
                    }
                });
            }
        }
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
}

async fn respond(stream: &mut Stream, status: OpenStatus, duration: Duration) -> io::Result<()> {
    timeout(duration, status.write(stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "QUIC open response timed out"))?
}

async fn handle(
    mut stream: Stream,
    connection: quinn::Connection,
    router: Arc<DatagramRouter>,
    config: Config,
) -> io::Result<()> {
    let request = match timeout(config.handshake_timeout, OpenRequest::read(&mut stream)).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            let status = if error.kind() == io::ErrorKind::Unsupported {
                OpenStatus::UnsupportedVersion
            } else {
                OpenStatus::InvalidRequest
            };
            return respond(&mut stream, status, config.handshake_timeout).await;
        }
        Err(_) => return respond(&mut stream, OpenStatus::Timeout, config.handshake_timeout).await,
    };
    match request.transport {
        Transport::Tcp => {
            let mut target =
                match timeout(config.connect_timeout, TcpStream::connect(request.target)).await {
                    Ok(Ok(target)) => target,
                    Ok(Err(_)) => {
                        return respond(
                            &mut stream,
                            OpenStatus::ConnectFailed,
                            config.handshake_timeout,
                        )
                        .await;
                    }
                    Err(_) => {
                        return respond(&mut stream, OpenStatus::Timeout, config.handshake_timeout)
                            .await;
                    }
                };
            target.set_nodelay(true)?;
            respond(&mut stream, OpenStatus::Ok, config.handshake_timeout).await?;
            copy_bidirectional(&mut stream, &mut target).await?;
            Ok(())
        }
        Transport::Udp => {
            let socket = match timeout(config.connect_timeout, udp_socket(request.target)).await {
                Ok(Ok(socket)) => socket,
                Ok(Err(_)) => {
                    return respond(
                        &mut stream,
                        OpenStatus::ConnectFailed,
                        config.handshake_timeout,
                    )
                    .await;
                }
                Err(_) => {
                    return respond(&mut stream, OpenStatus::Timeout, config.handshake_timeout)
                        .await;
                }
            };
            let flow = router.register(stream.id())?;
            respond(&mut stream, OpenStatus::Ok, config.handshake_timeout).await?;
            relay_udp(stream, connection, flow, socket, config.udp_idle_timeout).await
        }
    }
}

async fn relay_udp(
    mut control: Stream,
    connection: quinn::Connection,
    mut flow: Registration,
    socket: UdpSocket,
    idle: Duration,
) -> io::Result<()> {
    let id = control.id();
    let (activity, receiver) = watch::channel(Instant::now());
    let uplink = async {
        while let Some(packet) = flow.recv().await {
            match socket.send(&packet).await {
                Ok(size) if size == packet.len() => {
                    activity.send_replace(Instant::now());
                }
                Ok(_) => return Err(io::Error::new(io::ErrorKind::WriteZero, "partial UDP send")),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    };
    let downlink = async {
        let mut buffer = vec![0; MAX_DATAGRAM_SIZE + 1];
        loop {
            let size = match socket.recv(&mut buffer).await {
                Ok(size) => size,
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(error) => return Err(error),
            };
            if size > MAX_DATAGRAM_SIZE {
                continue;
            }
            match send_udp(&connection, id, &buffer[..size]) {
                Ok(()) => {
                    activity.send_replace(Instant::now());
                }
                // Oversized target packets are dropped whole, including after MTU changes.
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => continue,
                Err(error) => return Err(error),
            }
        }
    };
    let mut byte = [0];
    let outcome = tokio::select! {
        result = uplink => result,
        result = downlink => result,
        _ = wait_idle(receiver, idle) => Ok(()),
        _ = AsyncReadExt::read(&mut control.recv, &mut byte) => Ok(()),
        _ = control.send.stopped() => Ok(()),
    };
    let _ = control.send.finish();
    outcome
}
