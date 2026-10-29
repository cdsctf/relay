use cds_relay_server::{Config, serve};
use clap::Parser;
use std::{io, net::SocketAddr, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "CdsCTF TCP/UDP relay server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9527")]
    listen: SocketAddr,
    /// Check that the listener accepts TCP connections, then exit (for exec probes).
    #[arg(long)]
    check: bool,
    #[arg(long, default_value_t = 256)]
    max_connections: usize,
    #[arg(long, default_value_t = 5)]
    handshake_timeout: u64,
    #[arg(long, default_value_t = 5)]
    connect_timeout: u64,
    #[arg(long, default_value_t = 60)]
    udp_idle_timeout: u64,
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    if args.check {
        std::net::TcpStream::connect_timeout(&args.listen, Duration::from_secs(1))?;
        return Ok(());
    }
    run(args)
}

#[tokio::main]
async fn run(args: Args) -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config {
        max_connections: args.max_connections,
        handshake_timeout: Duration::from_secs(args.handshake_timeout),
        connect_timeout: Duration::from_secs(args.connect_timeout),
        udp_idle_timeout: Duration::from_secs(args.udp_idle_timeout),
    };
    config.validate()?;
    let listener = TcpListener::bind(args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "relay listening");
    let shutdown = CancellationToken::new();
    let signals = async {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        Ok::<_, io::Error>(())
    };
    let server = serve(listener, config, shutdown.clone());
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        signal = signals => {
            shutdown.cancel();
            let result = server.await;
            signal.and(result)
        }
    }
}
