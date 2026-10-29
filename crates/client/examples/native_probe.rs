//! Run with: native_probe <platform.json> <tcp|udp> <target IP:port> <payload>.
use cds_relay_client::{ClientCredentials, RelayClient};
use std::{error::Error, io, net::SocketAddr, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "usage: native_probe <platform.json> <tcp|udp> <target IP:port> <payload>".into(),
        );
    }
    let credentials = ClientCredentials::read(&args[0])?;
    let target: SocketAddr = args[2].parse()?;
    let response = tokio::time::timeout(Duration::from_secs(15), async {
        let client = RelayClient::connect(&credentials).await?;
        match args[1].as_str() {
            "tcp" => {
                let mut stream = client.open_tcp(target).await?;
                stream.write_all(args[3].as_bytes()).await?;
                stream.shutdown().await?;
                let mut buffer = vec![0; 65_507];
                let size = stream.read(&mut buffer).await?;
                buffer.truncate(size);
                Ok::<_, Box<dyn Error>>(buffer)
            }
            "udp" => {
                let mut tunnel = client.open_udp(target).await?;
                tunnel.send(args[3].as_bytes()).await?;
                let packet = tunnel.recv().await?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "UDP association closed before reply",
                    )
                })?;
                Ok(packet.to_vec())
            }
            _ => Err("transport must be tcp or udp".into()),
        }
    })
    .await??;
    println!("{}", String::from_utf8_lossy(&response));
    Ok(())
}
