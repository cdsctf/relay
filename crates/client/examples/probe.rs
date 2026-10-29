//! cargo run -p cds-relay-client --example probe -- udp 127.0.0.1:9527 127.0.0.1:9000 hello
use cds_relay_client::Client;
use std::{env, error::Error, io, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() != 4 || !matches!(args[0].as_str(), "tcp" | "udp") {
        return Err("usage: probe <tcp|udp> <relay IP:port> <target IP:port> <payload>".into());
    }
    let relay: SocketAddr = args[1].parse()?;
    let target: SocketAddr = args[2].parse()?;
    tokio::time::timeout(Duration::from_secs(15), async {
        let stream = TcpStream::connect(relay).await?;
        let client = Client::default();
        let payload = args[3].as_bytes();
        let response = if args[0] == "tcp" {
            let mut stream = client
                .open_tcp(stream, target)
                .await
                .map_err(io::Error::other)?;
            stream.write_all(payload).await?;
            stream.shutdown().await?;
            let mut buffer = vec![0; 65_507];
            let size = stream.read(&mut buffer).await?;
            buffer.truncate(size);
            buffer
        } else {
            let (mut reader, mut writer) = client
                .open_udp(stream, target)
                .await
                .map_err(io::Error::other)?
                .split();
            writer.send(payload).await?;
            reader.recv().await?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "relay closed without a response",
                )
            })?
        };
        println!("{}", String::from_utf8_lossy(&response));
        Ok::<_, io::Error>(())
    })
    .await??;
    Ok(())
}
