use crate::{ALPN, MAX_STREAMS, invalid, transport};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    net::SocketAddr,
    path::Path,
    sync::Arc,
};

/// A secret platform-side bundle. Never log or send it to end users.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCredentials {
    pub version: u8,
    pub endpoint: SocketAddr,
    pub server_name: String,
    pub ca_cert_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
}

/// A secret relay-side identity, separate from the platform's private key.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerIdentity {
    pub version: u8,
    pub client_ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
}

fn read_file<T: DeserializeOwned>(path: impl AsRef<Path>) -> io::Result<T> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err(invalid("credential file exceeds 1 MiB"));
    }
    serde_json::from_slice(&bytes).map_err(invalid)
}

fn write_file<T: Serialize>(value: &T, path: impl AsRef<Path>) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(invalid)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

impl ClientCredentials {
    pub fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let credentials: Self = read_file(path)?;
        credentials.validate()?;
        Ok(credentials)
    }

    pub fn write_new(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.validate()?;
        write_file(self, path)
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.version != 2 || self.endpoint.port() == 0 || self.server_name.is_empty() {
            return Err(invalid(
                "expected v2 credentials with an endpoint and TLS server name",
            ));
        }
        rustls::pki_types::ServerName::try_from(self.server_name.clone()).map_err(invalid)?;
        Ok(())
    }
}

impl ServerIdentity {
    pub fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let identity: Self = read_file(path)?;
        identity.validate()?;
        Ok(identity)
    }

    pub fn write_new(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.validate()?;
        write_file(self, path)
    }

    fn validate(&self) -> io::Result<()> {
        if self.version != 2 {
            return Err(invalid("expected v2 server identity"));
        }
        Ok(())
    }
}

fn certs(pem: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = rustls_pemfile::certs(&mut pem.as_bytes()).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(invalid("certificate chain is empty"));
    }
    Ok(certs)
}

fn key(pem: &str) -> io::Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut pem.as_bytes())?
        .ok_or_else(|| invalid("private key is missing"))
}

fn roots(pem: &str) -> io::Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certs(pem)? {
        roots.add(certificate).map_err(invalid)?;
    }
    Ok(roots)
}

pub fn client_config(credentials: &ClientCredentials) -> io::Result<quinn::ClientConfig> {
    credentials.validate()?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(invalid)?
        .with_root_certificates(roots(&credentials.ca_cert_pem)?)
        .with_client_auth_cert(certs(&credentials.cert_pem)?, key(&credentials.key_pem)?)
        .map_err(invalid)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.enable_early_data = false;
    let mut config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).map_err(invalid)?));
    config.transport_config(Arc::new(transport(0)));
    Ok(config)
}

pub fn server_config(identity: &ServerIdentity) -> io::Result<quinn::ServerConfig> {
    identity.validate()?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots(&identity.client_ca_pem)?),
        provider.clone(),
    )
    .build()
    .map_err(invalid)?;
    let mut tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(invalid)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs(&identity.cert_pem)?, key(&identity.key_pem)?)
        .map_err(invalid)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.max_early_data_size = 0;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(tls).map_err(invalid)?,
    ));
    config.transport_config(Arc::new(transport(MAX_STREAMS)));
    config.max_incoming(256);
    Ok(config)
}
