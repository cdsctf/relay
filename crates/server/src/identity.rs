//! Generate a separate relay and platform identity for one installation.

use cds_relay_quic::{ClientCredentials, ServerIdentity, invalid};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::{io, net::SocketAddr, path::Path};

/// The CA signing key is discarded. Reinitialize both identities to rotate trust.
/// Leaf certificates expire after 365 days; the platform never receives the server key.
pub fn generate(
    endpoint: SocketAddr,
    server_name: String,
) -> io::Result<(ServerIdentity, ClientCredentials)> {
    let now = time::OffsetDateTime::now_utc();
    let mut ca_params = CertificateParams::new(Vec::new()).map_err(invalid)?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "cds-relay installation CA");
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params.not_before = now - time::Duration::minutes(5);
    ca_params.not_after = now + time::Duration::days(366);
    let ca_key = KeyPair::generate().map_err(invalid)?;
    let ca = ca_params.self_signed(&ca_key).map_err(invalid)?;

    let mut server_params = CertificateParams::new(vec![server_name.clone()]).map_err(invalid)?;
    server_params
        .distinguished_name
        .push(DnType::CommonName, "cds-relay server");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.not_before = now - time::Duration::minutes(5);
    server_params.not_after = now + time::Duration::days(365);
    let server_key = KeyPair::generate().map_err(invalid)?;
    let server = server_params
        .signed_by(&server_key, &ca, &ca_key)
        .map_err(invalid)?;

    let mut client_params = CertificateParams::new(Vec::new()).map_err(invalid)?;
    client_params
        .distinguished_name
        .push(DnType::CommonName, "cds-relay platform");
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    client_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    client_params.not_before = now - time::Duration::minutes(5);
    client_params.not_after = now + time::Duration::days(365);
    let client_key = KeyPair::generate().map_err(invalid)?;
    let client = client_params
        .signed_by(&client_key, &ca, &ca_key)
        .map_err(invalid)?;

    let identity = ServerIdentity {
        version: 2,
        client_ca_pem: ca.pem(),
        cert_pem: server.pem(),
        key_pem: server_key.serialize_pem(),
    };
    let credentials = ClientCredentials {
        version: 2,
        endpoint,
        server_name,
        ca_cert_pem: ca.pem(),
        cert_pem: client.pem(),
        key_pem: client_key.serialize_pem(),
    };
    credentials.validate()?;
    Ok((identity, credentials))
}

/// Creates a new private directory; never overwrites an existing installation.
pub fn initialize(directory: &Path, endpoint: SocketAddr, server_name: String) -> io::Result<()> {
    let (identity, credentials) = generate(endpoint, server_name)?;
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(directory)?;
    identity.write_new(directory.join("server.json"))?;
    credentials.write_new(directory.join("platform.json"))
}
