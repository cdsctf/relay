use cds_relay_client::ClientCredentials;
use cds_relay_server::identity;

#[test]
fn generated_credentials_are_separate_private_and_never_overwritten() {
    let directory = std::env::temp_dir().join(format!("cds-relay-identity-{}", std::process::id()));
    let endpoint = "127.0.0.1:9527".parse().unwrap();
    identity::initialize(&directory, endpoint, "cds-relay".into()).unwrap();
    let server = cds_relay_native::ServerIdentity::read(directory.join("server.json")).unwrap();
    let client = ClientCredentials::read(directory.join("platform.json")).unwrap();
    assert_ne!(server.key_pem, client.key_pem);
    assert_ne!(server.cert_pem, client.cert_pem);
    assert!(cds_relay_native::tls_connector(&client).is_ok());
    assert!(cds_relay_native::tls_acceptor(&server).is_ok());
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
