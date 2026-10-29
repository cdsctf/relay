use cds_relay_protocol::{
    DatagramReader, DatagramWriter, MAX_DATAGRAM_SIZE, OpenRequest, Transport,
};
use std::{io, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

#[tokio::test]
async fn requests_roundtrip_both_address_families_and_transports() {
    for target in ["127.0.0.1:8080", "[::1]:5353"] {
        for transport in [Transport::Tcp, Transport::Udp] {
            let request = OpenRequest {
                transport,
                target: target.parse().unwrap(),
            };
            let (mut left, mut right) = duplex(3);
            let write = tokio::spawn(async move {
                request.write(&mut left).await.unwrap();
            });
            assert_eq!(OpenRequest::read(&mut right).await.unwrap(), request);
            write.await.unwrap();
        }
    }
}

#[tokio::test]
async fn empty_and_maximum_packets_survive_fragmented_streams() {
    let (left, right) = duplex(17);
    let task = tokio::spawn(async move {
        let mut writer = DatagramWriter::new(left);
        writer.send(b"").await.unwrap();
        writer.send(&vec![0x5a; MAX_DATAGRAM_SIZE]).await.unwrap();
        writer.send(b"last").await.unwrap();
        writer.shutdown().await.unwrap();
    });
    let mut reader = DatagramReader::new(right);
    assert_eq!(reader.recv().await.unwrap().unwrap(), b"");
    assert_eq!(
        reader.recv().await.unwrap().unwrap(),
        vec![0x5a; MAX_DATAGRAM_SIZE]
    );
    assert_eq!(reader.recv().await.unwrap().unwrap(), b"last");
    assert!(reader.recv().await.unwrap().is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn cancelled_reads_resume_in_both_header_and_payload() {
    let (mut wire, input) = duplex(32);
    let mut reader = DatagramReader::new(input);
    wire.write_all(&[0, 0]).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), reader.recv())
            .await
            .is_err()
    );
    wire.write_all(&[0, 3, b'a']).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), reader.recv())
            .await
            .is_err()
    );
    wire.write_all(b"bc").await.unwrap();
    assert_eq!(reader.recv().await.unwrap().unwrap(), b"abc");
}

#[tokio::test]
async fn malformed_frames_are_terminal_and_rejected_before_payload_allocation() {
    for bytes in [
        vec![0, 0],
        vec![0, 0, 0, 3, 42],
        u32::MAX.to_be_bytes().to_vec(),
    ] {
        let (mut wire, input) = duplex(32);
        wire.write_all(&bytes).await.unwrap();
        wire.shutdown().await.unwrap();
        let mut reader = DatagramReader::new(input);
        assert!(reader.recv().await.is_err());
        assert!(reader.recv().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn cancelling_a_partial_write_poisons_the_writer() {
    let (wire, mut output) = duplex(2);
    let mut writer = DatagramWriter::new(wire);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), writer.send(b"payload"))
            .await
            .is_err()
    );
    assert_eq!(
        writer.send(b"next").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    let mut prefix = [0; 2];
    output.read_exact(&mut prefix).await.unwrap();
    assert_eq!(prefix, [0, 0]);
}

#[tokio::test]
async fn rejecting_an_oversized_send_does_not_poison_the_writer() {
    let (wire, output) = duplex(32);
    let mut writer = DatagramWriter::new(wire);
    assert!(writer.send(&vec![0; MAX_DATAGRAM_SIZE + 1]).await.is_err());
    writer.send(b"ok").await.unwrap();
    assert_eq!(
        DatagramReader::new(output).recv().await.unwrap().unwrap(),
        b"ok"
    );
}
