# cds-relay QUIC wire protocol v2

All multi-byte integers use big-endian encoding. The only backend transport is QUIC with ALPN `cds-relay/2`. Each client-initiated bidirectional stream begins with a version 2 opening request selecting one numeric target and target-side transport protocol. QUIC authenticates the platform using mutual TLS; opening requests carry no individual user authentication.

## Opening a connection

Client → server:

| Field | Bytes | Value |
| --- | --- | --- |
| magic | 4 | ASCII `CDSR` |
| version | 1 | `2` |
| transport | 1 | `1` for TCP, `2` for UDP |
| address family | 1 | `4` for IPv4, `6` for IPv6 |
| address | 4 / 16 | IP address octets in network byte order |
| port | 2 | Nonzero target port |

The request is 13 bytes for IPv4 or 25 bytes for IPv6. Addresses do not include a hostname, scope ID, or flowinfo.

Server → client: one status byte. The client must wait for a success response before sending application data.

| Status | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Invalid request |
| `2` | Unsupported version |
| `3` | Target socket setup failed |
| `4` | Handshake or target socket setup timed out |

The server closes the stream after a failure response. Incomplete handshakes, transport failures, or capacity exhaustion may instead result in EOF or a reset without a status byte. No further status bytes are sent after a successful handshake.

For TCP, success means the target TCP connection has been established. For UDP, success means the local UDP socket has been bound and connected; it does not establish that a remote UDP service is running.

## TCP data

All bytes after a successful response are raw TCP data. FIN propagates to the opposite socket's write direction while the other direction continues to drain. Errors, a platform disconnect, or server shutdown terminate the session.

## UDP data

Each UDP association has a client-initiated bidirectional control stream. After its opening request and successful response, neither endpoint sends application bytes on that stream. FIN, RESET_STREAM, STOP_SENDING, unexpected control data, idle expiry, connection loss, or shutdown ends the association. There is no half-close or datagram-draining guarantee for UDP.

Each QUIC DATAGRAM payload has this layout:

| Field | Bytes | Meaning |
| --- | --- | --- |
| flow ID | 8 | Full QUIC stream ID of the UDP association, encoded as fixed-width `u64`, not a QUIC variable-length integer |
| payload | Remaining | One complete UDP datagram, possibly empty |

There is no length prefix, target address, sequence number, acknowledgement, or retransmission. The QUIC frame defines the packet boundary. A DATAGRAM shorter than eight bytes, larger than 65,515 bytes, or addressed to an unregistered flow is discarded. IDs are scoped to the QUIC connection and never reused within that connection. TCP stream IDs are not registered as UDP flows.

The client registers its flow before sending OPEN; the server registers before sending success. The client must wait for success before sending datagrams. Stream and DATAGRAM ordering are independent, so packets sent early can be dropped. Full receive queues drop new packets without delaying other flows. Closing the control stream removes the flow mapping; late packets are discarded.

The outbound UDP payload must fit `min(65507, max_datagram_size - 8)` for the current QUIC connection and direction. Oversized platform packets produce a local error. Oversized target packets are dropped whole; no truncation, reliable-stream fallback, or application fragmentation is allowed. A valid eight-byte DATAGRAM carries an empty UDP packet.

Each association uses one connected target socket, preserving a stable source port and filtering unexpected target sources. Successful target sends and successfully queued target replies update the idle deadline. Dropped malformed/oversized/unknown packets do not. Closing the whole connection cancels and joins its target tasks; server shutdown performs the same cleanup for all connections.

See the [QUIC guide](quic.md) for authentication, buffer limits, and credential lifecycle.

## Versioning

Only opening version 2 is supported. Other versions are rejected with `UnsupportedVersion`; no transport fallback or downgrade is available. QUIC DATAGRAM support is required even for a connection initially used only for TCP. Any incompatible change requires a new protocol version. Release the image, client crate, and protocol crates together.
