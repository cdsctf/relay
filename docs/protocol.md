# cds-relay wire protocol v1

All multi-byte integers use big-endian encoding. One reliable, ordered, bidirectional byte stream carries one TCP connection or UDP association. The protocol does not multiplex targets, renegotiate an existing stream, or carry user authentication.

## Opening a connection

Client → server:

| Field | Bytes | Value |
| --- | --- | --- |
| magic | 4 | ASCII `CDSR` |
| version | 1 | `1` |
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

Both directions repeatedly send the following frame:

| Field | Bytes | Meaning |
| --- | --- | --- |
| length | 4 | Payload length in `0..=65507` |
| payload | length | One complete UDP datagram |

A zero length represents a valid empty datagram, not a heartbeat or error. Decoders validate the length before allocating payload memory. EOF between frames is a clean end; EOF inside a frame is a truncation error. An oversized frame terminates the tunnel. Oversized datagrams received from the target are dropped as whole packets instead of forwarding truncated contents.

Each association uses a separate UDP socket connected to one fixed target, with a distinct source port that remains stable for the association's lifetime. Only replies from that target are accepted. Addresses and ports are not repeated in each frame. The platform and client maintain their own mapping from WebTransport sessions and flows to relay associations outside this protocol.

EOF on either direction of a UDP transport closes the entire association. UDP associations do not provide TCP-style half-close or reply-draining guarantees. Successful datagram transfers update the idle deadline; partial handshakes and partial frames do not extend an association's lifetime. Idle expiry remains active while waiting for target replies or tunnel writes.

`DatagramReader` retains partial frame state, so a cancelled `recv` can be called again. An I/O error moves the reader to a terminal state. If `DatagramWriter::send` is cancelled or fails after starting I/O, the writer is poisoned to prevent another frame from corrupting the incomplete one. The caller must discard both halves of that association. Rejecting an oversized packet before writing does not poison the writer.

## Versioning

This implementation supports only v1. Any incompatible change to the handshake or framing requires a new protocol version. Release the image, client crate, and protocol crate together. Servers must reject unsupported versions rather than guessing a client's format.
