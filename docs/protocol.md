# cds-relay wire protocol v2

All multi-byte integers use big-endian encoding. Each target uses a separate
mutually authenticated TLS 1.3 connection with ALPN `cds-relay-native/1`.
Opening requests select one numeric target and target-side transport protocol;
they carry no individual user authentication. Credential JSON and opening
headers retain version 2. There is no transport negotiation or fallback.

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

## UDP setup and data

After the success status, the relay sends a random 16-byte association ID on
the TLS control connection. Both peers derive a 32-byte PSK using the TLS
exporter label `EXPORTER-cds-relay-native-dtls-v1` and that ID as context.
They establish DTLS 1.2 with `TLS_PSK_WITH_AES_128_GCM_SHA256`, requiring
Extended Master Secret. The relay then sends one zero ready byte on TLS.
`open_udp` returns only after this confirmation. The initial success status
alone does not establish DTLS readiness or target reachability.

Each outer UDP datagram contains the 16-byte ID followed by a DTLS datagram.
Encrypted application records start with a one-byte type: `0` carries one
complete target packet (0–1,152 bytes), and `1` is a keepalive with an empty
body. The routing ID is not a secret or bearer credential. DTLS authenticates
payloads and protects against replay. UDP application data never enters TCP.

The TCP control connection stays open for the association lifetime. EOF,
errors, unexpected control data, target idle expiry, setup failure, or shutdown
removes the mapping and connected target socket. Dropping either client UDP
half also closes the association. There is no UDP half-close, retransmission,
application fragmentation, or reliable fallback. Oversized replies are dropped
whole. Keepalives do not refresh target application activity.

Both protocols must reach the same relay process. The dispatcher requires the
UDP source IP to match the TLS peer and pins the first UDP source port; roaming,
rebinding, and split egress IPs are unsupported. Associations close after 2^24
outbound application records and must be reopened for fresh keys.

See the [native transport guide](native.md) for resource bounds, MTU, NAT,
authentication, and key-lifetime details. Incompatible changes require a new
protocol version; release the image and client/protocol crates together.
