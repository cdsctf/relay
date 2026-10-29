# Native QUIC backend

The platform initiates one QUIC connection to a reachable relay UDP endpoint. This connection multiplexes independent TCP streams and UDP associations. It requires neither a reverse connection from the relay nor a Kubernetes API connection. The relay still needs a route to the target IP addresses. The platform remains in every user's traffic path.

This is a native QUIC protocol with ALPN `cds-relay/2`, not HTTP/3 or WebTransport. The platform separately terminates its public WebTransport endpoint and maps its streams/datagram flows to relay sessions.

## Trust and credential generation

```sh
cds-relay-server init --out relay-identity --endpoint 192.0.2.10:9527 --server-name cds-relay
```

The endpoint is a numeric address reachable from the platform. The server name is a DNS name or IP in the relay certificate; it is checked during TLS independently of the dial address and does not require a DNS lookup. The generated platform file can be mounted into a binary, a Docker container, or a Kubernetes workload.

The initializer creates a dedicated CA and two different ECDSA P-256 key pairs. One certificate has the TLS server-auth usage and the other has client-auth usage. Both leaf certificates expire 365 days after generation, with five minutes of clock-skew allowance at the start. The CA private key is discarded rather than exported. All future renewals require reinitializing and replacing both identities; there is no built-in enrollment or certificate rotation service.

| File | Holder | Contents |
| --- | --- | --- |
| `server.json` | Relay only | Format version, trusted client CA certificate, server certificate, server private key |
| `platform.json` | Platform only | Format version, endpoint, TLS server name, trusted server CA certificate, client certificate, client private key |

The initializer writes a new directory only. On Unix it requests `0700` for that directory and `0600` for each file; the process umask can further restrict these modes. Files are created exclusively and never overwritten. JSON readers reject unknown fields, unsupported versions, and files larger than 1 MiB. Both files are secrets; do not put them in frontend configuration or return them to a user. Generation prints only the output directory.

Start the relay with its own identity:

```sh
cds-relay-server --listen 0.0.0.0:9527 --identity /run/secrets/relay/server.json
```

When running under another UID, arrange file ownership/read permissions for that process. Do not mount the platform private key on the relay. Credential files are read once when creating a server configuration or connecting a client; changes are not hot-reloaded.

## Authentication boundary

rustls verifies the server certificate chain, validity, TLS server name, and server-auth usage. The relay requires and verifies the client's certificate chain, validity, and client-auth usage against its installation CA. TLS 1.3 is the only enabled TLS version. The application never disables certificate verification and never accepts unauthenticated clients. Server and client configurations explicitly disable 0-RTT data, and the server awaits the complete mutual TLS handshake before accepting target requests.

A client may complete its side of TLS before learning that the relay rejected its certificate. Therefore connection establishment alone is not an application readiness check; a successful target open confirms that the relay accepted authentication and processed that request. No target socket is opened before server-side authentication completes.

An authenticated platform certificate can open any supported numeric target reachable from the relay. There is no target ACL or per-user authentication in this backend. The platform must authorize challenge access before opening a tunnel. Separate installations must use separate generated identities. To revoke a leaked platform credential in this version, replace both identities and restart/reconnect; existing authenticated connections otherwise retain access until closed.

QUIC Retry validates source addresses before a full handshake. Connection/session caps and finite buffers bound application state. These measures do not eliminate CPU, packet-rate, or bandwidth attacks against a public UDP endpoint. Network exposure and deployment policy remain a separate layer.

## Streams and datagrams

Each client-initiated bidirectional stream begins with a version 2 opening request and a status response. After success, TCP uses raw stream bytes with independent half-closes. A UDP stream remains open only to define association lifetime; its payload packets use QUIC DATAGRAM frames with that stream's ID as the flow identifier. Stream IDs are never reused within a connection.

One connected UDP target socket belongs to each association. The relay can forward multiple target replies without waiting for another request. Empty UDP datagrams are valid. Closing either control-stream direction, dropping either client UDP half, transport loss, idle expiry, or shutdown ends the whole association. Pending datagrams are not drained on close.

Packets are unreliable and can be lost, reordered, or discarded under congestion. The relay does not retransmit them or place them in a reliable stream. `send` success means locally queued, not delivered. TCP and UDP still share connection congestion control and link capacity, so large TCP transfers can affect UDP throughput even though UDP does not wait for TCP retransmissions.

## Packet sizes and pressure

The current outbound payload limit is:

```text
min(65507, connection.max_datagram_size() - 8)
```

The eight bytes carry the flow ID. QUIC's limit depends on path MTU, transport overhead, and the peer's negotiated DATAGRAM limit. It can change during a connection, and each direction can have a different limit. There is no fixed promise that a 65,507-byte UDP packet will fit. The initial payload budget is typically around 1,100 bytes or more, but callers must query the API rather than hard-code a value.

The sender checks the current limit and rejects oversized packets. The relay receives a whole target UDP packet before testing the outbound QUIC limit and drops oversized packets rather than truncating them. No application fragmentation/reassembly is implemented. Protocols requiring larger datagrams need their own packetization or a future explicitly versioned fragmentation extension.

Each connection has one datagram reader. It dispatches through bounded per-flow queues using nonblocking enqueue: each queue holds at most 32 packets, each at most 65,507 payload bytes. Unknown flow IDs and malformed datagrams are dropped. An association is registered before its success response, and the client registers before sending its opening request. DATAGRAM and stream ordering are independent; early packets sent without waiting for success can be discarded. Packets arriving after closure are discarded.

Quinn's datagram send and receive buffers are each 256 KiB per connection. Sending uses Quinn's nonblocking, drop-oldest behavior under outgoing buffer pressure; a saturated receive flow instead drops new packets. No reliable queue is added above these buffers. QUIC stream windows are 1 MiB per stream and 8 MiB per connection, with an 8 MiB connection send window.

At most 256 target streams are allowed on one connection. `--max-connections` independently bounds pending/established connections and target sessions across all connections; incomplete target handshakes consume a target slot. Excess connections are refused, and excess target streams are reset. A stream blocked on QUIC capacity is bounded by the client's open timeout. Limits provide finite capacity rather than per-user fairness.

## Compatibility and verification

QUIC is the only platform-to-relay transport and uses ALPN `cds-relay/2` with request version 2. Other opening versions are rejected. The server requires `--identity` when serving and has no TCP listener or transport selector. The Kubernetes example mounts a pre-created server identity Secret and provides an internal UDP Service. External UDP exposure and routing are separate deployment decisions.

For a local authenticated connectivity check, use the `quic_probe` example with `platform.json` and an echo target. UDP checks may fail because of ordinary packet loss. There is no separate unauthenticated QUIC health endpoint.

See [wire protocols](protocol.md) for byte layout. The implementation follows [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html), [QUIC TLS in RFC 9001](https://www.rfc-editor.org/rfc/rfc9001.html), and [QUIC DATAGRAM in RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html).
