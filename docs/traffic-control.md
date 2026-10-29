# Relay traffic control

The server provides an optional aggregate TCP payload budget, independently
for uploads and downloads. Every connection in one relay process shares that
direction's allowance. Opening more connections cannot multiply it. Replicas
have separate budgets; this is not a distributed limiter.

```sh
cds-relay-server --identity /run/relay/server.json --listen 0.0.0.0:9527 \
  --tcp-rate-limit-mbps 2
```

This example sets a ceiling of 2 decimal Mbps of TCP application payload in
each direction. It is illustrative, not a universal deployment setting. Zero,
the default, disables pacing. Library users set `Config::tcp_rate_limit` in
bytes per second before calling `NativeServer::bind`.

## TCP enforcement

A FIFO asynchronous token bucket retains fractional-byte credit between timer
wakeups. Burst credit is one millisecond of the configured rate, bounded
between 8 KiB and 256 KiB; idle time cannot accumulate more. Each copy direction
reads at most one 4 KiB application chunk ahead of its pacing wait. Slower
reads/writes apply ordinary TCP backpressure to the platform or target.
TCP bytes are delayed rather than dropped, and independent half-closes and
replies after request EOF are preserved. Cancellation and shutdown interrupt
pending waits without reserving future bandwidth. With pacing disabled,
forwarding uses Tokio's unrestricted bidirectional copy.

At shutdown, an enabled pacer logs admitted payload bytes, wait counts, and
acquisition time in each direction. Admitted bytes include quota charged before
a write that subsequently failed; they are not delivery confirmations.
Acquisition time includes mutex contention and timer overhead.

## UDP expiry and observability

UDP data and target-open control messages do not consume the TCP payload
budget. `--udp-max-queue-delay-ms 20` optionally expires packets waiting more
than 20 ms after arrival in the relay's raw receive-dispatch queue. Zero,
the default, disables expiry. The library setting is
`Config::udp_max_queue_delay`, with a maximum of one second.

This expiry does not cover client queues, DTLS internal queues, target sockets,
or network buffers. It is not an end-to-end deadline. Keep expiry disabled
unless the application's semantics justify dropping old packets.

With `RUST_LOG=cds_relay_server::native=debug`, shutdown logs include dispatcher
counts for unknown IDs, full queues, shared-byte-budget exhaustion, and expiry.
These do not account for every kernel, network, or DTLS rejection.

## Limits

The TCP ceiling excludes TLS/DTLS overhead, kernel acknowledgements and
retransmissions, and unrelated traffic. It does not shape the physical link,
reserve capacity, estimate available bandwidth, or configure the kernel's
congestion controller. Delivered goodput can be lower than the configured
ceiling because of scheduling, timers, RTT, and competing traffic.

Native UDP has no congestion controller. Size and admit aggregate UDP traffic
for the provisioned path; UDP alone can saturate a link or CPU. TCP pacing and
receive expiry cannot guarantee low UDP loss or latency. The
[experiment conclusions](experiments.md) retain both the measured benefits
and the failed latency holdouts.
