# Transport experiment conclusions

These conclusions summarize the local experiments completed on 2026-10-29.
The project now supports only independent TLS/TCP and DTLS/UDP channels.
Historical QUIC implementations, custom encrypted-UDP prototypes, benchmark
runners, raw measurements, logs, and tuning plans have been removed. This is
a retained decision record, not a reproducible benchmark archive. Figures below
are historical observations rather than measurements of each future revision.

## Transport decision

Use one mutually authenticated TLS/TCP connection per TCP target and a separate
DTLS/UDP association per UDP target. User traffic continues through the platform.
The platform initiates every connection and can run as a binary, Docker process,
or Pod when both relay listeners are reachable. UDP application data never
travels through TCP. The reliable control connection handles authentication,
setup, and association lifetime only.

QUIC DATAGRAM avoids reliable retransmission but shares congestion control with
reliable streams. That coupling affected UDP delivery or tail latency under
loss, jitter, and mixed bulk TCP load in the tested configurations. Quinn BBR
improved below-capacity UDP loss substantially over the tested CUBIC settings,
but did not consistently provide good mixed-load latency or bottleneck fairness.
A favorable BBR result did not establish that every QUIC implementation or
controller behaves the same way. QUIC was removed after the deployment
requirement changed to allow both TCP and UDP ports.

The earlier custom TLS-exporter/AES-GCM UDP prototype demonstrated feasibility,
not a security audit or a reason to maintain a custom cryptographic protocol.
The retained production backend instead uses webrtc-dtls with DTLS 1.2,
exporter-derived PSKs, AES-128-GCM, and required Extended Master Secret.

## Queue, buffer, and scheduling findings

No screened queue size, batching policy, or expiry interval improved loss and
tail latency universally across macOS/Linux workloads. Smaller queues could
reduce observed latency while dropping substantially more packets; larger queues
could reduce drops while increasing tail latency. Compare both metrics together.
Linux batching of already-ready target packets was not a consistent improvement.
Separate QUIC connections and smaller DATAGRAM send buffers did not establish
a general mixed-load solution. These experimental paths are removed.

The native dispatcher retains 128 packets per association and a shared 1 MiB
ciphertext queue budget. These bound resources, not end-to-end delay. Optional
receive expiry is local to that queue and remains disabled by default.

## TCP pacing findings

The relay-wide fractional-credit token bucket addressed underutilization seen
with the earlier timer-only limiter. Upload and download budgets are independent
and shared across connections. This production feature remains available,
disabled by default.

In the historical QUIC pacing study, limiting TCP to 2 Mbps reduced typical UDP
tail latency, at the cost of TCP goodput. The final six mixed-direction holdouts
had median upload/download UDP p99 of 52.17/95.18 ms and TCP goodput of
1.99/1.82 Mbps. Two of the six runs still failed the strict 100 ms latency gate,
reaching 275.03 ms for upload and 142.23 ms for download. Loss stayed within
the predeclared bound. A small TCP ceiling therefore did not demonstrate both
high TCP throughput and a reliable UDP latency bound. Those figures describe
the removed QUIC implementation, not the current native backend's pacing.

TCP pacing is a configurable payload ceiling, not an adaptive capacity estimator
or a guarantee against other traffic. Selecting a different congestion
controller only on the relay did not change an existing remote sender's behavior.

## Native TLS/DTLS comparison

The final native comparison used 18 serial runs: two rounds each of UDP-only,
mixed upload, and mixed download for native TLS/DTLS, QUIC/BBR, and an unencrypted
direct socket baseline. Every implementation used the same new client build.
Tests ran without simultaneous builds or other benchmarks on one ARM64 Linux
VM on the same physical host as the development environment.

The private Docker topology used separate network namespaces, disabled
TSO/GSO/GRO, and HTB/netem. Relay and target shared 1 CPU / 128 MiB; the client
had 2 CPUs / 256 MiB. Each offered-load phase lasted 12 seconds. UDP sent
1,000-byte echo requests at 1,000 packets/s (8 Mbps payload). UDP-only capacity
was 100 Mbps with 20 ± 3 ms delay and 1% random loss per direction. Mixed cases
used 20 Mbps, the same delay, 0.1% loss per direction, and one unbounded bulk TCP
transfer. No TCP payload cap was enabled. Native kernel TCP used CUBIC;
BBR refers to the removed Quinn QUIC controller.

Values are medians of two runs. Loss counts missing round-trip echoes rather
than one-way drops; p99 covers received echoes. TCP goodput counts confirmed
payload and includes drain time.

| Workload | Backend | UDP echo loss % | UDP p99 ms | TCP Mbps |
| --- | --- | ---: | ---: | ---: |
| UDP only | Native TLS/DTLS | 2.037 | 52.1 | — |
| UDP only | QUIC BBR | 1.942 | 51.4 | — |
| UDP only | Direct | 2.042 | 50.9 | — |
| Mixed upload | Native TLS/DTLS | 1.158 | 186.4 | 11.08 |
| Mixed upload | QUIC BBR | 0.242 | 293.7 | 7.64 |
| Mixed upload | Direct | 0.196 | 115.0 | 11.10 |
| Mixed download | Native TLS/DTLS | 0.312 | 148.0 | 11.50 |
| Mixed download | QUIC BBR | 0.150 | 267.7 | 8.41 |
| Mixed download | Direct | 0.204 | 136.2 | 11.20 |

All 18 runs completed without job errors or OOM kills. Every UDP-only run met
the nominal two-leg loss floor plus one percentage point. Every mixed run met
the loss floor plus two percentage points and at least 5 Mbps TCP goodput.
**Every mixed run failed the absolute 100 ms UDP p99 gate, including direct
traffic. No backend passed the complete joint gate.** The largest sampled
relay/target cgroup footprint was 43.27 MiB, not an exact allocation peak.

Native mode improved mixed TCP goodput and UDP tail latency over QUIC BBR in
these runs. It did not improve every metric: native mixed-upload echo loss
ranged from 0.467% to 1.850%, higher than BBR. Direct mixed p99 of
112.7–148.1 ms also demonstrates bottleneck queueing; it does not waive a
failed native latency target. Two rounds on one host do not support broad
statistical or production-capacity claims.

## Kubernetes and development findings

The native backend reached real same-namespace Service ClusterIPs from a macOS
development binary, Docker, and a Pod on the local OrbStack Kubernetes cluster.
Relay Pods ran non-root with a read-only filesystem, all capabilities dropped,
no service-account token, and 1 CPU / 128 MiB limits. The five final ordinary
scenarios had no job errors, untrusted client credentials were rejected, and
Pod deletion terminated active TCP and UDP jobs.

Initial post-replacement UDP reconnect timed out. A validator initially missed
that per-job failure despite a zero process exit code; a later validator also
aborted on an empty EndpointSlice. A subsequent run recorded a refused initial
Service connection and correctly failed overall. In the final sequence,
authenticated startup readiness succeeded on its first attempt; replacement
reconnected on attempt two after an initial refusal. These failures remain
part of the conclusion: readiness/rollout status alone does not establish
immediate data-path readiness, and recovery requires retries. Owned temporary
namespaces, containers, and networks were cleaned up.

Both control TCP and UDP data must reach the same relay process. The deployment
example uses one replica and `Recreate` updates; independent replicas behind
ordinary load balancing require cross-protocol affinity. Replacement interrupts
tunnels. There is no transparent failover or automatic SDK reconnection.

## Remaining limits

Native UDP has no congestion controller. Aggregate offered load must fit the
provisioned backend, and Internet deployments need an appropriate fairness
policy. Separate transports, local expiry, and TCP pacing cannot guarantee UDP
latency at arbitrary load. Current UDP payloads are limited to 1,152 bytes;
source addresses are pinned; associations must reopen after 2^24 outbound
application records. Transparent rekey, roaming, and NAT rebinding are absent.

The measurements exclude cross-host WAN replication, physical-NIC capacity,
the public WebTransport hop, sustained many-tenant load, and long-duration
key rotation. Earlier multi-gigabit loopback results reflected local software,
crypto, offload, and scheduling costs; they were not physical-network capacity
or supported-user-count promises. See the [native guide](native.md) and
[traffic-control guide](traffic-control.md) for the retained implementation.
