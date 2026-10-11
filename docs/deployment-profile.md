# Deployment profile after Sider 1.0.1

This document defines the current operating profile for the published
[v1.0.1 release](https://github.com/djairofilho/sider/releases/tag/v1.0.1),
built from source SHA `b738a0b4f97074fe272bc21f1eacf5db93cd020e`.
It is the deliverable for [issue #105](https://github.com/djairofilho/sider/issues/105).
The release's validation archive and manifest belong to that SHA. Results from
the current `main` or earlier releases do not attest to the published binary.

## Supported profile

Run one Sider instance on Windows 11 x64 or Ubuntu 24.04 x86_64 GNU, using the
matching published package. Bind the RESP listener to loopback. Enable AOF with
`SIDER_AOF_SYNC=always` when data must survive a process restart. Keep the AOF
directory under exclusive local control and maintain verified backups in a
separate directory or volume. Use clients that send the documented RESP2 subset
without a required unsupported handshake. The [client audit](client-integration.md)
names two tested RESP2 client versions and their required options on Windows.

This profile is suitable for local development and controlled single-host use.
It has no authentication, ACL, or TLS. A non-loopback bind emits a warning but is
not blocked; it does not make the listener safe for untrusted or shared networks.
The internal replication, backup, and administration listener also lacks
authentication and TLS. Do not expose either listener publicly.

An optional replica requires AOF on both instances, matching Sider version and
shard layout, and a controlled connection between them. Replication is
asynchronous and promotion is manual. The operator must remove the old primary
from write traffic before redirecting clients to a promoted replica. A private
network alone is an environmental control, not a Sider authorization feature.

## Data and recovery contract

| Mode or event | Current contract | Operator action |
| --- | --- | --- |
| No AOF | The dataset is in memory and disappears when the process terminates. | Use only for disposable data. |
| AOF `always`, ordinary restart or process crash | Acknowledged batches have completed `write_all` and `sync_all` before application and response; the process crash suite checks their recovery. Filesystem or device failure can still defeat durability. | Restart with the same directory, shard count, routing layout, and compatible version; inspect readiness and data. |
| AOF `everysec`, system failure | Acknowledged writes can be lost until a successful sync; one second is a scheduling target, not a strict loss bound. | Use only when this loss mode is accepted. |
| Replica promotion | The replica can lack an acknowledged primary suffix; there is no guaranteed time or operation loss bound. | Stop or fence old primary writes, inspect replica position, promote explicitly, then redirect clients. |
| Backup restore | A verified backup restores a consistent snapshot into a new directory; TTL continues to elapse after capture. Writes after the captured point are absent. | Verify checksums and layout, restore into an isolated directory, start a separate instance, and compare expected data before switching clients. |

See [AOF persistence](persistence.md), [backup](backup.md), and
[replication](replication.md) for exact procedures and failure boundaries.
Neither process crash tests nor file checksums prove survival of power loss,
device failure, or malicious file replacement. The logical dataset quota does
not cap RSS; queues, buffers, snapshots, and replication history use additional
memory.

## Operator schedule and validation targets

For the controlled single-host profile, schedule and verify a backup at least
once every 24 hours and before a planned migration or promotion. Keep the last
verified backup on a separate directory or volume. Alert when a scheduled backup
or verification fails. Sider does not schedule backups; this cadence is an
operator procedure, not a guaranteed 24-hour recovery point. Recovery from a
backup loses changes after its captured cut. AOF `always` is the primary process
crash recovery path, subject to the filesystem and device limits above.

The following are **targets for the next native-host validation**, not measured
service levels or claims about the published package. The source, package,
hardware, cache state, dataset and workload must be recorded with each run.

| Target | Measurable check | Current evidence and limit |
| --- | --- | --- |
| Recovery | With one and four shards and a dataset between 63 and 64 MiB logical usage, complete AOF restart or isolated backup restore through the full data and TTL oracle within 60 seconds on each native supported platform. | The final Windows observations were below 19 seconds; the WSL2 Ubuntu observations were below 30 seconds. Native Ubuntu and cold-cache conditions remain unmeasured. |
| Operating latency | For the [resource-envelope saturation workload](resource-envelope.md) with eight clients, 1,024 concurrent `SET`s, 128-byte values, AOF `always`, and one or four shards, keep p99 round-trip latency at or below 250 ms on each native supported platform. | WSL2 observed 93.88 ms for one shard and 155.64 ms for four. This does not predict another host or a near-quota dataset. |
| Operating memory and queues | For that same workload, record peak RSS below 256 MiB and no full worker or AOF queue. Also record logical bytes separately; the 64 MiB quota is not an RSS bound. | WSL2 sampled at most 11.73 MiB RSS and neither queue filled. Windows and native Ubuntu measurements remain pending. |

The operator must set stricter recovery-point, recovery-time, latency, and memory
requirements for their application and verify them on its actual host and workload.
Missing or failed native-host results leave these project targets unachieved; they
do not turn into a production guarantee.

## Recovery and resource evidence still needed

The [recovery drill](recovery-drill.md) exercised the published v1.0.1 packages
with a dataset near the default 64 MiB logical quota and one and four shards.
The final Windows 11 run and an Ubuntu 24.04 run under WSL2 passed backup restore,
AOF restart, incomplete-tail repair, TTL deadline checks, and negative corruption
cases. The runbook links raw logs, package hashes, hardware context, and measured
phase times. The WSL2 result establishes a virtualized Linux functional run;
a native Ubuntu host run is still pending. These observations are **not a
published recovery-time guarantee**. [Issue #107](https://github.com/djairofilho/sider/issues/107)
tracks the remaining evidence.

The [resource-envelope exercise](resource-envelope.md) now records throughput,
p50/p95/p99 command latency, RSS, CPU, queue pressure, startup, recovery, slow
clients, expiration, and compaction for one declared workload on Ubuntu 24.04
under WSL2. It is descriptive evidence from a virtualized host; it does not set
an operating envelope for Windows or native Ubuntu. The
[resource-envelope issue](https://github.com/djairofilho/sider/issues/108) tracks
that remaining work. [Comparative benchmarking](https://github.com/djairofilho/sider/issues/97)
answers a separate Sider-versus-Redis question. No throughput, latency, RSS,
or recovery-time service level is promised here.

Networked production support requires a reviewed
[security contract](security-boundary.md) ([issue #115](https://github.com/djairofilho/sider/issues/115)), verified
[TLS, authentication, and ACL implementation](https://github.com/djairofilho/sider/issues/93),
and complete [release validation](https://github.com/djairofilho/sider/issues/96).
Automatic failover, online resharding, Redis Cluster, cross-shard transactions,
RESP3, and Lua are outside the published 1.0.1 profile. Their development and
issue states are tracked separately.

## Evidence and change control

- The [v1.0.1 release](https://github.com/djairofilho/sider/releases/tag/v1.0.1)
  publishes Windows and Linux packages, an image archive, manifest, checksums,
  and a validation ZIP for source SHA
  `b738a0b4f97074fe272bc21f1eacf5db93cd020e`.
- The [runtime requirements](runtime-requirements.md) define the tested native
  systems without claiming older OS, musl, ARM64, or a clean Windows install.
- The [compatibility matrix](compatibility-matrix.md) defines the command subset
  and distinguishes historical test counts from candidate evidence.
- A new source SHA, configuration, type, wire protocol, or release bundle needs
  its own affected tests and evidence. Do not transfer receipts between builds.

Operators should validate a downloaded package and archive against the published
SHA-256 list before use. The [release guide](releases.md) defines the candidate
and final publication process; this deployment profile does not approve a new
release or close its gates.
