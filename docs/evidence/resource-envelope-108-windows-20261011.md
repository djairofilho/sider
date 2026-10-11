# Issue #108: Windows resource-envelope observation

Run date: 2026-10-11. Source commit:
`ad801c2b7e9ae6adc1f4b4c93be5189afffa3981`; Git tree:
`d235ce2e1bf6ad7f571fb8faab25c94886d5b737` (clean checkout).
Runner: `cargo test --locked --test resource_envelope -- --ignored --exact resource_envelope_workload --nocapture`.
The test exited zero after 51.68 seconds, completed all 18 declared scenarios,
and wrote a schema-2 `measurement_only` report. Its fixture SHA-256 is
`f90e1ba198009eac3f954db80ab34390357221dec78b01c40b8964895fd6ebef`.

The locally built development `sider.exe` SHA-256 was
`956f09e8df169fea64e3dd99338f0544b59de85ea9807c90d9895e8fa6ea0a63`;
the compiled test runner SHA-256 was
`e7df417af365aacad318f35143191360bee6ea7fadb4b2da700ce7aa57a6d897`.
The runner verified both remained unchanged. Their hashes identify bytes but
do not independently attest to a build relationship with the Git tree.
The host was Windows 11 Pro, build 26300, NTFS, Intel Core i5-9300H with
eight logical processors and 34,188,873,728 bytes visible RAM. The fixture
uses seed `27688995`, one and four shards, AOF off/`always`/`everysec`, three
repetitions per combination, a 64 MiB logical quota, 1,024 SETs across eight
clients per repetition, large values, a slow subscriber, expiry, and compaction.
The run was reserved after a five-second process-CPU check found low activity;
Code and Docker Desktop remained open. Continuous host idleness was not proven.

The [raw archive](resource-envelope-108-windows-20261011.zip) contains the
72,843-byte report, 1,076,446-byte observation JSONL, 2,643,398-byte latency
JSONL, and Cargo log. The archive is 220,528 bytes with SHA-256
`c63d40f781f2745e6f24b1de2312df2638e284e3233092cf7771ee8079e9019b`.
The report records the individual file hashes and every scenario. The table
shows medians of each repetition's nearest-rank saturation percentiles and
throughput; RSS, logical bytes, and queues are sampled maxima.

| Shards | AOF | p50/p95/p99 RTT, ms | SET/s | RSS, MiB | Logical, MiB | Worker/AOF queue | Startup median, ms | Recovery median, ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | off | 0.396 / 0.568 / 0.757 | 18,438.6 | 15.77 | 4.05 | 0 / 0 | 36.68 | — |
| 1 | always | 7.945 / 40.156 / 51.692 | 780.3 | 17.66 | 4.57 | 7 / 1 | 42.22 | 256.45 |
| 1 | everysec | 0.843 / 1.060 / 1.164 | 8,899.7 | 16.79 | 4.58 | 4 / 0 | 41.42 | 283.87 |
| 4 | off | 0.374 / 0.657 / 0.885 | 18,320.1 | 15.87 | 4.05 | 1 / 0 | 36.50 | — |
| 4 | always | 4.435 / 17.494 / 55.896 | 927.8 | 17.07 | 4.57 | 4 / 4 | 41.41 | 270.58 |
| 4 | everysec | 0.432 / 0.609 / 0.797 | 16,386.2 | 18.53 | 4.57 | 1 / 0 | 41.28 | 263.52 |

The slow-subscriber eviction occurred in 18/18 scenarios, expiry removals
were observed in 18/18, and compaction completed in all 12 AOF scenarios.
Sampled server CPU increments ranged from 234 to 1,485 milliseconds per
scenario, using Windows process user plus kernel time. Worker queue capacity
was eight per shard; AOF queue capacity was 32. The largest observed worker
depth was seven and AOF depth four; no sampled queue reached its capacity.
The maximum sampled RSS across scenarios was 19,431,424 bytes, while the
largest logical charge was 4,800,452 bytes. Logical quota is separate from
process RSS and does not account for connections, runtime, or temporary buffers.

These observations on one host are descriptive. The 100 ms sampling interval
can miss RSS/queue peaks, and background activity and file-cache state were
not controlled continuously. Startup/recovery timings include helper work and
readiness checks. This does not define a throughput floor, RSS cap, recovery
objective, or cross-platform ranking. The earlier
[Linux WSL2 measurement](resource-envelope-108-wsl2-20261010.json) used the
published 1.0.1 Linux package under a separate binary and host context;
the two tables must be interpreted independently. Operator response to quota,
queue, and subscriber pressure is specified in the
[resource guide](../resource-envelope.md).
