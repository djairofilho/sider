# Issue #112: memory search corpus and load evidence

Validation date: 2026-10-11. Source commit:
`e2475bdd47f6f6fbe33661a090f10fc9827bcdb8` (PR #142 merge).
Both runs used a clean checkout, corpus `sider-memory-corpus/1`, corpus SHA-256
`16ae499aae3ce7a8c1541789d134c1f11dfe523a986fbf8cec0bda1beae74167`,
seed `1142026`, Python 3.10+ and the exact [runner](../../../benchmarks/memory/run.py).
The [versioned corpus](../../../benchmarks/memory/corpus-v1.json) and each raw
file contain the inputs needed to repeat the run.

| Environment | Binary SHA-256 | Raw and generated report |
| --- | --- | --- |
| Windows NT 10.0.26300, NTFS, Intel Family 6 Model 158 Stepping 13, 8 logical CPUs, 34,188,873,728 bytes system RAM | `ebbc711909fc6a0a135eac9924fb2a64996b9ee6e46f55686723842090c45236` | [raw JSON](windows/raw.json), [report](windows/report.md) |
| Ubuntu 22.04 on WSL2, Linux 6.6.87.2, ext4, Intel i5-9300H, 8 logical CPUs, 16,685,158,400 bytes guest RAM | `62d07fa605d8ea48159e0716645e3a13ab2de7d1d4ed2ff2489cd272945c4ff6` | [raw JSON](linux/raw.json), [report](linux/report.md) |

Both runs used an isolated loopback process, one shard, AOF record 2 with
`always` sync, 256 generated load records, five query shapes, 100 serial
repetitions and 20 repetitions on each of eight concurrent clients. The first
queries after ingestion and AOF recovery, update, deletion, TTL expiry, broad
and selective filters, namespace exclusion, and binary-ID ties matched an
independent exact oracle. Every measured query matched the expected ID order
and binary64 distance bytes. The nonempty tested cases have exact recall 1.0.
Human-assigned toy relevance precision@k averaged 0.700, a separate judgment
from geometric correctness.

| Observation | Windows/NTFS | Linux WSL2/ext4 |
| --- | ---: | ---: |
| Serial queries / concurrent queries | 500 / 800 | 500 / 800 |
| Serial throughput, queries/s | 1,503.1 | 1,744.6 |
| Serial p50/p95/p99 RTT, ms | 0.804 / 0.971 / 1.169 | 0.632 / 0.821 / 1.080 |
| Concurrent throughput, queries/s | 930.9 | 132.3 |
| Concurrent p50/p95/p99 RTT, ms | 12.789 / 15.327 / 16.878 | 91.908 / 111.525 / 120.670 |
| Maximum sampled server RSS, bytes | 12,648,448 | 15,331,328 |
| Dataset logical charge, bytes | 96,554 | 96,554 |
| Maximum sampled worker/AOF queue depth | 1 / 0 | 0 / 0 |

The raw results retain 500 serial and 800 concurrent latency samples per
platform, 76 Windows and 342 Linux RSS/queue samples, selected `INFO` counters,
all generated records, configuration, hardware, and query replies. The Linux
concurrent result differs substantially from Windows on this WSL2 host; the
recorded data do not isolate its cause. Queue and RSS samples may miss peaks.
An independent Linux host run remains pending. These are development source
results, not extracted-package release receipts or throughput/latency promises.
The small toy relevance labels do not establish application relevance. A later
approximate-index decision uses this exact baseline under [issue #114](https://github.com/djairofilho/sider/issues/114).
