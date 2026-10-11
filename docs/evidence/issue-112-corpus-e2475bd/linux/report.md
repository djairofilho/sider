# Memory search corpus result

Source SHA: `e2475bdd47f6f6fbe33661a090f10fc9827bcdb8`. Binary SHA-256: `62d07fa605d8ea48159e0716645e3a13ab2de7d1d4ed2ff2489cd272945c4ff6`.
Corpus: `sider-memory-corpus/1` (`16ae499aae3ce7a8c1541789d134c1f11dfe523a986fbf8cec0bda1beae74167`); seed `1142026`.
Platform: `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.35`; CPU: `Intel(R) Core(TM) i5-9300H CPU @ 2.40GHz`;
logical CPUs: `8`;
system memory: `16685158400` bytes.
Python: `3.10.12`. Raw inputs and observations: [raw.json](raw.json).

The run used 256 generated load records, one serial loopback
client, 100 repetitions of 5 query shapes,
one queue observer, one shard, and AOF record 2 with `always` sync.
The concurrent phase used 8 clients for
20 repetitions each.
The first query after ingestion and the first after recovery are recorded separately
from the repeated warm load. Updates, deletion, TTL expiry, and recovery were checked.

| Observation | Value |
| --- | ---: |
| Exact oracle recall on tested nonempty cases | 1.0 |
| Mean judged relevance precision@k on nonempty fixture queries | 0.700 |
| Serial query throughput | 1744.6 queries/s |
| p50 / p95 / p99 round-trip latency | 0.632 / 0.821 / 1.080 ms |
| Concurrent query throughput | 132.3 queries/s |
| Concurrent p50 / p95 / p99 round-trip latency | 91.908 / 111.525 / 120.670 ms |
| Maximum sampled server RSS | 15331328 bytes |
| Dataset logical charge after load | 96554 bytes |
| Maximum sampled worker / AOF queue depth | 0 / 0 |

Exact correctness compares returned IDs and binary64 distance bytes with an
independent Python oracle on each query. Relevance judgments are fixture labels,
not geometric truth; their precision is reported separately. RSS and queues are
periodic samples and can miss peaks. This one machine and toy dataset establish
neither a service latency guarantee nor a general relevance claim. Runs without
RSS or queue samples remain pending for those measurements.
