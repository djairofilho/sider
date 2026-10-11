# Memory search corpus result

Source SHA: `e2475bdd47f6f6fbe33661a090f10fc9827bcdb8`. Binary SHA-256: `ebbc711909fc6a0a135eac9924fb2a64996b9ee6e46f55686723842090c45236`.
Corpus: `sider-memory-corpus/1` (`16ae499aae3ce7a8c1541789d134c1f11dfe523a986fbf8cec0bda1beae74167`); seed `1142026`.
Platform: `Windows-10-10.0.26300-SP0`; CPU: `Intel64 Family 6 Model 158 Stepping 13, GenuineIntel`;
logical CPUs: `8`;
system memory: `34188873728` bytes.
Python: `3.11.9`. Raw inputs and observations: [raw.json](raw.json).

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
| Serial query throughput | 1503.1 queries/s |
| p50 / p95 / p99 round-trip latency | 0.804 / 0.971 / 1.169 ms |
| Concurrent query throughput | 930.9 queries/s |
| Concurrent p50 / p95 / p99 round-trip latency | 12.789 / 15.327 / 16.878 ms |
| Maximum sampled server RSS | 12648448 bytes |
| Dataset logical charge after load | 96554 bytes |
| Maximum sampled worker / AOF queue depth | 1 / 0 |

Exact correctness compares returned IDs and binary64 distance bytes with an
independent Python oracle on each query. Relevance judgments are fixture labels,
not geometric truth; their precision is reported separately. RSS and queues are
periodic samples and can miss peaks. This one machine and toy dataset establish
neither a service latency guarantee nor a general relevance claim. Runs without
RSS or queue samples remain pending for those measurements.
