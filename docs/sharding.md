# Shards and routing

`SIDER_SHARDS` sets from 1 to 256 owning workers, defaulting to 1. Configuration is
fixed while running. Each worker owns its map, TTL index, and queue bounded by
`SIDER_WORKER_QUEUE_CAPACITY`; there is no direct concurrent map access.

## Stable distribution

The router extracts content between the first `{` and following `}` when that
content is nonempty. An empty first pair, a missing closing brace, or no opening
brace uses the entire key. Nested openings are tag bytes; `a{{tag}}` uses `{tag`.
There is no UTF-8 conversion.

Over those bytes, 64-bit FNV-1a starts at `0xcbf29ce484222325`; for each byte it
applies XOR and multiplies by `0x100000001b3`, modulo 2^64. The shard is the hash
modulo worker count. Vectors in `storage::routing` fix the result independently of
platform or the randomized hasher used inside each map.

Hash tags colocate `customer:{42}:name` and `customer:{42}:email`. This does not
implement the Redis Cluster protocol or its slots. There is no online resharding.

## Multi-key commands and progress

`DEL`, `EXISTS`, `MGET`, and `MSET` require all keys to be in one shard. The router
checks the whole command before sending and returns
`CROSSSLOT Keys in request don't hash to the same slot` when it finds a crossing.
No worker receives part of a rejected operation. Duplicates, key ordering, and the
last MSET value retain their original semantics. The connection remains usable
after rejection.

Commands with no key use worker zero. Each connection still has one in-flight
request. Queues for separate shards allow independent progress; clients contending
for a hot shard share that worker's queue. A worker failure stops server admission
and starts draining, avoiding service of an incomplete dataset.

## Quota and expiration

The total `SIDER_MAX_DATASET_BYTES` quota is split deterministically: each shard
receives `total / shards`, and the first `total % shards` receive one more byte.
The sum never exceeds the total and each partition needs at least one byte. No quota
is borrowed between workers: a full shard can reject a write despite room in another.
The budget remains logical, distinct from RSS and network buffers.

Each worker maintains passive expiration and cleans up to 64 events per 100 ms
round. Shutdown closes admission for every worker and drains accepted commands
within the global deadline. Cancelling the supervisor aborts asynchronous tasks.
The writer uses blocking operating-system I/O; aborting or exhausting a wait does
not interrupt a `write`/`sync` stuck in the kernel. The deadline bounds API waiting;
terminating a process with blocked I/O depends on the operating system.

## Evidence and durable integration

Tests verify binary vectors and hash tags, rejection before enqueue, a saturated
queue while another worker progresses, partitioned quota, and TCP batches:

```sh
cargo test --locked --lib storage::routing::
cargo test --locked --lib storage::worker::tests::cross_shard
cargo test --locked --lib storage::worker::tests::saturated_shard
cargo test --locked --test tcp shard
```

R04-04 integrates the global AOF writer, sequence, snapshot, and recovery across
shards. Each request keeps shared admission from enqueue through apply/reply. The
snapshot obtains exclusion, waits for accepted requests, and collects workers over
their own channels. Expiration only attempts admission and skips a round during a
snapshot, avoiding blockage of a worker that must respond to collection.

Compaction queues the snapshot in the writer before releasing new mutations; the AOF
captures the delta until publishing the new generation. Recovery validates each
shard's layout, sequence, integrity, and quota before binding. Changing worker
count with existing data requires [offline migration](aof-migration.md). The
`Worker::with_aof` API disables local compaction thresholds when a worker belongs
to a multi-shard set; only global coordination can compact them.

```sh
cargo test --locked --lib storage::snapshot
cargo test --locked --test sharding
cargo test --locked --test persistence
cargo test --locked --test aof_migration
```

## R04-05 exploratory measurement

The release build at SHA `626e1aef8c093b390527719eae49b451ff113edb` ran 16
scenarios and checked final state for 32,768 TCP INCR operations. The rehearsal
used Windows x86_64, an Intel i5-9300H (4 cores/8 threads), four clients, seed
`0x52404005`, 512 operations per client, 1/4 shards, pipeline 1/16, and
concentrated/distributed keys. No builds or other test loads ran concurrently.
[Raw results](evidence/R04-05-626e1ae-windows.json) include compiler, throughput,
batch RTT, percentiles, before/after RSS, and complete configuration.

Without AOF, the rehearsal measured 33,687 to 85,788 operations/s. With `always`
AOF, it measured 665 to 945 operations/s. Four shards showed no consistent gain in
this sample; global-writer sync dominated durable cost. Pipelining increases the
batch observed by the client but retains sync per mutation, without grouping
acknowledgments.

These are exploratory loopback results, without warmup or statistical repetition;
clients share the machine with the server. RSS is sampled, not a peak. RTT is per
batch, not individual latency for pipelined commands. There is no speed comparison
with Redis or performance promise in another environment.

The historical command below belongs to the SHA above. For the 1.0 candidate, use
the [package benchmark gate](benchmarks.md), with warmup, three repetitions, and
RSS observed during measurement.

To reproduce the historical rehearsal in PowerShell, use a clean, separate checkout
of that SHA and a new destination:

```powershell
cargo test --locked --release --test shard_benchmark --no-run
$env:SIDER_SHARD_BENCH_OUTPUT = Join-Path (Get-Location) 'target/R04-05-new.json'
cargo test --locked --release --test shard_benchmark -- --ignored --exact exploratory_shard_benchmark --nocapture
```

Build first and run measurement with the machine free of builds and other loads.
Later milestones retain this evidence linked to the original SHA.
