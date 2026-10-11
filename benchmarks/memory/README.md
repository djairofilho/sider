# Memory search corpus v1

This development-checkout workload supports issue #112. The versioned
[corpus](corpus-v1.json) contains two-dimensional F32LE embeddings, typed
metadata, two namespaces, binary member IDs, a 150 ms TTL case, and explicit
expected eligible IDs. `relevant_hex` is a small human-assigned toy judgment,
separate from geometric nearest-neighbor correctness. The deterministic seed
generates additional bounded load records. It does not call an embedding model.

Build `sider` from a clean checkout, then run from the repository root with
Python 3.10 or newer. Use an output directory outside the checkout. On Windows:

```powershell
cargo build --locked --bin sider
python -B benchmarks/memory/run.py --binary target/debug/sider.exe --output-dir "$env:TEMP/sider-memory-corpus-result" --load-records 256 --repetitions 100
```

On Linux:

```sh
cargo build --locked --bin sider
python3 -B benchmarks/memory/run.py --binary target/debug/sider --output-dir /tmp/sider-memory-corpus-result --load-records 256 --repetitions 100
```

The runner requires a clean Git checkout and records its SHA, binary and corpus
SHA-256 hashes, Python and platform version, CPU model, logical CPU count,
system memory, seed, input records, exact result IDs and distance bytes, raw
latency samples, RSS samples, queue samples, and selected `INFO` counters. It
writes `raw.json` and a Markdown `report.md`; a failed or incomplete run leaves
no success report. The output directory is created by the runner. Each run
creates a fresh temporary AOF directory and loopback server, then removes it.

The first quality queries run after ingestion and TTL expiry. The runner
updates one vector, deletes another record, checks exact results, inserts
generated load records, then repeats five query shapes through one serial
client. It restarts the server and checks both collections after AOF recovery.
The exact oracle independently filters records, computes squared Euclidean
distance from F32LE values in Python binary64, and sorts by distance and
unsigned binary ID. Each measured reply must match the oracle, including the
distance bytes. The declared eligible set is checked before the first query.

p50/p95/p99 use nearest rank on per-command loopback round-trip times;
throughput divides completed serial queries by the measured interval. A
second connection samples `INFO` worker and AOF queue depth, and the process
RSS is sampled from `/proc/PID/status` or Windows process counters. Sampling
may miss peaks. The load oracle is prepared before timing; serial throughput
includes client framing and response comparison. This toy corpus and one
machine cannot justify a production performance or relevance claim. These are
source-checkout observations, not candidate-package release receipts.
