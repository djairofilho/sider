# Reproducible Sider versus Redis comparison

Issue #97 adds an independent descriptive comparison in
`tests/redis_benchmark.rs`. It does not publish a release gate receipt or change
the candidate approval contract. No live comparison results are recorded yet.

Both executables run natively on the same Linux GNU x86_64 host and OS image,
using the same Rust client, loopback transport, precomputed requests, dataset,
four connections, seed, and pipeline sizes. Redis is not measured through a
Docker port mapping. The Redis executable must match `/usr/local/bin/redis-server`
from the exact image digest in `releases/plan.json`; the harness checks image
platform, digest, executable hash, version, and native dynamic linkage. Docker
creates an unstarted container solely to verify bytes, then removes it. Missing
native libraries are a failure, not a fallback to another Redis build.

## Workloads and observations

The matrix covers Sider shards 1/4, one shared hot key/256 keys per client,
pipeline sizes 1/16, and persistence off/AOF always: 16 scenarios, three fresh
process and dataset repetitions per server. Redis standalone executes commands
on one thread; four Sider shards have no exact Redis standalone equivalent.
These rows describe that architectural difference and cannot establish equal
execution parallelism. Both servers use the identical single-key INCR trace.
Values are decimal counters; this benchmark does not represent large payloads
or mixed command families. Keys and all final values are verified.

The seed is `0x52404005 XOR client_id`; the LCG uses multiplier
`6364136223846793005`, increment 1, and wrapping u64 arithmetic. Every connection
executes 128 warmup operations before a synchronized measured phase of 2,048
operations. The sequence continues after warmup. All requests are generated
before timing. Server order alternates within each scenario by repetition.

Throughput covers earliest client start to latest finish. RTT samples represent
whole batches, including write/decode/deadline client work. Nearest rank p50,
p95, and p99 are not divided by pipeline depth. Every repetition is retained;
minimum/median/maximum/mean, population standard deviation, and coefficient of
variation are descriptive. There is no hypothesis test, outlier removal,
superiority threshold, or performance commitment.

CPU reads Linux `/proc/PID/stat` user/system ticks before releasing clients and
after joining them, with `getconf CLK_TCK` conversion. This observation window
includes synchronization outside the narrower throughput window. Raw before and
after counters and elapsed window are retained. RSS reads VmRSS at a requested
1 ms interval, retaining only samples inside the throughput window. Observed
maximum RSS is not a guaranteed peak. Client and sampler consume the same host.

Startup times launch through literal PING readiness, including Sider's version
check. AOF rows additionally force termination, restart with the same dedicated
dataset, measure replay-to-readiness, and verify every acknowledged counter.
This measures process restart/replay, not power loss durability. Sider's custom
AOF, logical quota, shard workers, and Redis's AOF format differ. Redis snapshot
saves and AOF rewrites are disabled; Sider automatic compaction is disabled.
Both use AOF always, with no replication, eviction, authentication, or TLS.
Persistence uses dedicated directories on the same temporary filesystem;
identify its backing device and mount options in operator evidence. Native
runtime libraries are from the same host, rather than the image's original
userland, and their resolved paths are recorded for Redis.

## Reproduction

Prepare an exact clean candidate checkout, its frozen Linux package and
preliminary manifest, and the extracted executable as described in
[package benchmarks](benchmarks.md). Set the same `SIDER_RELEASE_*`,
`SIDER_REFERENCE_IMAGE`, and `SIDER_PACKAGE_DIR` input variables. The existing
input helper rejects an altered checkout, package, or executable. This comparison
uses that identity validation but writes no `benchmarks` gate receipt.

Pull the manifest's pinned image explicitly before reserving the machine. Copy
its `/usr/local/bin/redis-server` into an operator-controlled absolute path;
keep the executable mode and confirm its native libraries are available. Set
`SIDER_BENCH_REDIS_BINARY` to that path. Set `SIDER_COMPARE_DIR` to an absolute,
nonexistent output directory outside the checkout.

Compile before the machine reservation:

```sh
cargo test --locked --release --test redis_benchmark --no-run
```

When builds, tests, load tests, and other workloads have stopped:

```sh
SIDER_BENCH_IDLE_MACHINE=1 cargo test --locked --release --test redis_benchmark -- --ignored --exact redis_performance_comparison --nocapture
```

The idle declaration is operator evidence, not proof of machine isolation.
Do not permit Cargo to rebuild during measurement. Record OS image identity,
CPU governor/affinity, storage topology and mount options, and other host settings
with the run. Hardware, kernel, OS release, toolchain, load averages, exact package
and Redis identities, effective configurations, seeds, raw RTT/RSS samples, CPU
counters, startup and replay timings are recorded by the harness.

The output contains incremental per-repetition JSON, `samples.json`,
`comparison.json`, and a side-by-side `report.md`. Only `comparison.json` with
`status=complete` is a successful comparison receipt; `status.json` records the
initial incomplete attempt and remains diagnostic material. Existing evidence
is never overwritten. Missing tools, timeouts, protocol errors, final state or
recovery differences, missing RSS, changed bytes, and incomplete matrix prevent
the success receipt. Retain failed directories and console logs. Repeat using a
new directory, preserving outliers and failures.

Focused implementation validation (no live benchmark):

```sh
cargo test --locked --test redis_benchmark benchmark_contract
cargo clippy --locked --test redis_benchmark -- -D warnings
```

These checks validate the harness, not performance. The comparison remains
pending until the Linux run supplies exact candidate evidence and real rows.
