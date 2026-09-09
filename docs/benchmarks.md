# Candidate package benchmarks

The `benchmarks` gate runs the Linux GNU x86_64 binary extracted from the
candidate package and produces reproducible loopback measurements. It does not
establish superiority over Redis or a throughput or latency commitment.

The workload starts only when the operator sets `SIDER_BENCH_IDLE_MACHINE=1`.
Compile first and reserve the machine for the test. The variable records that
declaration; the runner cannot prove that no other processes are running.
Do not run builds, tests, soak tests, or other workloads concurrently.

## Input identity

The [input helper](../tests/common/release_input.rs) can be shared with other gates
that run the package. It requires a clean checkout at the declared SHA, Cargo and
artifact version `1.0.0`, a native target, and the private publication policy
from the [release contract](releases.md).

Before and after the test, the helper checks:

- Schema 2 manifest identity: version, SHA, target, and checkout provenance.
- tar.gz name, size, and SHA-256 against the manifest.
- Full equality between the extracted executable and its unique corresponding tar.gz
  member, with a size limit and rejection of ambiguous member names.
- Extracted README and license equality with checkout files.
- Executable SHA-256 and size, preserved through the end of measurement.

The comparison reads the member with [GNU tar --to-stdout](https://www.gnu.org/software/tar/manual/html_node/Writing-to-Standard-Output.html);
it does not extract other members during the gate. GNU tar and `sha256sum` must be
available. The input directory must remain under operator control.
Hashes verify identity and integrity without authenticating file authorship.

The manifest may be preliminary, without all receipts yet. The gate does not require
final approval in advance, since its own receipt will contribute to that approval.
The selected package and its bytes must already be fixed. After all gates, the
bundle is frozen; the candidate and final release reuse the same files.

## Matrix and methodology

The [runner](../tests/shard_benchmark.rs) executes 16 scenarios in fixed order:

| Dimension | Values |
| --- | --- |
| Shards | 1 and 4 |
| Keys | One shared key or 256 keys per client |
| Pipeline | 1 and 16 commands per batch |
| Persistence | Disabled or AOF `always`, with automatic compaction disabled |

Each scenario has three repetitions, each with a fresh process and dataset.
Four connections execute 128 warmup operations per client, followed by
2,048 measured operations per client. Measurement begins only after all clients
complete warmup. The receipt counts **393,216 measured operations**;
the 24,576 warmup operations are recorded separately.

The generator uses seed `0x52404005 XOR client_id`, LCG multiplier
`6364136223846793005`, increment `1`, and modular 64-bit arithmetic.
The measured phase continues the warmup sequence. Requests are precomputed;
each INCR response must be a positive integer. At completion, GET checks every
key's count, including both warmup and measurement.

Throughput divides measured operations by the interval from the first client
starting to the last client finishing. Latency is the **RTT of the entire batch**:
from writing the batch to reading all its responses. It includes client work
to configure deadlines, write, and decode responses. Pipeline 16 does not divide
this interval by 16 to invent individual latencies.
The p50/p95/p99 percentiles use nearest rank on actual RTT samples.

A thread reads `/proc/PID/status` during measurement, at a requested 1 ms interval,
and converts VmRSS from KiB to bytes. The raw file preserves observed timestamps;
system scheduling may lengthen the interval. Only samples within the measured
window count. Missing samples fail the gate.
The observed maximum does not guarantee the actual peak memory usage.

The three rates per scenario produce minimum, median, maximum, mean, population
standard deviation, and coefficient of variation. These are descriptive statistics;
there is no confidence interval, automatic outlier removal, superiority threshold,
or Redis speed comparison. The gate receives the pinned Redis image only as
part of the shared context, without starting Redis.

## Manual execution

Use the frozen 1.0 publication checkout with packages already prepared.
Internal milestones do not satisfy this gate's contract. The evidence directory
must contain the tar.gz and preliminary manifest, with no previous benchmark
receipt or raw sample file.

Set `SIDER_RELEASE_VERSION=1.0.0`, `SIDER_RELEASE_SHA`,
`SIDER_RELEASE_TARGET=x86_64-unknown-linux-gnu`, `SIDER_REFERENCE_IMAGE` to the
exact value in `releases/plan.json`, and `SIDER_RELEASE_DIR` to the absolute
evidence path. `SIDER_PACKAGE_DIR` points to the absolute extracted directory
containing `sider`, `README.md`, and `LICENSE`.

Compile before reserving the machine:

```sh
cargo test --locked --release --test shard_benchmark --no-run
```

When other workloads have finished, run:

```sh
SIDER_BENCH_IDLE_MACHINE=1 cargo test --locked --release --test shard_benchmark -- --ignored --exact release_benchmarks_gate --nocapture
```

Execution uses the already compiled Cargo target. Changes requiring recompilation
need fresh preparation before reservation. The server executable always comes
from `SIDER_PACKAGE_DIR`; the runner does not use `CARGO_BIN_EXE_sider` as input.

The separate `benchmarks-samples.json` result contains hardware, system, toolchain,
load average before/after, effective configuration for each repetition, seeds,
and all ordered RTT and RSS samples. Configuration diagnostics are collected before
starting the server; readiness and the ephemeral port added by the helper are
recorded as runtime data. This file has a 64 MiB limit. The receipt records the
raw file's hash and size, package/binary hashes, per-repetition summaries, and
variation by scenario.

Timeouts, invalid responses, mismatched final state, observation failures, or
changed input prevent receipt publication. An interrupted run is not approved.
Preserve incomplete material in another directory for diagnostics; do not
overwrite approved evidence to repeat the gate.

## Verification during implementation

```sh
cargo test --locked --test shard_benchmark benchmark_contract -- --nocapture
cargo test --locked --test shard_benchmark release_input -- --nocapture
cargo clippy --locked --test shard_benchmark -- -D warnings
```

These tests check the matrix, counts, percentile calculation, dispersion,
RSS units, and input identity rejection. They compile the runner without
starting measurement. The implementation was validated this way on Windows;
the full Linux package test remains reserved for the candidate build on an idle machine.

The [R04-05 exploratory measurement](sharding.md#r04-05-exploratory-measurement)
remains historical evidence for its recorded SHA. It used a different volume,
had no warmup/repetitions, and does not satisfy this gate.
