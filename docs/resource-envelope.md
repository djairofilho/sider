# Resource-envelope workload

This opt-in workload prepares evidence for [issue #108](https://github.com/djairofilho/sider/issues/108).
It records observations from a fixed loopback fixture; it does not establish a
supported deployment profile, capacity limit, performance commitment, or resource
guarantee. The supported profile and operator actions remain tied to the separate
deployment contract in [deployment-profile.md](deployment-profile.md).

## What it runs

[`tests/fixtures/resource-envelope.json`](../tests/fixtures/resource-envelope.json)
is the versioned workload input. It fixes seed `27688995`, an LCG64 byte generator,
three repetitions with no warm-up operations, one and four shards, and AOF disabled,
`always`, and `everysec`. The slow-client payload has its own declared seed
`5311518`. Each process also
uses a 64 MiB logical dataset quota, worker queue capacity 8, and Pub/Sub queue
capacity 8. Worker queue capacity is configured per shard; `INFO` reports the
sum of queue capacities and occupancy across shards. Pub/Sub queue capacity
applies to the single slow subscriber. This is an intentionally declared test
configuration, not the profile selected for deployment.

Each repetition runs these phases in order:

| Phase | Declared work | Recorded result |
| --- | --- | --- |
| Saturation | 8 clients, 128 concurrent `SET`s each, sharing 256 deterministic keys with 128-byte values | Per-command RTT samples, throughput, INFO queues, logical bytes, RSS, and CPU ticks |
| Large values | 16 values of 256 KiB, each written and read back | SET and GET RTT samples and exact value verification |
| Slow client | One subscribed client does not read; 256 publications of 64 KiB | Publication duration and change in `pubsub_evictions_total` |
| Expiration | 128 keys with a 1,000 ms TTL | SET RTT and whether expiration removals were observed before the declared timeout |
| Compaction | AOF scenarios: 256 writes of 2 KiB with automatic compaction at 256 KiB; skipped when AOF is off | Write RTT and whether a completed compaction was observed within 30 seconds |
| Recovery | On AOF runs, wait until written and synced sequences agree, terminate that child, restart on the same directory, and read a confirmed sentinel and 256 KiB value | Harness restart-to-readiness timing and verification of both recovered values |

The AOF compaction threshold of 262,144 bytes is positive; `AofConfig::validate`
accepts it. The compaction phase's 256 x 2 KiB writes fit below the default
16 MiB maximum delta size. The 256 KiB values fit below the default 64 MiB
maximum AOF record size. Each recorded sample identifies the scenario and phase.
RTT percentiles are
nearest-rank p50/p95/p99 over individual command round trips, in nanoseconds.
Throughput is the operation count divided by the wall time of that phase. It is
descriptive for this loopback client and includes client-side encoding, socket I/O,
response reading, scheduling, and the configured server behavior.

## Resource observations

The sampler requests `INFO all` at 100 ms intervals and records the complete field
map together with process RSS and CPU ticks. `dataset_logical_bytes` is the Store's
logical accounting, including its per-entry charge. It is not RSS and excludes
runtime, allocator, socket buffers, queues, and temporary structures. Queue INFO
fields count requests, not bytes. CPU is recorded as the process's user-plus-system
ticks from `/proc/PID/stat`; the report records clock ticks per second. Sampling
adds INFO traffic, can miss short peaks, and is not a global snapshot across fields
or shards. Divide a tick delta by the reported clock ticks per second to obtain
process CPU seconds; divide that by elapsed seconds for average core utilization.

Startup durations include the test helper's `--version` preflight and readiness
PING. Recovery timing starts before the helper's version preflight and ends at
readiness; the harness only counts it after subsequent sentinel and 256 KiB value
reads succeed. These are harness timings, not server-only startup or recovery
service-level objectives. The runner supports Linux and Windows 11; measurements
retain the platform-specific semantics below.

The runner records the source commit SHA and Git tree SHA separately from the
binary version and SHA-256. For a local change set, stage all intended source
inputs before building and running; the harness records the index tree and
requires the worktree to match it. The binary hash identifies executable bytes
and detects changes during the run; it does not prove those bytes were built from
that source tree. Binary build provenance is therefore marked unverified. The runner creates a
new evidence directory and never overwrites one. It writes
incremental JSONL observations and RTT samples. It writes
`resource-envelope-report.json` only when all declared repetitions complete. If a
phase fails or times out, the raw files remain for diagnosis and no report claims a
complete run. A complete report means only that this declared runner completed; it
does not mean a particular RSS limit, queue bound, or throughput threshold passed.
Observation samples stop with an explicit error if they exceed the fixed 256 MiB
file budget. Both JSONL files begin with a run header that identifies the source
commit and staged tree, binary hash, platform, machine, and fixture; the report
records file hashes and sizes.

## Execution

Build the test binary before reserving the host, then run the ignored workload on
an otherwise idle Linux or Windows 11 machine. The binary must be an absolute path to the Sider
executable matching this checkout's Cargo version. The runner records its SHA-256,
the source commit and staged tree identities, platform, machine details, fixture,
and raw samples.
This development runner does not authenticate a release package or publish a gate
receipt.

```sh
cargo test --locked --test resource_envelope --no-run
```

After other builds and workloads finish:

```sh
SIDER_RESOURCE_IDLE_MACHINE=1 \
SIDER_RESOURCE_BINARY=/absolute/path/to/sider \
SIDER_RESOURCE_OUTPUT_DIR=/absolute/path/to/new-evidence-directory \
cargo test --locked --test resource_envelope -- --ignored --exact resource_envelope_workload --nocapture
```

The runner rejects a missing idle-machine declaration, relative paths, unstaged
or untracked source changes, and an existing output directory. Keep raw files
and the report together with the exact binary and host context when interpreting
results. Do not compare these results with the separate Sider-versus-Redis
benchmark in [benchmarks.md](benchmarks.md).

## Windows observations and schema compatibility

The same fixture and scenario checks run on Windows 11. A target-specific
`sysinfo = 0.39.6` development dependency supplies safe Rust process and machine
APIs; it is not linked into the server's production dependencies. The sampler
creates a fresh system snapshot for the requested child PID at each observation.
[`Process::memory`](https://docs.rs/sysinfo/0.39.6/sysinfo/struct.Process.html#method.memory)
returns current resident bytes. The
[Windows implementation](https://github.com/GuillaumeGomez/sysinfo/blob/v0.39.6/src/windows/process.rs)
uses `WorkingSetSize`; this includes resident shared pages and is neither private
commit nor a process lifetime peak. Linux continues to record `VmRSS` in bytes.

Windows records `process_cpu_milliseconds`, the process lifetime user plus kernel
CPU time. The implementation converts Windows FILETIME's 100 ns units to whole
CPU milliseconds. Its difference is `process_cpu_milliseconds_delta`; divide by
1,000 for CPU seconds. Windows `process_cpu_ticks` and its delta are null.
Linux keeps both tick fields and its `getconf CLK_TCK` conversion. Neither
platform records the library's CPU percentage estimate. CPU observations cover
sample times, not the entire process lifetime or all recovery work.

The library discards native CPU query errors. The runner rejects decreasing CPU
counters (including a positive value followed by zero), absent children, zero
working sets, and a Windows process whose final CPU observation is zero. Fresh
snapshots prevent a failed memory refresh from carrying forward an old working
set. These checks cannot distinguish every native CPU failure from genuine zero
CPU, so the report records that limitation. Windows has no Linux load-average
equivalent: both load-average fields are null, with an explicit explanation.
The idle-machine declaration remains required and does not establish continuous
host monitoring. Record filesystem, competing processes, and host observations
separately when preserving a Windows run.

New reports and JSONL headers use schema 2. They add platform units, compiled
harness and embedded fixture SHA-256, and a scenario label on each observation.
Linux CPU fields retain their original meaning; Windows adds separate CPU fields.
Portable streaming SHA-256 replaces the external `sha256sum` command, retaining
its hash-and-path output shape. The harness checks executable hashes and source
identity again before publishing a report. Hashes identify bytes; neither the
server nor compiled harness hash proves a build relationship to the staged tree.
The retained WSL2 report and raw archive remain schema 1 and keep their original
identities and observations.

On Windows, set the same three environment variables in PowerShell before
invoking the ignored workload:

```powershell
$env:SIDER_RESOURCE_IDLE_MACHINE = '1'
$env:SIDER_RESOURCE_BINARY = 'C:\absolute\path\to\sider.exe'
$env:SIDER_RESOURCE_OUTPUT_DIR = 'C:\absolute\path\to\new-evidence-directory'
cargo test --locked --test resource_envelope -- --ignored --exact resource_envelope_workload --nocapture
```

Build the runner before reserving the host. The output directory must be new and
outside the checkout, so writing evidence does not change the source status.
Windows focused tests passed 16 tests with the workload ignored; Clippy, formatting,
`cargo doc --locked --no-deps`, and `cargo build --locked --release` passed.
A native workload run remains pending. On 2026-10-10, preflight found substantial
background CPU activity: over a five-second wait plus snapshot overhead, the
preexisting Docker Desktop backend consumed 28.45 CPU seconds. The formatted WMI
processor provider returned 100% busy in all five samples; its raw processor-time
field did not advance, so those provider values are retained as suspect rather
than a reliable utilization estimate. Independent process CPU differences
established real contention. The idle declaration was not set, and no workload
or successful native report was produced. Docker Desktop and editor processes
were left running. Retry the preflight on an actually idle host before running
the fixture; do not infer idle time from an empty container list.

The [Windows preflight summary](evidence/resource-envelope-108-windows-preflight-20261010.json)
and [raw archive](evidence/resource-envelope-108-windows-preflight-20261010.zip)
preserve the blocked preflight, native check results, host observations, package
verification, and exact harness inputs. This is preparation evidence and does
not establish a measured Windows envelope.

## Verification status

The integrated main worktree passed
`cargo test --locked --test resource_envelope -- --nocapture` (13 passed, 1 ignored),
`cargo clippy --locked --test resource_envelope -- -D warnings`, and the earlier
`cargo xtask check`. The Ubuntu harness compiled with
`cargo test --locked --test resource_envelope --no-run`; the ignored workload then
completed all 18 declared scenarios in 81.99 seconds on 2026-10-10.

The run used Ubuntu 24.04.4 under WSL2, kernel
`6.6.87.2-microsoft-standard-WSL2`, with `/` on ext4, 8 logical CPUs (Intel Core
i5-9300H, 4 cores), and about 15 GiB memory. Immediately before the run,
`vmstat` showed 96% idle in its initial interval and 100% in the next four; no
Docker containers were running. The runner recorded load averages
`0.89 / 1.28 / 1.05` before the workload and `1.64 / 1.43 / 1.12` after it. WSL
system services, including the Docker daemon, remained present, and the host was
not sampled continuously during the workload. These observations do not rule out
all host-level contention.

The workload used the Sider 1.0.1 Linux x86_64 package binary. Its executable
SHA-256 was
`ae5b426e6f7ca3c47c18beb2fe4926c1401d7d53c9ecc6253cb61573449fee6a`; the package
archive SHA-256 was
`acc9520e120d5960c286a184d8e0079774398ad4479610cd9e6a540f5274243d`. The package
manifest records source commit
`b738a0b4f97074fe272bc21f1eacf5db93cd020e`. The measured harness inputs were a
staged local tree based on commit `0f0f232f56fd42b88a56e61954f2bc892458f04d`,
with tree SHA-1 `1b221ce7eec93d112cb896b98d1839f14cd5f9ff`; the worktree matched
that staged tree. The report keeps binary provenance marked unverified because
the harness does not validate a signed build attestation or prove that the
executable was built from the recorded harness tree.

The table reports the median of each repetition's nearest-rank saturation
percentiles and throughput. It describes 1,024 concurrent `SET` operations per
repetition. RSS and logical bytes are the largest sampled values across the
three repetitions; queue values are peak request counts. Times are milliseconds,
throughput is operations per second, and memory is MiB.

| Shards | AOF policy | p50 / p95 / p99 | Throughput | Peak RSS | Peak logical bytes | Peak worker / AOF queue | Startup median | Recovery median |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | off | 0.150 / 0.542 / 7.920 | 15,486 | 9.72 | 4.05 | 0 / 0 | 23.73 ms | — |
| 1 | always | 38.838 / 77.378 / 93.880 | 180 | 11.15 | 4.57 | 7 / 1 | 43.38 ms | 104.23 ms |
| 1 | everysec | 0.506 / 0.724 / 10.723 | 6,813 | 10.95 | 4.57 | 6 / 1 | 43.18 ms | 103.89 ms |
| 4 | off | 0.162 / 0.536 / 3.200 | 17,335 | 9.96 | 4.05 | 0 / 0 | 46.83 ms | — |
| 4 | always | 24.593 / 117.131 / 155.635 | 181 | 11.73 | 4.57 | 4 / 4 | 33.62 ms | 93.65 ms |
| 4 | everysec | 0.259 / 1.421 / 13.470 | 7,351 | 11.30 | 4.58 | 1 / 1 | 33.67 ms | 93.85 ms |

Across the run, the expiration phase observed removals in 18 of 18 scenarios,
the slow-subscriber phase observed an eviction in 18 of 18, and automatic
compaction completed in all 12 AOF scenarios. The maximum observed worker queue
was 7 requests; its reported capacity was 8 per shard (32 summed across four
shards). The maximum AOF queue was 4 requests against its capacity of 32. Neither
queue reached its configured capacity in this workload. Logical bytes peaked at
4.58 MiB while the fixture quota was 64 MiB; this is Store accounting and does
not estimate RSS. Sampled RSS peaked at 11.73 MiB. Server CPU ticks were sampled
at 100 ticks per second; scenario summaries ranged from 0 to 0.98 sampled CPU
seconds, with coarse granularity for short runs.

These are observations from one controlled WSL2 host and three repetitions, not
capacity thresholds, operator limits, production predictions, or durability
guarantees. The run does not support a numeric RSS headroom, throughput floor,
or queue limit. Operators should set the logical quota independently from a
host-memory budget, monitor `dataset_logical_bytes` and process RSS separately,
and watch worker/AOF queue occupancy alongside their capacities. Select AOF
policy according to the separate deployment contract and validate its startup,
recovery, and compaction behavior on the intended host. The ignored runner remains
separate from registered release gates.

When a logical-quota rejection occurs, remove data or raise the configured quota
only after measuring RSS headroom on that host. If worker or AOF queues stay near
their configured capacities, pace producers or reduce concurrent requests and
investigate slow storage or clients before increasing queue capacity; larger queues
also consume memory. Repair or disconnect persistently slow subscribers after
observing eviction counters. Retest on the intended hardware and AOF policy before
assigning a numeric admission threshold or recovery objective.

The [evidence summary](evidence/resource-envelope-108-wsl2-20261010.json) and
[raw archive](evidence/resource-envelope-108-wsl2-20261010.zip) retain the report,
both JSONL files, host observations, logs, and exact measured harness inputs in
the checkout. The original files also remain under
`/var/tmp/sider-resource-envelope-108/evidence-attempt-1`. The report SHA-256 is
`3deefcfaf8d0220acf099c41c651f3ddd7659768cbabe5917f5484435ec2c0a0`; the
observation and latency file hashes are recorded in the report and summary.
The complete report is marked `measurement_only`.
