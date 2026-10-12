# Operator backup and AOF recovery drill

Runbook version 1, for Sider 1.0.1 and [issue #107](https://github.com/djairofilho/sider/issues/107).
Use the published package for the host: Windows 11 x64 MSVC or Ubuntu 24.04
x86_64 GNU. The source SHA of the published v1.0.1 package is
`b738a0b4f97074fe272bc21f1eacf5db93cd020e`.
This exercise measures recovery of a declared dataset under the
[deployment profile](deployment-profile.md), [backup contract](backup.md), and
[AOF contract](persistence.md). It produces retained operator evidence, without
issuing a release receipt or promising a recovery time.

## Inputs and preparation

Run on local storage with enough free space for the original, backup, restored
data, failure copies, and retained AOF tail backups. Reserve at least 3 GiB per attempt.
Stop competing benchmarks, builds, and load tests. The runner starts only its own
loopback instances with ephemeral ports, discovers each PID through readiness,
and terminates only its owned children. It never opens an operator's existing
data directory. Automatic AOF compaction is disabled to make the generation used
for failure injection reproducible.

Use an independently trusted copy of the published `SHA256SUMS` and manifest.
Checksums detect accidental corruption; someone able to replace the archive,
manifest, and checksum list can replace all three consistently. Keep these inputs
unchanged throughout the exercise. The runner checks the manifest and archive
against the list, the archive's size/hash against the schema 2 manifest, its version,
target and full source SHA, and all four extracted executable hashes against the
corresponding archive members. `tar` must be available; Windows includes `tar.exe`.
Archive membership checks do not extract files or follow archive links.

The test runner itself comes from this checkout. Its Git SHA, copied source, and
source hash are retained separately from the distributed package's SHA. Build it
before collecting timings:

```sh
cargo test --locked --test recovery_drill --no-run
```

Create a UTF-8 `context.json` using actual observations:

```json
{
  "os": "Windows 11 Pro, exact build, x64",
  "cpu": "processor model and core count",
  "ram_bytes": 34359738368,
  "storage": "local device model, SSD/HDD, interface and selected volume",
  "filesystem": "NTFS",
  "workload_isolation": "no competing benchmark, build, or load test"
}
```

Replace the sample hardware values with the host's values. Include virtualization,
power configuration, or other relevant conditions as additional fields. Windows
observations can be collected with `Get-CimInstance Win32_OperatingSystem`,
`Get-CimInstance Win32_Processor`, `Get-PhysicalDisk`, and `Get-Volume`. On Ubuntu,
record `/etc/os-release`, `uname -a`, `lscpu`, `free -b`, `lsblk`, and `findmnt`.
Use native Linux filesystems when claiming a native Linux run.

## Run on Windows

Run from the repository root. Select a new evidence directory for every attempt;
failed and incomplete attempts must remain available. Download and extract once,
then set the input paths:

```powershell
$drillInputs = Join-Path $env:TEMP 'sider-recovery-v1.0.1'
New-Item -ItemType Directory -Path $drillInputs | Out-Null
gh release download v1.0.1 --repo djairofilho/sider --dir $drillInputs `
  --pattern SHA256SUMS --pattern release-manifest.json `
  --pattern sider-v1.0.1-x86_64-pc-windows-msvc.zip
$drillArchive = Join-Path $drillInputs 'sider-v1.0.1-x86_64-pc-windows-msvc.zip'
Get-FileHash -Algorithm SHA256 -LiteralPath $drillArchive
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $drillArchive).Hash.ToLowerInvariant() `
  -ne '40c9f8363d2d9524815f240466b44efdaa4dcb857a536218b541eb403fe547e1') {
  throw 'Published archive checksum mismatch'
}
Expand-Archive -LiteralPath $drillArchive -DestinationPath $drillInputs
$env:SIDER_DRILL_PACKAGE_DIR = Join-Path $drillInputs 'sider-v1.0.1-x86_64-pc-windows-msvc'
$env:SIDER_DRILL_ARCHIVE = $drillArchive
$env:SIDER_DRILL_MANIFEST = Join-Path $drillInputs 'release-manifest.json'
$env:SIDER_DRILL_SHA256SUMS = Join-Path $drillInputs 'SHA256SUMS'
$env:SIDER_DRILL_SOURCE_SHA = 'b738a0b4f97074fe272bc21f1eacf5db93cd020e'
$env:SIDER_DRILL_CONTEXT = Join-Path $drillInputs 'context.json'
$env:SIDER_DRILL_EVIDENCE = Join-Path $drillInputs 'windows-attempt-001'
# Windows PowerShell 5 can treat redirected native stderr as an error record.
$drillPreviousErrorAction = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
cargo test --locked --test recovery_drill -- --ignored `
  --exact extracted_package_operator_recovery_drill --nocapture `
  *> (Join-Path $drillInputs 'windows-attempt-001-cargo.log')
$drillExit = $LASTEXITCODE
$ErrorActionPreference = $drillPreviousErrorAction
Get-Content (Join-Path $env:SIDER_DRILL_EVIDENCE 'result.json') -Encoding UTF8
if ($drillExit -ne 0) { throw "Recovery drill failed: exit $drillExit" }
```

The v1.0.1 Windows archive SHA-256 is
`40c9f8363d2d9524815f240466b44efdaa4dcb857a536218b541eb403fe547e1`.
Place the populated context file at the configured path before invoking Cargo.
All `SIDER_` configuration inherited by the harness is removed from child
environments; the server receives only the recorded drill configuration.

## Run on Ubuntu

Download the Linux archive, verify it, and extract it into a new local directory:

```sh
drill_inputs=$(mktemp -d /tmp/sider-recovery-v1.0.1.XXXXXX)
gh release download v1.0.1 --repo djairofilho/sider --dir "$drill_inputs" \
  --pattern SHA256SUMS --pattern release-manifest.json \
  --pattern sider-v1.0.1-x86_64-unknown-linux-gnu.tar.gz
(cd "$drill_inputs" && sha256sum --check SHA256SUMS --ignore-missing) || exit 1
tar -xzf "$drill_inputs/sider-v1.0.1-x86_64-unknown-linux-gnu.tar.gz" -C "$drill_inputs"
export SIDER_DRILL_PACKAGE_DIR="$drill_inputs/sider-v1.0.1-x86_64-unknown-linux-gnu"
export SIDER_DRILL_ARCHIVE="$drill_inputs/sider-v1.0.1-x86_64-unknown-linux-gnu.tar.gz"
export SIDER_DRILL_MANIFEST="$drill_inputs/release-manifest.json"
export SIDER_DRILL_SHA256SUMS="$drill_inputs/SHA256SUMS"
export SIDER_DRILL_SOURCE_SHA=b738a0b4f97074fe272bc21f1eacf5db93cd020e
export SIDER_DRILL_CONTEXT="$drill_inputs/context.json"
export SIDER_DRILL_EVIDENCE="$drill_inputs/ubuntu-attempt-001"
cargo test --locked --test recovery_drill -- --ignored \
  --exact extracted_package_operator_recovery_drill --nocapture \
  > "$drill_inputs/ubuntu-attempt-001-cargo.log" 2>&1
drill_exit=$?
cat "$SIDER_DRILL_EVIDENCE/result.json"
test "$drill_exit" -eq 0
```

Create the observed Ubuntu context before invoking Cargo. The Linux archive hash
for v1.0.1 is
`acc9520e120d5960c286a184d8e0079774398ad4479610cd9e6a540f5274243d`.
A Windows execution cannot supply the Ubuntu result.

For WSL2, run the Linux package and Rust harness inside Ubuntu, with the checkout,
package, and AOF directories on its ext4 filesystem. Record the Microsoft kernel,
virtualization, visible memory, and virtual disk explicitly. An execution under
WSL2 is a Linux functional result with a virtualized storage path; it does not
establish behavior on a native Ubuntu host. Use `/var/tmp` for retained evidence
if the distribution cleans `/tmp` when it starts. When `gh` is unavailable inside
Ubuntu, download the Linux archive, manifest, and checksum list in Windows and
copy them into the Linux filesystem; check them again before extraction.

## Dataset and expected comparison

Each layout runs sequentially with one or four shards, routing version 1,
64 MiB total logical quota, AOF `always`, and default protocol/backup limits.
The large string fixture uses seed `107`: 128 keys each contain 520,000 bytes,
for 66,560,000 payload bytes (63.48 MiB). Candidate keys `bulk:0`, `bulk:1`,
and so on are selected by the recorded routing algorithm until every shard has
its share: 128 strings on one shard or 32 per shard with four shards. Key and
metadata charges add to the payload. The CLI verification report records actual
logical usage and checks that the dataset lies between 63 and 64 MiB and each
shard remains within its quota.

The byte at position `i` of selected key number `k` is
`(i * 31 + k * 17 + 107) % 256`. The fixture also includes strings, a hash,
a list, a set, and a sorted set with NUL, non-UTF-8, CRLF, and empty payloads.
One same-tag `MULTI`/`EXEC` creates all five types and the long TTL value.
Binary key/field/member bytes are compared explicitly. Ordered collections keep
their order; the one-member set avoids relying on unordered response order.

A one-hour TTL must stay live after restoration, AOF restart, and tail repair.
The runner brackets each `PTTL` query with `SystemTime` samples and derives an
absolute deadline interval by adding the returned TTL to both ends. The source
interval and each recovered interval must overlap or differ by at most 250 ms;
restarting a relative TTL fails this check. Queries longer than 250 ms, a backwards
civil sample, or a civil/monotonic elapsed difference above 250 ms during one
query invalidate the run. Raw samples and the interval gap are recorded before
assertion. Recovered TTL is sampled immediately after readiness/PING, when replay
has converted its persisted absolute deadline to a runtime monotonic expiry.
The full data read then checks that the long value remains live. Civil clock
adjustments during that longer read cannot be assumed to advance at the same
rate as `Instant`; phase durations continue to use `Instant`. A five-second
TTL is live before capture and counted in the exported snapshot; restoration waits
until that original deadline has elapsed and must observe absence. TTL is never
restarted. Concurrent same-tag transactions increment two counters together while
the backup exports. Progress must increase between invocation and completion.
The restored pair must be equal and lie within the observed capture bounds;
the source AOF restart must recover both counters at the final acknowledged count.
Post-cut writes may exist in the source and remain absent from the backup.

Every large value and every type is checked against the independent deterministic
fixture, using the bounded RESP2 oracle in `tests/common/wire.rs`. A SHA-256 digest
over length-prefixed large-key bytes and exact response bytes is compared between
the source, isolated restoration, normal AOF restart, and repaired-tail copy.
PTTL values and concurrent counters are checked separately because they vary with
time and the capture cut. `dataset.json` records seed, keys, formula, TTLs and quota.

## Phases and failure cases

`phases.jsonl` uses a monotonic `Instant` origin with start/end nanoseconds and
elapsed milliseconds. It records identity, load, export, verification, restore,
readiness/PING, data comparison, and AOF recovery. `verify_to_data_validated`
measures from starting verification through restore, readiness, and comparison;
`restore_to_data_validated` begins at restoration. Readiness timings include
starting the selected binary, waiting for its exact PID/loopback readiness file,
and receiving literal `PONG`. Recovery-to-comparison timings include the full
dataset read and validation, so they are not server startup times alone.
The documented Cargo command uses a debug test harness and release package
executables; fixture generation, RESP parsing, hashing, and file copying add
harness work. Keep the profile when comparing repeated measurements.
The runner does not flush the operating system's file cache; preceding export,
comparison, and checksum reads can warm it. These are not cold-cache timings.
Wall-clock values label run metadata and the TTL query brackets; they do not
measure phase duration.

| Case | Required outcome |
| --- | --- |
| Backup export under transactions | Valid consistent capture; writes progress during export; pair is never half of EXEC |
| Verified isolated restore | New data directory starts, digest matches, expired TTL stays absent |
| Forced primary process termination | AOF restart reaches readiness and retains every acknowledged fixture/counter |
| Incomplete post-seal AOF tail on a stopped copy | Startup repairs the tail, saves the complete damaged generation in `tail-*.bak`, and matches data |
| Snapshot byte corruption on a copy | Verify and restore fail; requested new destination is absent |
| Wrong backup shard layout | Verify and restore fail; requested new destination is absent |
| Existing restore destination | Restore fails and existing generation hashes remain unchanged |
| AOF checksum corruption, unknown version, or wrong shard layout on copies | Startup exits unsuccessfully without readiness; generation hashes stay unchanged |

Failure injection always uses a separate stopped copy. Valid backup and primary
generation hashes are checked after the failure cases. Normal restart is allowed
to establish its new primary epoch; hashes before and after that restart are
recorded separately. Failure rejection must never fall back silently to an older
generation. A process kill is not a power loss or storage device failure test,
and Windows does not establish Linux directory synchronization guarantees.

## Retain and interpret evidence

The output directory contains the input manifest/checksum list, exact package and
runner identity, host context, phase journal, dataset definition, per-process
configuration/PID/readiness, CLI commands/exit codes, raw stdout/stderr, original
AOF directories, verified backup, restored AOF, failure copies, and tail backups.
Preserve the outer Cargo log with it. Copy the evidence to durable storage before
temporary-directory cleanup and keep its inventory/hash outside the directory.

Accept a completed run only if Cargo exits zero, `result.json` parses with
`status: "success"`, both shard layouts have complete results, and the recorded
files and comparisons can be inspected. A missing, malformed, `running`, or
`failed` result never counts as success. Panic paths record failure; forced harness
termination can leave `running`. Every new attempt uses a new destination.

The optional replica profile needs a separate two-instance drill following
[replication](replication.md): record upstream/applied positions and epoch, fence
the primary, accept any unapplied suffix loss explicitly, promote the replica,
and verify durable role and data after restart. This runbook's single-host backup
and AOF exercise leaves replica promotion pending and does not establish an
asynchronous loss bound or a failover recovery objective.

Focused harness contracts can run without downloaded packages:

```sh
cargo test --locked --test recovery_drill
```

The ignored package test is not executed by this command and cannot count as
package evidence. Supported platforms, optional replica promotion, power failure,
and device failure remain separate rows in the operator's evidence record.

## Historical v1.0.1 Windows exercises

The [Windows summary](evidence/R107-v1.0.1-windows.json) and
[raw archive](evidence/R107-v1.0.1-windows-raw.zip) retain two completed attempts
on Windows 11 Pro 10.0.26300, Intel Core i5-9300H (4 cores/8 threads),
34,188,873,728 bytes visible RAM, and an SSDG3NVME1T-W NVMe SSD with NTFS.
Both used the published package and a debug harness without a competing workload.
The first attempt used the earlier reduced-PTTL check. Attempt 2 establishes
the earlier full monotonic elapsed-time TTL anchor; it does not execute the later
absolute-deadline bracket oracle described in this version of the runbook.
Its copied runner SHA-256 is
`cb561e15b6545ef4aafc89b5e68c46f5d9327cb22b8c2a3698e981df6a98231c`;
the harness checkout was `0f0f232f56fd42b88a56e61954f2bc892458f04d`.

Attempt 2 passed the complete package test in 462.45 seconds. Each shard layout
had 66,578,821 logical bytes. The timings below are individual observations,
including the full data oracle where indicated, and are not an RTO guarantee.

| Phase, milliseconds | 1 shard | 4 shards |
| --- | ---: | ---: |
| Backup verify | 2,276.43 | 2,305.77 |
| Isolated restore | 5,994.27 | 6,347.65 |
| Verify through restored data validation | 18,240.07 | 19,450.26 |
| Forced AOF restart through readiness/PING | 1,830.15 | 1,670.47 |
| Forced AOF restart through data validation | 10,236.45 | 10,698.56 |
| Incomplete-tail repair through data validation | 17,993.30 | 18,340.46 |

The raw ZIP is 310,555 bytes and has SHA-256
`c3f22d2b608e105e3de7ba1276370218e7055a5a4d02deb74ba286c52cea2077`.
It includes 299 raw logs, metadata, command records, phase journals, and runner
sources from both attempts. All archived members were checked against the JSON
inventory after creation. The 60 large AOF/backup/tail files are excluded from
Git; their hashes and temporary retention paths are in the JSON. They remain
under `%TEMP%\sider-recovery-107-v1.0.1\windows-attempt-1` and `windows-attempt-2`.
Move those large files to durable storage before temporary cleanup if full replay
of the retained bytes is required.

## Final v1.0.1 Windows exercise

The [final Windows summary](evidence/R107-v1.0.1-windows-final.json) and
[final raw archive](evidence/R107-v1.0.1-windows-final-raw.zip) retain attempt 3.
It passed the complete package exercise in 448.84 seconds with the same final
runner used by the successful Linux attempt:
`9a6cf3d1a619dcd95bc7d1eadd5e8b77e3118b59f2c37e112e9ceeb3a9d81ca6`.
The checkout and verified package are the same as the historical Windows runs.
All six absolute deadline checks had interval gaps of 0 or 1 ms, within the
250 ms tolerance, and every query passed the clock validity check. Both layouts
verified 66,578,821 logical bytes and the expected fixture digests; all corruption,
incompatibility, destination preservation, and incomplete-tail cases passed.
The final native contracts passed 11 tests with one ignored package test, and
focused Clippy completed without warnings.

The recorded Windows host used the Balanced power plan and had no competing
benchmark, build, or load test. File caches were not flushed. The following
individual observations are neither cold-cache timings nor an RTO guarantee.

| Observed phase, milliseconds | 1 shard | 4 shards |
| --- | ---: | ---: |
| Backup verify | 2,189.11 | 2,231.78 |
| Isolated restore | 5,770.65 | 4,746.99 |
| Verify through restored data validation | 18,246.35 | 16,568.71 |
| Forced AOF restart through readiness/PING | 782.38 | 1,083.31 |
| Forced AOF restart through data validation | 9,337.74 | 9,410.71 |
| Incomplete-tail repair through data validation | 16,592.22 | 16,908.82 |

The final ZIP is 165,064 bytes, SHA-256
`136b0a311f3ecde11067d6a85e3cb864e40c2391d3615a119673ce4be894e470`.
Its 157 logs, metadata, command records, phase journal, and source files were
checked against the JSON inventory after creation. The 30 large AOF, backup,
and tail files remain outside Git under
`%TEMP%\sider-recovery-107-v1.0.1\windows-attempt-3`; their sizes and hashes are
in the summary. Preserve them before temporary cleanup. All three Windows
attempts remain available, with their different runner versions identified.
The embedded per-run `linux: "pending"` field describes this Windows run alone;
the separate Ubuntu WSL2 evidence below records the actual Linux execution.

## WSL2 clock diagnosis

Linux attempts 1 and 2 failed the earlier oracle that compared recovered `PTTL`
against a long interval of `Instant`. They remain failed evidence. The persisted
long-TTL deadline was identical in the backup, primary AOF, and restored AOF;
the diagnostic attempt recorded these observations:

| Observation, milliseconds | Value |
| --- | ---: |
| Source PTTL | 3,599,989 |
| Restored PTTL after the complete data read | 3,552,978 |
| Elapsed `Instant` | 46,315 |
| Earlier oracle's delta | -696 |
| Civil anchor query bracket | 1,791,613,350,453 to 1,791,613,350,453 |
| Civil restored query bracket | 1,791,613,398,120 to 1,791,613,398,121 |

The civil interval advanced 1,352–1,353 ms more than `Instant`. An independent
20 ms clock sampler recorded 1,352.251 ms net offset change across 61.526 seconds,
including an adjacent-sample adjustment of 482.305 ms. The absolute-deadline
oracle now samples immediately after readiness, before the long data read,
and records query brackets and validity before asserting. Its 250 ms tolerance
is unchanged. Phase duration remains monotonic; a civil jump during an individual
query invalidates that observation and the run. The published package and its
TTL values were not changed.

## Recorded v1.0.1 Ubuntu WSL2 exercise

The [Ubuntu WSL2 summary](evidence/R107-v1.0.1-linux-wsl2.json) and
[combined raw archive](evidence/R107-v1.0.1-linux-wsl2-raw.zip) preserve all three
Linux attempts. The [initial failure archive](evidence/R107-v1.0.1-linux-wsl2-attempt-1-raw.zip)
was retained separately before diagnosis. Attempts 1 and 2 exited Cargo with 101
and `status: "failed"`. Attempt 3 passed the complete functional exercise in
807.65 seconds with the final absolute deadline bracket oracle.

The final runner SHA-256 is
`9a6cf3d1a619dcd95bc7d1eadd5e8b77e3118b59f2c37e112e9ceeb3a9d81ca6`,
using checkout `0f0f232f56fd42b88a56e61954f2bc892458f04d`. The Linux package ran
inside Ubuntu 24.04.4 LTS x86_64 GNU, kernel
`6.6.87.2-microsoft-standard-WSL2`, on `/dev/sdg` ext4. The guest exposed eight
logical processors of the Intel Core i5-9300H and 16,685,158,400 bytes RAM.
Both layouts verified 66,578,819 logical bytes and the expected fixture digests.
All six restored/restarted/repaired deadline intervals matched the corresponding
source interval with a 0 ms gap; all queries passed the clock validity check.
The final native contracts passed 11 tests with one ignored package test, and
focused Clippy completed without warnings.

An automatic Ubuntu `unattended-upgrade` process, PID 1634, started 357.82 seconds
after the test process. It was observed using approximately 102% of one CPU core,
and remained present through an archived sample approximately 675 seconds into the run.
It was absent at the first post-run observation. The raw activity samples and
`/proc` start ticks bound the observed overlap; they do not establish its exact
exit instant. No timers or services were changed. Accept this as functional
Linux evidence under observed contention. These Linux timings do not establish
an idle-host baseline and must not be compared with Windows timings.

| Observed phase under the recorded conditions, milliseconds | 1 shard | 4 shards |
| --- | ---: | ---: |
| Backup verify | 4,775.12 | 2,998.47 |
| Isolated restore | 7,283.00 | 6,076.92 |
| Verify through restored data validation | 28,382.47 | 28,004.23 |
| Forced AOF restart through readiness/PING | 1,055.34 | 1,306.43 |
| Forced AOF restart through data validation | 14,321.07 | 18,178.38 |
| Incomplete-tail repair through data validation | 29,382.12 | 29,545.01 |

The combined ZIP is 1,220,531 bytes, SHA-256
`ff9556d3667541c8bcf79e3f2964966188c272aa8c618caa407fdb8544e50c93`.
Its 257 archived logs, metadata, sources, and clock/activity observations were
checked against the JSON inventory after creation. The 40 large AOF, backup,
and tail files remain outside Git under
`/var/tmp/sider-recovery-107-v1.0.1/linux-wsl2-attempt-{1,2,3}` in the Ubuntu
distribution. Their sizes and SHA-256 hashes are versioned in the summary;
copy them to durable storage before removing that directory or distribution.
A non-WSL Ubuntu VM exercise is recorded below. Bare-metal host behavior, power
loss, device failure, and optional replica promotion remain pending.

## Published v1.0.1 Ubuntu GitHub-hosted VM exercise

The [2026-10-12 result](evidence/R107-v1.0.1-ubuntu-actions-38208988384.md) and
[raw evidence](evidence/R107-v1.0.1-ubuntu-actions-38208988384-raw.zip) record a
successful manual drill of the exact published Linux package on a GitHub-hosted
Ubuntu 24.04.5 x86_64 Azure VM with an ext4 test filesystem. The run covered one
and four shards, backup verification and restore, AOF restart and incomplete-tail
recovery, full data and TTL checks, and negative corruption cases. Its measured
restore and recovery phases completed within 9 seconds under the recorded VM
conditions. The linked summary identifies the package and runner SHAs, checksums,
host context, commands, raw observations, and limitations. This result does not
validate a later candidate package or physical power loss.
