# Operational testing of the extracted package

The opt-in `tests/operational_package.rs::operational_extracted_package` test
checks diagnostics and limits through the public interfaces of the distributed
executables. It receives an extracted directory, runs short cases, and writes
JSON with actual observations. It does not create a release receipt or replace
verification of the package manifest and hashes.

## Input and execution

The directory must contain the four executables from the same build:
`sider`, `sider-aof-migrate`, `sider-backup`, and `sider-replica`, with the
`.exe` suffix on Windows. All must be regular files. The runner calculates
SHA-256, verifies each CLI's help text, and confirms the version reported by
`sider` and `sider-backup`. The migration and replication CLIs do not provide
`--version` in this interface. The same hashes are checked at the end.

First verify the package according to the [release guide](releases.md). Set
`SIDER_OPERATIONAL_PACKAGE_DIR` to the absolute path of the extracted directory
and `SIDER_OPERATIONAL_OUTPUT_DIR` to an absolute path that does not yet exist.
The runner version must match the binary version.

On Linux:

```sh
SIDER_OPERATIONAL_PACKAGE_DIR=/path/extracted-package \
SIDER_OPERATIONAL_OUTPUT_DIR=/path/new-evidence \
cargo test --locked --test operational_package -- --ignored --exact operational_extracted_package --nocapture
```

In PowerShell:

```powershell
$env:SIDER_OPERATIONAL_PACKAGE_DIR = 'C:/path/extracted-package'
$env:SIDER_OPERATIONAL_OUTPUT_DIR = 'C:/path/new-evidence'
cargo test --locked --test operational_package -- --ignored --exact operational_extracted_package --nocapture
```

The test uses only the executables in that directory. It does not consult
`CARGO_BIN_EXE_sider` to choose the server. Child environments are cleared of
inherited `SIDER_*` options and receive only each scenario's configuration.
The changes do not affect the test process's global environment.

## Observed cases

| Case | Required condition and outcome |
| --- | --- |
| Configuration and diagnostics | With the RESP and internal ports occupied, `--diagnose` succeeds without creating an AOF or readiness file. An invalid configuration fails and identifies the option without repeating the received sensitive value. |
| Quota | With four shards and a total 4 KiB quota, a large SET is rejected with OOM. GET retains the previous value and INFO retains usage/key information. DEL frees consumption; a new SET and PING work. |
| Connections | Four slots are confirmed with PING. The excess connection closes and increments `rejected_connections`; an admitted connection can still query INFO. After a slot is released, another connection executes PING. |
| Slow client | Two subscribers receive binary publications. One does not drain its socket; the other confirms each frame in order. The bounded queue removes the slow client without blocking the healthy one, and PING continues to work. After closing the subscribers, subscriptions reach zero. |
| AOF opening and recovery | A regular file obstructs the AOF parent directory. Startup fails, preserves that file, and does not publish readiness. After removing only the obstruction created by the test, the server confirms a write with `always` AOF. A second instance using the same AOF is rejected; the first keeps serving. After collecting the first instance, reopening recovers exactly the binary value and confirmed sequence. |

The slow-client load is limited to 512 messages of 64 KiB and a queue of one
message per subscriber. Confirmation by the healthy reader orders each
publication; no fixed delay is used as proof of processing. Metric convergence,
network operations, and processes have bounded deadlines.

The obstruction tests a **path-opening error**. It does not simulate ENOSPC,
permission removal during append, a write failure on an already open file, or
atomicity under a short write. Those paths remain in the native injection suites
and persistence tests. Do not interpret this rehearsal as evidence of a
full-disk failure.

The baseline test covers migration, backup/restore, types, TTL, replicas, and
cooperative shutdown where supported. Those scenarios are not repeated here.
This runner collects only PIDs it owns; the AOF is reopened after a confirmed
write under the `always` policy. This collection does not demonstrate
cooperative shutdown on Windows.

## Evidence and limits

Each completed case is written to `observed-cases.jsonl`. The final
`operational-report.json` collects the platform, version, duration, hashes of
the four CLIs, exit codes, diagnostics, and observed counts. It is produced
only when every case passes and the executables remain unchanged. A failure
preserves previous cases and data directories for inspection without reporting
success.

Output requires a new directory. Data and evidence are preserved; the test does
not remove an existing data source. The only file removals performed by the load
target the obstruction created by the scenario itself. The process helper removes
its own readiness file and temporary files when it collects the child.

To check the build without running rehearsal processes:

```sh
cargo clippy --locked --test operational_package -- -D warnings
```

A run using development binaries is useful for debugging the runner. The
implementation passed in that mode on Windows, with all five cases, including
rejection of an excess connection, removal of the slow subscriber, and the AOF
path-opening error. This is not distribution evidence. Operational distribution
evidence requires rerunning the test with packages actually extracted on Windows
and Linux after verifying their identity. An ignored entry in the native suite
does not constitute approval.
