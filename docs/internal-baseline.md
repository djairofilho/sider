# Internal R10 baseline and migration to 1.0

The R10 baseline is a private set frozen by SHA and hashes, with package `0.1.0`,
real data, and backups. Freezing occurs after runner integration, in a clean checkout,
before changing the version to `1.0.0`. It creates no tag, release, or publication
receipt. Prepare one set per supported platform: Windows MSVC x86_64 and Linux GNU x86_64.

The selected source is `0021d875dde9da6cbbe9b5b84cd640681128e6ea`. Run the freeze in
a clean worktree at that SHA, which still identifies itself as `0.1.0`.
The candidate checkout identifies itself as `1.0.0` and consumes the frozen set
during migration. Hashes come from the files actually produced and are kept with
the evidence; they are not inferred from the source SHA.

## Preparing the package and recording the build

At the SHA to be frozen, run `cargo build --locked --release --bins` and package
all four executables with README, license, and notices, according to the
[package contract](packages.md). Keep actual build output, exit code, and observed
hashes. Compile the test runner in the normal profile; do not overwrite the four
`target/release` files during collection.

Immediately after building and packaging, produce a UTF-8 JSON sidecar.
This contract also applies to the candidate package, with its own version and SHA:

| Field | Required value |
| --- | --- |
| `schema_version` | `1` |
| `source_sha` | Full SHA, 40 lowercase hexadecimal characters, from the clean checkout |
| `target` | `x86_64-pc-windows-msvc` or `x86_64-unknown-linux-gnu` |
| `version` | `0.1.0` at the source; candidate version during migration |
| `compiler` | Complete stdout from `rustc --version --verbose`, including final LF |
| `command` | `cargo` |
| `args` | `["build","--locked","--release","--bins"]` |
| `exit_code` | `0`, observed during the build |
| `source_clean_before`, `source_clean_after` | `true`, observed before and after the build |
| `binaries` | Four `{ "path", "bytes", "sha256" }` objects, sorted by `path` |
| `archive` | `{ "bytes", "sha256" }` object for the produced ZIP or tar.gz |

Names in `binaries` are `sider-aof-migrate`, `sider-backup`, `sider-replica`,
and `sider`, with `.exe` on Windows. Each hash has 64 lowercase hexadecimal
characters and each size is measured in bytes. `args` also accepts
`"--target", "TARGET"` at the end when the actual build used that argument.
There are no extra fields.

The sidecar is an operator observation record. It links declarations to checked
files; it does not authenticate who produced the build. Preserve logs and the
external hash of `baseline.json` outside the frozen directory.

## Freezing without reopening original data

Set absolute paths. The output directory must be new and have an existing parent:

| Variable | Input |
| --- | --- |
| `SIDER_BASELINE_PACKAGE` | ZIP or tar.gz archive actually produced |
| `SIDER_BASELINE_BUILD_DIR` | Directory with the four original build executables |
| `SIDER_BASELINE_PROVENANCE` | Build sidecar described above |
| `SIDER_INTERNAL_BASELINE_DIR` | New directory for the frozen set |
| `SIDER_BASELINE_LONG_TTL_MS` | Optional: defaults to 604800000 ms, seven days |

Long TTL accepts one to 365 days. Choose a deadline that will still be live during
candidate validation. Short TTL is 60 seconds and must still be live when process
shutdown completes.

```sh
cargo test --locked --test persistence -- --ignored --exact freeze_internal_baseline --nocapture
```

The runner checks the checkout, toolchain, sidecar, package, and hashes of the
four original and extracted binaries. Extraction limits names, inventory, and bytes,
writes each member to a new file, and does not materialize archive links.
Every server and CLI used in the test comes from that extracted package.

Scenarios use one and four shards, routing version 1, a total 4 MiB quota,
AOF records up to 65536 bytes, `always`, and automatic compaction disabled.
Each shard receives strings, hashes, lists, sets, and sorted sets with binary bytes.
EXEC contains a WRONGTYPE operation and confirms the subsequent batch write.
Data, order, members, and extreme-score representation are checked.

After these comparisons, the runner seeds short TTL, exports the backup, checks
absolute expiration times, and stops the process. On Linux, it sends SIGTERM to
the child and requires successful exit and readiness file removal. On Windows,
it explicitly records forced child termination after backup flush under the
`always` policy; this does not represent cooperative console-signal shutdown.

The backup is verified and restored into another directory, opened by another
process from the R10 package. The stopped original data is not reopened by the server.
The manifest is saved only after these checks and a fresh verification of checkout
and build file identity.

Success JSON reports the path, source SHA, and `manifest_sha256`.
Store this hash externally. The set contains:

- `baseline.json`, schema 1, task `R10`, identity, and complete inventory;
- `build-provenance.json`, the package archive, and its four extracted executables;
- `datasets/shards-1` and `datasets/shards-4`, closed at the source;
- `backups/shards-1` and `backups/shards-4`, produced by the actual CLI.

Each scenario records configuration, tags by shard, response digest, Unix expiration
times, AOF format, role, epoch, sequence, and shutdown method/time.
Extra, missing, or changed files, links, and nonportable paths fail validation.
An interrupted run may leave a partial directory; it is not a completed baseline
without a valid manifest and external hash.

## Upgrading through copies and checking the candidate

The gate uses the same-platform baseline, with short TTL already expired and long
TTL still live. Preserve the original set. Set:

| Variable | Input |
| --- | --- |
| `SIDER_INTERNAL_BASELINE_DIR` | Frozen R10 set |
| `SIDER_INTERNAL_BASELINE_SHA256` | External hash of `baseline.json` |
| `SIDER_MIGRATION_PACKAGE` | Actual packaged candidate archive |
| `SIDER_MIGRATION_BUILD_DIR` | Four original candidate build executables |
| `SIDER_MIGRATION_PROVENANCE` | Sidecar for this candidate build |
| `SIDER_MIGRATION_OUTPUT_DIR` | New directory outside the baseline for the test |

With the [candidate build](releases.md) `GateContext(migration)` context:

```sh
cargo test --locked --test persistence -- --ignored --exact release_migration_gate --nocapture
```

The gate directly calls the existing initial-format, unknown-version, and typed
migration cases, plus both R10 scenarios. It publishes a receipt only after
complete success and publication context validation.

For each layout, the runner opens a data copy with the new executable and compares
all five types, EXEC effects, absolute long TTL, and the absence of short TTL that
expired during downtime. It starts an empty replica of the same new version,
checks snapshot, delta, and write rejection. It also produces and restores a
candidate backup. Separate copies with corruption or incompatible shard counts
must be rejected without readiness or file changes.

Restoring the old backup uses the **frozen 0.1.0 CLI** to create a new directory.
Only then does the **candidate server** open that directory.
`sider-backup` requires the same version as the backup manifest; do not use the new
CLI directly on the old backup. This rule preserves the tool's contract.
To return to the source, restore another copy with the old CLI and server, keep
the same layout, and check data before redirecting clients.
Writes after the backup point are not part of this recovery.

At completion, the runner checks the entire frozen inventory again and records
candidate package and provenance hashes. It neither tests nor promises replication
across different versions. These cases use real packages and processes, but do
not replace the other [operational tests](metrics.md) or the soak gate.

## Rehearsing the runner before the official freeze

With the `0.1.0` package, build, and provenance from a clean checkout, set the three
inputs `SIDER_BASELINE_PACKAGE`, `SIDER_BASELINE_BUILD_DIR`, and `SIDER_BASELINE_PROVENANCE`:

```sh
cargo test --locked --test persistence -- --ignored --exact rehearse_internal_baseline_migration --nocapture
```

This rehearsal creates temporary data with a short TTL of 30 seconds and a long
TTL of one hour, waits for actual expiration, and migrates to the same package version.
The report identifies `same_version_short_rehearsal_not_frozen_baseline`.
It issues no receipt, saves no official baseline, and does not prove an upgrade
to 1.0. Evidence for 0.1.0 to 1.0 comes from the later candidate gate execution.

Native inventory and provenance tests use files explicitly declared as artificial
fixtures; they validate contract rejection and are never counted as evidence
of running real packages.
