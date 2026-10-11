# Staged offline memory migration

This development slice of issue #110 adds an explicit offline
upgrade to header 4/record 2. It is not a published Sider 1.0.1 capability or
completion of the memory lifecycle. The CLI without record-version flags and
the existing `migration::migrate_offline` retain their original record-1 path.
Explicit CLI record-version flags now select the memory migration API. No
environment variable, server profile option or implicit upgrade was added.

## Explicit CLI upgrade

The source must be stopped and its layout specified. Both record flags default
to 1; the tool never guesses them from current values or headers. For a legacy
header-1/2/3 directory:

```text
sider-aof-migrate --source ./stopped-legacy --source-shards 1 --source-routing 1 --destination ./new-memory --shards 1 --routing 1 --record-version 2
```

For a header-4/5 memory directory, including an explicit rehash to four shards:

```text
sider-aof-migrate --source ./stopped-memory --source-shards 1 --source-routing 1 --destination ./rehash-memory --shards 4 --routing 1 --source-record-version 2 --record-version 2
```

In PowerShell, use `& .\target\release\sider-aof-migrate.exe` with these arguments.
Version flags accept only 1 or 2. Repeated/missing/invalid flags reject the request.
Source record 2 with destination record 1, including its default, rejects before
source I/O. A wrong source profile refuses the actual header without repair.
Omitting both flags retains the original migration behavior and exact report.

Memory JSON retains the original sequence/layout/entry/usage/tail fields and adds
`target_format`, `source_record_version`, `target_record_version`, both routing
versions, `source_generation`, `source_sha256`, `target_sha256`,
`retained_memory_collections` and `retained_memory_members`. Hashes and counts have
the scope described below. A successful migration does not start a listener.
The default server still refuses the new directory; it requires a proved memory
reader selected explicitly. Public memory server activation remains a separate gate.

## Profile selection

| Source configuration and actual header | Destination configuration | Result |
| --- | --- | --- |
| Default profile, header 1/2/3, record 1 | Explicit memory profile | Sealed header 4, record 2 |
| Explicit memory profile, header 4/5, record 2 | Explicit memory profile | Sealed header 4, record 2 |
| Any source | Default profile | Refused by the memory migration API before source I/O |
| A mismatched source profile or unsupported header | Explicit memory profile | Refused without repair or output |

The new API is `migration::migrate_memory_offline(MigrationOptions, Arc<dyn Clock>)`.
Both layouts are explicit in `MigrationOptions`. Copy the source layout into the
destination configuration to preserve it; setting a different destination shard
count explicitly requests rehashing of complete keys. Memory member IDs do not
become independent shard keys. Header 6/7 and codec downgrade are unsupported.

```rust
use std::sync::Arc;
use sider::persistence::{AofConfig, migration::{MigrationOptions, migrate_memory_offline}};
use sider::storage::{StoreConfig, SystemClock};

let source = AofConfig::new("/data/stopped-legacy".into());
let mut destination = AofConfig::new("/data/new-memory".into()).with_memory_profile();
destination.layout = source.layout;
let report = migrate_memory_offline(
    MigrationOptions {
        source,
        destination,
        source_store: StoreConfig::default(),
        destination_store: StoreConfig::default(),
    },
    Arc::new(SystemClock),
)?;
```

The server and source writer must be stopped. The source must have its existing
`writer.lock`. The source lock remains held until destination verification and
publication finish. The destination must not exist; its parent must exist and
it must be outside the source directory. Existing destinations are preserved.

## Preservation and validation

Migration reads the selected source generation without repair. Valid complete
history is replayed in sequence, including guarded member deltas. An incomplete
tail after the seal is reported but never truncated. Invalid complete history,
unsupported versions, layout mismatches and source quota failures reject output.
Source generations, installation residues and other files are not cleaned up.

One frozen clock pair governs recovery and destination quota validation. All
retained images preserve exact binary keys/values, vector bits, metadata types,
provenance and absolute key/member deadlines. Expired legacy values are omitted
by existing recovery semantics. Memory state remains physically retained and
charged, including elapsed members, elapsed outer deadlines and empty schemas.
This choice uses the contract's permission to retain final expired state; it
does not restart relative TTL or prune historical deltas. Primary cleanup can
later remove expired memory state through its ordinary durable path.

Before creating the destination, migration validates each destination shard's
quota and every complete record against the destination record limit. It shares
immutable source values and materializes one encoded image at a time. This is
not an RSS cap or a complete lifecycle scratch-allocation proof.

The output is a sealed header-4 snapshot at the last complete source sequence.
It deliberately clears source role/epoch. The generated file is synced, reopened,
checked against every expected image and seal, and checked for exact EOF before
the generation rename. Publication synchronizes the destination and its parent.
These operations retain the existing [platform assumptions](persistence.md#platform-guarantees).

## Report and failures

`MemoryMigrationReport.migration` contains source recovery metadata, both layouts,
sequence, entries, incomplete tail bytes and destination per-shard usage. The
additional fields record source/destination record versions, destination header
version, retained memory collection/member counts and SHA-256 hashes of the
selected source generation and generated destination. Source hashing includes
any incomplete tail. It is repeated before publication to detect a changed
selected generation. The path comes directly from the generation held by recovery,
without independently selecting a generation from filenames. The report does not
attest to a source executable version
or hash every old generation/residue in the directory.

`migrate_memory_offline_with_faults` accepts the existing `FaultInjector`. Partial
writes, sync boundaries and failures before/after generation publication remove
only names owned by this operation. No recursive deletion is used. A successful
operation publishes one complete generation; an injected returned failure leaves
no destination owned by the operation. Forced termination may leave an incomplete
directory and requires separate child-process/power-loss evidence.

Default record-1 readers reject header 4 before scanning records or repairing a
tail, including a truncated first record. Keep a verified legacy backup for
rollback: it restores the old cut and loses later destination writes. A newer
directory cannot be downgraded simply because it currently contains no memory.

## Verification and remaining gates

Validation commands:

```text
cargo test --locked --test agent_memory_migration
cargo test --locked --test aof_migration --test agent_memory_aof --test agent_memory_backup
cargo clippy --locked --all-targets -- -D warnings
cargo doc --locked --no-deps
cargo build --locked --release
cargo xtask check
```

### Preceding library receipts

At baseline `ac1757f` with the preceding library slice, the final Windows
migration suite passed 9/9 in 2.32 seconds, including recovery's actual generation
path/hash case. The affected regression run passed legacy migration 14/14,
memory AOF 10/10 and memory backup 9/9. Final all-target Clippy passed with
`-D warnings` in 14.41 seconds, API documentation passed in 13.38 seconds, and
the optimized release build passed in 1 minute 31 seconds. Formatting completed
and the diff/text checks passed. No source changes followed final formatting.
The complete `cargo xtask check` also passed on the committed source, including
formatting, all-target Clippy, debug binary, non-ignored tests and doctest.
The focused receipt used the integrated dependency cache through `--target-dir` under
an exclusive Cargo slot; the main source checkout was not modified. These are
implementation receipts, not full issue #110 or external release approval.

### CLI validation

The subsequent CLI slice at baseline `b025718` passed its Windows normal process
suite: 9 passed and 1 external gate ignored in the normal selection. That count
includes five CLI behavior cases and four shared process-helper cases; the final
rerun took 0.85 seconds. Native memory migration passed 9/9 in 2.33 seconds and
legacy migration passed 14/14 in 0.60 seconds. All-target Clippy with `-D warnings`
passed in 18.40 seconds, API documentation passed in 8.58 seconds and the optimized
release build passed in 54.83 seconds. The integrated full check remains pending
with the integrator. No functional test-first
RED/GREEN history is claimed for the CLI implementation.

The preceding receipts do not validate the new parser, dispatch or process tests.
Run the process suite serially to keep one child process active at a time:

```text
cargo test --locked --test agent_memory_migration_cli -- --test-threads=1
cargo test --locked --test aof_migration --test agent_memory_migration -- --test-threads=1
```

The separate Windows compatibility gate requires the original published v1.0.1
package under `target/legacy-memory-cli/package/sider-v1.0.1-x86_64-pc-windows-msvc`.
It verifies pinned server/migrator hashes, runs both old executables against the
intact migrated file and every first-record prefix, checks exact version refusal
and source files, and fails if artifacts are absent. It never rebuilds the
old executables or treats an empty selection as success. The gate separately
characterizes the historical server residue cleanup described below:

```text
cargo test --locked --test agent_memory_migration_cli -- --ignored --exact published_old_binaries_refuse_all_first_record_prefixes_preserving_aof_and_characterizing_residue_cleanup --nocapture --test-threads=1
```

The [published package](https://github.com/djairofilho/sider/releases/tag/v1.0.1)
was built at `b738a0b4f97074fe272bc21f1eacf5db93cd020e`. Its Windows ZIP SHA-256 is
`40c9f8363d2d9524815f240466b44efdaa4dcb857a536218b541eb403fe547e1`.
Acquire it with `gh release download v1.0.1 --repo djairofilho/sider --pattern
sider-v1.0.1-x86_64-pc-windows-msvc.zip --dir target/legacy-memory-cli`, verify
that hash, then extract into the `package` directory above. Downloading and
verifying artifacts does not establish that the compatibility gate passed.

The separately selected Windows published-binary gate passed 1/1 in 12.68 seconds:
287 complete-header/first-record-prefix/intact-file variants, each checked by the
original server and migrator (574 historical subprocesses). Both refused header 4
with `unsupported AOF version: 4`; neither changed any generation bytes or lock.
The old migrator preserved every file, including a known installation residue.

The first gate run failed a real full-directory preservation assertion in
1.06 seconds at the complete 36-byte header: v1.0.1 server startup deleted
`replication-install-preserved.tmp` before refusing the version. Its published
`writer::load` performs this cleanup before `read_header_with_layout`. The
characterization gate now asserts that exact historical deletion for every
variant and requires every generation, lock and unrelated sentinel unchanged.
This is evidence of version refusal and AOF preservation, not blanket directory
preservation by the old server. The current migration CLI preserves all source
files on its success and refusal paths; its normal process suite verifies this.

The migration target covers legacy headers 1/2/3, memory headers 4/5, mixed binary
types, guarded history and absolute deadlines, rehashing, source preservation
and locking, quota/record refusal, failures around publication and old-reader
refusal for every first-record prefix. Linux frozen old-binary subprocesses and
filesystem checks, power-loss tests and transport-2
replication/live backup remain separate issue #110 gates.
