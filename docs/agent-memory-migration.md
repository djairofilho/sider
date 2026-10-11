# Staged offline memory migration

This library-only development slice of issue #110 adds an explicit offline
upgrade to header 4/record 2. It is not a published Sider 1.0.1 capability or
completion of the memory lifecycle. The existing `sider-aof-migrate` CLI and
`migration::migrate_offline` still accept only their original record-1 profile.
No environment variable, server option or implicit upgrade was added.

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

At baseline `ac1757f` with this committed slice, the final Windows
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

The migration target covers legacy headers 1/2/3, memory headers 4/5, mixed binary
types, guarded history and absolute deadlines, rehashing, source preservation
and locking, quota/record refusal, failures around publication and old-reader
refusal for every first-record prefix. Actual frozen old-binary subprocesses,
Linux filesystem checks, power-loss tests, public CLI activation and transport-2
replication/live backup remain separate issue #110 gates.
