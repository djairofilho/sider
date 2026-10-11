# Staged memory backup library

This development slice implements schema-2 archive export, verification and
restoration for issue #110. It is not part of the published 1.0.1 package and
does not complete the memory durability contract. The default server and backup
CLI retain record 1. An explicit transport-2 snapshot producer and live backup
consumer are now available as library APIs. The development server can expose
an explicitly configured live record-2 source on its internal listener; native
replication and the default backup CLI remain separate gates.

## Explicit profiles

| API | Accepted input | Output |
| --- | --- | --- |
| Existing `export`, `export_secure`, `export_stream` | Transport 1 | Schema 1, header 2, record 1 |
| Existing `verify`, `restore` | Schema 1, header 2, record 1 | Original legacy profile |
| `export_memory_offline` | Stopped AOF explicitly configured with `with_memory_profile`, header 4/5, record 2 | Schema 2, header 4, record 2 |
| `export_memory`, `export_memory_stream` | Explicit transport-2 memory snapshot source | Schema 2, header 4, record 2 |
| `verify_memory`, `restore_memory` | Schema 2, header 4, record 2 | Verified original memory profile |

No API infers a profile from the current values. Memory verification refuses
legacy archives; the default verifier refuses schema 2. Both refuse header 7 and
other unsupported versions. No implicit migration or downgrade is introduced.
The server selects record 2 only with `SIDER_AOF_RECORD_VERSION=2`.

## Offline export

Stop the source writer. Supply its exact shard/routing layout and record limits,
the desired archive ceilings and a new destination outside the source tree.
The source directory and its `writer.lock` must already exist. Export acquires
the same exclusive writer lock and holds it until the operation finishes.

```rust
use std::sync::Arc;
use sider::persistence::{AofConfig, backup};
use sider::storage::SystemClock;

let source = AofConfig::new("/data/stopped-memory".into()).with_memory_profile();
let manifest = backup::export_memory_offline(
    backup::OfflineMemoryExportOptions {
        source,
        destination: "/backups/memory-001".into(),
        source_sha: "0123456789012345678901234567890123456789".into(),
        limits: backup::Limits::default(),
    },
    Arc::new(SystemClock),
)?;
# Ok::<(), backup::Error>(())
```

Read-only recovery validates the entire source history, including guarded member
deltas. It rejects an incomplete tail rather than truncating it. Unsupported
header/record versions, quota failures and invalid history fail before creating
the archive. Installation residues and every source file remain unchanged.

One frozen clock sample governs export replay, quotas and output validation.
The archive preserves ordered complete key images, binary vector bits, typed
metadata, provenance, empty collections, and absolute member/key deadlines.
Retained elapsed memory state is included and charged, so it can never receive
a fresh relative TTL during restoration. Expired legacy values retain the
existing legacy replay semantics and may be absent from the final snapshot.

The three archive files keep the existing names: `snapshot.aof`,
`backup-manifest.json` and `SHA256SUMS`. Schema 2 keeps schema 1's field names,
with `schema_version: 2`, `aof_header_version: 4`, and `record_version: 2`.
The AOF is sealed and contains no source role or epoch. The manifest records
the source cursor separately; restoration does not install it as a replication
identity or inherit credentials.

`source_sha_declared` is an operator declaration. AOF files do not attest to a
source executable's Sider version. The manifest identifies the exact exporting
tool version and verification requires that same version. Offline
`source_max_snapshot_bytes` records the explicit export ceiling, since an AOF
source has no transport handshake announcing a snapshot limit. The transport
digest is calculated from the frozen transport-2 tag-4 envelope for each record.
A separate live consumer reads these frames. The development server exposes a
memory-profile listener only when record 2 and `SIDER_REPLICATION_ADDR` are both
configured; the default record-1 listener retains transport 1.

## Explicit live export stage

`replication::memory::export_snapshot` accepts a caller-captured, ordered,
consistent snapshot and its matching cursor. The caller owns authorization,
the snapshot cut and the source stream. The producer checks transport version,
record version and receiver capacity before sending any image; it does not
capture a worker cut, publish a journal or start a listener. Transport 1 remains
the default for existing replication and backup entry points.

`backup::export_memory_stream` accepts an injected transport-2 stream;
`backup::export_memory` connects to an explicitly configured loopback source;
`backup::export_memory_secure` uses authenticated mutual TLS.
They verify the handshake and ordered complete images, write a sealed schema-2
archive, and use the existing memory verifier and restorer. The server source
captures all owner workers under the database barrier, flushes the same AOF cut,
and emits its sequence with a zero epoch because no replica stream is active.
It checks transport, record version, and receiver capacity before capture.
Plaintext source connections require loopback; protected connections require a
current internal backup role and recheck authorization during transfer. The
listener accepts only a snapshot export request in this stage. The default CLI
does not select the memory profile.

## Verify and restore

Call `verify_memory` with the expected layout and sufficient per-shard quota.
Call `restore_memory` with the same checks and a new directory outside the backup.
The manifest profile and exact Sider version are checked before snapshot I/O.
The actual header is cross-checked before hashing/replay. Required files must
be bounded regular files, and archive/file symlinks are refused.

Verification checks SHA-256, size, record CRC, complete images, key order,
duplicates, sequence, count, seal, transport digest and EOF. It processes one
image at a time. Quota includes retained elapsed memory members and collections;
the report's `live_entries` excludes elapsed outer-key deadlines. Each shard
must fit its own quota. Free space on another shard cannot compensate.

Restore verifies the complete archive before creating any destination, then
copies it, syncs it and verifies the copy again before publishing the initial
generation. The bytes and absolute TTL are preserved. Starting recovery at a
later time consumes elapsed TTL; it does not extend it. Existing destinations
are refused and their contents are preserved.

Errors remove only files owned by the operation. Injectable write/sync/publication
boundaries exercise cleanup before and after a complete generation rename.
Forced process termination can leave a partial directory; it is never valid
without complete checksums and a sealed generation. Filesystem power-loss
guarantees retain the existing platform-specific directory-sync assumptions.

## Bounds and remaining evidence

Existing backup limits apply: record payload, mutation count, archive and framed
snapshot bytes, and logical quota divided among shards. Source record/mutation
capacities must fit the receiver before recovery. `Limits::timeout` applies to
network export; synchronous offline export/verify/restore do not claim a wall-time
deadline. Logical charge is not an RSS bound. Full source recovery and the frozen
snapshot retain bounded dataset state, but this slice does not establish the
contract's complete 16 MiB lifecycle scratch proof.

Focused Windows tests cover mixed types/memory, schema/version preflight,
elapsed TTL, binary data, corruption, incomplete source history, source locking,
quota/layout rejection, and injected partial writes and publication failures.
Memory-specific child process/power-loss gates, Linux filesystem checks, the
extracted-package exercise, the default CLI integration, replica synchronization,
and full release gates remain pending.

## Verification commands

The Windows implementation check at the development baseline `31e18ab`, with
this committed slice, passed nine memory backup cases, ten memory
AOF cases and seven native memory cases. The legacy backup target passed 11
cases; the real-process target passed 11 with its existing extracted-package
gate ignored. The library target passed 177 tests. Reproduce the focused checks:

```text
cargo test --locked --test agent_memory_backup --test agent_memory_aof
cargo test --locked --test backup --test backup_process --test agent_memory_native -- --test-threads=1
cargo test --locked --lib
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo doc --locked --no-deps
cargo build --locked --release
cargo xtask check
```

The actual receipt used `--target-dir` with the integrated dependency cache under
an exclusive Cargo slot. Both the serial affected regression run and the final
default-parallel memory backup/AOF run passed. The original AOF timestamp-only
fixture hit a Windows `AlreadyExists`; its separately scoped counter suffix now
prevents that path collision. Clippy passed without further source changes.

API documentation passed in 7.94 seconds and the optimized release build passed
in 44.38 seconds using the same exclusive shared-target cache. These focused
checks and the subsequent full `cargo xtask check` passed. They do not
constitute full issue #110 or external release approval.
