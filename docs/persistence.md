# AOF persistence

The AOF records each command's final state in indivisible batches. It is available
from internal milestone R03. The format has its own version, independent of the
binary version. Without `SIDER_AOF_DIR`, the server continues to run in memory only.

## Running

```powershell
$env:SIDER_AOF_DIR = 'C:\data\sider'
$env:SIDER_AOF_SYNC = 'always'
cargo run --locked
```

```sh
SIDER_AOF_DIR=./data SIDER_AOF_SYNC=always cargo run --locked
```

Use the same directory when restarting. An exclusive operating system lock prevents
two writers from opening that directory. Recovery occurs before binding and creating
the readiness file. In the API that receives an already open listener, `serve`
recovers before accepting connections. `server::prepare` supports explicit recovery
before opening the listener.

The header records the shard count and routing version. A mismatch prevents recovery
before any tail repair. Legacy AOF v1 implies one shard. Changing the partitioning
requires [offline migration](aof-migration.md).

| Variable | Default | Contract |
| --- | --- | --- |
| `SIDER_AOF_DIR` | Unset | Native directory path; enables persistence |
| `SIDER_AOF_SYNC` | `always` | `always` or `everysec` |
| `SIDER_AOF_QUEUE_CAPACITY` | `32` | Maximum number of requests waiting for the writer |
| `SIDER_AOF_MAX_RECORD_BYTES` | `67108864` | Record payload; between 64 bytes and 64 MiB |
| `SIDER_AOF_MAX_DELTA_BYTES` | `16777216` | Bytes of records captured during compaction |
| `SIDER_AOF_COMPACT_AFTER_BYTES` | `67108864` | Accumulated append bytes that request compaction; `0` disables automatic compaction |

AOF options are parsed when the directory is configured. The record limit must fit
a complete mutation and its internal framing. Reducing this limit may prevent reading
an existing AOF or writing a value that previously fit. A write exceeding the limit
receives `ERR AOF record limit exceeded`, without append or application, while the
connection remains available. The 64 MiB ceiling still applies to configurations
with larger datasets.
The default limit of 100,000 mutations per batch is also checked before allocation.

## Acknowledgment and failure

The worker prepares only the keys touched by the command, sharing immutable bytes.
It resolves `SET` conditions, increments, TTL, and quota using a consistent clock
reading. It then sends a `ResolvedBatch` to the global writer, which assigns an
increasing sequence. The worker applies the prepared state and responds only after
the writer acknowledges it. Replay never reexecutes conditions such as `NX`, `XX`,
or increments.

| Policy | What acknowledgment guarantees | Possible loss |
| --- | --- | --- |
| `always` | `write_all` and `sync_all` completed before application and response | The process crash suite requires recovery of all acknowledged batches; physical integrity depends on the filesystem and device |
| `everysec` | `write_all` completed; the next sync is periodic | Anything that has not reached a successful `sync_all` may be lost in a system failure |

`everysec` requests synchronization every second, including when there are no new
writes. This interval is not a strict loss bound: I/O and scheduling delays can
extend the window. Normal shutdown drains accepted requests and synchronizes the
writer. The drain deadline does not make filesystem calls cancelable.

In replication, the replica synchronizes the applied batch before sending an ACK,
including with `everysec`. This ACK does not change the primary's acknowledgment
policy or make its responses wait for replicas; see [replication](replication.md).

Append or sync failure prevents a success response and closes worker admission.
A completely written batch may reappear on restart even if the response was lost.
A timeout or disconnection after acceptance does not prove the absence of effects
and does not justify automatically retrying an increment.

Active expiration uses the same durable path, with origin `Expiration`. Reads that
find an expired entry also produce tombstones with this origin on the primary.
Replicas hide expired entries from reads but receive tombstones from the primary;
they neither run active expiration nor generate local removals.

## Header formats v1, v2, and v3

All integers use little endian. Keys and values are bytes, with no UTF-8 requirement.
CRC-32/ISO-HDLC detects accidental corruption; it does not authenticate files.

Files and compactions without replication metadata use the 32-byte v2 header:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | ASCII magic `SIDERAOF` |
| 8 | 4 | Format version, `2` |
| 12 | 8 | Sequence represented by the initial snapshot |
| 20 | 4 | Shard count, between 1 and 256 |
| 24 | 4 | Routing version, `1` |
| 28 | 4 | CRC of the first 28 bytes |

With durable replication role and epoch metadata, the writer uses the 52-byte v3
header. The first 28 bytes contain the same fields, with version `3` at offset 8.
They are followed by:

| Offset | Size | Field |
| --- | --- | --- |
| 28 | 1 | Role: `1` primary or `2` replica |
| 29 | 3 | Reserved; must be zero |
| 32 | 16 | Replication stream epoch |
| 48 | 4 | CRC of the first 48 bytes |

The dataset, sequence, role, and epoch belong to the same published generation.
Compaction preserves this metadata. Backup/restore writes v2 without inheriting
the source role; starting a replicated instance establishes its durable metadata
according to the [replication contract](replication.md).

The reader also accepts the 24-byte v1 header: the same magic, version `1`, sequence,
and CRC of the first 20 bytes at offset 20. Its implicit configuration is one shard
with routing v1. Routing version 1 identifies FNV-1a64 over the hash tags in
`storage::routing`; unknown versions are rejected. Records and mutations retain the
same encoding across all three header versions. The record version remains `1`,
independent of the header version. Older binaries that do not recognize v3 must
reject it without editing the file.

Each record contains a `u32` payload length, its `u32` bitwise complement, a `u32`
payload CRC, and the payload. Length and complement are checked before allocation;
the checksum is checked before decoding. Internal fields must fit entirely within
the record. Unknown types, origins, and extra bytes are errors.

| Tag | Payload after the tag |
| --- | --- |
| `1`, snapshot | One `Put` mutation |
| `2`, seal | `u64` sequence, `u64` entry count, `u32` digest |
| `3`, batch | `u64` sequence, `u8` origin, `u32` count, mutations |

Snapshot entries are in ascending binary order, without duplicates. The digest
chains the CRCs of complete records using `snapshot_digest` and starts at zero.
The seal's count and digest also detect removal of a complete record. A file is
recoverable only after a valid seal. Subsequent batches must have consecutive
sequences starting from the snapshot.

The initial mutations use tag `1` for string `Put` and `2` for `Delete`. Both contain
a `u32` key length and the key. `Put` adds a `u32` value length, the value, a `u8` TTL
flag, and an `i64` Unix deadline in milliseconds. Without TTL, both final fields are
zero. Origin `1` indicates a client; `2` indicates expiration.

R05/R06 add complete postimages: tag `3` for hash, `4` for list, `5` for set, and `6`
for sorted set. Collections cannot be empty in the file; keys/fields/members remain
binary. Scores preserve IEEE bits and reject NaN even with a valid checksum. The
[typed persistence contract](types-persistence.md) describes ordering, quota, TTL,
and crash tests for these families.

The parser and storage prevalidate the entire batch. Duplicate keys within a batch,
exceeded quota, or an unrepresentable deadline prevent application. During recovery,
an already expired `Put` removes the previous value and does not become persistent
again. The logical quota and expiration index are rebuilt.

Each batch must belong to a single shard under the recorded configuration. The total
quota is divided by the shard count, distributing the remainder among the first
indices. Recovery tracks each shard's usage and rejects local excess even when the
global total still fits. Recovered metadata is available before the writer starts.

## Recovery and compaction

Published files are named `generation-NNNNNNNNNNNNNNNNNNNN.aof`. The server opens
the highest generation. Corruption in that generation stops startup; there is no
silent fallback to an older generation that could lose acknowledged writes.

A tail containing an incomplete record after a valid seal is recoverable. Before
truncating it, the server copies and synchronizes the original to `tail-*.bak`.
An invalid header, incorrect checksum, unknown version, or internal corruption
preserves the AOF and ends startup with an error. For diagnosis, work on a copy of
the directory with the server stopped. Do not rename an older generation as current
without assessing the corresponding data loss.

Compaction captures a consistent snapshot, writes its records and seal to a temporary
file, and continues appending to the current file. The writer accumulates a bounded
delta, including expirations. Once the snapshot is complete, it writes the delta,
synchronizes the new file, and publishes a generation under an unused name. Only
then does it switch the append destination. The previous file remains complete;
the generation preceding this backup is removed at the next successful compaction.

If the delta exceeds its budget, compaction aborts and the current file remains
valid. The producer finishes before another snapshot is allowed, bounding concurrent
work. Failure before publication also preserves the current writer. Failure after
publication is fatal, preventing new writes from continuing only in the old
generation. Interrupted temporary files are never selected for replay and may be
removed with the server stopped after checking the current generation.

Immutable values are shared by the snapshot; snapshot metadata and the delta consume
additional memory. The dataset's logical quota is not an RSS limit. With multiple
workers, the integrator must coordinate a global barrier before enqueueing
`begin_compaction`; an isolated snapshot of one shard does not represent the global AOF.

## Platform guarantees

The lock uses `File::try_lock`, opened for reading and writing, compatible with
Windows locking requirements. `sync_all` requests persistence of file contents and
metadata. These are the contracts of the
[Rust standard library](https://doc.rust-lang.org/std/fs/struct.File.html).
The guard explicitly releases the lock before closing the file. On Linux, this
prevents descriptors duplicated by `fork`/`dup` from extending ownership after the
writer stops, according to
[`flock` semantics](https://man7.org/linux/man-pages/man2/flock.2.html).

On Linux, publication also synchronizes the directory after rename. On Windows,
Rust does not provide this directory synchronization portably. The implementation
publishes under a new name, retains the previous generation, and was tested with
abrupt process termination during the switch phases. This does not demonstrate
atomicity against power failure. `rename` behavior depends on the system, as
described in its [documentation](https://doc.rust-lang.org/std/fs/fn.rename.html).

## Verification

```sh
cargo test --locked --lib persistence
cargo test --locked --lib storage::mutation
cargo test --locked --test persistence
cargo test --locked --test aof_migration
```

The internal suite is independent of release context and uses real files in temporary
directories. The parent starts children from the suite itself and terminates them
only after an explicit signal at the failure points. It does not terminate unrelated
processes.

The `release_crash_gate`, `release_recovery_gate`, and `release_migration_gate` runners
are registered in `releases/gates.json`. They require `GateContext`, repeat the actual
cases, and publish receipts only after validating the SHA, platform, and bundle
context. The four ignored tests in the internal run are these three wrappers and
the child process helper; their absence does not count as a passed gate.

The initial migration gate reads the fixed fixture `tests/fixtures/aof-v1.hex`,
compacts it, reopens it, and compares its state. It also rejects an unknown version
while preserving the bytes. The fixture represents the first AOF format, without
claiming migration from an earlier published version with persistence.

R03 development validation: the `persistence` suite passed 20 tests on Windows and
Ubuntu via WSL, including nine process crash points. Linux test files reside in
`/tmp`, on the Linux filesystem. These results are local functional verification;
final bundle receipts require a new run on the frozen SHA according to the
[release guide](releases.md).
