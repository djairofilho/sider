# Offline AOF migration

Partitioning is part of the directory's durable configuration. Recovery checks
the shard count and routing version before repairing a tail or accepting data.
Changing `SIDER_SHARDS` does not redistribute an existing AOF.

The R03 v1 header represents one shard. New directories and compactions use v2,
which records the shard count and routing. Routing version 1 uses FNV-1a64 and
the same hash tags as the server. Record and mutation versions do not change
in this transition.

## Running the migration

Stop the source server and choose a destination directory that does not yet exist.
Its parent must exist, and the destination must be outside the source tree.
The source must contain its `writer.lock`, created by the server. The migrator
acquires this lock and refuses the operation if another writer is still active.

```sh
cargo build --locked --release --bin sider-aof-migrate
target/release/sider-aof-migrate \
  --source ./data-r03 --source-shards 1 --source-routing 1 \
  --destination ./data-r04 --shards 4 --routing 1
```

In PowerShell:

```powershell
& .\target\release\sider-aof-migrate.exe `
  --source 'C:\data\sider-r03' --source-shards 1 --source-routing 1 `
  --destination 'C:\data\sider-r04' --shards 4 --routing 1
```

All six directory and identity options are required. Paths preserve the system's
native representation. The parser is injectable and does not modify the environment.
`cargo run --locked` continues to start the server; to run the migrator through
Cargo, use `cargo run --locked --bin sider-aof-migrate -- ...`.

| Additional option | Default | Purpose |
| --- | --- | --- |
| `--source-max-dataset-bytes` | `67108864` | Quota used to recover the source |
| `--max-dataset-bytes` | `67108864` | Total destination quota |
| `--source-max-record-bytes` | `67108864` | Accepted source record limit |
| `--max-record-bytes` | `67108864` | Destination record write limit |

The destination quota is divided statically: `total / shards`, plus one byte
for each index below `total % shards`. Migration rejects a distribution with
an overloaded shard even if the dataset fits within the total. In that case,
choose a quota that accommodates the distribution or adjust the data before retrying.

## What is preserved

The migrator recovers the source read-only, without truncating or compacting its
files. Values, binary keys, Unix deadlines, and the last complete sequence are
preserved. Values already expired at migration time are absent.
A frozen clock pair makes snapshot reading and validation consistent.

An incomplete tail after a valid seal is ignored in the destination snapshot,
but remains intact in the source and appears in the report. Corruption, an unknown
version, mismatched configuration, or an invalid batch stops the operation.

The snapshot is routed by the actual server implementation. Accounting uses a
temporary `Store` with at most one entry, sharing the immutable value; it does not
build a second complete dataset. After checking quotas, the migrator reserves the
new directory, writes a complete snapshot and seal, and synchronizes the file.
It reopens the temporary file, checks the header, every record, and EOF,
and only then publishes generation zero.

If writing fails, cleanup removes only files created by that operation.
An existing destination is never overwritten and the source remains usable.
Publication and synchronization follow the [platform guarantees](persistence.md#platform-guarantees).

The CLI returns JSON with the source format, sequence, entry count, source and
destination identities, per-shard usage, and incomplete tail bytes. Keep this
report with the configuration and directory hashes. The migrator does not change
server configuration or begin accepting connections.

After checking the result, start the server with the new directory and matching
shard count. Migration does not change key names: multikey operations that previously
used a single worker may be rejected as `CROSSSHARD` under the new distribution.
Hash tags allow groups of keys to stay together.

To return to the previous configuration, stop the new server and use the preserved
source with its compatible binary and configuration. Writes made only to the
destination after migration will not exist in that source. The old R03 binary
does not understand v2 headers.

## API and verification

`AofConfig.layout` defines `DurableLayout { shard_count, routing_version }`.
`recover` returns `Recovered.metadata` with identity, sequence, header version,
per-shard usage, and valid/incomplete extent. The integrator may distribute the
snapshot to workers only after these checks.

`migration::migrate_offline(MigrationOptions, Arc<dyn Clock>)` accepts separate
source and destination configurations and quotas. `migration::options_from_args`
is the pure parser used by the CLI.

```sh
cargo test --locked --test aof_migration
cargo test --locked --test persistence
cargo test --locked --lib persistence
```

Cases include v1/v2, header truncation and corruption, configuration mismatches,
cross-shard batches, local quota, TTL and sequence preservation, an active source
lock, overwrite refusal, cleanup after errors, and the actual CLI.

The internal R03 baseline was frozen at SHA
`c7b148abeff43fd354d29b1bbce36aea43ea8f3f`, with the binary, strings/MSET/TTL AOF,
configuration, logs, and hashes. Actual migration of that directory from one to
four shards was checked with the migrator at SHA
`4739d596f8d80d0038a9a97838395c90be60ceff`: three entries, sequence two, and an
identical source hash before and after. Local artifacts are in
`target/baselines/r03-<SHA>` and `target/baselines/r04-migration-<SHA>`.
These records are internal baselines, without release identity or publication.
