# Consistent backup and restoration

`sider-backup` captures a consistent point from the primary over TCP and produces
a sealed AOF snapshot, a manifest, and checksums. Restoration verifies the set
before creating a new data directory. Use the executable from the
[package of the same version](packages.md), with the `.exe` suffix on Windows.

## Preparing the source

The primary needs AOF and the internal replication/export listener.
Configure separate addresses for RESP clients and export:

```sh
SIDER_ADDR=127.0.0.1:6379 SIDER_AOF_DIR=/data/primary \
SIDER_AOF_SYNC=always SIDER_SHARDS=4 \
SIDER_REPLICATION_ADDR=127.0.0.1:6381 ./sider
```

In PowerShell, set the same `$env:SIDER_...` variables before starting
`.\sider.exe`. The address requires a literal IP and port. This listener does not
use RESP or accept Redis clients. It has no authentication or TLS; keep it on
loopback or the service's controlled private network.

For ephemeral ports in tests, use `SIDER_REPLICATION_ADDR=127.0.0.1:0` and
`SIDER_REPLICATION_READY_FILE` with a unique path. The separate file publishes
PID, host, and port when durable preparation is complete. RESP readiness
remains in `SIDER_READY_FILE`.

## Exporting

Consult the build manifest for the full source SHA. Choose a directory that does
not yet exist, with an existing parent:

```sh
./sider-backup export --source 127.0.0.1:6381 \
  --destination /backups/backup-001 \
  --source-sha SHA_COMPLETO_DE_40_HEXADECIMAIS
```

The client requires the same Sider version, checks the layout and limits announced
during the handshake, and receives `Hello`, `FullStart`, ordered entries, `FullEnd`,
and EOF. Cursor, count, order, lengths, and digest must match. There is no ACK
or incremental stream on this connection.

The snapshot combines all shards at the same durable cut, including every type
and complete transaction effects. The worker barrier covers capture and the sequence
cut. Transfer uses this immutable image outside the barrier; a slow client does
not hold the global lock during transmission. Writes after the cut are excluded
from this backup.

A successful run returns JSON on stdout and leaves three files:

| File | Contents |
| --- | --- |
| `snapshot.aof` | v2 header with layout/sequence, typed records, and seal |
| `backup-manifest.json` | Version, cursor, declared SHA, limits, count, size, and hash |
| `SHA256SUMS` | SHA-256 of the manifest and snapshot |

The v2 header does not copy the source's replica role or replication identity.
The full cursor is in the manifest. `source_sha_declared` is an operator
declaration: the protocol does not attest to the remote binary's SHA.
`validation_max_dataset_bytes` records the quota chosen to validate the capture,
not a quota inferred from the source.

Files are synchronized before success is returned. Errors or cancellation remove
partial files owned by the command. Forced termination may leave a partial directory;
it is never accepted without a valid manifest, checksums, and seal.
Do not reuse its name without inspecting its contents. Existing destinations are rejected.

## Verifying and restoring

Keep the backup unchanged while the commands read it. Specify the expected layout
and a sufficient quota per shard. These examples use four shards, routing version 1,
and the default total quota of 64 MiB:

```sh
./sider-backup verify --source /backups/backup-001 --shards 4 --routing 1
./sider-backup restore --source /backups/backup-001 \
  --destination /data/restored-001 --shards 4 --routing 1
```

`verify` does not modify data. `restore` checks the manifest, checksums, header,
records, order, seal, and EOF before creating the destination. It validates the
copy again before publishing it as the initial AOF generation. Symbolic links in
required files and in the selected directory are rejected. The destination must
be new and outside the backup; a directory in use is not overwritten.

Keys retain their absolute Unix expiration. Time between capture and restoration
consumes TTL: already expired keys do not become visible again. Quota is divided
with the same algorithm as the server; free space on another shard does not
compensate for local overflow. The report gives per-shard usage and still-live entries.

Start the isolated instance with the same layout and quota used for restoration:

```sh
SIDER_ADDR=127.0.0.1:6382 SIDER_SHARDS=4 \
SIDER_AOF_DIR=/data/restored-001 SIDER_AOF_SYNC=always ./sider
```

Changing the shard count requires [explicit offline migration](aof-migration.md)
after restoration. Do not edit the header or manifest to simulate a different
layout. Promote the instance after checking its data and readiness.

## Limits and guarantees

| Option | Default and effect |
| --- | --- |
| `--max-record-bytes` | 64 MiB per AOF payload, maximum 64 MiB |
| `--max-mutations` | 100,000 per batch, maximum 100,000 |
| `--max-snapshot-bytes` | 256 MiB, maximum 2 GiB; limits accumulated frames and the local file |
| `--max-dataset-bytes` | 64 MiB total logical quota, divided by shard |
| `--timeout-ms` | 120,000 ms for the entire connection/export, including EOF |

Limits announced by the source must fit the receiver, even when the current snapshot
is small. Invalid limits are rejected before connection. Logical quota is not
an RSS ceiling. Local validation processes one entry at a time and does not restore
the entire dataset in memory.

SHA-256 and record checksums detect corruption; they do not authenticate a backup
against someone who can replace all files and checksums. Keep the source and
files under the environment's access controls.

The network deadline does not interrupt blocking filesystem I/O. Sync guarantees
follow [Sider persistence](persistence.md): process termination tests are not
equivalent to power-loss tests. Windows does not promise directory sync as Linux does.
A completed backup also depends on the durability of the device or volume where
it was written.

## Reproducible evidence

Record the clean checkout SHA, executable versions, source configuration, commands,
outputs, exit codes, and hashes. Preserve the backup and original data for the
internal R10 baseline and 1.0 migration. Do not transfer results to another SHA
or rewrite earlier baselines.

The [internal baseline runbook](internal-baseline.md) records this cycle using
extracted packages with one and four shards. During migration, the R10 backup is
restored by the frozen source CLI before opening the data with the new server.

`cargo test --locked --test backup` covers the actual CLI over TCP, all five types,
binary bytes, absolute TTL, layout, corruption, quotas, existing directories,
EOF deadlines, and cancellation on Linux and Windows. It uses injected clocks and
ephemeral ports. `cargo test --locked --test backup_process` uses actual primary
and CLI processes: it maintains a slow receiver with a reduced buffer, confirms
transaction progress, exports another snapshot, and starts isolated restoration.
It compares all five types, indivisible EXEC cuts, per-shard quotas, and TTL consumed by time.

To repeat this scenario with executables from the actual extracted archive:

```sh
SIDER_BACKUP_PACKAGE_DIR=/path/extracted/sider-vTARGET-VERSION \
  cargo test --locked --test backup_process -- --ignored --exact extracted_package_backup_roundtrip_under_traffic --nocapture
```

The result records selected paths and observed data. Provenance from the distributed
archive, SHA, and hashes still require separate evidence of building, packaging,
and extraction. This test does not issue release approval.
