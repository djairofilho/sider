# Composed backup profile

The protected Docker profile serves a one-shot composed export on its internal
mTLS listener. Use `SIDER_AOF_RECORD_VERSION=1` and `SIDER_AOF_COMPOSED=1` on
the source, provision a backup principal in `internal-policy.json`, and give
`sider-backup` a restricted security generation containing its client
certificate, key, trust anchor, and policy manifest. Connect the backup tool to
the source's internal TLS address with `--profile composed`; `verify` and
`restore` use the same archive format. The source checks the certificate role
before accepting the export request. Revoking the backup role with a complete,
manifest-last security generation prevents subsequent exports while a separate
monitor role can continue to read status.

The Docker rehearsal in `tests/docker_oci.rs` exercises protected export,
verification, restore, a mixed-owner transaction, replica reconnect, and
backup-role revocation with real containers. This is development evidence;
the release requires a new receipt against the public image pulled by digest.

## Legacy loopback development path

The development checkout supports a schema-3 path for header-7, record-1
mixed-owner snapshots. The server selects the local AOF profile only with
`SIDER_AOF_COMPOSED=1`. Set `SIDER_COMPOSED_EXPORT_ADDR` to a literal loopback
address to start its separate transport-3 backup listener. The listener is
plaintext and unauthenticated, so it requires the explicit
`SIDER_SECURITY_PROFILE=legacy-loopback` profile; protected startup rejects it.
It accepts one-shot backup exports, not replica sessions. The default AOF reader
still rejects header 7. A restored directory requires the same explicit profile
and matching shard count.

Start a two-shard source with an AOF directory and export readiness file (set
equivalent environment variables in PowerShell on Windows):

```sh
SIDER_SECURITY_PROFILE=legacy-loopback SIDER_AOF_DIR=/data/composed \
SIDER_AOF_COMPOSED=1 SIDER_SHARDS=2 \
SIDER_COMPOSED_EXPORT_ADDR=127.0.0.1:6381 \
SIDER_COMPOSED_EXPORT_READY_FILE=/run/sider-composed-export.json ./sider
```

Use the exact source SHA from the build receipt. All three CLI operations require
`--profile composed`; the older `--record-version 1` option selects the legacy
schema-1 archive instead.

```sh
./sider-backup export --profile composed --source 127.0.0.1:6381 \
  --destination /backups/composed-001 --source-sha SOURCE_SHA_40_HEX
./sider-backup verify --profile composed --source /backups/composed-001 \
  --shards 2 --routing 1
./sider-backup restore --profile composed --source /backups/composed-001 \
  --destination /data/restored-composed --shards 2 --routing 1
SIDER_SECURITY_PROFILE=legacy-loopback SIDER_AOF_DIR=/data/restored-composed \
  SIDER_AOF_COMPOSED=1 SIDER_SHARDS=2 ./sider
```

The export listener binds during preparation and writes its own readiness file
after worker startup. Its address must stay on loopback. Export requests have
bounded concurrent sessions; slow or disconnected consumers do not change source
data. A source profile mismatch fails before capture. This development path does
not establish a protected network or native release receipt.

Transport 3 sends a standalone `ComposedExport` request with exact record version
`1`, header version `7`, and the `MIXED_OWNER_BATCH` capability bit. A source can
call `replication::composed::negotiate_export` before snapshot capture. The returned
admission token binds the captured Hello to the accepted profile and limits; a
profile change or invalid snapshot emits no valid cut. The source sends ordered
record-1 snapshot entries, a digest-bearing `FullEnd`, and EOF without an ACK or
incremental subscription.

`replication::composed::export_database` connects that negotiation to a live
`DbHandle`. A library caller explicitly recovers a header-7 directory, binds the
handle and every owner worker to the same AOF writer, then passes that handle,
writer, source profile, and nonzero epoch to the export function. It negotiates
before `DbHandle::composed_snapshot` freezes the workers and flushes the writer.
The captured sequence and sorted mutations form one cut. A handle bound to a
different writer cannot supply that cut. The executable's explicit loopback
listener invokes this path for each admitted backup request.

`persistence::backup::export_composed_stream` receives that stream and publishes a
new archive only after validating the Hello profile, cursor, frame order, per-shard
quota, digest, seal, and EOF. `export_composed` and `export_composed_secure` use an
explicit transport-3 source address. The local executable also serves plaintext
loopback export only when `SIDER_SECURITY_PROFILE=legacy-loopback` is selected;
the protected internal listener serves the same composed snapshot over mTLS.
The manifest uses schema `3` with a mandatory `required_capabilities: 1` field.
The sealed `snapshot.aof` has a role-0 header 7, record-1 images, and zero epoch.
The archive also contains `backup-manifest.json` and `SHA256SUMS`.

`verify_composed` checks the manifest, hashes, header profile, records, quotas,
seal, and transport digest. `restore_composed` copies only a verified archive into
a new directory and revalidates the copy before publication. The caller must
set the restored layout on `AofConfig` and call `persistence::recover_composed`
to replay header 7. Every owner must use its returned writer handle for
atomic mixed-owner batches. Server startup requires `SIDER_AOF_COMPOSED=1`. The
schema-1 and schema-2 APIs reject a schema-3 archive and retain their existing
wire and manifest formats.

Focused library tests run with `cargo test --locked --test replication_composed_backup`.
They cover a complete export/verify/restore/recover cycle from both a captured
fixture and a live mixed-owner transaction, refusal of an unbound source before
archive creation, cleanup after a truncated stream, and semantic profile
mismatches after recomputing checksums. `cargo test --locked --test composed_server
composed_backup_cli_exports_verifies_restores_and_restarts` covers the executable
source, CLI export/verify/restore, and restart from the restored directory.
Publication still requires the candidate's complete Docker, network-failure,
recovery, soak, benchmark, and external release evidence.
