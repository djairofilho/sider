# Isolated composed backup profile

The development library has an explicit schema-3 path for header-7, record-1
mixed-owner snapshots. It is not selected by the Sider executable or backup CLI.
The default AOF reader still rejects header 7, so a restored directory cannot yet
be started through public server configuration.

Transport 3 sends a standalone `ComposedExport` request with exact record version
`1`, header version `7`, and the `MIXED_OWNER_BATCH` capability bit. A source can
call `replication::composed::negotiate_export` before snapshot capture. The returned
admission token binds the captured Hello to the accepted profile and limits; a
profile change or invalid snapshot emits no valid cut. The source sends ordered
record-1 snapshot entries, a digest-bearing `FullEnd`, and EOF without an ACK or
incremental subscription.

`persistence::backup::export_composed_stream` receives that stream and publishes a
new archive only after validating the Hello profile, cursor, frame order, per-shard
quota, digest, seal, and EOF. `export_composed` and `export_composed_secure` use an
explicit transport-3 source address; no production listener currently serves it.
The manifest uses schema `3` with a mandatory `required_capabilities: 1` field.
The sealed `snapshot.aof` has a role-0 header 7, record-1 images, and zero epoch.
The archive also contains `backup-manifest.json` and `SHA256SUMS`.

`verify_composed` checks the manifest, hashes, header profile, records, quotas,
seal, and transport digest. `restore_composed` copies only a verified archive into
a new directory and revalidates the copy before publication. It does not enable
normal startup of the restored directory yet. The schema-1 and schema-2 APIs
reject a schema-3 archive and retain their existing wire and manifest formats.

Focused tests run with `cargo test --locked --test replication_composed_backup`.
They cover a complete export/verify/restore cycle, refusal before archive
creation, cleanup after a truncated stream, and semantic profile mismatches after
recomputing checksums. Production use still needs a selectable header-7 source,
replica and recovery paths, the backup CLI, and native release evidence.
