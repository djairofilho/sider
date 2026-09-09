# Changelog

## [Unreleased]

## [1.0.0] - Candidate preparation

The package identifies itself as `1.0.0`; the candidate and final release use the same build and files.
The [release notes](releases/notes/v1.0.0.md) describe capabilities and limitations.
Approval depends on gates for the exact SHA and the evidence set produced for it;
this section does not declare publication or passing gates.

- Single-shard transactions with `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH`;
  validation before application, one AOF batch per EXEC, and individual errors without rollback.
  Publications in transactions preserve order after applying the batch acknowledged
  according to the configured AOF policy.
- Asynchronous Sider → Sider replication with snapshots, bounded history,
  FULL/CONTINUE reconnection, read-only replicas, and durable manual promotion.
  The AOF v3 header preserves role and epoch; current readers retain v1/v2 support.
- `sider-backup` exports consistent snapshots during traffic, checks the manifest
  and checksums, and restores into a new directory, preserving types, batches, and absolute TTL.
- `INFO`, operational sections, and `sider --diagnose` for metrics, queues, persistence,
  replication, and configuration validation without starting listeners.
- Windows/Linux packages with four executables: server, AOF migrator,
  backup, and replica administration. The private Docker image uses the package binaries,
  an unprivileged user, persistence, and an actual export/load test.
- Complete compatibility matrix and differential audit of sequences across
  types, TTL, transactions, and Pub/Sub, with fixed seeds and separate counts.
- 3600-second soak and benchmark runners for the extracted package, with samples,
  hashes, and documented limits. Approval depends on execution against the candidate build.
- Binary hashes, lists, sets, and sorted sets, with WRONGTYPE, TTL, and atomic quota;
  AOF preserves typed postimages, scores, and deadlines during replay/compaction.
- Collection and ordering differential tests against Redis; safe Rust score conversion
  based on fpconv under Boost 1.0, with license notices preserved.

- Binary AOF with checksums, resolved batches, configurable sync, recovery before
  binding, global compaction, and recoverable rejection of oversized records.
- Shard metadata in AOF v2, a legacy v1 reader, and explicit offline migration
  with `sider-aof-migrate`. Snapshots wait for accepted requests to be applied.
- Crash/migration tests on both platforms and exploratory R04 TCP measurements.

- SUBSCRIBE, UNSUBSCRIBE, PUBLISH, and PING in subscriber mode, with binary channels,
  bounded queues, and cleanup on full queues, timeouts, EOF, cancellation, and shutdown.
  Ephemeral messages remain separate from the dataset and AOF.
- Fixed shards with independent workers and queues, stable binary hashing, and hash tags.
  Multikey commands across shards are rejected before enqueue; the total quota
  is divided among workers and checked again during AOF recovery.
- Additional strings (`EXISTS`, `INCR`, `DECR`, `MGET`, `MSET`) and SET with NX, XX,
  EX, PX, GET, and KEEPTTL, with overflow and rejection causing no partial changes.
- EXPIRE, PEXPIRE, TTL, PTTL, and PERSIST; injectable clock and bounded active cleanup.
- Configurable logical dataset quota, defaulting to 64 MiB, with batch accounting
  based on final state and growth rejection without eviction.
- R02 differential tests against Redis 8.10.1 and time, quota, and TCP limit regressions.
- Local planning, backlog, validation, and artifact integrity tools in Rust,
  accessible through `cargo xtask` and isolated from the server.
- Removal of Python helpers and archived workflows. Publication remains manual;
  CI remains deferred until after 1.0.
- A short cycle with focused tests and separate database/tool checks,
  without running external gates after every edit or duplicating check and Clippy.
- Complete removal of the fuzz infrastructure, dependencies, and gate. Native,
  property, differential, and extracted-package tests remain.
- Internal R01–R10 milestones, technical checkpoints, and explicit dependencies,
  preserving IDs and scope. Only R11 publishes a candidate and final release.
- Artifact manifest v2: the build already uses `1.0.0` in the candidate, and the final
  release promotes the same SHA and files. Identity or hash mismatches are rejected.
- Migration contract for 1.0 from an internal R10 baseline, without intermediate publication.
- Baseline freezing and migration using extracted packages, with observed provenance,
  an immutable inventory, five types, EXEC, TTL during downtime, backup/restoration,
  and recreation of a replica running the same version. The R10 baseline retains package `0.1.0`.

## [0.1.0] - Previous preparation, unpublished

- The previous preparation closed without publication. Milestone 0.1 becomes
  an internal checkpoint; the earlier candidate remains a historical record.
- [Draft final release notes](releases/notes/v0.1.0.md), including the
  Visual C++ v14 Redistributable x64 runtime requirement for the Windows executable.

## [0.1.0-rc.1] - 2026-09-08

- First functional candidate, with Linux GNU and Windows MSVC packages validated
  after extraction, a distribution README, and dependency notices included.
- Independent Sider/Redis differential suite, `redis-cli` integration,
  connection reuse tests, and local Rust gates with verified receipts.
- Isolated fuzz target with AddressSanitizer, a versioned corpus, and a minimum
  900-second run; reproducible local Ubuntu test environment.
- Owning worker and RESP2/TCP server, with bounded queues and connections,
  timeouts, atomic readiness, and supervised shutdown.
- Rust bootstrap with validated configuration and tests.
- Parsing and synchronous storage for `PING`, `ECHO`, `GET`, basic `SET`, and
  `DEL`, with rejection without mutation, binary data, and fixture-verified responses.
- Incremental RESP2 codec and atomic encoder, with limits, binary data,
  literal fixtures, fragmentation, and property tests.
- Binary fixtures and disposable Redis/CLI 8.10.1 infrastructure, tested
  in Rust without depending on the future Sider codec.
- MIT license, keeping the repository and artifacts private.
- Versioned planning, synchronizable backlog, and optional release tools.
- Manual validation and publication through and including 1.0; CI and automatic
  publication deferred until after 1.0.

The [candidate notes](releases/notes/v0.1.0-rc.1.md) describe the subset and its
limitations and preserve the validation policy in effect at that publication.
The RC was published and approved under that revision's policy. Its evidence does
not approve later SHAs; the current workflow publishes only the 1.0 candidate and final release.
