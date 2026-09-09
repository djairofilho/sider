# Accelerated execution through v1.0

The full v1 functional scope remains intact. Milestones `R01` through `R10` are
internal; only `R11` publishes a candidate and final release. The final release
promotes exactly the SHA and files approved in the candidate. Fuzz remains removed,
and CI and automatic publication remain disabled.

IDs, dependencies, and criteria are in [releases/plan.json](../releases/plan.json).
The [ROADMAP](../ROADMAP.md) is generated; issues record operational status.
The [release guide](releases.md) defines evidence and publication contracts.

The preparation PR identifies the package as `1.0.0`. The selected source for
the internal R10 baseline is SHA `0021d875dde9da6cbbe9b5b84cd640681128e6ea`, still
with package `0.1.0`. Its files, hashes, and tests are recorded separately;
migration and the other candidate gates must run at the preparation merge SHA.
The version bump does not close these criteria.

## Internal checkpoints

- Preserve existing IDs, issues, and milestones. The `publication` field distinguishes
  internal milestones from publishable releases.
- Close `R01-GATE` through `R10-GATE` after completing milestone implementation and
  validation, without requiring a candidate, packaging, or publication for the checkpoint.
- Record results by task ID and source SHA. Do not change the package version at
  each checkpoint or attribute historical results to another SHA.
- Complete `R01-GATE` with the already verified deliverables. The historical
  [v0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
  candidate remains intact; this workflow will have no other 0.1 candidate or final release.
- Each checkpoint depends on local tasks. `R11-GATE` depends on all internal
  checkpoints and final tasks.

## Execution by technical dependencies

| Stage | Work | Completion condition |
| --- | --- | --- |
| Foundation | R02: strings, SET options, TTL, and quota | Verified semantics, overflow, expiration, and rejection without mutation |
| Workstream A | R03: AOF; then R04: shards and durable integration | Verified replay, compaction, write failures, and cross-shard operations |
| Workstream B | R05: typed values and collections; then R06: sorted sets | Integrated commands, WRONGTYPE, TTL, quota, and persistence |
| Workstream C | R08: Pub/Sub; then R07: transactions | Isolated slow clients; WATCH and atomic batch execution/persistence |
| Final integration | R09: replication, alongside R10: operations and distribution | Working snapshots, resumption, manual promotion, backup/restoration, and packages |
| Stabilization | R11: audit, migration, load, and benchmarks | Full scope demonstrated in the candidate build |

JSON order does not create dependencies. `xtask` validates references and cycles,
but considers only explicit technical links; it does not inject the previous gate.

One integrator coordinates interfaces and merges; up to three agents work in
separate worktrees. Collections start after TTL/quota and their durable integration
waits for AOF format and replay. Pub/Sub can use the current networking layer.
Transactions wait for routing/execution per worker and, to complete, durable batches.
Replication needs mutation sequences, snapshots, and shards; its approval covers
all types and transactions.

Metrics and diagnostics accompany their subsystems. Backup depends on consistent
snapshots; complete operational tests wait for integration. The integrator owns
shared contracts: arrays/errors, entries with TTL and accounting, resolved
mutations, batches, and connection modes. Every abstraction has a real consumer.

Use PRs for cohesive deliverables, potentially closing several related issues.
Keep commits small by responsibility, with implementation and required tests
together; integrate through a merge commit after local validation.

## Proportionate validation

| When | Validation |
| --- | --- |
| During implementation | Focused tests for changed behavior |
| Before integrating each database PR | One `cargo xtask check` run on the final diff |
| Tools/plan | `cargo xtask check --tools` |
| New commands or semantics | Affected-family differential tests against the pinned reference |
| Persistence, shards, and transactions | Affected failure, replay, migration, and atomicity checks; Windows/Linux for filesystem behavior |
| Replication and operations | Interrupted snapshots, sequences, reconnection, TTL, batches, backup, and restoration |
| 1.0 candidate | Complete matrix, extracted packages, Docker, migration, 3600-second soak, and benchmarks |

Run an integrated suite when completing the durable core with shards and
transactions. The next complete suite will run on the 1.0 candidate build.
Internal tests call suites directly and record task/SHA, without requiring
release receipts or packages. Implement future runners alongside their capabilities.

Documentation-only changes receive text, link, and command review. Interfaces and
build changes also require `cargo doc --locked --no-deps` and
`cargo build --locked --release`. Preserve caches and do not repeat passed checks
without relevant changes. Benchmarks run without concurrent builds or load on
the same machine. A missing, skipped, or failed gate remains pending.

## 1.0 baseline and publication

1. Freeze an internal R10 baseline with executable, data, configuration, AOF format,
   SHA, and hashes. It replaces the requirement for a published 0.10 final release
   in the migration test to 1.0.
2. Complete documentation/notes, set the package to `1.0.0`, and integrate the
   preparation PR. Build and validate its exact SHA.
3. Produce packages and receipts with version `1.0.0`. Artifact manifest v2 uses
   `artifact_version: "1.0.0"`; the GitHub tag and status identify `v1.0.0-rc.N`.
4. Publish the private candidate, verify downloads, and record approval.
5. Create the final tag at the same SHA and publish the same files, including
   manifest, evidence, and checksums. Do not rebuild, repackage, or repeat the soak.
6. Record promotion verification outside the immutable set. Any change to that
   set requires another candidate.

The verifier accepts RC and final release for the same build and rejects version,
SHA, and hash mismatches. Checking byte provenance and remote approval remains
part of manual publication, not authorization inferred from the local verifier.

Execution ends when all capabilities are demonstrated, final 1.0 is published,
and its files are verified. Repository and artifacts remain private;
`publish = false`, Rust, binary RESP2, owning workers, bounded channels,
and Linux/Windows support remain mandatory.

## Out of scope through 1.0

Redis Cluster, Sentinel, automatic failover, online resharding, RESP3, Lua,
blocking operations, cross-shard transactions, TLS, and ACL. Replication is
asynchronous between Sider instances with the same version and shard configuration,
with manual promotion in a controlled environment.
