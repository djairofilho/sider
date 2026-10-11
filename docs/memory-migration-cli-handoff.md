# Explicit memory migration CLI handoff

## Integration boundary

This stage is stacked on `feat/memory-offline-migration` at `b025718`.
The previous backup and native migration stages remain separate. No merge or
release approval is claimed for this stage.

The frozen migration contract is `docs/agent-memory-v1.md`. This slice exposes
the existing verified native migration through two explicit CLI options. It
does not activate a memory server profile or change transport, security,
writer, recovery, compaction or backup behavior.

## Exact changed paths

| Path | Reviewable change |
| --- | --- |
| `src/persistence/migration.rs` | Pure parser accepts source/destination record-version options, defaults each to 1, restricts both to 1 or 2 and rejects source 2 to destination 1 before source I/O. |
| `src/bin/sider-aof-migrate.rs` | Explicit destination record 2 dispatches to the existing memory migration API; help and memory JSON report expose the profile. The original legacy JSON output template is preserved exactly. |
| `src/persistence/migration/memory.rs` | Module comment now identifies the explicit CLI consumer; no executable code changes. |
| `tests/agent_memory_migration_cli.rs` | New bounded real-process tests, plus a separate ignored Windows gate for checksum-pinned published old binaries. |
| `docs/aof-migration.md` | Documents both optional flags and retained legacy default behavior. |
| `docs/agent-memory-migration.md` | Documents explicit upgrade/rehash, report fields and separate CLI validation evidence. |
| `docs/memory-migration-cli-handoff.md` | This integration receipt and proposed atomic commit plan. |

There are no dependency changes. The implementation paths overlap the previous
migration branch only; no security/internal replication files changed.

## Behavior and tests

- Legacy headers 1/2/3 explicitly upgrade to a sealed header-4/record-2 destination.
- Memory headers 4/5 require explicit source record 2 and destination record 2.
- Omitting record-version flags uses the original record-1 migrator and exact report.
- Source record 2 with default or explicit destination record 1 rejects before I/O.
- The process matrix checks exact binary images, vector bits, typed metadata,
  provenance, absolute outer/member deadlines, guarded Persist history, empty
  schemas, retained elapsed memory and omitted expired legacy values.
- Four-shard rehash and source/destination SHA-256 are checked against actual bytes.
- Active source lock, existing destination, quota, record limit, wrong source
  profile and complete-record corruption refuse without source repair or output.
- Incomplete source tails are reported and hashed without truncation.
- Child processes have bounded waits/output, own their cleanup and run serially.

The optional Windows compatibility gate launches the original v1.0.1 server and
migrator against the intact migrated file and every first-record prefix. It
checks version refusal and source files, characterizing the old server's
historical install-residue cleanup separately. Missing artifacts fail the gate;
an empty selection is not a pass. Published package acquisition and checksum
pins are recorded in `agent-memory-migration.md`. Artifact acquisition alone
does not establish compatibility.

## Validation receipt

The focused Windows checks used a shared Cargo target cache under the exclusive
Cargo slot. The complete `cargo xtask check` was then run in this stage's
worktree. The main checkout was not modified.

| Check | Observed result |
| --- | --- |
| `cargo fmt --all` | Passed; final source formatting preceded the final test/Clippy checks. |
| CLI/process normal selection | 9 passed, 1 external gate ignored; final rerun 0.85 seconds (five CLI cases and four helper cases). |
| Native memory migration | 9/9 passed, 2.33 seconds. |
| Legacy migration | 14/14 passed, 0.60 seconds. |
| Published v1.0.1 Windows binaries | Separate gate 1/1 passed, 12.68 seconds; 287 variants and 574 historical subprocesses. |
| All-target Clippy, `-D warnings` | Passed, 18.40 seconds. |
| `cargo doc --locked --no-deps` | Passed, 8.58 seconds. |
| `cargo build --locked --release` | Passed, 54.83 seconds. |
| `cargo xtask check` | Passed: format, all-target Clippy, debug binary, all non-ignored native tests and doctest. |
| `git diff --check` | Passed; final UTF-8/mojibake scan also passed. |

No functional test-first RED/GREEN evidence has been recorded for the CLI.
The original external gate did fail a real preservation assertion in 1.06 seconds
at the complete 36-byte header: the published v1.0.1 server removed
`replication-install-preserved.tmp` before refusing header 4. The old migrator
preserved every file. Source inspection confirms the historical server's cleanup
occurs before header validation. With the integrator's approval, the final gate
characterizes precisely that deletion in every variant while requiring all AOF
generation bytes, the lock and an unrelated sentinel unchanged. This is not
blanket directory-preservation evidence for the old server; the limitation cannot
be fixed in already published executables. The current migration CLI's own
full source-preservation assertions passed on success and refusal paths.

The completed bounded sequence was:

```text
cargo fmt --all
cargo test --locked --test agent_memory_migration_cli -- --test-threads=1
cargo test --locked --test aof_migration --test agent_memory_migration -- --test-threads=1
cargo test --locked --test agent_memory_migration_cli -- --ignored --exact published_old_binaries_refuse_all_first_record_prefixes_preserving_aof_and_characterizing_residue_cleanup --nocapture --test-threads=1
cargo clippy --locked --all-targets -- -D warnings
cargo doc --locked --no-deps
cargo build --locked --release
```

Prior library receipts apply to the preceding native implementation, not this
CLI. The published old-binary gate above was selected separately and is not
part of the default `cargo xtask check` selection.

## Remaining issue #110 gates

This is not completion of issue #110. Transport-2 live backup and replication,
memory full install/promotion, lifecycle allocation/work counters, Linux
filesystem and frozen old-binary evidence, forced-termination/power-loss
matrices, explicit public server profile activation and cumulative release
gates remain separate work. The Windows old-binary gate passed for version refusal
and AOF/lock/unowned-file preservation, with the historical residue exception above.

## Atomic commit structure

Four responsibility groups changed: parser/profile contract, CLI consumer,
tests and documentation. The stage uses two commits.

1. `feat(migration): add explicit memory record flags to cli`
   - Parser, CLI, module comment and required process tests stay together because
     the tests verify the same explicit upgrade/refusal interface.
   - Depends on native migration at `b025718`; preserves a working legacy default.
2. `docs(memory): document explicit cli upgrade and compatibility`
   - Both guides and this receipt describe the validated interface and its gates.
   - Depends on the first commit. Documentation can be reverted independently.

The tests were prepared alongside the implementation while Cargo was reserved.
They do not establish a test-first functional RED history; no RED commit is proposed.
