# Local development and releases

The database, tests, and project tools use Rust. CI and automatic publication are
deferred until after 1.0. The `gh` CLI performs manual GitHub operations; there is
no other publisher or scripting framework. The repository and artifacts remain
private, with `publish = false`.

## Short development cycle

| Stage | Command or evidence |
| --- | --- |
| During implementation | `cargo test --locked <filtro>` |
| Before integrating each database PR | One run of `cargo xtask check` against the final diff |
| Tools or plan changed | `cargo xtask check --tools` |
| Command semantics changed | Differential tests for the affected family against the pinned reference |
| Filesystem, persistence, or atomicity changed | Relevant failure/replay/migration tests on Linux and Windows |
| Durable core with shards and transactions integrated | Integrated suite of available subsystems |
| 1.0 candidate build frozen | Full gate matrix, extracted packages, Docker image, and evidence |

Clippy already checks the targets; do not duplicate it with `cargo check`. Checks
stop at the first failure and do not start Docker or external tests. Documentation-only
changes require reviewing text, links, and commands. Interfaces and build changes also
require `cargo doc --locked --no-deps` and `cargo build --locked --release`.

Preserve Cargo caches and do not repeat successful checks without relevant changes.
Internal tests call suites directly, recording the task ID and SHA, without requiring
packaging or release receipts. An ignored test does not count as approval.
Benchmarks must not compete with builds or other loads on the same machine.

## Rust tooling and backlog

`cargo xtask` uses the package in [xtask/](../xtask/), with its own manifest and
lockfile. Tooling dependencies do not enter the server. Run from the repository root:

```sh
cargo xtask --help
cargo xtask validate
cargo xtask roadmap
cargo xtask roadmap --write
cargo xtask sync
cargo xtask sync --json
cargo xtask sync --apply
```

[releases/plan.json](../releases/plan.json), schema 2, preserves the 11 milestones,
50 tasks, and their IDs. `publication: false` in R01–R10 defines internal checkpoints;
only R11 is publishable. [ROADMAP.md](../ROADMAP.md) is generated.
Operational state lives in issues. Do not change the package version for each checkpoint.

The DAG uses explicit technical dependencies, regardless of position in the JSON.
Each checkpoint depends on the tasks in its own milestone; the publication gate also
depends on every internal checkpoint. Invalid references and cycles fail validation.
No task automatically inherits the previous version's gate.

Choose unblocked tasks, implement them in separate worktrees, and integrate cohesive
PRs through merge commits after local validation. Up to three workstreams may implement
in parallel, with one integrator responsible for shared contracts.
Functional PRs may close several related issues. Internal checkpoints close after
checking criteria and source evidence, without a candidate or publication.
`R11-GATE` closes only after the final release is published and verified.

`validate` checks the plan, gates, and roadmap. Future runners with `command: null`
are valid pending work, never passed results. Implement them alongside their features.
`roadmap` writes only with `--write`.

`sync` performs a dry run by default; `--json` shows complete bodies, and `--apply`
allows writes. Review the dry run before applying. The `sider:task` and
`sider:managed` markers are stable; text outside the managed block, human comments,
unmanaged labels, and existing states are preserved. Duplicate or ambiguous IDs
stop synchronization before any writes. The synchronizer does not close issues or
milestones. After applying, a new dry run should show zero changes.
The client uses `gh api`, separate arguments, and UTF-8 JSON, without extracting tokens.

## History and internal baseline

[v0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
remains a historical publication. Preparation of final 0.1 ended without publication;
there will be no further 0.1 candidate in this workflow. Earlier tags, notes, and
evidence remain tied to their SHAs and the policy in force at the time.
Do not transfer old results to the current SHA or rewrite historical assets.

`R01-GATE` may use already recorded deliveries and validations, identifying their
source SHAs. Completing this technical checkpoint does not approve a new build for
publication. Verify historical bundles using the policy at their SHA.

At R10, freeze an internal baseline with an executable, data of every type, TTL and
transactions, configuration, AOF format, toolchain, SHA, and hashes. Preserve the
files and the backup/restore procedure. The 1.0 migration gate starts from this
baseline, without requiring a published final 0.10 release. Also validate initial
format fixtures, corruption, and unknown versions under the AOF contracts.

The [R10 baseline runbook](internal-baseline.md) defines the observational build
sidecar, freeze inputs, and migration using extracted packages. Preserve the external
hash of `baseline.json` and never start a server on frozen data directories.
A same-version test does not replace the candidate gate.

## Preparing the 1.0 candidate build

Complete scope, documentation, and notes before building the candidate. Update
`main` and tags and use branch `chore/release-v1.0.0`. Pin both the server version
in `Cargo.toml` and its entry in `Cargo.lock` to `1.0.0`; update the changelog and
`releases/notes/v1.0.0.md`. The `sider-xtask` package is independent.
Check `cargo metadata --locked --format-version 1` and preserve `publish = false`.

Open the preparation PR in the repository targeting `main`, with `type:release`
and a reference to the gate, without closing it. Integrate through a merge commit
and record its full SHA. The merge does not publish anything. Build and validate
that exact SHA, without substituting a later `main`. Use a `CARGO_TARGET_DIR`
isolated by SHA and platform for distributed binaries; download and image caches
may be preserved.

Packages, binaries, and receipts use `1.0.0` starting with the candidate. The RC
number is a publication identity, not an executable version. Any change to the SHA
or approved file set requires a new candidate.

## Product gates

[releases/gates.json](../releases/gates.json) defines commands as argument arrays
without a shell. Each internal checkpoint validates the affected capabilities;
the 1.0 candidate requires the full matrix below. A missing, canceled, ignored,
failed gate, or one with no positive cases, blocks publication.

| Gate | Required platform for the 1.0 candidate |
| --- | --- |
| `native`, `tcp_smoke` | Linux GNU x86_64 and Windows MSVC x86_64 |
| `crash`, `recovery`, `migration` | Linux GNU x86_64 and Windows MSVC x86_64 |
| `compatibility`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication` | Linux GNU x86_64 |
| `docker`, `soak`, `benchmarks` | Linux GNU x86_64 |

The runner receives `SIDER_REFERENCE_IMAGE`, `SIDER_RELEASE_VERSION=1.0.0`,
`SIDER_RELEASE_SHA`, `SIDER_RELEASE_TARGET`, and `SIDER_RELEASE_DIR`.
The evidence directory must be new. Schema 1 receipts record `version: "1.0.0"`,
SHA, target, `status: "success"`, a positive case count, and actual details.
Do not fabricate results or reuse receipts from another SHA.

The soak lasts at least 3600 seconds. Benchmarks record throughput, p50/p95/p99,
memory, pipelines, hot keys, and shard counts with reproducible configuration.
Redis and `redis-cli` use the version/digest pinned in the plan. Normalize only
responses with no guaranteed order. See [differential tests](differential.md)
and [testing](testing.md).

The smoke test runs the extracted binary with `SIDER_ADDR=127.0.0.1:0` and
`SIDER_READY_FILE`; it checks PID, loopback, port, `--version`, PING, and TCP
operations. The server publishes readiness atomically. The [Docker image](docker.md)
must run, be exported, and be restored; no dummy file counts as a tested image.
The runner copies the four executables from the validated Linux package and preserves
hashes and Image ID before and after `docker load`.

## Immutable file set

Follow the [package guide](packages.md): Linux GNU x86_64 in `.tar.gz`, built on
Ubuntu 24.04, and Windows MSVC x86_64 in `.zip`, with the binary, README, MIT license,
and notices from [releases/licenses/](../releases/licenses/). Include the exported
Docker image, notes, requirements, manifest, evidence, and `SHA256SUMS`.

The artifact manifest uses schema 2 and `artifact_version: "1.0.0"`. Names use
`sider-v1.0.0-*` in both RC and final releases. Receipts and the local preflight
report belong to this set and are not rewritten during promotion.
The tag, prerelease status, and candidate approval live on GitHub or in a record
outside the set. Do not insert `tag`, `prerelease`, or `approved_candidate`
into the immutable manifest.

```sh
cargo xtask verify-release 1.0.0-rc.1 <SHA_COMPLETO> <DIRETORIO_DOS_ASSETS>
cargo xtask verify-release 1.0.0 <SHA_COMPLETO> <DIRETORIO_DOS_ASSETS>
```

Both identifiers accept the same build. The verifier checks names, sizes, SHA-256,
receipt version/SHA, the gate matrix, and the evidence ZIP. It rejects internal
milestone publications and mismatched identities. It does not execute binaries or
confirm byte provenance, remote approval, or publication authorization; its result
retains `publication_authorized: false`.

## Publishing the candidate and promoting the final release

1. Confirm the repository is private, the preparation PR is merged, the label and
   SHA are correct, tasks are complete, and the full matrix passed on the frozen build.
2. Check the local bundle with the RC identifier. Create tag `v1.0.0-rc.N` at the
   approved SHA, respecting signing configuration, and create a private draft.
3. Upload files without `--clobber`. Download everything into a new directory,
   verify the bundle, and compare hashes with the originals and GitHub's digests.
4. Publish the candidate as a prerelease, never latest. Check the published release
   again and record approval with tag, SHA, and hashes outside the immutable assets.
5. To promote, confirm approval and create `v1.0.0` at the same SHA. Publish the
   same files downloaded from the candidate. Do not perform another merge, bump,
   build, packaging run, or soak. The final identifier must pass the same verifier.
6. Compare final downloads with the candidate's, including manifest, evidence, and
   checksums. Record promotion outside the asset directory. Only then close
   `R11-GATE` and the 1.0 milestone.

Any file change requires a new candidate and validation of the affected build.
Do not move tags or overwrite existing publications. Keep one publication in progress
at a time; after failure or a lost response, reread remote state before retrying.
A draft can be resumed. Failure to comment or close a milestone calls for backlog
reconciliation, not rebuilding or republishing.

The [MIT license](../LICENSE) does not make the repository or artifacts public.
The crate is not published to crates.io; the image is a private asset, with no public
registry. Future CI requires an explicit request and must reuse existing Rust
commands, without automatic activation when 1.0 is released.
