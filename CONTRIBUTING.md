# Contributing to Sider

Development follows the [ROADMAP](ROADMAP.md), backed by
[releases/plan.json](releases/plan.json). The 0.1 contracts are in
[PLAN.md](PLAN.md). Each deliverable must compile, pass the available checks,
and document its actual behavior. A stage ends only when its exit criteria
have been verified.

## Preparing the environment

Use `rustup` with the toolchain declared in
[rust-toolchain.toml](rust-toolchain.toml). Run Cargo commands at the repository root and
preserve `Cargo.lock`, since the project distributes a binary. Update dependencies
deliberately and review lockfile changes.

Native tests do not require Redis, Docker, or a running external service.
Differential tests have separate instructions and must be run explicitly.

Development, tests, and project tools use Rust. The `xtask/` utility has its own
manifest and lockfile, without adding dependencies to the server.
GitHub operations use `gh` authenticated with an account that can manage the public repository.

CI and automatic publication are deferred until after 1.0. The workflows and Python
helpers were removed. Future CI must call the existing Rust commands.

## Workflow

1. Select an issue unblocked by its technical dependencies, update `main`, and create
   a branch with a clear responsibility, such as `feat/resp2-decoder` or
   `docs/compatibility-matrix`.
2. Implement a reviewable part of the stage, with the tests needed to establish
   its behavior. Create modules as real consumers arise.
3. Run local checks and inspect the diff, including new files.
4. Organize atomic commits. Each commit must leave the project compiling and
   passing the available checks.
5. Open a pull request to `main` linked to the issue, describing the problem,
   resulting behavior, tests run, and any known limitations.
6. Integrate with a merge commit after local validation, without waiting for CI.
   Update the compatibility matrix and notes as each capability is demonstrated.

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

```text
feat(config): validate the initial server address
test(resp): cover fragmented bulk strings
docs(compatibility): record supported command forms
```

Separate independent responsibilities when each part can be reviewed and reverted
on its own. Keep code together with its tests and required generated artifacts.
Pull requests use merge commits by default.

## Local checks

During implementation, use the affected test; before integrating, run the local check:

```sh
cargo test --locked <filter>
cargo xtask check
```

`check` runs fmt, Clippy, the actual binary build, and native tests, stopping at
the first failure. Clippy already checks the targets, without another redundant
`cargo check`. It does not start Docker or publish anything. Preserve Cargo caches;
do not clear `target/` as a routine part of each task. Extend validation according to scope:

```sh
# Interfaces, API documentation, or build configuration
cargo doc --locked --no-deps
cargo build --locked --release

# Only when changing the manifest or backlog/release tools
cargo xtask check --tools
```

`check --tools` tests only the utility and validates the plan and roadmap; it does
not repeat the database suite. Documentation-only changes require reviewing text,
links, and commands, without rerunning all tests when code has not changed.

There is no automatic Linux or Windows execution at this stage. Before publishing
a release, manually run every required gate on the platforms in the manifest.
A pass on one platform does not replace the other. Record commands, SHA,
environment, results, and pending tests in the PR or publication issue.
A missing tool or an unexecuted test does not count as a pass.

## Tests and boundaries

Start with the smallest unit that exposes the behavior: configuration without
networking, codec without storage, and storage without a runtime. Use literal
fixtures when checking protocol bytes.

In configuration tests, pass values explicitly to the parser or run the binary in
a child process with its own environment. Do not modify the test process's global
environment with `std::env::set_var` or `std::env::remove_var`: this interferes
with parallel tests and requires `unsafe` in Edition 2024.

In TCP tests, use ephemeral ports and explicit synchronization. Do not depend on
a fixed port being available or arbitrary sleeps to coordinate tasks.
Redis comparisons must use disposable instances and a reference version recorded
in the compatibility matrix.

## Code and documentation

Project code forbids `unsafe`. Handle invalid input with typed errors and avoid
panics in the client data path. Introduce dependencies when they have concrete
uses and enable only the required features.

Write documentation in English with correct spelling and punctuation. Preserve
identifiers, paths, and commands. Before submitting a change, check the text for
characters corrupted by encoding errors.

Update [docs/architecture.md](docs/architecture.md) when a boundary changes and
[docs/compatibility.md](docs/compatibility.md) when there is new evidence of
compatible behavior. Label planned functionality as planned; do not mark an entire
stage complete after implementing only part of it.

## Backlog and releases

Edit the manifest and regenerate its projection with `cargo xtask roadmap --write`.
IDs such as `R01-01` are stable and must not be reused. Milestone dates are optional.
R01–R10 are internal milestones with technical checkpoints and evidence by task/SHA.
Only R11 publishes a candidate and final release in this workflow.

The default mode displays the planned synchronization; `--apply` writes to GitHub:

```sh
cargo xtask sync
cargo xtask sync --apply
```

When all functional tasks are complete, prepare the UTF-8 notes and 1.0 candidate
according to the [release guide](docs/releases.md). Merging the `chore/release-v1.0.0`
PR, labeled `type:release`, publishes nothing. The package already uses `1.0.0`
in the candidate. The final release promotes the same SHA and approved files,
without rebuilding, repackaging, or rerunning gates. A bundle change requires a new RC.

A required test that is missing, skipped, canceled, or lacks evidence blocks the release.
No functional release is created for the bootstrap. Internal milestones close after
their technical criteria have been validated. The 1.0 milestone closes after verifying
the final publication, public Linux/Windows packages, and exported Docker image.
Crate publication remains disabled.
