# Sider

Sider is an in-memory database server project written in Rust.
Its goal is to provide an explicit subset of Redis compatibility over RESP2.
The name is Redis spelled backward.

This checkout prepares package `1.0.0`. Publication depends on validating the
candidate build and its files; the [release notes](releases/notes/v1.0.0.md)
describe the scope without replacing gate receipts.

The binary serves strings, multikey operations, `SET` options, and TTL over RESP2/TCP.
Hashes, lists, sets, and sorted sets share TTL, quota, and typed persistence.
Pub/Sub provides binary channels, per-connection subscriptions, and bounded queues.
Single-shard transactions provide `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH`,
with one AOF append per batch and no rollback for individual execution errors.
Owning workers serialize each shard, with bounded queues, connections, and buffers
and a default total logical quota of 64 MiB. Optional AOF provides replay,
global compaction, and offline shard migration.

[Asynchronous Sider → Sider replication](docs/replication.md) transfers snapshots
and durable batches, resumes available history, and allows manual promotion.
Replicas require the same version and shard configuration; Pub/Sub remains local.
[Consistent backup](docs/backup.md) exports data while traffic continues,
with integrity verification and restoration into a new directory.
Authentication, TLS, and automatic failover are not available yet; use a controlled environment.

The [strings guide](docs/strings.md) describes commands and limits.
The [collections](docs/collections.md) and [sorted sets](docs/sorted-sets.md) guides
describe commands in those families and their differential evidence.
The [complete matrix](docs/compatibility-matrix.md) brings together supported forms,
restrictions, and evidence, including transactions and sequences across families.
The differential reference uses Redis and `redis-cli` 8.10.1, pinned in the plan. The
[0.1.0-rc.1 candidate](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
was published in the private repository and remains a historical record.

Deliverables through 1.0 are organized in the [ROADMAP](ROADMAP.md), with 11 milestones
and 50 implementation tasks. Milestones 0.1–0.10 have technical checkpoints;
only 1.0 will have a candidate and final release. The
[release guide](docs/releases.md) describes synchronizing the backlog and preparing
a candidate and final release with the same SHA and approved files.
The [execution plan through v1](docs/execution-to-v1.md) summarizes the current state,
next deliverable, and criteria for each version.

CI and automatic publication are deferred until after 1.0. Through and including 1.0,
development uses local validation and releases are published manually.

`INFO` queries instance metrics and `sider --diagnose` validates configuration
without starting the server. The [operations guide](docs/metrics.md) defines fields,
limits, and procedures for full queues, slow clients, and AOF failures.
The [packages](docs/packages.md) include `sider`, `sider-aof-migrate`, `sider-backup`,
and `sider-replica`. The [private Docker image](docs/docker.md) copies these same
Linux executables without rebuilding the server.
See the [runtime requirements](docs/runtime-requirements.md) to run the packages
on Windows 11 x64 or Ubuntu 24.04 GNU without installing Rust or Cargo.

## Running the server

Install Rust with `rustup`. The toolchain and development components are defined
in [rust-toolchain.toml](rust-toolchain.toml); `rustup` uses that file when running
commands in the project directory.

```sh
git clone https://github.com/djairofilho/sider.git
cd sider
cargo run --locked -- --help
cargo run --locked -- --version
cargo run --locked
```

The repository is private. Cloning requires account access or Git authentication
authorized for the project.

Without arguments, the program validates configuration and opens the TCP listener.
Use Ctrl+C to shut down. In another terminal, a RESP2 client can send the supported
commands. Without `SIDER_AOF_DIR`, data is lost when the process terminates.
With AOF, recovery completes before binding and follows the synchronization policy
described in the [persistence guide](docs/persistence.md).

### Configuration

| Variable | Default | Format |
| --- | --- | --- |
| `SIDER_ADDR` | `127.0.0.1:6379` | IP address and port; IPv6 in brackets |
| `SIDER_READY_FILE` | Unset | New readiness file with PID, IP, and actual port |
| `SIDER_MAX_DATASET_BYTES` | `67108864` | Positive logical quota, distinct from RSS; no eviction |
| `SIDER_SHARDS` | `1` | Between 1 and 256 workers, with divided quota and fixed configuration |
| `SIDER_AOF_DIR` | Unset | Exclusive data directory; enables AOF |
| `SIDER_AOF_SYNC` | `always` | `always` waits for sync per batch; `everysec` synchronizes periodically |
| `SIDER_REPLICATION_ADDR` | Unset | Internal replication, backup, and administration listener; requires AOF |
| `SIDER_REPLICA_OF` | Unset | Upstream IP and port; configures the instance as a replica |
| `SIDER_REPLICATION_READY_FILE` | Unset | Separate readiness file for the internal listener |

The address is validated strictly. Use an IP, such as `127.0.0.1:6380` or
`[::1]:6380`, rather than a hostname. Invalid configuration terminates the program
with an error. Port `0` requests an ephemeral port from the system. The
[networking guide](docs/network.md) lists all `SIDER_*` limits and deadlines,
their defaults, and the readiness file contract.

To try another configuration in PowerShell:

```powershell
$env:SIDER_ADDR = '127.0.0.1:6380'
cargo run --locked
Remove-Item Env:SIDER_ADDR
```

In a POSIX shell:

```sh
SIDER_ADDR=127.0.0.1:6380 cargo run --locked
```

## Checking changes

During implementation, run the affected test. Before integrating, use the local
command that combines formatting, Clippy, the binary build, and native tests:

```sh
cargo test --locked <filter>
cargo xtask check
```

Extend validation according to the change: documentation and distribution builds
when their interfaces or configuration are affected. Do not repeat `cargo check`
after Clippy, which already checks the targets. Preserve the Cargo cache between runs.
The [contribution guide](CONTRIBUTING.md#local-checks) lists the commands.
There is no need to wait for CI to integrate a PR. Record local tests in the PR.

Tests cover the foundation, codec, commands, worker, TCP, and actual binary.
Redis fixtures are replayed over TCP and by the server differential suite.
The [testing guide](docs/testing.md) explains local commands; the
[differential guide](docs/differential.md) covers Redis/CLI and gate receipts.
All new tests use Rust and external tests are opt-in.
Before each release, cumulative gates remain mandatory,
with manual execution and evidence on the required platforms.

## Structure

| Path | Responsibility |
| --- | --- |
| `src/lib.rs` | Library interface |
| `src/main.rs` | Binary entry point, runtime, logging, and shutdown signals |
| `src/config.rs` | Validated address, limits, and deadlines |
| `src/resp/` | Frames, limits, atomic encoder, and incremental decoder |
| `src/command/` and `src/storage/` | Parser, typed replies, and synchronously owned map |
| `src/storage/worker.rs` | Bounded queue, acceptance, and execution by the owner |
| `src/persistence/` and `src/storage/snapshot.rs` | AOF format, writer, replay, and global snapshots |
| `src/bin/sider-aof-migrate.rs` | Explicit offline migration into a new directory |
| `src/replication/` and `src/bin/sider-replica.rs` | Snapshots, history, resumption, and replica administration |
| `src/persistence/backup/` and `src/bin/sider-backup.rs` | Export, manifest, verification, and restoration |
| `src/server.rs` and `src/connection.rs` | TCP, ordering, timeouts, and supervision |
| `src/readiness.rs` | Atomic readiness file publication |
| `src/error.rs` | Typed configuration errors |
| `tests/cli.rs` | Binary integration tests |
| `tests/reference.rs` and `tests/common/` | Binary fixtures and disposable Redis reference |
| `tests/resp_codec.rs` | Literal fixtures, fragmentation, and codec property tests |
| `tests/commands.rs` | Five-command semantics and rejection without mutation |
| `tests/tcp.rs` | TCP fixtures, pipelines, fragmentation, and connection lifecycle |
| `tests/compatibility.rs` | Independent Sider/Redis comparison and redis-cli integration |
| `tests/gate_contract.rs` and `tests/harness.rs` | Evidence, isolation, and test infrastructure failures |
| `dev/test.Dockerfile` | Local Ubuntu test environment, distinct from the distribution image |
| `Cargo.toml` and `Cargo.lock` | Rust package and pinned dependencies |
| `rust-toolchain.toml` | Toolchain and development components |
| `xtask/` and `.cargo/config.toml` | Local Rust tools, isolated from database dependencies |
| `AGENTS.md` | Local instructions for coding agents |
| [PLAN.md](PLAN.md) | Historical record of the initial 0.1 design and stages |
| [ROADMAP.md](ROADMAP.md) | Release sequence and dependencies through 1.0 |
| [releases/plan.json](releases/plan.json) | Versioned source for milestones, tasks, and criteria |
| [docs/releases.md](docs/releases.md) | Backlog execution, candidates, publication, and recovery |
| [docs/architecture.md](docs/architecture.md) | Current boundaries and planned architecture |
| [docs/compatibility.md](docs/compatibility.md) | Compatibility scope and status |
| [docs/compatibility-matrix.md](docs/compatibility-matrix.md) | Supported forms, restrictions, and evidence by capability |
| [docs/testing.md](docs/testing.md) | Local tests and Redis reference reproduction |
| [docs/resp.md](docs/resp.md) | RESP2 codec contracts, limits, and usage |
| [docs/network.md](docs/network.md) | TCP configuration, acceptance, timeouts, and shutdown |
| [docs/pubsub.md](docs/pubsub.md) | Binary channels, subscriber mode, bounded queues, and Pub/Sub differential tests |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Workflow and review criteria |

## Next step

Strings, TTL, collections, [transactions](docs/transactions.md),
Pub/Sub, AOF, shards, replication, backup, and administration are integrated.
Stabilization brings together the internal R10 baseline, migration between
executables, the compatibility audit, [soak testing](docs/soak.md), and
[benchmarks](docs/benchmarks.md). Development results remain tied to their SHAs;
the presence of runners does not mean gates have passed.

The preparation PR sets the package version to `1.0.0`.
The exact merge SHA will be built, packaged, and validated on both platforms
to publish the candidate. The final release will promote the same approved files.
The [0.1 plan](PLAN.md) preserves the historical design; the
[ROADMAP](ROADMAP.md) and issues record criteria and current progress.

The database, tests, and project tools use Rust. The project has no Python scripts
or CI workflows. When changing the tools or manifest, use:

```sh
cargo xtask check --tools
cargo xtask roadmap --write
cargo xtask sync
```

`sync` simulates; only `sync --apply` writes to GitHub through the authenticated `gh` CLI.
The [release guide](docs/releases.md) separates the fast development cycle from publication tests.
Publishing 1.0 requires a candidate and evidence for every capability.
Pending gates block publication, even when bootstrap tests pass.

## License

Sider's own code uses the [MIT license](LICENSE), with the standard text from the
[Open Source Initiative](https://opensource.org/license/mit).
Dependencies retain their own licenses.

Distributed packages include the `LICENSE` file. Adopting this license does not change
the repository's private visibility or enable publication to crates.io.
