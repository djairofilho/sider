# Sider testing

Database and reference tests are written in Rust. Python is not required for the
commands in this document. CI remains disabled through 1.0.

## Contents

- [Local cycle](#local-cycle)
- [Standalone codec](#standalone-codec)
- [Commands without networking](#commands-without-networking)
- [Worker, TCP, and binary](#worker-tcp-and-binary)
- [Differential testing and robustness](#differential-testing-and-robustness)
- [Integrated capability suites](#integrated-capability-suites)
- [Packages and stabilization](#packages-and-stabilization)
- [Disposable Redis reference](#disposable-redis-reference)
- [What the fixtures cover](#what-the-fixtures-cover)
- [Recorded runs](#recorded-runs)
- [Limits of this evidence](#limits-of-this-evidence)

## Local cycle

From the repository root:

```sh
cargo test --locked <filtro>
cargo xtask check
```

Use focused tests during implementation. Before integration, the check runs
formatting, Clippy, the binary build, and native tests once. Clippy already checks
the targets; do not repeat `cargo check` in the same sequence.

External entrypoints appear as `ignored` in the normal cycle. This does not mean
the reference, package, or any gate has passed. Explicitly run the relevant
entrypoint and check its cases, results, and environment requirements.

## Standalone codec

The R01-02 codec has unit and integration tests without Docker:

```sh
cargo test --locked --lib resp::
cargo test --locked --test resp_codec
```

The suite covers the five types, literal fixtures, fragmentation, concatenation,
limits, and invalid input. Four properties run 512 cases each, including valid
trees and arbitrary bytes. The unit tests' work counter checks linear growth when
fragmenting headers and arrays. See the [codec contracts](resp.md).

## Commands without networking

```sh
cargo test --locked --lib command::
cargo test --locked --lib storage::
cargo test --locked --test commands
```

R01-03 reproduces the Redis fixtures' eight cases and 48 command exchanges in the
Sider core. Each case is repeated to check its final state. Tests also verify arity,
error classification, `SET` options rejected without effects, and immutable payload
sharing. There are no sockets or runtime.

## Worker, TCP, and binary

```sh
cargo test --locked --lib storage::worker::
cargo test --locked --lib connection::
cargo test --locked --lib server::
cargo test --locked --lib readiness::
cargo test --locked --test tcp
cargo test --locked --test cli
```

R01-04 reproduces fixtures over the network using ephemeral ports, literal responses,
pipelines, fragmentation, concurrent clients, half-close, and truncation.
The real binary publishes readiness after binding and serves binary commands.
Unix tests also send `SIGTERM` only to the child process created by the test.
On Windows, draining is tested through the server API without sending signals to
the runner's shared console.

Worker and connection tests use channels, controlled I/O, explicit polling, and
a paused clock to demonstrate backpressure, the acceptance boundary, and total
deadlines. Waiting for the readiness file polls a live child process with a deadline;
it does not use an arbitrary pause to determine command order.
The [network guide](network.md) details the contracts being verified.

## Differential testing and robustness

```sh
cargo test --locked --test compatibility --test harness --test gate_contract
cargo test --locked --test robustness
```

These commands cover the independent response reader, sequence generation, disposable
processes, gate receipts, and reuse of all connection slots after waves of
disconnections or slow frames. They do not measure dataset quota or prove the
general absence of leaks.

[External differential tests](differential.md) send the same bytes to Redis and Sider,
compare types and complete responses, and observe the final key state. The shared
Linux path also runs `redis-cli` against Sider. Missing Docker or images cause
failure; these entrypoints do not run implicitly in the regular suite.

External runners write receipts only after success and clean-checkout validation.

## Integrated capability suites

Choose the suites affected by the change. The table lists tests and guides
containing contracts, opt-in commands, and evidence limitations:

| Capability | Rust suites | Guide |
| --- | --- | --- |
| Strings, TTL, and quota | `strings`, `expiration`, `memory` | [Strings](strings.md) |
| AOF, crash, and recovery | `persistence`, `aof_migration` | [Persistence](persistence.md), [migration](aof-migration.md) |
| Shards and global snapshots | `sharding`, `storage::snapshot` and `storage::worker` modules | [Shards](sharding.md) |
| Hashes, lists, sets, and sorted sets | `collections`, `sorted_sets`, `collections_differential` | [Collections](collections.md), [ordering](sorted-sets.md), [typed AOF](types-persistence.md) |
| Transactions and WATCH | `transactions`, `transactions_persistence` | [Transactions](transactions.md) |
| Subscriptions and publications | `pubsub` | [Pub/Sub](pubsub.md) |
| Replication, resumption, and promotion | `replication_protocol`, `replication_journal`, `replication_storage`, `replication_persistence`, `replication_network` | [Replication](replication.md) |
| Backup during traffic and restore | `backup`, `backup_process` | [Backup](backup.md) |
| INFO and diagnostics | `metrics`, `cli` | [Operations](metrics.md) |
| Cross-family sequences | `compatibility`, `common/cross_family.rs` helper | [Matrix](compatibility-matrix.md), [differential tests](differential.md#r11-cross-family-audit) |

For example, a backup change can be checked with:

```sh
cargo test --locked --test backup --test backup_process
```

The new cross-family audit corpus can also run independently, with the reference
and network prepared as described in the differential testing guide:

```sh
cargo test --locked --test compatibility -- --ignored --exact cross_family_audit_only --nocapture
```

It preserves historical R01/R02 counts and separately records cases across types,
TTL, transactions, and Pub/Sub. Persistence tests must run natively on Windows
MSVC and Linux GNU when filesystem behavior or durability changes.

## Packages and stabilization

[Package smoke tests](packages.md) run all four executables actually extracted
from the packages. The [Docker test](docker.md) verifies image construction without
recompilation, an unprivileged user, TCP, AOF/restart, signals, and the image after
`save`/compression/`load`. The [package backup test](backup.md#reproducible-evidence)
exercises export and restore under traffic; it is not limited to the development binary.

The [soak](soak.md) sustains load for at least 3600 seconds and observes invariants,
TTL, slow clients, compaction, and replication recovery. A short run checks the
runner and does not approve the duration gate. [Benchmarks](benchmarks.md) measure
the extracted package using fixed scenarios and repetitions, latency/RSS samples,
and recorded configuration. Compile the harness before reserving the machine and
do not run benchmarks alongside builds or other loads.

The candidate uses [20 gate receipts](releases.md#product-gates), distributed across
the two platforms, plus packages and their hashes. The 1.0 migration must consume
the preserved R10 internal baseline; old-format fixtures remain supplementary.
Development results are not transferred to another SHA. The final release promotes
the same files approved in the RC.

## Disposable Redis reference

Requirements: Rust from the pinned toolchain, Docker CLI, and an active Linux amd64
daemon. On Windows, Docker Desktop in Linux mode is sufficient. Use the same Docker
context for the pull and test. A daemon installed inside WSL may differ from Docker
Desktop; images in one are not automatically available in the other.

Check the environment and pull the exact image:

```sh
docker version
docker context show
docker pull --platform linux/amd64 redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
cargo test --locked --test reference -- --ignored --nocapture
```

The test reads the version, image, digest, and platform directly from
[`releases/plan.json`](../releases/plan.json). It neither accepts an external Redis
endpoint nor clears existing databases. The infrastructure:

1. Checks image availability, digest, platform, and immutable ID.
2. Creates its own container without persistence, with an ephemeral port published
   only on `127.0.0.1`.
3. Checks the image used, server version, and `redis-cli` version.
4. Waits for connectivity with a deadline and compares raw TCP responses.
5. Separately executes the five commands using `redis-cli -2 --raw`.
6. Removes only the container created by the test, including on normal setup or
   assertion failures. There is no global cleanup of containers or volumes.

Missing Docker or images, a mismatched version, timeout, and unexpected responses
make the test fail. None of these cases is converted to success or a skip.
Docker commands and socket operations have bounded deadlines.

Forced termination of the test process may prevent cleanup. In that case, use
the exact ID recorded by the harness to inspect the container and confirm ownership
before removing it. Do not use `docker system prune` for this task.

## What the fixtures cover

Requests and responses in
[`tests/common/resp_fixtures.rs`](../tests/common/resp_fixtures.rs) are Rust byte
literals. No Sider encoder or decoder generates the expected response. Local tests
check request lengths with a separate reader, restricted to arrays of bulk strings,
before querying the reference.

| Case | Contract verified against the reference |
| --- | --- |
| `ping` | Simple string without an argument; message, empty, and binary payloads as bulk strings |
| `echo` | Preserve ASCII, empty content, NUL, CRLF, and non-UTF-8 bytes |
| `strings_and_missing` | Missing key, creation, reading, overwrite with empty content, and removal |
| `empty_key_and_binary_value` | Empty key and binary value |
| `binary_key` | Key with NUL, CRLF, and a non-UTF-8 byte |
| `del_duplicates` | Count actual removals; ignore duplicates and missing keys |
| `ascii_command_case_and_distinct_keys` | ASCII case-insensitive commands; case-sensitive keys |
| `arity_errors_preserve_connection_and_state` | Arity of the five commands, reusable connection, and preserved value |

Each case is repeated in a pipeline, comparing the literal concatenation of responses.
A `PING` after each pipeline detects residual bytes before the next case.
At the end, the client closes its write half and requires EOF without additional
bytes. Every case leaves none of its keys behind and can be repeated on the same instance.

Type contracts follow the [official RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).
Arities and responses are verified by executing the pinned version, not inferred
from documentation alone.

## Recorded runs

On September 8, 2026, the external test passed on Windows x86_64 with Rust 1.97.1
and Linux amd64 Redis in Docker Desktop. It verified eight cases, 48 sequential
exchanges, eight pipelines, EOF without extra bytes, and the five commands through
`redis-cli`. Server and CLI reported 8.10.1; the image matched the digest pinned
above. The harness confirmed container removal.

Local tests also include a child process without Docker in `PATH` to demonstrate
that missing infrastructure produces an explicit failure. This environment is
configured only in the child process, without changing the tests' global environment.

During R01-03 validation on the same date, the 89 local tests and one doctest passed
on Windows x86_64 MSVC and Linux x86_64 GNU, using Rust 1.97.1 and the versioned
lockfile. On Linux, building and running occurred in an Ubuntu 24.04 container,
with sources mounted read-only and a build cache separate from Windows.
`cargo fmt --check`, `cargo check --locked --all-targets`,
`cargo clippy --locked --all-targets -- -D warnings`,
`cargo doc --locked --no-deps`, and `cargo build --locked --release` also passed
in both environments. The external test, ignored in the default cycle, was run
separately on Windows against Redis in Docker and passed again.

R01-04 passed the same suite on September 8, 2026, including a release build:
150 local tests on Windows and 151 on Ubuntu 24.04, plus one doctest on each system.
The difference is the Unix `SIGTERM` test, which confirmed successful exit and
readiness file removal. The 16 worker tests, 14 connection tests, and 11 TCP tests
include cancellation, backpressure, deadlines, and shutdown. The Redis reference
remains opt-in and was not counted as passed based on the ignored test.
These results are neither release gates nor tests of extracted packages.

### R01-05 history before fuzz removal

The results below describe the original implementation and its environment at the
time. The fuzz infrastructure and gate were removed; paths and tools cited here
are historical records, not execution instructions for the current checkout.
The [published candidate's notes](../releases/notes/v0.1.0-rc.1.md) also preserve
the criteria and evidence required at that revision.

R01-05 passed on Windows x86_64 MSVC and Linux x86_64 GNU (Ubuntu 24.04), with
Rust 1.97.1: 230 and 231 regular tests, respectively, plus one doctest on each system.
Formatting, all-target checks, Clippy without warnings, documentation, and release
builds also passed. Six opt-in entrypoints were ignored in this cycle: the external
reference, three compatibility entrypoints, corpus preparation, and the fuzz gate.
They were not counted as passed gates.

Separately, `sider_matches_redis` passed on Windows with 3,588 binary comparisons.
On Linux, `sider_matches_redis_and_cli` passed with the same 3,588 comparisons and
nine additional CLI scenarios. Server and CLI reported 8.10.1, with digest and
platform checked; cleanup was confirmed on both paths. The local recipe
`dev/test.Dockerfile` was also built and its Rust, cargo-fuzz, Docker CLI, and
Clang versions were checked.

The initial decoder fuzz run ended with exit code 0 after 903.636 seconds of actual
execution, excluding compilation. libFuzzer reported 452,886 executions in 902
seconds, coverage 539, and peak RSS of 596 MiB. There was no panic, sanitizer
diagnostic, or failure file. The run used AddressSanitizer, `nightly-2026-09-07`,
cargo-fuzz 0.13.2, seed `1397310533`, and a 4,096-byte input limit.
The initial corpus contained 155 files: 27 versioned seeds and 128 entries preserved
from an earlier short sample.

Logs and corpus were preserved locally in `target/fuzz-initial-r01/`.
The SHA-256 of `initial.stderr.log` is
`85583e8d6e323f83078828c25880ac6b82d043d5819280f6cfb843c3f0c7d671`.
The compiled target corresponds to `fuzz/fuzz_targets/resp_decoder.rs` with SHA-256
`18ac54c4679546482512e4c6794a1699ee8077d168b8b7d38a73c00de59bfaaa`;
the isolated lockfile has SHA-256
`f6433cd44db1590a09afa270cba31822ff8a59b314204b1cec3ad3efeed56ee2`.
This was the initial R01-05 run, while documentation was still being edited.
It did not create a release receipt or approve any RC or final release gates.

## Limits of this evidence

R01-01 verifies the reference and its fixtures. R01-03 reproduces those fixtures
in the synchronous Sider core, and R01-04 reproduces them over TCP. R01-05 adds
simultaneous comparison of the servers and CLI. Each command form is verified
only in the described scenarios; compatibility with clients requiring other
commands or automatic handshakes is not promised.

`SET` options, unknown commands, requests outside the subset, and Sider-specific
limits are not implicitly treated as equivalent to Redis. Intentional differences
are documented in the [compatibility matrix](compatibility.md).

Redis running on Linux inside Docker does not prove that the Sider binary was
built and tested natively on Linux. The native R01-03 through R01-05 runs described
above are separate checks from the Redis reference. They neither validate extracted
packages nor replace gates at the exact SHA of a future release.
