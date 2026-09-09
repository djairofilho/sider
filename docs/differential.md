# Differential tests and local gates

New tests use Rust/Cargo. The suite sends the same bytes to the Sider binary
and a disposable Redis instance from the image pinned in `releases/plan.json`.
The response reader in `tests/common/wire.rs` does not import the product codec:
it distinguishes types, preserves complete bytes, and limits bytes, lines, nodes, and depth.

## Coverage

- Eight fixtures, 48 sequential exchanges, and pipeline repetition, with literal
  responses and a sentinel to detect residual bytes.
- Six fixed seeds, 256 operations per seed, repeated sequentially and in pipelines
  of 16 commands. They include the five commands, binary/empty keys,
  overwrites, and duplicates in `DEL`.
- Observation of every key after each sequence; removal of only that case's keys
  and verification of their absence. There is no `FLUSHALL` or external Redis endpoint.
- Payloads of 0, 1, 127, 8,192, and 1,048,576 bytes in `ECHO`, `SET`, and `GET`.
- Comparison of response bytes and types, including subset arity errors,
  followed by half-close and EOF with no additional bytes.
- Nine separate `redis-cli -2 --raw` calls to both servers, covering all five
  commands. Textual CLI output does not replace the binary oracle.

The R01 core compares 3,588 binary responses. The nine additional CLI calls are
counted only in the shared Linux path. `SET` options, unknown commands, and framing
outside the subset are not advertised as equivalent to Redis.
See the [compatibility matrix](compatibility.md).

R02 adds 461 binary comparisons for strings and SET options, keeping R01 counts
separate. It includes 48 condition/return/deadline combinations, invalid integers
and overflow, duplicates, MGET with three 1 MiB payloads, and errors followed by
new operations. The independent reader accepts responses up to 4 MiB.

Timing observations remain outside `binary_comparisons`: `PTTL` allows 100 ms and
`TTL` one second between the two processes. Polling with a five-second deadline
confirms actual expiration on both servers. Its iteration count is recorded
separately. The [strings guide](strings.md) covers semantics and deterministic
tests that check exact boundaries and quota.

## R11 cross-family audit

The [cross_family.rs](../tests/common/cross_family.rs) helper is consumed by the
`compatibility` gate using the already running processes. Its report is under
`r11_cross_family`; `r01_binary_comparisons`, `r02_binary_comparisons`, and
`r01_cli_cases` preserve historical counts. Totals include the new corpus
without attributing it to R01/R02.

There are four seeds: `1`, `42`, `0x511de011`, and `0xfeedfacedeadbeef`.
Each seed uses 40 type/TTL rounds, ten transaction rounds, and four Pub/Sub rounds.
The existing `Sequence` generator provides reproducible binary payloads, including
empty ones. Cases alternate string, hash, list, set, and sorted set on the same
key; they check type/state after rejection, replacement with KEEPTTL, immediate
TTL removal, WATCH conflicts from expiration and creation/removal, individual
errors inside EXEC, DISCARD/UNWATCH, and publications alongside transactional mutations.

The corpus passed on September 9, 2026 with **5,128 binary comparisons** on
Windows and Linux. The Linux path added **16 redis-cli cases** against both
servers, totaling 5,144 new checks. The Redis/CLI 8.10.1 reference and its digest
were checked by the harness. There were no undeclared differences in these cases.
Processes, subscriptions, keys, and the Redis container were cleaned up at completion.

Every response in this corpus is exact. It avoids reads without publicly guaranteed
order and uses PEXPIRE zero, absent/persistent TTL, and PERSIST to check deadline
presence without adding clock tolerance. Exact timing boundaries remain in
injected-clock tests; positive PTTL/TTL retain R02 tolerances.
It does not test eviction, Redis replication, commands outside the matrix, or RSS
equivalence. Persistence, shards, and transport failures remain in their respective
runners; this new corpus does not replace the gate matrix.

During development, run only the new corpus:

```powershell
cargo test --locked --test compatibility -- --ignored --exact cross_family_audit_only --nocapture
```

In the isolated Linux runner described below, use that same name instead of
`sider_matches_redis_and_cli`. The presence of `SIDER_TEST_RUNNER_CONTAINER`
enables the 16 CLI cases. The candidate uses `release_compatibility_gate`,
which includes R01, R02, CLI, and R11 together. A partial run produces no release receipt.

## Native cycle without external infrastructure

```sh
cargo test --locked --test compatibility --test harness --test gate_contract
cargo clippy --locked --all-targets -- -D warnings
```

External tests are explicitly ignored by default. This does not approve gates.
Native tests cover the independent reader, reproducible generator, processes with
deadlines/bounded output, invalid readiness, mismatched release context, stale
receipts, and zero cases.

## Binary comparison on Windows

With Docker Desktop in Linux mode and the pinned image available:

```powershell
docker --context desktop-linux pull --platform linux/amd64 redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
cargo test --locked --test compatibility -- --ignored --exact sider_matches_redis --nocapture
```

Use the same Docker context for the pull and test process. The helper calls the
Docker CLI available in the environment; this example assumes `desktop-linux`
is already the current context. It does not change the global context.
This path runs Windows Sider and the Linux Redis reference, with an ephemeral
port published on loopback. It does not run `redis-cli` against Sider or approve
the Linux gate.

## Linux and redis-cli on an isolated network

`127.0.0.1` inside a container is not the Docker host. Redis therefore uses
`--network container:<runner-id>` and shares the Ubuntu runner's loopback.
Sider uses an ephemeral port; Redis uses 6379. No ports are published and Sider
does not bind outside loopback. Run one reference per runner, without parallelizing
external entry points. [Docker shared networking](https://docs.docker.com/engine/network/#container-networks).

The [dev/test.Dockerfile](../dev/test.Dockerfile) recipe pins Ubuntu 24.04 and
Docker CLI by digest, Rust 1.97.1, and rustup by checksum.
It is a local tool, not the distribution image planned for 0.10.
Ubuntu packages come from the distribution repositories; there is no promise
of bit-for-bit identical images built on different dates.

PowerShell example, run at the repository root:

```powershell
docker --context desktop-linux build --platform linux/amd64 --tag sider-dev:tests --file dev/test.Dockerfile dev
if ($LASTEXITCODE -ne 0) { throw 'Environment build failed' }

$siderRoot = (Get-Location).Path
$siderRunner = docker --context desktop-linux run --detach --rm --platform linux/amd64 --label dev.sider.purpose=local-tests --mount "type=bind,source=$siderRoot,target=/workspace,readonly" --mount 'type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock' --env DOCKER_HOST=unix:///var/run/docker.sock --env GIT_OPTIONAL_LOCKS=0 --workdir /workspace sider-dev:tests tail -f /dev/null
if ($LASTEXITCODE -ne 0 -or $siderRunner -cnotmatch '^[a-f0-9]{64}$') { throw 'Runner not identified' }
try {
    docker --context desktop-linux exec --env "SIDER_TEST_RUNNER_CONTAINER=$siderRunner" $siderRunner cargo test --locked --test compatibility -- --ignored --exact sider_matches_redis_and_cli --nocapture
    if ($LASTEXITCODE -ne 0) { throw 'Differential/CLI failed' }
} finally {
    docker --context desktop-linux stop $siderRunner
}
```

The Docker socket gives the runner administrative access to the daemon. Use only
trusted sources and test images in a controlled development environment. Do not
use this command to run unreviewed external PRs. Sources are read-only; the build
goes in `/tmp/sider-target` inside the container. An optional cache must be separate
from the Windows target and must not hide `/opt/cargo/bin`.

The harness checks the image, digest, Redis/CLI versions, full IDs, container
state, and network before testing. Sider readiness requires the version, live
child PID, IP/port, and literal PING. On success it confirms the child has been
reaped and the Redis container and its own temporary files removed. On normal
failures, guards also attempt cleanup. Forcibly killing the executor may prevent
cleanup; inspect the recorded ID before removing a resource. Never use a global prune.

## Release receipts

Commands in [releases/gates.json](../releases/gates.json) use explicit Cargo tests.
The process must run in a clean checkout of the selected SHA, with the stable
toolchain and a native Linux GNU target. In addition to the runner ID, provide:

| Variable | Contents |
| --- | --- |
| `SIDER_RELEASE_VERSION` | Exact version of the built package |
| `SIDER_RELEASE_SHA` | Full HEAD, checked before and after execution |
| `SIDER_RELEASE_TARGET` | `x86_64-unknown-linux-gnu` |
| `SIDER_REFERENCE_IMAGE` | Exact Redis image from the manifest |
| `SIDER_RELEASE_DIR` | Existing absolute directory for this run |

```sh
cargo test --locked --test compatibility -- --ignored --exact release_compatibility_gate --nocapture
```

Receipts are published only after success and cleanup, with positive case counts,
duration, seeds, and tools. Writing uses a temporary file and hard link without
replacing the destination; the results filesystem must support hard links.
Missing context, changed checkout, wrong target, stale receipt, or a filtered run
with no cases produces no approval. If the directory is inside the checkout,
use `target/`, which Git ignores. Copy results out of the runner before removing it.

The 1.0 candidate requires new receipts at its own SHA, with build version `1.0.0`.
The final release promotes these same files; it does not generate new receipts.
Results from a working branch do not replace candidate build validation.
Internal tests use the suite's direct entry points and record task/SHA without release context.
