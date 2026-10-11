# Live server startup preflight, issue #93

## Source and scope

This stage builds on source commit `bbe3205`, which attached live replication,
backup export, status and promotion consumers to the shared generation runtime.
That consumer stage is published in [PR #122](https://github.com/djairofilho/sider/pull/122).
The [security boundary](security-boundary.md) remains the accepted contract.
Both executable gates remain closed: protected internal startup and the protected
Windows restricted-directory loader. In-memory embedding tests do not establish
filesystem permissions or authorize lifting either gate.

## Runtime wiring

`ServerPreflight` validates a complete generation against an exact immutable
`ServerConfig`: client identity is required, the internal lane must match enabled
internal networking, and expected upstream policy must match outgoing replica
configuration. Current transport certificate/trust/CRL bounds are checked.
The preflight object does not implement `Debug` or expose secret material.

`StartupSecurity::preflight` uses the existing profile and restricted-directory
checks. It retains their failure behavior. The executable uses one live runtime
for client admission, internal admission and complete `poll_server` replacement
validation. The previous protected static internal fallback is removed.

`server::serve_live` owns recovery and both listener binds. It rechecks the exact
configuration and active transport bounds before recovery, after recovery before
the internal bind, and before the client bind. Its prepared variant rechecks
before starting workers and publishing readiness. Caller-managed prepared
listeners remain the embedding caller's responsibility.

The server dispatcher uses `session::serve_live` and `session::follow_live` for
live preflights. Existing static and legacy embedding entry points retain their
transport model. The executable's poller shares the runtime and is stopped and
joined when server startup fails or normal service ends.

## Focused test plan and evidence

The focused startup cases cover:

- Complete live server TLS readiness, client authentication requirement,
  authenticated internal status, combined certificate rotation, closed old
  sessions and a fresh internal connection.
- Server-owned full replica synchronization and reconnection with a rotated
  runtime certificate after the primary revokes the old serial.
- Exact lane and outgoing-policy requirements.
- Configuration changes and natural transport expiry after preflight creation,
  rejected before AOF creation, listener binds or readiness.
- Preserved protected Windows and internal executable gates.
- A real executable denial with both requested ports occupied, bounded child
  capture and isolated child configuration; the security error must precede
  AOF recovery, bind errors and readiness.

Additional `cfg(unix)` cases cover a real protected client executable startup
with a restricted complete generation and the restricted-loader matrix. The
matrix covers positive exact file and directory modes, rejected broad
file/directory permissions, rejected writable
ancestors, and rejected file/directory symlinks. Its executing checkout must have
contract-compliant ancestors; `/tmp` is deliberately unsuitable. This case has
not been executed on the current Windows host. The client executable case is
also unexecuted here and does not exercise protected internal startup.

On the Windows host, the first focused run passed all seven startup cases:

```text
cargo test --locked --test security_internal security_startup -- --test-threads=1
7 passed; 0 failed; 27 filtered out (2.71 s)
```

A negative control removed only the initial preflight validation in recovery,
retaining the check after recovery. The changed-configuration case failed at
`assert!(!directory.0.join("aof").exists())`, with Cargo exit 101. Thus the test
detects validation that happens after AOF creation, even when the later security
check prevents listener binding. Original source was restored immediately after
that controlled run. This is test sensitivity evidence, not a historical TDD
claim.

The restored focused/regression command passed:

```text
cargo test --locked --test security_internal --test security --test replication_network --test agent_memory_native -- --test-threads=1
security_internal: 34 passed (24.34 s)
security: 10 passed (47.70 s)
replication_network: 15 passed; 1 existing release-context gate ignored (4.25 s)
agent_memory_native: 7 passed (0.03 s)
```

The 34 internal cases include the seven new startup cases, the 23 inherited
internal/runtime/consumer cases and four bounded process-capture helper cases.
The restored before-AOF case passed in that run. The ignored replication case
requires its exact release context and is not claimed as executed here.

The first all-target Clippy run rejected two nested `if` statements in the new
preflight checks. Their equivalent let-chain forms passed the final command:

```text
cargo clippy --locked --all-targets -- -D warnings
exit 0 (15.60 s)
```

The focused functional receipts precede those two equivalent predicate
rewrites; final Clippy checked the rewritten source and all host targets.
`cargo xtask check` then passed on the final source, including format,
all-target Clippy, debug binary, every non-ignored test and doctest. Direct
`rustfmt`, `git diff --check` and UTF-8/mojibake checks passed. Focused runs used
a process-local `CARGO_TARGET_DIR` pointing at the consumer stage's existing
cache. `cargo doc --locked --no-deps` and `cargo build --locked --release`
passed in this stage's worktree. Neither POSIX-only test was compiled or
executed on this Windows host. These results are not release receipts.

## Windows permission limitation

Read-only inspection on the current Windows host found trusted ancestor owners
but untrusted write grants in the workspace ancestor chain and volume root under
the contract's rights mask. The volume-root findings include a non-inherit-only
grant. A complete positive protected-directory test therefore cannot establish
this contract here. No ancestor ACL was changed. No WSL executable was available
for local POSIX execution.

The existing Windows helper remains gated. A positive owner/DACL/ancestor/reparse
matrix on an approved host, checks of the actual opened objects and bounded
helper behavior remain required. Synthetic ACLs and live TLS embedding tests do
not substitute for this matrix.

## Remaining issue #93 gates

Actual protected executable internal operation, approved Windows permission
evidence, executed POSIX material and ownership matrix, complete manifest-last
polling/revocation evidence, diagnostic canaries, failure-log deduplication and
operator migration/recovery remain pending. The new startup wiring does not
close issue #93 or establish safety for network exposure.
