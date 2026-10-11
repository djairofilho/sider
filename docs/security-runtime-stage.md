# Shared security generation runtime stage

This follow-up to [#93](https://github.com/djairofilho/sider/issues/93) starts at
`31e18ab0953ec499cdb5064d0dd787c07d6aec57`. The local Windows evidence below covers
that baseline plus this stage's source changes; it does not validate a published
package or a later integration SHA. The accepted [security boundary](security-boundary.md) remains
the target. Protected internal startup and protected Windows startup remain gated.

## Implemented boundary

`GenerationRuntime` names the existing `ClientRuntime` implementation. Existing
client callers retain their API. The runtime accepts a complete server generation
with both client and internal material and uses one read/write lock and serialized
activation. Adding or removing a material lane during reload fails closed; listener
configuration belongs to startup. The authentication pool and reconnect budgets
remain shared across replacement generations.

`Generation::server_files` validates all eight in-memory server components and their
canonical manifest digests for embedding and tests. Executable loading still checks
restricted file objects and retains the Windows permission validation gate.
`poll_server` loads the complete server file set with the startup outgoing setting.

`runtime.internal()` provides an `InternalRuntime` handle. Each incoming handshake
or outgoing reconnect takes the active generation lease and registers its actual
verified peer and owned TLS/TCP stream before releasing that lease. Activation
cannot miss an established socket between handshake and registry insertion.

Under the generation write lock, the registry revalidates every established peer's
actual chain, original roles, expected upstream URI and IP SAN against replacement
material. Own certificate/key changes, role removal, certificate revocation and
trust changes close affected sockets before activation succeeds. Taking and dropping
the owned stream is the local close acknowledgement; this does not claim remote
TCP acknowledgement or recall bytes already written to the network. Socket read
and write waiters are awakened with a fixed error. Retained peer proofs become
permanently invalid after close, including if later policy restores their authority.

An independent one-second expiry task closes registered sockets even when no I/O
is being polled. It retains only a weak registry reference while sleeping. Session
drop closes the owned socket immediately. Valid sessions survive policy expansion
and same-material replacement. Client cleanup acknowledgements and internal closes
complete under the same activation lock, before the new generation becomes available.
Activation remains owned after its caller is canceled.

Replacement certificate/root/CRL validity is checked again after the generation
writer lock is acquired, before any session cleanup. Material that expired while
an accepted lease delayed activation is rejected without closing old sessions.

`InternalLease` provides current policy and chain/role validation for one frame or
complete operation. A clone retains the same generation read lease. Callers must
release it while idle and transfer it to the owning worker for accepted effects.

## Deliberate remaining gates

- Replication, backup and administrative consumers still use their static transport
  APIs. They do not automatically join this registry. This stage changes no
  persistence, backup, database worker, connection or command files.
- Internal frame readers need asynchronous lease acquisition after their bounded
  header and before payload allocation. Final snapshot installation, durable
  promotion, snapshot capture and response writes need worker-owned operation
  leases and cancellation tests. This API alone does not prove those effects safe.
- Surviving established streams retain their original certificate/root/CRL expiry
  deadline. A valid replacement does not renew their deadline in this stage.
  Reconnect uses new material; invalid replacement cannot extend an old deadline.
- Tool-only generation runtimes are not introduced. Tools retain one invocation's
  static validated generation, as specified by the contract.
- Polling still requires one failure log per distinct manifest contents rather than
  repeating a failure log every tick. Filesystem replacement/permission matrix,
  non-loopback protected exposure, complete diagnostic canaries, operator recovery
  and native POSIX/Windows security evidence remain pending.

None of these APIs removes the protected internal startup gate. #93 remains open.

## Changed files and integration

- `src/security.rs`: exports the shared runtime name and internal runtime/lease.
- `src/security/generation.rs`: complete in-memory server generation constructor.
- `src/security/runtime.rs`: shared activation, internal registry and complete poll set.
- `src/security/internal_runtime.rs`: owned sockets, lease API and local close barrier.
- `src/security/internal.rs`: retained peer close flag.
- `src/security/transport.rs`: static proofs initialize without a runtime close flag.
- `tests/security_internal.rs`: focused runtime mTLS and combined listener tests.
- `docs/security-runtime-stage.md`: evidence and remaining integration requirements.

No dependency or license inventory changes are introduced. This stage changes
only the security module, its integration tests, and this documentation.

## Validation

Windows local commands on this baseline plus this stage's source changes:

| Command | Result |
| --- | --- |
| `cargo test --locked --test security_internal security_runtime` | Initial eight runtime cases passed; final nine are covered by the full internal suite below. |
| `cargo test --locked --test security_internal --test security` | Client security 10/10 and then-current internal suite 13/13 passed before the final validity correction. |
| `cargo test --locked --lib security::` | Five native security cases passed before the final validity correction. |
| `cargo test --locked --test security_internal` | Final full internal suite 15/15 passed, including all nine runtime cases (5.09 seconds; compile 17.84 seconds). |
| `cargo clippy --locked --all-targets -- -D warnings` | First run rejected one nested test `if` (`collapsible_if`); corrected final run after all source changes passed (7.24 seconds). |
| Focused pending-read case after lint correction | 1/1 passed. |
| `cargo doc --locked --no-deps` | Passed after all source changes (11.89 seconds). |
| `cargo build --locked --release` | Passed after all source changes (1 minute 26 seconds). |
| `cargo test --locked --test security security_reload` | Final affected client reload regression 1/1 passed after all source changes (17.42 seconds; compile 7.08 seconds). |
| `cargo xtask check` | Passed on the committed source: formatting, all-target Clippy, debug binary, complete non-ignored test suite and doctest. |

The permission matrix, protected executable internal workflow and release gates
are not implied by these component and repository checks.
