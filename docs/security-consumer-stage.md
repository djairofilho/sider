# Internal live consumers, issue #93

## Source and scope

This stage builds on `a967a11ed86d49c26a0359ceceb496e8ed14dbae`
(the shared generation runtime in draft PR #118). The accepted contract remains
[security-boundary.md](security-boundary.md). This stage does not lift protected
internal or Windows startup gates.

`replication::session::serve_live` attaches actual replication, backup export,
status and promotion connections to the shared internal socket registry.
`follow_live` resolves the active generation on each outgoing reconnect and
binds received snapshot and batch effects to the verified upstream.

The existing static embedding APIs retain their static transport model and
existing queue ordering. They do not acquire the shared runtime's reload barrier.
Protected executable configuration must not use them as lifecycle evidence.
Backup tools keep their one-invocation generation contract; archive and restore
code are outside this stage.

## Admission and ownership

- A reader validates the fixed 24-byte frame header and direction before
  acquiring a generation lease, allocating a body or decoding payload data.
  Header, lease acquisition and body reception share one frame deadline.
- No generation lease is retained while waiting for the next frame header,
  journal notification, heartbeat interval or reconnect backoff.
- Each outgoing frame has a bounded lease through write and flush. Typed response
  encoding also occurs under that lease. Snapshot and journal frames can be
  prepared earlier; transmission always obtains a fresh lease.
- Snapshot installation, incremental application, capture and promotion acquire
  a fresh operation lease. Incoming leases retain the initial request kind;
  outgoing leases retain the verified upstream chain and identity.
- Live internal database barrier and pre-acceptance queue waits use the existing
  database request timeout. Final expiry and peer admission checks occur after
  exclusion and capacity reservation. All destination control capacity is
  reserved before durable snapshot publication.
- Owned database tasks and queued worker controls retain accepted operation
  leases through durable publication and complete worker effects. Cancelling a
  network session cannot release those leases early. Accepted operations may
  complete after credential expiry; expiry refuses subsequent acceptance.

## Focused evidence

Eight real-mTLS consumer cases exercise actual workers and AOF: live status and
idle role revocation; forbidden promotion before payload delivery; permitted
durable promotion; partial-body frame deadline; expiry while waiting behind a
database barrier; cancellation during accepted promotion publication; backup
tool export; and full plus incremental live replication with role revocation.

The blocked-barrier expiry case has an explicit regression negative control:
temporarily disabling only the final snapshot admission checks caused
`security_live_consumer_expiry_after_blocked_barrier_refuses_promotion` to fail
with `expired waiting promotion was never accepted` (exit 101, one failed case,
2.06 seconds). The expired request promoted the replica after its barrier wait.
Restoring the checks rejects it. This records a negative control; it does not
claim that the preceding consumer plumbing was written test first.

Final focused results on Windows, on the stated baseline plus this stage:

| Command | Result |
| --- | --- |
| `cargo test --locked --test security_internal security_live_consumer -- --test-threads=1` | Eight consumer cases passed (6.11 seconds). Initial compile found missing fixture `unwrap()` calls; corrected before functional execution. |
| `cargo test --locked --test security_internal --test replication_storage --test replication_protocol -- --test-threads=1` | Internal 23/23, storage 6/6 and protocol 5/5 passed after restoring the negative-control checks. The blocked-barrier expiry case is included. |
| `cargo test --locked --test security --test replication_network --test agent_memory_native -- --test-threads=1` | Client security 10/10, native memory 7/7 and network 15/15 passed. One existing exact-release-context network gate remained ignored. |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed after the final source changes (1 minute 14 seconds). |
| `cargo fmt --all` | Passed before the final functional runs. |

`cargo doc --locked --no-deps` and `cargo build --locked --release` passed on
Windows. The complete `cargo xtask check` also passed, including format,
all-target Clippy, debug binary, the non-ignored test suite and doctest. Its
first run had one intermittent `security_reload_revalidates_queue_subscriptions_and_closes_removed_credentials_and_certificates`
failure (`WRONGPASS` where `OK` was expected). That unchanged client test passed
in isolation and on the second complete check. The five-second first-authentication
deadline and debug Argon2 cost remain a timing sensitivity in this evidence.
Earlier PR #118 results are not attributed to this stage.

## Remaining issue #93 gates

The protected executable startup path remains disabled for internal consumers.
Integration with executable configuration and complete file polling, combined
client/internal activation evidence on these consumers, reconnect certificate
and trust rotation, CRL and original-role invalidation during final snapshot and
batch operations, and adverse cancellation/publication coverage across multiple
workers still require further evidence. The Windows permission matrix and
restricted material loader remain unproven. Static tool invocations require
deployment material and permission verification. Distinct-manifest failure log
deduplication, diagnostic secret canaries and operator recovery evidence remain
pending from the shared runtime stage. Issue #93 remains open.

## Integration map

Changed source: `src/replication/mod.rs`, `src/replication/protocol.rs`,
`src/replication/session.rs`, new `src/replication/security.rs`,
`src/security/internal_runtime.rs`, `src/storage/worker.rs`,
`src/storage/replication.rs`, and `src/storage/snapshot.rs`.
Tests extend `tests/security_internal.rs`; this document records the stage.

There are no persistence/backup archive, memory, command, connection, dependency,
toolchain or license inventory changes. The shared `DbHandle` and replication
control hunks must be integrated with current worker ownership code; retain its
memory dispatch, client security leases, admission checks and RESP3 behavior.
The snapshot hunk is confined to internal admission and capture acceptance.
