# Redis compatibility

See the [consolidated 1.0 matrix](compatibility-matrix.md) for all implemented
forms, restrictions, supporting tests, and post-1.0 policy.
This page preserves historical evidence by milestone. Consolidation does not
close gates or represent candidate approval.

The five 0.1 commands are implemented and verified by differential comparison
of the Sider binary with Redis 8.10.1. The separate `redis-cli` test also passed
against Sider. This establishes the subset below for the recorded cases,
not compatibility with every Redis command or client.

R02 added strings, SET options, TTL, and quota. This extension is described below
and in the [strings guide](strings.md). Internal milestones retained Cargo version
`0.1.0`; the current checkout prepares `1.0.0`, with separate candidate build
validation before any publication.

## Version 0.1 matrix

| Command form | Target behavior | Implementation | Verification against Redis |
| --- | --- | --- | --- |
| `PING` | Return simple string `PONG` | TCP | Literal differential and CLI |
| `PING message` | Return the message as a bulk string | TCP | Literal/generated differential and CLI |
| `ECHO message` | Return the exact message bytes | TCP | Differential, boundaries up to 1 MiB, and CLI |
| `GET key` | Return a bulk string or null bulk string if absent | TCP | Differential, final state, and CLI |
| `SET key value` | Create or replace; return simple string `OK` | TCP | Differential, overwrite/binary data, and CLI |
| `DEL key [key ...]` | Count only keys actually removed | TCP | Differential, duplicates/absent keys, and CLI |

The initial reference is Redis and `redis-cli` **8.10.1**, on Linux amd64.
The image tag and digest are pinned in [releases/plan.json](../releases/plan.json):

```text
redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
```

In `R01-01`, the reference passed the literal fixtures: eight cases, 48 sequential
exchanges, eight pipelines, and the five commands through `redis-cli`.
The test checks digest, platform, and server/CLI versions before execution.
See the [reproduction commands](testing.md).

In `R01-03`, `tests/commands.rs` runs these same fixtures against the Sider core
and checks the expected bytes without sockets. R01-04 repeats the fixtures over
Sider TCP, including pipelines. In R01-05, `tests/compatibility.rs` passed with
3,588 binary comparisons on Windows and Linux Ubuntu 24.04. The Linux path also
passed nine CLI scenarios against both servers, using disposable processes and
containers. The [reproducible commands](differential.md) record seeds, coverage,
and limits. The 1.0 candidate validates the complete matrix at its SHA;
the final release promotes the same files and evidence from that approved build.

## Matrix implemented in R02

| Form | Verified contract | Evidence |
| --- | --- | --- |
| `EXISTS key [key ...]` | Duplicates count; absent and expired keys do not | Native, TCP, and Redis differential |
| `INCR key` / `DECR key` | Canonical decimal i64; absent starts at zero; rejection preserves value/TTL | Native, TCP, and Redis differential |
| `MGET key [key ...]` | Ordered array with nulls and duplicates | Native, TCP, and differential with up to three 1 MiB payloads |
| `MSET key value [key value ...]` | Indivisible batch; last pair wins; clears TTL | Native, TCP concurrency, and Redis differential |
| `SET ... [NX\|XX] [EX seconds\|PX ms\|KEEPTTL] [GET]` | Validated conditions, previous-value return, combinations, and deadlines | 48 combinations and invalid cases compared with Redis |
| `EXPIRE key seconds` / `PEXPIRE key ms` | Relative deadline; nonpositive removes; absent returns zero | Injected clock and Redis differential |
| `TTL key` / `PTTL key` | Remaining deadline, -1 persistent, -2 absent/expired | Exact native boundaries; differential with declared tolerance |
| `PERSIST key` | Removes only an existing deadline | Native and Redis differential |

The report separates 3,588 historical R01 binary comparisons from 461 new R02
comparisons. Timing observations use a 100 ms tolerance for `PTTL` and one second
for `TTL`; actual expiration is observed with a five-second deadline.
This path passed with Windows Sider and Linux Redis in Docker; it does not
establish a native Linux build or package approval. See
[reproduction](strings.md#reproducible-validation).

## Collections implemented in R05 and R06

Hashes, lists, and sets are described in the [collections guide](collections.md),
with 2,383 differential comparisons. Sorted sets are in the
[scores and ordering guide](sorted-sets.md), with 8,561 exact responses.
The families preserve TTL and quota; `WRONGTYPE` rejects operations across types,
`MGET` returns null for collections, and `SET` without `GET` can replace any type.
[Full AOF writer validation](types-persistence.md) remains separate from command
comparison and distinguishes format evolution from migration between frozen executables.

## Transactions implemented in R07

`MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` are implemented for one shard,
including expiration conflicts and individual errors without rollback. Queue
and watch limits are Sider-specific. The Redis 8.10.1 differential test passed
91 binary comparisons, including `WRONGTYPE`, collections, subscriptions inside
`EXEC`, and messages to the same connection. The
[transactions guide](transactions.md) records commands, restrictions, special
RESP2 framing, persistence, and verification reproduction.

## Implemented protocol

`INFO [section ...]` exposes Sider-specific diagnostics over RESP2. Sections and
indicators are in the [operations guide](metrics.md); there is no promise to
reproduce every Redis INFO field. The query respects subscriber mode and response
limits and can be queued in `MULTI`.

Sider accepts RESP2 requests consisting of nonempty arrays of non-null bulk
strings. Command names are ASCII case-insensitive. Keys and values are binary,
without requiring UTF-8.

The codec represents all five RESP2 types, but that does not mean all types are
accepted as command arguments. Null values, empty values, null arrays, and empty
arrays have distinct representations.

Fragmented frames and concatenated commands have been handled since R01-04.
Each connection processes commands sequentially, with one request in flight.

## Current limitations and differences

| Area | Current development contract |
| --- | --- |
| `SET` options | NX, XX, EX, PX, GET, and KEEPTTL; EXAT, PXAT, and value conditions are outside the subset |
| Unknown command | Return `ERR unknown command`, with simplified text that does not repeat arguments |
| Supported command arity | Produce a response compatible with the selected Redis version, after verification |
| Invalid request format | Reject and close the connection; no Redis equivalence promise outside the declared subset |
| Protocol | RESP2; no RESP3 or inline commands |
| Logical database | Default database only; no `SELECT` |
| Handshake and authentication | No `AUTH`, `HELLO`, `COMMAND`, or `CLIENT`; clients requiring them are not covered |
| Data types | Strings, hashes, lists, sets, and sorted sets in the documented subset; all use binary payloads |
| Expiration | Basic EXPIRE and PEXPIRE; no NX, XX, GT, or LT; monotonic during execution |
| Persistence and replication | Custom binary AOF and global snapshots; asynchronous Sider → Sider replication with the same version/configuration, no Redis replication or Redis Cluster |
| Dataset memory | Custom logical quota with atomic rejection, no eviction; does not reproduce Redis maxmemory/RSS |
| Operational use | Prototype for local development and tests, with a loopback default address |

Input limits and deadlines are in the [networking guide](network.md).
They are Sider-specific limits, not reproductions of Redis defaults.

## Required evidence

To repeat core validation without networking:

```sh
cargo test --locked --lib command::
cargo test --locked --lib storage::
cargo test --locked --test commands
cargo test --locked --test tcp
cargo test --locked --test cli
```

`tests/commands.rs` also checks that invalid `SET` options, unknown commands, and
invalid formats do not reach storage. The parser moves `Bytes` into the command;
`GET` shares immutable content without a copy proportional to the value.

To declare a command form verified, record its reproducible test and reference
version. The differential suite compares raw responses and observed state using
disposable instances. Its response reader is independent of the codec under test.

Include cases with non-UTF-8 bytes, empty keys and values, absent keys, overwrites,
and repeated keys in `DEL`. Also check command casing, arity, rejection without
mutation, and continuation after recoverable errors.

The `redis-cli` test is separate integration evidence: its textual output does
not replace protocol byte comparison. A missing tool or skipped test must remain
recorded as pending.

## Capability integration

Pub/Sub is implemented in R08: `SUBSCRIBE channel [channel ...]`,
`UNSUBSCRIBE [channel ...]`, `PUBLISH channel message`, and `PING [message]` in
subscriber mode. The dedicated Redis 8.10.1 differential test recorded 245 comparisons,
64 messages, and 16 reconnections with cleanup, alongside native backpressure tests.
The [Pub/Sub guide](pubsub.md) describes ephemeral delivery, limits, permitted
commands, and differences. Transactions and their interaction with Pub/Sub remain in R07.

The [ROADMAP](../ROADMAP.md) is the official sequence. R02 strings, `SET` options,
TTL, and quota are implemented, as are AOF and durable fixed shards (R03/R04).
AOF is a custom format, without Redis file compatibility. Hashes, lists, sets,
and sorted sets are implemented with TTL/quota and typed persistence.
Single-shard transactions are implemented in R07; Sider→Sider replication in R09;
backup, metrics, and distribution have implementations and runners in R10.
The complete operational baseline and 1.0 candidate remain subject to gates.

With `SIDER_SHARDS` greater than one, multikey operations require the same shard,
with `CROSSSLOT` rejection before any effect. This intentional difference from
standalone Redis is verified in native/TCP tests; hash tags allow keys to be
colocated, as described in the [sharding guide](sharding.md).
R09 replication uses full and incremental synchronization, read-only replicas,
and durable manual promotion. Tests with actual processes cover types, TTL,
batches, reconnection, history loss, crashes, and delayed promotion.
The [replication guide](replication.md) details limits and possible asynchronous loss.

For collections without guaranteed order, such as `SMEMBERS`, the suite compares
normalized content. Ordered responses, such as `LRANGE` and `ZRANGE`, retain their
order during comparison. Each new command form moves from planned to verified
only when its test and reference are recorded.

Cumulative gates and the candidate workflow are in the [release guide](releases.md).
The bootstrap does not satisfy these gates and will not be published as a functional version.
