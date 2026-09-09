# 1.0 subset matrix

This is the technical consolidation for R11-01 and R11-02. The checkout prepares
package `1.0.0`; candidate build testing and publication depend on the gates and
exact files. Internal milestones and the R10 baseline retained `0.1.0`, without
expanding the historical 0.1 release or approving the current candidate.

The Redis contract uses server and `redis-cli` **8.10.1**, with a Linux amd64 image
pinned by tag and digest in [releases/plan.json](../releases/plan.json).
Every form below uses nonempty RESP2 arrays of non-null bulk strings.
Names and options are ASCII case-insensitive; keys, values, members, fields,
and channels preserve arbitrary bytes, including empty and non-UTF-8 bytes.
`[argument]` means optional; `...` repeats the preceding group.

## Supported forms

Evidence abbreviations refer to the files and results in the following section.
Sider's memory, queue, and network limits also apply to every form.

| Form | Contract and restrictions | Evidence |
| --- | --- | --- |
| `PING [message]` | Normal mode: `PONG` or bulk; subscriber mode: `pong`/message array | R, P, X |
| `ECHO message` | Returns the exact bytes | R |
| `GET key` | String or null; collections produce `WRONGTYPE` | R, S, C, X |
| `SET key value [NX\|XX] [EX seconds\|PX ms\|KEEPTTL] [GET]` | Last write replaces the type; GET requires a string before evaluating NX/XX; no EXAT/PXAT | S, C, X |
| `DEL key [key ...]` | Counts removed entries once; removes TTL; multikey requires the same shard | R, S, C, X |
| `EXISTS key [key ...]` | Counts duplicates; excludes expired entries; same shard | S |
| `INCR key` | Canonical decimal i64; absent starts at zero; errors preserve value and TTL | S, X |
| `DECR key` | Same numeric rules as INCR; detects underflow | S |
| `MGET key [key ...]` | Ordered array, preserving duplicates; collections appear as null; same shard | S, C, X |
| `MSET key value [key value ...]` | One batch, last repeated pair wins; replaces types and clears TTL; same shard | S, C |
| `EXPIRE key seconds` | Relative deadline; nonpositive values remove; no NX/XX/GT/LT | S |
| `PEXPIRE key ms` | Relative deadline in milliseconds; immediate removal with zero/negative values | S, C, X |
| `TTL key` | Remaining seconds, -1 persistent, -2 absent/expired | S, X |
| `PTTL key` | Remaining milliseconds, -1 persistent, -2 absent/expired | S |
| `PERSIST key` | Removes an existing deadline and returns 1; otherwise returns 0 | S, C, X |
| `HSET key field value [field value ...]` | Counts new fields; last repeated field wins | C, X |
| `HGET key field` | Bulk or null | C, X |
| `HDEL key field [field ...]` | Counts removed fields; the last field removes the key/TTL | C |
| `HEXISTS key field` | 0 or 1 | C |
| `HLEN key` | Cardinality; zero if absent | C |
| `HGETALL key` | Field/value pairs, with no publicly guaranteed order | C |
| `LPUSH key value [value ...]` | Inserts each argument on the left | C |
| `RPUSH key value [value ...]` | Inserts each argument on the right | C, X |
| `LPOP key` | Removes one element from the left; no count option | C |
| `RPOP key` | Removes one element from the right; no count option | C |
| `LLEN key` | Length; zero if absent | C |
| `LRANGE key start stop` | i64 indexes, negative from the end; inclusive endpoints | C, X |
| `SADD key member [member ...]` | Counts new members, excluding duplicates | C, X |
| `SREM key member [member ...]` | Counts removals, excluding duplicates; last member removes the key | C |
| `SISMEMBER key member` | 0 or 1 | C, X |
| `SCARD key` | Cardinality; zero if absent | C |
| `SMEMBERS key` | Unique members, with no publicly guaranteed order | C |
| `ZADD key score member [score member ...]` | Basic pairs only; validates all scores before changing state | Z, X |
| `ZREM key member [member ...]` | Counts removals, excluding duplicates; last member removes the key | Z |
| `ZCARD key` | Cardinality; zero if absent | Z |
| `ZSCORE key member` | Score as bulk or null | Z |
| `ZRANGE key start stop [WITHSCORES]` | Inclusive rank, negative indexes; no BYSCORE/BYLEX/REV/LIMIT | Z, X |
| `MULTI` | Starts the connection queue; does not execute queued commands | T, X |
| `EXEC` | Executes on the same shard; ordered array, no rollback of individual errors | T, X |
| `DISCARD` | Discards queue and watches; error outside MULTI | T, X |
| `WATCH key [key ...]` | Watches until EXEC/DISCARD/UNWATCH/EOF; conflicts include expiration and ABA | T, X |
| `UNWATCH` | Removes watches; queued inside MULTI | T, X |
| `SUBSCRIBE channel [channel ...]` | Acknowledges every argument and enters RESP2 subscriber mode | P, T, X |
| `UNSUBSCRIBE [channel ...]` | Acknowledges every argument; without arguments removes all; returns to normal after the last | P, T, X |
| `PUBLISH channel message` | Counts queues that accepted; only normal mode or a previously queued command | P, T, X |
| `INFO [section ...]` | Sider-specific diagnostics; does not reproduce every Redis field | I |

Collection-specific operations require the corresponding type. Valid mutations
preserve TTL; removing the last item removes the key, deadline, and quota usage.
`SET` without GET and `MSET` can replace collections. Type, score, integer, or quota
errors leave no partial command changes. See [collections](collections.md),
[sorted sets](sorted-sets.md), and [strings](strings.md) for details and verified
error precedence.

Scores are IEEE-754 f64. They accept the verified decimal, exponent, hexadecimal,
and infinity forms; they reject NaN, whitespace, finite overflow, and nonzero
underflow to zero. Negative zero is normalized. Score ordering and text
representation are compared exactly, without test normalization.

## Evidence by form

| Code | Tests and reference | Observed scope |
| --- | --- | --- |
| R | [commands.rs](../tests/commands.rs), [tcp.rs](../tests/tcp.rs), [compatibility.rs](../tests/compatibility.rs), Redis/CLI 8.10.1 | 3,588 historical R01 binary comparisons; nine CLI cases; fixtures, pipelines, binary data, and payloads up to 1 MiB |
| S | [compatibility.rs](../tests/compatibility.rs), storage and TCP string/TTL tests, Redis 8.10.1 | 461 R02 binary comparisons; deadlines observed separately; 48 SET combinations |
| C | [collections.rs](../tests/collections.rs), [collections_differential.rs](../tests/collections_differential.rs), Redis 8.10.1 | 2,383 comparisons: 2,188 exact and 195 normalized; only HGETALL pairs and SMEMBERS members have normalized order |
| Z | [sorted_sets.rs](../tests/sorted_sets.rs), [collections_differential.rs](../tests/collections_differential.rs), Redis 8.10.1 | 8,561 exact responses; scores, binary tie-breaking, rank, errors, and numeric extremes |
| T | [transactions.rs](../tests/transactions.rs), [transactions_persistence.rs](../tests/transactions_persistence.rs), Redis 8.10.1 | 91 historical differential responses; WATCH, errors, EXEC framing, cancellation, single append, and replay |
| P | [pubsub.rs](../tests/pubsub.rs), hub and connection tests, Redis 8.10.1 | 245 historical comparisons, 64 messages, 16 reconnections; queues and slow clients in dedicated tests |
| I | [metrics.rs](../tests/metrics.rs), INFO/diagnostic tests | Sider values observed in the real system; no Redis field equivalence |
| X | [cross_family.rs](../tests/common/cross_family.rs), consumed by the compatibility gate | R11 corpus across types, TTL, EXEC/WATCH, and Pub/Sub; separate report without reattributing R01/R02 counts |

Historical counts identify their original test runs. They do not establish results
for another SHA or a future candidate. The candidate's complete matrix requires
running gates against the frozen build, as described in [releases](releases.md).
Corpus X counts and reproduction commands are in [differential.md](differential.md).

## Protocols, persistence, and operations

| Capability | Contract and limitation | Supporting evidence |
| --- | --- | --- |
| RESP2/TCP | Bounded framing, fragmentation/pipeline, one in-flight request per connection; invalid requests close the connection | [resp_codec.rs](../tests/resp_codec.rs), [tcp.rs](../tests/tcp.rs), fixtures and differential tests |
| Shards | FNV-1a 64 with hash tags; multikey and EXEC on the same shard; CROSSSLOT before any effect | [sharding.rs](../tests/sharding.rs), [transactions](transactions.md) |
| AOF | Custom format, indivisible resolved batch, checksum/limits/seal; no Redis file compatibility | [persistence.rs](../tests/persistence.rs), [types-persistence.md](types-persistence.md) |
| Sync | `always` acknowledges after sync; `everysec` allows a window before sync | [persistence.md](persistence.md), failure and recovery tests |
| Compaction | Global snapshot and delta; publication preserves the cut and complete batches | [sharding.rs](../tests/sharding.rs), persistence/transaction tests |
| Durable TTL | Absolute Unix deadline in the file; downtime consumes TTL | [aof_migration.rs](../tests/aof_migration.rs), [backup.rs](../tests/backup.rs) |
| Replication | Asynchronous Sider→Sider, same version/layout, FULL/CONTINUE and ACK after durable apply | [replication_network.rs](../tests/replication_network.rs), [replication_storage.rs](../tests/replication_storage.rs) |
| Role/promotion | Replica rejects client writes; explicit local promotion; no election, distributed fencing, or automatic failover | [replication_persistence.rs](../tests/replication_persistence.rs), sider-replica CLI |
| Backup | Export through internal listener, consistent cut, manifest/checksums, restoration into a new directory | [backup.rs](../tests/backup.rs), [backup_process.rs](../tests/backup_process.rs), [guide](backup.md) |
| Shard changes | Offline migration to a new directory with verified quotas and replay | [aof_migration.rs](../tests/aof_migration.rs), [guide](aof-migration.md) |
| Distribution | Public Linux/Windows packages with four binaries; Docker contains the same Linux bytes | [package.rs](../tests/package.rs), [docker_distribution.rs](../tests/docker_distribution.rs) |
| Observability | INFO and `--diagnose` describe state/configuration; no dataset content | [metrics.rs](../tests/metrics.rs), [metrics.md](metrics.md) |

Without AOF, the process does not promise recovery after termination. Pub/Sub is
ephemeral and is not included in AOF, backup, or replication. Replication ACK does
not make primary writes synchronous. A primary failure may lose data not yet
applied on the replica. Client timeout or disconnection after acceptance does
not undo a command or prove that it had no effect.

## Intentional differences

Only the default logical database and array-based RESP2 are supported. AUTH, ACL,
TLS, SELECT, RESP3, the inline protocol, Redis Cluster/replication, scripts, modules,
streams, eviction, and unlisted commands are outside the subset. Compatibility is
not promised for libraries that require HELLO, CLIENT, or COMMAND during handshake.

Unlisted SET, EXPIRE, ZADD, and ZRANGE options are not supported capabilities.
Unknown-command rejection text is simplified and does not repeat arguments.
Arity and errors for declared forms have their own evidence; this does not extend
equivalence to excluded forms.

Per-shard logical quota, queues, connections, deadlines, and response limits are
Sider policies. They do not represent Redis maxmemory, RSS, or defaults. Output
above the limit may close the connection; an accepted command may already have
changed data. Pub/Sub evicts subscribers with full queues and its count confirms
queue acceptance, not client receipt. HGETALL/SMEMBERS have no publicly guaranteed
order. The order of UNSUBSCRIBE acknowledgments without arguments and with multiple
channels may differ.

## Post-1.0 policy and remaining freeze requirements

Each new form needs a parser, semantics, limits, and evidence before entering the
matrix. An incompatible change to the declared contract requires a new major
version and migration guide. Fixes restoring the contract may be patches, with a
reproducible regression and description of the changed behavior. Compatible
additions may enter a minor release; none retroactively expands published artifacts.

AOF evolution requires an explicit version, verified fixtures, and migration.
There is no promise to read future versions or maintain replication across
different versions. The matrix records intentional differences; a divergent result
outside it is investigated as a defect or gap before publication.

The integrator checks the complete R10 baseline, migration to 1.0, a soak of at
least 3,600 seconds, benchmarks, and every candidate gate.
These items remain pending until evidence exists for the exact SHA and assets.
This document and corpus X do not close R10, R11, or release approval.
