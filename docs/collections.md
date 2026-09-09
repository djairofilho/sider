# Hashes, lists, and sets

All three families use binary keys and payloads. Strings and collections share
TTL, quota, and resolved storage mutations. The worker continues to own the data.

## Supported forms

| Form | Response and effect |
| --- | --- |
| `HSET key field value [field value ...]` | Integer count of new fields; the last occurrence of a field wins. |
| `HGET key field` | Bulk value, or null if the field/key is absent. |
| `HDEL key field [field ...]` | Integer count of removed fields, excluding duplicates. |
| `HEXISTS key field` | `1` if present, `0` if absent. |
| `HLEN key` | Field count, `0` if absent. |
| `HGETALL key` | Array alternating fields and values; empty if absent, with no publicly guaranteed order. |
| `LPUSH key value [value ...]` | Inserts each argument on the left and returns the final length. |
| `RPUSH key value [value ...]` | Inserts each argument on the right and returns the final length. |
| `LPOP key` / `RPOP key` | Removes one element from the end; null bulk if absent. The `count` option is outside the subset. |
| `LLEN key` | List length, `0` if absent. |
| `LRANGE key start stop` | Array in list order, with inclusive endpoints and negative indexes counted from the end. |
| `SADD key member [member ...]` | Integer count of new members, excluding duplicates. |
| `SREM key member [member ...]` | Integer count of removed members, excluding duplicates. |
| `SISMEMBER key member` | `1` if a member of the set, `0` if absent. |
| `SCARD key` | Member count, `0` if absent. |
| `SMEMBERS key` | Array of unique members; empty if absent, with no publicly guaranteed order. |

`LPUSH l a b` produces the list `b, a`. In ranges, indexes before the start are
clamped to zero; the end is clamped to the last element. An inverted range or one
entirely outside the list returns an empty array. Index arguments use decimal
`i64` integers, with the same format rejections as [strings](strings.md).

## Types, TTL, and quota

A command applied to another type returns
`WRONGTYPE Operation against a key holding the wrong kind of value` without
changing the value. `GET`, `INCR`, `DECR`, and `SET ... GET` also reject collections.
`MGET` returns null for each key of another type. `SET` without `GET` and `MSET`
replace any type; `SET ... GET` checks type before the `NX`/`XX` conditions.

Reading absent keys does not create collections. Removing the last field, element,
or member removes the entry, its expiration index, and its logical usage.
Mutations preserve TTL, while replacement by strings follows `SET` options.
Passive expiration occurs before the type check.

Quota adds `128 + key.len()` per entry and the following payload cost:

| Type | Logical payload cost |
| --- | --- |
| String | `value.len()` |
| Hash | Sum of `field.len() + value.len() + 64` per field |
| List | Sum of `value.len() + 32` per element |
| Set | Sum of `member.len() + 64` per member |

This budget does not measure RSS. Writes validate the complete result before
replacing the entry. A batch rejected for quota leaves no partial effect;
repeated fields and members are accounted for by final state.

The [network](network.md) RESP and response limits also apply to collections.
Ranges and arrays exceeding them close the connection without a partial response.
`LRANGE`, `HGETALL`, and `SMEMBERS` do not change data when exceeding this limit.

## Persistence and evidence

Snapshots share immutable payloads through `Arc`; a write creates a new postimage.
`Mutation::Put` includes the complete type and already resolved absolute deadline.
The AOF v1 codec retains the previous string representation (tag `1`) and uses
tags `3`, `4`, and `5` for hashes, lists, and sets. Replay validates types,
uniqueness, and quota; empty collections or records with duplicate fields/members are invalid.

```powershell
cargo test --locked --test collections
cargo test --locked --test tcp r05_
cargo test --locked --test collections_differential -- --ignored --exact collections_match_redis --nocapture
```

The differential test uses the Windows Sider binary and the Redis/CLI 8.10.1 image
pinned in `releases/plan.json`, with disposable instances. There are 2,188 exact
responses and 195 normalized arrays, totaling 2,383 comparisons. `HGETALL`
normalization preserves field/value pairs; both normalizations reject duplicates.
The four seeds are in the test, and each runs 256 operations with intermediate
reads and a final state check.

Native tests cover quota, TTL, types, binary data, arity, extreme indexes, codec,
and replay. [AOF writer integration](types-persistence.md) checks crashes,
compaction, recovery, and evolution of the string fixture. The command differential
test remains separate from this evidence and does not establish native Linux execution.
