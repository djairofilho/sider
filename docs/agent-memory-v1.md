# Sider agent memory contract version 1

Contract ID: `sider-memory/1`. Status: **architecturally accepted; awaiting versioning; not released**.
This is the deliverable for [#109](https://github.com/djairofilho/sider/issues/109).
It defines observables for the lifecycle in
[#110](https://github.com/djairofilho/sider/issues/110) and exact retrieval in
[#111](https://github.com/djairofilho/sider/issues/111). It does not describe a
capability of Sider 1.0.1. Contract version, Sider release version, durable header
version, durable record version, and internal transport version are distinct.

The normative words MUST, MUST NOT, and SHOULD describe implementation requirements.
The choices below freeze version 1 wire and durable behavior for
implementation in #110 and #111. The first deployment profile is controlled
loopback as defined by #105. Protected deployment remains gated by #115/#93.
Changes to these observables after acceptance require
a new contract version and migration policy.

## Contents

- [Identity and ownership](#identity-and-ownership)
- [Collection and record schema](#collection-and-record-schema)
- [Vector and distance rules](#vector-and-distance-rules)
- [Commands and replies](#commands-and-replies)
- [Filtering and ordering](#filtering-and-ordering)
- [Expiry and lifecycle](#expiry-and-lifecycle)
- [Resource admission](#resource-admission)
- [Errors and atomicity](#errors-and-atomicity)
- [Exact RESP2 examples](#exact-resp2-examples)
- [Existing contracts and durable evolution](#existing-contracts-and-durable-evolution)
- [Durable encoding and migration](#durable-encoding-and-migration)
- [Implementation test matrix](#implementation-test-matrix)
- [Minimum gates and diagnostics](#minimum-gates-and-diagnostics)
- [Integrator decisions](#integrator-decisions)

## Identity and ownership

A memory collection is one new typed value under one ordinary binary database
key. The collection key is its complete identity in Sider's single logical
database. There is no separate collection registry, hidden member key, database
selection, or server-generated ID. A member ID is unique within its collection,
including across namespaces. Replacing an ID can change its namespace.

The router visits only the collection key. It MUST use the existing
[routing version 1](sharding.md): FNV-1a64 over the first nonempty hash tag,
modulo the configured worker count. For example, `memory:{agent}:a` and
`memory:{agent}:b` colocate. Braces in member IDs, namespaces, or metadata do not
affect routing. Keys and IDs compare as unsigned bytes, without Unicode
normalization, case folding, or UTF-8 decoding.

Every memory command below accesses exactly one collection. There is no
multi-collection search, cross-shard merge, scatter/gather, online resharding,
or approximate index in version 1. Different connections to the same collection
serialize on its owning worker. Existing multi-key commands and transactions
retain their [single-shard rule](sharding.md#multi-key-commands-and-progress).

A namespace is an application eligibility field. It MUST NOT imply identity,
authorization, tenant ownership, or a protected partition. The current published
profile has no authentication, ACL, or TLS. Until
[#93](https://github.com/djairofilho/sider/issues/93), any client reaching the data
listener can request any collection or namespace. Under the future
[#115](https://github.com/djairofilho/sider/issues/115) boundary, each `MEM.*`
command requires its exact command grant and read or write scope for the
collection key before worker admission. Namespace and metadata filtering still
do not replace those grants. Internal replication and backup authorization is
separate from data command authority.

## Collection and record schema

All lengths below count bytes, not characters. Binary fields permit NUL, CRLF,
empty bytes where allowed, and invalid UTF-8. The schema has no implicit text
decoding. Numeric wire arguments use the canonical ASCII rules below.

| Collection field | Version 1 value |
| --- | --- |
| `key` | 1–256 bytes; ordinary database key |
| `schema_version` | Integer `1`, immutable |
| `dimension` | Integer 1–4,096, immutable |
| `encoding` | Implicit `F32LE`, immutable |
| `metric` | ASCII token `L2SQ`, immutable; no other metric |
| `provider`, `model`, `revision`, `transform` | Four separate binary strings, each 1–256 bytes, immutable |
| `records` | Map from binary member ID to the complete record below; may be empty |
| Collection deadline | Ordinary key expiry, independently optional |

The four model fields form one ordered identity tuple. Each incoming tuple MUST
equal all four stored byte strings. `revision` identifies the actual embedding
model revision, and `transform` identifies preprocessing, pooling, normalization,
and any other embedding transformation. Clients supply stable identities. Sider
compares bytes; it cannot verify that a provider, revision, or transformation was
used. A mutable provider alias such as `latest` does not make mixed vectors safe.
Sider performs no inference, network provider call, or automatic conversion.

| Record field | Version 1 value |
| --- | --- |
| `id` | 1–256 bytes |
| `namespace` | 0–256 bytes; mandatory argument, including an empty namespace |
| `content` | 0–65,536 bytes; retained exactly |
| `embedding` | Exactly `4 * dimension` bytes under the collection's immutable identity |
| `metadata` | 0–32 unique binary fields, each 1–64 bytes, with a typed scalar |
| `provenance` | 0–16 unique binary fields, each 1–64 bytes, with an opaque byte value |
| Record deadline | Optional positive Unix milliseconds fitting `i64`; resolved from the write's TTL |

Metadata types are ASCII tokens `BYTES`, `I64`, and `BOOL`. `BYTES` values have
0–4,096 bytes. `I64` is a signed integer in the inclusive `i64` range. Its wire
encoding is canonical ASCII decimal: `0`, or an optional `-` followed by a
nonzero digit and remaining digits; no `+`, whitespace, leading zero, or `-0`.
`BOOL` accepts only `0` and `1`. Type tokens are ASCII case-insensitive. Fields
are binary and case-sensitive. Duplicate fields are rejected even if their
types and values agree. Arrays, objects, null, floating metadata, and implicit
coercion are unsupported.

Provenance values have 0–4,096 bytes. Provenance is descriptive application data,
never evaluated as a search filter or authenticated evidence. Suggested fields
include `source`, `source_revision`, `content_hash`, and `created_at`; none is
reserved or mandatory. Source deletion or access changes require explicit
application action. Sider does not follow source URLs or verify hashes.

The sum of metadata and provenance field lengths and canonical value lengths
MUST be at most 32,768 bytes per record. Counts, field bounds, and this aggregate
bound apply together. Every reply orders fields by ascending unsigned binary
field bytes. Stored `I64` and `BOOL` values return in canonical decimal form.

## Vector and distance rules

`F32LE` means contiguous IEEE 754 binary32 elements, in coordinate order, each
encoded little endian, with no header, alignment padding, text, or base64.
For example, `[1.0, 0.0]` is `00 00 80 3f 00 00 00 00`. A length different
from `4 * dimension`, including any trailing byte, is rejected. Every exponent
of `0xff` is rejected: NaN of any payload/sign and positive or negative infinity
are invalid in stored and query vectors. All finite values, including subnormals,
zero vectors, negative coordinates, and signed zero, are valid. The original
finite bits MUST round-trip through reads and every durable path; neither zero
sign nor normalization is rewritten.

Distance is squared Euclidean distance:

```text
distance(q, x) = sum from i = 0 to dimension - 1 of (f64(q[i]) - f64(x[i]))^2
```

Conversion from each binary32 input to binary64 is exact. Evaluate coordinates
in ascending order with binary64 round-to-nearest, ties-to-even; each subtraction,
multiplication, and addition rounds separately. Start at positive binary64 zero.
Do not fuse multiply/add, use parallel reduction, flush subnormals, normalize,
take a square root, or use an epsilon to merge distances. This fixes score bits
across the supported platforms. With these input and dimension bounds the result
is finite and nonnegative. Version 1 has no cosine/inner-product option.

A distance reply is **eight binary bytes**, IEEE 754 binary64 little endian,
inside a RESP2 bulk string. It is not a RESP integer or decimal score string.
Clients must decode it as `F64LE`. Both stored and query identities and dimensions
must match; no search or replacement silently reembeds data.

## Commands and replies

These names are **Sider extensions**, with no Redis module or command compatibility
promise. Command names and enum tokens are ASCII case-insensitive; binary fields
are exact. Requests are nonempty RESP2 arrays of non-null bulk strings, including
numeric arguments. Counts and positive integers use canonical unsigned decimal
(`0`, or a nonzero digit followed by digits). Signed fields use the `I64` rules.
There are no implicit defaults or optional trailing arguments.

In the grammar, `identity` expands to `provider model revision transform`.
`metadata` expands to `metadata-count (field type value)*`; `provenance` expands
to `provenance-count (field value)*`. Counts give the exact number of following
groups. Extra, missing, or duplicate fields are errors. `ttl` is `-1` for
persistent, `KEEP` to retain the previous live record deadline, or positive
canonical milliseconds. `condition` is `UPSERT`, `NX`, or `XX`.

| Command grammar | Success or ordinary absence |
| --- | --- |
| `MEM.CREATE key 1 dimension L2SQ identity` | `+OK`; same existing schema also returns `+OK` without resetting records or TTL |
| `MEM.INFO key` | Seven-element array `[version integer, dimension integer, metric bulk, provider bulk, model bulk, revision bulk, transform bulk]`; missing key: null bulk |
| `MEM.PUT key id namespace content identity vector ttl condition metadata provenance` | Integer `1` for an accepted create or full replacement; integer `0` when the existence condition fails |
| `MEM.GET key id` | Six-element record array described below; missing collection or record: null bulk |
| `MEM.DEL key id` | Integer `1` when a live record is removed, otherwise `0` |
| `MEM.PTTL key id` | Integer remaining milliseconds; `-1` for a persistent record; `-2` for absent or expired collection/record |
| `MEM.EXPIRE key id milliseconds` | Integer `1` for a live record whose deadline is changed or whose nonpositive deadline deletes it; otherwise `0` |
| `MEM.PERSIST key id` | Integer `1` when a live record deadline is removed; otherwise `0` |
| `MEM.SEARCH key identity vector k namespace metadata` | Array of zero or more `[id bulk, distance F64LE bulk]` rows |

`MEM.GET` returns the positional array:

```text
[namespace bulk, content bulk, embedding bulk, deadline integer,
 metadata array of [field bulk, type bulk, value bulk],
 provenance array of [field bulk, value bulk]]
```

`deadline` is the absolute record Unix millisecond deadline, or `-1` when
persistent. It does not substitute the collection deadline. `MEM.PTTL` returns
the smaller remaining collection or record deadline, or `-1` only if both are
persistent. Positive submillisecond remaining time rounds down to `0`; absence
begins at the deadline. `MEM.INFO` omits record counts to avoid an implicit scan.

`MEM.CREATE` on an existing different memory schema returns a schema mismatch;
on another type it returns `WRONGTYPE`. Creating an empty collection charges
quota and leaves it present. Deleting the last member preserves schema and key
TTL. `MEM.PUT` never creates the collection. Its complete replacement removes
all previous content, namespace, metadata, and provenance. `KEEP` preserves the
old record deadline only if the member is live; for an absent member it means
persistent. Any other TTL replaces that deadline. The collection deadline is
preserved on all member operations.

Before deciding `NX` or `XX`, validate the entire request, tuple, and vector;
malformed data is not silently accepted because a condition fails. After that,
an unmet condition returns `0` before proposed-state quota, durable size, or
generation changes. A successful identical replacement still counts as a write
and invalidates watchers. The collection identity can change only by deleting
or replacing the entire key, then creating a new collection and reingesting.

`MEM.EXPIRE` accepts any canonical signed `i64` milliseconds. Nonpositive
values delete a live record; positive values resolve as for `MEM.PUT`. No
member patch, append, increment, auto-ID, batch ingestion, pagination, or
conditional delete is provided.

On a missing key, `MEM.SEARCH` returns an empty array after syntactic and finite
vector checks. Its vector must be nonempty, divisible by four, and within the
dimension bound; tuple/dimension comparison requires an existing schema.
`MEM.PUT` returns a missing-collection error. Other record commands return their
ordinary absence sentinel. Every command except `MEM.CREATE` returns `WRONGTYPE`
for an existing non-memory value before schema-dependent validation.

## Filtering and ordering

Search always requires one namespace byte string. A record is eligible only if
it is live at the operation's clock sample, its namespace is exactly equal to
the query namespace, and **every** query metadata triple matches. Zero metadata
triples means no metadata constraint. A missing field fails equality. Equality
requires the same type and value; `BYTES "1"`, `I64 1`, and `BOOL 1` differ.
Metadata predicate duplicates are invalid. Unsupported operators, OR, ranges,
substring/prefix matching, and provenance predicates are errors; no fallback
to a weaker predicate or unfiltered search is allowed.

Evaluate eligibility before retaining a candidate for top-k. Return at most `k`
eligible members, ordered by ascending exact distance, then ascending unsigned
binary member ID. A shorter ID sorts before a longer ID with the same prefix.
No map iteration order, namespace order, locale collation, insertion order,
or randomized tie breaking enters the result. An empty collection, no eligible
members, or an absent collection returns `*0\r\n`.

Search returns IDs and distances only, bounding replies independently of content.
A later `MEM.GET` observes a later operation and can see replacement, expiry,
or absence. There is no multi-command snapshot token. Clients needing source
attribution obtain provenance with `MEM.GET` and handle that race explicitly.

## Expiry and lifecycle

One owning-worker operation samples monotonic time and Unix milliseconds once.
A record is visible iff the collection and record deadlines are both later than
that sample, or absent. Visibility does not depend on background cleanup speed.
Relative deadlines are resolved before persistence; a nonpositive absolute
deadline, overflow of signed Unix milliseconds, or an unrepresentable monotonic
deadline rejects a positive-TTL command. `-1` in GET is therefore unambiguous.
Elapsed TTL is never restarted by replay, compaction, replication, backup,
restore, promotion, or offline routing migration.

Member content, searchable membership, metadata, namespace, vector, and record
deadline MUST become visible together. A successful write follows the existing
prepare, append/sync, apply, reply path. A record cannot be visible in reads but
missing from exact search, or retain an old vector after replacement. There is no
derived approximate index in this version.

Primary member cleanup produces `Expiration`-origin resolved member deltas for
its collection. Collection deletion uses the existing key tombstone. Each cleanup
round may resolve at most 64 expired member/key events per shard, on the existing
100 ms scheduling target. Do not process an unbounded chain of overdue records
in a read or insert. Passive `MEM.GET`/`MEM.PTTL` can resolve their requested
expired member; search hides all expired members and may defer physical removal
to bounded maintenance. Cleanup must discard stale deadlines from replacement.
Expiry bookkeeping must be bounded by retained records; obsolete events cannot
accumulate forever when the same ID is repeatedly rewritten.

Replicas hide elapsed members and collections but MUST NOT generate independent
client mutations or expiration deltas. They follow primary durable state.
After an expiration delta removes a member, clock rollback and restart must
not resurrect it. An expired member still physically retained may consume quota
until cleanup; targeting its ID with a new write may reclaim its old charge.

`MEM.*` commands are **not queueable inside `MULTI` in version 1**. Return
`ERR memory commands are not supported in MULTI`, invalidate the transaction,
and let existing `EXECABORT` behavior prevent execution. This prevents a queue
from multiplying scan/preparation budgets. Ordinary generic commands on the
collection key retain their existing transaction semantics.

`WATCH collection-key` observes the entire collection. Any successful member
write, deadline change, member deletion or expiry, collection replacement, or
collection expiry invalidates it. A member reaching its deadline is a conflict
even before physical cleanup: `WATCH` records the earliest future deadline among
members live at registration, and `EXEC` checks whether it elapsed, along with
the collection deadline. A member already expired before registration does not
abort that new watch merely because its physical record remains. An absent
delete, failed `NX`/`XX`, or an identical `MEM.CREATE` does not invalidate it.

## Resource admission

These are fixed version 1 maxima, with checked size arithmetic. Existing
operator RESP, response, dataset, queue, transaction, and AOF limits still
apply; the effective budget is the smaller applicable limit. No configuration
may silently raise a memory contract maximum. A later larger profile requires
a new version or explicitly negotiated contract revision.

| Budget | Maximum and counting rule |
| --- | --- |
| Input | 262,144 bytes of one complete memory RESP request, including all framing |
| Output | 131,072 bytes of one complete memory RESP reply, including all framing |
| Retained members | 1,024 per collection, including expired members awaiting removal |
| Collection logical charge | 8,388,608 bytes, including key, schema, records, and the charges below |
| `k` | 1–128; `0` and larger values are errors |
| Search record visits | At most 1,024 retained members, irrespective of selectivity |
| Search component work | `retained_count * dimension <= 1,048,576`, enforced on member admission and checked before scanning; includes ineligible and expired members |
| Search scalar/predicate work | At most 32 predicates per visited member, at most 32,768 aggregate stored metadata/provenance bytes per member, bounded further by collection charge |
| Search scratch | At most 262,144 bytes of requested allocation capacity, excluding borrowed immutable record payloads and the separately bounded reply buffer |
| Lifecycle scratch | At most 16,777,216 bytes of requested allocation capacity per owning-worker operation, excluding shared immutable payloads, persistent state, and writer-owned buffers |
| New persistent vector index | Zero; version 1 performs exact scans |

The component bound deliberately charges worst-case work before evaluating
filters. `MEM.PUT` MUST reject a new member when its proposed retained count
times dimension would exceed this bound; replacement, deletion, and expiration
may reduce the count. Thus every accepted collection remains searchable under
the fixed version 1 work budget. A dimension-1,024 collection can retain 1,024
members, while a dimension-4,096 collection can retain at most 256. Filters and
a small `k` do not raise the budget. Search repeats the check defensively after
restore and replay; it never returns a partial top-k, cursor, or silently
truncated scan. Search uses a bounded top-k structure and MUST NOT allocate all
vectors or distances.

The logical charge formula is normative, platform-independent accounting:

```text
collection = len(key) + 128 + 256
             + len(provider) + len(model) + len(revision) + len(transform)
             + sum(record_charge)
record_charge = 256 + len(id) + len(namespace) + len(content) + 4 * dimension
                + sum(64 + len(metadata_field) + metadata_value_charge)
                + sum(64 + len(provenance_field) + len(provenance_value))
metadata_value_charge = len(value) for BYTES, 8 for I64, 1 for BOOL
```

The 256-byte record charge includes logical allowance for member-map membership,
TTL scheduling, and exact-search references. The 256-byte schema charge includes
schema/map bookkeeping. Both persistent records and expiry bookkeeping consume
quota even when their raw payloads are empty. Any future derived index must have
a separately versioned charge and migration rules; it cannot consume uncharged
logical storage under this contract.

Apply the existing deterministic per-shard partition of
`SIDER_MAX_DATASET_BYTES`. There is no borrowing across shards or automatic
eviction. Replacement admission subtracts the old record charge and adds the
new charge, then checks member count, collection charge, and shard quota before
mutating. Deletion and expiry release the precise retained charge. Restore and
replay validate the same formula. A lower configured quota can reject recovery;
it does not discard selected records to fit.

Before sending a mutation to the writer, measure its bounded member delta and
the full resolved-batch AOF payload against the configured AOF record limit,
including tags, counts, schema, edits, and key TTL but excluding the existing
12-byte durable frame. Snapshot/full-create encoding is separately preflighted
against the same record limit. Collection logical charge is not an AOF size
formula. The writer must preflight encoded size before reserving its body. A
lower `SIDER_AOF_MAX_RECORD_BYTES` can make a valid record update fail; this
returns the existing AOF record-limit error with no client mutation. No-AOF
operation still enforces all memory limits. Replay and snapshot installation use
the same quota and schema validation, without materializing unrelated records.

Before encoding a memory reply, calculate the whole RESP frame size, node count,
depth, and bulk limits. Exceeding a configured output constraint returns the
reply-limit error without a partial frame. The error itself must fit the existing
minimum error budget. Input decoder limit failure retains the existing close
behavior; it is not converted into a recoverable command error.

These are logical charges and explicit allocation/work ceilings, **not an RSS
cap or a latency guarantee**. Input copies, connection buffers, bounded worker
queues, copy-on-write snapshot retention, AOF encoding and queueing, replication
journals, and backup images have separate existing bounds. Snapshot capture can
retain old collection images across concurrent replacements. #108 and #112 must
measure that amplification, RSS, worker occupancy, and queue saturation. The
worker has no provider I/O or unbounded scan to hide behind a request deadline.

## Errors and atomicity

These complete ASCII error payloads are stable. RESP adds `-` and trailing CRLF.
Errors MUST NOT echo binary arguments, content, model identities, or provenance.
Existing protocol framing, unavailable-worker, authentication, and I/O failure
behavior remains governed by its respective layer.

| Rejection | Error payload |
| --- | --- |
| Wrong arity | `ERR wrong number of arguments for 'mem.<name>' command` with the actual lowercase suffix |
| Invalid numeric grammar other than TTL, count, field, type, condition, or duplicate field | `ERR invalid memory argument` |
| Unsupported collection version | `ERR unsupported memory schema version` |
| Unsupported metric | `ERR unsupported memory metric` |
| Existing collection has different CREATE schema | `ERR memory schema mismatch` |
| Missing collection for PUT | `ERR memory collection does not exist` |
| Wrong existing type | `WRONGTYPE Operation against a key holding the wrong kind of value` |
| Identity tuple differs | `ERR memory model identity mismatch` |
| Vector length differs from schema, or is malformed without a schema | `ERR memory vector length mismatch` |
| NaN or infinity | `ERR memory vector contains non-finite value` |
| `k` outside 1–128 | `ERR memory k is out of range` |
| Invalid/unrepresentable relative TTL | `ERR invalid memory ttl` |
| Unsupported SEARCH predicate type/operator (including tokens other than the three scalar types) | `ERR unsupported memory filter` |
| Field/content/dimension/count/request/member/collection/scratch maximum | `ERR memory limit exceeded` |
| Proposed or retained member count times dimension exceeds search work maximum | `ERR memory search work limit exceeded` |
| Memory reply exceeds effective encoding/output limit | `ERR memory reply limit exceeded` |
| Owning shard dataset quota | `OOM dataset memory quota exceeded` |
| AOF payload size | `ERR AOF record limit exceeded` |
| Client mutation on replica | `READONLY You can't write against a read only replica.` |
| Memory command in MULTI | `ERR memory commands are not supported in MULTI` |

Validation order is: bounded framing; arity and left-to-right argument parsing,
including argument maxima and finite vector checks; authorization when present;
transaction/routing and replica write policy; collection existence/type; immutable
schema and tuple; collection-dependent vector length; existence condition;
checked deadline; proposed member/collection/shard charge; work/scratch/reply
preflight; durable record preflight. Unsupported version or metric is diagnosed
at parsing before lookup. Resolve a missing collection's read/search sentinel
before schema-dependent checks. Invalid vectors and fields never reach mutation.

A rejected client operation cannot append a client batch, advance its generation,
or leave a partial replacement. Existing independent expiration maintenance may
append expiration-origin removals; rejection is not a guarantee that wall time
stops or that unrelated expiry cannot progress. Failed writer append/sync closes
admission according to [AOF](persistence.md#acknowledgment-and-failure).
A disconnect or timeout after acceptance can leave a committed complete record.
There is no exactly-once token. Applications must reconcile the ID and state
before retries; retrying a relative TTL changes its deadline.

## Exact RESP2 examples

The following Python `bytes` literals specify the complete wire octets.
`\r\n` and `\xNN` represent actual bytes after decoding the literal, not text
sent to Sider. Every request field, including a count, is a bulk string. No
placeholder or ellipsis occurs in a wire fixture. These sequential examples use
a two-dimensional persistent collection, an empty metadata/provenance map,
identity `(p, m, r, t)`, namespace `n`, and binary content `ff 00 0d 0a`.

Create and inspect:

```python
request = b'*9\r\n$10\r\nMEM.CREATE\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\n2\r\n$4\r\nL2SQ\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n'
reply = b'+OK\r\n'

request = b'*2\r\n$8\r\nMEM.INFO\r\n$1\r\nc\r\n'
reply = b'*7\r\n:1\r\n:2\r\n$4\r\nL2SQ\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n'
```

Put record ID `00 61`, vector `[1, 0]`, and read exact stored bytes:

```python
request = b'*14\r\n$7\r\nMEM.PUT\r\n$1\r\nc\r\n$2\r\n\x00a\r\n$1\r\nn\r\n$4\r\n\xff\x00\r\n\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n$8\r\n\x00\x00\x80\x3f\x00\x00\x00\x00\r\n$2\r\n-1\r\n$6\r\nUPSERT\r\n$1\r\n0\r\n$1\r\n0\r\n'
reply = b':1\r\n'

request = b'*3\r\n$7\r\nMEM.GET\r\n$1\r\nc\r\n$2\r\n\x00a\r\n'
reply = b'*6\r\n$1\r\nn\r\n$4\r\n\xff\x00\r\n\r\n$8\r\n\x00\x00\x80\x3f\x00\x00\x00\x00\r\n:-1\r\n*0\r\n*0\r\n'
```

Search `[0, 0]` in namespace `n`, `k=1`, no predicates. Distance `1.0` is the
binary64 payload `00 00 00 00 00 00 f0 3f`:

```python
request = b'*10\r\n$10\r\nMEM.SEARCH\r\n$1\r\nc\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n$8\r\n\x00\x00\x00\x00\x00\x00\x00\x00\r\n$1\r\n1\r\n$1\r\nn\r\n$1\r\n0\r\n'
reply = b'*1\r\n*2\r\n$2\r\n\x00a\r\n$8\r\n\x00\x00\x00\x00\x00\x00\xf0\x3f\r\n'
```

Replace the same ID with metadata `kind:BYTES=note`, `rank:I64=-7`,
`seen:BOOL=1`, and provenance `source=ff`; then query the rank equality:

```python
request = b'*25\r\n$7\r\nMEM.PUT\r\n$1\r\nc\r\n$2\r\n\x00a\r\n$1\r\nn\r\n$0\r\n\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n$8\r\n\x00\x00\x80\x3f\x00\x00\x00\x00\r\n$4\r\nKEEP\r\n$2\r\nXX\r\n$1\r\n3\r\n$4\r\nkind\r\n$5\r\nBYTES\r\n$4\r\nnote\r\n$4\r\nrank\r\n$3\r\nI64\r\n$2\r\n-7\r\n$4\r\nseen\r\n$4\r\nBOOL\r\n$1\r\n1\r\n$1\r\n1\r\n$6\r\nsource\r\n$1\r\n\xff\r\n'
reply = b':1\r\n'

request = b'*13\r\n$10\r\nMEM.SEARCH\r\n$1\r\nc\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n$8\r\n\x00\x00\x00\x00\x00\x00\x00\x00\r\n$1\r\n1\r\n$1\r\nn\r\n$1\r\n1\r\n$4\r\nrank\r\n$3\r\nI64\r\n$2\r\n-7\r\n'
reply = b'*1\r\n*2\r\n$2\r\n\x00a\r\n$8\r\n\x00\x00\x00\x00\x00\x00\xf0\x3f\r\n'
```

A finite vector with only one coordinate is a length error. A two-coordinate
vector containing positive infinity is rejected before search:

```python
request = b'*10\r\n$10\r\nMEM.SEARCH\r\n$1\r\nc\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n$4\r\n\x00\x00\x00\x00\r\n$1\r\n1\r\n$1\r\nn\r\n$1\r\n0\r\n'
reply = b'-ERR memory vector length mismatch\r\n'

request = b'*10\r\n$10\r\nMEM.SEARCH\r\n$1\r\nc\r\n$1\r\np\r\n$1\r\nm\r\n$1\r\nr\r\n$1\r\nt\r\n$8\r\n\x00\x00\x80\x7f\x00\x00\x00\x00\r\n$1\r\n1\r\n$1\r\nn\r\n$1\r\n0\r\n'
reply = b'-ERR memory vector contains non-finite value\r\n'
```

At an injected Unix clock of exactly `1000`, set a ten-millisecond record TTL.
Before advancing time, GET's record deadline is `1010`, and PTTL is `10`.
At clock `1010`, GET is null even if cleanup has not run:

```python
request = b'*4\r\n$10\r\nMEM.EXPIRE\r\n$1\r\nc\r\n$2\r\n\x00a\r\n$2\r\n10\r\n'
reply = b':1\r\n'

request = b'*3\r\n$8\r\nMEM.PTTL\r\n$1\r\nc\r\n$2\r\n\x00a\r\n'
reply = b':10\r\n'

request = b'*3\r\n$7\r\nMEM.GET\r\n$1\r\nc\r\n$2\r\n\x00a\r\n'
reply = b'$-1\r\n'
```

To test explicit deletion independently, reset the clock, reinsert the first
persistent PUT fixture, then delete twice:

```python
request = b'*3\r\n$7\r\nMEM.DEL\r\n$1\r\nc\r\n$2\r\n\x00a\r\n'
reply = b':1\r\n'

request = b'*3\r\n$7\r\nMEM.DEL\r\n$1\r\nc\r\n$2\r\n\x00a\r\n'
reply = b':0\r\n'
```

## Existing contracts and durable evolution

The audit baseline is source `0f0f232f56fd42b88a56e61954f2bc892458f04d`, containing
Sider 1.0.1 plus its public-repository release preflight fix. Existing observables
below are grounded in local source and repository contracts, not new test results.

| Boundary | Existing contract | Required effect in #110/#111 |
| --- | --- | --- |
| RESP/parser/reply | [RESP2](resp.md), `src/command/{parser,mod,reply}.rs`; default 4 MiB frame, 1 MiB bulk, 1,024 nodes, depth 16 | Add bounded flat requests and nested replies; preserve binary input, parser/worker separation, and atomic encoding. Existing decoder limits can be stricter. |
| Typed storage | `src/storage/value.rs`; five value types, shared immutable snapshots; `src/storage/mod.rs` charges key length plus 128 bytes | Add a memory type that can remain empty, with immutable schema, bounded members/deadline index, exact charge, and no worker-external map access. |
| Routing | [Sharding](sharding.md), `src/storage/routing.rs` | Visit one collection key; preserve routing version 1 and quota partitions. No global memory registry on worker zero. |
| Resolved mutations | `src/storage/mutation.rs`; `Put` contains complete typed post-image and absolute key deadline; prepare precedes durable confirmation | Add one guarded member-delta mutation per touched collection key; retain full typed images for snapshot/create. Do not replay raw commands, relative TTL, or conditions. |
| AOF | [Persistence](persistence.md), [typed values](types-persistence.md); record version 1, header versions 1/2/3, mutation tags 1–6 | Record version 2, headers 4/5, mutation tags 7/8 below. Validate complete delta and proposed quota before applying any edit. Retain bits; CRC is integrity, not authentication. |
| Compaction/snapshot | `src/storage/snapshot.rs`, [persistence](persistence.md); global barrier, ordered key post-images, seal, sequence plus delta | Include empty collections and retained elapsed deadlines. Sort member IDs and both field maps. Copy-on-write values cannot expose half a replacement. |
| Replication | [Replication](replication.md), `src/replication/{protocol,session}.rs`; transport v1; Hello compares exact Sider and record versions, layout and capacities; payload uses AOF codec | Transport v2 and record v2 negotiation below precede transfer. Apply a whole delta atomically; no independent replica expirations/provider calls. Preserve asynchronous primary acknowledgement. |
| Backup | [Backup](backup.md), `src/persistence/backup/manifest.rs`; manifest schema 1 advertises AOF header/record version; current export writes sealed header v2 without role/epoch | Manifest schema 2, header 4, record 2 below. Verify before restoring into a new directory. Do not inherit authentication, role, or reset member TTL. |
| Offline migration | [AOF migration](aof-migration.md), `src/persistence/migration.rs`; source preserved, destination new, layout changes offline | Rehash whole collection keys; preserve schema/vector bits, metadata types, provenance, and absolute deadlines. Reject unsupported memory downgrade or insufficient destination quota. |
| Transactions/WATCH | [Transactions](transactions.md), `src/storage/watch.rs` | Reject MEM queueing as specified. Extend collection WATCH conflict deadlines to members; generic collection replacement/deletion still works inside one-shard transactions. |
| Metrics | [Metrics](metrics.md); `INFO` schema 1, bounded aggregates, no key/channel labels | INFO schema 2 and frozen bounded counters below; used-byte gauges include memory charge after apply. Never label by collection, ID, namespace, provider, or provenance. |

Generic commands MUST retain their current type rules. `DEL` removes the whole
memory collection and counts one key. `EXISTS` counts the key even when it has no
live members. `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, and `PERSIST` address the
collection deadline; they do not enumerate member IDs or remove member deadlines.
`GET`, integer operations, and commands for other collections return their
existing `WRONGTYPE` behavior. `MGET` retains its existing null entry for a
non-string value. `SET`/`MSET` may replace a memory key with a string under their
existing conditions and expiry rules, releasing its full memory charge. No new
type check may silently block a replacement valid for existing Redis-like types.
Generic multi-key routing still checks the entire key list before enqueue.

Old format readers reject unknown mutation tags, but memory MUST NOT simply be
appended under a header advertising only the old capabilities. An incompatible
binary must refuse the directory **before append, compaction, migration output,
or tail repair**, including a torn memory record at EOF. New readers accept
supported legacy data through the offline migration path below. Opening old data
is not permission to modify its original files or upgrade them in place.

Duplicate or unsorted member/field records, non-finite vectors, malformed lengths,
unknown versions/types, invalid deadline flags, extra bytes, and quota violations
are corruption, never candidates for lossy recovery.

## Durable encoding and migration

Record version **2** retains the existing 12-byte length/complement/CRC framing,
record tags `1` snapshot, `2` seal, `3` resolved batch, sequence rules, origin
tags, and mutation tags `1`–`6`. New mutation tag **7** is a complete memory
image for snapshot or creation; **8** is a guarded memory delta for an existing
collection. These tags are unallocated in the audit baseline. Every batch still
contains at most one mutation per database key and belongs to one shard.

All integer fields are little endian. A `blob` is `u32 length` followed by exactly
that many bytes. Lengths/counts are checked against available bytes and contract
bounds before allocation. A `deadline` is `u8 flag` then `i64 unix_ms`; absent
means both zero, present means flag `1` and a positive representable value for
member TTL. Outer key TTL retains existing signed deadline rules. Unknown flags
are corruption. The complete new binary grammar is:

```text
descriptor = u32 schema_version(1), u32 dimension,
             u8 encoding(1=F32LE), u8 metric(1=L2SQ),
             blob provider, blob model, blob revision, blob transform
scalar = u8 type(1=BYTES), blob value
       | u8 type(2=I64), i64 value
       | u8 type(3=BOOL), u8 value(0 or 1)
record = blob id, blob namespace, blob content,
         u32 vector_dimension, [4 * vector_dimension raw F32LE bytes],
         deadline member_deadline,
         u32 metadata_count, [blob field, scalar] in field order,
         u32 provenance_count, [blob field, blob value] in field order
tag7 = u8(7), blob key, descriptor,
       u32 member_count, [record] in member ID order, deadline key_deadline
edit = u8(1=upsert), record | u8(2=remove), blob id
tag8 = u8(8), blob key, descriptor,
       u32 edit_count, [edit] in member ID order, deadline key_deadline
```

`vector_dimension` must equal descriptor dimension; it makes each record
self-delimiting without worker schema access during codec decoding. Exact raw
vector bits are preserved. Scalar bits and canonical typed values are validated
before replay. An empty complete image is legal. Tag 8's descriptor MUST match
the existing memory collection exactly; missing/wrong-type/mismatched state is
corruption. It never creates or changes a schema. Edit IDs are strictly ordered
and unique. `edit_count` is 0–64; zero changes only the key deadline, supporting
generic `EXPIRE`/`PERSIST` without serializing all members. Client member commands
produce one edit. Expiration rounds can produce up to 64 removes per collection,
within the shared shard event budget. Removal of an already absent member is
corruption: repeated records must be caught by sequence checks, not hidden.

`MEM.PUT`, `MEM.EXPIRE`, and `MEM.PERSIST` resolve complete upsert records with
absolute deadlines. `MEM.DEL` and member expiration resolve removes. The key
deadline is the resolved final value, including when unchanged. Generic `DEL`
uses existing tag 2; replacing memory with a string uses existing tag 1. New
memory creation uses tag 7 with zero members. Identical CREATE emits no mutation.
Snapshots MUST contain tag 7, never tag 8. Replay validates the entire batch,
schema guards, and net quota before atomically applying any key/member edit.
An elapsed upsert still replaces the prior ID completely; visibility hides the
new elapsed record instead of revealing the earlier value. Replay/replication
MUST retain intermediate expired memory state and apply deltas in sequence,
without pruning it against recovery wall time between records. A later historical
TTL change may make that state persistent again, and a later remove must still
validate against it. After recovery/full installation, expired memory state
remains physically charged and hidden until primary cleanup; a replica waits for
primary deltas/tombstones. Offline migration may prune only the final recovered
state. This avoids making delta replay depend on the reader's clock.

For a member operation, encoded delta size is O(affected member bytes + schema
bytes), independent of unrelated collection members. No full image is appended
for routine member writes. Snapshot encoding is O(collection size), as required
for full backup/recovery. Internal immutable map/preparation algorithms may
still incur bounded O(member count) bookkeeping; #108 must measure that cost.
Every snapshot image must fit the configured record limit before accepting the
operation that would enlarge it, even when its incremental delta fits. A node
must not acknowledge a dataset that cannot later be snapshotted under its own
configuration. Charge and encoded-image length can be maintained with checked
incremental counters; do not rescan all payload bytes for each append.

New nonreplicated AOF files and backups use header **4**, length **36**:

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 8 | `SIDERAOF` |
| 8 | 4 | Header version `4` |
| 12 | 8 | Snapshot sequence |
| 20 | 4 | Shard count |
| 24 | 4 | Routing version `1` |
| 28 | 4 | Record version `2` |
| 32 | 4 | Existing CRC-32/ISO-HDLC over bytes 0–31 |

Replicated files use header **5**, length **56**. Bytes 0–31 follow header 4,
with header version `5`. Offset 32 has the existing `u8` role (`1` primary,
`2` replica); offsets 33–35 are zero; offsets 36–51 contain the 16-byte epoch;
offset 52 contains the CRC of bytes 0–51. Unknown header/record/routing versions
are rejected before record scanning. Source 1.0.1's `read_header_with_layout`
rejects versions 4/5 at the header, before `writer` can truncate an incomplete
tail. A new empty directory also receives header 4/5; capability cannot depend
on whether its first memory record has been appended yet.

The exact header-4 fixture below has sequence zero, one shard, routing 1,
record 2, and its CRC. The framed snapshot fixture creates empty key `c`,
dimension 2, identity `(p, m, r, t)`, no key TTL. It is one snapshot record,
not a complete sealed AOF file; a seal must follow before recovery accepts it.

```text
header4 (36 bytes):
53 49 44 45 52 41 4f 46 04 00 00 00 00 00 00 00 00 00 00 00
01 00 00 00 01 00 00 00 02 00 00 00 b2 e1 d1 d1

empty_snapshot_record (62 bytes):
32 00 00 00 cd ff ff ff 62 41 9a 44 01 07 01 00 00 00 63
01 00 00 00 02 00 00 00 01 01 01 00 00 00 70 01 00 00 00 6d
01 00 00 00 72 01 00 00 00 74 00 00 00 00 00 00 00 00 00 00
00 00 00
```

Internal transport version is **2**, preserving its 24-byte frame header and
all existing message tags. Hello advertises record version 2. In `Export`, add
`u32 record_version` immediately after the existing length-prefixed Sider version
and before `max_record_bytes`; require `2`. Other payload layouts remain the
same, except snapshot/batch records use the new codec. Version 1 peers reject
the frame header. Continue requiring exact Sider version, record version,
layout, and sufficient receiver capacity before streaming. No mixed-version
rolling upgrade is supported.

Backup manifest schema is **2**, with the same field names as schema 1 and
`aof_header_version: 4`, `record_version: 2`. The snapshot is a sealed header-4
file without source role/epoch. New export/verify/restore require matching
transport, Sider, and record versions. An old backup is verified with its matching
legacy tool, restored into a disposable legacy directory, then offline migrated;
it is not silently reinterpreted as a schema-2 backup.

The sole legacy upgrade path is the existing `sider-aof-migrate` offline workflow,
extended to accept header 1/2/3 with record 1 and header 4/5 with record 2. Stop
the source, acquire its exclusive lock, read without repair, freeze the clock
for validation, and write a sealed header-4 destination outside the source tree
that does not exist. Preserve sequence, layout unless explicitly changed,
vector bits, type/provenance data, and absolute TTL. As in the existing migrator,
the destination intentionally does not inherit role/epoch. Verify the generated
bytes, sync file and publication directories, then publish atomically and report
source/destination layout, record versions, hashes, and retained counts.
Expired state may be omitted using the frozen migration clock; time spent
migrating never extends a retained deadline.

The memory-capable server refuses legacy directories with an explicit migration
required diagnostic before repair or writer startup. Migration preserves source
bytes on success/failure and reports incomplete tails without truncation. There
is no in-place upgrade, automatic downgrade, or output to headers 1/2/3, even
for a dataset with no memory. Retain a verified legacy backup for operational
rollback; reopening it restores its old cut, not later memory writes. A replica
deployment upgrades/migrates both nodes offline and establishes a new compatible
stream identity before reconnecting; old continuation cursors cannot cross the
format upgrade.

## Implementation test matrix

These are required future cases, not evidence of implemented memory behavior.
Use injected clocks, ephemeral ports, bounded allocation/work counters, literal
RESP transcripts, an independent brute-force oracle, and frozen durable fixtures.
Record source SHA, platform, configuration, command, and observed result. Do not
reuse 1.0.1 release receipts as evidence for this contract.

| Area | Accepted fixtures | Rejected/failure fixtures and assertions | Owner |
| --- | --- | --- | --- |
| Wire codec/parser | Every literal above, every fragmentation boundary, pipelines, empty binary content, CRLF/NUL/non-UTF-8 IDs and fields | Null or non-bulk arguments, truncated frames, wrong arity/count, extra args; protocol errors close, command errors preserve next PING | #110 |
| Schema/model | Idempotent identical CREATE; empty retained schema; separate collections per model | Version/metric/dimension bounds; each tuple component mismatch; CREATE conflict does not erase members/TTL | #110 |
| Vectors | Dimension 1 and 4,096, all finite signs/subnormals/signed zero, exact bit readback | Every NaN class, ±infinity, 0/short/long/trailing-byte vectors; no change to state, quota or client AOF sequence | #110/#111 |
| Scalars/provenance | BYTES empty/non-UTF-8, i64 endpoints, BOOL 0/1, sorted read fields, opaque source values | Duplicate fields, types/coercion, null/nested values, bad decimal, aggregate 32 KiB plus one, field/value/count maxima plus one | #110 |
| Lifecycle | Full replace clears absent old fields; UPSERT/NX/XX; KEEP; namespace move; last delete retains schema | Missing collection, wrong type, failed condition, malformed replacement; old content/vector/deadline remain together | #110 |
| TTL | Relative resolution, exact deadline boundary, effective minimum key/member PTTL, persistent/absent sentinels, stale rewrite deadline | Overflow/unrepresentable time; expiry after response; cleanup limit 64; repeated overwrite cannot grow stale events; clock rollback cannot resurrect a durably removed member | #110 |
| WATCH/transactions | Generic key operations and WATCH on memory collections; member deadline conflict before cleanup | MEM queueing invalidates MULTI; no prefix; no-op condition/create does not invalidate; read-only replica mutation rejection | #110 |
| Routing/isolation | Binary FNV/hash-tag vectors; ID braces do not route; independent shard progress | Generic multi-key CROSSSLOT before enqueue; no hidden key registry; namespace is not authorization | #110 |
| Filter eligibility | Empty namespace/map; typed AND; missing-field exclusion; byte-exact namespace; ineligible nearest record excluded | Unsupported operators, repeated predicates, type mismatch, namespace mismatch; no fallback or leakage with selective filters | #111 |
| Exact ordering | Independent binary64 sequential oracle; equal-distance binary IDs, prefix IDs, k above eligible count, empty result | No fused/parallel reduction drift; non-finite query; k 0/129; randomized insertion order gives identical bytes | #111 |
| Resource admission | Exact field/frame/member/charge/work limits, old-charge reclamation, no-AOF profile, lower configured limits | Member insertion above the dimension/count work product, other limits plus one, integer overflow, hot full shard while another has space; no eviction/partial result; bounded top-k/scratch; oversized reply returns one complete error | #110/#111 |
| Durable size/failure | Complete snapshot and bounded member delta below record limit; `always` and `everysec` policies | Delta and snapshot limits plus one, append/sync failures, response loss; no partial state/search membership; crash before/after append/sync/apply/reply; write bytes independent of unrelated content | #110 |
| Replay/compaction | Frozen legacy fixtures, complete memory schema/record bits/deadlines, concurrent snapshot delta, late recovery after historical TTL extension | Truncate every new record prefix; duplicate edits/members/fields, missing/mismatched delta target, invalid floats/counts/deadlines/order/checksum, unsupported schema; previous or complete new state only; no intermediate clock-dependent pruning | #110 |
| Backup/migration | Mixed old types/memory, empty memory, expired records, new shard layout, verified restore into new directory | Bad manifest/version/seal/hash, insufficient shard quota, unsupported downgrade; source files unchanged; TTL elapsed during capture/restore | #110 |
| Replication | Full sync, suffix batches, replacement/delete/expiry, restart and fenced promotion, bit-identical exact queries | Version/layout/capacity mismatch before install; interrupted full sync, journal gap, primary expiration only; no primary wait-for-replica claim | #110/#111 |
| Metrics/load | Used-byte accounting after apply/replay/cleanup; bounded work counters; declared mixed ingestion/search workload | Rejects counted without raw-data labels; queue saturation, repeated snapshot/replacement retention, allocation bounds/RSS measured separately | #108/#110/#112 |

Persistence, crash, backup/restore, migration, and bit-identical distance cases
must run on the supported Windows and Linux targets before publication. #110
must also pass existing type, transaction, routing, and compatibility checks.
#111 must compare its exact implementation to the independent oracle and verify
search consistency after every lifecycle and durable path. Performance/relevance
claims belong to #112 with corpus, workload, limits, SHA, and measurements.

## Minimum gates and diagnostics

#110 MUST add a `memory_lifecycle` gate to `releases/gates.json` and its chosen
release tasks, with argv
`cargo test --locked --test agent_memory -- --ignored --exact release_memory_lifecycle_gate --nocapture`
and a 1,200-second timeout. #111 MUST add `memory_search`, with argv
`cargo test --locked --test agent_memory_search -- --ignored --exact release_memory_search_gate --nocapture`
and a 1,200-second timeout. Both follow existing release-input and gate-receipt
contracts: exact source SHA/platform/configuration, explicit assertions that cases
ran, nonzero failure if missing/skipped, no success receipt from an empty test
selection, and process cleanup before receipt publication.

The lifecycle gate covers every #110 matrix row, literal fixtures and golden
new-format bytes, complete mixed-type backup/restore, full/incremental replication,
legacy migration and old-binary refusal against intact **and every truncated new
record prefix**. It hashes source files before/after refusal/migration. It includes
crashes at existing append, sync, response, snapshot, and generation-publication
boundaries, preserving all acknowledged `always` writes. The retrieval gate
covers every #111 row with an independent oracle, deterministic score bytes on
both platforms, post-lifecycle/durable consistency, no filter leakage, and
counter-based work/allocation bounds. Tests must distinguish wall-clock replay
pruning from final visibility using a historical expire-then-persist fixture.

Before integration, run focused native/TCP cases and `cargo xtask check`. Before
publication, both new gates must pass independently on Windows and Linux with
new receipts. Existing compatibility, types, transactions, sharding, crash,
recovery, migration, replication, soak, and package gates remain required by the
release manifest; changing format cannot waive them. Gate registration also
requires `cargo xtask check --tools`. #112 workload/relevance evidence is required
before making measured capacity/relevance claims; gate timeouts are test-run
deadlines, not promised command latency.

`INFO` advances `metrics_schema_version` to **2** while preserving existing field
meaning. Add the following aggregate saturating `u64` counters to `stats`:
`memory_searches_total` (completed successful searches including empty replies),
`memory_search_record_visits_total`, `memory_search_components_total` (actual
distance coordinates evaluated), `memory_rejections_total` (recoverable memory
command errors, not decoder failures), and `memory_expired_members_total`
(primary applied expiration removals, not merely hidden members). Counters reset
with the instance. No identity-derived labels or per-collection metrics exist.
Rejected preflight work adds zero visits/components. Used-byte gauges retain
logical rather than RSS meaning and include every physically retained memory
record, including expired state, after apply.

`INFO config` and `--diagnose` publish fixed numeric limits as
`memory_contract_version:1`, `memory_max_dimension:4096`,
`memory_max_members:1024`, `memory_max_collection_bytes:8388608`,
`memory_max_k:128`, `memory_max_search_components:1048576`,
`memory_max_request_bytes:262144`, `memory_max_reply_bytes:131072`,
`memory_max_search_scratch_bytes:262144`, and
`memory_max_lifecycle_scratch_bytes:16777216`. Existing configured RESP/AOF/quota
fields allow the operator to calculate smaller effective budgets. No new memory
environment variables raise these maxima. Existing metrics and `--diagnose`
response bounds, redaction, and offline non-readiness semantics remain in force.

## Integrator decisions

The integrator accepts `sider-memory/1` for versioning. Wire,
tags, versions, binary grammar, delta/snapshot behavior, offline migration, hard
limits, metric names, and minimum gates are chosen here; #110/#111 must not
silently choose different observable behavior. Golden fixtures and implementation
validation are deliverables of those issues, not unresolved architecture choices.

The first memory release is limited to the controlled loopback profile established
by #105. Namespaces remain filters and every reachable client can access every
collection. A protected deployment requires the #115/#93 controls and explicit
command/key grants for every MEM command. Release numbering and milestone scheduling follow
the existing manifest policy. Admission maxima still need #108/#112 workload
evidence before any production capacity or latency claim.

This contract does not certify implementation, publication, or a release gate.
The staged native lifecycle has Store/worker, format, and explicit record-2 AOF
tests. A library-only `AofConfig::with_memory_profile()` now exercises header 4/5
recovery, guarded append, and compaction. The default server, CLI, and environment
remain on record 1 and reject headers 4/5; backup, replication transport, and
offline migration refuse the new profile. The TCP parser exposes no MEM commands.
#109 closes after this contract is versioned; #110 and #111 remain separate
implementation and evidence tasks.
