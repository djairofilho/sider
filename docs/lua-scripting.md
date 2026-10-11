# Proposed bounded Lua scripting contract

Status: **proposed, not implemented**. This document scopes
[issue #95](https://github.com/djairofilho/sider/issues/95) against baseline
`0f0f232`. It freezes observable choices for a first implementation; it does not
close the issue, establish a sandbox guarantee, or approve a release. Runtime and
operating-system containment must pass the feasibility gate below before this
feature can be enabled. Scripting defaults to disabled.

The [isolated runtime spike](../spikes/lua-containment/README.md) exercised the
language limits on Windows 11 and Ubuntu 24.04 under WSL2. It demonstrated Linux
cgroup resource controls as root and, subsequently, as uid 65534 in a private
systemd delegated subtree. Windows process-count denial and cumulative user/kernel
CPU accounting were demonstrated through a safe dependency, including suspended
helper startup. The strict Windows memory gate failed: reported Job peak committed
memory exceeded the configured 64 MiB ceiling even with suspended startup.
The [controlled Windows repro](../spikes/lua-containment/WINDOWS-MEMORY-REPRO.md)
shows the reported peak crossing that ceiling after a denied allocation while
Private Bytes snapshots remain below it. This narrows the measurement question;
it does not prove continuous Job commit or pass the strict gate.
That discrepancy, dependency/startup security review, native Ubuntu, and production
delegation requirements remain pending.
These results do not enable scripting or satisfy #95's acceptance criteria.

The proposed switch is `SIDER_LUA_ENABLED`, accepting only `true` or `false` and
defaulting to `false`. Disabled instances return `LUAERR disabled` for the whole
scripting surface. Enabled startup validates the helper and OS containment before
readiness; diagnostic mode reports the selected runtime and boundary requirements
without running client source. This switch does not exist in the current binary.

Existing contracts remain authoritative for [binary RESP2](compatibility.md),
[shard ownership](sharding.md), [transactions](transactions.md),
[persistence](persistence.md), and [replication](replication.md). Scripts are a
Sider extension with Redis-shaped invocation syntax and a deliberately smaller
API. They do not establish Redis Lua 5.1 compatibility.

## Contents

- [Invocation and cache](#invocation-and-cache)
- [Keys, commands, and values](#keys-commands-and-values)
- [Runtime and sandbox](#runtime-and-sandbox)
- [Resource envelope](#resource-envelope)
- [Ownership and commit](#ownership-and-commit)
- [Durability and recovery](#durability-and-recovery)
- [Errors and cancellation](#errors-and-cancellation)
- [Required negative tests](#required-negative-tests)
- [Implementation sequence and risks](#implementation-sequence-and-risks)

## Invocation and cache

The initial wire surface is:

```text
EVAL source numkeys [key ...] [arg ...]
EVALSHA sha256 numkeys [key ...] [arg ...]
SCRIPT LOAD source
SCRIPT EXISTS sha256 [sha256 ...]
SCRIPT FLUSH
```

`numkeys` is canonical nonnegative decimal, at most 128, and cannot exceed the
remaining argument count. Keys and arguments are bulk bytes. Source is valid
UTF-8 Lua text without NUL; reject compiled chunks, including the Lua bytecode
signature, before invoking the text-only compiler. Empty source is valid and
returns null. Command and subcommand names use ASCII case folding. Digests are
exactly 64 lowercase hexadecimal ASCII bytes, SHA-256 over the original source;
no whitespace, newline, or Unicode normalization occurs. This differs from the
SHA-1 identity used by Redis EVALSHA.

`SCRIPT LOAD` compiles under the same sandbox and limits without executing the
chunk, then returns its digest as a bulk string. Compilation must succeed before
cache insertion. Cache operations use a bounded process-wide registry independently
of Store; LOAD takes a global helper permit with the same CPU/wall/memory limits,
and compilation never holds the registry lock. EXISTS/FLUSH do not launch helpers
or acquire snapshot admission. `SCRIPT EXISTS` returns integer 0/1 in input order, preserving
duplicates. `SCRIPT FLUSH` returns `OK`, advances the cache generation, and removes
all entries synchronously; ASYNC/SYNC options are unsupported. An invocation
already admitted with a source lease continues using that lease. A subsequent
lookup sees the new generation. A late compiler completion cannot repopulate an
older generation.

The process-wide cache holds source bytes only: 256 entries and 8 MiB including
source, digest, and a charged 128-byte entry overhead. Admission has no automatic
eviction: reject a new entry that exceeds either limit with `LUALIMIT cache`.
Loading an identical entry is idempotent and consumes no new slot. Digest matches
must compare full source and reject unequal content with `LUAERR digest collision`.
No VM, closure, bytecode, or script globals survive an invocation. Restart, restore,
or promotion clears the cache; it is neither backed up nor replicated. Connection
closure does not clear the process-wide cache. Source leases held by running calls
are charged to invocation memory as well as cache memory when applicable.

`EVAL` does not populate the cache. `EVALSHA` takes its source lease at admission;
a missing entry returns `NOSCRIPT No matching script`. Clients reload explicitly.
Cache operations and EVAL are prohibited in subscriber mode and inside MULTI.
Inside MULTI their rejection invalidates the queue, so EXEC returns EXECABORT.
WATCH outside MULTI remains valid; committed script writes invalidate observations
under existing worker rules. Every scripting command is rejected on a replica
with READONLY, including nominally read-only scripts and cache operations. This
avoids a second execution policy in v1. Unsupported SCRIPT subcommands fail with
`LUAERR unsupported script subcommand`; there is no SCRIPT KILL or functions API.

## Keys, commands, and values

KEYS and ARGV are 1-based dense tables created afresh from invocation bytes.
Changing a table or a global affects only the current VM. The host retains an
immutable declaration independently of those tables. Every key extracted from
every host command must byte-match a declared key. A computed string is permitted
only if it equals a declaration; a common hash tag alone does not grant access.
Duplicates are preserved in KEYS but counted once for membership. Empty and
non-UTF-8 keys are valid. All declared keys must route to one existing worker,
including unused keys; crossing declarations return the current CROSSSLOT error
before helper creation. Zero declared keys routes to worker zero and permits only
keyless commands. The current hash-tag/FNV routing algorithm is unchanged.

Only `sider.call(command, ...)` performs database operations. Every argument,
including numeric command arguments, must be a Lua string; there is no implicit
number formatting. The existing parser validates arity and options, then a second
allowlist check and key check precede preparation. All supported forms in the
following table are permitted; future parser additions require an explicit
allowlist revision.

| Commands | Allowed forms |
| --- | --- |
| PING, ECHO | Existing ordinary keyless forms |
| GET, SET | GET and current SET basic/NX/XX/EX/PX/GET/KEEPTTL combinations |
| DEL, EXISTS, MGET, MSET | Existing multikey forms; every key must be declared |
| INCR, DECR | Existing i64 decimal operations |
| EXPIRE, PEXPIRE, TTL, PTTL, PERSIST | Current forms without conditional expiry flags |

Collections, sorted sets, AI memory commands, scans, blocking commands, INFO,
Pub/Sub, transactions, WATCH, EVAL/EVALSHA, SCRIPT, administrative operations,
authentication, replication, persistence controls, and unknown commands are
denied. A command cannot recursively enter a script; nested host-call depth is
one. Lua function recursion is separately bounded below. This initial subset
avoids collection traversal and unordered responses; extend it only with work
budgets and additional failure evidence.

Host reply mapping is explicit:

| RESP2 reply | Lua value |
| --- | --- |
| Bulk string | Byte-preserving Lua string |
| Null bulk or null array | `false` |
| Integer | Lua integer, exactly preserving signed i64 |
| Simple string | Single-field table `{ok = bytes}` |
| Array | Dense 1-based table, recursively mapped, retaining false entries |
| Error | Fatal invocation error; no error value returned |

The script must return zero or one result. Zero results, nil, and false encode
as null bulk; true encodes as integer 1; Lua integers encode as RESP integers;
strings encode as bulk bytes. A plain table must contain exactly integer keys
1 through N with no holes, extra fields, metatable, or cycles; it encodes as an
array, including an empty table. `{ok = text}` alone encodes a simple string and
`{err = text}` alone fails the invocation. Their text must be ASCII without CR/LF
and at most 256 bytes. Floats, NaN, infinities, functions, threads, and userdata
are invalid results. Validate raw entries without invoking metamethods. Multiple
results or malformed tables produce LUAERR and discard pending mutations.

There is no pcall/xpcall or `sider.pcall`: a host command error always aborts the
invocation. Invalid integer, WRONGTYPE, quota, undeclared key, and command denial
cannot be caught to commit an earlier write. False results for valid conditions
(for example a failed SET NX) are ordinary results, not errors.

```lua
-- Declare both keys with the same hash tag in EVAL's wire arguments.
local value = sider.call('GET', KEYS[1])
if value == false then return false end
sider.call('SET', KEYS[2], value, 'PX', '1000')
return value
```

Lua strings retain NUL and arbitrary bytes in values and command replies. UTF-8
source does not require UTF-8 runtime strings: escape sequences and ARGV can
construct arbitrary bytes. Decimal integers stay textual until the server parser;
scripts should use string data to avoid floating-point precision loss.

## Runtime and sandbox

Select standard Lua 5.4 through **mlua 0.12.2**, with `lua54` and `vendored`,
default features disabled, in a separate helper binary. Pin its dependencies and
bundled Lua patch version in the lockfile and record them in runtime diagnostics
and license inventory. No LuaJIT, Luau, C module loading, serde, async Lua, or raw
FFI APIs are required. Project crates retain `#![forbid(unsafe_code)]`; safe
bindings do not imply that upstream C or dependencies contain no unsafe code.
The [mlua primary API documentation](https://docs.rs/mlua/latest/mlua/struct.Lua.html)
documents safe construction, hooks, and allocator limits; its `sandbox` method is
Luau-specific and is not this Lua 5.4 security boundary.

Construct a fresh VM with `Lua::new_with` and explicitly selected libraries,
install the allocator limit before loading source, then attach the count/call
hook before compilation/execution. Load only text chunks. Give the chunk a fresh
environment populated solely with KEYS, ARGV, sider.call, and these functions:

- Base: assert, error, ipairs, next, pairs, select, tonumber, type.
- String: byte, char, len, sub; the string type metatable exposes only this same
  subset, so method syntax cannot recover omitted functions.

No `_G`, debug, io, os, package, require, load, loadfile, dofile, print,
collectgarbage, coroutine, pcall, xpcall, rawset/rawget, getmetatable/setmetatable,
tostring, string.dump, patterns, format, repetition, table.sort, math library,
randomness, wall-clock access, locale access, or environment access is exposed.
Global assignment remains local to the fresh environment; replacing sider.call
cannot acquire authority. User code receives no host objects or file handles.
The helper's host callback is the only IPC capability. No source, keys, arguments,
or Lua error text enters logs; log bounded error codes and invocation identifiers.
Review environment escape paths rather than treating a blacklist as sufficient.

The [Lua 5.4 manual](https://www.lua.org/manual/5.4/manual.html) defines byte strings,
integer/float values, text loading, and count hooks. Instruction hooks alone do
not preempt parsing or native functions. Therefore every compile/invocation uses
a disposable helper process, with framed private pipes, no inherited listener or
AOF handles, and an empty host environment. The parent accepts at most one
outstanding callback frame and independently validates all IPC lengths, command
identities, budgets, sequence numbers, and key access. The helper never reads or
writes Store, AOF, snapshots, replication streams, or client sockets directly.

OS containment is a mandatory feasibility gate: Linux requires a configured
cgroup v2 memory boundary plus CPU usage monitoring and process-tree termination;
Windows requires a Job Object with process/job memory accounting, CPU accounting,
one-process limit, and kill-on-close. Use audited safe wrapper APIs or dependencies,
not unsafe project code. Start children suspended or behind a startup handshake
until the boundary is installed; do not send untrusted source beforehand. Linux
address-space rlimits may supplement this boundary but do not replace measured
resident-memory control. Remove ambient credentials and deny additional child
creation; Lua language capability denial remains required on both targets.
If containment setup is unavailable, fail startup when scripting is enabled.
Disabled scripting needs no helper or privileged cgroup configuration.

## Resource envelope

These are proposed fixed v1 limits, not existing environment variables or measured
performance. They can be lowered by a future validated configuration interface;
raising them requires new envelope evidence. Limits apply cumulatively, even to
commands that produce no mutation.

| Resource | Proposed ceiling | Enforcement |
| --- | --- | --- |
| Source | 64 KiB | Parent validates before compile |
| Declared keys | 128 distinct and 128 total | Parser and host declaration |
| Invocation input | 1 MiB including source, keys, ARGV and framing | Parent before helper launch |
| Concurrent helpers | 4 globally, 1 per owning worker | Try-acquire bounded permits; no helper wait queue |
| VM instructions | 1,000,000 | Count hook every 1,000 instructions; stop before another interval |
| Helper CPU | 100 ms cumulative user + kernel time | Parent samples OS accounting at most every 10 ms |
| Execution wall time | 500 ms from admission through prepared result | Monotonic parent deadline; includes launch, compile, IPC, conversion |
| Lua allocation | 8 MiB including VM, compilation, inputs and outputs | mlua allocator limit; initialization counts |
| Helper memory | 64 MiB OS-accounted process/job/cgroup memory | OS containment before accepting source |
| Parent temporary state | 16 MiB per invocation | Charged overlay/post-images plus IPC/conversion buffers; reserve before allocate |
| Host calls | 128 total | Parent monotonic counter |
| Host arguments | 256 per call; 1 MiB cumulative callback request bytes | Parent IPC decoder before allocation |
| Host responses | 1 MiB cumulative encoded bytes | Preflight before IPC send; duplicates count again |
| Final reply | 1 MiB encoded RESP and current server reply limit, whichever is smaller | Full encoding before commit |
| Return tree | Depth 16, 4,096 nodes including root | Bounded iterative raw traversal |
| Lua call stack | 64 active Lua function frames | Call/return hook including tail-call accounting; reject recursion excess |
| Nested scripting/host calls | No script recursion; one host call active | Allowlist and IPC state machine |

CPU accounting overshoot is at least the watchdog sampling interval and scheduler
delay; a 100 ms CPU threshold is an abort trigger, not an exact CPU-time cap.
The wall deadline bounds parent waiting for a prepared result, not OS scheduling
or blocked kernel I/O. Termination and reaping must be supervised; a stuck helper
retains its permit and prevents replacement capacity from exceeding four. A
termination failure disables new scripting admission and starts server draining.
Do not advertise hard real-time guarantees. Instruction, allocation, output, and
state-accounting ceilings remain independently enforced.

Only string operations with input-proportional work are exposed. Each string
result must fit the Lua allocation ceiling. Host multikey operations have at most
128 declared keys and 256 arguments; callbacks charge request and response bytes
before copying. Cloning a touched value into an overlay charges its full logical
size even when immutable Bytes are shared; include parent container overhead in
the 16 MiB temporary budget. Reject a large preexisting value if touching or
returning it would exceed a budget. Dataset quota, AOF record bounds, worker queue,
connection limits, and network deadlines still apply. This envelope is not a cap
on total server RSS; cache, four helpers, queued input, dataset and parent buffers
must all be included in the resource profile.

## Ownership and commit

The router validates declarations and acquires global snapshot admission, then
enqueues one script request to its owning worker through the existing bounded
channel. Helper capacity is acquired when the worker starts the request; unavailable
capacity returns `LUABUSY capacity` without compiling or preparing changes.
The worker retains the snapshot admission lease and sole shard ownership through
execution, durable acceptance, application, and reply completion. Other workers
may progress; this shard cannot process another request, snapshot collection, or
active expiration while the script runs. No Lua VM executes on a Tokio runtime
thread; the parent only awaits bounded pipe messages and services callbacks.

Freeze the worker clock at preparation start. Callbacks operate sequentially on a
private touched-key overlay, reading earlier script writes. Use the existing typed
command parser and preparation logic; refactor reusable preparation where needed,
without running each callback through the normal independently committing worker
path. Overlay reads resolve expiry at the frozen instant. Expiration tombstones,
TTL changes, quota deltas, and WATCH notifications remain tentative. On abort,
Store and its TTL/WATCH registries are unchanged; normal expiry becomes visible
on the next ordinary request. Freeze relative TTLs at preparation start and persist
the resolved absolute deadlines, so helper latency cannot extend their lifetimes.

A valid final result is completely converted and encoded before persistence.
Collapse successful overlay changes into one existing ResolvedBatch of final
post-images/tombstones; preserve existing mutation/no-op and WATCH invalidation
semantics, including a same-value write and a create-then-delete of an initially
absent watched key. Track keys actually changed by successful commands independently
of final image equality; do not elide their WATCH invalidation during coalescing.
Recheck the worker quota and serialized
AOF size before durable submission. Only after writer acceptance apply the complete
batch and WATCH notifications without interleaving. No Pub/Sub effects exist in
v1. A script with no durable changes emits no mutation record.

Unlike the existing MULTI execution contract, a script error discards every
tentative change. SCRIPT operations cannot be queued in MULTI, and transactions
cannot run inside Lua. This gives v1 one explicit rollback boundary without
changing existing EXEC behavior.

## Durability and recovery

Persist and replicate **resolved effects**, never source, digest, callback command
text, or the result of rerunning Lua. Use the existing batch format, global AOF
sequence, writer admission/fsync policy, and replica application path. Do not add
a Lua opcode to AOF. A format change discovered necessary during implementation
requires a separate format/migration review before coding it. Bytecode and runtime
versions consequently do not become prerequisites for replaying existing effects.

Replicas do not launch helpers. Relative TTLs arrive as resolved absolute deadlines;
snapshots, backup, compaction and offline migration contain ordinary resulting
strings/deletions/TTLs. A replica with scripting disabled must still replay them.
Promotion clears source cache and needs explicit reloads; ordinary asynchronous
replication loss semantics still apply. The current same-version/configuration
requirement remains in force even though Lua is absent from replay.

Crash before batch submission leaves no script record. A complete batch accepted
by the writer can recover even if the client never received success; torn records
follow the existing AOF tail/integrity policy. Crash between append and application
must recover the whole batch, never a callback prefix. An unacknowledged fsync or
append error can leave a complete record that is later replayed: do not promise
that an error implies absence of durable effects. Preserve existing writer failure
and draining behavior. Without AOF, a process crash loses the dataset normally.

Determinism means replay of identical resolved batches yields identical state;
it does not promise identical Lua execution for arbitrary table iteration or
platform floating-point operations. No random/time/host inputs are exposed, and
TTL reads use the frozen clock, but pairs/next order is unspecified. Tests must
prove replicas receive effects rather than silently relying on script purity.
For comparison, [Redis's scripting introduction](https://redis.io/docs/latest/develop/programmability/eval-intro/)
describes ephemeral scripts and effect propagation; this proposal defines its own
atomic failure and cache rules.

## Errors and cancellation

Return controlled ASCII RESP errors without script content or argument values:
`LUAERR syntax`, `LUAERR runtime`, `LUAERR undeclared key`,
`LUAERR command denied`, `LUAERR return type`, `LUAERR nested call`,
`LUALIMIT instructions`, `LUALIMIT cpu`, `LUALIMIT wall`, `LUALIMIT memory`,
`LUALIMIT input`, `LUALIMIT output`, `LUALIMIT temporary`, `LUALIMIT calls`,
`LUALIMIT depth`, `LUALIMIT cache`, `LUABUSY capacity`, and `LUACANCELLED`.
Malformed protocol frames retain current connection-close behavior. Valid command
syntax errors remain recoverable and leave the connection usable. Callback
execution errors preserve controlled existing codes such as WRONGTYPE or quota;
they still abort the entire overlay. A missing/failed helper returns
`LUAERR helper failure` and discards the overlay.

Cancellation before worker acceptance discards the request. During preparation,
disconnect, request cancellation, timeout, and shutdown signal the helper, discard
the overlay, and release leases after cleanup. After result validation, perform
one final cancellation/deadline check and atomically transition the parent state
from Preparing to Committing. No cancellation can abandon the batch after this
transition: complete durable submission and application using the ordinary worker
path, even if the connection closes. A response-writing failure then has uncertain
client outcome and must not trigger retry. Document this commit point in the
client-facing guide; scripts are not inherently idempotent.

The 500 ms preparation deadline ends at Committing. Writer fsync and socket output
use their existing limits, so total command latency can exceed 500 ms. Supervisor
shutdown includes helper termination, pipe closure, reaping, worker drain, and
normal writer drain. A helper panic, crash, malformed frame, or budget breach
cannot cause parent Store mutation or an unbounded IPC allocation.

## Required negative tests

These are acceptance tests to implement, not passing evidence. Record platform,
exact SHA, runtime versions, limits, raw responses/state, and process cleanup.

| Area | Mandatory cases and outcome |
| --- | --- |
| Source | Syntax errors, invalid UTF-8, NUL, bytecode, oversized/truncated input; no cache or Store changes |
| Sandbox | Every denied global, string method escape, module/C loading, inherited credentials/handles, filesystem/network/process/environment attempt fails |
| Work limits | Infinite loop, recursion/tail recursion, compiler stress, allocation flood, giant concatenation and host callback storm; bounded abort, helper cleanup and next request succeeds |
| Host IPC | Forged lengths/sequences/command names, unsolicited second callback, closed/truncated pipes; parent rejects before allocation/effects |
| Key boundary | Binary/empty/duplicate declarations, undeclared same-tag key, changed KEYS table, mixed-shard unused declarations; no unauthorized access |
| Values | NUL/non-UTF-8 payload, i64 extrema, false array entries, empty table, holes/cycles/metatables/float/multiple returns; exact mapping or atomic rejection |
| Rollback | SET then WRONGTYPE/denied call/runtime error/resource error/invalid return/oversized output; original values, TTL, quota and WATCH state preserved |
| Visibility | Read-your-writes, SET NX/XX, frozen expiry boundary, simultaneous same-shard reader, another-shard progress, snapshot waiting; no partial exposure |
| Durability | Record-size and append/fsync failures, every truncated batch prefix, crash before submit/after append/before apply/after apply/before reply; existing recovery guarantees, no callback prefix |
| Replication | Binary multikey effects and TTL, disconnect/history loss/full sync, disabled scripting replica, helper launch counter stays zero on replay; matching resolved state |
| Lifecycle | Cache full/idempotent load/collision seam, concurrent FLUSH with source lease and compiler completion, restart/restore/promotion, malformed digest; declared cache generation semantics |
| Cancellation | Disconnect before admission/during compile/IPC/conversion/before commit/after commit; cleanup before commit and completed whole effect after commit |
| Containment | CPU/memory kill on Windows and Linux, launch before boundary prohibited, missing containment fails enabled startup, killed child cannot spawn descendants, four-helper exhaustion, leaked-helper supervisor response |
| Integration | MULTI invalidation, WATCH conflict only on committed effects, subscriber denial, READONLY denial, disabled-feature behavior, low network/AOF/output limits and shutdown |

Use injected clocks, cancellation barriers and fault points for deterministic
boundary tests; use real processes for containment and crash evidence. A Redis
differential test can cover the shared ordinary command subset and binary values,
but cannot validate Sider's rollback policy, SHA-256 cache or Lua 5.4 runtime.

## Implementation sequence and risks

1. Build an isolated runtime/containment spike on Windows 11 x64 and Ubuntu 24.04
   GNU. Verify safe APIs, exact vendored version, text-only loading, allocator/hook
   behavior, 64-frame accounting, private IPC, startup boundary, CPU overshoot,
   child reaping, and 64 MiB feasibility. Measure launch/IPC cost with no competing
   load. Resolve failures by changing this proposal before production integration.
2. Implement bounded parser/cache and helper IPC modules with the sandbox and
   negative fixtures. Add dependencies only with these real consumers; review
   licenses and package/helper distribution together.
3. Integrate touched-key preparation, cancellation state machine and one existing
   ResolvedBatch commit in the owning worker. Prove atomic rollback, TTL/WATCH,
   quota, output preflight and snapshot admission before adding TCP exposure.
4. Exercise real AOF/replica/backup/recovery and crash points on both supported
   platforms. Add a feature-specific release gate and update compatibility,
   deployment, package and resource documents only after verified implementation.

Remaining technical risks are the availability of safe Windows Job Object/Linux
cgroup wrappers, permissions in containers, baseline native VM memory, compiler
and native function watchdog behavior, reliable Lua tail-call depth accounting,
temporary accounting through existing preparation APIs, and helper overhead for
short scripts. The 500 ms/100 ms/64 MiB choices require measurement, not claims of
present feasibility. Process isolation does not itself establish a complete OS
filesystem/network security sandbox against a compromised native runtime; assess
that threat and platform policy separately before network exposure. The feature
stays disabled if the mandatory language and resource boundaries cannot be proved.

This document introduces no production dependency, command, configuration variable,
AOF change, or passing feature evidence. Issue #95 remains open until implementation
and all relevant acceptance tests are complete.
