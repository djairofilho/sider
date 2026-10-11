# Proposed cross-shard transaction contract

**Status: protocol proposed; preparation and isolated replay foundations integrated.**
This design addresses
[issue #92](https://github.com/djairofilho/sider/issues/92) against source baseline
`0f0f232`. It supplies a written contract and an implementation plan; it does not
complete the issue's implementation or crash/network validation criteria.
The current [transactions](transactions.md), [routing](sharding.md), and
[compatibility](compatibility.md) contracts still require one shard.
Participant discovery includes watched-only keys and keeps owner IDs bounded and
sorted. Owner-local candidate preparation and a shared execution time are in the
development checkout; single-shard EXEC uses the latter for WATCH and TTL.
Mixed-owner EXEC, durable append, replay, and replica application are not enabled.
The recovery helper now stages postimages and validates each touched shard's
projected quota before applying a record. An internal test exercises a mixed-owner
record and a quota failure on its last owner; all production recovery calls still
pass the single-owner gate and reject such records before preparation. This proves
the helper's local all-or-none application, without enabling a durable profile.
An internal, unselectable header-7/record-1 profile now exercises one mixed batch
through an actual AOF append, restart, replay, compaction, and every torn-record
prefix. It rejects an over-quota owner and unsupported profiles without repair.
The public server and tools still cannot select header 7; transport 3, backup 3,
replica application, and coordinated EXEC remain unimplemented.

## Contents

- [Scope and consistency](#scope-and-consistency)
- [Routing and command semantics](#routing-and-command-semantics)
- [Coordinator protocol](#coordinator-protocol)
- [WATCH, expiration, and quota](#watch-expiration-and-quota)
- [Durability and replay](#durability-and-replay)
- [Replication and snapshot cuts](#replication-and-snapshot-cuts)
- [Failure and cancellation](#failure-and-cancellation)
- [Observability](#observability)
- [Proof obligations and implementation stages](#proof-obligations-and-implementation-stages)
- [Required deterministic tests](#required-deterministic-tests)
- [Later cluster participants](#later-cluster-participants)

## Scope and consistency

The first implementation coordinates fixed owning workers inside **one process**,
sharing one admission barrier and one global AOF writer. Participants are worker
IDs, not machines. No remote prepare/commit RPC, consensus, Redis Cluster slots,
online resharding, or automatic failover is implied.

A successful EXEC publishes all valid dataset operations as one indivisible unit.
Every admitted dataset command sees the state before or after that unit. Other
requests cannot see an applied prefix, including reads on nonparticipating shards.
Commands inside EXEC run in queue order and see earlier successful commands in
that EXEC. Individual execution errors retain their array positions and do not
roll back other successful commands, preserving the existing Redis subset.
Atomicity applies to that resulting set of valid effects.

The visibility point is release of exclusive admission after every participant
has applied. It is separate from the durable decision, which is acceptance of the
one global AOF batch. Before visibility, internal owner maps can be at different
application steps but no dataset reader may access them. All snapshots, promotion,
replica installation, diagnostic dataset reads, and expiration must participate in
this exclusion. Independent GET requests straddling the visibility point can see
different generations; callers requiring a common cut must use EXEC or snapshot.

The first design intentionally pauses all dataset admission while coordinating a
multi-shard EXEC. Single-shard transactions can keep their existing shared admission
path. Optimizing to participant locks or MVCC requires a new proof of the same
contract and measurements; it is not part of this proposal.

## Routing and command semantics

Discover participants before enqueueing EXEC using the existing binary key visitor,
hash tag extraction, FNV-1a hash, fixed shard count, and routing version. Include
every watched key even when it is absent or not mentioned by a queued command.
Deduplicate participant IDs and sort ascending. Preserve original key occurrences
and command indices separately so duplicates and reply order remain intact.
Empty and keyless batches use worker zero where owner state is needed.

The proposed enabled mode allows queued supported commands to span workers. No new
RESP command is required. A proposed startup opt-in, disabled by default, must be
documented and validated before enabling; until the complete path is implemented,
the existing CROSSSLOT restriction remains. Ordinary multi-key commands outside
EXEC retain their current restriction in the first stage.

Inside enabled EXEC, DEL, EXISTS, MGET, MSET, and WATCH can discover several
participants. MGET returns original order and duplicates; EXISTS counts occurrences;
DEL counts each actual removal once; MSET uses the last value per key and clears TTL.
For MSET, all destination preparations and quota checks must succeed before any
tentative effect from that command is retained. An error rejects that entire MSET
while later queued commands continue. Do not split one command into independently
committing subcommands.

Initially reject a multi-shard EXEC containing PUBLISH, SUBSCRIBE, or UNSUBSCRIBE
with proposed `ERR unsupported cross-shard transaction command` during queueing,
marking the queue invalid so EXEC returns EXECABORT. This avoids promising an atomic
publication relationship with the independently accessible Pub/Sub hub. Existing
single-shard Pub/Sub transactions retain their documented behavior. INFO is allowed
only with a defined capture that cannot expose tentative participant statistics.
PING, ECHO, and queued UNWATCH retain current semantics. Future commands require
explicit key discovery and preparation support before joining this allowlist.
Unknown keys, unsupported commands, queue limits, or invalid layouts must reject
before participant preparation, expiration mutation, or WATCH registration.

## Coordinator protocol

Use a bounded coordinator queue and a supervised task owned by the database
supervisor, not by the connection future. Retain the existing transaction command,
byte, watch, input, response, and total request limits. Proposed coordinator
capacity and temporary preparation byte limits require explicit configuration
defaults and validation; bound reservations before cloning or allocating state.
Participant count cannot exceed configured workers (maximum 256).

1. **Admit:** validate the entire queue and participant set, reserve bounded
   coordinator capacity, and transfer commands, watch tokens, and response sender
   into the supervisor-owned request. Waiting before this transfer has no effects.
2. **Exclude:** acquire the existing barrier exclusively without holding a shared
   admission guard. Previously accepted shared requests drain first. Revalidate
   role, layout, shutdown state, and participant availability under exclusion.
3. **Prepare:** freeze one execution clock after exclusion. Through dedicated
   owner control channels, validate all watch tokens before any tentative command.
   Keep touched-key overlays and quota accounting inside owning workers. Execute
   commands serially across participants; gather per-command candidate replies and
   accept or discard local candidates together for a multi-key command. No Store,
   watch registry, metrics dataset state, or external hub changes yet.
4. **Seal:** gather final typed postimages and tombstones into one ResolvedBatch,
   ordered deterministically by binary key, plus ordered replies. Validate record
   bytes, mutation count, participant routing, quota reservations, and prepared
   application capacity before append. Reserve application resources so applying
   does not discover an ordinary allocation/limit error after decision.
5. **Decide:** append that batch once via the coordinator's global AOF handle.
   With no AOF, make the in-memory commit decision after sealing. Read-only or
   no-change batches need no mutation append. Do not assign a sequence to each
   participant and do not persist their local subsets independently.
6. **Apply:** send owner-only apply controls carrying the prepared state and
   transaction identity. Each owner updates data, TTL indexes, WATCH invalidations,
   usage, and metrics exactly once and acknowledges. Empty participants acknowledge
   without effects. No participant append occurs in this phase.
7. **Publish:** after all acknowledgments, update applied status and release
   exclusive admission. Encode the bounded EXEC output and deliver it without
   holding admission while waiting on the socket. Encoding failure or a lost reply
   cannot reverse the decision. Release all watch tokens and temporary reservations.

Owner controls must be processed while exclusive admission is held; normal request
submission cannot be used because it reacquires shared admission. Use separate
bounded control channels with a defined priority and no dependency on normal queue
capacity. Workers continue servicing coordinator controls and supervisor failure
signals while preparation is active, but cannot serve normal requests or expiration.

Lock order is exclusive database admission, owner controls in ascending worker ID,
then writer requests. Owners never acquire database admission or wait for another
owner. The writer never calls back into an owner or requests a snapshot barrier
while processing append. Automatic compaction only requests capture from its
independent coordinator. No lock upgrade is permitted. A single exclusive holder
serializes concurrent multi-shard transactions and snapshots, eliminating participant
lock cycles. Control-channel capacity, writer waits, and starvation remain bounded
resource/liveness concerns and need deadline and shutdown tests.

## WATCH, expiration, and quota

WATCH can register keys across shards under one exclusive coordinator operation.
Validate the complete request and distinct-key limit before any registration.
Resolve necessary pending expirations in one durable expiration batch before
installing tokens; failure installs no tokens. Publish all registrations together.
Repeated WATCH calls accumulate owned tokens as today, subject to the total limit.
WATCH is an observation operation, not a long-lived transaction lock.

EXEC checks every watched participant at the frozen clock. Same-value writes,
create/remove cycles, writes by the watching connection, and expiration deadlines
are conflicts as in the existing contract. An expired deadline invalidates WATCH
even if cleanup has not run. Any conflict returns a null array and executes no
queued command. Conflict checking itself must not emit a new expiration batch.
Queued UNWATCH cannot clear an already detected conflict. DISCARD, UNWATCH, EOF,
and completed EXEC release tokens; replacing a replica snapshot invalidates them.

All commands in the batch use the same monotonic execution time and corresponding
absolute Unix deadline conversion. Preparation treats entries expired at that time
as absent and collects required tombstones with successful changes into the one
record. No participant independently samples a later time. TTL can expire while
append is blocked: the transaction's calculations remain frozen, but subsequent
reads after publication use their own clock and can immediately hide such keys.
Replay and replication retain absolute deadlines and never extend TTL.

Keep deterministic per-shard logical quotas; unused capacity cannot be borrowed.
Validate each queued operation in order against that owner's tentative usage.
One shard's quota error occupies its command reply position; successful preceding
and following commands still form the committed batch. For a command spanning
shards, discard all its candidate changes if any shard rejects it. Temporary
overlay/coordination memory needs an additional budget and is not counted as dataset
usage or advertised as an RSS ceiling. Exhausting this preparation budget rejects
the whole EXEC before append with a documented resource error.

## Durability and replay

The AOF record is the durable decision and includes every participant's resolved
mutations, one checksum, and one increasing global sequence. It never reexecutes
INCR, conditional SET, quota decisions, or relative expiration. The
[existing persistence policies](persistence.md) remain: `always` completes
write_all and sync_all before application/success; `everysec` accepts write_all and
can lose the unsynchronized suffix on system failure. Loss must remove the complete
transaction, not a participant subset. In-memory mode has no restart durability.

Current records can encode several keys, but current recovery rejects mixed-shard
batches. Introduce an explicit format/capability version that older readers reject
before applying anything, and update the replication handshake accordingly. Decide
and document the exact version and migration compatibility during implementation;
do not silently reinterpret a v2/v3 file as guaranteeing cross-shard recovery.
The accepted [memory contract](agent-memory-v1.md#durable-encoding-and-migration)
reserves header versions 4 and 5 for a different record format. Cross-shard
capability numbering and migration must account for both features, including files
that eventually contain memory mutations and multi-shard batches.
The [proposed durable format registry](durable-format-registry.md) defines a
composable profile for review; it does not enable a reader, writer, or replica.
Its explicit profile allowlist permits a complete record-1 mixed-owner release
before memory record-2 support, provided every record-2 profile is rejected
before scanning or repairing a file. Combined memory and mixed-owner data needs
the separate record-2 proof.
Preserve existing frozen fixtures and legacy readers' semantics.

Recovery validates the whole record, sequence, origin, lengths, checksum, types,
routing, mutation uniqueness, and every destination quota before applying any
subset. Stage all participant postimages, then apply as one recovery unit. Recovery
finishes before listener readiness, so no partial recovery state is served. A torn
last record follows existing tail-repair rules and contributes no mutations. Interior
corruption remains fatal. Any failure discards the staged record and fails startup;
do not repair by skipping a bad participant. Compaction must preserve complete
resolved batches in the delta and use a complete global snapshot cut.

## Replication and snapshot cuts

Primary journal publication may follow append before primary application completes,
as today; a journal head is not an applied visibility position. Replicate one whole
batch at its one sequence. Asynchronous replication and manual promotion retain
their [existing loss contract](replication.md): a promoted replica may omit complete
unapplied transactions, including ones acknowledged by the primary.

On the replica, validate generation, epoch, next sequence, complete frame, routing,
types, and all quotas under exclusive admission. Prepare all owners, append the
received batch once with append_expected, apply every owner without local append,
flush, then advance the applied cursor and ACK. Retain exclusion through that
process. Exact permitted duplicates repeat ACK; gaps or conflicting duplicates end
the session without mutation. A lost socket cannot cancel accepted application.
After a durable decision, participant failure closes all database admission and
requires restart/recovery before service; never ACK an applied prefix.

Snapshot/backup capture, full replica installation, promotion, and compaction share
the exclusive admission discipline. A capture either runs before EXEC starts or
after all owners apply and EXEC releases. It reads the sequence cut under the same
exclusion, so it cannot pair a post-append sequence with pre-apply maps. Subscription
to incremental history starts at that cut. [Backup](backup.md) transfer occurs from
the immutable captured image outside exclusion; a slow receiver does not hold
transaction locks. Full installation remains a complete generation publication.

## Failure and cancellation

| Boundary | Permitted result and required action |
| --- | --- |
| Queueing or admission wait | Invalid queue returns EXECABORT; timeout/disconnect before supervisor acceptance has no transaction effects |
| Accepted, before decision | Supervisor owns cleanup; preparation/availability failure aborts overlays and releases tokens, with no dataset mutation |
| Before append submission | A verified record/resource rejection can return a definite error; no application occurs |
| Append submitted, outcome uncertain | Timeout, disconnect, or writer I/O failure cannot establish absence of a complete record; stop admission on writer failure and recover |
| Complete append, before acknowledgment | Restart may recover the entire batch even though the client saw no success |
| Durable acknowledgment, before/during owner apply | Finish under exclusion; owner failure or supervisor panic stops service before any prefix becomes visible; restart replays the complete record |
| All owners applied, before release/reply | Release and respond if possible; client outcome can be unknown, but no rollback |
| Reply encoding/write failure | Close the connection; transaction effects remain and response may have been lost |
| In-memory decision followed by process failure | All data is lost on restart; no durability claim |

Use one total deadline for client waiting across queue, exclusion, preparation,
writer, and reply availability. Before append, a coordinator execution deadline
can abort safely. After append submission it cannot abandon the decision/application
task. A client timeout ends its wait; the supervised owner continues. Shutdown closes
new admission and drains accepted tasks within the global shutdown wait. Blocking
filesystem calls retain the existing operating-system limitations.

Crucially, dropping an exclusive guard during unwinding must not reopen admission
after only some owners applied. Close the supervisor admission state synchronously
before releasing exclusion on any fatal path, and make every normal admission path
recheck that state after acquiring its guard. A guard alone is not this proof.
Forced supervisor cancellation after decision must terminate service, not let
surviving workers continue. No independently recovered participant is served.

Retries are never automatic. There is no persistent client idempotency key or
transaction outcome lookup in the first protocol. After acceptance a disconnected
client has an unknown outcome and must reconcile its application state before
retrying non-idempotent operations. Internal transaction IDs identify supervised
state and duplicate owner controls; they do not supply exactly-once client execution.

## Observability

Proposed INFO counters: cross-shard accepted, committed, WATCH conflicts, queue
rejections, preparation/resource rejections, timeouts before decision, client waits
expired after acceptance, and fatal failures after decision. Gauges: pending
coordinators, active phase, participant count, prepared bytes, and exclusive waiters.
Measure exclusion wait, preparation, append/sync, apply, and exclusion hold durations.
Update committed only after complete application, separately from written/synced
AOF sequence. Expose last fully applied sequence separately from journal head.

Trace phase changes with internal transaction ID, participant IDs, sequence,
durability mode, abort reason, and whether the outcome is uncertain. Never log raw
keys, values, or client queue bodies. Metrics must not expose temporary usage as
committed dataset state. Bound retained diagnostics; do not keep an unbounded
transaction history. These names and metrics are proposed, not currently available.

## Proof obligations and implementation stages

The baseline cannot meet this contract by removing CROSSSLOT alone:

| Current boundary | Required proof/change |
| --- | --- |
| [Connection transaction state](../src/connection/transaction.rs) and [router](../src/storage/routing.rs) select one shard | Discover all participants and reject unsupported cases before controls |
| [Worker](../src/storage/worker.rs) prepares/applies one Store and appends locally | Owner overlays, atomic per-command candidate acceptance, centralized single append, no-append apply controls |
| [Snapshot barrier](../src/storage/snapshot.rs) excludes capture but ordinary EXEC holds shared admission | Exclusive supervised transaction path, no shared-to-exclusive upgrade, all dataset observers covered |
| [AOF recovery](../src/persistence/writer.rs) batch_shard rejects mixed mutations | Version gating, validation/staging for every shard, correct per-shard quota accounting |
| [Replica storage](../src/storage/replication.rs) rejects mixed participants and Control::Apply persists locally | One replica append/flush and atomic global application/cursor advancement |
| Current prepared application and dropped guards | Reservation/application failure analysis and supervisor fail-closed behavior after decision |

Implement in independently reviewable stages, with cross-shard enablement gated
until all required paths pass:

1. Introduce bounded participant discovery, coordinator ownership/state machine,
   explicit enablement, and rejection tests while preserving current execution.
2. Add owner overlays and frozen clock injection, WATCH aggregation, multi-key
   command candidate acceptance, quota/resource checks, and exclusive visibility.
   Verify in-memory behavior without enabling durable cross-shard writes.
3. Introduce the explicit format version, complete-record recovery, single append,
   crash/fail-closed paths, and compaction/migration fixtures. Reject enablement with
   unsupported readers/configuration rather than permitting a broken combination.
4. Integrate replica one-batch application, full synchronization, promotion,
   backup cuts, diagnostics, and network fault tests.
5. Run integrated native/TCP, recovery, migration, transactions, replication, and
   backup suites on Linux and Windows, plus every deterministic boundary below.
   Record exact SHAs and actual evidence; update public capability documentation
   only after passing results. Release requirements remain in [releases](releases.md).

Issue #92 remains open until implementation and crash/network evidence satisfy all
four acceptance criteria. Focused foundation tests pass locally, but they do not
exercise a coordinated mixed-owner EXEC or authorize a release claim.

## Required deterministic tests

Use injected clocks, explicit phase barriers/fault hooks, bounded channels,
ephemeral ports, actual child processes for crash recovery, and exact state/sequence
assertions. Synchronize at hooks rather than sleeping to guess execution order.

| Scenario | Required assertion |
| --- | --- |
| Binary/empty keys, hash tags, duplicates, 1/4/256 workers | Stable participants, preserved command semantics, bounded discovery |
| Unsupported command late in queue, layout/role error, saturated coordinator | No owner preparation/mutation and no append; connection remains usable for recoverable errors |
| Interdependent commands and mixed WRONGTYPE/integer/quota errors | Ordered replies, earlier writes visible inside EXEC, valid effects in one final batch |
| MSET quota failure on last participant | No candidate from that MSET retained; other queued valid commands commit |
| Opposite participant orders, snapshot/compaction/promotion races | No deadlock; one exclusive order and bounded shutdown |
| Pause after each individual owner apply and issue reads/snapshot | Every dataset observer blocks; after release sees the complete state |
| WATCH across workers, same-value write, create/remove, frozen expiration boundary | Conflict returns null array with no queued effects/record; tokens released without permanent registry growth |
| Abort preparation or WATCH registration on each participant | No tokens or overlay leak; necessary WATCH expiration batch is complete or absent |
| Record limit, mutation limit, prepared-byte limit, response limit | Predecision rejection has no effects; postdecision response failure retains complete effects |
| Kill before append, after every byte prefix, after complete write, before/after sync/ACK | Replay contains no transaction or its complete successful effects; always-acknowledged batches survive process crash |
| Kill before/after each owner apply, before release, before/after reply write | Recovery never serves a participant prefix; unknown client outcomes documented |
| Writer failure, owner channel closure/panic, supervisor cancellation after decision | Global admission closes before exclusion release; no remaining worker serves mixed state |
| Backup/full sync/compaction captured at every phase | Snapshot sequence matches a complete cut; restored image contains whole transactions |
| Replica frame truncated at each byte, invalid checksum/quota, reordered/gapped/duplicate frames | No invalid prefix applies or ACKs; permitted exact duplicate only repeats ACK |
| Disconnect before replica append, after append, after each apply, before/after flush/ACK | Supervised completion or fail-closed restart; applied cursor and ACK never lead complete durable application |
| Primary crash/reconnect, slow history consumer, lagging promotion | Missing suffix consists of complete batches; FULL fallback preserves atomic cuts |
| Client disconnect/timeout at every phase, worker queue saturation | Before acceptance no effects; after acceptance no abandoned commit; reservations/tokens return to baseline |

Fault evidence must distinguish process termination from power loss, and Windows
filesystem guarantees from Unix directory synchronization. Do not claim everysec
has a strict one-second loss bound. Tests must exercise the real mixed-shard record,
not several single-shard transactions that happen to use the same values.

## Later cluster participants

Network partitions in the first implementation affect client/replica transport;
local shard owners have no network commit protocol. A later cluster design requires
separate participant discovery with topology epochs, fenced coordinator leadership,
durable prepare records/reservations, durable commit decisions, recovery of in-doubt
transactions, decision discovery, and idempotent messages across restarts. It must
define read visibility while one node is partitioned and the availability sacrificed
to prevent partial reads, plus failover/consensus and snapshot-cut coordination.

Timeout alone cannot safely abort a participant after another may have committed.
The local global barrier and shared AOF do not prove atomicity across machines.
Until such a protocol and partition/crash tests exist, remote participants are
unsupported and rejected before any local or remote participant mutation. This
later work cannot be counted as implemented by the single-process design.
