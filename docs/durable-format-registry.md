# Proposed durable format registry

**Status: proposed release allocation with an explicit local record-1 mixed-owner
implementation in the development checkout.** This document coordinates [cross-shard transactions](cross-shard-transactions.md)
with the accepted [AI memory contract](agent-memory-v1.md). Its audit
baseline is `0f0f232`. It does not change published layouts, allocate memory tags
again, enable mixed-owner EXEC, or authorize mixed-version rolling upgrades.

## Registry and ownership

Header version selects one complete binary grammar. Record version selects the
mutation codec and replay rules. Required capability bits select additional
semantics needed to read every permitted batch. Role and epoch select replication
identity. None can be inferred from the Sider product version, file contents, or
the absence of memory/mixed batches so far.

| Header | Status | Header bytes | Record codec | Batch owner rule | Replication identity |
| --- | --- | ---: | --- | --- | --- |
| 1 | Published, immutable | 24 | 1, implicit | One | None; implicit one-shard layout |
| 2 | Published, immutable | 32 | 1, implicit | One | None |
| 3 | Published, immutable | 52 | 1, implicit | One | Existing role and epoch |
| 4 | Reserved by accepted memory contract | 36 | 2, explicit | One | None |
| 5 | Reserved by accepted memory contract | 56 | 2, explicit | One | Memory contract role and epoch |
| 6 | Local experiment, never a release target | 32 | 1, implicit | Mixed recovery only | None |
| 7 | Proposed release profile; explicit local record-1 mixed implementation | 64 | 1 or 2, explicit | Required capability below | Explicit optional role and epoch |

Numbers 1 through 5 retain their exact bytes and meaning. Do not turn 4/5 into
mixed-owner profiles. Retire the local 6 experiment rather than reusing its number
for a different grammar. No official release, backup format, migration target, or
replication Hello advertises 6. Its current implementation remains isolated in
the #92 worktree for recovery tests; it is not a compatibility promise.

Register any later header, record tag/version, transport version, capability bit,
or backup schema here before implementation, with its owning contract and literal
fixtures. A conflicting allocation blocks integration. A future extension cannot
silently change an existing allocation even if no new value occurs in a fixture.

## Header 7 grammar

All integers are little endian. The header has exactly 64 bytes, independent of
role, codec, and capabilities. Length is validated against this exact constant;
it does not authorize allocation or skipping an arbitrary extension. A different
header length or grammar requires a new header version.

| Offset | Bytes | Field |
| --- | ---: | --- |
| 0 | 8 | Existing magic `SIDERAOF` |
| 8 | 4 | Header version `7` |
| 12 | 4 | Header length `64` |
| 16 | 8 | Snapshot sequence |
| 24 | 4 | Shard count, existing valid range 1 through 256 |
| 28 | 4 | Routing version `1` |
| 32 | 4 | Record version `1` or `2` |
| 36 | 4 | Required capability bitset |
| 40 | 1 | Role: `0` independent, `1` primary, `2` replica |
| 41 | 3 | Reserved, all zero |
| 44 | 16 | Epoch; all zero for role 0, nonzero for roles 1/2 |
| 60 | 4 | Existing CRC-32/ISO-HDLC over bytes 0 through 59 |

Bit `0x00000001` is `MIXED_OWNER_BATCH`. It permits one resolved Batch record to
touch several local owners. All other bits are unallocated and rejected. The bit
does not change record framing, mutation origin tags, sequence rules, routing, or
snapshot seals. It requires the runtime, replay, compaction, backup and replication
atomicity specified by #92; a reader cannot ignore it because the current file
only contains single-owner batches.

Record version 1 retains the existing codec and complete postimages. Version 2
retains the memory contract's framing and tags 1 through 8, including guarded
memory deltas. Record 2 must preserve the intermediate expired memory state and
validate historical deltas without clock pruning between records. Its final
visibility, cleanup, charge, and migration rules remain those of the memory
contract. The mixed-owner bit adds global staging and quota validation before
any owner changes; it does not weaken schema guards, ordering, or retention.

The four `(record version, capability)` pairs `(1,0)`, `(1,1)`, `(2,0)`, `(2,1)`
are distinct profiles. Each permits any valid role above. Every implementation
must explicitly list the profiles it proves. Header parsing alone is insufficient
to claim a supported profile. Roles 1/2 additionally require the matching
replication transport and complete application/cursor proof.

## Admission and preservation rules

Readers first validate magic/version, the fixed header length, checksum, layout,
record version, required bits, reserved bytes, and role/epoch. Then they check
that the exact profile is supported. Both steps precede scanning records, tail
repair, append, compaction, server bind, and readiness publication. Unknown or
unsupported combinations produce an explicit diagnostic and preserve source
bytes, including every torn first-record prefix.

A server creates the complete selected profile in an empty directory. Choosing
record 2 must not depend on the first memory write; choosing mixed-owner support
must not depend on the first mixed batch. Configuration cannot upgrade or
downgrade an existing directory implicitly. Opening a supported header for
read-only inspection is not permission to change its profile.

Compaction and received full installation preserve record version and required
capabilities. Promotion changes only role and epoch using one durable publication;
it must preserve header 7 and its codec/bits. A full sync installs the negotiated
profile, layout, cursor and dataset atomically. No rewrite to 2/3/4/5 occurs merely
because a compacted snapshot contains individually encoded keys.

## Transport and backup composition

Transport 1 remains the published record-1 single-owner grammar. Transport 2
remains reserved for memory's record-2 single-owner grammar. Proposed transport
3 is required for header-7 replication, with an explicit supported profile in
Hello. Existing framing, frame tags, checksums, capacity negotiation and exact
Sider-version requirement remain; an altered Hello grammar must receive this
new transport version rather than extra fields under 1/2.

After Hello's existing fields, transport 3 adds `u32 aof_header_version(7)` and
`u32 required_capabilities`. The existing `record_version` identifies codec 1/2.
Before transfer, both peers require equal header, record, capability, routing and
shard versions and sufficient limits. Receiver support is not a subset shortcut:
profile equality avoids an install/compaction/promotion changing durable meaning.
No mixed batch is sent to transport 1/2. Unsupported profiles reject before
subscription, snapshot publication or batch ACK. A capability bit does not prove
the receiver's atomic application; its matching implementation and fault gates
are required before advertising it.

Backup sends `Export` directly, without a client Hello. Under transport 3, its
tag-10 payload is exactly `u8 sider_version_len`, that many ASCII Sider-version
bytes, `u32 record_version`, `u32 aof_header_version` (exactly `7`),
`u32 required_capabilities`, `u32 max_record_bytes`, and
`u64 max_snapshot_bytes`, in that order and little endian. The server validates
the complete requested profile against its source before snapshot capture and
rejects a mismatch without creating output. The backup client checks the returned
Hello profile against its request before creating its local archive. Literal-wire,
truncation, and mismatch tests are required for this standalone request path.

Proposed backup manifest schema 3 retains schema 2's fields and adds mandatory
`required_capabilities`. It advertises `aof_header_version: 7` and the exact
`record_version`. Export writes a sealed role-0 header-7 snapshot with zero epoch,
preserving the source codec/capabilities. Verify cross-checks these manifest
fields against the actual header before replay. Restore uses a new directory
with the same profile; it does not inherit source role, epoch, credentials or
TTL duration. Schema 1/2 and transport 1/2 remain immutable. The explicit
schema-3 stream export, verification and isolated restore library paths are now
implemented for record 1 with the mixed bit. Explicit library recovery and live
source export are also implemented. The executable selects local record-1 mixed
recovery and EXEC only with `SIDER_AOF_COMPOSED=1`; no transport-3 listener is
bound, and the default reader still refuses a restored header-7 directory.

## Offline migration and rollback

An explicit target profile is mandatory. Migration stops the source, holds its
exclusive directory lock, reads without repair, uses a frozen migration clock,
and writes a verified sealed snapshot into a new directory outside the source
tree. Preserve sequence, exact retained value/vector bits, type and provenance,
absolute deadlines, and layout unless an explicit rehash was requested. Clear
role/epoch in the independent destination. Validate all destination quotas and
snapshot record limits before publication; sync files and publication directories.
No outcome modifies source bytes. Report source/destination profiles, layouts,
sequence, hashes, retained counts and incomplete tail status.

| Source | Explicit target | Policy |
| --- | --- | --- |
| 1/2/3 record 1 | 4 record 2 | Accepted memory contract's legacy upgrade; requires full record-2 implementation |
| 1/2/3 record 1 | 7 record 1 or 2 | Proposed offline upgrade; every target profile must be implemented |
| 4/5 record 2 | 7 record 2 | Preserve memory historical replay and final migration rules; optional mixed bit selected explicitly |
| 7 record 1, any mixed bit | 2, or 7 record 1 with bit cleared | Explicit final-state snapshot export only; no batch history retained; full source recovery required |
| 7 record 1 | 4, or 7 record 2 | Explicit codec upgrade with supported target, never in place |
| 7 record 2 | 4, or 7 record 2 with bit cleared | Explicit final-state snapshot export; retain memory grammar and reject any unsupported target |
| 4/5/7 record 2 | 1/2/3, or 7 record 1 | Reject codec downgrade, even when no memory currently remains |
| Experimental 6 | Any released or composed target | No production migration promise; use isolated fixtures only until an explicit importer is approved |

The memory contract's prohibition on downgrade applies to all record-2 sources.
Changing the mixed bit through explicit snapshot export changes only future
batch capability, never the memory codec. A header-4 target can subsequently be
served only by a proved memory single-owner implementation. Snapshot export
does not preserve past client idempotency/outcome history.

Profile changes invalidate continuation across deployment. Upgrade both primary
and replica offline, establish a new epoch with the compatible transport, and
perform FULL before CONTINUE. No cursor from the old profile is reused. Retain a
verified old backup for rollback: it restores the old cut and loses later writes;
it does not make a newer directory readable by an older binary.

## Literal proposed fixtures

These fixtures specify header bytes only, not sealed recoverable files. CRCs
were generated independently with Python `zlib.crc32`, cross-checked against the
accepted memory header-4 fixture. They are contract evidence for encoding, not
passing implementation or atomicity tests.

Header 7, length 64, sequence 9, four owners, routing 1, record 1, mixed bit,
independent role and zero epoch:

```text
53 49 44 45 52 41 4f 46 07 00 00 00 40 00 00 00
09 00 00 00 00 00 00 00 04 00 00 00 01 00 00 00
01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 fa fc 1a 97
```

Header 7, same length/sequence/layout, record 2, mixed bit, replica role, epoch
`0102030405060708090a0b0c0d0e0f10`:

```text
53 49 44 45 52 41 4f 46 07 00 00 00 40 00 00 00
09 00 00 00 00 00 00 00 04 00 00 00 01 00 00 00
02 00 00 00 01 00 00 00 02 00 00 00 01 02 03 04
05 06 07 08 09 0a 0b 0c 0d 0e 0f 10 f5 46 fd ba
```

## Implementation gate and profile sequencing

Published readers retain their existing profile behavior; the experimental 6
reader remains isolated. The development checkout validates the header-7 grammar
against literal fixtures, truncated headers, and invalid fields with recomputed
checksums. `SIDER_AOF_COMPOSED=1` selects the exact record-1 mixed profile for local
server recovery and RESP EXEC. The writer appends one combined owner record before
application; replay, compaction, restart, and torn-tail repair preserve that
decision. Wrong-profile startup rejects before readiness and preserves the AOF.
Process-kill tests cover nine boundaries from before append through owner
application and verify that restart never replays only one owner.
The internal replica path accepts a record-1 mixed snapshot, one expected-sequence
batch, whole-owner preparation and application, and a flush before advancing its
cursor. Tests cover quota rejection, restart, uncertain writer outcome, partial
apply failure, and caller cancellation. It still refuses record 2 and the missing
mixed bit before record scan. The explicit reader also accepts a persisted primary
role, and the composed writer can publish one transport-3 journal frame for a
mixed-owner batch after promotion. The executable refuses a persisted composed
role without an explicit replication configuration, and its current composed
configuration still rejects replication. Network following is not enabled yet.
An isolated transport-3 codec now frames record-1 snapshot and batch records and
requires an exact header-7/mixed-bit Hello. Its bounded reader rejects transport
1/2 frames before payload allocation. Its isolated Export codec carries the same
exact profile before capacities; invalid profile fields reject even with valid
checksums. An explicit captured-snapshot exporter now checks the request against
the source profile and capacities before sending a Hello or snapshot payload. It
preflights record encoding, key order and transfer bytes before advertising a cut.
The two-phase export API also returns an admission token before capture and binds
the captured Hello to that exact admitted profile; a mismatch emits no snapshot.
The schema-3 archive library exports, verifies, restores, and recovers record-1
mixed snapshots. A live library source negotiates transport 3 before capture.
The server selects local header-7 storage and EXEC, while the default reader
still rejects version 7 before inspecting records and preserves source bytes.
An explicit loopback-only composed export listener and backup CLI select schema 3;
no composed replica follower or protected network source is available. Coordinate
acceptance of allocations 7, transport 3, and backup schema 3 with the memory
contract owner before a production release.

A first header-7 implementation may support only record-1 profiles with an
explicit capability allowlist. It must reject every record-2 profile before record
scan, tail repair, append or readiness, including an empty file. This permits #92
to complete its record-1 path independently of #110. The record-2 profiles remain
unavailable until #110 proves the memory codec and its historical replay. Adding
them later does not reinterpret an existing record-1 directory or upgrade it in
place. Every supported profile still needs matching compaction, backup, transport,
promotion and offline migration behavior.

Keep public CROSSSLOT until the record-1 mixed-owner profile has a supervised
single decision, complete owner apply, replica/full sync/promotion,
backup/compaction and crash/cancellation tests. Do not infer that a passing
record-1 profile proves either record-2 profile.

Required format tests cover all 64 truncated prefixes, every byte corruption,
unknown bits/versions, wrong fixed lengths, every reserved/role/epoch rule with
recomputed checksums, and every supported/unsupported profile. Full-file tests
must hash source bytes after incompatible-binary refusal and tail repair refusal.
Migration tests exercise every matrix edge, destination failure and clock cut.
Later combined memory/mixed records must exercise guarded deltas, expired-then-persisted
members, quota rejection on the last owner, one append decision, each apply/ACK
boundary, compaction, snapshot, promotion and restart on Windows and Linux.
