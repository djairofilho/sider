# Exact agent-memory search, issue #111

## Scope and baseline

This stage is stacked on the explicit record-2 native migration at `b025718`
and implements the `MEM.SEARCH` portion of the frozen
[`sider-memory/1` contract](agent-memory-v1.md). The default server remains on
record 1; this stage does not activate the memory durability profile or make a
protected deployment claim.

The command searches one collection on its owning shard. The parser validates
the model tuple, finite `F32LE` query, `k`, namespace and typed metadata equality
predicates before worker admission. The worker checks the stored tuple and
dimension, scans at most the retained collection bound, filters live records
before retaining candidates, and keeps a bounded heap of at most 128 hits.
Replies contain only binary IDs and exact `F64LE` squared distances, sorted by
distance then unsigned binary ID. Search does not mutate the collection or
create a persistent index.

Visibility checks both the absolute record deadline and the owning worker's
monotonic deadline. A wall-clock stall or rollback cannot reveal a member whose
monotonic deadline elapsed while it remains physically retained.

## Validation

The independent oracle scans and sorts all eligible records for comparison
with the bounded heap. Its fixtures include equal distances, binary and prefix
IDs, typed metadata, selective filters, expiry and a full 1,024-member scan.
Native lifecycle cases compare replacement, deletion and replayed images.
The TCP case verifies filter admission before top-k, binary distance bytes,
missing collection, unsupported predicate type, invalid `k`, non-finite query,
identity mismatch, duplicate predicate and connection reuse after rejection.

Windows results on this stage:

| Check | Result |
| --- | --- |
| `cargo test --locked --test agent_memory_search --test agent_memory_tcp -- --test-threads=1` | Oracle/lifecycle 4/4 and TCP 17/17 passed. |
| `cargo xtask check` | Passed: format, all-target Clippy, debug binary, every non-ignored test and doctest. |
| `cargo doc --locked --no-deps` | Passed. |
| `cargo build --locked --release` | Passed. |

Results from predecessor PRs are not attributed to this stage. The existing
release-context tests ignored by the default suite remain separate gates.

## Remaining issue gates

The release search gate and manifest registration, complete work and allocation
counters, Windows and Linux exact-score evidence, recovery/backup/replication
matrices, and measured capacity and relevance corpus remain pending. These
require the rest of #110 and #112. Neither this stage nor its local tests
approve a release or close #111.
