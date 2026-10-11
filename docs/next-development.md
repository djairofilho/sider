# Sider development fronts after 1.0.1

This is the execution design for closing the operational base and delivering the
first AI-native use case. [The operational-base epic](https://github.com/djairofilho/sider/issues/103)
and [AI-memory epic](https://github.com/djairofilho/sider/issues/104) contain the
post-1.0.1 issue backlog, native dependencies, execution state, and evidence.
This document explains their shared contracts; it does not imply implemented
behavior or release approval. [The release manifest](../releases/plan.json)
remains the source for versioned release tasks, gates, and publication criteria.

## Outcome and scope

The first AI-native outcome is **retrievable memory for applications and agents**:
store a record with content, an externally generated embedding, metadata,
provenance, and an optional lifetime; retrieve relevant records with filters and
stable result semantics. Sider remains a bounded, in-memory database with optional
AOF. Model inference, document parsing, and embedding generation run outside its
storage workers.

The base is considered closed for a declared deployment profile when connection,
recovery, resource, and security behavior is documented and exercised under that
profile. A controlled loopback deployment and a networked production deployment
have different entry criteria. Do not describe the latter as supported until its
authentication and transport controls are implemented and verified.

The initial AI release does not require approximate nearest-neighbor indexing,
model hosting, a general-purpose search language, or Redis Search compatibility.
Each new command must be documented as a Sider extension rather than an implied
Redis-compatible command.

## Fronts and work packages

The order below follows the issue dependency graph, not a calendar or release
commitment. Each package should fit one reviewable PR with relevant tests. The
GitHub issue is the executable task; these descriptions are an overview.

| Front | Issue | Depends on | Default model | Deliverable and exit check |
| --- | --- | --- | --- | --- |
| Base contract | [Deployment profile #105](https://github.com/djairofilho/sider/issues/105) | Existing 1.0.1 contract | Sol | Declare supported topology, failure assumptions, durability, recovery objective, and explicit exclusions; reconcile README and compatibility documentation with published tags. |
| Base contract | [Client integration audit #106](https://github.com/djairofilho/sider/issues/106) | #105 | Luna for inventory, Sol for decisions | Test representative RESP2 clients against the actual handshake and commands; choose a bounded compatibility subset or a Sider client, with recorded failures and tests. |
| Operations | [Recovery drill #107](https://github.com/djairofilho/sider/issues/107) | #105 | Sol | Restore from backup and recover from AOF on both supported platforms; record exact commands, SHA, data comparison, elapsed TTL behavior, and failure cases. |
| Operations | [Resource envelope #108](https://github.com/djairofilho/sider/issues/108) | #105 | Sol | Measure logical bytes, RSS, queue pressure, p50/p95/p99 latency, and saturation for declared workloads; define limits and admission behavior without calling logical quota an RSS cap. |
| Security | [Network exposure contract #115](https://github.com/djairofilho/sider/issues/115) | #105 | Sol | Specify authenticated data and administrative access, transport protection, secret handling, and default binding before claiming networked production support. |
| Security | [Implement and verify controls #93](https://github.com/djairofilho/sider/issues/93) | #115 | Sol | Reject unauthorized operations on both listeners; verify secure transport, failure paths, rotation procedure, and compatibility impact with integration tests. |
| AI data | [Memory record and wire contract #109](https://github.com/djairofilho/sider/issues/109) | #105 | Sol | Specify collection identity, binary content, embedding type/dimension/metric, metadata types, model identity, provenance, TTL, error replies, and resource accounting. |
| AI data | [Durable record lifecycle #110](https://github.com/djairofilho/sider/issues/110) | #109 | Sol | Create, replace, delete, expire, replay, replicate, back up, restore, and migrate memory records without inconsistent visible state. |
| Retrieval | [Exact filtered search #111](https://github.com/djairofilho/sider/issues/111) | #110 | Sol | Return top-k exact matches with deterministic ties and namespace filters; reject dimension mismatches and non-finite values; enforce bounded work and output. |
| Retrieval | [Relevance and load evidence #112](https://github.com/djairofilho/sider/issues/112) | #111 | Luna for fixtures, Sol for evaluation | Keep a versioned query corpus with expected eligible records, recall at k, filter correctness, latency, and memory measurements; report workload and SHA. |
| Agent use case | [Reference integration #113](https://github.com/djairofilho/sider/issues/113) | #111 | Luna for example scaffolding, Sol for contract review | Demonstrate external embedding generation, ingest, retrieval, deletion, TTL, and source attribution in one reproducible application. |
| Scale decision | [Approximate index experiment #114](https://github.com/djairofilho/sider/issues/114) | #112 | Sol | Compare exact search with an index on the same corpus and filters. Implement an index only if measured latency, recall, build cost, recovery, and memory justify it. |

[Release validation #96](https://github.com/djairofilho/sider/issues/96) remains part
of the operational epic. [Comparative benchmarking #97](https://github.com/djairofilho/sider/issues/97)
is related work, separate from the Sider resource envelope and from release approval.

### Existing advanced backlog

The older open issues also remain executable backlog. They do not silently become
requirements of the first agent-memory release. Their issue bodies hold the
acceptance criteria. GitHub records #90 as blocked by #93, #89 as blocked by #90,
and #91 as blocked by #90. Other prerequisites in the table remain planning
decisions until their contracts and native dependencies are recorded.

| Front | Issue | Model route | Prerequisite decision and exit evidence |
| --- | --- | --- | --- |
| Wire protocol | [RESP3 #94](https://github.com/djairofilho/sider/issues/94) | Sol for codec and connection state; Luna for literal fixtures after the subset is frozen | Negotiate per connection, preserve RESP2, cover declared types and Redis differential cases. |
| Scripting | [Sandboxed Lua #95](https://github.com/djairofilho/sider/issues/95) | Sol for runtime, limits, mutation, and replay; Luna for bounded script fixtures | Review the [proposed Lua contract](lua-scripting.md), then prove runtime containment, effect replay, and sandbox denial before enabling scripts. |
| Cluster control plane | [Topology and membership #90](https://github.com/djairofilho/sider/issues/90) | Sol | Build on #115/#93 network identity and an explicit ownership/redirect contract; test stale epochs and recovery. |
| Cluster safety | [Automatic failover #89](https://github.com/djairofilho/sider/issues/89) | Sol for quorum/fencing; Luna for partition fixture preparation | Specify quorum and fencing against #90 topology before promising single-primary writes; deterministic partition tests are required. |
| Data movement | [Online resharding #91](https://github.com/djairofilho/sider/issues/91) | Sol; Luna may generate migration datasets | Define ownership cutover and AOF/replica recovery against the chosen #90 topology; prove pause, resume, and crash behavior. |
| Transactions | [Cross-shard transactions #92](https://github.com/djairofilho/sider/issues/92) | Sol | Review the [proposed single-process contract](cross-shard-transactions.md), then prove atomicity and recovery with the current local shard layout; a later cluster participant protocol depends on #90. |

Luna should not own final decisions about quorum, script sandboxing, online
migration, or multi-shard commit. A focused inventory or deterministic fixture
set can run in parallel once Sol states the exact contract and validation
commands. #96 measures a chosen publishable candidate; closing an individual
feature issue is not a substitute for those release receipts.

The client audit, recovery drill, resource envelope, security specification, and AI
record specification can proceed independently after the deployment profile. The
same integrator owns changes to `src/command/`, `src/storage/`, persistence format,
and release contracts. Parallel workers use separate worktrees and do not edit
those shared contracts concurrently.

## AI memory contract for implementation

The architecturally accepted [version 1 memory contract](agent-memory-v1.md)
fixes #109's design before #110 changes storage. It chooses a single-shard collection
key with binary member IDs; immutable dimension, `F32LE` encoding, `L2SQ`
distance, and model identity; whole-record visibility with durable member
deltas; exact namespace and typed metadata filtering; and explicit admission,
search, reply, and temporary-allocation bounds. A new member cannot make its
collection exceed the exact-search work budget. Namespace eligibility is a
query predicate, not authorization. The database receives vectors and never
runs inference in an owning worker. The reference integration records provider,
model revision, transformation, and source provenance.

#109 remains open until the accepted document is versioned. #110
and #111 own implementation and failure evidence, including AOF, replication,
backup, TTL, and an independent exact-search oracle. The contract does not
claim that the current binary stores or searches memory records.

For small datasets, exact search supplies a correctness oracle. Approximate
indexes trade recall for speed and add memory and rebuild costs. Filtered search
needs explicit semantics and evaluation across selective filters. See
[pgvector's index guidance](https://github.com/pgvector/pgvector/blob/master/README.md)
and [Redis vector search concepts](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/)
for comparison; neither is a Sider compatibility promise.

## Promotion into the release manifest

The operational-base epic and the AI-memory epic will have **separate publishable
gates**. The base gate must first establish its deployment profile, security
boundary, recovery and resource evidence, and cumulative release validation.
The memory gate then adds the record lifecycle, exact search, corpus, and
reference integration on top of an approved base. This prevents unfinished
memory work from weakening base claims and prevents base evidence from being
mistaken for memory evidence. Neither gate is approved by this sequencing
decision. Version numbers remain unassigned until their entries are accepted
in the manifest. RESP3, Lua, cross-shard transactions, and cluster work require
their own explicit release-scope decision before they enter a candidate; code
in a development checkout alone does not expand a published profile.

1. Execute the issue graph from #105. Keep each issue's native parent and
   dependency links; use its acceptance evidence to decide when its work is
   complete.
2. Before release preparation, convert chosen issues into `releases/plan.json`
   tasks with objective,
   deliverables, tests, acceptance, technical dependencies, and a milestone gate.
   Add new gate commands and update `xtask` contracts when the feature needs them;
   a pending gate must fail publication rather than count as a pass.
3. Regenerate `ROADMAP.md` with `cargo xtask roadmap --write`, run
   `cargo xtask check --tools`, and inspect `cargo xtask sync` as a dry run.
4. Before applying synchronization, bind each new manifest task ID to its
   existing GitHub issue with exactly one `sider:task` marker inside one
   `sider:managed` block. For example, append the following block to the
   existing issue body, replacing `R13-01` with its actual manifest task ID:

   ```text
   <!-- sider:managed:start -->
   <!-- sider:task R13-01 -->
   <!-- sider:managed:end -->
   ```

   Preserve the original issue body outside that block. Review the dry run for
   duplicate issue creation, title/body changes, and milestone placement. The
   synchronizer replaces the managed block, preserves human comments and
   unmanaged text, and neither closes tasks nor approves releases.
5. Record focused tests and SHA in the active issue. A PR may close several
   related tasks only when each acceptance criterion is verified.

## Routing and escalation

Use `gpt-6-luna` for bounded inventories, documentation corrections, fixture
preparation, and examples with a frozen contract. Use `gpt-6.1-sol` for architecture,
new wire or storage contracts, security, concurrency, durability, integration,
and release decisions. Sol integrates changes that touch shared contracts.
The [official OpenAI model selection guide](https://developers.openai.com/api/docs/guides/model-selection)
describes Luna for scoped, frequent work and Sol for complex technical work.
This is a task routing policy, not a claim that a model alone validates a result.

Escalate a Luna task to Sol when it changes observable behavior, crosses module
boundaries, has an ambiguous requirement, fails validation twice for different
causes, or requires a new persistence or security decision. Reduce a Sol task to
Luna only after Sol has frozen the contract and written a bounded handoff with
expected files and checks. If either model is unavailable in the current tool,
keep the task requirements and assign the available capable model; record the
substitution in the issue.

The operational loop and stop conditions are in [the agent runbook](agent-development.md).
