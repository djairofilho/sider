# Candidate soak test

The `tests/soak.rs::release_soak_gate` runner executes at least 3,600 seconds of
load against real Linux processes from the extracted package. It checks the
executable against the `.tar.gz` member and preliminary manifest before and after
the rehearsal. The receipt is published only after verifying invariants and
collecting the processes.

## Declared load and limits

There are eight binary-key sets, distributed by hash tags across four shards,
with seed `0x511e1103`. Each iteration confirms an eight-command EXEC: two paired
strings, a hash, list, set, and sorted set. The independent model retains each
set's expected value and checks bytes, cardinality, list order, and score. The
replica must converge and retain the same state.

Load is limited to 20 iterations per second, with a 4 MiB quota per instance,
`always` AOF, and compaction starting at 128 KiB. This is this duration test's
configuration, not a product throughput promise. The quota measures logical
dataset memory; RSS is read separately from `/proc/PID/status`. The rehearsal
envelope is 512 MiB RSS per process, including runtime and buffers.

Every second the runner records RSS, queues, dataset, expiration, AOF, and
replication state in `soak-samples.jsonl`. Queues must not exceed capacity, the
dataset must not exceed quota, and worker/AOF failures, including compaction
failures, are blocking. At least one compaction must be observed; the report sums
each process's counters before interruptions and at the end, accumulating
compactions seen before each restart. Results include configuration, monotonic
duration, counts, and final state. Sampling does not promise to capture peaks
between observations.

## Failures and progress

Every minute, the runner checks an aborted WATCH, actual TTL expiration, RESP
reconnection, and isolation of a slow subscriber. The healthy subscriber must
receive 128 64 KiB messages in order; the slow one must be removed by the bounded
queue.

Every five minutes the replica is interrupted and restarted. Alternately, the
primary is also interrupted, recovers confirmed data, and starts a new epoch.
The report requires observing both CONTINUE and FULL. Writes occur while the
replica is unavailable, and all types are verified after recovery. Interruptions
happen between confirmed batches; interruptions during append, sync, and
publication belong to failure tests and the `crash` gate.

Data directories and rehearsal records are preserved in a new output directory.
No existing directory is overwritten. A failure stops the test without a success
receipt; samples already written remain available.

## Execution

Prepare the [release](releases.md) context, with a preliminary manifest and Linux
package at the root of `SIDER_RELEASE_DIR`. `SIDER_PACKAGE_DIR` points to the
directory actually extracted. Run explicitly:

```sh
cargo test --locked --test soak -- --ignored --exact release_soak_gate --nocapture
```

The runner creates `SIDER_RELEASE_DIR/soak`; that path must be absent. The receipt
includes record hashes in addition to package identity. The files enter the
candidate evidence set. The final promotes exactly that set without repeating the
hour of load.

To test the runner itself during development, there is a 25-second rehearsal with
accelerated events and no release receipt:

```sh
SIDER_SOAK_BINARY=/absolute/path/sider \
SIDER_SOAK_OUTPUT_DIR=/absolute/path/new-output \
cargo test --locked --test soak -- --ignored --exact internal_soak_rehearsal --nocapture
```

The short rehearsal reduces the compaction threshold to **8 KiB** and retains the
requirement to observe a completed compaction with no failures. The report records
`compaction_after_bytes:8192` and the `compactions` count. Functional events occur
every five seconds and restarts every eight seconds. The 3,600-second gate retains
128 KiB, events per minute, and restarts every five minutes.

This short rehearsal requires the binary integrated with replication and metrics.
It does not approve duration, the package, or the candidate gate. An ignored entry
in the ordinary suite is also not evidence of approval.
