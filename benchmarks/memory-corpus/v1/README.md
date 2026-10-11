# Sider memory search corpus v1

This directory contains a small deterministic fixture for issue [#112](https://github.com/djairofilho/sider/issues/112). Its contract reference is `sider-memory/1`; the corpus version is `sider-memory-corpus/1`. It is fixture data, not a measured workload or a relevance benchmark.

## Contents and reproducibility

- `corpus.json` defines the fixed seed (`1122026`), frozen Unix-millisecond clock, typed metadata, namespaces, TTL cases, lifecycle operations, and queries.
- `vectors.f32le` contains 14 contiguous two-dimensional vectors, each encoded as two little-endian IEEE-754 binary32 values. `vector_index` is the zero-based vector slot.
- `expected.json` records eligible IDs and top-k IDs after the update, delete, and recovery sequence.
- `checker.py` is a standard-library-only independent brute-force oracle. It applies typed metadata and namespace eligibility before ranking, uses sequential binary64 squared-L2, and breaks equal scores by unsigned ASCII ID bytes.

Run from this directory's parent or from any working directory:

```sh
python benchmarks/memory-corpus/v1/checker.py
```

The fixed seed documents corpus version 1; the fixture is literal and does not generate random values. `SHA256SUMS` pins the checker and three source inputs; verify them with `sha256sum -c benchmarks/memory-corpus/v1/SHA256SUMS` from the repository root. The fixture includes a record expiring exactly at the frozen clock (ineligible), a still-live TTL record, a separate namespace, selective typed predicates (including a live rank-2 control record), a BOOL-vs-I64 mismatch, a NUL byte in a member ID and BYTES value, an update that replaces the old vector and metadata, a deletion, recovery, and equal-distance IDs to exercise deterministic tie-breaking. The broad query has `k=6` with seven eligible records, testing truncation while its ranked result visibly orders `tie-1` before `tie-2` and `eligible_ids` retains the full set. The `recover` operation models the expected post-recovery visible state; it does not launch Sider or prove persistence behavior.

## Evaluation protocol (pending)

Runtime evaluation is **pending #111**. Do not attribute runtime behavior, latency, throughput, RSS, queue pressure, or search correctness to this fixture until an executable implementing the contract is available and the run is captured. No performance measurement is included or implied here.

For a future run, first record the exact Sider source SHA and binary SHA-256, corpus version and SHA-256 of every input, OS and version, CPU model, physical/logical core counts, RAM, compiler/toolchain, and effective server configuration. Store raw requests/results and a run manifest beside the report. Preserve the seed and input bytes. Measure cold start separately from warm steady state; run broad and selective filters; capture updates, deletes, and post-recovery results. Keep builds and other load tests off the measurement machine.

Report p50, p95, and p99 search latency, throughput, peak observed RSS with sampling interval, logical bytes, and queue pressure with the precise workload counts and run duration. Include repeated runs and raw observations; state that results describe only the recorded binary, host, configuration, and workload. Record failed, skipped, and pending runs explicitly. Do not infer a production guarantee from a single host or corpus.

Geometric correctness and semantic relevance are separate. Compare exact ranked IDs and filter eligibility against `expected.json` for contract correctness. Relevance judgments require separately authored query-to-record judgments and a documented judgment protocol; this fixture contains no human relevance labels and supports no relevance claim.
