# Derived-index screening protocol

This issue #114 experiment uses [corpus v1](corpus-v1.json) and the exact
server results under [issue #112](../../docs/evidence/issue-112-corpus-e2475bd/README.md).
The frozen memory contract admits at most 1,024 members per collection, so the
runner tests 256 members from the measured corpus and 1,024 from the same seed.
It uses the same namespace and metadata filters, query vectors, and `k = 10`.
Run the Python 3.10+ prototype from a clean checkout with an output directory
outside the checkout:

```sh
python3 -B benchmarks/memory/approximate.py --output-dir /tmp/sider-index-screening --repetitions 100
```

The in-process exact scanner is the oracle. One derived candidate partitions
two-dimensional vectors into a fixed 4×4 centroid grid and probes either the
nearest one or four cells. A second candidate sorts by the first coordinate
and scores the 64 nearest values on that axis. Both reapply the exact namespace
and typed metadata filters before ranking. Missing eligible neighbors count
against recall@10. Neither candidate is installed in Sider.

Before measuring, the decision threshold is:

- Every nonempty query shape must reach recall@10 of at least 0.99, with no
  ineligible record in any result. A selective filter cannot be rescued by a
  broad-filter average.
- At 1,024 members, p99 in-process query time must be no more than half of
  the exact scanner's p99 on the same platform and run. Serialized index bytes
  must be at most 262,144, and median full rebuild at most 100 ms.
- A passing prototype would justify a Rust implementation trial, not release
  or capacity claims. A failing candidate supports deferring the index under
  the present 1,024-member contract.

The runner records all latency, build, Python allocation, and whole-process RSS
samples, index layouts, and per-query recall. It rebuilds after replacement,
deletion, and TTL expiry. It also times rebuilds from equivalent record images
labeled AOF, backup, and replica installation. These are derived-state
simulations: the prototype does not consume Sider AOF, backup, or replication
frames. Actual persistence-path tests would be required before implementing
an index. The separate #112 server baseline is identified by its raw file and
binary SHA; Python in-process times are compared only with the Python exact
scanner, not with Sider's TCP measurements.
