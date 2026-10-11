# Issue #114: approximate-index decision

Decision date: 2026-10-11. Source commit:
`0d88504c2db54059423cd1cd5c666592c0316ae4` (PR #147 merge).
The predeclared [screening protocol](../../../benchmarks/memory/index-screening.md)
used corpus `sider-memory-corpus/1`, SHA-256
`16ae499aae3ce7a8c1541789d134c1f11dfe523a986fbf8cec0bda1beae74167`,
seed `1142026`, 256 and 1,024 records, 37 query shapes and 100 repetitions.
The earlier [exact Sider server baseline](../issue-112-corpus-e2475bd/README.md)
identifies its own binary and source SHA. The Python in-process comparisons here
are algorithmic screening, not Sider TCP latency measurements.

| Environment | Raw input/results and generated report | Archive SHA-256 |
| --- | --- | --- |
| Windows NT 10.0.26300, NTFS, Python 3.11.9 | [ZIP](windows.zip) | `abf1c1cf627db16809925bc369fa577ad04722a63c5dffcb461967a9deb80a27` |
| Ubuntu 22.04 on WSL2/ext4, Python 3.10.12 | [ZIP](linux.zip) | `b2045ee4cc2360f7993af0c1eca247db33552e5bb37dfbb78511ae4d1e008d51` |

Each archive contains `raw.json` and `report.md`. Both were extracted and
compared byte for byte with the original output. The raw files retain all
37 query definitions, 3,700 latency samples per method and size, RSS samples,
three build-time and Python allocation samples, serialized index size,
per-query recall, and rebuild timings after replacement, deletion, TTL expiry,
and equivalent recovery images. The AOF, backup, and replica labels represent
rebuilding derived prototypes from identical record images; no indexed Sider
process or format was exercised.

Both environments used the same Intel Core i5-9300H host with eight logical
processors. Windows exposed 34,188,873,728 bytes of RAM and the WSL2 guest
16,685,158,400 bytes, as recorded in the linked #112 baseline. The Python
runner recorded each operating-system and interpreter version in its raw file.

| Records | Method | Windows mean/min recall@10 | Linux mean/min recall@10 | Windows/Linux p99, ms | Serialized index bytes at 1,024 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 256 | Exact | 1.000 / 1.000 | 1.000 / 1.000 | 1.296 / 0.543 | 0 |
| 256 | 4-by-4 grid, 1 probe | 0.527 / 0.100 | 0.527 / 0.100 | 0.080 / 0.072 | 7,297 |
| 256 | 4-by-4 grid, 4 probes | 0.980 / 0.800 | 0.980 / 0.800 | 0.173 / 0.184 | 7,297 |
| 256 | Sorted x-axis, 64 candidates | 0.797 / 0.400 | 0.797 / 0.400 | 0.178 / 0.287 | 29,463 |
| 1,024 | Exact | 1.000 / 1.000 | 1.000 / 1.000 | 2.829 / 3.013 | 0 |
| 1,024 | 4-by-4 grid, 1 probe | 0.650 / 0.000 | 0.650 / 0.000 | 0.207 / 0.189 | 7,297 |
| 1,024 | 4-by-4 grid, 4 probes | 1.000 / 1.000 | 1.000 / 1.000 | 0.735 / 0.587 | 7,297 |
| 1,024 | Sorted x-axis, 64 candidates | 0.450 / 0.200 | 0.450 / 0.200 | 0.292 / 0.145 | 29,463 |

The size column gives each 1,024-record candidate's serialized prototype
size; the exact method has no index. Full build/rebuild samples and sampled
whole-process RSS are in the raw files. Python RSS does not isolate index
memory, and the same machine's WSL2 result is not an independent native host.
Both archives preserve references to the exact server baselines, including
their binary hashes. Their latency values are not mixed into this table.

## Decision

**Defer a persistent or derived approximate index under the current
`sider-memory/1` contract.** Every nonempty query had to reach recall@10 of
at least 0.99. The fastest strategies lost many eligible top-k records. The
four-probe grid met the target at 1,024 on this toy corpus but reached only
0.800 minimum recall at 256, a supported collection size. Its faster Python
p99 and small serialized size do not repair that correctness shortfall or
demonstrate an end-to-end server gain. The exact implementation remains the
search oracle and is bounded to 1,024 members per collection.

The failure threshold is the per-query recall target, not a claimed Sider
latency or RSS limit. Revisit only with a larger representative embedding and
filter corpus, a measured Sider bottleneck, and a design that meets the same
recall, memory, update, rebuild, backup, replication, and release gates. No
approximate result is advertised as exact, and this decision grants no release
gate or publication approval.
