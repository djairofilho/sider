# Issue #111: exact memory-search validation

Validation date: 2026-10-11. Source commit:
`df38b940fac32a05431d6fdb194cd538d487c59d` (PR #140).
The Windows and Linux checkouts were clean at that commit. Rust toolchain:
`rustc 1.97.1 (8bab26f4f 2026-07-14)`.

| Environment | Filesystem | Debug server SHA-256 |
| --- | --- | --- |
| Windows NT 10.0.26300.0, x86-64 | NTFS (`C:`) | `63ACF34F696BE2B0AD1B94B66A06931AC3A584541AD245D66E367CA2449FD932` |
| Ubuntu 22.04 in WSL2, Linux 6.6.87.2, x86-64 | ext4 (`/home/djairofilho`) | `b35b684822c3ed658f6e0140c04fb1c3c527091589c8021a875799709db63e14` |

## Commands and observations

`cargo xtask check` passed on the final PR #140 source tree on Windows. It ran
format, Clippy, a server build, every non-ignored native test, and doctests.
Focused `agent_memory_search`, `agent_memory_tcp`, `agent_memory_backup`, and
`replication_network` cases passed before integration. The merged checkout on
Linux/ext4 passed:

```sh
cargo test --locked --test agent_memory_search --test agent_memory_tcp \
  --test agent_memory_backup --test replication_network
```

The exact oracle compares binary64 scores and unsigned binary-ID tie order.
Fixed fixtures include binary and prefix IDs, typed metadata filters, namespace
exclusion, and expired records. A generated test compares 48 bounded collections
with their independently sorted oracle under forward and reverse insertion.
Lifecycle tests check replacement, deletion, and replayed images. Backup and
CLI-restored images return the same exact search result, and two real processes
return identical search replies after full sync, a delta, source restart, and
replica restart.

The TCP limit case inserts 1,024 records, requests the maximum `k = 128`, and
observes exactly 1,024 record visits and 2,048 scored components. Its reply is
under 8 KiB and the local query completes within five seconds; these are
test assertions for this declared fixture, not service latency guarantees.
`k = 129` is rejected while the connection stays usable. Other native and TCP
tests cover wrong dimensions, non-finite vectors, unsupported or duplicate
filters, empty eligible sets, TTL visibility, and collection-key-only routing.
The implementation's declared bounds are 1,024 retained members, 1,048,576
scored components, and 128 retained hits per query.

## Boundary

These are development-checkout results, not release receipts or extracted
package claims. WSL2/ext4 exercises Linux behavior without representing an
independent Linux host. The relevance and load corpus belongs to #112. A
future memory publication gate and candidate artifacts remain under
[epic #104](https://github.com/djairofilho/sider/issues/104).
