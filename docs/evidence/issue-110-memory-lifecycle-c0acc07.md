# Issue #110: record-2 lifecycle validation

Validation date: 2026-10-11. Source commit:
`c0acc07e77503487023b7e36dc5bc7e849c1a869` (PRs #130–#138).
Both checkouts were clean at that commit. Rust toolchain:
`rustc 1.97.1 (8bab26f4f 2026-07-14)`.

| Environment | Filesystem | Debug server SHA-256 |
| --- | --- | --- |
| Windows NT 10.0.26300.0, x86-64 | NTFS (`C:`) | `87950660766E52F838883D17A8D5CE41E83C7CF824D1CAC832AE0CAE669B5507` |
| Ubuntu 22.04 in WSL2, Linux 6.6.87.2, x86-64 | ext4 (`/home/djairofilho`) | `b35b684822c3ed658f6e0140c04fb1c3c527091589c8021a875799709db63e14` |

## Commands and results

On Windows, `cargo xtask check`, `cargo doc --locked --no-deps`, and
`cargo build --locked --release` passed on the final PR #138 source tree, which
was merged without code changes into the commit above. The native gate
includes format, Clippy, the server build, and the full non-ignored test suite.
Focused record-2 AOF, backup, and two-process replication tests passed during
the implementing PRs.

On Linux, a clean checkout of the same commit was placed on ext4 under a
restricted `/home/djairofilho` ancestor. These commands passed:

```sh
cargo test --locked --test agent_memory_aof --test agent_memory_backup \
  --test agent_memory_migration --test agent_memory_tcp --test replication_network
cargo test --locked --test agent_memory_migration_cli --test agent_memory_format
cargo xtask check
```

The targeted suites cover record-2 framing and every truncated record prefix,
AOF fault boundaries and replay, compaction and generation installation,
backup/restore, offline migration, and TCP lifecycle behavior. The two-process
replication case kills and restarts source and replica processes, checks full
sync and deltas across an epoch change, promotes the replica, then checks local
writes and recovery after another process kill. Existing type, transaction,
routing, compatibility, and security tests also passed in both native gates.

The first Linux gate attempt ran from `/tmp` and stopped in the POSIX security
test because its writable ancestor violates that test's documented permission
contract. The checkout was moved to a restricted home directory. The isolated
POSIX test and the complete gate then passed there without source changes.

## Boundary

These are development-checkout results, not a release receipt or a claim about
an extracted package. WSL2/ext4 exercises Linux filesystem behavior, but it is
not an independent Linux host. The later memory publication gate, candidate
artifacts, and cumulative release validation remain owned by the release work
under [epic #104](https://github.com/djairofilho/sider/issues/104).
