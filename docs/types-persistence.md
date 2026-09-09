# Persistence for collections and sorted sets

The `typed_migration_from_frozen_r04_binary_output_preserves_shards_and_elapsed_ttl`
test reads `tests/fixtures/aof-r04-four-shards.hex`, an exact copy of the AOF
produced by migrator `4739d596f8d80d0038a9a97838395c90be60ceff` from the real R03
baseline `c7b148abeff43fd354d29b1bbce36aea43ea8f3f`. SHA-256 of the bytes:
`b6be7a45ad5e57eb7957d10136afddec6488c60f7aa59bb1c5522388ba4a246f`.
With an injected clock, it verifies four shards, sequence two, old data, and TTL;
adds every type, compacts, and recovers after the deadline. It is evidence for the
frozen file and current reader; it does not attribute results to the historical
executable.

Hashes, lists, sets, and sorted sets use the same AOF writer as strings. Writing
receives the complete typed post-image and resolved absolute deadline. Under
`always`, storage applies that image after the writer confirms batch sync.

The AOF v1 codec retains the earlier string and deletion tags. Tags `3`, `4`, `5`,
and `6` identify hash, list, set, and sorted set. The decoder bounds a record
before allocating its body and validates counts, duplicates, and scores. Compaction
preserves sequence and reapplies the delta before publishing the new generation.

## Limits and expiration

The AOF record has a configurable limit of up to 64 MiB, also its default. A
command whose result fits quota but exceeds that record returns
`ERR AOF record limit exceeded`. It does not change data, sequence, or file and
allows continued use of the worker. Logical quota follows the costs defined in
the [collections](collections.md) and [sorted sets](sorted-sets.md) guides.

Reads that encounter expired collections prepare tombstones with `Expiration`
origin. Deletion passes through the AOF before being applied. After this durable
deletion, moving wall clock backward during restart does not resurrect the key.
Replay also discards post-images whose absolute deadline has elapsed and restores
quota only for values still present.

## Executed evidence

```powershell
cargo test --locked --test persistence typed_
```

Five tests pass on Windows, each covering all four families:

- Round trip and compaction restore complete data, score bits, TTL, and quota.
  An injected clock verifies a 250 ms deadline and deletion on expiration.
- Type, quota, and record-limit errors preserve the value, AOF sequence, and
  `PING` service.
- Updates while a snapshot is paused appear in the recovered delta. A later
  passive expiration persists its tombstone before restart.
- Thirty-six processes are interrupted at the nine append, sync, response,
  snapshot, and publication points. Recovery accepts the previous or complete new
  state when there was no confirmation; after sync or confirmation, it requires
  the new state.
- The fixed v1 AOF strings fixture can receive every new type, pass through
  compaction, and recover without losing the prior string.

The `typed_aof_process_child` helper is ignored in ordinary runs and started
explicitly by parent tests with variables restricted to the child process. No test
changes the global environment or depends on fixed ports.

## Limits of verified migration

A clean-checkout executable at `a615f705266c7562eece5e751d43d94b2b0eb363`
generated the R05 internal baseline with four shards, strings, hashes, lists,
sets, and TTL. The process confirmed five mutations with `always`; restarting the
same binary checked all four types. The binary, configuration, commands, logs,
and hashes are preserved in
`target/baselines/r05-a615f705266c7562eece5e751d43d94b2b0eb363/`.
The executable has SHA-256
`7eb68f536d9ba8c6e41f430fb7ed66cb04b2e0dab38dcb8f6e6401b20da42aba`;
the 403 AOF bytes have SHA-256
`87d12a8fc88698266882a6dce88506249fdd7dac3ffe594fc12e42cded47242b`.

`sorted_set_migration_from_frozen_r05_binary_output_preserves_collections`
uses those bytes in `tests/fixtures/aof-r05-collections.hex`, recovers previous
types, adds and updates a sorted set, compacts, and restarts with an injected
clock after hash expiration. The other types and ordering remain. The rehearsal
passed on Windows and Linux; the historical SHA identifies the data source without
attributing validation of new code to it.

`tests/fixtures/aof-v1.hex` was introduced at commit
`ea86917`, before the new types. The SHA-256 of the text file is
`9236640383afe34c7733dddc5cbf30153b0be8b1f7108c584bff3133f8b10862`.
The rehearsal verifies preserved format evolution, including reading, new writes,
and compaction. It does not replace migration between executables from internal
baselines frozen by SHA and hashes, or demonstrate native Linux execution or
approval of 1.0 candidate artifacts.
