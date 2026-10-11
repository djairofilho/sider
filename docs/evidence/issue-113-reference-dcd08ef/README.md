# Issue #113: extracted reference integration

Validation date: 2026-10-11. Source commit:
`dcd08ef183e43c76f400c25876c0df07cdacddff` (PR #144 merge).
The [reference guide](../../../examples/agent-memory/README.md) gives clean
Windows and Linux commands. The verifier created a temporary development ZIP,
extracted its only executable, confirmed byte identity with SHA-256, and ran
that extracted binary with record-2 AOF and `always` sync. These runs do not
represent an official released package.

| Environment | Extracted binary SHA-256 | Full command/reply transcript |
| --- | --- | --- |
| Windows NT 10.0.26300, NTFS, Python 3.11.9 | `ebbc711909fc6a0a135eac9924fb2a64996b9ee6e46f55686723842090c45236` | [Windows JSON](windows.json) |
| Ubuntu 22.04 on WSL2/ext4, Python 3.10.12 | `62d07fa605d8ea48159e0716645e3a13ab2de7d1d4ed2ff2489cd272945c4ff6` | [Linux JSON](linux.json) |

Each run recorded 20 RESP commands and replies. A requested provider failure
sent zero commands and `MEM.INFO` confirmed the collection was absent before
ingestion. The deterministic external fixture then ingested three records.
Filtered search returned only `policy-1` in `team-a`; `MEM.GET` returned its
source and model provenance. Replacement changed the content and source
revision to `rev-8`. A 150 ms TTL record expired, and `note-2` was deleted.
After process restart from the same AOF directory, the schema, filtered hit,
replacement content, source attribution, and other-namespace record remained;
the expired and deleted records were absent. Six Python unit tests passed on
both platforms, including a missing-credential preflight fixture and binary
RESP framing.

The ZIP and AOF directories were temporary. This evidence validates the
development integration and cleanup path, not a candidate-package release
gate. The loopback profile has no authentication or ACL. Namespaces are
eligibility filters, not access controls; protected deployment requires
[issue #93](https://github.com/djairofilho/sider/issues/93).
