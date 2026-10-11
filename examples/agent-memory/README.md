# Reference agent memory client

This credential-free example demonstrates `sider-memory/1` with deterministic
two-dimensional embedding fixtures. The vectors are generated outside Sider;
the example does not call a model or make a relevance claim. Each record carries
the provider, model revision, input transformation, and source provenance.

## Verify an extracted development binary

Use a development `sider` binary with `MEM.*` and record-2 AOF. The published
1.0.1 binary does not implement these commands. From a clean checkout, run on
Windows with Python 3.10 or newer:

```powershell
cargo build --locked --bin sider
python -B .\examples\agent-memory\verify_extracted.py --binary .\target\debug\sider.exe --output "$env:TEMP\sider-memory-reference.json"
python -B -m unittest discover -s .\examples\agent-memory -p 'test_*.py' -v
```

On Linux:

```sh
cargo build --locked --bin sider
python3 -B examples/agent-memory/verify_extracted.py --binary target/debug/sider --output /tmp/sider-memory-reference.json
python3 -B -m unittest discover -s examples/agent-memory -p 'test_*.py' -v
```

The verifier creates a temporary development ZIP from the built binary,
extracts it, checks byte equality by SHA-256, then starts the **extracted
executable** on loopback with a fresh record-2 AOF directory. The JSON records
the source SHA, source/archive/extracted binary hashes, configuration,
platform, and exact RESP bulk arguments and replies as hex. It tests provider
failure before any write, filtered retrieval, source attribution, replacement,
deletion, TTL expiry, and recovered state after process restart. It publishes
JSON only after all assertions pass. The temporary ZIP and AOF directory are
removed. This is a development-binary integration check, not a release receipt.

The expected filtered result is `policy-1` in `team-a`. The `team-b` policy is
excluded by namespace, and the `team-a` note by the typed
`category BYTES policy` predicate. After replacement, `policy-1` has source
`handbook://security` and revision `rev-8`. `note-2` and the short-lived
`temporary` record remain absent after restart.

## Manual in-memory run

Start a development binary in one PowerShell terminal:

```powershell
$env:SIDER_SECURITY_PROFILE = 'legacy-loopback'
$env:SIDER_ADDR = '127.0.0.1:6380'
Remove-Item Env:SIDER_SECURITY_DIR -ErrorAction SilentlyContinue
Remove-Item Env:SIDER_AOF_DIR -ErrorAction SilentlyContinue
& .\target\debug\sider.exe
```

In another terminal:

```powershell
python -B .\examples\agent-memory\reference.py --host 127.0.0.1 --port 6380
python -B .\examples\agent-memory\reference.py --fail-provider
```

After stopping and restarting this in-memory process, verify absence:

```powershell
python -B .\examples\agent-memory\reference.py --host 127.0.0.1 --port 6380 --check-empty
```

A manually started record-2 AOF process can instead be checked after restart
with `--check-recovered`. The automated verifier supplies the exact durable
configuration and cleanup.

## Security and write boundary

`legacy-loopback` has plaintext access without authentication or ACL. The
`namespace` field is a search eligibility filter, not an authorization
boundary: every reachable client can request every namespace. Protected
deployment requires the separate controls under issue #93.

The example computes and validates all vectors before `MEM.CREATE`. Missing
credentials or a provider failure can therefore send zero Sider commands, as
the unit tests and extracted-binary verifier check. The fixture needs no
credentials or paid service. Once writes begin, a network disconnect can leave
a partial batch. A caller requiring batch atomicity must provide retry and
reconciliation logic or use a future transaction contract.
