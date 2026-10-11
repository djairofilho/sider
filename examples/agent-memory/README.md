# Reference agent memory client

This credential-free example demonstrates the frozen `sider-memory/1` TCP
commands with a deterministic two-dimensional embedding fixture. The fixture
stands in for an external embedding provider; it does not call a model or make
a relevance claim. In an application, generate vectors before sending commands
to Sider and record the provider, model revision, transformation, and source
provenance with the record.

## Prerequisites and run

Use a development `sider` binary that includes the `MEM.*` TCP parser. The
published 1.0.1 binary does not implement these commands. The memory search
stage also leaves the default server AOF profile on record 1, so run this
example in in-memory mode. It demonstrates process restart behavior as data
loss; durable restart/recovery awaits activation of the record-2 profile and
the remaining lifecycle/recovery gates.

From this development branch, build the executable. In the first terminal,
set its path and start it. An extracted development bundle with these changes
can be used by setting `$SiderBin` to its executable instead:

```powershell
cargo build --locked --bin sider
$env:SIDER_SECURITY_PROFILE = 'legacy-loopback'
$env:SIDER_ADDR = '127.0.0.1:6380'
Remove-Item Env:SIDER_SECURITY_DIR -ErrorAction SilentlyContinue
Remove-Item Env:SIDER_AOF_DIR -ErrorAction SilentlyContinue
$SiderBin = (Resolve-Path .\target\debug\sider.exe).Path
& $SiderBin --version
& $SiderBin
```

The explicit `legacy-loopback` profile binds plaintext access to loopback and
has no authentication or ACL. Keep this example on a trusted local machine.
The `namespace` argument is an application eligibility filter only; every
client that reaches the listener can request any collection or namespace. It
does not authenticate a user or authorize access. Protected network use waits
for the separate security implementation and verification.

In a second terminal at the repository root, use Python 3.10 or newer:

```powershell
python .\examples\agent-memory\reference.py --host 127.0.0.1 --port 6380
python .\examples\agent-memory\reference.py --fail-provider
python -m unittest discover -s .\examples\agent-memory -p 'test_*.py' -v
```

The first command prints the filtered hit, source attribution, replacement,
expiry, deletion, and the in-memory restart limitation. Each record carries the
fixture provider, model, revision, and transform as provenance as well as the
collection's immutable model identity. A filtered search should
return only `policy-1` in `team-a`; the `team-b` policy is excluded by namespace
and the `team-a` note is excluded by the typed `category BYTES policy` predicate.
`--fail-provider` prints `preflight failed: fixture provider failure requested;
Sider commands sent: 0` and exits without opening a connection. The unit tests
check this preflight boundary, reject non-finite output, and verify binary-safe
RESP request framing.

## Failure and write boundary

The example computes and validates every vector before `MEM.CREATE`, so a
missing credential or failed external provider can follow the same application
boundary: fail preflight and send zero Sider commands. The deterministic fixture
needs no credentials. This guarantee does not make a sequence of network writes
transactional. A disconnect or server failure after `MEM.CREATE` or some
`MEM.PUT` commands can leave a partial batch; callers needing batch atomicity
must define retry/reconciliation behavior or wait for a separately specified
server transaction contract.

## Restart check

After the example finishes, stop Sider with Ctrl+C and start it again using the
same commands. Then run:

```powershell
python .\examples\agent-memory\reference.py --host 127.0.0.1 --port 6380 --check-empty
```

It expects `MEM.INFO after restart: None` because this process has no AOF. Keep
`SIDER_AOF_DIR` unset for this draft: this branch's default server writes record
1 and rejects the memory durability profile. Record-2 restart/recovery is not
claimed by this example.
