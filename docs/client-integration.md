# RESP2 client integration audit for Sider 1.0.1

This is the bounded [issue #106](https://github.com/djairofilho/sider/issues/106)
audit of the published 1.0.1 Windows package. It describes tested client
versions and options, not general Redis client compatibility. The
[deployment profile](deployment-profile.md) limits this binary to controlled
loopback use.

## Tested artifact and environment

| Item | Recorded identity |
| --- | --- |
| Source | Tag `v1.0.1`, SHA `b738a0b4f97074fe272bc21f1eacf5db93cd020e` |
| Package | `sider-v1.0.1-x86_64-pc-windows-msvc.zip`, SHA-256 `40c9f8363d2d9524815f240466b44efdaa4dcb857a536218b541eb403fe547e1`, matching release `SHA256SUMS` |
| Extracted executable | `sider.exe`, SHA-256 `2ffe4aa76fd42e450bce2dfac49139c5ab9caff0b1608219298a4640b3d4068a`; `--version` returned `sider 1.0.1` |
| Host | Windows 11 Pro x64, build `10.0.26300`; Python 3.11.9; Node 22.23.0; npm 10.9.2 |
| Server | `SIDER_ADDR=127.0.0.1:6389`, one shard, other settings unset |

The package is an [asset of the published release](https://github.com/djairofilho/sider/releases/tag/v1.0.1).
The test used a loopback forwarding proxy on port 6390 to record exact request
and response bytes. The server was stopped after both clients and raw probes.
There is no Ubuntu client-library result in this audit.

## Observed client behavior

| Client and configuration | Startup | Operations observed | Required application choice |
| --- | --- | --- | --- |
| `redis-py==5.0.8`, `protocol=2`, `decode_responses=False`, `health_check_interval=0` | Sends two `CLIENT SETINFO` requests; ignores Sider's unknown-command replies | `PING`, binary `SET`/`GET`, and nontransactional `SET`/`GET`/`PING` pipeline passed | Keep RESP2 and bytes replies for binary values; close the connection directly |
| `redis@4.7.0` (`node-redis`), default RESP2 | Sends two `CLIENT SETINFO` requests; ignores the replies | `PING`, binary `SET`/`GET`, and `MULTI`/`SET`/`GET`/`EXEC` passed | Use `sendCommand(..., { returnBuffers: true })` for binary replies and `disconnect()` to close |

The binary test key was `audit:key\x00\xff`; its value was
`value\x00\xfe`. Sider replied with the exact seven value bytes. Without
`returnBuffers: true`, node-redis decoded the invalid UTF-8 byte `0xfe` as
U+FFFD in its string API. Its `quit()` helper sent unsupported `QUIT` and did
not complete during the bounded run; `disconnect()` completed. The Node
package was pinned at 4.7.0 with the
[recorded npm lock](../tests/client-audit/package-lock.json), which resolves
`@redis/client` 1.6.0. Its `CLIENT SETINFO LIB-VER` wire value was `1.5.17`;
the advertised value is not treated as the installed package version.

Representative captured RESP2 traffic (`\r\n` denotes two wire bytes):

```text
redis-py -> Sider: *4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$8\r\nredis-py\r\n
Sider -> redis-py: -ERR unknown command\r\n
redis-py -> Sider: *4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$7\r\nLIB-VER\r\n$5\r\n5.0.8\r\n
Sider -> redis-py: -ERR unknown command\r\n
node-redis -> Sider: CLIENT SETINFO LIB-NAME node-redis; CLIENT SETINFO LIB-VER 1.5.17 (two RESP2 arrays)
Sider -> node-redis: -ERR unknown command\r\n; -ERR unknown command\r\n
either client -> Sider: *2\r\n$3\r\nGET\r\n$11\r\naudit:key\x00\xff\r\n
Sider -> client: $7\r\nvalue\x00\xfe\r\n
```

Separate raw RESP2 probes sent `HELLO 3`, `AUTH user password`, `COMMAND`,
`CLIENT SETINFO LIB-NAME probe`, `QUIT`, and `SELECT 0`. Each returned exactly
`-ERR unknown command\r\n` (`2d45525220756e6b6e6f776e20636f6d6d616e640d0a`
hex), kept the socket open, and allowed a subsequent `PING` to return
`+PONG\r\n`. They neither negotiated a protocol nor selected a database.

## Reproduce the bounded smoke on Windows

Use a disposable directory, keep the server on loopback, and choose free ports.
The commands below reproduce the two client outcomes directly; the captured
wire excerpts above came from an intervening loopback proxy. The hash check is
against the published release, not a local build.

```powershell
$auditDir = Join-Path $env:TEMP 'sider-client-audit-repro'
New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
gh release download v1.0.1 --repo djairofilho/sider --pattern 'SHA256SUMS' --pattern 'sider-v1.0.1-x86_64-pc-windows-msvc.zip' --dir $auditDir
$zip = Join-Path $auditDir 'sider-v1.0.1-x86_64-pc-windows-msvc.zip'
(Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash.ToLowerInvariant()
# Expected: 40c9f8363d2d9524815f240466b44efdaa4dcb857a536218b541eb403fe547e1
Expand-Archive -LiteralPath $zip -DestinationPath (Join-Path $auditDir 'package')
$exe = Join-Path $auditDir 'package\sider-v1.0.1-x86_64-pc-windows-msvc\sider.exe'
(Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash.ToLowerInvariant()
# Expected: 2ffe4aa76fd42e450bce2dfac49139c5ab9caff0b1608219298a4640b3d4068a
python -m venv (Join-Path $auditDir 'pyenv')
$py = Join-Path $auditDir 'pyenv\Scripts\python.exe'
& $py -m pip install -r 'tests/client-audit/requirements.txt'
Copy-Item -LiteralPath 'tests/client-audit/package.json' -Destination $auditDir
Copy-Item -LiteralPath 'tests/client-audit/package-lock.json' -Destination $auditDir
npm ci --prefix $auditDir --ignore-scripts --no-audit --no-fund
$env:SIDER_ADDR = '127.0.0.1:6389'
$server = Start-Process -FilePath $exe -WindowStyle Hidden -PassThru
try {
    Start-Sleep -Seconds 1
    & $py -c "import redis; r=redis.Redis(host='127.0.0.1',port=6389,protocol=2,decode_responses=False,health_check_interval=0); k=bytes.fromhex('61756469743a6b657900ff'); v=bytes.fromhex('76616c756500fe'); assert r.ping(); assert r.set(k,v); assert r.get(k)==v; p=r.pipeline(transaction=False); p.set(b'audit:pipeline',b'one'); p.get(b'audit:pipeline'); p.ping(); assert p.execute()==[True,b'one',True]; r.close(); print('redis-py PASS')"
    $env:NODE_PATH = Join-Path $auditDir 'node_modules'
    node -e 'const redis=require("redis");(async()=>{const c=redis.createClient({socket:{host:"127.0.0.1",port:6389},disableOfflineQueue:true});c.on("error",e=>console.error(e));await c.connect();const k=Buffer.from("61756469743a6b657900ff","hex"),v=Buffer.from("76616c756500fe","hex");if(await c.ping()!=="PONG")throw Error("PING");await c.sendCommand(["SET",k,v]);const got=await c.sendCommand(["GET",k],{returnBuffers:true});if(!Buffer.isBuffer(got)||!got.equals(v))throw Error("binary GET");const result=await c.multi().set("audit:pipeline","one").get("audit:pipeline").exec();if(JSON.stringify(result)!==JSON.stringify(["OK","one"]))throw Error("MULTI");c.disconnect();console.log("node-redis PASS")})().catch(e=>{console.error(e);process.exitCode=1})'
} finally {
    Stop-Process -Id $server.Id -ErrorAction SilentlyContinue
    Remove-Item Env:SIDER_ADDR -ErrorAction SilentlyContinue
    Remove-Item Env:NODE_PATH -ErrorAction SilentlyContinue
}
```

Inspect the package path after extraction; the release bundle uses the
directory above. On a different host, record its OS, binary and dependency
hashes rather than copying this run's evidence.

## Integration decision

For the tested versions, document an **explicit RESP2 client configuration**
and the cleanup/binary API requirements above. No compatibility command is
added solely to make their ordinary operations work: both libraries already
tolerate the two `CLIENT SETINFO` errors. Adding partial `CLIENT` or `QUIT`
behavior would expand the wire contract and require native and Redis
differential tests. A later version or option that treats these errors as
fatal needs a new audit or a narrow implementation. Clients requiring `AUTH`,
`SELECT`, `COMMAND`, or a RESP3 handshake cannot be claimed compatible with
the 1.0.1 binary. #94 and #93 will change the future protocol and security
profile; retest clients at that exact source SHA before extending this table.

The alternative is a small Sider-specific client or adapter with an explicit
handshake and command allowlist. It could shield an application from Redis-client
helpers such as `QUIT`, but it would need its own framing, fragmentation,
timeouts, binary-value, transaction, error, and version tests across two client
languages. That is a larger maintenance surface than pinning the two tested
libraries and their required options for this bounded use case. No adapter is
needed for the observed `PING`, `SET`, `GET`, pipeline, and `MULTI` flows. If an
application requires unsupported startup commands or a new client version
treats `CLIENT SETINFO` errors as fatal, reassess this choice with a fresh wire
capture and a scoped adapter estimate instead of assuming compatibility.
