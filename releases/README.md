# Running Sider

This package contains the Sider server, the `sider-backup`, `sider-aof-migrate`,
and `sider-replica` CLIs, this README, the MIT license for project code, and
third-party notices in the `licenses/` directory with their hash inventory.
Check the version with `sider --version`. The release also provides `SHA256SUMS`,
`release-manifest.json`, notes, and validation evidence.

The Windows 11 x64 package requires `VCRUNTIME140.dll`, Universal CRT, and the
[Visual C++ v14 Redistributable x64](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)
compatible with the build; Microsoft DLLs are not included in the ZIP. The Linux
GNU package is dynamically linked, tested on Ubuntu 24.04, and depends on system
libraries. See the `runtime-requirements.md` asset and evidence for the release's
four executables. Windows tests use an installed runtime; they do not claim testing
on a clean installation.

## Starting

Linux x86_64 GNU, built and tested on Ubuntu 24.04:

```sh
./sider --version
./sider
```

Windows x86_64 MSVC, in PowerShell:

```powershell
.\sider.exe --version
.\sider.exe
```

The process stays in the foreground and listens on `127.0.0.1:6379` by default.
Use `Ctrl+C` to stop. To choose another port on Linux:

```sh
SIDER_ADDR=127.0.0.1:6380 ./sider
```

In PowerShell, set `$env:SIDER_ADDR = '127.0.0.1:6380'` before starting.
The address requires a literal IP, not a hostname. `--help` shows the binary's interface.

## Trying it

In another terminal, with `redis-cli` installed separately:

```sh
redis-cli -2 -h 127.0.0.1 -p 6379 PING
redis-cli -2 -h 127.0.0.1 -p 6379 SET example value
redis-cli -2 -h 127.0.0.1 -p 6379 GET example
redis-cli -2 -h 127.0.0.1 -p 6379 DEL example
```

The core supports `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`, `INCR`, `DECR`,
`MGET`, `MSET`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, and `PERSIST`. SET accepts
NX, XX, EX, PX, GET, and KEEPTTL. Requests use RESP2 arrays of bulk strings.
Hashes, lists, sets, sorted sets, and Pub/Sub are also supported.
Transactions provide `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` within one
shard. Individual errors during EXEC do not roll back the other commands.
With more than one shard, multikey commands and transactions must use keys in the
same shard; a shared hash tag, such as `{account}:balance` and `{account}:limit`, keeps
those keys together. Cross-shard operations return an error before execution.
Keys and values preserve arbitrary bytes. There is no inline mode or RESP3.
Clients that send additional initialization commands may be incompatible.

## Persistence and diagnostics

AOF is optional. To start a persistent primary with four shards and an internal
backup/replication listener on Linux:

```sh
SIDER_ADDR=127.0.0.1:6379 SIDER_SHARDS=4 SIDER_AOF_DIR=./primary-aof \
SIDER_AOF_SYNC=always SIDER_REPLICATION_ADDR=127.0.0.1:6381 ./sider
```

In PowerShell, set each variable with `$env:NAME = 'value'` and run `.\sider.exe`.
Each instance needs its own AOF directory. Recovery finishes before readiness;
corruption or an incompatible shard layout prevents startup. `always` waits for
synchronization per batch. `everysec` allows loss of writes not yet synchronized
if the process or machine fails.

`sider --diagnose` checks configuration without opening listeners or starting recovery.
While the server runs, `redis-cli -2 INFO` queries instance metrics.
Logical dataset quota and RSS memory are different measurements.

## Backup and administration

The operational CLIs are included in the package and require neither Cargo nor
a checkout. See `./sider-backup --help`, `./sider-aof-migrate --help`, and
`./sider-replica --help`. On Windows, use `.\` and append `.exe` to the executable name.

The primary needs `SIDER_AOF_DIR` and the internal listener at
`SIDER_REPLICATION_ADDR`, separate from the RESP address. Keep it on loopback
or a controlled private network. With the listener at `127.0.0.1:6381`:

```sh
./sider-replica --addr 127.0.0.1:6381 --status
./sider-backup export --source 127.0.0.1:6381 --destination new-backup --source-sha FULL_SHA
./sider-backup verify --source new-backup --shards 4 --routing 1
./sider-backup restore --source new-backup --destination new-data --shards 4 --routing 1
```

Supply the source manifest's 40-digit SHA and the actual shard count.
Backup and restore reject an existing destination; restore checks checksums, format,
layout, and quota. TTL retains absolute expiration, consuming elapsed time.
Start the restored data in an isolated instance with the same configuration and
check the result before using it.

To start the restored directory from the example, use `SIDER_AOF_DIR=./new-data`
and `SIDER_SHARDS=4`. For a source with one shard, use `--shards 1` in the
verify/restore commands and `SIDER_SHARDS=1` on the restored instance.

Offline migration explicitly changes the layout in another directory. Stop the source
instance before running it, for example:

```sh
./sider-aof-migrate --source original-data --source-shards 1 --source-routing 1 \
  --destination four-shard-data --shards 4 --routing 1
```

The destination must be new. The tool checks the format and per-shard quota and
preserves source files; changing only `SIDER_SHARDS` does not migrate data.

## Replica and promotion

On the same host as the primary above, start a replica with a separate directory:

```sh
SIDER_ADDR=127.0.0.1:6380 SIDER_SHARDS=4 SIDER_AOF_DIR=./replica-aof \
SIDER_REPLICATION_ADDR=127.0.0.1:6382 SIDER_REPLICA_OF=127.0.0.1:6381 ./sider
```

In another terminal, check status:

```sh
./sider-replica --addr 127.0.0.1:6382 --status
```

Replication requires the same version and layout; the replica rejects client writes.
Pub/Sub remains local to each instance.
Keep clocks synchronized because TTL uses absolute deadlines.

`sider-replica --addr IP:PORT --promote` requires loopback and explicitly promotes
the replica after stopping the upstream session. Replication is asynchronous:
writes acknowledged by the primary but not yet applied on the replica may be lost.
For a planned switch, suspend writes on the old primary, wait for positions to
converge, promote, and redirect clients. The old primary is not demoted automatically.
If promotion times out, query status before retrying.
There is no election or automatic failover.

## Limits and security

- Use only in a controlled environment. There is no authentication, ACL, or TLS.
- Persistence is optional through `SIDER_AOF_DIR`; without AOF, data is lost on exit.
  AOF and replication do not replace a preserved and verified backup.
- TTL has passive expiration and bounded active cleanup. The default logical quota
  is 64 MiB (`SIDER_MAX_DATASET_BYTES`); excess growth is rejected without eviction.
  Accounting includes the key, value, and a fixed 128-byte charge; it does not measure RSS.
- Defaults allow 32 connections and 32 commands in the worker queue, payloads up to
  1 MiB, and frames/input buffers up to 4 MiB. This does not limit total memory.
- Default deadlines are 10 seconds to form a frame, 5 seconds for queueing and
  response, 5 seconds for writing, and 5 seconds for draining during shutdown.
  Idle connections without a partial frame have no idle timeout.
- Once a command enters the queue, losing the connection or response does not undo
  its execution. After a timeout, the outcome may be unknown to the client.
- Shutdown is best-effort; draining is not guaranteed after forced termination.

## Version documentation

The [private repository](https://github.com/djairofilho/sider) contains configuration,
networking, compatibility, and testing guides in `docs/`. Use the manifest SHA or
publication tag to consult the same code as the package. See `docs/replication.md`,
`docs/backup.md`, `docs/persistence.md`, `docs/metrics.md`, and
`docs/compatibility-matrix.md` at that revision. The complete guides are not included
in this archive.

The package does not include Redis or redis-cli. Sider dependencies retain their
own licenses. The MIT license does not change the artifacts' private visibility.
