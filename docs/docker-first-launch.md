# Docker-only launch after v1.0.1

The [v1.1.0 milestone](../ROADMAP.md) targets one public Linux amd64 image at
`ghcr.io/djairofilho/sider`. It includes the existing database, `MEM.*`, RESP3,
protected TLS/authentication/ACL, and the explicit composed transaction mode.
The image has **not** been published. Its new gates are pending, and neither this
page nor the development checkout approves a release. The [v1.0.1 release](https://github.com/djairofilho/sider/releases/tag/v1.0.1)
remains the published version; it uses an image archive and does not include
these post-1.0.1 features.

## Two durable profiles

The same image has two explicitly selected AOF profiles. Use a different volume
for each; do not point one profile at the other's data directory.

| Profile | Selection | Capabilities |
| --- | --- | --- |
| Memory | `SIDER_AOF_RECORD_VERSION=2` | Durable `MEM.*`, exact filtered search, backup, and same-profile replication. |
| Composed | `SIDER_AOF_RECORD_VERSION=1` and `SIDER_AOF_COMPOSED=1` | Mixed-owner `MULTI`/`EXEC`, backup, and same-profile replication. |

RESP2/RESP3 and protected client authentication are available in either
profile. The current format does not combine `MEM.*` and mixed-owner `EXEC` in
one instance. A profile mismatch must fail before readiness. Automatic failover,
cluster membership, online resharding, Lua, and an approximate memory index are
outside this release.

## Candidate container configuration

The [v1.1.0 Compose file](../deploy/compose.v1.1.yaml) selects one profile at a
time and gives each its own named volume. It binds the client port to host
loopback. The server still requires protected TLS credentials and ACL material:
`SIDER_SECURITY_DIR_HOST` must be an absolute directory with a complete,
restricted generation readable by UID 10001. Mounting it read-only does not
replace the [security validation contract](security-boundary.md). Missing or
invalid material fails startup; there is no automatic plaintext fallback.

On a Linux Docker host, after a candidate digest is approved, create a new
client-only generation with Docker, OpenSSL 3, and GNU coreutils. The example
provisions one named administrator with explicit grants for every current
command and all key/channel scopes. It reads the password without terminal
echo and sends its bytes to the exact Sider image on stdin. Run these commands
from the repository root in Bash. Use a fresh security directory under a
root-owned, non-writable parent:

```bash
export SIDER_IMAGE='ghcr.io/djairofilho/sider@sha256:<approved-digest>'
docker pull "$SIDER_IMAGE"
sudo install -d -m 0755 /srv/sider
test ! -e /srv/sider/security || { echo 'Choose a fresh security directory' >&2; exit 1; }
sudo install -d -o 10001 -g 10001 -m 0700 /srv/sider/security
read -r -s -p 'Sider admin password (16-1024 bytes): ' sider_password
printf '\n'
verifier=$(printf %s "$sider_password" |
  docker run --rm -i --entrypoint /usr/local/bin/sider \
    "$SIDER_IMAGE" --hash-password)
unset sider_password
start=$(date -u -d '-1 minute' +%Y-%m-%dT%H:%M:%SZ)
end=$(date -u -d '+90 days' +%Y-%m-%dT%H:%M:%SZ)
sudo openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -noenc -days 90 -subj /CN=sider \
  -addext 'subjectAltName=IP:127.0.0.1' \
  -addext 'extendedKeyUsage=serverAuth' \
  -keyout /srv/sider/security/client-key.pem \
  -out /srv/sider/security/client-cert.pem
sudo tee /srv/sider/security/client-policy.json >/dev/null <<EOF
{
  "version": 1,
  "users": [{
    "name": "admin",
    "credentials": [{
      "verifier": "$verifier",
      "not_before": "$start",
      "expires_at": "$end"
    }],
    "commands": [
      "GET", "MGET", "EXISTS", "TTL", "PTTL", "HGET", "HEXISTS", "HLEN",
      "HGETALL", "LLEN", "LRANGE", "SISMEMBER", "SCARD", "SMEMBERS",
      "ZCARD", "ZSCORE", "ZRANGE", "SET", "DEL", "MSET", "INCR", "DECR",
      "EXPIRE", "PEXPIRE", "PERSIST", "HSET", "HDEL", "LPUSH", "RPUSH",
      "LPOP", "RPOP", "SADD", "SREM", "ZADD", "ZREM", "PUBLISH",
      "SUBSCRIBE", "MULTI", "EXEC", "DISCARD", "UNWATCH", "WATCH",
      "INFO", "PING", "ECHO", "MEM.INFO", "MEM.GET", "MEM.PTTL",
      "MEM.SEARCH", "MEM.CREATE", "MEM.PUT", "MEM.DEL", "MEM.EXPIRE",
      "MEM.PERSIST"
    ],
    "read_keys": {"exact": [], "prefix": [], "all": true},
    "write_keys": {"exact": [], "prefix": [], "all": true},
    "publish_channels": {"exact": [], "prefix": [], "all": true},
    "subscribe_channels": {"exact": [], "prefix": [], "all": true},
    "info_sections": [
      "server", "clients", "stats", "memory", "persistence",
      "config", "replication"
    ]
  }]
}
EOF
sudo chown 10001:10001 /srv/sider/security/client-policy.json \
  /srv/sider/security/client-cert.pem /srv/sider/security/client-key.pem
sudo chmod 0600 /srv/sider/security/client-policy.json \
  /srv/sider/security/client-cert.pem /srv/sider/security/client-key.pem
policy_sha=$(sudo sha256sum /srv/sider/security/client-policy.json | cut -d ' ' -f 1)
cert_sha=$(sudo sha256sum /srv/sider/security/client-cert.pem | cut -d ' ' -f 1)
key_sha=$(sudo sha256sum /srv/sider/security/client-key.pem | cut -d ' ' -f 1)
sudo tee /srv/sider/security/security-manifest.json >/dev/null <<EOF
{"version":1,"generation":1,"files":{"client-policy.json":"$policy_sha","client-cert.pem":"$cert_sha","client-key.pem":"$key_sha"}}
EOF
sudo chown 10001:10001 /srv/sider/security/security-manifest.json
sudo chmod 0600 /srv/sider/security/security-manifest.json
sudo install -m 0644 /srv/sider/security/client-cert.pem \
  /srv/sider/client-cert.pem
unset verifier policy_sha cert_sha key_sha start end
export SIDER_SECURITY_DIR_HOST=/srv/sider/security
docker compose -f deploy/compose.v1.1.yaml --profile memory up -d
docker compose -f deploy/compose.v1.1.yaml --profile memory ps
```

Keep the public certificate as the client's trust anchor and renew the
credentials and certificate before expiry. This initial procedure does not
provision internal TLS identities for backup or replication. Use the
[manifest-last rotation contract](security-boundary.md) for replacements.

The client must use TLS, trust
`/srv/sider/client-cert.pem`, and authenticate as `admin` with the
password entered above. Plaintext clients receive no RESP response. Use a
separate security generation and internal identity for another host or replica.
For a local RESP2 check with a Redis CLI that supports TLS:

```sh
redis-cli --tls --cacert /srv/sider/client-cert.pem \
  -h 127.0.0.1 -p 6379 --user admin --askpass PING
```

## Provision internal TLS for backup and replication

For a deployment that needs the internal listener, use the Linux provisioning
script to create a **fresh** primary, replica, and backup/monitor generation.
It prompts for the administrator password, creates an internal CA and CRL,
issues URI SAN identities, and writes restricted manifests. Run it as root on
the Docker host; keep the generated `ca/ca-key.pem` off the containers and
renew the 30-day CRL before it expires. The resulting client policy grants a
single administrator broad access as a starting point; narrow it for shared
deployments. Use a separate output directory for each primary instance.

```bash
sudo env SIDER_IMAGE="$SIDER_IMAGE" bash deploy/provision-v1.1-security.sh \
  /srv/sider/memory-security
export SIDER_SECURITY_DIR_HOST=/srv/sider/memory-security/primary
docker compose -f deploy/compose.v1.1.yaml \
  -f deploy/compose.v1.1.internal.yaml --profile memory up -d
```

The second Compose file enables the internal mTLS listener and binds its host
port to loopback (`6381` for memory or `6382` for composed). For the composed
profile, provision another fresh directory, set its `/primary` directory as
`SIDER_SECURITY_DIR_HOST`, and use `--profile composed`. The security directory
must include all eight protected material files and its manifest. A client-only
generation from the previous section cannot start this listener.

Keep backups in a separate volume. For example, after starting the memory
profile, export a record-2 cut using the generated backup identity:

```bash
docker volume create sider-memory-backups
docker run --rm --network none --user 0:0 \
  --mount type=volume,source=sider-memory-backups,target=/backups \
  --entrypoint /usr/bin/chown "$SIDER_IMAGE" 10001:10001 /backups
source_sha=$(docker image inspect --format \
  '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$SIDER_IMAGE")
docker run --rm --network host --user 10001:10001 --read-only \
  --cap-drop ALL --security-opt no-new-privileges \
  --mount type=bind,source=/srv/sider/memory-security/backup,target=/run/sider/security,readonly \
  --mount type=volume,source=sider-memory-backups,target=/backups \
  --env SIDER_SECURITY_PROFILE=protected \
  --env SIDER_SECURITY_DIR=/run/sider/security \
  --entrypoint /usr/local/bin/sider-backup "$SIDER_IMAGE" \
  export --record-version 2 --source 127.0.0.1:6381 \
  --destination /backups/memory-001 --source-sha "$source_sha"
```

For composed, select `--profile composed`, connect to `127.0.0.1:6382`, and
use its own backup volume and identity. The [backup guide](backup.md) covers
verification and restore. The generated `/replica` directory supplies a
separate identity for an explicitly configured follower; the follower needs
its own data volume. Do not copy one primary's internal certificate to another
instance. Rotate complete generations as described in the
[security contract](security-boundary.md).

The composed profile uses its own data volume and host port 6380:

```sh
docker compose -f deploy/compose.v1.1.yaml --profile composed up -d
```

Stop the selected profile:

```sh
docker compose -f deploy/compose.v1.1.yaml --profile memory down
# Or: docker compose -f deploy/compose.v1.1.yaml --profile composed down
```

Keep the named data volume when stopping; do not pass `--volumes` unless
intentionally discarding its data.

Do not start both profiles against the same external client port or reuse their
security identity for separate replicas. Local Docker rehearsals have exercised
restricted internal TLS material, backup and restore, replica reconnect, and
role revocation in both applicable durable profiles. The public image gate and
clean-host pull remain work in
[issue #208](https://github.com/djairofilho/sider/issues/208). The local image
is not a release candidate.

## Release boundary

The [release epic](https://github.com/djairofilho/sider/issues/206) groups the
remaining security and composed work with Docker packaging, release gates, and
manual GHCR publication. A candidate is built from one frozen source SHA. The
public RC image must be pulled by OCI digest on a clean Linux Docker host and
pass every required gate, including a 3600-second soak and an isolated
benchmark. The final tag must point to that same digest without rebuilding.
The exact SHA, digest, commands, and results belong in the release evidence.

Only the image is a public runtime distribution target for v1.1.0. Linux build
and test executables can be used internally to prepare and validate it; native
Windows and Linux packages are not release assets. The historical v1.0.0 and
v1.0.1 package rules, hashes, and receipts remain unchanged. Keep verified
backups separate from the AOF volume; see the [backup guide](backup.md) for the
applicable profile and restore limitations.
