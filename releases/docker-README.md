# Sider 1.1.0 Linux Docker image

This image is the public runtime distribution of Sider 1.1.0 for Linux amd64.
Native Windows and Linux packages are not 1.1.0 release assets.
It contains `sider`, `sider-aof-migrate`, `sider-backup`, and `sider-replica`.
Their source revision and SHA-256 hashes are recorded in OCI image labels.
The MIT license and third-party notices are in `/usr/share/doc/sider/`.

The server starts in protected mode. Supply a restricted security generation
at `/run/sider/security`; missing or invalid material prevents startup. Run
the container as UID/GID 10001 with a read-only root filesystem and a writable
volume at `/var/lib/sider`. Publish its TLS client port only where intended.

Choose one durable profile and keep its volume separate:

| Profile | Environment | Purpose |
| --- | --- | --- |
| Memory | `SIDER_AOF_RECORD_VERSION=2` | Durable `MEM.*` and exact search |
| Composed | `SIDER_AOF_RECORD_VERSION=1`, `SIDER_AOF_COMPOSED=1` | Mixed-owner `MULTI`/`EXEC` |

The profiles cannot share an AOF volume. See the repository's
`docs/docker-first-launch.md`, `docs/security-boundary.md`, and
`docs/backup.md` at the OCI source revision for provisioning, Compose,
restart, backup, and restore instructions. Verify the public image by its OCI
digest. The final tag must resolve to the approved candidate digest.
