# Distribution image

The Linux amd64 image contains `sider`, `sider-aof-migrate`, `sider-backup`,
and `sider-replica` from the validated Linux package.
The [Dockerfile](../deploy/Dockerfile) copies the binaries; it does not run Cargo
or install packages. The Ubuntu 24.04 base is pinned by digest and compatible
with the environment used to build the GNU package.

The process runs as UID/GID `10001:10001`, is PID 1, and receives `SIGTERM` directly.
The `/var/lib/sider` directory belongs to that user and is the AOF volume.
The image defaults to address `0.0.0.0:6379`, one shard, and `always` synchronization.
The persisted shard count must match on subsequent starts.

## Input and identity

Follow the [package guide](packages.md) and preserve evidence of the clean SHA,
build, hashes, packaging, extraction, and smoke test. The image receives the
**extracted** directory with all four executables, `README.md`, `LICENSE`, and
the `licenses/` tree.

OCI labels record the version and source SHA; `io.sider.binary.sha256`,
`io.sider.migrator.sha256`, `io.sider.backup.sha256`, and `io.sider.replica.sha256`
record executable hashes. The build checks hashes, versions, and CLI help.
The SHA is a packager declaration supported by the build procedure; the runner
does not infer the commit from executable bytes without that information embedded.

Product notices are in `/usr/share/doc/sider`. The base retains its own package
notices in `/usr/share/doc`. Reassess the base, libraries, and notices before
publication when these components change.

## Producing and testing the archive

The Rust runner requires Linux GNU x86_64, a Linux Docker daemon, the `docker`
CLI, `gzip`, and `sha256sum`. It does not require Buildx. The daemon may need to
fetch the pinned public base on the first build; build instructions do not use
the network. The context contains only the package and Dockerfile, without the
checkout or credentials.

```sh
export SIDER_DOCKER_PACKAGE_DIR=/data/extracted/sider-v1.0.0-x86_64-unknown-linux-gnu
export SIDER_DOCKER_OUTPUT_DIR=/data/new-docker-rehearsal
export SIDER_DOCKER_SOURCE_SHA=SHA_COMPLETO_DO_BUILD
export SIDER_DOCKER_BINARY_SHA256=SHA256_REGISTRADO_DO_SIDER
export SIDER_DOCKER_MIGRATOR_SHA256=SHA256_REGISTRADO_DO_MIGRADOR
export SIDER_DOCKER_BACKUP_SHA256=SHA256_REGISTRADO_DO_BACKUP
export SIDER_DOCKER_REPLICA_SHA256=SHA256_REGISTRADO_DA_CLI_REPLICA
cargo test --locked --test docker_distribution -- --ignored --exact exported_image_runs_after_load --nocapture
```

Output must be a new directory with an existing parent. The runner checks binaries,
rejects links, and builds the image. It runs `docker save`, compresses with
`gzip -n -6`, removes its temporary tag, and loads the `.tar.gz`. It compares
Image ID, platform, user, signal, labels, and hashes inside the reloaded image.

The eight scenarios check export/reload, hashes, versions/CLIs, UID/PID 1,
binary TCP, AOF/TTL on a volume reused by another container, SIGTERM, and invalid
configuration. The rootfs is read-only and the container receives no additional
capabilities. The test confirms normal termination and removes only its own
containers, volume, and tag. The exported image, context, and logs remain in the output.

`docker-report.json` contains Image ID, tag, hashes, size, and scenarios;
`commands.json` preserves arguments, outputs, duration, and process exit codes.
Failure or timeout produces no success report. Opt-in entry points ignored by
the regular suite do not count as evidence of this test.

On a Linux host, the test publishes an ephemeral port only on `127.0.0.1`.
If the runner is itself a container, set `SIDER_DOCKER_RUNNER_ID` to its full ID.
The test requires an active Linux runner, a private network, and no published ports.
It uses `--network container:ID`, address `127.0.0.1:0`, and a readiness file
with PID 1 to discover the port. This mode records the private topology;
it does not establish Docker Desktop host port forwarding.

A containerized runner needs the Docker CLI and socket. The socket grants control
of the test daemon and must not be mounted in the product image. The CLI sends
the context; its internal paths do not need to exist on the daemon host.

## 1.0 manifest

The schema 2 verifier already requires `sider-v1.0.0-linux-amd64-image.tar.gz`.
Copy only that file to the assets root. Include the report's `artifact` object
in `release-manifest.json.artifacts`, with name, size, and SHA-256, and the
corresponding line in `SHA256SUMS`. The report and logs go in the evidence ZIP
and `evidence_files`. Context and staging remain outside the assets root.

For the gate, use `release_docker_gate` with the context from [releases.md](releases.md).
It requires the frozen build SHA and version, runs the same test, and publishes
the receipt after all eight cases. The Cargo package must be version `1.0.0`.
RC and final release use the same exported archive, without rebuilding, changing
the internal tag, recompressing, or running another `docker save` during promotion.
The procedure is reproducible, but does not promise identical bytes across
independent daemon builds. There is no image push, public registry, or automatic CI.

## Running a received archive

Check `SHA256SUMS`, load the archive, and use the tag recorded in the report:
the Compose command uses the checkout example, which can also be copied on its
own to `deploy/compose.yaml`.

```sh
docker load --input sider-v1.0.0-linux-amd64-image.tar.gz
export SIDER_IMAGE=TAG_LOCAL_EXATA_DO_RELATORIO
docker image inspect "$SIDER_IMAGE"
docker run --rm --network none "$SIDER_IMAGE" --version
docker compose -f deploy/compose.yaml up -d
docker compose -f deploy/compose.yaml logs sider
docker compose -f deploy/compose.yaml stop
```

The [Compose example](../deploy/compose.yaml) never pulls from a registry,
publishes only on loopback, and retains the `sider-data` volume. It allows
adjusting port, quota, shards, and AOF policy. On an initialized volume, changing
shards requires [offline migration](aof-migration.md). Bind mounts need write
access for UID/GID 10001; the process does not elevate privileges to change directories.

`stop_grace_period: 10s` allows the normal drain configured for five seconds.
Blocking I/O may exceed the server deadline; when the Docker deadline expires,
the daemon may send `SIGKILL`, which does not establish successful draining.
Preserve the volume when recreating the service. Backup and restoration have
their own procedures; copying an active AOF without a consistent point is not a validated backup.

## Recorded internal test

Clean checkout `676f4f978df00383794104f8b3f3f66441362cbf`, still at Cargo version
`0.1.0`, passed all eight scenarios in 57.76 seconds on Linux Ubuntu 24.04 in
`sider-dev:tests`, with Docker Desktop and the runner's private namespace.
The extracted Linux package smoke test also passed. No release receipt was issued.

The archive is 30,701,303 bytes with SHA-256
`fa9467a983ad6da27bda783fb48e4d2291af5a8f60e25b4d9ebc02b405515017`.
The package, image, context, and logs are preserved in
`target/baselines/r10-docker-676f4f978df00383794104f8b3f3f66441362cbf/`.
Earlier attempts that identified missing Buildx and a namespace difference remain
separate and do not count as approval.
