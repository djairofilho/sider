# Local packages and extracted-executable smoke test

Before publishing the 1.0 candidate, build and test the exact SHA of the
preparation merge on the native system. Use new directories for staging, archive,
and extraction. The RC and final use the same packages, already identified as
`1.0.0`. Promotion verifies the same bytes; it does not rebuild or repackage.

The [runtime requirements](runtime-requirements.md) describe Windows 11 x64,
Ubuntu 24.04 GNU, and external dependencies. The text is the versioned source
of the `runtime-requirements.md` asset; candidate evidence must verify all four
extracted executables in each platform's environment.

## Contents

Each archive contains a `sider-vVERSAO-TARGET/` directory with:

- `sider` on GNU Linux or `sider.exe` on Windows MSVC;
- `sider-aof-migrate` on Linux or `sider-aof-migrate.exe` on Windows, for
  [explicit offline migration](aof-migration.md);
- `sider-backup` on Linux or `sider-backup.exe` on Windows, to
  [export, verify, and restore backups](backup.md);
- `sider-replica` on Linux or `sider-replica.exe` on Windows, to query status
  and perform manual promotion through the internal listener;
- `README.md`, copied from [releases/README.md](../releases/README.md);
- `LICENSE`, copied in full from the checkout root;
- `licenses/`, a full copy of [releases/licenses/](../releases/licenses/),
  with upstream notices and a file/hash inventory.

Use `.tar.gz` for `x86_64-unknown-linux-gnu` and `.zip` for
`x86_64-pc-windows-msvc`. The Linux build runs on Ubuntu 24.04. The distribution
README explains how to run the binary without requiring Cargo or the checkout.

Before packaging, verify the hashes and sizes of every file in the notice
inventory. After extraction, also compare the complete `licenses/` tree against
staging and the checkout. Reassess the collection when production dependencies
or the toolchain change; it is not a statement about future dependency licenses.
Build all four with `cargo build --locked --release --bins`. Preserve the four
Linux executables as `0755` and documents as `0644`.

## Manual smoke test

The Rust test `extracted_package_runs_version_and_tcp` receives
`SIDER_PACKAGE_DIR` with the absolute path of the directory actually extracted.
It does not extract the archive, choose a build binary, or approve a release on
its own. The operator must demonstrate extraction, SHA, target, and hashes in
publication evidence. The smoke test requires all four executables, the README,
and license; it also runs `--help` for the migrator and replication CLI, plus
`--version` for the backup CLI. Notices are verified separately through the
inventory and comparison of every extracted file.

```sh
SIDER_PACKAGE_DIR=/path/extracted/sider-vTARGET-VERSION cargo test --locked --test package -- --ignored --exact extracted_package_runs_version_and_tcp --nocapture
```

In PowerShell, set `SIDER_PACKAGE_DIR` to the extracted directory before running
the same Cargo command. Preserve and restore its previous value when the
validation session ends. The test reads the environment without changing it.

The smoke test requires regular files, the correct README and license, the exact
compiled version, a live owned process, and readiness with verified PID/port. It
runs literal TCP with binary data, pipelining, a read after deletion, and EOF
without a residual response. It confirms that the child is collected before
printing its JSON result. Missing configuration, an invalid file, a timeout, or
a divergent response fails the test.

The default cycle tests harness validation but explicitly ignores external input.
Testing a copy of the development binary does not count as a smoke test of the
extracted package. Run the opt-in entry on both systems for every candidate build.
The final reuses this evidence only for the same SHA and files.

## Evidence and publication

Record the build, packaging, extraction, and smoke commands; their output and
exit code; file and package hashes; system, compiler, and checkout SHA. Include
this evidence in the local manifest and attach the logs to the private release.

The [release procedure](releases.md) adds the cumulative gates, candidate,
verification downloads, and milestone closure only after the final is published.

The [Docker image](docker.md) copies these same Linux executables, verifies the
hashes, and tests the exported archive after `docker load`. It does not rebuild
the product during the image build or alter the schema 2 manifest format.

## Recorded initial rehearsal

On September 8, 2026, at commit `9768b1f`, the rehearsal created a Windows ZIP
and a Linux tar.gz, extracted each into another directory, and passed the opt-in
smoke test with one approved test and none ignored per system. The hashes of the
extracted executables matched the original builds; README and LICENSE were
verified. These rehearsals still contained only the three main files, before the
addition of `licenses/`, and are not release gates. The artifacts were preserved
locally in `target/package-rehearsal-9768b1f/` and
`target/package-rehearsal-linux-9768b1f/`.

The expanded native suite passed with 241 tests on Windows and 243 on Linux, plus
one doctest on each system. Seven opt-in entries remain explicitly ignored in the
normal run. Formatting, check, Clippy, documentation, and build also passed. An
earlier Windows run failed to open a listener with error `10055`. System/Tcpip
event `4231`, at local time 01:40:49, recorded exhaustion of the global ephemeral
port pool; it does not identify the responsible process. After observed resource
recovery, the isolated case, all 11 TCP tests, and the complete suite passed
without code changes or scenario suppression. The failed attempt was not used as
release approval.
