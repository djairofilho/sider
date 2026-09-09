# Runtime requirements

These general instructions apply to the `sider` server and the
`sider-aof-migrate`, `sider-backup`, and `sider-replica` CLIs. Rust and Cargo are
not required to run the packages. Redis and `redis-cli` are not bundled; a RESP2
client is needed only to interact with the server.

## Supported environments

Development rehearsals use the following native environments:

| System | Package architecture and target |
| --- | --- |
| Windows 11 x64 | `x86_64-pc-windows-msvc` |
| Ubuntu 24.04 x86_64 GNU | `x86_64-unknown-linux-gnu` |

These environments define the tested support without inferring a minimum Windows
or glibc version from executable headers. No support is claimed for earlier
Windows versions, other Linux distributions, musl, or ARM64.

## Windows

MSVC executables depend on `VCRUNTIME140.dll`, the Universal CRT, and Windows
APIs. The
[Microsoft Visual C++ v14 Redistributable x64](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)
must be compatible with the build toolchain, and Windows components must remain
up to date. Microsoft DLLs are not included in the Sider ZIP.

Recorded rehearsals ran on Windows with the runtime already installed. This does
not validate a clean system installation. The runtime version observed on one test
machine does not, by itself, establish the required minimum version.

## GNU Linux

The executables are dynamically linked. The environment must provide the GNU
loader for x86_64, glibc, and the other libraries imported by the build, including
`libgcc_s.so.1` and, when imported, `libm.so.6`. The package does not include
these system libraries and is neither static nor intended for musl.

The build and test environment is Ubuntu 24.04. The highest `GLIBC_*` symbol
version found during inspection does not demonstrate support for every
distribution with that glibc version; the loader, other libraries, and APIs also
participate in execution.

## Evidence for each candidate

This text is the versioned source of `runtime-requirements.md` in the asset set.
It neither identifies nor approves a build. The candidate manifest and evidence
must record the exact SHA, toolchain, system, and hashes of all four executables
actually extracted from the packages.

Before publishing, inspect the architecture and imported DLLs of all four Windows
executables with `objdump -f`/`objdump -p` or an equivalent tool. On Linux, check
architecture, loader, libraries, and symbol versions with `readelf -h -l -d -V`,
and resolve the binaries' dependencies with `ldd`.

Preserve output for each executable and the observed environment. Run `--version`
for the server and backup, `--help` for the migrator and replica CLI, and the
package smoke test on both platforms. Results from earlier builds do not replace
these checks. Promotion from candidate to final preserves the same assets,
requirements, and evidence without rebuilding the executables.
