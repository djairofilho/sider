# Lua containment feasibility spike

Status: **partial feasibility evidence, not a production sandbox**. Related task:
[Sider #95](https://github.com/djairofilho/sider/issues/95). Baseline `0f0f232`,
uncommitted isolated worktree, execution date October 10, 2026. The
[proposed scripting contract](../../docs/lua-scripting.md) is unchanged.
The current strict Windows memory gate fails; delegated Linux resource controls
pass under WSL2. Neither result enables production scripting.

This standalone Rust package has its own workspace and locked dependencies. It
does not alter Sider, its Cargo files, command parser, Store, AOF, replication,
configuration or packages. `#![forbid(unsafe_code)]` applies to the spike; native
Lua and dependency internals still require security review. Direct dependencies
are mlua `=0.12.2` and, on Windows only, processkit `=3.3.4` and Tokio `=1.53.2`.
The initial Windows experiment used win32job `=2.0.3`; its evidence is preserved.
The lockfile resolves
mlua-sys 0.13.0 and lua-src 551.0.2; the selected `lua54` source is **Lua 5.4.9**.
The presence of luajit-src in the dependency graph does not select LuaJIT.

## Reproduce

Use one build/test slot, with no measurement competing for machine resources:

```powershell
cargo build --locked --manifest-path spikes/lua-containment/Cargo.toml
cargo clippy --locked --manifest-path spikes/lua-containment/Cargo.toml --all-targets -- -D warnings
spikes/lua-containment/target/debug/sider-lua-containment-spike.exe
spikes/lua-containment/target/debug/windows-job-gate.exe
```

Both Windows runtime commands currently return exit 1 for the unresolved strict
memory gate. Keep that failure as evidence; build and Clippy success do not clear it.

For Ubuntu WSL2, from the worktree mounted under `/mnt/c`:

```sh
CARGO_TARGET_DIR=/var/tmp/sider-lua-containment-delegated-20261010 \
  cargo build --locked --manifest-path spikes/lua-containment/Cargo.toml
sh spikes/lua-containment/run-delegated-service.sh \
  /var/tmp/sider-lua-containment-delegated-20261010/debug/sider-lua-containment-spike
sh spikes/lua-containment/run-linux-negative.sh \
  /var/tmp/sider-lua-containment-delegated-20261010/debug/sider-lua-containment-spike
```

Run the service launcher and negative-test launcher with administrator permission
to create a transient service and select its user. The service payload and Rust
supervisor run as `nobody` (uid/gid 65534), with a private systemd delegation.
`run-delegated.sh` moves only its own process into a supervisor leaf and enables
controllers only in that service's delegated subtree. The service is collected
after exit; the launcher checks that its subtree was removed, including on failure.
The layout follows the Linux kernel's
[cgroup v2 delegation and internal-process rules](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html).

The Linux executable accepts `--cgroup-root PATH`, validates the required enabled
controllers, and creates only private `PATH/sider-lua-spike-PID` directories.
It writes that directory's controls,
places its own helper PID there, and removes the directory after wait/reap. It
does not enable controllers itself or change other groups. A setup
failure kills/reaps the helper with its source gate still closed. Do not run this
spike as a production server. No-argument Linux execution retains the historical
`/sys/fs/cgroup` root path; ordinary users are denied there. Deployment still needs
an operator-provided delegation. This does not demonstrate that a compromised
native helper sharing the supervisor's uid cannot write its delegated controls.

`main` with no arguments supervises the fixed fixture matrix. Child mode with a
fixture argument emits READY and blocks on stdin until receiving GO. The parent
installs the OS boundary before releasing GO. User source is not accepted by this
binary. Pipes carry fixed diagnostic lines, not production command IPC. Child
stderr is suppressed; compiler error details are emitted only for the fixed
compiler fixture. Empty host environment is applied to each child.

## Initial recorded results

| Platform | Final matrix | Build/check | Resource boundary |
| --- | --- | --- | --- |
| Windows 11 Pro x64, build 26300 | 14 pass, 3 explicit skips | Locked build and Clippy `-D warnings` passed | Safe Job assignment and kill-on-close demonstrated; OS memory/CPU/process limits remain unproved |
| Ubuntu 24.04.4 x64 on WSL2 kernel 6.6.87.2 | 17 pass, no skips | Locked build passed | Private cgroup memory/CPU accounting/process controls and cgroup.kill demonstrated as root |

Both use rustc 1.97.1 (`8bab26f4f`, July 14, 2026) and Cargo 1.97.1
(`c980f4866`, June 30, 2026). Linux evidence is **WSL2, not native Ubuntu**.
Platform/toolchain records are [Windows](evidence/windows-platform.txt) and
[Linux](evidence/linux-platform.txt). Build output is
[Windows](evidence/windows-build.txt),
[initial Linux build](evidence/linux-build.txt), and
[final Linux build](evidence/linux-build-final.txt).

The raw final matrices are [Windows](evidence/windows-runtime.txt) and
[Linux](evidence/linux-runtime.txt). Times are exploratory single observations
from an unoptimized development build, including child launch, pipes, polling,
containment setup, termination and reaping. They are not a benchmark or a latency
promise. The parent polls every 5 ms, with ordinary scheduler delay.

### Runtime cases

- Restricted environment omits module loading, filesystem/process/environment
  libraries, debug, coroutine, metatable manipulation and catchable errors.
  STRING is restricted both in the exposed table and in the original library
  table used by the string type's metatable. Assertions verify that method syntax
  cannot recover dump/rep/gsub. These selected assertions do not prove every escape
  path or OS access denial against a compromised native runtime.
- Arbitrary bytes `[00, ff, 0d, 0a]` round-trip unchanged through ARGV and a Lua
  string result. Text mode rejects a bytecode-signature fixture. Syntax failure is
  returned without executing a chunk.
- An infinite loop stops at the count-hook instruction threshold of 1,000,000.
  Non-tail recursion fails at the proposed 64-frame threshold. One thousand tail
  replacements finish with measured maximum depth one and 1,001 tail events: Lua
  5.4 tail calls replace frames rather than growing the active stack. Tail recursion
  is still subject to the independent instruction budget.
  This spike counts all call/return events, including native calls; exact Lua-only
  frame accounting and balancing through error exits need review before integration.
- Repeated concatenation exhausts the 8 MiB Lua allocator ceiling and returns a
  controlled memory error. VM setup starts with approximately 14-16 KiB in these
  small fixtures. This is Lua allocator accounting, not helper or parent RSS.
- Two new processes independently verify a fresh script environment. No persistent
  VM/cache or host Store exists in this spike.
- A 60,008-byte deeply parenthesized source returns a native compiler C-stack error
  without crashing the helper. This is one compiler-stress shape, not proof of all
  compilation bounds. Instruction hooks do not make compilation interruptible.
- The cooperative cancellation fixture throws from a count hook after a 10 ms
  monotonic trigger. It does not exercise Sider client disconnects or commit races.
- A fixture-only native callback sleeps for five seconds. Its Lua count hook cannot
  preempt that work; the parent kills/reaps it at the 500 ms wall trigger. Final
  observed end-to-end time was 505,554 microseconds on Windows and 501,268 on WSL2.
  This callback is installed only for this fixture, not the restricted cases.

### OS cases and observed failure attempts

Linux uses `memory.max=67108864`, `memory.swap.max=0` and `pids.max=1`, monitors
`cpu.stat usage_usec`, and terminates through `cgroup.kill`. A busy native loop
was killed at observed cumulative CPU 112,500 microseconds for a 100,000-microsecond
trigger. The 12,500-microsecond overshoot demonstrates why the proposed contract
does not claim exact CPU timing. `pids.max=1` rejected an attempted second child.
Growing Rust allocations outside the Lua allocator caused kernel OOM termination,
with `memory.events` showing `oom=1` and `oom_kill=1`. An independent cancellation
case also ended in SIGKILL. Every successful Linux fixture is waited/reaped before
its private cgroup is removed; a final inventory found no private spike groups or
live helper processes. These are OS-accounted controls, not a total Sider RSS cap.

Linux ran as uid/gid 0. The root already exposed all relevant controllers and
enabled them in `cgroup.subtree_control`. A read-only permission check as `nobody`
found the cgroup root unwritable. Ordinary non-root or container deployment,
delegation setup, native Ubuntu, and lost-controller permission paths remain
required feasibility work. Pre-GO bootstrap runs before cgroup migration, so this
spike also does not prove that every bootstrap memory page is charged to the child
group or that hostile native initialization is contained from its first instruction.

Preserve the failed attempts:

- [Initial Windows matrix](evidence/windows-initial.txt) failed the compiler fixture
  expectation: the observed error was RuntimeError with `C stack overflow`, rather
  than SyntaxError. The final fixture correctly expects the observed runtime error;
  no compiler resource limit was relaxed.
- [Initial Linux matrix](evidence/linux-initial.txt) reported the memory test as
  CPU_KILLED while its cgroup already recorded `oom_kill=1`. OOM reclaim consumed
  CPU and raced the observer. The final **memory fixture only** disables the CPU
  watchdog to isolate proof of the memory controller; its wall watchdog and memory
  ceiling remain enabled. This is not a proposed production policy. Attribution
  of overlapping OOM/CPU events needs an explicit supervisor policy before integration.

Windows Job close terminated both the independent cancellation case and the native
callback, then wait/reap completed. Windows reported exit code zero for these forced
terminations; the supervisor's observed cancellation/kill reason is therefore
essential, and exit status alone cannot distinguish success. The selected safe
wrapper does not expose committed-memory/process limits or user/kernel CPU
accounting; the corresponding tests are skipped, never counted as passes.

## Follow-up resource gates on October 10, 2026

The Linux delegated matrix has **19 passes, zero skips**, with locked build and
Clippy `--all-targets -- -D warnings` passing. The raw
[delegated runtime](evidence/linux-mount-runtime.txt) records uid/gid 65534,
systemd 255, and enabled `cpu memory pids` controls under one private service.
Native CPU work was killed at 113,508 microseconds; repeated native filesystem
metadata queries were killed at 106,813 microseconds. The memory fixture produced
`oom=1`, `oom_kill=1`, and SIGKILL with the same 64 MiB limit. Process creation was
denied. All helpers were reaped, no helper cgroups remained, and systemd removed
the transient service subtree. This is still **WSL2, not native Ubuntu**.

[Negative tests](evidence/linux-mount-negative.txt) independently exercised a
non-delegated root permission failure, a real cgroup lacking enabled controllers,
an ordinary directory containing fake control files, and a symlink resolving
outside the mount. Every case requires the specific diagnostic and exit 1.
Permission failure kills/reaps helpers without GO; the other three failures
precede helper creation. The final executable inventory was empty. The Rust
supervisor also now treats missing/malformed CPU accounting as an error instead
of silently disabling its CPU watchdog. Full live-controller revocation fault
injection remains pending.

The delegated-root guard canonicalizes the path and reads
[`/proc/self/mountinfo`](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html).
It requires the expected `/sys/fs/cgroup` cgroup2 mount and rejects nested or
stacked mounts, including mount records listed in either order. Ordinary files
named after controllers cannot establish containment. Root validation precedes
helper creation and is repeated before assignment. This check does not eliminate
mount/controller races against a privileged actor; production isolation and
startup auditing remain pending.
The [three parser tests](evidence/linux-mount-tests.txt) passed. Earlier delegated
and negative records remain available as historical observations. The first new
[launcher attempt](evidence/linux-mount-launcher-path-failure.txt) failed with
exit 127 because argument quoting reduced the binary path to `/debug/...`.
It never ran a helper and removed its private service subtree. The successful
rerun passed an absolute executable path.

The Windows follow-up uses the published safe
[processkit 3.3.4 API](https://docs.rs/processkit/latest/processkit/struct.ProcessGroup.html),
with a 64 MiB Job committed-memory limit and active-process limit one.
[`ProcessGroupStats`](https://docs.rs/processkit/latest/processkit/struct.ProcessGroupStats.html)
exposes cumulative user-plus-kernel CPU and Job peak committed memory. The
[reviewed dependency source excerpts](evidence/windows-processkit-api-source.txt)
confirm `JOB_OBJECT_LIMIT_JOB_MEMORY`, `JobMemoryLimit=67108864`,
`JOB_OBJECT_LIMIT_ACTIVE_PROCESS`, and the sum of `TotalUserTime` and
`TotalKernelTime`. No CPU-rate or user-only time limit substitutes for the
cumulative 100 ms trigger. Project code retains `#![forbid(unsafe_code)]`.

The main runner adopts only its newly spawned helper after READY and verifies Job
membership/accounting before GO. The separate `windows-job-gate` executable uses
`ProcessGroup::spawn` with piped Tokio stdin/stdout: the dependency creates the
helper suspended, assigns the already configured Job, and resumes it. It verifies
membership before GO and reads cumulative accounting after wait/reap. These are
fixed fixture pipes, not production IPC.

**The Windows strict memory gate fails.** The final suspended-spawn run has
four passes and one failure: a reservation was denied after 63 MiB, but
`PeakJobMemoryUsed=68173824`, 1,064,960 bytes above `JobMemoryLimit=67108864`.
Every reaped helper reported zero active processes. The busy-loop and native
metadata-loop CPU totals were 109,375 and 125,000 microseconds after reap; process
creation was denied. An allocation refusal alone does not prove the unchanged
64 MiB ceiling. Both runners explicitly fail when the reported peak exceeds it.
The discrepancy is unresolved; it is not explained solely by late assignment,
because it also occurred with suspended startup. See
[suspended runtime](evidence/windows-suspended-runtime.txt) and
[main strict matrix](evidence/windows-processkit-strict.txt). The main matrix has
18 passes and one failure, with peak 67,117,056 bytes (8,192 bytes above its cap).
Both commands return exit 1. Neither failure is counted as a pass or a skip.

Preserved follow-up failure evidence:

- [First processkit matrix](evidence/windows-processkit-initial.txt) reported
  peak 67,125,248 bytes; the
  [second matrix before the strict assertion](evidence/windows-processkit-runtime.txt)
  reported 68,136,960 bytes. Their functional PASS lines do not approve memory.
- [Initial suspended gate](evidence/windows-suspended-initial.txt) also failed,
  with peak 67,129,344 bytes. The
  [intermediate strict main matrix](evidence/windows-processkit-strict-intermediate.txt)
  failed with peak 67,141,632 bytes. Faster initialized-byte copies in the final
  fixture did not clear the strict memory failure.
- The first locked suspended build needed a lockfile update:
  [build failure](evidence/windows-suspended-build.txt),
  [Clippy failure](evidence/windows-suspended-clippy.txt), and
  [stale-binary run](evidence/windows-stale-runtime-after-locked-build-failure.txt).
  The stale run is invalid validation evidence. The dependency update was then
  made offline, followed by successful locked build and Clippy.
- [Initial delegated memory test](evidence/linux-delegated-initial.txt) hit its
  wall watchdog before reaching the memory ceiling. The development build's
  per-byte `Vec::resize` was replaced by initialized 4 KiB slice copies; no OS,
  CPU, memory, or wall ceiling was raised.
- [Initial negative test](evidence/linux-negative-initial-exec-failure.txt) used
  a missing executable after the `/tmp` build disappeared between WSL sessions.
  Its printed PASS lines are invalid. The corrected launcher requires an existing
  executable and the exact containment failure, uses `/var/tmp`, and was run in
  the same session as the build and delegated matrix.
- [Linux Clippy failure](evidence/linux-delegated-clippy-initial-failure.txt)
  caught an unnecessary literal unwrap in a diagnostic field. The corrected
  code emits `windows_peak_within_limit=None` on Linux, without claiming a Linux
  peak test.

Build/check records: [Linux build](evidence/linux-mount-build.txt),
[Linux Clippy](evidence/linux-mount-clippy.txt),
[Windows locked build](evidence/windows-final-build.txt), and
[Windows Clippy](evidence/windows-final-clippy.txt).
The final Linux-only guard change also passed
[Windows documentation generation](evidence/windows-final-doc.txt) and
[Windows release build](evidence/windows-final-release.txt), both locked.
The [final SHA-256 inventory](evidence/artifact-sha256-final.txt) covers current
sources, scripts, documentation, evidence and Windows executables; the
[Linux binary digest](evidence/linux-final-binary-sha256.txt) records the tested
Linux executable. The original `artifact-sha256.txt` remains historical.

## Initial Windows API assessment

The follow-up [controlled memory investigation](WINDOWS-MEMORY-REPRO.md)
found that Job peak first exceeded the cap at allocation-denial checkpoints,
including a refused 128 MiB first allocation with zero retained payload and
unchanged 663,552 process Private Bytes. This narrows the measurement discrepancy,
but does not prove continuous Job commit or change the **FAIL** result.

Primary sources distinguish committed-memory limits from working-set trimming:
[Microsoft extended-limit documentation](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_extended_limit_information)
defines `ProcessMemoryLimit` and `JobMemoryLimit`, while
[Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)
provides resource accounting and process-tree management. Job Objects alone do not
deny filesystem or network access.

The [win32job 2.0.3 safe API](https://docs.rs/win32job/2.0.3/win32job/struct.ExtendedLimitInfo.html)
exposes working-set limits, priority/affinity and kill-on-close. Its backing extended
structure is private; there are no public setters for committed process/job memory
or active process count, and no public cumulative user/kernel accounting query.
Its `limit_working_memory` is not a substitute for the proposed committed-memory
boundary. The spike therefore implements only the useful safe subset and records
the remaining hard gate rather than adding unsafe project code.

Alternative primary API documents were also assessed:

| Candidate | Relevant surface | Remaining mismatch; not adopted or tested |
| --- | --- | --- |
| [process-wrap](https://docs.rs/process-wrap/latest/x86_64-pc-windows-msvc/process_wrap/) | Safe Job wrapper; suspended creation and resume; process-tree cleanup | Public JobObject wrapper does not expose memory/active-count configuration or cumulative CPU accounting |
| [io-harness WindowsSandbox](https://docs.rs/io-harness/latest/io_harness/sandbox/windows/index.html) | Safe whole-command API with committed process memory, active process count, suspended assignment and user-CPU limit | Documented CPU limit excludes kernel time; high-level capture API does not expose the live accounting/private-IPC handles needed for Sider callbacks |

These are candidates for further review, not a claim that no suitable safe wrapper
can exist. No maintenance or audit guarantee is inferred from a published crate.
An io-harness-style resource boundary could prove additional Windows limits in a
separate spike, but its documented user-only CPU policy does not satisfy the
unchanged Sider contract's cumulative user-plus-kernel trigger.

The initial Windows implementation gate required an audited safe dependency exposing
committed process/job memory and active-process limits, cumulative user/kernel CPU
accounting, controlled startup, private live IPC and whole-job termination/reaping.
The follow-up candidate supplies those resource APIs, but does not yet pass the
strict memory observation or a production security audit. Do not enable scripting
until those gates are demonstrated. Neither introducing
unsafe project code nor quietly reducing CPU accounting to user time is an
acceptable resolution. The processkit source also documents an abrupt-parent-death
gap between suspended process creation and Job assignment, which may leave an
unassigned suspended helper. This was not fault-injected; production cleanup must
resolve it. A separately reviewed boundary is still required.

## Next implementation decision

Keep #95 open and production scripting disabled. The runtime choice is promising
for the bounded subset: byte strings, text-only loading, safe environment setup,
allocator ceilings and Lua hooks worked in both environments. Resolve the Windows
strict memory discrepancy and complete the dependency/startup audit and native
Ubuntu/deployment evidence before moving to production helper IPC, parent
temporary-accounting and worker overlays. Private
callback protocol framing, cache lifecycle, output limits, AOF effects, WATCH/TTL,
snapshot admission, replication and error atomicity were not implemented or tested
here. The full contract and release acceptance remain pending.
