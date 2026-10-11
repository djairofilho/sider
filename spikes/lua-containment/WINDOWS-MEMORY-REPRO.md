# Controlled Windows Job memory discrepancy

Recorded October 10, 2026 on Windows NT 10.0.26300.0, x86_64 MSVC,
processkit 3.3.4. Production scripting remains disabled. The configured Job
committed-memory ceiling remains **67,108,864 bytes (64 MiB)**.

## Method and evidence

[`windows-memory-repro.rs`](src/bin/windows-memory-repro.rs) creates a fresh Job
with memory 64 MiB and one active process. Its helper starts through the same
safe suspended-spawn API as the previous Job gate. The supervisor checks exact
membership before every acknowledgement. Each allocation has a BEFORE checkpoint,
then SUCCESS or DENIED. Successful blocks remain allocated through measurement
of DENIED. Bookkeeping capacity is reserved before READY.

The supervisor reads processkit's Job peak at each checkpoint. A separate,
uncontained diagnostic [PowerShell observer](observe-private-memory.ps1) reads
`System.Diagnostics.Process.PrivateMemorySize64` and `WorkingSet64` for only the
new helper PID, calls `Refresh`, then disposes its process handle. It uses no
P/Invoke. Its footprint is outside the measured Job.

The slow checkpoint diagnostic has a 15-second cleanup deadline per case;
it is not the production-contract wall or CPU proof. The original 500 ms wall
fixture, cumulative CPU fixture and strict peak comparison are unchanged.

Reproduce:

```powershell
cargo build --locked --manifest-path spikes/lua-containment/Cargo.toml --bin windows-memory-repro
spikes/lua-containment/target/debug/windows-memory-repro.exe
```

[Build](evidence/windows-memory-repro-build.txt) and
[all-target Clippy](evidence/windows-memory-repro-clippy.txt) passed.
[Locked documentation](evidence/windows-memory-repro-doc.txt) and the
[release binary build](evidence/windows-memory-repro-release.txt) also passed.
[Host/observer versions](evidence/windows-memory-repro-platform.txt) are preserved.
[Raw runtime](evidence/windows-memory-repro-runtime.txt) exited **1**, preserving
the strict gate failure. Each helper reported allocation denial, exited normally,
was reaped and left zero active Job processes. The observer was also waited.

## Measurements

All values below are bytes. The BEFORE row is immediately before the denied
attempt. No successful retained allocation was released between these checkpoints.

| Requested allocation | Retained at denial | Job peak before attempt | Job peak after denial | Private bytes before / after denial |
| --- | ---: | ---: | ---: | ---: |
| 1 MiB | 65,011,712 | 66,084,864 | **67,125,248** | 66,052,096 / 66,052,096 |
| 64 KiB | 65,863,680 | 67,104,768 | **67,178,496** | 67,104,768 / 67,104,768 |
| 128 MiB, first attempt | 0 | 663,552 | **135,155,712** | 663,552 / 663,552 |

Every SUCCESS checkpoint stayed below the 64 MiB Job peak ceiling. The first
checkpoint exceeding it in each case was DENIED. The 128 MiB case had no retained
payload at all and returned an allocation error. A reserve-failure-only repro
therefore explains substantially more than the prior suspected assignment race.

**Inference:** these observations are consistent with a Job high-water value
including a refused allocation attempt or transient charging that is rolled back.
They do not establish Windows kernel implementation details. No official source
found in this investigation documents that specific high-water behavior, and
an independent snapshot cannot rule out a short-lived charge between checkpoints.
The strict `PeakJobMemoryUsed <= 64 MiB` gate remains **FAIL**.

## What the primary sources establish

- [Microsoft extended Job limits](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_extended_limit_information)
  describes committed-memory limits and the constantly tracked Job peak. Its
  remarks distinguish per-process and aggregate Job ceilings.
- [Microsoft basic Job limits](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_limit_information)
  documents failure of a commit that would exceed the Job memory limit. This
  is compatible with the observed allocation errors; it does not resolve the
  peak discrepancy.
- [Microsoft PrivateMemorySize64](https://learn.microsoft.com/en-us/dotnet/api/system.diagnostics.process.privatememorysize64)
  defines the refreshed process Private Bytes measurement. It is not a continuous
  Job-wide meter, and WorkingSet64 is not the committed-memory contract.
- The [published processkit source](https://docs.rs/crate/processkit/3.3.4/source/src/sys/windows.rs)
  configures `JOB_OBJECT_LIMIT_JOB_MEMORY`, then reads `PeakJobMemoryUsed` directly
  for `ProcessGroupStats::peak_memory_bytes`. The limit-evidence implementation
  deliberately reports memory violation evidence as unknown: an allocation failure
  and a high-water number alone do not prove which boundary fired. It does not
  expose current Job committed memory or configured-limit readback in its safe API.

## Safe containment options and remaining work

The existing safe Job configuration remains a candidate for the actual committed
memory contract, but these tests cannot approve the stricter peak assertion or
claim continuous commit proof. A reviewed safe wrapper exposing configured-limit
readback, current Job commit and live memory-limit completion-port notifications
would allow a stronger experiment without unsafe project code. Notification-only
limits do not replace the hard memory cap.

The [io-harness Windows backend](https://docs.rs/io-harness/0.86.0/io_harness/sandbox/windows/struct.WindowsSandbox.html)
is another safe Job-based candidate already documented in the spike. Switching
wrappers does not establish different Windows peak semantics. It also needs
the previously identified cumulative kernel CPU and private live IPC gaps
resolved before satisfying the full contract. It was not adopted or tested here.

Reducing the configured limit by an arbitrary margin does not establish a strict
peak bound: the refused 128 MiB attempt already produced a much larger peak with
no retained payload. No margin, working-set ceiling, periodic Private Bytes
poll, allocation error, or user-only CPU substitute is accepted as passing this
gate. The next useful proof needs authoritative measurement semantics and failure
evidence, preferably on a supported stable Windows release as well as this host.
Startup/crash cleanup auditing and production IPC remain separate open gates.
