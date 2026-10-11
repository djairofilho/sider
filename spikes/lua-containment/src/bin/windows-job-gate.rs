#![forbid(unsafe_code)]

#[cfg(not(windows))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("this resource gate requires Windows".into())
}

#[cfg(windows)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(windows::run())
}

#[cfg(windows)]
mod windows {
    use processkit::{ProcessGroup, ProcessGroupOptions};
    use std::process::Stdio;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    type Error = Box<dyn std::error::Error>;
    const MEMORY: u64 = 64 * 1024 * 1024;

    pub async fn run() -> Result<(), Error> {
        let helper = std::env::current_exe()?.with_file_name("sider-lua-containment-spike.exe");
        let mut failures = 0;
        for (case, expected) in [
            ("os-cpu", "CPU_KILLED"),
            ("os-kernel-cpu", "CPU_KILLED"),
            ("os-memory", "MEMORY_DENIED"),
            ("os-pids", "OK os-pids denied"),
            ("native-call", "WALL_KILLED"),
        ] {
            let started = Instant::now();
            let group = ProcessGroup::with_options(
                ProcessGroupOptions::default()
                    .max_memory(MEMORY)
                    .max_processes(1),
            )?;
            let mut command = tokio::process::Command::new(&helper);
            command
                .arg(case)
                .env_clear()
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
            // The dependency creates suspended, assigns the configured Job, and
            // resumes before returning. This owns only the newly spawned helper.
            let mut child = group.spawn(command)?;
            let pid = child.id().ok_or("missing helper PID")?;
            let mut lines = BufReader::new(child.stdout.take().ok_or("missing stdout")?).lines();
            let ready = tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await;
            if !matches!(ready, Ok(Ok(Some(ref line))) if line == "READY") {
                group.kill_all()?;
                child.wait().await?;
                return Err("helper readiness failed; GO withheld".into());
            }
            let before = group.stats()?;
            if group.members()? != [pid]
                || before.total_cpu_time.is_none()
                || before.peak_memory_bytes.is_none()
            {
                group.kill_all()?;
                child.wait().await?;
                return Err("required Job membership/accounting unavailable; GO withheld".into());
            }
            println!(
                "BOUNDARY suspended_spawn=true before_GO pid={pid} configured_memory={MEMORY} max_processes=1 stats={before:?}"
            );
            let mut input = child.stdin.take().ok_or("missing stdin")?;
            input.write_all(b"GO\n").await?;
            input.shutdown().await?;
            drop(input);
            let result = loop {
                match tokio::time::timeout(Duration::from_millis(5), lines.next_line()).await {
                    Ok(Ok(Some(line))) => break line,
                    Ok(Ok(None)) => break "EXIT_WITHOUT_RESULT".into(),
                    Ok(Err(error)) => {
                        group.kill_all()?;
                        child.wait().await?;
                        return Err(error.into());
                    }
                    Err(_) => {}
                }
                let stats = group.stats()?;
                let cpu = stats.total_cpu_time.ok_or("CPU accounting unavailable")?;
                // Fixture-only separation of memory proof from reclaim CPU.
                if case != "os-memory" && cpu >= Duration::from_millis(100) {
                    group.kill_all()?;
                    break "CPU_KILLED".into();
                }
                if started.elapsed() >= Duration::from_millis(500) {
                    group.kill_all()?;
                    break "WALL_KILLED".into();
                }
            };
            let status = child.wait().await?;
            let after = group.stats()?;
            let peak = after
                .peak_memory_bytes
                .ok_or("memory accounting unavailable")?;
            let passed =
                result.starts_with(expected) && peak <= MEMORY && after.active_process_count == 0;
            failures += usize::from(!passed);
            println!(
                "{} case={case} result={result} status={status} elapsed_us={} peak_within_limit={} after_reap={after:?}",
                if passed { "PASS" } else { "FAIL" },
                started.elapsed().as_micros(),
                peak <= MEMORY
            );
        }
        println!(
            "SUMMARY failures={failures} strict_peak_gate={} full_contract_proven=false",
            failures == 0
        );
        if failures != 0 {
            return Err("Windows resource gate failed".into());
        }
        Ok(())
    }
}
