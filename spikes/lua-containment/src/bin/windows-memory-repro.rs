#![forbid(unsafe_code)]

#[cfg(not(windows))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("this diagnostic requires Windows".into())
}

#[cfg(windows)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(chunk) = std::env::args().nth(1) {
        return windows::helper(chunk.parse()?);
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(windows::run())
}

#[cfg(windows)]
mod windows {
    use processkit::{ProcessGroup, ProcessGroupOptions};
    use std::io::Write;
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    type Error = Box<dyn std::error::Error>;
    const CAP: u64 = 64 * 1024 * 1024;

    fn checkpoint(line: &str) -> Result<(), Error> {
        println!("{line}");
        std::io::stdout().flush()?;
        let mut ack = String::new();
        std::io::stdin().read_line(&mut ack)?;
        if ack != "CONTINUE\n" {
            return Err("diagnostic gate closed".into());
        }
        Ok(())
    }

    pub fn helper(chunk: usize) -> Result<(), Error> {
        // Reserve bookkeeping before READY, so growth of this vector does not
        // obscure the single allocation under observation.
        let mut blocks = Vec::with_capacity(2048);
        checkpoint("READY")?;
        for attempt in 0..2048 {
            checkpoint(&format!(
                "BEFORE attempt={attempt} retained_bytes={}",
                blocks.len() * chunk
            ))?;
            let mut block = Vec::new();
            if let Err(error) = block.try_reserve_exact(chunk) {
                checkpoint(&format!(
                    "DENIED attempt={attempt} retained_bytes={} error={error}",
                    blocks.len() * chunk
                ))?;
                return Ok(());
            }
            // Touch every byte, retaining successful allocations throughout
            // the DENIED checkpoint and its independent process measurement.
            block.resize(chunk, 0x42_u8);
            blocks.push(block);
            checkpoint(&format!(
                "SUCCESS attempt={attempt} retained_bytes={}",
                blocks.len() * chunk
            ))?;
            std::hint::black_box(&blocks);
        }
        Err("allocation denial not reached".into())
    }

    pub async fn run() -> Result<(), Error> {
        println!(
            "DIAGNOSTIC cap={CAP} suspended_spawn=true strict_gate_unchanged=true snapshot_private_bytes_not_job_commit=true"
        );
        let observer_script =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("observe-private-memory.ps1");
        let shell = std::path::PathBuf::from(std::env::var("SystemRoot")?)
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut observer = tokio::process::Command::new(shell)
            .args(["-NoProfile", "-NonInteractive", "-File"])
            .arg(observer_script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let mut observer_input = observer.stdin.take().ok_or("observer stdin missing")?;
        let mut observer_lines =
            BufReader::new(observer.stdout.take().ok_or("observer stdout missing")?).lines();
        let mut failed = false;
        for chunk in [1024 * 1024_usize, 64 * 1024, 128 * 1024 * 1024] {
            let group = ProcessGroup::with_options(
                ProcessGroupOptions::default()
                    .max_memory(CAP)
                    .max_processes(1),
            )?;
            let mut command = tokio::process::Command::new(std::env::current_exe()?);
            command
                .arg(chunk.to_string())
                .env_clear()
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            let mut child = group.spawn(command)?;
            let pid = child.id().ok_or("helper PID missing")?;
            let mut input = child.stdin.take().ok_or("helper stdin missing")?;
            let mut lines =
                BufReader::new(child.stdout.take().ok_or("helper stdout missing")?).lines();
            // This slow, handshake-based diagnostic is separate from the
            // unchanged 500 ms production-contract fixture. Bound its cleanup.
            let observation: Result<bool, Error> = match tokio::time::timeout(Duration::from_secs(15), async {
                let mut denied = false;
                loop {
                    let Some(line) = lines.next_line().await? else { break };
                    if group.members()? != [pid] { return Err("helper membership missing".into()); }
                    let stats = group.stats()?;
                    let peak = stats.peak_memory_bytes.ok_or("Job peak missing")?;
                    failed |= peak > CAP;
                    observer_input.write_all(format!("{pid}\n").as_bytes()).await?;
                    observer_input.flush().await?;
                    let private = observer_lines.next_line().await?.ok_or("observer stopped")?;
                    println!("SAMPLE chunk={chunk} pid={pid} {line} job_peak={peak} within_cap={} {private}", peak <= CAP);
                    denied |= line.starts_with("DENIED");
                    input.write_all(b"CONTINUE\n").await?;
                    input.flush().await?;
                }
                Ok(denied)
            }).await {
                Ok(result) => result,
                Err(_) => Err("diagnostic checkpoint timeout".into()),
            };
            if observation.is_err() {
                group.kill_all()?;
            }
            let status = child.wait().await?;
            let denied = observation?;
            let stats = group.stats()?;
            let peak = stats
                .peak_memory_bytes
                .ok_or("post-reap Job peak missing")?;
            failed |= !denied || !status.success() || peak > CAP || stats.active_process_count != 0;
            println!("CASE chunk={chunk} denied={denied} status={status} after_reap={stats:?}");
        }
        observer_input.shutdown().await?;
        drop(observer_input);
        let observer_status =
            tokio::time::timeout(Duration::from_secs(5), observer.wait()).await??;
        if !observer_status.success() {
            return Err("observer failed".into());
        }
        println!(
            "SUMMARY strict_peak_gate={} production_enabled=false",
            !failed
        );
        if failed {
            return Err("strict peak discrepancy reproduced".into());
        }
        Ok(())
    }
}
