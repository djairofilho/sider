//! Test-owned processes, with bounded output and waits.

#![forbid(unsafe_code)]
// Each integration binary uses a different part of this shared API.
#![allow(dead_code)]

use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

pub const OUTPUT_LIMIT: usize = 1024 * 1024;
const POLL: Duration = Duration::from_millis(10);
const DROP_TIMEOUT: Duration = Duration::from_millis(500);

type Capture = Receiver<io::Result<Vec<u8>>>;

/// One specific child; never terminates processes by name or a global process tree.
pub struct OwnedChild {
    child: Child,
    stdout: Option<Capture>,
    stderr: Option<Capture>,
    exceeded: Arc<AtomicBool>,
    status: Option<ExitStatus>,
}

impl OwnedChild {
    pub fn spawn(command: &mut Command) -> Result<Self, String> {
        Self::spawn_with_stdout_limit(command, OUTPUT_LIMIT)
    }

    fn spawn_with_stdout_limit(command: &mut Command, stdout_limit: usize) -> Result<Self, String> {
        if stdout_limit == 0 || stdout_limit > 128 * 1024 * 1024 {
            return Err("invalid stdout capture limit".into());
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("start test process: {error}"))?;
        let exceeded = Arc::new(AtomicBool::new(false));
        let mut owned = Self {
            child,
            stdout: None,
            stderr: None,
            exceeded,
            status: None,
        };
        owned.stdout = Some(capture_thread(
            owned.child.stdout.take().expect("stdout pipe"),
            owned.exceeded.clone(),
            stdout_limit,
        )?);
        owned.stderr = Some(capture_thread(
            owned.child.stderr.take().expect("stderr pipe"),
            owned.exceeded.clone(),
            OUTPUT_LIMIT,
        )?);
        Ok(owned)
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn assert_alive(&mut self) -> Result<(), String> {
        if self.exceeded.load(Ordering::Relaxed) {
            return Err("process exceeded the stdout/stderr limit".into());
        }
        match self.poll()? {
            None => Ok(()),
            Some(status) => Err(format!("process exited prematurely: {status}")),
        }
    }

    fn poll(&mut self) -> Result<Option<ExitStatus>, String> {
        if self.status.is_none() {
            self.status = self
                .child
                .try_wait()
                .map_err(|e| format!("wait for process: {e}"))?;
        }
        Ok(self.status)
    }

    pub fn wait(&mut self, timeout: Duration) -> Result<Output, String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("invalid process deadline")?;
        let status = loop {
            if self.exceeded.load(Ordering::Relaxed) {
                return Err("process exceeded the stdout/stderr limit".into());
            }
            if let Some(status) = self.poll()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("process exceeded the execution deadline".into());
            }
            pause(deadline);
        };
        self.output(status, deadline)
    }

    /// Kills only the guarded child and verifies that it was reaped.
    pub fn terminate(&mut self, timeout: Duration) -> Result<Output, String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("invalid termination deadline")?;
        if self.poll()?.is_none()
            && let Err(error) = self.child.kill()
            && self.poll()?.is_none()
        {
            return Err(format!("terminate child {}: {error}", self.id()));
        }
        let status = loop {
            if let Some(status) = self.poll()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("child was not reaped before the deadline".into());
            }
            pause(deadline);
        };
        self.output(status, deadline)
    }

    fn output(&mut self, status: ExitStatus, deadline: Instant) -> Result<Output, String> {
        let stdout = receive(
            self.stdout.take().ok_or("stdout already consumed")?,
            "stdout",
            deadline,
        )?;
        let stderr = receive(
            self.stderr.take().ok_or("stderr already consumed")?,
            "stderr",
            deadline,
        )?;
        if self.exceeded.load(Ordering::Relaxed) {
            return Err("process exceeded the stdout/stderr limit".into());
        }
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.poll().ok().flatten().is_some() {
            return;
        }
        let _ = self.child.kill();
        let deadline = Instant::now() + DROP_TIMEOUT;
        while matches!(self.poll(), Ok(None)) && Instant::now() < deadline {
            pause(deadline);
        }
        // No unbounded join: a descendant may have inherited a pipe. Each
        // reader retains at most its capture limit and does not prevent the test from finishing.
    }
}

/// The deadline includes execution and EOF on both streams; nonzero status is preserved.
pub fn run(command: &mut Command, timeout: Duration) -> Result<Output, String> {
    let mut child = OwnedChild::spawn(command)?;
    child.wait(timeout)
}

/// Captures a package member with an explicit limit; stderr retains OUTPUT_LIMIT.
pub fn run_with_stdout_limit(
    command: &mut Command,
    timeout: Duration,
    limit: usize,
) -> Result<Output, String> {
    let mut child = OwnedChild::spawn_with_stdout_limit(command, limit)?;
    child.wait(timeout)
}

fn pause(deadline: Instant) {
    thread::sleep(POLL.min(deadline.saturating_duration_since(Instant::now())));
}

fn capture_thread(
    stream: impl Read + Send + 'static,
    exceeded: Arc<AtomicBool>,
    limit: usize,
) -> Result<Capture, String> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("sider-test-output".into())
        .spawn(move || {
            let _ = sender.send(capture(stream, &exceeded, limit));
        })
        .map_err(|e| format!("create output reader: {e}"))?;
    Ok(receiver)
}

fn capture(mut stream: impl Read, exceeded: &AtomicBool, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Ok(bytes);
        }
        let retained = count.min(limit.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&chunk[..retained]);
        if retained != count {
            exceeded.store(true, Ordering::Relaxed);
        }
    }
}

fn receive(capture: Capture, name: &str, deadline: Instant) -> Result<Vec<u8>, String> {
    capture
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|e| format!("capture {name} within deadline: {e}"))?
        .map_err(|e| format!("capture {name}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_drains_but_flags_overflow() {
        let input = vec![b'x'; OUTPUT_LIMIT + 1];
        let mut reader = input.as_slice();
        let exceeded = AtomicBool::new(false);
        let result = capture(&mut reader, &exceeded, OUTPUT_LIMIT).unwrap();
        assert_eq!(result.len(), OUTPUT_LIMIT);
        assert!(exceeded.load(Ordering::Relaxed));
        assert!(reader.is_empty());
    }

    #[test]
    fn capture_accepts_exact_limit() {
        let exceeded = AtomicBool::new(false);
        assert_eq!(
            capture(vec![b'x'; OUTPUT_LIMIT].as_slice(), &exceeded, OUTPUT_LIMIT)
                .unwrap()
                .len(),
            OUTPUT_LIMIT
        );
        assert!(!exceeded.load(Ordering::Relaxed));
    }

    #[test]
    fn release_input_capture_keeps_explicit_binary_limit() {
        for limit in [1, OUTPUT_LIMIT + 8] {
            let exceeded = AtomicBool::new(false);
            assert_eq!(
                capture(vec![7; limit + 1].as_slice(), &exceeded, limit)
                    .unwrap()
                    .len(),
                limit
            );
            assert!(exceeded.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn open_pipe_sender_cannot_extend_the_capture_deadline() {
        let (_sender, receiver) = mpsc::sync_channel(1);
        assert!(receive(receiver, "stdout", Instant::now()).is_err());
    }
}
