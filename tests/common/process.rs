//! Processos pertencentes aos testes, com saída e espera limitadas.

#![forbid(unsafe_code)]
// Cada binário de integração usa uma parte diferente desta API comum.
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

/// Um filho específico; nunca encerra processos por nome ou árvore global.
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
            return Err("limite de captura stdout inválido".into());
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("iniciar processo de teste: {error}"))?;
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
            return Err("processo excedeu limite de stdout/stderr".into());
        }
        match self.poll()? {
            None => Ok(()),
            Some(status) => Err(format!("processo terminou prematuramente: {status}")),
        }
    }

    fn poll(&mut self) -> Result<Option<ExitStatus>, String> {
        if self.status.is_none() {
            self.status = self
                .child
                .try_wait()
                .map_err(|e| format!("aguardar processo: {e}"))?;
        }
        Ok(self.status)
    }

    pub fn wait(&mut self, timeout: Duration) -> Result<Output, String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("prazo de processo inválido")?;
        let status = loop {
            if self.exceeded.load(Ordering::Relaxed) {
                return Err("processo excedeu limite de stdout/stderr".into());
            }
            if let Some(status) = self.poll()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("processo excedeu prazo de execução".into());
            }
            pause(deadline);
        };
        self.output(status, deadline)
    }

    /// Mata somente o filho guardado e comprova que ele foi recolhido.
    pub fn terminate(&mut self, timeout: Duration) -> Result<Output, String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("prazo de término inválido")?;
        if self.poll()?.is_none()
            && let Err(error) = self.child.kill()
            && self.poll()?.is_none()
        {
            return Err(format!("encerrar filho {}: {error}", self.id()));
        }
        let status = loop {
            if let Some(status) = self.poll()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("filho não foi recolhido dentro do prazo".into());
            }
            pause(deadline);
        };
        self.output(status, deadline)
    }

    fn output(&mut self, status: ExitStatus, deadline: Instant) -> Result<Output, String> {
        let stdout = receive(
            self.stdout.take().ok_or("stdout já consumido")?,
            "stdout",
            deadline,
        )?;
        let stderr = receive(
            self.stderr.take().ok_or("stderr já consumido")?,
            "stderr",
            deadline,
        )?;
        if self.exceeded.load(Ordering::Relaxed) {
            return Err("processo excedeu limite de stdout/stderr".into());
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
        // Sem join ilimitado: um descendente pode ter herdado um pipe. Cada
        // leitor retém no máximo seu limite de captura e não impede o término do teste.
    }
}

/// Prazo inclui execução e EOF dos dois streams; status não zero é preservado.
pub fn run(command: &mut Command, timeout: Duration) -> Result<Output, String> {
    let mut child = OwnedChild::spawn(command)?;
    child.wait(timeout)
}

/// Captura de um membro de pacote com limite explícito; stderr mantém OUTPUT_LIMIT.
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
        .map_err(|e| format!("criar leitor de saída: {e}"))?;
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
        .map_err(|e| format!("capturar {name} dentro do prazo: {e}"))?
        .map_err(|e| format!("capturar {name}: {e}"))
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
