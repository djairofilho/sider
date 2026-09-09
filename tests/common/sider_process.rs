//! Binário Sider descartável, validado por versão, PID, prontidão e PING literal.

#![forbid(unsafe_code)]
// Os consumidores de gates e de integração usam subconjuntos desta API.
#![allow(dead_code)]

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::process::{OwnedChild, run};

const TIMEOUT: Duration = Duration::from_secs(10);
const READY_LIMIT: u64 = 4096;
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

pub struct SiderProcess {
    child: OwnedChild,
    directory: Directory,
    address: SocketAddr,
}

impl SiderProcess {
    pub fn start(binary: &Path, expected_version: &str) -> Self {
        Self::try_start(binary, expected_version)
            .unwrap_or_else(|e| panic!("iniciar Sider descartável: {e}"))
    }

    pub fn try_start(binary: &Path, expected_version: &str) -> Result<Self, String> {
        Self::try_start_configured(binary, expected_version, &[])
    }

    /// Configuração injetada no filho; nunca altera o ambiente global dos testes.
    pub fn try_start_configured(
        binary: &Path,
        expected_version: &str,
        overrides: &[(&str, OsString)],
    ) -> Result<Self, String> {
        let version = run(sider(binary).arg("--version"), TIMEOUT)?;
        if !version.status.success()
            || version.stdout != format!("sider {expected_version}\n").as_bytes()
        {
            return Err(format!(
                "versão do binário divergente: status={} stdout={:?}",
                version.status,
                String::from_utf8_lossy(&version.stdout)
            ));
        }
        let mut directory = Directory::new()?;
        let mut command = sider(binary);
        for (name, value) in overrides {
            if !name.starts_with("SIDER_") || matches!(*name, "SIDER_ADDR" | "SIDER_READY_FILE") {
                return Err("override de teste inválido".into());
            }
            command.env(name, value);
        }
        let mut child = OwnedChild::spawn(
            command
                .env("SIDER_ADDR", "127.0.0.1:0")
                .env("SIDER_READY_FILE", directory.ready()),
        )?;
        directory.1 = Some(child.id());
        let address = await_readiness(&mut child, &directory.ready())?;
        child.assert_alive()?;
        eprintln!(
            "Sider descartável: version={expected_version} pid={} address={address}",
            child.id()
        );
        Ok(Self {
            child,
            directory,
            address,
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn assert_alive(&mut self) {
        self.child
            .assert_alive()
            .expect("Sider deve permanecer ativo");
    }

    pub fn finish(mut self) {
        let pid = self.child.id();
        self.child
            .terminate(TIMEOUT)
            .expect("recolher Sider descartável");
        self.directory
            .cleanup(Some(pid))
            .expect("remover arquivos próprios do Sider");
        assert!(!self.directory.0.exists(), "diretório Sider removido");
        eprintln!("processo Sider recolhido: {pid}");
    }
}

impl Drop for SiderProcess {
    fn drop(&mut self) {
        let _ = self.child.terminate(TIMEOUT);
        let _ = self.directory.cleanup(Some(self.child.id()));
    }
}

fn sider(binary: &Path) -> Command {
    let mut command = Command::new(binary);
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("SIDER_")
        {
            command.env_remove(name);
        }
    }
    command
}

struct Directory(PathBuf, Option<u32>);

impl Directory {
    fn new() -> Result<Self, String> {
        for _ in 0..100 {
            let path = std::env::temp_dir().join(format!(
                "sider-process-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path, None)),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(format!("criar diretório Sider exclusivo: {e}")),
            }
        }
        Err("não foi possível reservar diretório Sider exclusivo".into())
    }

    fn ready(&self) -> PathBuf {
        self.0.join("ready.json")
    }

    fn cleanup(&self, pid: Option<u32>) -> io::Result<()> {
        remove_if_present(&self.ready())?;
        if let Some(pid) = pid {
            remove_if_present(&self.0.join(format!(".sider-ready-{pid}-0.tmp")))?;
        }
        match fs::remove_dir(&self.0) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = self.cleanup(self.1);
    }
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn parse_readiness(bytes: &[u8], pid: u32) -> Result<SocketAddr, String> {
    if bytes.len() as u64 > READY_LIMIT {
        return Err("prontidão excedeu limite de bytes".into());
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| format!("JSON de prontidão inválido: {e}"))?;
    if value.as_object().is_none_or(|o| o.len() != 3)
        || value["pid"].as_u64() != Some(u64::from(pid))
    {
        return Err("prontidão não identifica exclusivamente o PID esperado".into());
    }
    let ip: IpAddr = value["host"]
        .as_str()
        .ok_or("host de prontidão ausente")?
        .parse()
        .map_err(|_| "host de prontidão não é IP literal")?;
    let port = value["port"]
        .as_u64()
        .and_then(|n| u16::try_from(n).ok())
        .filter(|p| *p != 0)
        .ok_or("porta de prontidão inválida")?;
    if ip != IpAddr::from([127, 0, 0, 1]) {
        return Err("prontidão deve usar 127.0.0.1".into());
    }
    Ok(SocketAddr::new(ip, port))
}

fn await_readiness(child: &mut OwnedChild, path: &Path) -> Result<SocketAddr, String> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        child.assert_alive()?;
        match File::open(path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(READY_LIMIT + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| format!("ler prontidão: {e}"))?;
                let address = parse_readiness(&bytes, child.id())?;
                let mut stream = TcpStream::connect_timeout(&address, remaining(deadline)?)
                    .map_err(|e| format!("conectar ao Sider pronto: {e}"))?;
                let mut request = b"*1\r\n$4\r\nPING\r\n".as_slice();
                while !request.is_empty() {
                    stream
                        .set_write_timeout(Some(remaining(deadline)?))
                        .map_err(|e| e.to_string())?;
                    let count = stream
                        .write(request)
                        .map_err(|e| format!("PING de prontidão: {e}"))?;
                    if count == 0 {
                        return Err("escrita de PING interrompida".into());
                    }
                    request = &request[count..];
                }
                let mut response = [0; 7];
                let mut offset = 0;
                while offset < response.len() {
                    stream
                        .set_read_timeout(Some(remaining(deadline)?))
                        .map_err(|e| e.to_string())?;
                    let count = stream
                        .read(&mut response[offset..])
                        .map_err(|e| format!("PONG de prontidão: {e}"))?;
                    if count == 0 {
                        return Err("Sider fechou antes de PONG completo".into());
                    }
                    offset += count;
                }
                if &response != b"+PONG\r\n" {
                    return Err("PONG de prontidão divergente".into());
                }
                return Ok(address);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("abrir prontidão: {e}")),
        }
        let remaining = remaining(deadline)?;
        std::thread::sleep(Duration::from_millis(10).min(remaining));
    }
}

fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| "Sider excedeu prazo de prontidão".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn readiness_rejects_wrong_pid_host_port_shape_and_partial_json() {
        let valid = json!({"pid":123,"host":"127.0.0.1","port":23456});
        assert_eq!(
            parse_readiness(&serde_json::to_vec(&valid).unwrap(), 123).unwrap(),
            "127.0.0.1:23456".parse().unwrap()
        );
        for (field, wrong) in [
            ("pid", json!(124)),
            ("host", json!("0.0.0.0")),
            ("host", json!("localhost")),
            ("port", json!(0)),
            ("port", json!(65536)),
            ("port", json!("23456")),
            ("extra", json!(true)),
        ] {
            let mut value = valid.clone();
            value[field] = wrong;
            assert!(parse_readiness(&serde_json::to_vec(&value).unwrap(), 123).is_err());
        }
        assert!(parse_readiness(b"{\"pid\":", 123).is_err());
        assert!(parse_readiness(&vec![b' '; 4097], 123).is_err());
    }

    #[test]
    fn directory_cleanup_is_scoped_and_confirmable() {
        let directory = Directory::new().unwrap();
        fs::write(directory.ready(), b"owned").unwrap();
        fs::write(directory.0.join(".sider-ready-123-0.tmp"), b"partial").unwrap();
        directory.cleanup(Some(123)).unwrap();
        assert!(!directory.0.exists());
    }
}
