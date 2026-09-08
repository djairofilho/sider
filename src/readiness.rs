//! Publicação atômica de prontidão, sem substituir arquivos de outro processo.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

/// Arquivo de prontidão pertencente ao processo atual.
///
/// Usa um hard link no mesmo diretório para publicar o JSON completo sem
/// substituir um destino existente. Requer filesystem com suporte a hard links.
/// No encerramento normal remove apenas um destino cujo conteúdo ainda é seu.
/// Término forçado pode deixar um arquivo antigo; o consumidor deve conferir PID.
pub struct ReadyFile {
    path: PathBuf,
    contents: Vec<u8>,
}

impl ReadyFile {
    /// Cria o arquivo depois do bind, com o endereço efetivamente atribuído.
    pub fn create(path: &Path, address: SocketAddr) -> io::Result<Self> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary = parent.join(format!(
            ".sider-ready-{}-{}.tmp",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let contents = format!(
            "{{\"pid\":{},\"host\":\"{}\",\"port\":{}}}\n",
            std::process::id(),
            address.ip(),
            address.port()
        )
        .into_bytes();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let cleanup = TemporaryFile(temporary);
        file.write_all(&contents)?;
        file.sync_all()?;
        drop(file);
        // Destino só passa a existir quando o JSON já está completo e fechado.
        // Diferentemente de rename em Unix, não substitui um destino existente.
        fs::hard_link(&cleanup.0, path)?;
        Ok(Self {
            path: path.to_owned(),
            contents,
        })
    }
}

impl Drop for ReadyFile {
    fn drop(&mut self) {
        let Ok(file) = fs::File::open(&self.path) else {
            return;
        };
        let mut actual = Vec::new();
        if file
            .take(self.contents.len() as u64 + 1)
            .read_to_end(&mut actual)
            .is_ok()
            && actual == self.contents
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct TemporaryFile(PathBuf);

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "sider-ready-test-{}-{}",
                std::process::id(),
                NEXT_FILE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir(&self.0);
        }
    }

    #[test]
    fn publishes_complete_json_and_removes_own_file() {
        let directory = Directory::new();
        let path = directory.0.join("ready.json");
        let ready = ReadyFile::create(&path, "127.0.0.1:12345".parse().unwrap()).unwrap();
        let data: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(data["pid"], std::process::id());
        assert_eq!(data["host"], "127.0.0.1");
        assert_eq!(data["port"], 12345);
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
        drop(ready);
        assert!(!path.exists());
    }

    #[test]
    fn does_not_replace_existing_destination_or_leave_temporary_files() {
        let directory = Directory::new();
        let path = directory.0.join("ready.json");
        fs::write(&path, b"another process").unwrap();
        assert!(ReadyFile::create(&path, "[::1]:1".parse().unwrap()).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"another process");
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn cleanup_preserves_changed_destination() {
        let directory = Directory::new();
        let path = directory.0.join("ready.json");
        let ready = ReadyFile::create(&path, "[::1]:1".parse().unwrap()).unwrap();
        fs::write(&path, b"changed").unwrap();
        drop(ready);
        assert_eq!(fs::read(&path).unwrap(), b"changed");
        fs::remove_file(path).unwrap();
    }
}
