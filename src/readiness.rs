//! Atomic readiness publication without replacing another process's files.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

/// Readiness file owned by the current process.
///
/// Uses a hard link in the same directory to publish complete JSON without
/// replacing an existing destination. Requires a filesystem supporting hard links.
/// On normal shutdown, removes only a destination whose content is still its own.
/// Forced termination can leave an old file; the consumer must check the PID.
pub struct ReadyFile {
    path: PathBuf,
    contents: Vec<u8>,
}

impl ReadyFile {
    /// Creates the file after bind, with the address actually assigned.
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
        // The destination exists only after the JSON is complete and closed.
        // Unlike Unix rename, it does not replace an existing destination.
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
