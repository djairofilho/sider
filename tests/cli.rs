#![forbid(unsafe_code)]

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const PROCESS_DEADLINE: Duration = Duration::from_secs(10);
const IO_DEADLINE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn sider() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sider"));
    // Changes only the child environment, never the global test environment.
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("SIDER_")
        {
            command.env_remove(name);
        }
    }
    command.stdin(Stdio::null());
    command
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        // create_dir establishes exclusive ownership, including when there is a
        // directory from a previous run with a reused PID.
        for _ in 0..100 {
            let path = std::env::temp_dir().join(format!(
                "sider-cli-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("create test directory: {error}"),
            }
        }
        panic!("could not reserve an exclusive CLI test directory");
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        // Exact names under a test-created directory; no recursive deletion.
        for name in ["ready.json", "stdout.log", "stderr.log"] {
            let _ = fs::remove_file(self.file(name));
        }
        let _ = fs::remove_dir(&self.0);
    }
}

struct ChildProcess {
    child: Child,
    directory: TestDirectory,
}

impl ChildProcess {
    fn start(command: &mut Command, directory: TestDirectory) -> Self {
        // Files prevent full-pipe deadlock while waiting for the child.
        let stdout = File::create(directory.file("stdout.log")).unwrap();
        let stderr = File::create(directory.file("stderr.log")).unwrap();
        let child = command
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn owned test process");
        // Releases the parent handle copies before any Windows cleanup.
        command.stdout(Stdio::null()).stderr(Stdio::null());
        Self { child, directory }
    }

    fn wait(&mut self) -> Output {
        let deadline = Instant::now() + PROCESS_DEADLINE;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child process") {
                return Output {
                    status,
                    stdout: read_small_file(&self.directory.file("stdout.log")),
                    stderr: read_small_file(&self.directory.file("stderr.log")),
                };
            }
            assert!(Instant::now() < deadline, "child process exit deadline");
            // Waits only for an observable change in child process state.
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    fn readiness(&mut self) -> SocketAddr {
        let deadline = Instant::now() + PROCESS_DEADLINE;
        let path = self.directory.file("ready.json");
        loop {
            match fs::read(&path) {
                Ok(bytes) => {
                    // Partial JSON is a failure: publication promises atomicity.
                    let ready: serde_json::Value =
                        serde_json::from_slice(&bytes).expect("complete readiness JSON");
                    let object = ready.as_object().expect("readiness object");
                    assert_eq!(object.len(), 3, "pid, host and port only");
                    assert_eq!(ready["pid"].as_u64(), Some(u64::from(self.child.id())));
                    let host: IpAddr = ready["host"]
                        .as_str()
                        .expect("readiness host string")
                        .parse()
                        .expect("readiness literal IP");
                    assert_eq!(host, IpAddr::from([127, 0, 0, 1]));
                    let port = u16::try_from(ready["port"].as_u64().expect("readiness port"))
                        .expect("port range");
                    assert_ne!(port, 0, "report the assigned port, not requested zero");
                    assert!(self.child.try_wait().unwrap().is_none(), "server is alive");
                    return SocketAddr::new(host, port);
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => panic!("read readiness file: {error}"),
            }
            if let Some(status) = self.child.try_wait().expect("poll startup child") {
                panic!(
                    "server exited before readiness: {status}; stderr: {}",
                    String::from_utf8_lossy(&read_small_file(&self.directory.file("stderr.log")))
                );
            }
            assert!(Instant::now() < deadline, "readiness publication deadline");
            // The file is the readiness signal. We do not probe ports or use
            // this wait to determine database command order.
            std::thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        // The binary creates a single temporary readiness file with serial 0.
        // It can remain here only if the test interrupted the child at that point.
        let own_temporary = self
            .directory
            .file(&format!(".sider-ready-{}-0.tmp", self.child.id()));
        let _ = fs::remove_file(own_temporary);
    }
}

fn read_small_file(path: &Path) -> Vec<u8> {
    let file = File::open(path).expect("open child output");
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .expect("read child output");
    assert!(bytes.len() <= 1024 * 1024, "excessive child output");
    bytes
}

fn output(command: &mut Command) -> Output {
    ChildProcess::start(command, TestDirectory::new()).wait()
}

fn start_server() -> ChildProcess {
    let directory = TestDirectory::new();
    ChildProcess::start(
        sider()
            .env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_READY_FILE", directory.file("ready.json")),
        directory,
    )
}

fn client(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, IO_DEADLINE).expect("connect to child");
    stream.set_read_timeout(Some(IO_DEADLINE)).unwrap();
    stream.set_write_timeout(Some(IO_DEADLINE)).unwrap();
    stream
}

fn exchange(stream: &mut TcpStream, request: &[u8], expected: &[u8]) {
    stream.write_all(request).expect("write literal request");
    let mut actual = vec![0; expected.len()];
    stream
        .read_exact(&mut actual)
        .expect("read literal response");
    assert_eq!(actual, expected);
}

#[test]
fn executable_publishes_readiness_and_serves_literal_tcp_commands() {
    let mut process = start_server();
    let address = process.readiness();
    let mut connection = client(address);
    exchange(&mut connection, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n");
    exchange(
        &mut connection,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n\x00\xff\r\n\r\n",
        b"+OK\r\n",
    );
    exchange(
        &mut connection,
        b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
        b"$4\r\n\x00\xff\r\n\r\n",
    );
    exchange(&mut connection, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n");
    assert!(process.child.try_wait().unwrap().is_none());
    // Drop terminates and reaps only this child. On Windows it sends no signals
    // to the shared console; cooperative shutdown is tested through the serve API.
}

#[test]
fn invalid_configuration_fails_with_a_diagnostic() {
    let output = output(sider().env("SIDER_ADDR", "invalid"));

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("SIDER_ADDR")
    );
}

#[test]
fn invalid_limit_is_rejected_before_binding_or_publishing_readiness() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let directory = TestDirectory::new();
    let ready = directory.file("ready.json");
    let mut process = ChildProcess::start(
        sider()
            .env("SIDER_ADDR", occupied.local_addr().unwrap().to_string())
            .env("SIDER_MAX_CONNECTIONS", "0")
            .env("SIDER_READY_FILE", &ready),
        directory,
    );
    let output = process.wait();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("SIDER_MAX_CONNECTIONS"),
        "configuration error must precede the occupied-address error"
    );
    assert!(!ready.exists());
}

#[test]
fn occupied_bind_address_fails_without_readiness() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let directory = TestDirectory::new();
    let ready = directory.file("ready.json");
    let mut process = ChildProcess::start(
        sider()
            .env("SIDER_ADDR", occupied.local_addr().unwrap().to_string())
            .env("SIDER_READY_FILE", &ready),
        directory,
    );
    let output = process.wait();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    assert!(!ready.exists());
    assert!(
        occupied.local_addr().is_ok(),
        "test still owns its listener"
    );
}

#[test]
fn existing_readiness_file_is_not_overwritten() {
    let directory = TestDirectory::new();
    let ready = directory.file("ready.json");
    let existing = b"readiness belongs to a different process\n";
    fs::write(&ready, existing).unwrap();
    let mut process = ChildProcess::start(
        sider()
            .env("SIDER_ADDR", "127.0.0.1:0")
            .env("SIDER_READY_FILE", &ready),
        directory,
    );
    let output = process.wait();

    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
    assert_eq!(fs::read(&ready).unwrap(), existing);
    assert_eq!(fs::read_dir(&process.directory.0).unwrap().count(), 3);
}

#[test]
fn help_does_not_require_valid_server_configuration() {
    let output = output(sider().env("SIDER_ADDR", "invalid").arg("--help"));

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Usage: sider"));
    assert!(stdout.contains("SIDER_READY_FILE"));
}

#[test]
fn metrics_diagnose_validates_without_binding_or_touching_aof_or_readiness() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let directory = TestDirectory::new();
    let ready = directory.file("ready.json");
    let missing_aof = directory.file("secret-aof-directory");
    let mut command = sider();
    command
        .arg("--diagnose")
        .env("SIDER_ADDR", listener.local_addr().unwrap().to_string())
        .env("SIDER_AOF_DIR", &missing_aof)
        .env(
            "SIDER_REPLICATION_ADDR",
            listener.local_addr().unwrap().to_string(),
        )
        .env("SIDER_REPLICA_OF", "127.0.0.1:1")
        .env("SIDER_REPLICATION_READY_FILE", &ready)
        .env("SIDER_READY_FILE", &ready);
    let mut process = ChildProcess::start(&mut command, directory);
    let output = process.wait();
    assert!(output.status.success(), "{:?}", output.stderr);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("diagnostic_scope:configuration_only"));
    assert!(stdout.contains("aof_configured:1"));
    assert!(stdout.contains("ready_file_enabled:1"));
    assert!(stdout.contains("replication_configured:1"));
    assert!(stdout.contains("replication_upstream_configured:1"));
    assert!(stdout.contains("replication_ready_file_enabled:1"));
    assert!(stdout.contains("replication_backlog_limit_batches:4096"));
    assert!(!stdout.contains("127.0.0.1:1\r\n"));
    assert!(!stdout.contains("secret-aof-directory"));
    assert!(!stdout.contains(&process.directory.0.to_string_lossy().to_string()));
    assert!(!missing_aof.exists());
    assert!(!ready.exists());
    assert!(output.stderr.is_empty());
}

#[test]
fn metrics_diagnose_errors_hide_invalid_environment_values() {
    let mut command = sider();
    command
        .arg("--diagnose")
        .env("SIDER_MAX_CONNECTIONS", "secret\nvalue");
    let mut process = ChildProcess::start(&mut command, TestDirectory::new());
    let output = process.wait();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("SIDER_MAX_CONNECTIONS"));
    assert!(!stderr.contains("secret"));
    assert!(!stderr.contains("value"));
}

#[test]
fn version_matches_the_package_manifest() {
    let output = output(sider().arg("--version"));

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("sider {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn unknown_arguments_are_rejected() {
    let output = output(sider().arg("--unknown"));

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains("usage:"));
}

#[cfg(unix)]
#[test]
fn sigterm_stops_the_owned_server_and_removes_readiness() {
    let mut process = start_server();
    let address = process.readiness();
    let ready = process.directory.file("ready.json");
    let mut connection = client(address);
    exchange(&mut connection, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n");

    let signal = output(
        Command::new("/bin/kill")
            .arg("-TERM")
            .arg(process.child.id().to_string()),
    );
    assert!(
        signal.status.success(),
        "signal only the owned positive PID"
    );
    let output = process.wait();
    assert!(
        output.status.success(),
        "graceful SIGTERM shutdown: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!ready.exists(), "graceful shutdown removes own readiness");
    let mut byte = [0];
    assert_eq!(connection.read(&mut byte).unwrap(), 0);
    assert!(TcpStream::connect_timeout(&address, IO_DEADLINE).is_err());
}
