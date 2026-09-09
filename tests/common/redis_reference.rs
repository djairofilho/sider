//! Disposable Redis reference, independent of the Sider codec and server.

#![forbid(unsafe_code)]

use std::fs;
use std::io::{self, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const OUTPUT_LIMIT: usize = 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct ReferenceConfig {
    image: String,
    digest: String,
    platform: String,
    redis_version: String,
    redis_cli_version: String,
}

impl ReferenceConfig {
    fn parse(manifest: &str) -> Result<Self, String> {
        let manifest: Value = serde_json::from_str(manifest).map_err(|e| e.to_string())?;
        let reference = &manifest["reference"];
        let field = |name| {
            reference[name]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("missing or invalid reference.{name}"))
        };
        let image = field("image")?;
        let platform = field("platform")?;
        let redis_version = field("redis_version")?;
        let redis_cli_version = field("redis_cli_version")?;
        if platform != "linux/amd64" {
            return Err("the reference must use linux/amd64".into());
        }
        for version in [&redis_version, &redis_cli_version] {
            let parts: Vec<_> = version.split('.').collect();
            if parts.len() != 3
                || parts
                    .iter()
                    .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
            {
                return Err("reference version must have three numeric components".into());
            }
        }
        let prefix = format!("redis:{redis_version}@sha256:");
        let digest = image
            .strip_prefix(&prefix)
            .filter(|digest| valid_hex_id(digest))
            .ok_or("image must pin the declared Redis tag and a complete SHA-256 digest")?;
        Ok(Self {
            digest: format!("sha256:{digest}"),
            image,
            platform,
            redis_version,
            redis_cli_version,
        })
    }

    fn inspect_image(&self, output: &[u8]) -> Result<String, String> {
        let value: Value = serde_json::from_slice(output).map_err(|e| e.to_string())?;
        let image = single_inspection(&value)?;
        if image["Os"] != "linux" || image["Architecture"] != "amd64" {
            return Err("local image does not match linux/amd64".into());
        }
        let digests = image["RepoDigests"]
            .as_array()
            .ok_or("local image has no RepoDigests")?;
        let expected = [
            format!("redis@{}", self.digest),
            format!("docker.io/library/redis@{}", self.digest),
        ];
        if !digests.iter().any(|value| {
            value
                .as_str()
                .is_some_and(|v| expected.iter().any(|e| e == v))
        }) {
            return Err("RepoDigests does not contain the pinned Redis digest".into());
        }
        let id = image["Id"].as_str().ok_or("image has no ID")?;
        if !id.strip_prefix("sha256:").is_some_and(valid_hex_id) {
            return Err("invalid image ID".into());
        }
        Ok(id.to_owned())
    }
}

/// Owns one Docker instance, accessible only on loopback.
pub struct RedisReference {
    container: ContainerGuard,
    address: SocketAddr,
    shared_runner: Option<String>,
}

impl RedisReference {
    /// Fails explicitly if Docker, image, digest, or versions do not match.
    pub fn start() -> Self {
        Self::start_mode(None)
    }

    /// Uses the network namespace of an isolated Linux runner, without publishing ports.
    /// The caller must run inside that runner and serialize execution to one reference at a time.
    #[allow(dead_code)] // The host reference also includes this shared module.
    pub fn start_shared(runner_id: &str) -> Self {
        assert!(
            valid_hex_id(runner_id),
            "runner requires a full Docker ID, not a name or prefix"
        );
        let runner = checked(
            docker(&["container", "inspect", runner_id]),
            "inspect shared runner",
        );
        inspect_runner(&runner.stdout, runner_id)
            .expect("Linux runner must have a private network without published ports");
        let address = SocketAddr::from(([127, 0, 0, 1], 6379));
        match TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
            Ok(_) => {
                panic!("runner loopback 6379 is already occupied; do not reuse another reference")
            }
            Err(error) => {
                panic!("could not confirm an available loopback port in the runner: {error}")
            }
        }
        Self::start_mode(Some(runner_id))
    }

    fn start_mode(runner_id: Option<&str>) -> Self {
        let config = ReferenceConfig::parse(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/releases/plan.json"
        )))
        .expect("invalid reference in releases/plan.json");
        let inspected = docker(&["image", "inspect", &config.image]);
        let inspected = inspected.unwrap_or_else(|error| {
            panic!("Docker unavailable: {error}. Install/start Docker before this test")
        });
        assert!(
            inspected.status.success(),
            "reference image unavailable; explicitly run: docker pull --platform {} {}\n{}",
            config.platform,
            config.image,
            String::from_utf8_lossy(&inspected.stderr)
        );
        let image_id = config
            .inspect_image(&inspected.stdout)
            .expect("reference image identity mismatch");

        let mut container = ContainerGuard::new();
        let cidfile = container.cidfile.to_str().expect("UTF-8 cidfile path");
        let network = runner_id.map(|id| format!("container:{id}"));
        let mut arguments = vec![
            "run",
            "--pull",
            "never",
            "--platform",
            &config.platform,
            "--detach",
            "--rm",
            "--cidfile",
            cidfile,
            "--user",
            "999:999",
            "--read-only",
            "--tmpfs",
            "/data",
            "--tmpfs",
            "/tmp",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
        ];
        if let Some(network) = &network {
            arguments.extend_from_slice(&["--network", network]);
        } else {
            arguments.extend_from_slice(&["--publish", "127.0.0.1::6379"]);
        }
        arguments.extend_from_slice(&[
            &config.image,
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
        ]);
        if runner_id.is_some() {
            arguments.extend_from_slice(&["--bind", "127.0.0.1"]);
        }
        let started = docker(&arguments);
        // The dedicated cidfile allows ID recovery even after the Docker client times out.
        container.capture_id();
        let started = checked(started, "start disposable Redis");
        let id = container
            .id
            .as_deref()
            .expect("Docker did not write a valid ID");
        assert_eq!(
            String::from_utf8_lossy(&started.stdout).trim(),
            id,
            "ID returned by Docker differs from the dedicated cidfile"
        );
        let inspected = checked(docker(&["container", "inspect", id]), "inspect Redis");
        let address = match runner_id {
            Some(runner) => inspect_shared_container(&inspected.stdout, id, &image_id, runner),
            None => inspect_container(&inspected.stdout, id, &image_id),
        }
        .expect("container must use the verified image and an ephemeral loopback port");

        let server = checked(
            docker(&["exec", id, "redis-server", "--version"]),
            "query Redis version",
        );
        let cli = checked(
            docker(&["exec", id, "redis-cli", "--version"]),
            "query redis-cli version",
        );
        assert!(
            server_version_matches(&server.stdout, &config.redis_version),
            "Redis version mismatch: {}",
            String::from_utf8_lossy(&server.stdout)
        );
        assert!(
            cli_version_matches(&cli.stdout, &config.redis_cli_version),
            "redis-cli version mismatch: {}",
            String::from_utf8_lossy(&cli.stdout)
        );
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            match TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
                Ok(_) => break,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "Redis did not become ready: {error}"
                    );
                    thread::sleep(Duration::from_millis(25));
                }
            }
        }
        eprintln!(
            "reference Redis={} redis-cli={} platform={} image={} image_id={} container={} address={}",
            config.redis_version,
            config.redis_cli_version,
            config.platform,
            config.image,
            image_id,
            id,
            address
        );
        let result = Self {
            container,
            address,
            shared_runner: runner_id.map(str::to_owned),
        };
        assert_eq!(
            result.cli(&["PING"]),
            b"PONG\n",
            "Redis must respond over the protocol"
        );
        let id = result.container.id.as_deref().expect("active container");
        let inspected = checked(
            docker(&["container", "inspect", id]),
            "confirm Redis after readiness",
        );
        match runner_id {
            Some(runner) => inspect_shared_container(&inspected.stdout, id, &image_id, runner),
            None => inspect_container(&inspected.stdout, id, &image_id),
        }
        .expect("reference must still be running and isolated after PING");
        result
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Runs redis-cli in RESP2 mode, without a TTY and with raw output, in the isolated container.
    pub fn cli(&self, args: &[&str]) -> Vec<u8> {
        let id = self.container.id.as_deref().expect("active container");
        let mut command = vec!["exec", id, "redis-cli", "-2", "--raw"];
        command.extend_from_slice(args);
        checked(docker(&command), "run redis-cli").stdout
    }

    /// Queries Sider in the same network namespace, only over loopback.
    #[allow(dead_code)] // Used by the differential suite, not the host reference.
    pub fn cli_at(&self, address: SocketAddr, args: &[&str]) -> Vec<u8> {
        validate_cli_address(self.shared_runner.as_deref(), address)
            .expect("redis-cli outside Redis requires shared mode and loopback");
        assert!(
            args.first().is_some_and(|arg| !arg.starts_with('-')),
            "first argument must be a command, not a CLI option"
        );
        let id = self.container.id.as_deref().expect("active container");
        let host = address.ip().to_string();
        let port = address.port().to_string();
        let mut command = vec![
            "exec",
            id,
            "redis-cli",
            "-2",
            "--raw",
            "-h",
            &host,
            "-p",
            &port,
        ];
        command.extend_from_slice(args);
        checked(docker(&command), "run redis-cli against shared Sider").stdout
    }

    /// Confirms removal on success; Drop also cleans up on test failures.
    pub fn finish(mut self) {
        let id = self.container.id.as_deref().expect("active container");
        checked(
            docker(&["rm", "--force", "--volumes", id]),
            "remove disposable Redis",
        );
        let remaining = checked(
            docker(&[
                "container",
                "ls",
                "--all",
                "--quiet",
                "--no-trunc",
                "--filter",
                &format!("id={id}"),
            ]),
            "confirm Redis removal",
        );
        assert!(
            remaining.stdout.iter().all(u8::is_ascii_whitespace),
            "Redis container still appears after removal"
        );
        self.container.removed = true;
        eprintln!("Redis container removed: {id}");
        let cidfile = self.container.cidfile.clone();
        let directory = self.container.directory.clone();
        drop(self);
        assert!(!cidfile.exists(), "owned cidfile must be removed");
        assert!(!directory.exists(), "owned directory must be removed");
    }
}

struct ContainerGuard {
    directory: PathBuf,
    cidfile: PathBuf,
    id: Option<String>,
    removed: bool,
}

impl ContainerGuard {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch");
        let directory = std::env::temp_dir().join(format!(
            "sider-redis-reference-{}-{}-{}",
            std::process::id(),
            nonce.as_nanos(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("create a dedicated temporary directory");
        Self {
            cidfile: directory.join("container.cid"),
            directory,
            id: None,
            removed: false,
        }
    }

    fn capture_id(&mut self) {
        if self.id.is_none()
            && let Ok(value) = fs::read_to_string(&self.cidfile)
            && valid_hex_id(value.trim())
        {
            self.id = Some(value.trim().to_owned());
        }
    }
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        if !self.removed {
            self.capture_id();
            if let Some(id) = &self.id {
                let _ = docker(&["rm", "--force", "--volumes", id]);
            }
        }
        // Only the two paths created by this guard, without recursive cleanup.
        let _ = fs::remove_file(&self.cidfile);
        let _ = fs::remove_dir(&self.directory);
    }
}

fn valid_hex_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn single_inspection(value: &Value) -> Result<&Value, String> {
    let values = value
        .as_array()
        .ok_or("Docker inspection must be an array")?;
    if values.len() != 1 {
        return Err("Docker inspection must contain exactly one object".into());
    }
    Ok(&values[0])
}

fn inspect_container(output: &[u8], id: &str, image_id: &str) -> Result<SocketAddr, String> {
    let value: Value = serde_json::from_slice(output).map_err(|e| e.to_string())?;
    let container = single_inspection(&value)?;
    if !valid_hex_id(id) || container["Id"] != id || container["Image"] != image_id {
        return Err("container or image identity mismatch".into());
    }
    if container["State"]["Running"] != true {
        return Err("Redis container is not running".into());
    }
    let ports = container["NetworkSettings"]["Ports"]["6379/tcp"]
        .as_array()
        .ok_or("Redis has no published TCP port")?;
    if ports.len() != 1 || ports[0]["HostIp"] != "127.0.0.1" {
        return Err("Redis port must be published exclusively on 127.0.0.1".into());
    }
    let port: u16 = ports[0]["HostPort"]
        .as_str()
        .ok_or("missing published port")?
        .parse()
        .map_err(|_| "invalid published port")?;
    if port == 0 {
        return Err("published port must not be zero".into());
    }
    Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

fn no_published_ports(value: &Value) -> bool {
    value.is_null()
        || value.as_object().is_some_and(|ports| {
            ports.values().all(|bindings| {
                bindings.is_null() || bindings.as_array().is_some_and(Vec::is_empty)
            })
        })
}

fn inspect_runner(output: &[u8], id: &str) -> Result<(), String> {
    let value: Value = serde_json::from_slice(output).map_err(|e| e.to_string())?;
    let runner = single_inspection(&value)?;
    if !valid_hex_id(id)
        || runner["Id"] != id
        || runner["State"]["Running"] != true
        || runner["Platform"] != "linux"
    {
        return Err("runner must be an active Linux container with the matching full ID".into());
    }
    let network = runner["HostConfig"]["NetworkMode"]
        .as_str()
        .ok_or("runner has no network mode")?;
    if network.is_empty()
        || matches!(network, "host" | "none")
        || network.starts_with("container:")
        || !no_published_ports(&runner["HostConfig"]["PortBindings"])
        || !no_published_ports(&runner["NetworkSettings"]["Ports"])
    {
        return Err("runner requires a private network namespace without published ports".into());
    }
    Ok(())
}

fn inspect_shared_container(
    output: &[u8],
    id: &str,
    image_id: &str,
    runner_id: &str,
) -> Result<SocketAddr, String> {
    let value: Value = serde_json::from_slice(output).map_err(|e| e.to_string())?;
    let container = single_inspection(&value)?;
    if !valid_hex_id(id)
        || !valid_hex_id(runner_id)
        || container["Id"] != id
        || container["Image"] != image_id
        || container["State"]["Running"] != true
    {
        return Err("shared Redis identity/state mismatch".into());
    }
    if container["HostConfig"]["NetworkMode"] != format!("container:{runner_id}")
        || !no_published_ports(&container["HostConfig"]["PortBindings"])
        || !no_published_ports(&container["NetworkSettings"]["Ports"])
    {
        return Err(
            "Redis must share only the specified runner network, without publishing ports".into(),
        );
    }
    Ok(SocketAddr::from(([127, 0, 0, 1], 6379)))
}

fn validate_cli_address(runner: Option<&str>, address: SocketAddr) -> Result<(), String> {
    if !runner.is_some_and(valid_hex_id) || !address.ip().is_loopback() || address.port() == 0 {
        return Err("redis-cli accesses only loopback in the verified shared runner".into());
    }
    Ok(())
}

fn server_version_matches(output: &[u8], expected: &str) -> bool {
    let Ok(output) = std::str::from_utf8(output) else {
        return false;
    };
    let words: Vec<_> = output.split_whitespace().collect();
    words.starts_with(&["Redis", "server"])
        && words
            .iter()
            .filter_map(|word| word.strip_prefix("v="))
            .collect::<Vec<_>>()
            == [expected]
}

fn cli_version_matches(output: &[u8], expected: &str) -> bool {
    let Ok(output) = std::str::from_utf8(output) else {
        return false;
    };
    output.split_whitespace().collect::<Vec<_>>() == ["redis-cli", expected]
}

fn checked(output: Result<Output, String>, operation: &str) -> Output {
    let output = output.unwrap_or_else(|error| panic!("{operation}: {error}"));
    assert!(
        output.status.success(),
        "{operation}: Docker returned {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn capture(mut stream: impl Read) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Ok(output);
        }
        let retain = count.min(OUTPUT_LIMIT.saturating_sub(output.len()));
        output.extend_from_slice(&buffer[..retain]);
        // Continues draining after the limit, without unbounded allocation or deadlock.
    }
}

fn docker(args: &[&str]) -> Result<Output, String> {
    let deadline = Instant::now() + COMMAND_TIMEOUT;
    let mut child = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("could not execute Docker: {error}"))?;
    let stdout = child.stdout.take().expect("stdout configured as a pipe");
    let stderr = child.stderr.take().expect("stderr configured as a pipe");
    let (stdout_sender, stdout_receiver) = mpsc::sync_channel(1);
    let (stderr_sender, stderr_receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = stdout_sender.send(capture(stdout));
    });
    thread::spawn(move || {
        let _ = stderr_sender.send(capture(stderr));
    });
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(
                Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())),
            ),
            Ok(None) => break Err("Docker command exceeded 30 seconds".to_owned()),
            Err(error) => break Err(format!("failed to wait for Docker: {error}")),
        }
    };
    let status = match status {
        Ok(status) => status,
        Err(error) => {
            let _ = child.kill();
            let reap_deadline = Instant::now() + Duration::from_millis(250);
            while matches!(child.try_wait(), Ok(None)) && Instant::now() < reap_deadline {
                thread::sleep(
                    Duration::from_millis(10)
                        .min(reap_deadline.saturating_duration_since(Instant::now())),
                );
            }
            // Does not wait for readers: another process may have inherited the pipes.
            return Err(error);
        }
    };
    let stdout = receive_capture(stdout_receiver, "stdout", deadline)?;
    let stderr = receive_capture(stderr_receiver, "stderr", deadline)?;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn receive_capture(
    receiver: mpsc::Receiver<io::Result<Vec<u8>>>,
    stream: &str,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    receiver
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|error| format!("read {stream} within Docker deadline: {error}"))?
        .map_err(|error| format!("read {stream}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn manifest() -> Value {
        serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/releases/plan.json"
        )))
        .unwrap()
    }

    #[test]
    fn manifest_requires_explicit_versions_pinned_digest_and_platform() {
        ReferenceConfig::parse(&manifest().to_string()).unwrap();
        for (field, invalid) in [
            ("image", "redis:latest"),
            ("image", "redis:8.10.1@sha256:1234"),
            ("platform", "linux/arm64"),
            ("redis_version", "8.10.10"),
            ("redis_cli_version", "8.10.1-extra"),
        ] {
            let mut value = manifest();
            value["reference"][field] = json!(invalid);
            assert!(
                ReferenceConfig::parse(&value.to_string()).is_err(),
                "{field}: {invalid}"
            );
        }
        let mut value = manifest();
        value["reference"]["redis_cli_version"] = Value::Null;
        assert!(ReferenceConfig::parse(&value.to_string()).is_err());
    }

    #[test]
    fn image_inspection_rejects_wrong_digest_platform_and_id() {
        let config = ReferenceConfig::parse(&manifest().to_string()).unwrap();
        let image_id = format!("sha256:{}", "a".repeat(64));
        let inspection = json!([{
            "Id": image_id, "Os": "linux", "Architecture": "amd64",
            "RepoDigests": [format!("redis@{}", config.digest)]
        }]);
        assert_eq!(
            config
                .inspect_image(&serde_json::to_vec(&inspection).unwrap())
                .unwrap(),
            image_id
        );
        for (field, invalid) in [
            (
                "RepoDigests",
                json!([format!("redis@sha256:{}", "b".repeat(64))]),
            ),
            ("Os", json!("windows")),
            ("Architecture", json!("arm64")),
            ("Id", json!("short-id")),
        ] {
            let mut value = inspection.clone();
            value[0][field] = invalid;
            assert!(
                config
                    .inspect_image(&serde_json::to_vec(&value).unwrap())
                    .is_err()
            );
        }
    }

    #[test]
    fn container_inspection_requires_owned_id_image_and_loopback_port() {
        let id = "a".repeat(64);
        let image = format!("sha256:{}", "b".repeat(64));
        let inspection = json!([{
            "Id": id, "Image": image, "State": {"Running": true},
            "NetworkSettings": {"Ports": {"6379/tcp": [{"HostIp": "127.0.0.1", "HostPort": "45678"}]}}
        }]);
        let parse =
            |value: &Value| inspect_container(&serde_json::to_vec(value).unwrap(), &id, &image);
        assert_eq!(
            parse(&inspection).unwrap(),
            "127.0.0.1:45678".parse().unwrap()
        );
        for (pointer, invalid) in [
            ("/0/Id", json!("c".repeat(64))),
            ("/0/Image", json!("another-image")),
            ("/0/State/Running", json!(false)),
            (
                "/0/NetworkSettings/Ports/6379~1tcp/0/HostIp",
                json!("0.0.0.0"),
            ),
            ("/0/NetworkSettings/Ports/6379~1tcp/0/HostPort", json!("0")),
            (
                "/0/NetworkSettings/Ports/6379~1tcp/0/HostPort",
                json!("65536"),
            ),
        ] {
            let mut value = inspection.clone();
            *value.pointer_mut(pointer).unwrap() = invalid;
            assert!(parse(&value).is_err(), "{pointer}");
        }
    }

    #[test]
    fn version_comparisons_are_exact_tokens() {
        assert!(server_version_matches(
            b"Redis server v=8.10.1 sha=00000000:0 malloc=jemalloc bits=64\n",
            "8.10.1"
        ));
        assert!(!server_version_matches(
            b"Redis server v=8.10.10\n",
            "8.10.1"
        ));
        assert!(!server_version_matches(
            b"Redis server v=8.10.1-rc.1\n",
            "8.10.1"
        ));
        assert!(!server_version_matches(
            b"Redis server v=8.10.1 v=8.10.2\n",
            "8.10.1"
        ));
        assert!(cli_version_matches(b"redis-cli 8.10.1\n", "8.10.1"));
        assert!(!cli_version_matches(b"redis-cli 8.10.10\n", "8.10.1"));
        assert!(!cli_version_matches(b"redis-cli 8.10.1 extra\n", "8.10.1"));
    }

    #[test]
    fn container_id_never_accepts_names_prefixes_or_options() {
        assert!(valid_hex_id(&"f".repeat(64)));
        for invalid in ["redis", "--all", "abc123", &"f".repeat(65), &"G".repeat(64)] {
            assert!(!valid_hex_id(invalid));
        }
    }

    #[test]
    fn shared_runner_requires_full_identity_linux_private_network_and_no_ports() {
        let id = "a".repeat(64);
        let inspection = json!([{
            "Id": id, "Platform": "linux", "State": {"Running": true},
            "HostConfig": {"NetworkMode":"bridge", "PortBindings":{}},
            "NetworkSettings": {"Ports":{}}
        }]);
        let parse = |value: &Value| inspect_runner(&serde_json::to_vec(value).unwrap(), &id);
        parse(&inspection).unwrap();
        for (pointer, invalid) in [
            ("/0/Id", json!("b".repeat(64))),
            ("/0/Platform", json!("windows")),
            ("/0/State/Running", json!(false)),
            ("/0/HostConfig/NetworkMode", json!("host")),
            ("/0/HostConfig/NetworkMode", json!("none")),
            (
                "/0/HostConfig/NetworkMode",
                json!(format!("container:{id}")),
            ),
            (
                "/0/HostConfig/PortBindings",
                json!({"6379/tcp":[{"HostIp":"127.0.0.1","HostPort":"6379"}]}),
            ),
            (
                "/0/NetworkSettings/Ports",
                json!({"6379/tcp":[{"HostIp":"0.0.0.0","HostPort":"6379"}]}),
            ),
        ] {
            let mut value = inspection.clone();
            *value.pointer_mut(pointer).unwrap() = invalid;
            assert!(parse(&value).is_err(), "{pointer}");
        }
        assert!(inspect_runner(&serde_json::to_vec(&inspection).unwrap(), "short").is_err());
    }

    #[test]
    fn shared_redis_requires_exact_runner_and_never_published_ports() {
        let id = "a".repeat(64);
        let runner = "b".repeat(64);
        let image = format!("sha256:{}", "c".repeat(64));
        let inspection = json!([{
            "Id":id,"Image":image,"State":{"Running":true},
            "HostConfig":{"NetworkMode":format!("container:{runner}"),"PortBindings":{}},
            "NetworkSettings":{"Ports":{}}
        }]);
        let parse = |value: &Value| {
            inspect_shared_container(&serde_json::to_vec(value).unwrap(), &id, &image, &runner)
        };
        assert_eq!(
            parse(&inspection).unwrap(),
            "127.0.0.1:6379".parse().unwrap()
        );
        for (pointer, invalid) in [
            ("/0/Id", json!("d".repeat(64))),
            ("/0/Image", json!("other")),
            ("/0/State/Running", json!(false)),
            ("/0/HostConfig/NetworkMode", json!("host")),
            (
                "/0/HostConfig/NetworkMode",
                json!(format!("container:{}", "e".repeat(64))),
            ),
            (
                "/0/HostConfig/PortBindings",
                json!({"6379/tcp":[{"HostIp":"127.0.0.1","HostPort":"12345"}]}),
            ),
            (
                "/0/NetworkSettings/Ports",
                json!({"6379/tcp":[{"HostIp":"127.0.0.1","HostPort":"12345"}]}),
            ),
        ] {
            let mut value = inspection.clone();
            *value.pointer_mut(pointer).unwrap() = invalid;
            assert!(parse(&value).is_err(), "{pointer}");
        }
    }

    #[test]
    fn external_cli_requires_shared_mode_loopback_and_nonzero_port() {
        let id = "a".repeat(64);
        for address in ["127.0.0.1:12345", "[::1]:12345"] {
            validate_cli_address(Some(&id), address.parse().unwrap()).unwrap();
        }
        for (runner, address) in [
            (None, "127.0.0.1:12345"),
            (Some("short"), "127.0.0.1:12345"),
            (Some(id.as_str()), "0.0.0.0:12345"),
            (Some(id.as_str()), "192.0.2.1:12345"),
            (Some(id.as_str()), "127.0.0.1:0"),
        ] {
            assert!(validate_cli_address(runner, address.parse().unwrap()).is_err());
        }
    }

    #[test]
    fn capture_drains_without_retaining_unbounded_output() {
        let input = vec![b'a'; OUTPUT_LIMIT + 1024];
        let mut reader = input.as_slice();
        assert_eq!(capture(&mut reader).unwrap().len(), OUTPUT_LIMIT);
        assert!(reader.is_empty());
    }

    #[test]
    fn capture_deadline_does_not_wait_for_an_open_sender() {
        let (_sender, receiver) = mpsc::sync_channel(1);
        assert!(receive_capture(receiver, "stdout", Instant::now()).is_err());
    }

    #[test]
    fn capture_receives_ready_output_and_reports_reader_errors() {
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(Ok(b"output".to_vec())).unwrap();
        assert_eq!(
            receive_capture(receiver, "stdout", Instant::now()).unwrap(),
            b"output"
        );

        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(Err(io::Error::other("broken pipe"))).unwrap();
        assert!(
            receive_capture(receiver, "stderr", Instant::now())
                .unwrap_err()
                .contains("broken pipe")
        );
    }
}
