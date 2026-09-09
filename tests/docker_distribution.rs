//! Constrói a imagem a partir dos bytes selecionados e testa o arquivo após docker load.
#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/wire.rs"]
mod wire;

use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const TIMEOUT: Duration = Duration::from_secs(20);
const BUILD_TIMEOUT: Duration = Duration::from_secs(180);
const BASE: &str =
    "ubuntu:24.04@sha256:1e0a86e57d247923571b75e0aaf48a1449cf8c543d51fb3e07a4a7d7bfa79316";

struct Options {
    package: PathBuf,
    output: PathBuf,
    source_sha: String,
    binary_sha: String,
    migrator_sha: String,
    backup_sha: String,
    replica_sha: String,
    version: String,
    runner_id: Option<String>,
}

fn hex(value: String, count: usize) -> Result<String, String> {
    if value.len() != count
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "identidade exige {count} dígitos hexadecimais minúsculos"
        ));
    }
    Ok(value)
}

impl Options {
    fn read(mut lookup: impl FnMut(&str) -> Option<OsString>) -> Result<Self, String> {
        let runner_id = lookup("SIDER_DOCKER_RUNNER_ID")
            .map(|value| {
                value
                    .into_string()
                    .map_err(|_| "ID do runner precisa ser UTF-8".to_owned())
            })
            .transpose()?
            .map(|value| hex(value, 64))
            .transpose()?;
        let mut required = |name| lookup(name).ok_or_else(|| format!("{name} ausente"));
        let package = PathBuf::from(required("SIDER_DOCKER_PACKAGE_DIR")?);
        let output = PathBuf::from(required("SIDER_DOCKER_OUTPUT_DIR")?);
        if !package.is_absolute() || !output.is_absolute() {
            return Err("diretórios de pacote e saída precisam ser absolutos".into());
        }
        let mut text = |name| {
            required(name)?
                .into_string()
                .map_err(|_| format!("{name} precisa ser UTF-8"))
        };
        Ok(Self {
            package,
            output,
            source_sha: hex(text("SIDER_DOCKER_SOURCE_SHA")?, 40)?,
            binary_sha: hex(text("SIDER_DOCKER_BINARY_SHA256")?, 64)?,
            migrator_sha: hex(text("SIDER_DOCKER_MIGRATOR_SHA256")?, 64)?,
            backup_sha: hex(text("SIDER_DOCKER_BACKUP_SHA256")?, 64)?,
            replica_sha: hex(text("SIDER_DOCKER_REPLICA_SHA256")?, 64)?,
            version: env!("CARGO_PKG_VERSION").into(),
            runner_id,
        })
    }
}

fn regular(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
        return Err(format!(
            "arquivo regular não vazio obrigatório: {}",
            path.display()
        ));
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Err("pacote não admite symlinks".into());
    }
    if metadata.is_dir() {
        fs::create_dir(destination).map_err(|error| error.to_string())?;
        for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            copy_tree(&entry.path(), &destination.join(entry.file_name()))?;
        }
    } else {
        regular(source)?;
        fs::copy(source, destination).map_err(|error| error.to_string())?;
    }
    Ok(())
}

struct Runner {
    evidence: PathBuf,
    commands: Vec<Value>,
    image: String,
    image_owned: bool,
    volume: Option<String>,
    containers: Vec<String>,
    shared_runner: Option<String>,
}

impl Runner {
    fn run(&mut self, command: &mut Command, timeout: Duration) -> Result<Output, String> {
        let description = format!("{command:?}");
        let started = Instant::now();
        let output = process::run(command, timeout)?;
        self.commands.push(json!({
            "command": description, "seconds": started.elapsed().as_secs_f64(),
            "exit_code": output.status.code(),
            "stdout": String::from_utf8_lossy(&output.stdout),
            "stderr": String::from_utf8_lossy(&output.stderr),
        }));
        fs::write(
            self.evidence.join("commands.json"),
            serde_json::to_vec_pretty(&self.commands).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        Ok(output)
    }

    fn checked(&mut self, command: &mut Command, timeout: Duration) -> Result<String, String> {
        let output = self.run(command, timeout)?;
        if !output.status.success() {
            return Err(format!(
                "{command:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        String::from_utf8(output.stdout).map_err(|error| error.to_string())
    }

    fn docker(&mut self, args: &[&str]) -> Result<String, String> {
        self.checked(Command::new("docker").args(args), TIMEOUT)
    }

    fn inspect(&mut self, kind: &str, name: &str) -> Result<Value, String> {
        let value: Value = serde_json::from_str(&self.docker(&[kind, "inspect", name])?)
            .map_err(|error| error.to_string())?;
        value
            .as_array()
            .and_then(|items| items.first())
            .cloned()
            .ok_or("inspect vazio".into())
    }

    fn hash(&mut self, path: &Path) -> Result<String, String> {
        let result = self.checked(Command::new("sha256sum").arg("--").arg(path), TIMEOUT)?;
        hex(
            result
                .split_whitespace()
                .next()
                .ok_or("checksum vazio")?
                .to_owned(),
            64,
        )
    }

    fn start(&mut self, name: &str, volume: &str) -> Result<SocketAddr, String> {
        self.containers.push(name.to_owned());
        let image = self.image.clone();
        let mut arguments: Vec<String> = [
            "run",
            "--detach",
            "--name",
            name,
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--mount",
            &format!("type=volume,source={volume},destination=/var/lib/sider"),
            "--env",
            "SIDER_SHARDS=4",
            "--env",
            "SIDER_READY_FILE=/var/lib/sider/docker-ready.json",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        if let Some(id) = &self.shared_runner {
            arguments.extend([
                "--network".into(),
                format!("container:{id}"),
                "--env".into(),
                "SIDER_ADDR=127.0.0.1:0".into(),
            ]);
        } else {
            arguments.extend(["--publish".into(), "127.0.0.1::6379".into()]);
        }
        arguments.push(image);
        self.checked(Command::new("docker").args(&arguments), TIMEOUT)?;
        let deadline = Instant::now() + TIMEOUT;
        let address: SocketAddr = if self.shared_runner.is_some() {
            loop {
                let ready = self.run(
                    Command::new("docker").args([
                        "exec",
                        name,
                        "cat",
                        "/var/lib/sider/docker-ready.json",
                    ]),
                    TIMEOUT,
                )?;
                if ready.status.success() {
                    let ready: Value =
                        serde_json::from_slice(&ready.stdout).map_err(|error| error.to_string())?;
                    if ready["pid"] != 1 || ready["host"] != "127.0.0.1" {
                        return Err("prontidão não pertence ao PID 1 em loopback".into());
                    }
                    let port = ready["port"]
                        .as_u64()
                        .and_then(|value| u16::try_from(value).ok())
                        .filter(|value| *value > 0)
                        .ok_or("porta de prontidão inválida")?;
                    break SocketAddr::from(([127, 0, 0, 1], port));
                }
                if Instant::now() >= deadline
                    || self.inspect("container", name)?["State"]["Running"] != true
                {
                    return Err("prontidão da imagem ausente".into());
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        } else {
            self.docker(&["port", name, "6379/tcp"])?
                .trim()
                .parse()
                .map_err(|error| format!("porta Docker inválida: {error}"))?
        };
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err("Docker precisa publicar somente porta efêmera em loopback".into());
        }
        loop {
            if matches!(exchange(address, &[b"PING"]), Ok(wire::Response::Simple(value)) if value == b"PONG")
            {
                break;
            }
            let state = self.inspect("container", name)?;
            if state["State"]["Running"] != true || Instant::now() >= deadline {
                let logs = self.run(Command::new("docker").args(["logs", name]), TIMEOUT)?;
                return Err(format!(
                    "contêiner sem prontidão TCP: {}",
                    String::from_utf8_lossy(&logs.stderr)
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        if self.docker(&["exec", name, "id", "-u"])?.trim() != "10001" {
            return Err("servidor não executa com UID 10001".into());
        }
        let status = self.docker(&["exec", name, "cat", "/proc/1/status"])?;
        if !status.lines().any(|line| {
            line.starts_with("Uid:") && line.split_whitespace().skip(1).all(|uid| uid == "10001")
        }) {
            return Err("PID 1 não pertence ao usuário sem privilégio".into());
        }
        let executable = self.docker(&["exec", name, "readlink", "/proc/1/exe"])?;
        if executable.trim() != "/usr/local/bin/sider" {
            return Err("o servidor precisa ser o PID 1".into());
        }
        Ok(address)
    }

    fn stop(&mut self, name: &str, explicit_signal: bool) -> Result<(), String> {
        if explicit_signal {
            self.docker(&["kill", "--signal", "TERM", name])?;
        } else {
            self.docker(&["stop", "--timeout", "7", name])?;
        }
        if self.docker(&["wait", name])?.trim() != "0" {
            return Err("SIGTERM não produziu encerramento normal".into());
        }
        let observed = self.inspect("container", name)?;
        if observed["State"]["Running"] != false || observed["State"]["OOMKilled"] != false {
            return Err("contêiner continuou ativo ou foi encerrado por OOM".into());
        }
        let logs = self.run(Command::new("docker").args(["logs", name]), TIMEOUT)?;
        if !logs.status.success()
            || !String::from_utf8_lossy(&logs.stderr).contains("servidor encerrado")
        {
            return Err("log não confirma drenagem do servidor".into());
        }
        self.docker(&["rm", "--volumes", name])?;
        self.containers.retain(|item| item != name);
        Ok(())
    }
}

fn private_runner(observed: &Value, id: &str) -> Result<(), String> {
    let network = observed["HostConfig"]["NetworkMode"]
        .as_str()
        .ok_or("rede do runner ausente")?;
    let no_ports = |value: &Value| {
        value.is_null()
            || value.as_object().is_some_and(|ports| {
                ports.values().all(|bindings| {
                    bindings.is_null() || bindings.as_array().is_some_and(Vec::is_empty)
                })
            })
    };
    if observed["Id"] != id
        || observed["State"]["Running"] != true
        || observed["Platform"] != "linux"
        || network.is_empty()
        || matches!(network, "host" | "none")
        || network.starts_with("container:")
        || !no_ports(&observed["HostConfig"]["PortBindings"])
        || !no_ports(&observed["NetworkSettings"]["Ports"])
    {
        return Err(
            "runner deve ser Linux ativo com ID completo e rede privada sem portas publicadas"
                .into(),
        );
    }
    Ok(())
}

impl Drop for Runner {
    fn drop(&mut self) {
        for container in &self.containers {
            let _ = process::run(
                Command::new("docker").args(["rm", "--force", "--volumes", container]),
                TIMEOUT,
            );
        }
        if let Some(volume) = &self.volume {
            let _ = process::run(
                Command::new("docker").args(["volume", "rm", volume]),
                TIMEOUT,
            );
        }
        if self.image_owned {
            let _ = process::run(
                Command::new("docker").args(["image", "rm", &self.image]),
                TIMEOUT,
            );
        }
    }
}

fn exchange(address: SocketAddr, args: &[&[u8]]) -> Result<wire::Response, String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(200))
        .map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(TIMEOUT))
        .map_err(|error| error.to_string())?;
    stream
        .set_write_timeout(Some(TIMEOUT))
        .map_err(|error| error.to_string())?;
    stream
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .map_err(|error| error.to_string())?;
    wire::read_response(&mut stream)
        .map(|observed| observed.value)
        .map_err(|error| error.to_string())
}

fn check_reply(
    address: SocketAddr,
    args: &[&[u8]],
    expected: wire::Response,
) -> Result<(), String> {
    if exchange(address, args)? != expected {
        return Err("resposta TCP divergente no ensaio de distribuição".into());
    }
    Ok(())
}

fn exercise(options: Options) -> Result<Value, String> {
    if !cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        return Err("runner Docker exige Linux GNU x86_64 nativo".into());
    }
    let package_metadata =
        fs::symlink_metadata(&options.package).map_err(|error| error.to_string())?;
    if !package_metadata.is_dir() || package_metadata.file_type().is_symlink() {
        return Err("pacote precisa ser diretório real".into());
    }
    // create_dir recusa saída existente; todos os arquivos de ensaio ficam preservados.
    fs::create_dir(&options.output).map_err(|error| format!("saída precisa ser nova: {error}"))?;
    let context = options.output.join("context");
    fs::create_dir(&context).map_err(|error| error.to_string())?;
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos()
    );
    let mut runner = Runner {
        evidence: options.output.clone(),
        commands: Vec::new(),
        image: format!(
            "sider-private:{}-{}-{suffix}",
            options.version,
            &options.source_sha[..12]
        ),
        image_owned: false,
        volume: None,
        containers: Vec::new(),
        shared_runner: options.runner_id.clone(),
    };
    if let Some(id) = &options.runner_id {
        private_runner(&runner.inspect("container", id)?, id)?;
    }
    for (file, expected) in [
        ("sider", &options.binary_sha),
        ("sider-aof-migrate", &options.migrator_sha),
        ("sider-backup", &options.backup_sha),
        ("sider-replica", &options.replica_sha),
    ] {
        regular(&options.package.join(file))?;
        if &runner.hash(&options.package.join(file))? != expected {
            return Err(format!("hash do executável selecionado diverge: {file}"));
        }
    }
    for name in [
        "sider",
        "sider-aof-migrate",
        "sider-backup",
        "sider-replica",
        "README.md",
        "LICENSE",
        "licenses",
    ] {
        copy_tree(&options.package.join(name), &context.join(name))?;
    }
    for (name, expected) in [
        (
            "README.md",
            include_bytes!("../releases/README.md").as_slice(),
        ),
        ("LICENSE", include_bytes!("../LICENSE").as_slice()),
    ] {
        if fs::read(context.join(name)).map_err(|error| error.to_string())? != expected {
            return Err(format!("{name} do pacote diverge do checkout do runner"));
        }
    }
    fs::write(
        context.join("Dockerfile"),
        include_bytes!("../deploy/Dockerfile"),
    )
    .map_err(|error| error.to_string())?;
    let image = runner.image.clone();
    if runner
        .run(
            Command::new("docker").args(["image", "inspect", &image]),
            TIMEOUT,
        )?
        .status
        .success()
    {
        return Err("tag local já existe; não sobrescrever".into());
    }
    runner.image_owned = true;
    runner.checked(
        Command::new("docker")
            .args([
                "build",
                "--platform",
                "linux/amd64",
                "--network",
                "none",
                "--tag",
                &image,
                "--build-arg",
                &format!("SIDER_VERSION={}", options.version),
                "--build-arg",
                &format!("SIDER_SOURCE_SHA={}", options.source_sha),
                "--build-arg",
                &format!("SIDER_SHA256={}", options.binary_sha),
                "--build-arg",
                &format!("SIDER_MIGRATOR_SHA256={}", options.migrator_sha),
                "--build-arg",
                &format!("SIDER_BACKUP_SHA256={}", options.backup_sha),
                "--build-arg",
                &format!("SIDER_REPLICA_SHA256={}", options.replica_sha),
            ])
            .arg(&context),
        BUILD_TIMEOUT,
    )?;
    let original = runner.inspect("image", &image)?;
    let archive_name = format!("sider-v{}-linux-amd64-image.tar.gz", options.version);
    let tar = options
        .output
        .join(archive_name.strip_suffix(".gz").unwrap());
    runner.checked(
        Command::new("docker")
            .args(["save", "--output"])
            .arg(&tar)
            .arg(&image),
        BUILD_TIMEOUT,
    )?;
    runner.checked(
        Command::new("gzip").args(["-n", "-6", "--"]).arg(&tar),
        BUILD_TIMEOUT,
    )?;
    let archive = options.output.join(&archive_name);
    runner.checked(
        Command::new("gzip").args(["--test", "--"]).arg(&archive),
        BUILD_TIMEOUT,
    )?;
    let archive_sha = runner.hash(&archive)?;
    runner.docker(&["image", "rm", &image])?;
    runner.image_owned = false;
    if runner
        .run(
            Command::new("docker").args(["image", "inspect", &image]),
            TIMEOUT,
        )?
        .status
        .success()
    {
        return Err("tag permaneceu antes do load".into());
    }
    runner.image_owned = true;
    runner.checked(
        Command::new("docker")
            .args(["load", "--input"])
            .arg(&archive),
        BUILD_TIMEOUT,
    )?;
    let loaded = runner.inspect("image", &image)?;
    if loaded["Id"] != original["Id"]
        || loaded["Os"] != "linux"
        || loaded["Architecture"] != "amd64"
        || loaded["Config"]["User"] != "10001:10001"
        || loaded["Config"]["StopSignal"] != "SIGTERM"
        || loaded["Config"]["Labels"]["org.opencontainers.image.version"] != options.version
        || loaded["Config"]["Labels"]["org.opencontainers.image.revision"] != options.source_sha
        || loaded["Config"]["Labels"]["io.sider.backup.sha256"] != options.backup_sha
        || loaded["Config"]["Labels"]["io.sider.replica.sha256"] != options.replica_sha
    {
        return Err("identidade/configuração da imagem recarregada divergente".into());
    }
    let hashes = runner.docker(&[
        "run",
        "--rm",
        "--network",
        "none",
        "--entrypoint",
        "sha256sum",
        &image,
        "/usr/local/bin/sider",
        "/usr/local/bin/sider-aof-migrate",
        "/usr/local/bin/sider-backup",
        "/usr/local/bin/sider-replica",
    ])?;
    let expected_hashes = format!(
        "{}  /usr/local/bin/sider\n{}  /usr/local/bin/sider-aof-migrate\n{}  /usr/local/bin/sider-backup\n{}  /usr/local/bin/sider-replica\n",
        options.binary_sha, options.migrator_sha, options.backup_sha, options.replica_sha
    );
    if hashes != expected_hashes {
        return Err("imagem recarregada não contém os executáveis selecionados".into());
    }
    if runner
        .docker(&["run", "--rm", "--network", "none", &image, "--version"])?
        .trim()
        != format!("sider {}", options.version)
    {
        return Err("--version da imagem diverge".into());
    }
    runner.docker(&[
        "run",
        "--rm",
        "--network",
        "none",
        "--entrypoint",
        "/usr/local/bin/sider-aof-migrate",
        &image,
        "--help",
    ])?;
    if runner
        .docker(&[
            "run",
            "--rm",
            "--network",
            "none",
            "--entrypoint",
            "/usr/local/bin/sider-backup",
            &image,
            "--version",
        ])?
        .trim()
        != format!("sider-backup {}", options.version)
    {
        return Err("versão da CLI de backup da imagem diverge".into());
    }
    runner.docker(&[
        "run",
        "--rm",
        "--network",
        "none",
        "--entrypoint",
        "/usr/local/bin/sider-replica",
        &image,
        "--help",
    ])?;
    let invalid_name = format!("sider-invalid-{suffix}");
    runner.containers.push(invalid_name.clone());
    let invalid = runner.run(
        Command::new("docker").args([
            "run",
            "--name",
            &invalid_name,
            "--network",
            "none",
            "--env",
            "SIDER_SHARDS=0",
            &image,
        ]),
        TIMEOUT,
    )?;
    if invalid.status.code() != Some(1)
        || !String::from_utf8_lossy(&invalid.stderr).contains("erro:")
    {
        return Err("configuração inválida não produziu erro do produto".into());
    }
    runner.docker(&["rm", "--volumes", &invalid_name])?;
    runner.containers.clear();
    let volume = format!("sider-data-{suffix}");
    runner.docker(&["volume", "create", &volume])?;
    runner.volume = Some(volume.clone());
    let first = format!("sider-first-{suffix}");
    let address = runner.start(&first, &volume)?;
    let key = b"{r10}\0\xffkey";
    let value = b"value\r\n\0\xff";
    check_reply(
        address,
        &[b"SET", key, value],
        wire::Response::Simple(b"OK".to_vec()),
    )?;
    check_reply(
        address,
        &[b"SET", b"{r10}:ttl", b"deadline", b"PX", b"120000"],
        wire::Response::Simple(b"OK".to_vec()),
    )?;
    check_reply(
        address,
        &[b"GET", key],
        wire::Response::Bulk(Some(value.to_vec())),
    )?;
    runner.stop(&first, false)?;
    let second = format!("sider-second-{suffix}");
    let address = runner.start(&second, &volume)?;
    check_reply(
        address,
        &[b"GET", key],
        wire::Response::Bulk(Some(value.to_vec())),
    )?;
    check_reply(
        address,
        &[b"GET", b"{r10}:ttl"],
        wire::Response::Bulk(Some(b"deadline".to_vec())),
    )?;
    let ttl = exchange(address, &[b"PTTL", b"{r10}:ttl"])?;
    if !matches!(ttl, wire::Response::Integer(value) if value > 0 && value < 120000) {
        return Err("TTL não foi preservado como deadline absoluto".into());
    }
    runner.stop(&second, true)?;
    let size = fs::metadata(&archive)
        .map_err(|error| error.to_string())?
        .len();
    if size == 0 || size > 2 * 1024 * 1024 * 1024 || runner.hash(&archive)? != archive_sha {
        return Err("arquivo vazio, excessivo ou alterado durante o ensaio".into());
    }
    runner.docker(&["volume", "rm", &volume])?;
    runner.volume = None;
    runner.docker(&["image", "rm", &image])?;
    runner.image_owned = false;
    let report = json!({
        "schema_version": 1, "task": "R10-04", "artifact_version": options.version,
        "source_sha": options.source_sha, "target": "x86_64-unknown-linux-gnu", "base": BASE,
        "image_id": loaded["Id"], "image_tag": image,
        "network": if options.runner_id.is_some() { "private_runner_namespace" } else { "host_loopback_published_port" },
        "artifact": {"name": archive_name, "size": size, "sha256": archive_sha},
        "binaries": {"sider": options.binary_sha, "sider-aof-migrate": options.migrator_sha, "sider-backup": options.backup_sha, "sider-replica": options.replica_sha},
        "cases": 8, "scenarios": ["save_gzip_remove_load_identity", "exact_binary_hashes", "versions_and_operational_clis", "uid_and_pid1", "binary_tcp", "aof_volume_restart_ttl", "sigterm_drain", "invalid_configuration"],
        "status": "success", "source_provenance": "SHA declarado pelo empacotador; bytes conferidos contra hashes fornecidos, sem inferir SHA do nome do arquivo"
    });
    fs::write(
        options.output.join("docker-report.json"),
        serde_json::to_vec_pretty(&report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(report)
}

#[test]
#[ignore = "ensaio real exige pacote Linux extraído, hashes e diretório novo; não é recibo de release"]
fn exported_image_runs_after_load() {
    let report = exercise(Options::read(|name| std::env::var_os(name)).unwrap()).unwrap();
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[test]
#[ignore = "gate exige contexto exato e arquivo Docker realmente exportado/recarregado"]
fn release_docker_gate() {
    let context = gate_receipt::GateContext::from_env("docker").unwrap();
    let options = Options::read(|name| std::env::var_os(name)).unwrap();
    assert_eq!(options.source_sha, context.sha());
    assert_eq!(options.version, context.version());
    let started = Instant::now();
    let report = exercise(options).unwrap();
    context.publish(8, started.elapsed(), report).unwrap();
}

#[test]
fn options_reject_missing_relative_or_invalid_identity_without_docker() {
    assert!(Options::read(|_| None).is_err());
    let values = |name: &str| {
        Some(OsString::from(match name {
            "SIDER_DOCKER_PACKAGE_DIR" => "relative-package".into(),
            "SIDER_DOCKER_OUTPUT_DIR" => "/output".into(),
            "SIDER_DOCKER_SOURCE_SHA" => "a".repeat(40),
            _ => "b".repeat(64),
        }))
    };
    assert!(Options::read(values).is_err());
    assert!(hex("A".repeat(40), 40).is_err());
    assert!(hex("a".repeat(39), 40).is_err());
    assert!(hex("a".repeat(40), 40).is_ok());
}

#[test]
fn shared_network_rejects_host_published_or_mismatched_runner() {
    let id = "a".repeat(64);
    let mut observed = json!({"Id":id,"Platform":"linux","State":{"Running":true},"HostConfig":{"NetworkMode":"bridge","PortBindings":{}},"NetworkSettings":{"Ports":{}}});
    private_runner(&observed, &id).unwrap();
    assert!(private_runner(&observed, &"b".repeat(64)).is_err());
    observed["HostConfig"]["NetworkMode"] = json!("host");
    assert!(private_runner(&observed, &id).is_err());
    observed["HostConfig"]["NetworkMode"] = json!("bridge");
    observed["NetworkSettings"]["Ports"]["80/tcp"] = json!([{"HostPort":"12345"}]);
    assert!(private_runner(&observed, &id).is_err());
}
