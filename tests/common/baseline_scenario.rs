//! Dados e operações reais sobre executáveis extraídos; nenhuma alteração global de ambiente.

use std::fs::{self, File};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sider::persistence::{DurableLayout, format};
use sider::storage::Mutation;

use super::super::{process, wire};
use super::{
    manifest::{self, Result},
    package,
};
use wire::Response;

pub const DATASET: usize = 4 * 1024 * 1024;
pub const RECORD: usize = 65536;

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub struct Scratch(pub PathBuf);
impl Scratch {
    pub fn new() -> Result<Self> {
        let directory = std::env::temp_dir().join(format!(
            "sider-baseline-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).map_err(|error| error.to_string())?;
        Ok(Self(directory))
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub fn server_command(
    package: &Path,
    data: &Path,
    shards: u32,
    control: &Path,
    upstream: Option<SocketAddr>,
) -> Command {
    let mut command = Command::new(package.join(package::filename("sider")));
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
        .env("SIDER_ADDR", "127.0.0.1:0")
        .env("SIDER_READY_FILE", control.join("resp.json"))
        .env("SIDER_REPLICATION_ADDR", "127.0.0.1:0")
        .env(
            "SIDER_REPLICATION_READY_FILE",
            control.join("internal.json"),
        )
        .env("SIDER_AOF_DIR", data)
        .env("SIDER_AOF_SYNC", "always")
        .env("SIDER_AOF_COMPACT_AFTER_BYTES", "0")
        .env("SIDER_AOF_MAX_RECORD_BYTES", RECORD.to_string())
        .env("SIDER_MAX_DATASET_BYTES", DATASET.to_string())
        .env("SIDER_REPLICATION_BACKLOG_BYTES", "131072")
        .env("SIDER_SHARDS", shards.to_string());
    if let Some(upstream) = upstream {
        command.env("SIDER_REPLICA_OF", upstream.to_string());
    }
    command
}

pub struct Node {
    child: Option<process::OwnedChild>,
    control: Scratch,
    pub address: SocketAddr,
    pub internal: SocketAddr,
}
impl Node {
    pub fn start(
        package: &Path,
        data: &Path,
        shards: u32,
        upstream: Option<SocketAddr>,
    ) -> Result<Self> {
        let control = Scratch::new()?;
        let mut child = process::OwnedChild::spawn(&mut server_command(
            package, data, shards, &control.0, upstream,
        ))?;
        let deadline = Instant::now() + package::TIMEOUT;
        while !control.0.join("resp.json").is_file() {
            if child.assert_alive().is_err() {
                let output = child.wait(package::TIMEOUT)?;
                return Err(format!(
                    "servidor recusou inicialização: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            if Instant::now() >= deadline {
                return Err("prontidão excedeu prazo".into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let read = |name| -> Result<SocketAddr> {
            let value: Value = serde_json::from_slice(
                &fs::read(control.0.join(name)).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            if value.as_object().is_none_or(|fields| fields.len() != 3)
                || value["pid"] != child.id()
                || value["host"] != "127.0.0.1"
            {
                return Err("prontidão não pertence ao filho iniciado".into());
            }
            let port = value["port"]
                .as_u64()
                .and_then(|port| u16::try_from(port).ok())
                .filter(|port| *port != 0)
                .ok_or("porta inválida")?;
            Ok(SocketAddr::from(([127, 0, 0, 1], port)))
        };
        let address = read("resp.json")?;
        let internal = read("internal.json")?;
        let node = Self {
            child: Some(child),
            control,
            address,
            internal,
        };
        expect(node.call(&[b"PING"])?, Response::Simple(b"PONG".to_vec()))?;
        Ok(node)
    }
    pub fn connect(&self) -> Result<TcpStream> {
        let stream = TcpStream::connect_timeout(&self.address, package::TIMEOUT)
            .map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(package::TIMEOUT))
            .map_err(|error| error.to_string())?;
        stream
            .set_write_timeout(Some(package::TIMEOUT))
            .map_err(|error| error.to_string())?;
        Ok(stream)
    }
    pub fn call(&self, args: &[&[u8]]) -> Result<Response> {
        call(&mut self.connect()?, args)
    }
    pub fn status(&self, package: &Path) -> Result<Value> {
        let output = package::run(
            Command::new(package.join(package::filename("sider-replica"))).args([
                "--addr",
                &self.internal.to_string(),
                "--status",
            ]),
        )?;
        serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
    }
    pub fn stop(&mut self) -> Result<Value> {
        let mut child = self.child.take().ok_or("processo já encerrado")?;
        #[cfg(unix)]
        let (output, method) = {
            package::run(
                Command::new("/bin/kill")
                    .arg("-TERM")
                    .arg(child.id().to_string()),
            )?;
            let output = child.wait(package::TIMEOUT)?;
            if !output.status.success()
                || self.control.0.join("resp.json").exists()
                || self.control.0.join("internal.json").exists()
            {
                return Err("SIGTERM não encerrou cooperativamente".into());
            }
            (output, "sigterm")
        };
        #[cfg(windows)]
        let (output, method) = (child.terminate(package::TIMEOUT)?, "owned_process_kill");
        #[cfg(windows)]
        for name in ["resp.json", "internal.json"] {
            let path = self.control.0.join(name);
            let value: Value =
                serde_json::from_slice(&fs::read(&path).map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?;
            if value["pid"] != child.id() {
                return Err("prontidão deixou de pertencer ao filho".into());
            }
            fs::remove_file(path).map_err(|error| error.to_string())?;
        }
        if TcpStream::connect_timeout(&self.address, Duration::from_millis(100)).is_ok() {
            return Err("listener permaneceu aberto após parada".into());
        }
        Ok(
            json!({"stopped_unix_ms":now(),"shutdown_method":method,"shutdown_exit_code":output.status.code()}),
        )
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.terminate(package::TIMEOUT);
        }
    }
}

pub fn call(stream: &mut TcpStream, args: &[&[u8]]) -> Result<Response> {
    stream
        .write_all(&wire::request(
            &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
        ))
        .map_err(|error| error.to_string())?;
    Ok(wire::read_response(stream)
        .map_err(|error| error.to_string())?
        .value)
}
fn expect(actual: Response, expected: Response) -> Result<()> {
    if actual != expected {
        return Err(format!(
            "resposta divergente: esperado {expected:?}, observado {actual:?}"
        ));
    }
    Ok(())
}
fn bulk(value: &[u8]) -> Response {
    Response::Bulk(Some(value.to_vec()))
}
fn array(values: &[&[u8]]) -> Response {
    Response::Array(Some(values.iter().map(|value| bulk(value)).collect()))
}
fn key(tag: &str, name: &str) -> Vec<u8> {
    format!("{tag}:{name}").into_bytes()
}

pub fn tags(shards: u32) -> Result<Vec<String>> {
    let layout = DurableLayout {
        shard_count: shards,
        routing_version: 1,
    };
    let mut tags = vec![String::new(); shards as usize];
    for index in 0..10000 {
        let tag = format!("{{r10-{index}}}");
        let shard = layout
            .shard_for(tag.as_bytes())
            .map_err(|error| error.to_string())?;
        if tags[shard].is_empty() {
            tags[shard] = tag;
        }
        if tags.iter().all(|tag| !tag.is_empty()) {
            return Ok(tags);
        }
    }
    Err("não foi possível distribuir a fixture entre shards".into())
}

pub fn seed(node: &Node, tags: &[String], long_ms: u64) -> Result<()> {
    let mut connection = node.connect()?;
    for tag in tags {
        expect(
            call(&mut connection, &[b"MULTI"])?,
            Response::Simple(b"OK".to_vec()),
        )?;
        let string = key(tag, "string");
        let hash = key(tag, "hash");
        let list = key(tag, "list");
        let set = key(tag, "set");
        let sorted = key(tag, "sorted");
        let a = key(tag, "tx-a");
        let b = key(tag, "tx-b");
        for command in [
            vec![b"SET".as_slice(), &string, b"\0\xffR10"],
            vec![b"HSET", &hash, b"\xff", b"\0", b"f", b"value"],
            vec![b"RPUSH", &list, b"\xff", b"", b"\0"],
            vec![b"SADD", &set, b"\xff", b"", b"\0"],
            vec![
                b"ZADD", &sorted, b"-inf", b"\xff", b"1e-7", b"", b"1e20", b"\0",
            ],
            vec![b"SET", &a, b"same-transaction"],
            vec![b"HSET", &string, b"wrong", b"type"],
            vec![b"SET", &b, b"same-transaction"],
        ] {
            expect(
                call(&mut connection, &command)?,
                Response::Simple(b"QUEUED".to_vec()),
            )?;
        }
        let Response::Array(Some(values)) = call(&mut connection, &[b"EXEC"])? else {
            return Err("EXEC não retornou array".into());
        };
        if values.len() != 8
            || !matches!(&values[6], Response::Error(error) if error.starts_with(b"WRONGTYPE"))
            || values[0] != Response::Simple(b"OK".to_vec())
            || values[7] != Response::Simple(b"OK".to_vec())
        {
            return Err("EXEC não preservou erro individual e escrita posterior".into());
        }
    }
    expect(
        node.call(&[
            b"SET",
            &key(&tags[0], "long"),
            b"long",
            b"PX",
            long_ms.to_string().as_bytes(),
        ])?,
        Response::Simple(b"OK".to_vec()),
    )
}

/// Semeado depois das comparações de dados, imediatamente antes do export final.
pub fn seed_short_ttl(node: &Node, tags: &[String], short_ms: u64) -> Result<()> {
    expect(
        node.call(&[
            b"SET",
            &key(&tags[0], "short"),
            b"short",
            b"PX",
            short_ms.to_string().as_bytes(),
        ])?,
        Response::Simple(b"OK".to_vec()),
    )
}

/// Hash de respostas normalizadas por consultas com ordem definida, sem PTTL variável.
pub fn check_state(node: &Node, tags: &[String]) -> Result<(u64, String)> {
    let mut hash = Sha256::new();
    let mut comparisons = 0;
    for tag in tags {
        let string = key(tag, "string");
        let h = key(tag, "hash");
        let l = key(tag, "list");
        let s = key(tag, "set");
        let z = key(tag, "sorted");
        let a = key(tag, "tx-a");
        let b = key(tag, "tx-b");
        let checks = vec![
            (vec![b"GET".as_slice(), &string], bulk(b"\0\xffR10")),
            (vec![b"HGET", &h, b"\xff"], bulk(b"\0")),
            (vec![b"HGET", &h, b"f"], bulk(b"value")),
            (vec![b"HLEN", &h], Response::Integer(2)),
            (
                vec![b"LRANGE", &l, b"0", b"-1"],
                array(&[b"\xff", b"", b"\0"]),
            ),
            (vec![b"SCARD", &s], Response::Integer(3)),
            (vec![b"SISMEMBER", &s, b"\xff"], Response::Integer(1)),
            (vec![b"SISMEMBER", &s, b""], Response::Integer(1)),
            (vec![b"SISMEMBER", &s, b"\0"], Response::Integer(1)),
            (
                vec![b"ZRANGE", &z, b"0", b"-1", b"WITHSCORES"],
                array(&[b"\xff", b"-inf", b"", b"1e-7", b"\0", b"1e+20"]),
            ),
            (
                vec![b"MGET", &a, &b],
                array(&[b"same-transaction", b"same-transaction"]),
            ),
        ];
        for (arguments, expected) in checks {
            hash.update(wire::request(
                &arguments
                    .iter()
                    .map(|value| value.to_vec())
                    .collect::<Vec<_>>(),
            ));
            // A resposta esperada foi conferida; hash independente de formatação Debug.
            let mut stream = node.connect()?;
            stream
                .write_all(&wire::request(
                    &arguments
                        .iter()
                        .map(|value| value.to_vec())
                        .collect::<Vec<_>>(),
                ))
                .map_err(|error| error.to_string())?;
            let actual = wire::read_response(&mut stream).map_err(|error| error.to_string())?;
            expect(actual.value, expected)?;
            hash.update(actual.bytes);
            comparisons += 1;
        }
    }
    expect(node.call(&[b"GET", &key(&tags[0], "long")])?, bulk(b"long"))?;
    Ok((comparisons + 1, format!("{:x}", hash.finalize())))
}

pub fn export(package: &Path, node: &Node, destination: &Path, sha: &str) -> Result<Value> {
    let output = package::run(
        Command::new(package.join(package::filename("sider-backup")))
            .arg("export")
            .arg("--source")
            .arg(node.internal.to_string())
            .arg("--destination")
            .arg(destination)
            .arg("--source-sha")
            .arg(sha)
            .args([
                "--max-record-bytes",
                &RECORD.to_string(),
                "--max-snapshot-bytes",
                &DATASET.to_string(),
                "--max-dataset-bytes",
                &DATASET.to_string(),
            ]),
    )?;
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}
pub fn backup_action(
    package: &Path,
    action: &str,
    source: &Path,
    destination: Option<&Path>,
    shards: u32,
) -> Result<Value> {
    let mut command = Command::new(package.join(package::filename("sider-backup")));
    command.arg(action).arg("--source").arg(source).args([
        "--shards",
        &shards.to_string(),
        "--routing",
        "1",
        "--max-dataset-bytes",
        &DATASET.to_string(),
    ]);
    if let Some(destination) = destination {
        command.arg("--destination").arg(destination);
    }
    serde_json::from_slice(&package::run(&mut command)?.stdout).map_err(|error| error.to_string())
}

pub fn deadlines(backup: &Path) -> Result<Vec<Value>> {
    let mut file = File::open(backup.join("snapshot.aof")).map_err(|error| error.to_string())?;
    format::read_header_with_layout(&mut file).map_err(|error| error.to_string())?;
    let mut deadlines = Vec::new();
    loop {
        match format::read_record(&mut file, format::Limits::default())
            .map_err(|error| error.to_string())?
        {
            format::Next::Record(format::Record::Snapshot(Mutation::Put {
                key,
                expires_at_unix_ms: Some(deadline),
                ..
            })) => {
                deadlines.push(json!({"key":std::str::from_utf8(&key).map_err(|error|error.to_string())?,"unix_ms":deadline}));
            }
            format::Next::End => break,
            format::Next::IncompleteTail => return Err("backup truncado".into()),
            _ => {}
        }
    }
    deadlines.sort_unstable_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
    if deadlines.len() != 2 {
        return Err("deadlines da fixture ausentes".into());
    }
    Ok(deadlines)
}

pub fn check_ttl(node: &Node, deadlines: &[Value], short_expired: bool) -> Result<()> {
    for deadline in deadlines {
        let key = deadline["key"].as_str().ok_or("chave TTL ausente")?;
        let absolute = deadline["unix_ms"].as_i64().ok_or("deadline ausente")?;
        let before = now();
        let actual = node.call(&[b"PTTL", key.as_bytes()])?;
        let after = now();
        if key.ends_with(":short") && short_expired {
            expect(actual, Response::Integer(-2))?;
            expect(node.call(&[b"GET", key.as_bytes()])?, Response::Bulk(None))?;
        } else {
            let Response::Integer(ttl) = actual else {
                return Err("PTTL não retornou inteiro".into());
            };
            if ttl <= 0 || ttl < absolute - after - 50 || ttl > absolute - before + 50 {
                return Err("deadline absoluto não foi preservado".into());
            }
        }
    }
    Ok(())
}

pub fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir(destination).map_err(|error| error.to_string())?;
    for file in manifest::tree(source)? {
        let relative = manifest::safe_relative(file["path"].as_str().unwrap())?;
        let output = destination.join(&relative);
        fs::create_dir_all(output.parent().unwrap()).map_err(|error| error.to_string())?;
        fs::copy(source.join(relative), output).map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub fn aof_metadata(directory: &Path) -> Result<Value> {
    let files = manifest::tree(directory)?;
    let latest = files
        .iter()
        .rfind(|file| file["path"].as_str().unwrap().ends_with(".aof"))
        .ok_or("geração AOF ausente")?;
    let mut file = File::open(directory.join(latest["path"].as_str().unwrap()))
        .map_err(|error| error.to_string())?;
    let header = format::read_header_with_layout(&mut file).map_err(|error| error.to_string())?;
    let role = header.replication.ok_or("papel AOF ausente")?;
    let mut sequence = header.sequence;
    loop {
        match format::read_record(&mut file, format::Limits::default())
            .map_err(|error| error.to_string())?
        {
            format::Next::Record(format::Record::Batch { sequence: next, .. }) => {
                if sequence.checked_add(1) != Some(next) {
                    return Err("sequência AOF descontínua".into());
                }
                sequence = next;
            }
            format::Next::End => break,
            format::Next::IncompleteTail => return Err("AOF de origem truncada".into()),
            _ => {}
        }
    }
    Ok(
        json!({"header_version":header.format_version,"record_version":format::VERSION,"role":if role.role==sider::persistence::Role::Primary {"primary"}else{"replica"},"epoch":role.epoch.iter().map(|byte|format!("{byte:02x}")).collect::<String>(),"sequence":sequence}),
    )
}
