//! Extração limitada do pacote, sem delegar caminhos de escrita ao tar.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::super::process;
use super::manifest::{self, Result};
use serde_json::{Value, json};

pub const TIMEOUT: Duration = Duration::from_secs(30);
pub const BINARIES: [&str; 4] = [
    "sider",
    "sider-backup",
    "sider-replica",
    "sider-aof-migrate",
];

fn tar() -> Command {
    let mut command = Command::new("tar");
    command.env_remove("TAR_OPTIONS").env_remove("GZIP");
    command
}

pub fn target() -> &'static str {
    if cfg!(all(windows, target_arch = "x86_64", target_env = "msvc")) {
        "x86_64-pc-windows-msvc"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        "x86_64-unknown-linux-gnu"
    } else {
        "unsupported"
    }
}
pub fn filename(binary: &str) -> String {
    format!("{binary}{}", if cfg!(windows) { ".exe" } else { "" })
}

pub fn run(command: &mut Command) -> Result<std::process::Output> {
    let output = process::run(command, TIMEOUT)?;
    if !output.status.success() {
        return Err(format!(
            "comando falhou: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output)
}

pub fn identities(root: &Path) -> Result<(String, String)> {
    let sha = String::from_utf8(
        run(Command::new("git")
            .current_dir(root)
            .args(["rev-parse", "HEAD"]))?
        .stdout,
    )
    .map_err(|error| error.to_string())?
    .trim()
    .to_owned();
    manifest::hex(&serde_json::Value::String(sha.clone()), 40)?;
    let status = run(Command::new("git").current_dir(root).args([
        "status",
        "--porcelain=v1",
        "--untracked-files=normal",
    ]))?;
    if !status.stdout.is_empty() {
        return Err("congelamento exige checkout limpo".into());
    }
    let toolchain = String::from_utf8(
        run(Command::new("rustc")
            .current_dir(root)
            .arg("--version")
            .arg("--verbose"))?
        .stdout,
    )
    .map_err(|error| error.to_string())?;
    Ok((sha, toolchain))
}

fn expected_files(checkout: &Path) -> Result<BTreeMap<String, Option<(u64, String)>>> {
    let mut expected = BTreeMap::new();
    for binary in BINARIES {
        expected.insert(filename(binary), None);
    }
    expected.insert(
        "README.md".into(),
        Some(manifest::digest(&checkout.join("releases/README.md"))?),
    );
    expected.insert(
        "LICENSE".into(),
        Some(manifest::digest(&checkout.join("LICENSE"))?),
    );
    for file in manifest::tree(&checkout.join("releases/licenses"))? {
        expected.insert(
            format!("licenses/{}", file["path"].as_str().unwrap()),
            Some((
                file["bytes"].as_u64().unwrap(),
                file["sha256"].as_str().unwrap().to_owned(),
            )),
        );
    }
    Ok(expected)
}

/// Cada membro é lido por stdout e gravado em arquivo novo após validar o caminho.
/// Links do arquivo compactado nunca são materializados no filesystem.
pub fn extract(
    archive: &Path,
    destination: &Path,
    version: &str,
    checkout: &Path,
) -> Result<PathBuf> {
    if !archive.is_absolute() || !destination.is_absolute() {
        return Err("pacote e destino exigem caminhos absolutos".into());
    }
    let archive_before = manifest::digest(archive)?;
    let expected = expected_files(checkout)?;
    let prefix = format!("sider-v{version}-{}", target());
    manifest::safe_relative(&prefix)?;
    let listing = String::from_utf8(run(tar().arg("-tf").arg(archive))?.stdout)
        .map_err(|error| error.to_string())?;
    let mut members = BTreeSet::new();
    for line in listing.lines() {
        let path = line.trim_end_matches('/');
        manifest::safe_relative(path)?;
        if path == prefix {
            continue;
        }
        let relative = path
            .strip_prefix(&format!("{prefix}/"))
            .ok_or("raiz do arquivo compactado diverge")?;
        if line.ends_with('/') {
            if !expected
                .keys()
                .any(|key| key.starts_with(&format!("{relative}/")))
            {
                return Err("diretório extra no pacote".into());
            }
        } else if !members.insert(relative.to_owned()) {
            return Err("membro duplicado no pacote".into());
        }
    }
    if members != expected.keys().cloned().collect() {
        return Err("pacote contém arquivos extras ou ausentes".into());
    }
    fs::create_dir(destination).map_err(|error| error.to_string())?;
    let mut remaining = 512 * 1024 * 1024u64;
    for relative in members {
        let output = destination.join(manifest::safe_relative(&relative)?);
        fs::create_dir_all(output.parent().unwrap()).map_err(|error| error.to_string())?;
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&output)
            .map_err(|error| error.to_string())?;
        stream_member(
            archive,
            &format!("{prefix}/{relative}"),
            file,
            remaining.min(256 * 1024 * 1024),
        )?;
        let actual = manifest::digest(&output)?;
        remaining = remaining
            .checked_sub(actual.0)
            .ok_or("expansão do pacote excedeu limite")?;
        if actual.0 == 0
            || expected[&relative]
                .as_ref()
                .is_some_and(|wanted| *wanted != actual)
        {
            return Err(format!("conteúdo extraído diverge: {relative}"));
        }
        #[cfg(unix)]
        if BINARIES.contains(&relative.as_str()) {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&output, fs::Permissions::from_mode(0o755))
                .map_err(|error| error.to_string())?;
        }
    }
    if archive_before != manifest::digest(archive)? {
        return Err("pacote mudou durante extração".into());
    }
    Ok(destination.to_owned())
}

struct Owned(Child);
impl Drop for Owned {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = self.0.kill();
        let deadline = Instant::now() + Duration::from_secs(1);
        while self.0.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn stream_member(archive: &Path, member: &str, mut output: File, limit: u64) -> Result<()> {
    let mut child = Owned(
        tar()
            .arg("-xOf")
            .arg(archive)
            .arg(member)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| error.to_string())?,
    );
    let mut stdout = child.0.stdout.take().unwrap();
    let (sender, completed) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut copy = || -> std::io::Result<()> {
            let mut size = 0u64;
            let mut buffer = [0; 65536];
            loop {
                let count = stdout.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                size += count as u64;
                if size > limit {
                    return Err(std::io::Error::other("membro excedeu limite de expansão"));
                }
                output.write_all(&buffer[..count])?;
            }
            output.sync_all()
        };
        let _ = sender.send(copy());
    });
    let deadline = Instant::now() + TIMEOUT;
    let status = loop {
        if let Some(status) = child.0.try_wait().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err("extração excedeu prazo".into());
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    completed
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    if !status.success() {
        return Err("tar recusou membro do pacote".into());
    }
    Ok(())
}

pub fn validate_binaries(directory: &Path, version: &str) -> Result<()> {
    for (name, argument, expected) in [
        ("sider", "--version", Some(format!("sider {version}\n"))),
        (
            "sider-backup",
            "--version",
            Some(format!("sider-backup {version}\n")),
        ),
        ("sider-replica", "--help", None),
        ("sider-aof-migrate", "--help", None),
    ] {
        let binary = directory.join(filename(name));
        if manifest::digest(&binary)?.0 == 0 {
            return Err("executável vazio".into());
        }
        let output = run(Command::new(binary).arg(argument))?;
        if expected
            .as_ref()
            .is_some_and(|expected| output.stdout != expected.as_bytes())
            || output.stdout.is_empty()
        {
            return Err(format!("identidade/ajuda divergente: {name}"));
        }
    }
    Ok(())
}

pub fn compare_builds(package: &Path, builds: &Path) -> Result<()> {
    for binary in BINARIES {
        let name = filename(binary);
        if manifest::digest(&package.join(&name))? != manifest::digest(&builds.join(name))? {
            return Err("executável extraído diverge do build declarado".into());
        }
    }
    Ok(())
}

/// Confere o registro observacional produzido logo após o build e o empacotamento.
/// Não autentica o operador: o hash externo da baseline e os logs são guardados à parte.
pub fn provenance(
    path: &Path,
    archive: &Path,
    binaries: &Path,
    identity: &(String, String),
    version: &str,
) -> Result<Value> {
    if manifest::digest(path)?.0 > 65536 {
        return Err("proveniência excedeu limite".into());
    }
    let value: Value = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    validate_provenance(&value, identity, version)?;
    if file_identity(archive)? != value["archive"] {
        return Err("pacote diverge da proveniência de build".into());
    }
    for binary in value["binaries"].as_array().unwrap() {
        let file = binaries.join(binary["path"].as_str().unwrap());
        let actual = file_identity(&file)?;
        if actual["bytes"] != binary["bytes"] || actual["sha256"] != binary["sha256"] {
            return Err("executável diverge da proveniência de build".into());
        }
    }
    Ok(value)
}

pub fn file_identity(path: &Path) -> Result<Value> {
    let (bytes, sha256) = manifest::digest(path)?;
    Ok(json!({"bytes":bytes,"sha256":sha256}))
}

fn validate_provenance(value: &Value, identity: &(String, String), version: &str) -> Result<()> {
    manifest::fields(
        value,
        &[
            "schema_version",
            "source_sha",
            "target",
            "version",
            "compiler",
            "command",
            "args",
            "exit_code",
            "source_clean_before",
            "source_clean_after",
            "binaries",
            "archive",
        ],
    )?;
    let default_args = json!(["build", "--locked", "--release", "--bins"]);
    let target_args = json!([
        "build",
        "--locked",
        "--release",
        "--bins",
        "--target",
        target()
    ]);
    if value["schema_version"] != 1
        || value["source_sha"] != identity.0
        || value["target"] != target()
        || value["version"] != version
        || value["compiler"] != identity.1
        || value["command"] != "cargo"
        || (value["args"] != default_args && value["args"] != target_args)
        || value["exit_code"] != 0
        || value["source_clean_before"] != true
        || value["source_clean_after"] != true
    {
        return Err("identidade ou execução do build diverge".into());
    }
    manifest::hex(&value["source_sha"], 40)?;
    manifest::fields(&value["archive"], &["bytes", "sha256"])?;
    let entries = value["binaries"]
        .as_array()
        .ok_or("binários da proveniência ausentes")?;
    let mut names = BINARIES.map(filename);
    names.sort_unstable();
    if entries.len() != names.len() {
        return Err("proveniência exige quatro executáveis".into());
    }
    for (entry, name) in entries.iter().zip(names) {
        manifest::fields(entry, &["path", "bytes", "sha256"])?;
        if entry["path"] != name {
            return Err("nomes ou ordem dos binários divergem".into());
        }
    }
    for entry in entries.iter().chain(std::iter::once(&value["archive"])) {
        if entry["bytes"]
            .as_u64()
            .is_none_or(|n| n == 0 || n > manifest::MAX_FILE_BYTES)
        {
            return Err("tamanho inválido na proveniência".into());
        }
        manifest::hex(&entry["sha256"], 64)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_provenance_binds_identity_command_archive_and_all_executables() {
        let directory = super::super::scenario::Scratch::new().unwrap();
        let archive = directory.0.join("package.zip");
        fs::write(&archive, b"archive fixture, not a real package").unwrap();
        let mut binaries = Vec::new();
        for name in BINARIES.map(filename) {
            fs::write(directory.0.join(&name), b"binary fixture, not executable").unwrap();
            let mut record = file_identity(&directory.0.join(&name)).unwrap();
            record["path"] = json!(name);
            binaries.push(record);
        }
        binaries.sort_unstable_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
        let identity = ("a".repeat(40), "rustc test fixture\n".to_owned());
        let valid = json!({"schema_version":1,"source_sha":identity.0,"target":target(),"version":"0.1.0","compiler":identity.1,"command":"cargo","args":["build","--locked","--release","--bins"],"exit_code":0,"source_clean_before":true,"source_clean_after":true,"binaries":binaries,"archive":file_identity(&archive).unwrap()});
        let sidecar = directory.0.join("provenance.json");
        fs::write(&sidecar, serde_json::to_vec(&valid).unwrap()).unwrap();
        assert!(provenance(&sidecar, &archive, &directory.0, &identity, "0.1.0").is_ok());
        for (pointer, wrong) in [
            ("/source_sha", json!("b".repeat(40))),
            ("/compiler", json!("old rustc")),
            ("/exit_code", json!(1)),
            ("/source_clean_before", json!(false)),
            ("/args", json!(["build"])),
            ("/archive/sha256", json!("f".repeat(64))),
            ("/binaries/0/sha256", json!("f".repeat(64))),
        ] {
            let mut changed = valid.clone();
            *changed.pointer_mut(pointer).unwrap() = wrong;
            fs::write(&sidecar, serde_json::to_vec(&changed).unwrap()).unwrap();
            assert!(
                provenance(&sidecar, &archive, &directory.0, &identity, "0.1.0").is_err(),
                "{pointer}"
            );
        }
        fs::write(&sidecar, serde_json::to_vec(&valid).unwrap()).unwrap();
        fs::write(directory.0.join(filename("sider")), b"old binary").unwrap();
        assert!(provenance(&sidecar, &archive, &directory.0, &identity, "0.1.0").is_err());
    }
}
