//! Contexto e recibo de gates externos; nenhuma ausência vira evidência de sucesso.

#![allow(dead_code)]

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use super::process;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const LINUX_TARGET: &str = "x86_64-unknown-linux-gnu";
const WINDOWS_TARGET: &str = "x86_64-pc-windows-msvc";
const MAX_RECEIPT_BYTES: usize = 1024 * 1024;
static NEXT_RECEIPT: AtomicU64 = AtomicU64::new(0);

type Observer = Box<dyn Fn(&Path) -> Result<Observation, String>>;

/// Observação injetável permite testar rejeições sem modificar Git ou o ambiente.
#[derive(Clone)]
pub(crate) struct Observation {
    pub head: String,
    pub status: Vec<u8>,
    pub cargo_metadata: Value,
    pub plan: Value,
    pub compiler: String,
    pub compiled_version: String,
    pub compiled_os: String,
    pub compiled_arch: String,
    pub compiled_env: String,
}

struct Expected {
    version: String,
    sha: String,
    target: String,
    reference_image: String,
}

/// Um contexto só publica depois de conferir novamente o mesmo checkout limpo.
pub struct GateContext {
    gate: String,
    root: PathBuf,
    release_dir: PathBuf,
    expected: Expected,
    observer: Observer,
}

impl GateContext {
    pub fn from_env(gate_id: &str) -> Result<Self, String> {
        Self::create(
            gate_id,
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            |name| std::env::var_os(name),
            Box::new(observe),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_observer(
        gate_id: &str,
        root: PathBuf,
        lookup: impl FnMut(&str) -> Option<OsString>,
        observer: impl Fn(&Path) -> Result<Observation, String> + 'static,
    ) -> Result<Self, String> {
        Self::create(gate_id, root, lookup, Box::new(observer))
    }

    fn create(
        gate_id: &str,
        root: PathBuf,
        mut lookup: impl FnMut(&str) -> Option<OsString>,
        observer: Observer,
    ) -> Result<Self, String> {
        if !matches!(
            gate_id,
            "compatibility"
                | "pubsub"
                | "crash"
                | "recovery"
                | "migration"
                | "sharding"
                | "types"
                | "sorted_sets"
                | "transactions"
                | "docker"
                | "replication"
        ) {
            return Err("gate desconhecido ou sem runner implementado".into());
        }
        let mut text = |name: &str| -> Result<String, String> {
            lookup(name)
                .ok_or_else(|| format!("{name} ausente"))?
                .into_string()
                .map_err(|_| format!("{name} precisa ser UTF-8"))
                .and_then(|value| {
                    if value.is_empty() {
                        Err(format!("{name} vazio"))
                    } else {
                        Ok(value)
                    }
                })
        };
        let expected = Expected {
            version: text("SIDER_RELEASE_VERSION")?,
            sha: text("SIDER_RELEASE_SHA")?,
            target: text("SIDER_RELEASE_TARGET")?,
            reference_image: text("SIDER_REFERENCE_IMAGE")?,
        };
        let release_dir =
            PathBuf::from(lookup("SIDER_RELEASE_DIR").ok_or("SIDER_RELEASE_DIR ausente")?);
        if !release_dir.is_absolute() || !release_dir.is_dir() {
            return Err("SIDER_RELEASE_DIR precisa ser diretório absoluto existente".into());
        }
        let release_dir = release_dir.canonicalize().map_err(|e| e.to_string())?;
        let root = root.canonicalize().map_err(|e| e.to_string())?;
        let context = Self {
            gate: gate_id.to_owned(),
            root,
            release_dir,
            expected,
            observer,
        };
        context.ensure_destination_absent()?;
        context.validate(&(context.observer)(&context.root)?)?;
        Ok(context)
    }

    pub fn release_dir(&self) -> &Path {
        &self.release_dir
    }

    pub fn version(&self) -> &str {
        &self.expected.version
    }

    pub fn sha(&self) -> &str {
        &self.expected.sha
    }

    pub fn target(&self) -> &str {
        &self.expected.target
    }

    /// O chamador comprova os casos e mede a duração real antes de chamar aqui.
    ///
    /// Não substitui recibos. O hard link publica somente JSON completo; o
    /// diretório deve ser controlado e seu filesystem precisa suportar hard links.
    pub fn publish(&self, cases: u64, duration: Duration, details: Value) -> Result<(), String> {
        if cases == 0 {
            return Err("gate sem casos executados".into());
        }
        self.ensure_destination_absent()?;
        let observed = (self.observer)(&self.root)?;
        self.validate(&observed)?;
        let receipt = json!({
            "schema_version": 1,
            "gate": self.gate,
            "sha": self.expected.sha,
            "version": self.expected.version,
            "target": self.expected.target,
            "reference_image": self.expected.reference_image,
            "compiler": observed.compiler,
            "status": "success",
            "cases": cases,
            "duration_seconds": duration.as_secs_f64(),
            "details": details,
        });
        let mut contents = serde_json::to_vec_pretty(&receipt).map_err(|e| e.to_string())?;
        contents.push(b'\n');
        if contents.len() > MAX_RECEIPT_BYTES {
            return Err("recibo excede 1 MiB; guarde logs separadamente".into());
        }
        let temporary = self.release_dir.join(format!(
            ".receipt-{}-{}-{}.tmp",
            self.gate,
            std::process::id(),
            NEXT_RECEIPT.fetch_add(1, Ordering::Relaxed)
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| format!("criar recibo temporário: {e}"))?;
        let mut cleanup = TemporaryReceipt {
            path: temporary,
            file: Some(file),
        };
        let file = cleanup.file.as_mut().ok_or("recibo temporário fechado")?;
        file.write_all(&contents).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        cleanup.file.take();

        // Também detecta mudanças ocorridas enquanto o recibo era preparado.
        self.validate(&(self.observer)(&self.root)?)?;
        fs::hard_link(&cleanup.path, self.receipt_path())
            .map_err(|e| format!("publicar recibo sem substituir destino: {e}"))?;
        Ok(())
    }

    fn receipt_path(&self) -> PathBuf {
        self.release_dir.join(format!("receipt-{}.json", self.gate))
    }

    fn ensure_destination_absent(&self) -> Result<(), String> {
        match fs::symlink_metadata(self.receipt_path()) {
            Ok(_) => Err("recibo antigo encontrado; use diretório novo".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("inspecionar destino do recibo: {error}")),
        }
    }

    fn validate(&self, observed: &Observation) -> Result<(), String> {
        let expected = &self.expected;
        if !hex_id(&expected.sha, 40) || observed.head != expected.sha {
            return Err("SHA de release inválido ou divergente do HEAD real".into());
        }
        if !observed.status.is_empty() {
            return Err(
                "checkout sujo; alterações tracked ou untracked não são evidência de release"
                    .into(),
            );
        }
        let base = base_version(&expected.version)?;
        if base != expected.version {
            return Err("recibos identificam a versão final do binário, sem sufixo RC".into());
        }
        if observed.compiled_version != expected.version {
            return Err("versão compilada do teste diverge da release".into());
        }
        let linux = expected.target == LINUX_TARGET
            && observed.compiled_os == "linux"
            && observed.compiled_arch == "x86_64"
            && observed.compiled_env == "gnu";
        let windows = expected.target == WINDOWS_TARGET
            && observed.compiled_os == "windows"
            && observed.compiled_arch == "x86_64"
            && observed.compiled_env == "msvc";
        let portable = matches!(self.gate.as_str(), "crash" | "recovery" | "migration");
        if !linux && !(portable && windows) {
            return Err("gate exige plataforma nativa e target suportados".into());
        }
        let hosts: Vec<_> = observed
            .compiler
            .lines()
            .filter_map(|line| line.strip_prefix("host: "))
            .collect();
        if hosts != [expected.target.as_str()] {
            return Err("host do compilador diverge do target declarado".into());
        }
        let plan = &observed.plan;
        let policy = &plan["release_policy"];
        if plan["schema_version"] != 2
            || policy["private"] != true
            || policy["publish_crate"] != false
            || policy["final_promotion"] != "same_sha_same_assets"
            || policy["bundle_change_requires_new_candidate"] != true
            || !policy["targets"]
                .as_array()
                .is_some_and(|targets| targets.contains(&json!(expected.target)))
        {
            return Err("política de release privada, bundle imutável ou target inválido".into());
        }
        let releases = plan["releases"].as_array().ok_or("releases ausente")?;
        let matches: Vec<_> = releases
            .iter()
            .filter(|release| release["version"] == base)
            .collect();
        if matches.len() != 1
            || matches[0]["publication"] != true
            || !matches[0]["required_gates"]
                .as_array()
                .is_some_and(|gates| gates.contains(&json!(self.gate)))
        {
            return Err(
                "versão publicável ou gate não registrado de forma única no manifesto".into(),
            );
        }
        let reference = &plan["reference"];
        let redis_version = reference["redis_version"]
            .as_str()
            .ok_or("versão Redis ausente")?;
        if base_version(redis_version)? != redis_version {
            return Err("referência Redis precisa de versão final, sem sufixo RC".into());
        }
        let prefix = format!("redis:{redis_version}@sha256:");
        if reference["image"] != expected.reference_image
            || reference["platform"] != "linux/amd64"
            || reference["redis_cli_version"] != redis_version
            || !expected
                .reference_image
                .strip_prefix(&prefix)
                .is_some_and(|digest| hex_id(digest, 64))
        {
            return Err("referência Redis/CLI diverge da imagem fixada no manifesto".into());
        }
        let repository = plan["repository"].as_str().ok_or("repositório ausente")?;
        let packages = observed.cargo_metadata["packages"]
            .as_array()
            .ok_or("packages ausente no cargo metadata")?;
        let manifests: Vec<_> = packages
            .iter()
            .filter(|package| {
                package["manifest_path"].as_str().is_some_and(|path| {
                    Path::new(path).canonicalize().ok() == Some(self.root.join("Cargo.toml"))
                })
            })
            .collect();
        if manifests.len() != 1 {
            return Err("pacote raiz não identificado de forma única no cargo metadata".into());
        }
        let package = manifests[0];
        if package["name"] != "sider"
            || package["version"] != expected.version
            || !package["publish"].as_array().is_some_and(Vec::is_empty)
            || package["license"] != "MIT"
            || package["repository"] != format!("https://github.com/{repository}")
        {
            return Err("metadados Cargo devem manter versão exata, MIT e publish=false".into());
        }
        Ok(())
    }
}

struct TemporaryReceipt {
    path: PathBuf,
    file: Option<File>,
}

impl Drop for TemporaryReceipt {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

fn hex_id(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn base_version(version: &str) -> Result<&str, String> {
    let base = if let Some((base, candidate)) = version.split_once("-rc.") {
        if !decimal(candidate, false) {
            return Err("número de candidata inválido".into());
        }
        base
    } else {
        version
    };
    let parts: Vec<_> = base.split('.').collect();
    if parts.len() != 3 || !parts.into_iter().all(|part| decimal(part, true)) {
        return Err("versão inválida; esperado x.y.z ou x.y.z-rc.N".into());
    }
    Ok(base)
}

fn decimal(value: &str, zero: bool) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" && zero || !value.starts_with('0'))
        && value.parse::<u64>().is_ok()
}

fn observe(root: &Path) -> Result<Observation, String> {
    let git = |args: &[&str]| -> Result<Vec<u8>, String> {
        checked(Command::new("git").current_dir(root).args(args))
    };
    let head =
        String::from_utf8(git(&["rev-parse", "--verify", "HEAD"])?).map_err(|e| e.to_string())?;
    let status = git(&[
        "status",
        "--porcelain=v1",
        "--untracked-files=normal",
        "--ignore-submodules=none",
    ])?;
    let metadata = checked(Command::new("cargo").current_dir(root).args([
        "metadata",
        "--locked",
        "--offline",
        "--no-deps",
        "--format-version=1",
    ]))?;
    let plan = fs::read(root.join("releases/plan.json")).map_err(|e| e.to_string())?;
    let compiler = checked(
        Command::new("rustc")
            .current_dir(root)
            .args(["--version", "--verbose"]),
    )?;
    let after =
        String::from_utf8(git(&["rev-parse", "--verify", "HEAD"])?).map_err(|e| e.to_string())?;
    if head != after
        || !git(&[
            "status",
            "--porcelain=v1",
            "--untracked-files=normal",
            "--ignore-submodules=none",
        ])?
        .is_empty()
    {
        return Err("checkout mudou durante a observação do gate".into());
    }
    Ok(Observation {
        head: head.trim_end().to_owned(),
        status,
        cargo_metadata: serde_json::from_slice(&metadata).map_err(|e| e.to_string())?,
        plan: serde_json::from_slice(&plan).map_err(|e| e.to_string())?,
        compiler: String::from_utf8(compiler).map_err(|e| e.to_string())?,
        compiled_version: env!("CARGO_PKG_VERSION").to_owned(),
        compiled_os: std::env::consts::OS.to_owned(),
        compiled_arch: std::env::consts::ARCH.to_owned(),
        compiled_env: if cfg!(target_env = "gnu") {
            "gnu"
        } else {
            "other"
        }
        .to_owned(),
    })
}

fn checked(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = process::run(command, COMMAND_TIMEOUT)?;
    if !output.status.success() {
        return Err(format!(
            "observação do gate falhou: {:?}: {}",
            command.get_program(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output.stdout)
}
