//! External gate context and receipt; missing results never become evidence of success.

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

/// Injectable observation allows rejection tests without modifying Git or the environment.
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

/// A context publishes only after rechecking the same clean checkout.
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
                | "soak"
                | "benchmarks"
        ) {
            return Err("unknown gate or runner not implemented".into());
        }
        let mut text = |name: &str| -> Result<String, String> {
            lookup(name)
                .ok_or_else(|| format!("missing {name}"))?
                .into_string()
                .map_err(|_| format!("{name} must be UTF-8"))
                .and_then(|value| {
                    if value.is_empty() {
                        Err(format!("empty {name}"))
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
            PathBuf::from(lookup("SIDER_RELEASE_DIR").ok_or("missing SIDER_RELEASE_DIR")?);
        if !release_dir.is_absolute() || !release_dir.is_dir() {
            return Err("SIDER_RELEASE_DIR must be an existing absolute directory".into());
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

    /// The caller verifies the cases and measures the actual duration before calling this.
    ///
    /// Does not replace receipts. The hard link publishes only complete JSON; the
    /// directory must be controlled and its filesystem must support hard links.
    pub fn publish(&self, cases: u64, duration: Duration, details: Value) -> Result<(), String> {
        if cases == 0 {
            return Err("gate has no executed cases".into());
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
            return Err("receipt exceeds 1 MiB; store logs separately".into());
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
            .map_err(|e| format!("create temporary receipt: {e}"))?;
        let mut cleanup = TemporaryReceipt {
            path: temporary,
            file: Some(file),
        };
        let file = cleanup.file.as_mut().ok_or("temporary receipt closed")?;
        file.write_all(&contents).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        cleanup.file.take();

        // Also detects changes that occurred while the receipt was being prepared.
        self.validate(&(self.observer)(&self.root)?)?;
        fs::hard_link(&cleanup.path, self.receipt_path())
            .map_err(|e| format!("publish receipt without replacing the destination: {e}"))?;
        Ok(())
    }

    fn receipt_path(&self) -> PathBuf {
        self.release_dir.join(format!("receipt-{}.json", self.gate))
    }

    fn ensure_destination_absent(&self) -> Result<(), String> {
        match fs::symlink_metadata(self.receipt_path()) {
            Ok(_) => Err("old receipt found; use a new directory".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("inspect receipt destination: {error}")),
        }
    }

    fn validate(&self, observed: &Observation) -> Result<(), String> {
        let expected = &self.expected;
        if !hex_id(&expected.sha, 40) || observed.head != expected.sha {
            return Err("invalid release SHA or mismatch with actual HEAD".into());
        }
        if !observed.status.is_empty() {
            return Err(
                "dirty checkout; tracked or untracked changes are not release evidence".into(),
            );
        }
        let base = base_version(&expected.version)?;
        if base != expected.version {
            return Err("receipts identify the final binary version, without an RC suffix".into());
        }
        if observed.compiled_version != expected.version {
            return Err("compiled test version differs from the release".into());
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
            return Err("gate requires a supported native platform and target".into());
        }
        let hosts: Vec<_> = observed
            .compiler
            .lines()
            .filter_map(|line| line.strip_prefix("host: "))
            .collect();
        if hosts != [expected.target.as_str()] {
            return Err("compiler host differs from the declared target".into());
        }
        let plan = &observed.plan;
        let policy = &plan["release_policy"];
        if plan["schema_version"] != 2
            || policy["private"] != false
            || policy["publish_crate"] != false
            || policy["final_promotion"] != "same_sha_same_assets"
            || policy["bundle_change_requires_new_candidate"] != true
            || !policy["targets"]
                .as_array()
                .is_some_and(|targets| targets.contains(&json!(expected.target)))
        {
            return Err("invalid public release policy, immutable bundle policy, or target".into());
        }
        let releases = plan["releases"].as_array().ok_or("missing releases")?;
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
                "publishable version or gate not uniquely registered in the manifest".into(),
            );
        }
        let reference = &plan["reference"];
        let redis_version = reference["redis_version"]
            .as_str()
            .ok_or("missing Redis version")?;
        if base_version(redis_version)? != redis_version {
            return Err("Redis reference requires a final version without an RC suffix".into());
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
            return Err("Redis/CLI reference differs from the image pinned in the manifest".into());
        }
        let repository = plan["repository"].as_str().ok_or("missing repository")?;
        let packages = observed.cargo_metadata["packages"]
            .as_array()
            .ok_or("packages missing from cargo metadata")?;
        let manifests: Vec<_> = packages
            .iter()
            .filter(|package| {
                package["manifest_path"].as_str().is_some_and(|path| {
                    Path::new(path).canonicalize().ok() == Some(self.root.join("Cargo.toml"))
                })
            })
            .collect();
        if manifests.len() != 1 {
            return Err("root package not uniquely identified in cargo metadata".into());
        }
        let package = manifests[0];
        if package["name"] != "sider"
            || package["version"] != expected.version
            || !package["publish"].as_array().is_some_and(Vec::is_empty)
            || package["license"] != "MIT"
            || package["repository"] != format!("https://github.com/{repository}")
        {
            return Err(
                "Cargo metadata must retain the exact version, MIT, and publish=false".into(),
            );
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
            return Err("invalid candidate number".into());
        }
        base
    } else {
        version
    };
    let parts: Vec<_> = base.split('.').collect();
    if parts.len() != 3 || !parts.into_iter().all(|part| decimal(part, true)) {
        return Err("invalid version; expected x.y.z or x.y.z-rc.N".into());
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
        return Err("checkout changed during gate observation".into());
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
        compiled_env: compiled_environment().to_owned(),
    })
}

pub(crate) fn compiled_environment() -> &'static str {
    if cfg!(target_env = "gnu") {
        "gnu"
    } else if cfg!(target_env = "msvc") {
        "msvc"
    } else {
        "other"
    }
}

fn checked(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = process::run(command, COMMAND_TIMEOUT)?;
    if !output.status.success() {
        return Err(format!(
            "gate observation failed: {:?}: {}",
            command.get_program(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output.stdout)
}
