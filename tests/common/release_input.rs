//! Vínculo verificável entre checkout, manifesto preliminar, tar.gz e executável extraído.
#![allow(dead_code)]

use super::{gate_receipt::GateContext, process};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

const TARGET: &str = "x86_64-unknown-linux-gnu";
const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BINARY: u64 = 128 * 1024 * 1024;
const MAX_ARCHIVE: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct PackageProof {
    name: String,
    size: u64,
    sha256: String,
}

/// O diretório precisa continuar controlado e imóvel; a conferência não autentica seu autor.
pub struct ReleaseInput {
    directory: PathBuf,
    binary: PathBuf,
    package: PackageProof,
    binary_size: u64,
    binary_sha256: String,
}

impl ReleaseInput {
    pub fn from_env(context: &GateContext) -> Result<Self, String> {
        let directory = PathBuf::from(
            std::env::var_os("SIDER_PACKAGE_DIR").ok_or("SIDER_PACKAGE_DIR ausente")?,
        );
        Self::load(context, &directory)
    }

    fn load(context: &GateContext, directory: &Path) -> Result<Self, String> {
        if context.target() != TARGET
            || !cfg!(all(
                target_os = "linux",
                target_arch = "x86_64",
                target_env = "gnu"
            ))
        {
            return Err("pacote de carga exige execução nativa Linux GNU x86_64".into());
        }
        if !directory.is_absolute() {
            return Err("SIDER_PACKAGE_DIR precisa ser absoluto".into());
        }
        let metadata = fs::symlink_metadata(directory).map_err(|e| e.to_string())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("pacote extraído precisa ser diretório real".into());
        }
        let directory = directory.canonicalize().map_err(|e| e.to_string())?;
        let manifest: Value = serde_json::from_slice(&read_bounded(
            &context.release_dir().join("release-manifest.json"),
            16 * 1024 * 1024,
        )?)
        .map_err(|e| e.to_string())?;
        let package = manifest_package(
            &manifest,
            context.version(),
            context.sha(),
            context.target(),
        )?;
        let archive = context.release_dir().join(&package.name);
        if regular_size(&archive, MAX_ARCHIVE)? != package.size
            || sha256(&archive)? != package.sha256
        {
            return Err("tar.gz diverge do hash ou tamanho no manifesto".into());
        }
        let binary = directory.join("sider");
        let binary_size = regular_size(&binary, MAX_BINARY)?;
        if binary_size == 0 {
            return Err("executável extraído vazio".into());
        }
        for (name, expected) in [
            (
                "README.md",
                include_bytes!("../../releases/README.md").as_slice(),
            ),
            ("LICENSE", include_bytes!("../../LICENSE").as_slice()),
        ] {
            if read_bounded(&directory.join(name), expected.len() as u64)? != expected {
                return Err(format!("{name} extraído diverge do checkout"));
            }
        }
        let member = format!("sider-v{}-{}/sider", context.version(), context.target());
        let listing = checked(
            Command::new("tar")
                .env_remove("TAR_OPTIONS")
                .env_remove("GZIP")
                .arg("--list")
                .arg("--gzip")
                .arg("--file")
                .arg(&archive),
        )?;
        unique_member(&listing, &member)?;
        let extracted = process::run_with_stdout_limit(
            Command::new("tar")
                .env_remove("TAR_OPTIONS")
                .env_remove("GZIP")
                .env("LC_ALL", "C")
                .arg("--extract")
                .arg("--to-stdout")
                .arg("--gzip")
                .arg("--file")
                .arg(&archive)
                .arg("--")
                .arg(&member),
            TIMEOUT,
            binary_size as usize,
        )?;
        if !extracted.status.success() || extracted.stdout != read_bounded(&binary, binary_size)? {
            return Err("executável não corresponde ao membro único do tar.gz".into());
        }
        let binary_sha256 = sha256(&binary)?;
        Ok(Self {
            directory,
            binary,
            package,
            binary_size,
            binary_sha256,
        })
    }

    pub fn binary(&self) -> &Path {
        &self.binary
    }

    pub fn details(&self) -> Value {
        json!({"package":self.package.name,"package_bytes":self.package.size,"package_sha256":self.package.sha256,
            "binary":"sider","binary_bytes":self.binary_size,"binary_sha256":self.binary_sha256,
            "binary_matches_archive_member":true,"readme_matches_checkout":true,"license_matches_checkout":true})
    }

    pub fn verify_again(&self, context: &GateContext) -> Result<(), String> {
        let current = Self::load(context, &self.directory)?;
        if current.package != self.package
            || current.binary_size != self.binary_size
            || current.binary_sha256 != self.binary_sha256
        {
            return Err("pacote ou executável mudou durante a validação".into());
        }
        Ok(())
    }
}

fn manifest_package(
    manifest: &Value,
    version: &str,
    sha: &str,
    target: &str,
) -> Result<PackageProof, String> {
    if !hex(sha, 40)
        || target != TARGET
        || manifest["schema_version"] != 2
        || manifest["artifact_version"] != version
        || manifest["sha"] != sha
        || manifest["private"] != true
        || manifest["crate_publication"] != false
        || manifest["provenance"]["frozen_checkout"] != sha
        || !manifest["targets"]
            .as_array()
            .is_some_and(|targets| targets.contains(&json!(target)))
    {
        return Err("identidade do manifesto preliminar diverge do contexto do gate".into());
    }
    let name = format!("sider-v{version}-{target}.tar.gz");
    let artifacts = manifest["artifacts"]
        .as_array()
        .ok_or("inventário de artefatos ausente")?;
    let mut selected = artifacts.iter().filter(|item| item["name"] == name);
    let proof = selected
        .next()
        .ok_or("tar.gz esperado ausente no manifesto")?;
    if selected.next().is_some() {
        return Err("tar.gz repetido no manifesto".into());
    }
    let size = proof["size"]
        .as_u64()
        .filter(|size| *size > 0 && *size <= MAX_ARCHIVE)
        .ok_or("tamanho do tar.gz inválido")?;
    let digest = proof["sha256"]
        .as_str()
        .filter(|digest| hex(digest, 64))
        .ok_or("hash do tar.gz inválido")?;
    Ok(PackageProof {
        name,
        size,
        sha256: digest.to_owned(),
    })
}

fn unique_member(listing: &[u8], member: &str) -> Result<(), String> {
    let listing =
        std::str::from_utf8(listing).map_err(|_| "lista de membros do tar não é UTF-8")?;
    let mut count = 0;
    let mut entries = 0;
    for entry in listing.lines() {
        entries += 1;
        if entries > 10000
            || entry.len() > 1024
            || entry.starts_with('/')
            || entry.contains('\\')
            || entry.split('/').any(|part| part == "..")
        {
            return Err("nome ou quantidade de membros do tar inválido".into());
        }
        count += usize::from(entry == member);
    }
    if count != 1 {
        return Err("membro do executável ausente ou repetido no tar".into());
    }
    Ok(())
}

fn regular_size(path: &Path, limit: u64) -> Result<u64, String> {
    let metadata = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err("arquivo regular obrigatório ausente, simbólico ou acima do limite".into());
    }
    Ok(metadata.len())
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    regular_size(path, limit)?;
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|e| e.to_string())?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > limit {
        return Err("arquivo cresceu durante leitura".into());
    }
    Ok(bytes)
}

fn checked(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = process::run(command.env("LC_ALL", "C"), TIMEOUT)?;
    if !output.status.success() {
        return Err("comando de integridade do pacote falhou".into());
    }
    Ok(output.stdout)
}

pub fn sha256(path: &Path) -> Result<String, String> {
    parse_digest(&checked(Command::new("sha256sum").arg("--").arg(path))?)
}

fn parse_digest(bytes: &[u8]) -> Result<String, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "saída sha256sum não é UTF-8")?;
    let digest = text
        .split_whitespace()
        .next()
        .filter(|value| hex(value, 64))
        .ok_or("saída sha256sum inválida")?;
    if text.lines().count() != 1 {
        return Err("sha256sum retornou múltiplas linhas".into());
    }
    Ok(digest.to_owned())
}
fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    const SHA: &str = "0123456789012345678901234567890123456789";
    fn manifest() -> Value {
        json!({"schema_version":2,"artifact_version":"1.0.0","sha":SHA,"private":true,"crate_publication":false,"targets":[TARGET],
        "provenance":{"frozen_checkout":SHA},"artifacts":[{"name":format!("sider-v1.0.0-{TARGET}.tar.gz"),"size":10,"sha256":"a".repeat(64)}]})
    }
    #[test]
    fn release_input_accepts_preliminary_manifest_without_final_gate_receipts() {
        let proof = manifest_package(&manifest(), "1.0.0", SHA, TARGET).unwrap();
        assert_eq!(proof.size, 10);
    }
    #[test]
    fn release_input_rejects_wrong_identity_duplicate_asset_and_invalid_digest() {
        for field in [
            "schema_version",
            "artifact_version",
            "sha",
            "private",
            "crate_publication",
            "provenance",
            "targets",
        ] {
            let mut changed = manifest();
            changed[field] = Value::Null;
            assert!(
                manifest_package(&changed, "1.0.0", SHA, TARGET).is_err(),
                "{field}"
            );
        }
        let mut changed = manifest();
        changed["artifacts"][0]["sha256"] = json!("A".repeat(64));
        assert!(manifest_package(&changed, "1.0.0", SHA, TARGET).is_err());
        let mut changed = manifest();
        let duplicate = changed["artifacts"][0].clone();
        changed["artifacts"].as_array_mut().unwrap().push(duplicate);
        assert!(manifest_package(&changed, "1.0.0", SHA, TARGET).is_err());
    }
    #[test]
    fn release_input_requires_exact_unique_archive_member() {
        assert!(unique_member(b"package/\npackage/sider\n", "package/sider").is_ok());
        for listing in [
            b"package/sider\npackage/sider\n".as_slice(),
            b"other/sider\n",
            b"../package/sider\n",
            b"/package/sider\n",
        ] {
            assert!(unique_member(listing, "package/sider").is_err());
        }
    }
    #[test]
    fn release_input_digest_parser_rejects_truncated_or_multiline_output() {
        assert_eq!(
            parse_digest(format!("{}  package\n", "a".repeat(64)).as_bytes()).unwrap(),
            "a".repeat(64)
        );
        assert!(parse_digest(b"abc package\n").is_err());
        assert!(
            parse_digest(format!("{}  x\n{}  y\n", "a".repeat(64), "a".repeat(64)).as_bytes())
                .is_err()
        );
    }
}
