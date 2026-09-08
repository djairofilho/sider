//! Integridade offline dos arquivos de uma release, sem execução ou publicação.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const LINUX: &str = "x86_64-unknown-linux-gnu";
const WINDOWS: &str = "x86_64-pc-windows-msvc";
const MAX_ASSET_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_JSON_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EVIDENCE_ZIP_BYTES: u64 = 256 * 1024 * 1024;
const MAX_ENTRY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_EXPANDED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_ENTRIES: usize = 10_000;
const MAX_NAME_BYTES: usize = 512;

#[derive(Debug, PartialEq, Eq)]
struct Proof {
    size: u64,
    sha256: String,
}

type GateKey = (String, String);

/// Confere bytes e coerência dos registros locais; não autentica sua origem.
///
/// Não consulta o GitHub, executa testes, extrai pacotes ou autoriza publicação.
/// O diretório deve permanecer imóvel durante a leitura. ZIP64, links e nomes
/// não portáveis são rejeitados no formato limitado do arquivo de evidências.
pub fn verify(plan: &Value, version: &str, sha: &str, directory: &Path) -> Result<Value, String> {
    let (base, candidate) = parse_version(version)?;
    require_hex(sha, 40, "SHA da release")?;
    let (required, docker) = required_gates(plan, base)?;
    let mut expected: BTreeSet<String> = [
        "release-manifest.json",
        "release-notes.md",
        "release-preflight.json",
        "runtime-requirements.md",
        "SHA256SUMS",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let evidence_name = format!("sider-v{version}-validation.zip");
    expected.extend([
        evidence_name.clone(),
        format!("sider-v{version}-{WINDOWS}.zip"),
        format!("sider-v{version}-{LINUX}.tar.gz"),
    ]);
    if docker {
        expected.insert(format!("sider-v{version}-linux-amd64-image.tar.gz"));
    }
    let proofs = directory_proofs(directory, &expected)?;
    verify_checksums(directory, &proofs)?;
    let manifest = read_json(&directory.join("release-manifest.json"))?;
    verify_identity(plan, &manifest, version, sha, candidate)?;
    if text(&manifest, "evidence_archive")? != evidence_name
        || text(&manifest, "notes")? != "release-notes.md"
        || text(&manifest, "runtime_requirements")? != "runtime-requirements.md"
        || text(&manifest, "checksums")? != "SHA256SUMS"
    {
        return Err("Nomes de arquivos do manifesto divergentes".into());
    }
    let manifest_assets = inventory(&manifest["artifacts"], MAX_ASSET_BYTES, false)?;
    let expected_inventory: BTreeSet<_> = expected
        .iter()
        .filter(|name| !matches!(name.as_str(), "SHA256SUMS" | "release-manifest.json"))
        .cloned()
        .collect();
    if manifest_assets.keys().cloned().collect::<BTreeSet<_>>() != expected_inventory {
        return Err("Inventário de assets incompleto ou inesperado".into());
    }
    for (name, proof) in &manifest_assets {
        if proofs.get(name) != Some(proof) {
            return Err(format!("Asset divergente do manifesto: {name}"));
        }
    }
    let gates = gate_records(plan, &manifest, &required, version, sha)?;
    let evidence = inventory(&manifest["evidence_files"], MAX_ENTRY_BYTES, true)?;
    if evidence.is_empty() || evidence.len() > MAX_ENTRIES {
        return Err("Quantidade de evidências inválida".into());
    }
    let expanded_bytes = evidence.values().try_fold(0_u64, |total, proof| {
        total
            .checked_add(proof.size)
            .filter(|size| *size <= MAX_EXPANDED_BYTES)
            .ok_or("Evidências excedem o limite total de bytes")
    })?;
    verify_evidence(&directory.join(&evidence_name), &evidence, &gates)?;
    for name in [
        "release-notes.md",
        "runtime-requirements.md",
        "release-preflight.json",
    ] {
        let bytes = read_small(&directory.join(name), MAX_JSON_BYTES)?;
        let body =
            std::str::from_utf8(&bytes).map_err(|e| format!("{name}: UTF-8 inválido: {e}"))?;
        if body.contains(['\u{fffd}', '\u{00c3}', '\u{00c2}', '\u{0007}']) {
            return Err(format!("{name}: texto com possível mojibake"));
        }
    }
    let preflight = read_json(&directory.join("release-preflight.json"))?;
    if preflight["repository"]["nameWithOwner"] != plan["repository"]
        || preflight["repository"]["isPrivate"] != true
        || preflight["pull_request"]["mergeCommit"]["oid"] != sha
    {
        return Err("Preflight local divergente da identidade declarada".into());
    }
    // Uma segunda leitura detecta alterações comuns durante a verificação; não
    // oferece snapshot atômico contra um escritor adversarial concorrente.
    if directory_proofs(directory, &expected)? != proofs {
        return Err("Arquivos mudaram durante a verificação".into());
    }
    Ok(json!({
        "status": "integrity_verified", "version": version, "sha": sha,
        "prerelease": candidate, "assets": proofs.len(), "checksums": proofs.len() - 1,
        "gate_records": gates.len(), "evidence_files": evidence.len(),
        "evidence_expanded_bytes": expanded_bytes, "publication_authorized": false,
        "scope": "Integridade e coerência offline; não comprova execução, autenticidade, aprovação da RC ou estado atual do GitHub.",
        "limits": {"asset_bytes": MAX_ASSET_BYTES, "json_bytes": MAX_JSON_BYTES,
            "evidence_zip_bytes": MAX_EVIDENCE_ZIP_BYTES, "entry_bytes": MAX_ENTRY_BYTES,
            "expanded_bytes": MAX_EXPANDED_BYTES, "entries": MAX_ENTRIES, "zip64": false}
    }))
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("Campo textual ausente ou inválido: {field}"))
}

fn require_hex(value: &str, size: usize, what: &str) -> Result<(), String> {
    if value.len() != size
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "{what}: hexadecimal minúsculo de {size} caracteres exigido"
        ));
    }
    Ok(())
}

fn parse_version(version: &str) -> Result<([u64; 3], bool), String> {
    if version.len() > 64 {
        return Err("Versão longa demais".into());
    }
    let (base, candidate) = if let Some((base, rc)) = version.split_once("-rc.") {
        if number(rc)? == 0 {
            return Err("RC deve ter número positivo".into());
        }
        (base, true)
    } else {
        (version, false)
    };
    let parts = base.split('.').map(number).collect::<Result<Vec<_>, _>>()?;
    let numbers = parts
        .try_into()
        .map_err(|_| "Versão deve ser MAJOR.MINOR.PATCH[-rc.N]".to_owned())?;
    Ok((numbers, candidate))
}

fn number(value: &str) -> Result<u64, String> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err("Componente numérico de versão inválido".into());
    }
    value.parse().map_err(|_| "Overflow em versão".into())
}

fn required_gates(plan: &Value, base: [u64; 3]) -> Result<(BTreeSet<GateKey>, bool), String> {
    let targets = plan["release_policy"]["targets"]
        .as_array()
        .ok_or("Targets ausentes no plano")?;
    if targets.len() != 2 || !targets.contains(&json!(LINUX)) || !targets.contains(&json!(WINDOWS))
    {
        return Err("Targets do plano não são Linux GNU e Windows MSVC".into());
    }
    let releases = plan["releases"]
        .as_array()
        .ok_or("Releases ausentes no plano")?;
    let mut versions = BTreeSet::new();
    let mut names: BTreeSet<&str> = ["native", "tcp_smoke", "compatibility", "fuzz"].into();
    for release in releases {
        let (release_version, candidate) = parse_version(text(release, "version")?)?;
        if candidate || !versions.insert(release_version) {
            return Err("Versão duplicada ou RC no plano".into());
        }
        if release_version <= base {
            for gate in release["required_gates"]
                .as_array()
                .ok_or("required_gates ausente")?
            {
                names.insert(gate.as_str().ok_or("Gate não textual no plano")?);
            }
        }
    }
    if !versions.contains(&base) {
        return Err("Versão-base não registrada no plano".into());
    }
    let (docker_since, _) = parse_version(text(&plan["release_policy"], "docker_since")?)?;
    if base >= docker_since {
        names.insert("docker");
    }
    let docker = names.contains("docker");
    let mut required = BTreeSet::new();
    for name in names {
        let platforms: &[&str] = match name {
            "native" | "tcp_smoke" | "crash" | "recovery" | "migration" => &[LINUX, WINDOWS],
            "compatibility" | "fuzz" | "sharding" | "types" | "sorted_sets" | "transactions"
            | "pubsub" | "replication" | "docker" | "soak" | "benchmarks" => &[LINUX],
            _ => return Err(format!("Gate desconhecido: {name}")),
        };
        required.extend(
            platforms
                .iter()
                .map(|target| (name.to_owned(), (*target).to_owned())),
        );
    }
    Ok((required, docker))
}

fn verify_identity(
    plan: &Value,
    manifest: &Value,
    version: &str,
    sha: &str,
    candidate: bool,
) -> Result<(), String> {
    if manifest["schema_version"] != 1
        || manifest["version"] != version
        || manifest["sha"] != sha
        || manifest["tag"] != format!("v{version}")
        || manifest["prerelease"] != candidate
        || manifest["private"] != true
        || manifest["crate_publication"] != false
        || manifest["license"] != "MIT"
        || manifest["repository"] != plan["repository"]
        || manifest["reference"] != plan["reference"]
        || manifest["reference"].is_null()
        || manifest["provenance"]["kind"] != "manual-local"
        || manifest["provenance"]["ci_enabled"] != false
        || !manifest["provenance"]["github_actions_run"].is_null()
        || manifest["provenance"]["frozen_checkout"] != sha
    {
        return Err("Identidade, referência ou proveniência local do manifesto inválida".into());
    }
    let targets = manifest["targets"]
        .as_array()
        .ok_or("Targets do manifesto ausentes")?;
    if targets.len() != 2 || !targets.contains(&json!(LINUX)) || !targets.contains(&json!(WINDOWS))
    {
        return Err("Targets do manifesto divergentes".into());
    }
    let approved = &manifest["approved_candidate"];
    if candidate {
        if !approved.is_null() {
            return Err("Uma RC não deve declarar promoção de candidata".into());
        }
    } else {
        let tag = text(approved, "tag")?
            .strip_prefix('v')
            .ok_or("Tag da candidata sem v")?;
        let (rc_base, is_rc) = parse_version(tag)?;
        if !is_rc || rc_base != parse_version(version)?.0 {
            return Err("Candidata declarada não pertence à versão-base".into());
        }
        require_hex(text(approved, "sha")?, 40, "SHA da candidata")?;
        require_hex(
            text(approved, "manifest_sha256")?,
            64,
            "Hash do manifesto da candidata",
        )?;
        for name in ["release", "approval", "published_at"] {
            text(approved, name)?;
        }
        if approved["version_only_cargo_diff"] != true {
            return Err("Promoção não declara diff somente de versão".into());
        }
    }
    Ok(())
}

fn gate_records<'a>(
    plan: &Value,
    manifest: &'a Value,
    required: &BTreeSet<GateKey>,
    version: &str,
    sha: &str,
) -> Result<BTreeMap<GateKey, &'a Value>, String> {
    let mut records = BTreeMap::new();
    for gate in manifest["gates"]
        .as_array()
        .ok_or("Gates ausentes no manifesto")?
    {
        let name = text(gate, "gate")?;
        let target = text(gate, "target")?;
        if gate["schema_version"] != 1
            || gate["sha"] != sha
            || gate["version"] != version
            || gate["status"] != "success"
            || !gate["cases"].as_u64().is_some_and(|n| n > 0)
        {
            return Err(format!("Registro de gate inválido: {name}/{target}"));
        }
        let minimum = match name {
            "fuzz" => Some(
                plan["release_policy"]["candidate_fuzz_seconds"]
                    .as_u64()
                    .unwrap_or(900)
                    .max(900),
            ),
            "soak" => Some(
                plan["release_policy"]["stable_soak_seconds"]
                    .as_u64()
                    .unwrap_or(3600)
                    .max(3600),
            ),
            _ => None,
        };
        let duration = &gate["duration_seconds"];
        if (!duration.is_null() && !duration.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0))
            || minimum.is_some_and(|min| {
                !duration
                    .as_f64()
                    .is_some_and(|n| n.is_finite() && n >= min as f64)
            })
        {
            return Err(format!("Duração inválida ou insuficiente: {name}/{target}"));
        }
        if (matches!(name, "compatibility" | "fuzz") || !gate["reference_image"].is_null())
            && gate["reference_image"] != plan["reference"]["image"]
        {
            return Err(format!("Referência divergente no gate {name}"));
        }
        if records
            .insert((name.to_owned(), target.to_owned()), gate)
            .is_some()
        {
            return Err(format!("Gate duplicado: {name}/{target}"));
        }
    }
    if records.keys().cloned().collect::<BTreeSet<_>>() != *required {
        return Err("Matriz cumulativa de gates ausente, extra ou com target incorreto".into());
    }
    Ok(records)
}

fn safe_name(name: &str, nested: bool) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || (!nested && name.contains('/')) {
        return Err(format!("Nome inválido: {name:?}"));
    }
    for part in name.split('/') {
        let upper = part.split('.').next().unwrap_or("").to_ascii_uppercase();
        if part.is_empty()
            || matches!(part, "." | "..")
            || part.ends_with('.')
            || !part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            || matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || (upper.len() == 4
                && (upper.starts_with("COM") || upper.starts_with("LPT"))
                && (b'1'..=b'9').contains(&upper.as_bytes()[3]))
        {
            return Err(format!("Caminho não portátil ou inseguro: {name:?}"));
        }
    }
    Ok(())
}

fn inventory(
    value: &Value,
    max_size: u64,
    nested: bool,
) -> Result<BTreeMap<String, Proof>, String> {
    let entries = value.as_array().ok_or("Inventário deve ser array")?;
    if entries.len() > MAX_ENTRIES {
        return Err("Inventário excessivo".into());
    }
    let mut result = BTreeMap::new();
    let mut folded = BTreeSet::new();
    for entry in entries {
        let name = text(entry, "name")?;
        safe_name(name, nested)?;
        let size = entry["size"]
            .as_u64()
            .filter(|n| *n <= max_size)
            .ok_or_else(|| format!("Tamanho inválido no inventário: {name}"))?;
        let sha256 = text(entry, "sha256")?;
        require_hex(sha256, 64, name)?;
        if !folded.insert(name.to_ascii_lowercase())
            || result
                .insert(
                    name.to_owned(),
                    Proof {
                        size,
                        sha256: sha256.to_owned(),
                    },
                )
                .is_some()
        {
            return Err(format!("Nome duplicado no inventário: {name}"));
        }
    }
    Ok(result)
}

fn reject_link(path: &Path, metadata: &Metadata) -> Result<(), String> {
    #[cfg(windows)]
    let reparse = {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    };
    #[cfg(not(windows))]
    let reparse = false;
    if metadata.file_type().is_symlink() || reparse {
        return Err(format!("Link/reparse point recusado: {}", path.display()));
    }
    Ok(())
}

fn directory_proofs(
    directory: &Path,
    expected: &BTreeSet<String>,
) -> Result<BTreeMap<String, Proof>, String> {
    let absolute = if directory.is_absolute() {
        directory.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(directory)
    };
    for parent in absolute.ancestors() {
        let metadata =
            fs::symlink_metadata(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
        reject_link(parent, &metadata)?;
        if !metadata.is_dir() {
            return Err(format!("Não é diretório: {}", parent.display()));
        }
    }
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "Asset com nome não Unicode")?;
        if !expected.contains(&name) {
            return Err(format!("Asset inesperado: {name}"));
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(|e| e.to_string())?;
        reject_link(&entry.path(), &metadata)?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_ASSET_BYTES {
            return Err(format!("Asset vazio, não regular ou excessivo: {name}"));
        }
        let (sha256, _) = hash_reader(
            File::open(entry.path()).map_err(|e| e.to_string())?,
            metadata.len(),
            false,
        )?;
        result.insert(
            name,
            Proof {
                size: metadata.len(),
                sha256,
            },
        );
    }
    if result.keys().cloned().collect::<BTreeSet<_>>() != *expected {
        return Err("Assets obrigatórios ausentes".into());
    }
    Ok(result)
}

fn hash_reader(
    reader: impl Read,
    expected: u64,
    capture: bool,
) -> Result<(String, Vec<u8>), String> {
    let mut reader = reader.take(expected.checked_add(1).ok_or("Overflow de leitura")?);
    let mut hasher = Sha256::new();
    let mut bytes = Vec::new();
    let mut count = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buffer)
            .map_err(|e| format!("Leitura/hash: {e}"))?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .ok_or("Overflow de bytes lidos")?;
        if count > expected {
            return Err("Conteúdo maior que o tamanho declarado".into());
        }
        hasher.update(&buffer[..n]);
        if capture {
            bytes.extend_from_slice(&buffer[..n]);
        }
    }
    if count != expected {
        return Err("Conteúdo truncado".into());
    }
    Ok((format!("{:x}", hasher.finalize()), bytes))
}

fn read_small(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    reject_link(path, &metadata)?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(format!(
            "Arquivo não regular ou excessivo: {}",
            path.display()
        ));
    }
    hash_reader(
        File::open(path).map_err(|e| e.to_string())?,
        metadata.len(),
        true,
    )
    .map(|(_, bytes)| bytes)
}

fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_slice(&read_small(path, MAX_JSON_BYTES)?)
        .map_err(|e| format!("{}: {e}", path.display()))
}

fn verify_checksums(directory: &Path, proofs: &BTreeMap<String, Proof>) -> Result<(), String> {
    let bytes = read_small(&directory.join("SHA256SUMS"), 16 * 1024)?;
    let body = std::str::from_utf8(&bytes).map_err(|e| e.to_string())?;
    let mut seen = BTreeSet::new();
    for line in body.lines() {
        let (digest, name) = line
            .split_once("  ")
            .ok_or("Linha inválida em SHA256SUMS")?;
        require_hex(digest, 64, "Checksum")?;
        safe_name(name, false)?;
        if name == "SHA256SUMS"
            || !seen.insert(name)
            || !proofs.get(name).is_some_and(|p| p.sha256 == digest)
        {
            return Err(format!("Checksum ausente, duplicado ou divergente: {name}"));
        }
    }
    if seen.len() + 1 != proofs.len() {
        return Err("SHA256SUMS incompleto".into());
    }
    Ok(())
}

// ZipArchive pode indexar entradas pelo nome. O diretório central bruto é
// conferido antes para não esconder duplicatas nem aceitar metadados ilimitados.
fn zip_names(file: &mut File) -> Result<Vec<String>, String> {
    let size = file.metadata().map_err(|e| e.to_string())?.len();
    if !(22..=MAX_EVIDENCE_ZIP_BYTES).contains(&size) {
        return Err("Tamanho do ZIP de evidências inválido".into());
    }
    let tail_size = size.min(65_557) as usize;
    file.seek(SeekFrom::End(-(tail_size as i64)))
        .map_err(|e| e.to_string())?;
    let mut tail = vec![0; tail_size];
    file.read_exact(&mut tail).map_err(|e| e.to_string())?;
    let end = (0..=tail_size - 22)
        .rev()
        .find(|offset| {
            tail[*offset..].starts_with(b"PK\x05\x06")
                && *offset + 22 + u16_at(&tail, *offset + 20) as usize == tail_size
        })
        .ok_or("Fim do diretório ZIP inválido")?;
    let entries = u16_at(&tail, end + 10) as usize;
    let central_size = u32_at(&tail, end + 12) as u64;
    let central_offset = u32_at(&tail, end + 16) as u64;
    let end_offset = size - tail_size as u64 + end as u64;
    if u16_at(&tail, end + 4) != 0
        || u16_at(&tail, end + 6) != 0
        || u16_at(&tail, end + 8) as usize != entries
        || entries == 0
        || entries > MAX_ENTRIES
        || central_size > MAX_JSON_BYTES
        || central_size == u32::MAX as u64
        || central_offset == u32::MAX as u64
        || central_offset.checked_add(central_size) != Some(end_offset)
    {
        return Err("ZIP multipartes, ZIP64 ou diretório excessivo/incoerente".into());
    }
    file.seek(SeekFrom::Start(central_offset))
        .map_err(|e| e.to_string())?;
    let mut central = vec![0; central_size as usize];
    file.read_exact(&mut central).map_err(|e| e.to_string())?;
    let mut at = 0_usize;
    let mut names = Vec::new();
    let mut seen = BTreeSet::new();
    for _ in 0..entries {
        if central.len().saturating_sub(at) < 46 || !central[at..].starts_with(b"PK\x01\x02") {
            return Err("Entrada central ZIP truncada ou inválida".into());
        }
        let name_size = u16_at(&central, at + 28) as usize;
        let next = at
            + 46
            + name_size
            + u16_at(&central, at + 30) as usize
            + u16_at(&central, at + 32) as usize;
        if next > central.len() || name_size > MAX_NAME_BYTES {
            return Err("Nome/metadados ZIP excessivos ou truncados".into());
        }
        let extra_start = at + 46 + name_size;
        let extra_end = extra_start + u16_at(&central, at + 30) as usize;
        validate_zip_extra(&central[extra_start..extra_end])?;
        let name = std::str::from_utf8(&central[at + 46..at + 46 + name_size])
            .map_err(|_| "Nome ZIP não UTF-8")?;
        safe_name(name, true)?;
        let attributes = u32_at(&central, at + 38);
        let mode = attributes >> 16;
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(format!("Entrada ZIP duplicada: {name}"));
        }
        if u16_at(&central, at + 8) & 1 != 0
            || ![0, 8].contains(&u16_at(&central, at + 10))
            || u16_at(&central, at + 34) != 0
            || u32_at(&central, at + 42) as u64 >= central_offset
            || u32_at(&central, at + 20) == u32::MAX
            || u32_at(&central, at + 24) as u64 > MAX_ENTRY_BYTES
            || attributes & 0x410 != 0
            || !matches!(mode & 0o170000, 0 | 0o100000)
        {
            return Err(format!(
                "Entrada ZIP criptografada, link, especial ou excessiva: {name}"
            ));
        }
        names.push(name.to_owned());
        at = next;
    }
    if at != central.len() {
        return Err("Entradas ZIP não correspondem ao diretório declarado".into());
    }
    file.rewind().map_err(|e| e.to_string())?;
    Ok(names)
}

fn validate_zip_extra(extra: &[u8]) -> Result<(), String> {
    let mut at = 0;
    while at < extra.len() {
        if extra.len() - at < 4 {
            return Err("Cabeçalho de extra field ZIP truncado".into());
        }
        let id = u16_at(extra, at);
        let end = at + 4 + u16_at(extra, at + 2) as usize;
        if end > extra.len() {
            return Err("Conteúdo de extra field ZIP truncado".into());
        }
        // A biblioteca pode aplicar estes u64 mesmo sem sentinelas nos campos
        // principais. Recusar o ID preserva os limites e o contrato sem ZIP64.
        if id == 0x0001 {
            return Err("Extra field ZIP64 recusado".into());
        }
        at = end;
    }
    Ok(())
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}
fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn verify_evidence(
    path: &Path,
    evidence: &BTreeMap<String, Proof>,
    gates: &BTreeMap<GateKey, &Value>,
) -> Result<(), String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let names = zip_names(&mut file)?;
    if names.len() != evidence.len() {
        return Err("Quantidade de entradas ZIP divergente do inventário".into());
    }
    let mut archive = zip::ZipArchive::new(file).map_err(|e| format!("ZIP inválido: {e}"))?;
    if archive.len() != names.len() {
        return Err("Índice ZIP esconde entradas duplicadas".into());
    }
    let mut receipts = BTreeSet::new();
    for (index, name) in names.iter().enumerate() {
        let mut entry = archive
            .by_index(index)
            .map_err(|e| format!("Entrada ZIP: {e}"))?;
        let proof = evidence
            .get(name)
            .ok_or_else(|| format!("Entrada ZIP inesperada: {name}"))?;
        if entry.name_raw() != name.as_bytes() || entry.size() != proof.size {
            return Err(format!("Nome/tamanho ZIP divergente: {name}"));
        }
        let basename = name.rsplit('/').next().unwrap_or(name);
        let is_receipt = basename.starts_with("receipt-") && basename.ends_with(".json");
        if is_receipt && proof.size > MAX_JSON_BYTES {
            return Err("Recibo ZIP excessivo".into());
        }
        let (sha256, bytes) = hash_reader(&mut entry, proof.size, is_receipt)?;
        if sha256 != proof.sha256 {
            return Err(format!("Hash da evidência divergente: {name}"));
        }
        if is_receipt {
            let receipt: Value =
                serde_json::from_slice(&bytes).map_err(|e| format!("Recibo {name}: {e}"))?;
            let gate = text(&receipt, "gate")?;
            let key = (gate.to_owned(), text(&receipt, "target")?.to_owned());
            if basename != format!("receipt-{gate}.json")
                || gates.get(&key).copied() != Some(&receipt)
                || !receipts.insert(key)
            {
                return Err(format!(
                    "Recibo ausente, duplicado ou divergente do gate: {name}"
                ));
            }
        }
    }
    if receipts != gates.keys().cloned().collect::<BTreeSet<_>>() {
        return Err("ZIP não contém exatamente os recibos dos gates declarados".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use zip::write::SimpleFileOptions;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
        plan: Value,
        manifest: Value,
        entries: Vec<(String, Vec<u8>)>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Apenas o diretório exclusivo criado pelo próprio teste.
            assert_eq!(self.root.parent(), Some(std::env::temp_dir().as_path()));
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    impl Fixture {
        fn new(version: &str) -> Self {
            let root = loop {
                let root = std::env::temp_dir().join(format!(
                    "sider-artifact-check-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                match fs::create_dir(&root) {
                    Ok(()) => break root,
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(e) => panic!("Temp fixture: {e}"),
                }
            };
            let plan = json!({"repository":"example/sider", "reference":{"image":"redis:fixed"},
            "release_policy":{"targets":[LINUX,WINDOWS], "docker_since":"0.10.0",
                "candidate_fuzz_seconds":900,"stable_soak_seconds":3600},
            "releases":[
                {"version":"0.1.0","required_gates":["native","tcp_smoke","compatibility","fuzz"]},
                {"version":"0.3.0","required_gates":["crash","recovery","migration"]},
                {"version":"0.10.0","required_gates":["docker"]}
            ]});
            let (base, candidate) = parse_version(version).unwrap();
            let (required, docker) = required_gates(&plan, base).unwrap();
            let gates: Vec<Value> = required
                .into_iter()
                .map(|(gate, target)| {
                    let mut record = json!({"schema_version":1,"gate":gate,"target":target,
                    "version":version,"sha":SHA,"status":"success","cases":1});
                    if matches!(record["gate"].as_str().unwrap(), "fuzz" | "compatibility") {
                        record["reference_image"] = json!("redis:fixed");
                    }
                    if record["gate"] == "fuzz" {
                        record["duration_seconds"] = json!(900.25);
                    }
                    record
                })
                .collect();
            let entries = gates
                .iter()
                .map(|gate| {
                    let folder = if gate["target"] == WINDOWS {
                        "windows"
                    } else {
                        "linux"
                    };
                    (
                        format!("{folder}/receipt-{}.json", gate["gate"].as_str().unwrap()),
                        serde_json::to_vec(gate).unwrap(),
                    )
                })
                .chain([
                    ("nested/empty.log".into(), Vec::new()),
                    ("nested/binary.log".into(), vec![0, 255, 13, 10]),
                ])
                .collect();
            let approved = if candidate {
                Value::Null
            } else {
                json!({"tag":format!("v{version}-rc.1"),"sha":SHA,"manifest_sha256":"ab".repeat(32),
                    "release":"https://github.com/example/sider/releases/tag/candidate",
                    "approval":"https://github.com/example/sider/issues/1#issuecomment-1",
                    "published_at":"2026-09-08T00:00:00Z","version_only_cargo_diff":true})
            };
            let manifest = json!({"schema_version":1,"repository":"example/sider","private":true,
                "version":version,"tag":format!("v{version}"),"sha":SHA,"prerelease":candidate,
                "provenance":{"kind":"manual-local","ci_enabled":false,"github_actions_run":null,"frozen_checkout":SHA},
                "approved_candidate":approved,"reference":plan["reference"],"targets":[LINUX,WINDOWS],
                "gates":gates,"evidence_archive":format!("sider-v{version}-validation.zip"),
                "evidence_files":[],"artifacts":[],"notes":"release-notes.md",
                "runtime_requirements":"runtime-requirements.md","checksums":"SHA256SUMS",
                "license":"MIT","crate_publication":false});
            for name in [
                format!("sider-v{version}-{WINDOWS}.zip"),
                format!("sider-v{version}-{LINUX}.tar.gz"),
            ] {
                fs::write(
                    root.join(name),
                    b"opaque package bytes: offline check does not execute binaries",
                )
                .unwrap();
            }
            if docker {
                fs::write(
                    root.join(format!("sider-v{version}-linux-amd64-image.tar.gz")),
                    b"opaque image bytes",
                )
                .unwrap();
            }
            fs::write(
                root.join("release-notes.md"),
                format!("# Sider v{version}\nNotas verificadas.\n"),
            )
            .unwrap();
            fs::write(
                root.join("runtime-requirements.md"),
                b"Local runtime requirements\n",
            )
            .unwrap();
            fs::write(
                root.join("release-preflight.json"),
                serde_json::to_vec(&json!({
                    "repository":{"nameWithOwner":"example/sider","isPrivate":true},
                    "pull_request":{"mergeCommit":{"oid":SHA}}
                }))
                .unwrap(),
            )
            .unwrap();
            let mut fixture = Self {
                root,
                plan,
                manifest,
                entries,
            };
            fixture.write_zip(true);
            fixture.refresh();
            fixture
        }

        fn version(&self) -> &str {
            self.manifest["version"].as_str().unwrap()
        }

        fn zip_path(&self) -> PathBuf {
            self.root
                .join(self.manifest["evidence_archive"].as_str().unwrap())
        }

        fn write_zip(&mut self, update_inventory: bool) {
            let mut writer = zip::ZipWriter::new(File::create(self.zip_path()).unwrap());
            let options =
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
            for (name, bytes) in &self.entries {
                writer.start_file(name, options).unwrap();
                writer.write_all(bytes).unwrap();
            }
            writer.finish().unwrap();
            if update_inventory {
                self.manifest["evidence_files"] = json!(self.entries.iter().map(|(name, bytes)|
                    json!({"name":name,"size":bytes.len(),"sha256":digest(bytes)})).collect::<Vec<_>>());
            }
        }

        fn refresh(&mut self) {
            let mut assets = Vec::new();
            for entry in fs::read_dir(&self.root).unwrap() {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_str().unwrap();
                if matches!(name, "release-manifest.json" | "SHA256SUMS") {
                    continue;
                }
                let bytes = fs::read(&path).unwrap();
                assets.push(json!({"name":name,"size":bytes.len(),"sha256":digest(&bytes)}));
            }
            self.manifest["artifacts"] = json!(assets);
            self.write_manifest_and_checksums();
        }

        fn write_manifest_and_checksums(&self) {
            fs::write(
                self.root.join("release-manifest.json"),
                serde_json::to_vec_pretty(&self.manifest).unwrap(),
            )
            .unwrap();
            let mut rows = BTreeMap::new();
            for entry in fs::read_dir(&self.root).unwrap() {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_str().unwrap();
                if name != "SHA256SUMS" {
                    rows.insert(name.to_owned(), digest(&fs::read(&path).unwrap()));
                }
            }
            fs::write(
                self.root.join("SHA256SUMS"),
                rows.into_iter()
                    .map(|(name, hash)| format!("{hash}  {name}\n"))
                    .collect::<String>(),
            )
            .unwrap();
        }

        fn check(&self) -> Result<Value, String> {
            verify(&self.plan, self.version(), SHA, &self.root)
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn valid_rc_and_final_report_integrity_not_publication_authority() {
        for version in ["0.1.0-rc.1", "0.1.0"] {
            let fixture = Fixture::new(version);
            let result = fixture.check().unwrap();
            assert_eq!(result["status"], "integrity_verified");
            assert_eq!(result["publication_authorized"], false);
            assert_eq!(result["gate_records"], 6);
            assert_eq!(result["assets"], 8);
        }
    }

    #[test]
    fn cumulative_gates_require_both_systems_for_persistence_and_docker_asset() {
        for (version, count, assets) in [("0.3.0-rc.1", 12, 8), ("0.10.0-rc.1", 13, 9)] {
            let mut fixture = Fixture::new(version);
            let result = fixture.check().unwrap();
            assert_eq!(result["gate_records"], count);
            assert_eq!(result["assets"], assets);
            fixture.manifest["gates"]
                .as_array_mut()
                .unwrap()
                .retain(|gate| gate["gate"] != "crash" || gate["target"] != WINDOWS);
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().unwrap_err().contains("Matriz cumulativa"));
        }
    }

    #[test]
    fn changed_bytes_wrong_sha_and_missing_asset_are_rejected() {
        let fixture = Fixture::new("0.1.0-rc.1");
        assert!(
            verify(
                &fixture.plan,
                fixture.version(),
                &"a".repeat(40),
                &fixture.root
            )
            .is_err()
        );
        fs::write(
            fixture.root.join("runtime-requirements.md"),
            b"changed bytes",
        )
        .unwrap();
        assert!(fixture.check().unwrap_err().contains("Checksum"));
        fs::remove_file(fixture.root.join("runtime-requirements.md")).unwrap();
        assert!(fixture.check().unwrap_err().contains("ausentes"));
    }

    #[test]
    fn checksum_missing_duplicate_and_self_reference_are_rejected() {
        for kind in 0..3 {
            let fixture = Fixture::new("0.1.0-rc.1");
            let path = fixture.root.join("SHA256SUMS");
            let original = fs::read_to_string(&path).unwrap();
            let first = original.lines().next().unwrap();
            let changed = match kind {
                0 => original.lines().skip(1).map(|s| format!("{s}\n")).collect(),
                1 => format!("{original}{first}\n"),
                _ => format!("{original}{}  SHA256SUMS\n", "0".repeat(64)),
            };
            fs::write(path, changed).unwrap();
            assert!(fixture.check().is_err());
        }
    }

    #[test]
    fn missing_duplicate_wrong_target_zero_or_failed_gate_is_rejected() {
        for kind in 0..6 {
            let mut fixture = Fixture::new("0.1.0-rc.1");
            let gates = fixture.manifest["gates"].as_array_mut().unwrap();
            match kind {
                0 => {
                    gates.pop();
                }
                1 => gates.push(gates[0].clone()),
                2 => gates[0]["target"] = json!("unknown"),
                3 => gates[0]["cases"] = json!(0),
                4 => gates[0]["status"] = json!("skipped"),
                _ => gates[0]["cases"] = json!(true),
            }
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().is_err(), "mutation {kind}");
        }
    }

    #[test]
    fn short_fuzz_and_fabricated_manifest_receipt_disagreement_are_rejected() {
        let mut fixture = Fixture::new("0.1.0-rc.1");
        let index = fixture.manifest["gates"]
            .as_array()
            .unwrap()
            .iter()
            .position(|g| g["gate"] == "fuzz")
            .unwrap();
        fixture.manifest["gates"][index]["duration_seconds"] = json!(899.9);
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("Duração"));
        fixture.manifest["gates"][index]["duration_seconds"] = json!(999.0);
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("Recibo"));
    }

    #[test]
    fn changed_zip_payload_is_rejected_even_with_updated_outer_checksums() {
        let mut fixture = Fixture::new("0.1.0-rc.1");
        fixture.entries.last_mut().unwrap().1[1] = 254;
        fixture.write_zip(false);
        fixture.refresh();
        assert!(fixture.check().unwrap_err().contains("Hash da evidência"));
    }

    #[test]
    fn missing_and_duplicate_inventory_entries_are_rejected() {
        for field in ["artifacts", "evidence_files"] {
            let mut fixture = Fixture::new("0.1.0-rc.1");
            let array = fixture.manifest[field].as_array_mut().unwrap();
            array.push(array[0].clone());
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().unwrap_err().contains("duplicado"));
            fixture.manifest[field].as_array_mut().unwrap().pop();
            fixture.manifest[field].as_array_mut().unwrap().pop();
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().is_err());
        }
    }

    #[test]
    fn unsafe_paths_and_nonportable_aliases_are_rejected() {
        for name in [
            "../outside",
            "/absolute",
            "a/../b",
            "a//b",
            "C:/drive",
            "a\\b",
            "a\0b",
            "a/",
            "a/CON.txt",
            "COM1",
            "a.",
            "a\u{202e}b",
        ] {
            assert!(safe_name(name, true).is_err(), "{name:?}");
        }
        let values = json!([{"name":"a/log.txt","size":0,"sha256":digest(b"")},
            {"name":"A/LOG.TXT","size":0,"sha256":digest(b"")}]);
        assert!(inventory(&values, MAX_ENTRY_BYTES, true).is_err());
    }

    #[test]
    fn zip_duplicate_names_are_not_hidden_by_the_library_index() {
        let mut fixture = Fixture::new("0.1.0-rc.1");
        fixture.entries.extend([
            ("extra/a.txt".into(), b"a".to_vec()),
            ("extra/b.txt".into(), b"b".to_vec()),
        ]);
        fixture.write_zip(true);
        let mut bytes = fs::read(fixture.zip_path()).unwrap();
        for at in 0..bytes.len() - 11 {
            if &bytes[at..at + 11] == b"extra/b.txt" {
                bytes[at + 6] = b'a';
            }
        }
        fs::write(fixture.zip_path(), bytes).unwrap();
        fixture.refresh();
        assert!(fixture.check().unwrap_err().contains("ZIP duplicada"));
    }

    #[test]
    fn zip_symlink_oversized_entry_and_zip64_are_rejected_before_decompression() {
        for kind in 0..3 {
            let mut fixture = Fixture::new("0.1.0-rc.1");
            let mut bytes = fs::read(fixture.zip_path()).unwrap();
            let central = bytes.windows(4).position(|s| s == b"PK\x01\x02").unwrap();
            match kind {
                0 => bytes[central + 38..central + 42]
                    .copy_from_slice(&(0o120777_u32 << 16).to_le_bytes()),
                1 => bytes[central + 24..central + 28]
                    .copy_from_slice(&((MAX_ENTRY_BYTES + 1) as u32).to_le_bytes()),
                _ => {
                    let end = bytes.windows(4).rposition(|s| s == b"PK\x05\x06").unwrap();
                    bytes[end + 10..end + 12].copy_from_slice(&u16::MAX.to_le_bytes());
                }
            }
            fs::write(fixture.zip_path(), bytes).unwrap();
            fixture.refresh();
            assert!(fixture.check().is_err(), "mutation {kind}");
        }
    }

    fn insert_central_extra(fixture: &mut Fixture, extra: &[u8]) {
        let mut bytes = fs::read(fixture.zip_path()).unwrap();
        let central = bytes.windows(4).position(|s| s == b"PK\x01\x02").unwrap();
        let end = bytes.windows(4).rposition(|s| s == b"PK\x05\x06").unwrap();
        let old_extra_size = u16_at(&bytes, central + 30);
        let insert_at =
            central + 46 + u16_at(&bytes, central + 28) as usize + old_extra_size as usize;
        let new_extra_size = old_extra_size + u16::try_from(extra.len()).unwrap();
        let new_central_size = u32_at(&bytes, end + 12) + u32::try_from(extra.len()).unwrap();
        bytes[central + 30..central + 32].copy_from_slice(&new_extra_size.to_le_bytes());
        bytes[end + 12..end + 16].copy_from_slice(&new_central_size.to_le_bytes());
        bytes.splice(insert_at..insert_at, extra.iter().copied());
        fs::write(fixture.zip_path(), bytes).unwrap();
        fixture.refresh();
    }

    #[test]
    fn zip64_extra_without_sentinels_is_rejected() {
        let mut fixture = Fixture::new("0.1.0-rc.1");
        let bytes = fs::read(fixture.zip_path()).unwrap();
        let central = bytes.windows(4).position(|s| s == b"PK\x01\x02").unwrap();
        let size = u32_at(&bytes, central + 24);
        let compressed = u32_at(&bytes, central + 20);
        let offset = u32_at(&bytes, central + 42);
        assert!(size < u32::MAX && compressed < u32::MAX && offset < u32::MAX);
        let mut extra = vec![1, 0, 24, 0];
        for value in [size, compressed, offset] {
            extra.extend_from_slice(&u64::from(value).to_le_bytes());
        }
        insert_central_extra(&mut fixture, &extra);
        assert!(fixture.check().unwrap_err().contains("Extra field ZIP64"));
    }

    #[test]
    fn central_extra_fields_have_bounded_headers_and_payloads() {
        for malformed in [
            vec![0xfe, 0xca, 0],
            vec![0xfe, 0xca, 4, 0, 1],
            vec![0xfe, 0xca, 0, 0, 1],
        ] {
            let mut fixture = Fixture::new("0.1.0-rc.1");
            insert_central_extra(&mut fixture, &malformed);
            assert!(
                fixture
                    .check()
                    .unwrap_err()
                    .contains("extra field ZIP truncado")
            );
        }
        let mut fixture = Fixture::new("0.1.0-rc.1");
        insert_central_extra(&mut fixture, &[0xfe, 0xca, 0, 0]);
        fixture.check().unwrap();
    }

    #[test]
    fn final_requires_same_base_candidate_claim_without_authenticating_it() {
        let mut fixture = Fixture::new("0.1.0");
        fixture.manifest["approved_candidate"]["tag"] = json!("v0.2.0-rc.1");
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("versão-base"));
        fixture.manifest["approved_candidate"] = Value::Null;
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().is_err());
    }

    #[test]
    fn versions_and_resource_limits_fail_closed() {
        for version in [
            "v0.1.0",
            "0.1",
            "01.1.0",
            "0.1.0-rc.0",
            "0.1.0-rc.01",
            "0.1.0+meta",
            "0.1.0-rc.1-rc.2",
        ] {
            assert!(parse_version(version).is_err(), "{version}");
        }
        let value = json!([{"name":"x","size":MAX_ENTRY_BYTES+1,"sha256":digest(b"")}]);
        assert!(inventory(&value, MAX_ENTRY_BYTES, true).is_err());
        let fixture = Fixture::new("0.1.0-rc.1");
        assert!(verify(&fixture.plan, "0.2.0-rc.1", SHA, &fixture.root).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;
        let fixture = Fixture::new("0.1.0-rc.1");
        let path = fixture.root.join("runtime-requirements.md");
        fs::remove_file(&path).unwrap();
        symlink("release-notes.md", &path).unwrap();
        assert!(fixture.check().unwrap_err().contains("Link/reparse"));
    }
}
