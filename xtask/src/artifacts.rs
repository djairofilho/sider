//! Offline integrity of release files, without execution or publication.

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

/// Verifies the same bundle for an RC identifier or its final version.
///
/// Does not query GitHub, run tests, extract packages, or authorize publication.
/// RC approval and equality of published assets are checked outside the
/// bundle; its files and receipts always use the final binary version.
/// The directory must remain unchanged during reading. ZIP64, links, and
/// nonportable names are rejected by the restricted evidence archive format.
pub fn verify(
    plan: &Value,
    release_identifier: &str,
    sha: &str,
    directory: &Path,
) -> Result<Value, String> {
    let (base, candidate) = parse_version(release_identifier)?;
    let artifact_version = format!("{}.{}.{}", base[0], base[1], base[2]);
    require_hex(sha, 40, "release SHA")?;
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
    let evidence_name = format!("sider-v{artifact_version}-validation.zip");
    expected.extend([
        evidence_name.clone(),
        format!("sider-v{artifact_version}-{WINDOWS}.zip"),
        format!("sider-v{artifact_version}-{LINUX}.tar.gz"),
    ]);
    if docker {
        expected.insert(format!(
            "sider-v{artifact_version}-linux-amd64-image.tar.gz"
        ));
    }
    let proofs = directory_proofs(directory, &expected)?;
    verify_checksums(directory, &proofs)?;
    let manifest = read_json(&directory.join("release-manifest.json"))?;
    verify_identity(plan, &manifest, &artifact_version, sha)?;
    if text(&manifest, "evidence_archive")? != evidence_name
        || text(&manifest, "notes")? != "release-notes.md"
        || text(&manifest, "runtime_requirements")? != "runtime-requirements.md"
        || text(&manifest, "checksums")? != "SHA256SUMS"
    {
        return Err("Manifest file name mismatch".into());
    }
    let manifest_assets = inventory(&manifest["artifacts"], MAX_ASSET_BYTES, false)?;
    let expected_inventory: BTreeSet<_> = expected
        .iter()
        .filter(|name| !matches!(name.as_str(), "SHA256SUMS" | "release-manifest.json"))
        .cloned()
        .collect();
    if manifest_assets.keys().cloned().collect::<BTreeSet<_>>() != expected_inventory {
        return Err("Incomplete or unexpected asset inventory".into());
    }
    for (name, proof) in &manifest_assets {
        if proofs.get(name) != Some(proof) {
            return Err(format!("Asset differs from manifest: {name}"));
        }
    }
    let gates = gate_records(plan, &manifest, &required, &artifact_version, sha)?;
    let evidence = inventory(&manifest["evidence_files"], MAX_ENTRY_BYTES, true)?;
    if evidence.is_empty() || evidence.len() > MAX_ENTRIES {
        return Err("Invalid evidence count".into());
    }
    let expanded_bytes = evidence.values().try_fold(0_u64, |total, proof| {
        total
            .checked_add(proof.size)
            .filter(|size| *size <= MAX_EXPANDED_BYTES)
            .ok_or("Evidence exceeds the total byte limit")
    })?;
    verify_evidence(&directory.join(&evidence_name), &evidence, &gates)?;
    for name in [
        "release-notes.md",
        "runtime-requirements.md",
        "release-preflight.json",
    ] {
        let bytes = read_small(&directory.join(name), MAX_JSON_BYTES)?;
        let body =
            std::str::from_utf8(&bytes).map_err(|e| format!("{name}: invalid UTF-8: {e}"))?;
        if body.contains(['\u{fffd}', '\u{00c3}', '\u{00c2}', '\u{0007}']) {
            return Err(format!("{name}: text with possible mojibake"));
        }
    }
    let preflight = read_json(&directory.join("release-preflight.json"))?;
    if preflight["repository"]["nameWithOwner"] != plan["repository"]
        || preflight["repository"]["isPrivate"] != true
        || preflight["pull_request"]["mergeCommit"]["oid"] != sha
    {
        return Err("Local preflight differs from declared identity".into());
    }
    // A second read detects common changes during verification; it does not
    // provide an atomic snapshot against a concurrent adversarial writer.
    if directory_proofs(directory, &expected)? != proofs {
        return Err("Files changed during verification".into());
    }
    Ok(json!({
        "status": "integrity_verified", "release_identifier": release_identifier,
        "artifact_version": artifact_version, "sha": sha,
        "prerelease": candidate, "assets": proofs.len(), "checksums": proofs.len() - 1,
        "gate_records": gates.len(), "evidence_files": evidence.len(),
        "evidence_expanded_bytes": expanded_bytes, "publication_authorized": false,
        "scope": "Offline integrity and consistency; does not establish execution, authenticity, RC approval, promotion of the same assets, or current GitHub state.",
        "limits": {"asset_bytes": MAX_ASSET_BYTES, "json_bytes": MAX_JSON_BYTES,
            "evidence_zip_bytes": MAX_EVIDENCE_ZIP_BYTES, "entry_bytes": MAX_ENTRY_BYTES,
            "expanded_bytes": MAX_EXPANDED_BYTES, "entries": MAX_ENTRIES, "zip64": false}
    }))
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("Missing or invalid text field: {field}"))
}

fn require_hex(value: &str, size: usize, what: &str) -> Result<(), String> {
    if value.len() != size
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "{what}: {size} lowercase hexadecimal characters required"
        ));
    }
    Ok(())
}

fn parse_version(version: &str) -> Result<([u64; 3], bool), String> {
    if version.len() > 64 {
        return Err("Version too long".into());
    }
    let (base, candidate) = if let Some((base, rc)) = version.split_once("-rc.") {
        if number(rc)? == 0 {
            return Err("RC must have a positive number".into());
        }
        (base, true)
    } else {
        (version, false)
    };
    let parts = base.split('.').map(number).collect::<Result<Vec<_>, _>>()?;
    let numbers = parts
        .try_into()
        .map_err(|_| "Version must be MAJOR.MINOR.PATCH[-rc.N]".to_owned())?;
    Ok((numbers, candidate))
}

fn number(value: &str) -> Result<u64, String> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err("Invalid numeric version component".into());
    }
    value.parse().map_err(|_| "Version overflow".into())
}

fn required_gates(plan: &Value, base: [u64; 3]) -> Result<(BTreeSet<GateKey>, bool), String> {
    let policy = &plan["release_policy"];
    if plan["schema_version"] != 2
        || policy["private"] != false
        || policy["publish_crate"] != false
        || policy["final_promotion"] != "same_sha_same_assets"
        || policy["bundle_change_requires_new_candidate"] != true
    {
        return Err("Plan must require public publication and immutable bundle promotion".into());
    }
    let targets = plan["release_policy"]["targets"]
        .as_array()
        .ok_or("Missing targets in plan")?;
    if targets.len() != 2 || !targets.contains(&json!(LINUX)) || !targets.contains(&json!(WINDOWS))
    {
        return Err("Plan targets are not Linux GNU and Windows MSVC".into());
    }
    let releases = plan["releases"]
        .as_array()
        .ok_or("Missing releases in plan")?;
    let mut versions = BTreeSet::new();
    let mut names: BTreeSet<&str> = ["native", "tcp_smoke", "compatibility"].into();
    for release in releases {
        let (release_version, candidate) = parse_version(text(release, "version")?)?;
        if candidate || !versions.insert(release_version) {
            return Err("Duplicate version or RC in plan".into());
        }
        if release_version == base && release["publication"] != true {
            return Err("Internal milestone does not allow publication verification".into());
        }
        if release_version <= base {
            for gate in release["required_gates"]
                .as_array()
                .ok_or("missing required_gates")?
            {
                names.insert(gate.as_str().ok_or("Non-text gate in plan")?);
            }
        }
    }
    if !versions.contains(&base) {
        return Err("Base version not registered in plan".into());
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
            "compatibility" | "sharding" | "types" | "sorted_sets" | "transactions" | "pubsub"
            | "replication" | "docker" | "soak" | "benchmarks" => &[LINUX],
            _ => return Err(format!("Unknown gate: {name}")),
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
    artifact_version: &str,
    sha: &str,
) -> Result<(), String> {
    if manifest["schema_version"] != 2
        || manifest["artifact_version"] != artifact_version
        || manifest["sha"] != sha
        || manifest["private"] != false
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
        return Err("Invalid manifest identity, reference, or local provenance".into());
    }
    for field in [
        "version",
        "tag",
        "prerelease",
        "approved_candidate",
        "release_identifier",
    ] {
        if manifest.get(field).is_some() {
            return Err(format!(
                "Publication field does not belong to the immutable bundle: {field}"
            ));
        }
    }
    let targets = manifest["targets"]
        .as_array()
        .ok_or("Missing manifest targets")?;
    if targets.len() != 2 || !targets.contains(&json!(LINUX)) || !targets.contains(&json!(WINDOWS))
    {
        return Err("Manifest target mismatch".into());
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
        .ok_or("Missing gates in manifest")?
    {
        let name = text(gate, "gate")?;
        let target = text(gate, "target")?;
        if gate["schema_version"] != 1
            || gate["sha"] != sha
            || gate["version"] != version
            || gate["status"] != "success"
            || !gate["cases"].as_u64().is_some_and(|n| n > 0)
        {
            return Err(format!("Invalid gate record: {name}/{target}"));
        }
        let minimum = match name {
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
            return Err(format!("Invalid or insufficient duration: {name}/{target}"));
        }
        if (name == "compatibility" || !gate["reference_image"].is_null())
            && gate["reference_image"] != plan["reference"]["image"]
        {
            return Err(format!("Reference mismatch in gate {name}"));
        }
        if records
            .insert((name.to_owned(), target.to_owned()), gate)
            .is_some()
        {
            return Err(format!("Duplicate gate: {name}/{target}"));
        }
    }
    if records.keys().cloned().collect::<BTreeSet<_>>() != *required {
        return Err(
            "Cumulative gate matrix has missing or extra entries, or an incorrect target".into(),
        );
    }
    Ok(records)
}

fn safe_name(name: &str, nested: bool) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || (!nested && name.contains('/')) {
        return Err(format!("Invalid name: {name:?}"));
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
            return Err(format!("Nonportable or unsafe path: {name:?}"));
        }
    }
    Ok(())
}

fn inventory(
    value: &Value,
    max_size: u64,
    nested: bool,
) -> Result<BTreeMap<String, Proof>, String> {
    let entries = value.as_array().ok_or("Inventory must be an array")?;
    if entries.len() > MAX_ENTRIES {
        return Err("Excessive inventory".into());
    }
    let mut result = BTreeMap::new();
    let mut folded = BTreeSet::new();
    for entry in entries {
        let name = text(entry, "name")?;
        safe_name(name, nested)?;
        let size = entry["size"]
            .as_u64()
            .filter(|n| *n <= max_size)
            .ok_or_else(|| format!("Invalid size in inventory: {name}"))?;
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
            return Err(format!("Duplicate name in inventory: {name}"));
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
        return Err(format!("Link/reparse point rejected: {}", path.display()));
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
            return Err(format!("Not a directory: {}", parent.display()));
        }
    }
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "Asset has a non-Unicode name")?;
        if !expected.contains(&name) {
            return Err(format!("Unexpected asset: {name}"));
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(|e| e.to_string())?;
        reject_link(&entry.path(), &metadata)?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_ASSET_BYTES {
            return Err(format!("Empty, nonregular, or oversized asset: {name}"));
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
        return Err("Missing required assets".into());
    }
    Ok(result)
}

fn hash_reader(
    reader: impl Read,
    expected: u64,
    capture: bool,
) -> Result<(String, Vec<u8>), String> {
    let mut reader = reader.take(expected.checked_add(1).ok_or("Read overflow")?);
    let mut hasher = Sha256::new();
    let mut bytes = Vec::new();
    let mut count = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buffer)
            .map_err(|e| format!("Read/hash: {e}"))?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .ok_or("Read byte count overflow")?;
        if count > expected {
            return Err("Content exceeds declared size".into());
        }
        hasher.update(&buffer[..n]);
        if capture {
            bytes.extend_from_slice(&buffer[..n]);
        }
    }
    if count != expected {
        return Err("Truncated content".into());
    }
    Ok((format!("{:x}", hasher.finalize()), bytes))
}

fn read_small(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    reject_link(path, &metadata)?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(format!("Nonregular or oversized file: {}", path.display()));
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
        let (digest, name) = line.split_once("  ").ok_or("Invalid line in SHA256SUMS")?;
        require_hex(digest, 64, "Checksum")?;
        safe_name(name, false)?;
        if name == "SHA256SUMS"
            || !seen.insert(name)
            || !proofs.get(name).is_some_and(|p| p.sha256 == digest)
        {
            return Err(format!(
                "Missing, duplicate, or mismatched checksum: {name}"
            ));
        }
    }
    if seen.len() + 1 != proofs.len() {
        return Err("Incomplete SHA256SUMS".into());
    }
    Ok(())
}

// ZipArchive may index entries by name. The raw central directory is
// checked first to avoid hiding duplicates or accepting unbounded metadata.
fn zip_names(file: &mut File) -> Result<Vec<String>, String> {
    let size = file.metadata().map_err(|e| e.to_string())?.len();
    if !(22..=MAX_EVIDENCE_ZIP_BYTES).contains(&size) {
        return Err("Invalid evidence ZIP size".into());
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
        .ok_or("Invalid ZIP end of central directory")?;
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
        return Err("Multipart ZIP, ZIP64, or excessive/inconsistent directory".into());
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
            return Err("Truncated or invalid ZIP central directory entry".into());
        }
        let name_size = u16_at(&central, at + 28) as usize;
        let next = at
            + 46
            + name_size
            + u16_at(&central, at + 30) as usize
            + u16_at(&central, at + 32) as usize;
        if next > central.len() || name_size > MAX_NAME_BYTES {
            return Err("ZIP name/metadata oversized or truncated".into());
        }
        let extra_start = at + 46 + name_size;
        let extra_end = extra_start + u16_at(&central, at + 30) as usize;
        validate_zip_extra(&central[extra_start..extra_end])?;
        let name = std::str::from_utf8(&central[at + 46..at + 46 + name_size])
            .map_err(|_| "Non-UTF-8 ZIP name")?;
        safe_name(name, true)?;
        let attributes = u32_at(&central, at + 38);
        let mode = attributes >> 16;
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate ZIP entry: {name}"));
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
                "Encrypted, linked, special, or oversized ZIP entry: {name}"
            ));
        }
        names.push(name.to_owned());
        at = next;
    }
    if at != central.len() {
        return Err("ZIP entries do not match the declared directory".into());
    }
    file.rewind().map_err(|e| e.to_string())?;
    Ok(names)
}

fn validate_zip_extra(extra: &[u8]) -> Result<(), String> {
    let mut at = 0;
    while at < extra.len() {
        if extra.len() - at < 4 {
            return Err("Truncated ZIP extra field header".into());
        }
        let id = u16_at(extra, at);
        let end = at + 4 + u16_at(extra, at + 2) as usize;
        if end > extra.len() {
            return Err("Truncated ZIP extra field content".into());
        }
        // The library may apply these u64 values even without sentinels in the
        // main fields. Rejecting the ID preserves the limits and the no-ZIP64 contract.
        if id == 0x0001 {
            return Err("ZIP64 extra field rejected".into());
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
        return Err("ZIP entry count differs from inventory".into());
    }
    let mut archive = zip::ZipArchive::new(file).map_err(|e| format!("Invalid ZIP: {e}"))?;
    if archive.len() != names.len() {
        return Err("ZIP index hides duplicate entries".into());
    }
    let mut receipts = BTreeSet::new();
    for (index, name) in names.iter().enumerate() {
        let mut entry = archive
            .by_index(index)
            .map_err(|e| format!("ZIP entry: {e}"))?;
        let proof = evidence
            .get(name)
            .ok_or_else(|| format!("Unexpected ZIP entry: {name}"))?;
        if entry.name_raw() != name.as_bytes() || entry.size() != proof.size {
            return Err(format!("ZIP name/size mismatch: {name}"));
        }
        let basename = name.rsplit('/').next().unwrap_or(name);
        let is_receipt = basename.starts_with("receipt-") && basename.ends_with(".json");
        if is_receipt && proof.size > MAX_JSON_BYTES {
            return Err("Oversized ZIP receipt".into());
        }
        let (sha256, bytes) = hash_reader(&mut entry, proof.size, is_receipt)?;
        if sha256 != proof.sha256 {
            return Err(format!("Evidence hash mismatch: {name}"));
        }
        if is_receipt {
            let receipt: Value =
                serde_json::from_slice(&bytes).map_err(|e| format!("Receipt {name}: {e}"))?;
            let gate = text(&receipt, "gate")?;
            let key = (gate.to_owned(), text(&receipt, "target")?.to_owned());
            if basename != format!("receipt-{gate}.json")
                || gates.get(&key).copied() != Some(&receipt)
                || !receipts.insert(key)
            {
                return Err(format!(
                    "Missing, duplicate, or mismatched gate receipt: {name}"
                ));
            }
        }
    }
    if receipts != gates.keys().cloned().collect::<BTreeSet<_>>() {
        return Err("ZIP does not contain exactly the declared gate receipts".into());
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
            // Only the dedicated directory created by the test itself.
            assert_eq!(self.root.parent(), Some(std::env::temp_dir().as_path()));
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    impl Fixture {
        fn new(version: &str) -> Self {
            let (base, _) = parse_version(version).unwrap();
            let artifact_version = format!("{}.{}.{}", base[0], base[1], base[2]);
            let version = artifact_version.as_str();
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
            let plan = json!({"schema_version":2,"repository":"example/sider", "reference":{"image":"redis:fixed"},
            "release_policy":{"targets":[LINUX,WINDOWS], "docker_since":"0.10.0",
                "private":false,"publish_crate":false,"final_promotion":"same_sha_same_assets",
                "bundle_change_requires_new_candidate":true,"stable_soak_seconds":3600},
            "releases":[
                {"version":"0.1.0","publication":false,"required_gates":["native","tcp_smoke","compatibility"]},
                {"version":"0.3.0","publication":false,"required_gates":["crash","recovery","migration"]},
                {"version":"0.10.0","publication":false,"required_gates":["docker"]},
                {"version":"1.0.0","publication":true,"required_gates":["sharding","types","sorted_sets",
                    "transactions","pubsub","replication","soak","benchmarks"]}
            ]});
            let (required, docker) = required_gates(&plan, base).unwrap();
            let gates: Vec<Value> = required
                .into_iter()
                .map(|(gate, target)| {
                    let mut record = json!({"schema_version":1,"gate":gate,"target":target,
                    "version":version,"sha":SHA,"status":"success","cases":1});
                    if record["gate"] == "compatibility" {
                        record["reference_image"] = json!("redis:fixed");
                    }
                    if record["gate"] == "soak" {
                        record["duration_seconds"] = json!(3600.25);
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
            let manifest = json!({"schema_version":2,"repository":"example/sider","private":false,
                "artifact_version":version,"sha":SHA,
                "provenance":{"kind":"manual-local","ci_enabled":false,"github_actions_run":null,"frozen_checkout":SHA},
                "reference":plan["reference"],"targets":[LINUX,WINDOWS],
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
                format!("# Sider v{version}\nVerified notes.\n"),
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
            self.manifest["artifact_version"].as_str().unwrap()
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
            self.check_as("1.0.0")
        }

        fn check_as(&self, release_identifier: &str) -> Result<Value, String> {
            verify(&self.plan, release_identifier, SHA, &self.root)
        }

        fn bytes(&self) -> BTreeMap<String, Vec<u8>> {
            fs::read_dir(&self.root)
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    (
                        path.file_name().unwrap().to_str().unwrap().to_owned(),
                        fs::read(path).unwrap(),
                    )
                })
                .collect()
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn same_bundle_verifies_as_rc_and_final_without_changing_any_bytes() {
        let fixture = Fixture::new("1.0.0-rc.1");
        let original = fixture.bytes();
        for (identifier, prerelease) in
            [("1.0.0-rc.1", true), ("1.0.0-rc.2", true), ("1.0.0", false)]
        {
            let result = fixture.check_as(identifier).unwrap();
            assert_eq!(result["status"], "integrity_verified");
            assert_eq!(result["publication_authorized"], false);
            assert_eq!(result["release_identifier"], identifier);
            assert_eq!(result["artifact_version"], "1.0.0");
            assert_eq!(result["sha"], SHA);
            assert_eq!(result["prerelease"], prerelease);
            assert_eq!(result["gate_records"], 20);
            assert_eq!(result["assets"], 9);
            assert_eq!(fixture.bytes(), original);
        }
        assert!(original.keys().all(|name| !name.contains("-rc.")));
    }

    #[test]
    fn cumulative_gates_require_both_systems_for_persistence_and_docker_asset() {
        for gate in ["crash", "recovery", "migration"] {
            let mut fixture = Fixture::new("1.0.0");
            fixture.manifest["gates"]
                .as_array_mut()
                .unwrap()
                .retain(|record| record["gate"] != gate || record["target"] != WINDOWS);
            fixture.write_manifest_and_checksums();
            assert!(
                fixture
                    .check()
                    .unwrap_err()
                    .contains("Cumulative gate matrix")
            );
        }
        let fixture = Fixture::new("1.0.0");
        fs::remove_file(fixture.root.join("sider-v1.0.0-linux-amd64-image.tar.gz")).unwrap();
        assert!(fixture.check().unwrap_err().contains("Missing"));
    }

    #[test]
    fn changed_bytes_wrong_sha_and_missing_asset_are_rejected() {
        let fixture = Fixture::new("1.0.0-rc.1");
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
        assert!(fixture.check().unwrap_err().contains("checksum"));
        fs::remove_file(fixture.root.join("runtime-requirements.md")).unwrap();
        assert!(fixture.check().unwrap_err().contains("Missing"));
    }

    #[test]
    fn checksum_missing_duplicate_and_self_reference_are_rejected() {
        for kind in 0..3 {
            let fixture = Fixture::new("1.0.0-rc.1");
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
    fn missing_extra_duplicate_wrong_target_zero_or_failed_gate_is_rejected() {
        for kind in 0..7 {
            let mut fixture = Fixture::new("1.0.0-rc.1");
            let gates = fixture.manifest["gates"].as_array_mut().unwrap();
            match kind {
                0 => {
                    gates.pop();
                }
                1 => gates.push(gates[0].clone()),
                2 => gates[0]["target"] = json!("unknown"),
                3 => gates[0]["cases"] = json!(0),
                4 => gates[0]["status"] = json!("skipped"),
                5 => gates[0]["cases"] = json!(true),
                _ => {
                    let mut extra = gates[0].clone();
                    extra["gate"] = json!("retired_gate");
                    gates.push(extra);
                }
            }
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().is_err(), "mutation {kind}");
        }
    }

    #[test]
    fn short_soak_and_fabricated_manifest_receipt_disagreement_are_rejected() {
        let mut fixture = Fixture::new("1.0.0-rc.1");
        let index = fixture.manifest["gates"]
            .as_array()
            .unwrap()
            .iter()
            .position(|g| g["gate"] == "soak")
            .unwrap();
        fixture.manifest["gates"][index]["duration_seconds"] = json!(3599.9);
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("duration"));
        fixture.manifest["gates"][index]["duration_seconds"] = json!(3999.0);
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("receipt"));
    }

    #[test]
    fn changed_zip_payload_is_rejected_even_with_updated_outer_checksums() {
        let mut fixture = Fixture::new("1.0.0-rc.1");
        fixture.entries.last_mut().unwrap().1[1] = 254;
        fixture.write_zip(false);
        fixture.refresh();
        assert!(fixture.check().unwrap_err().contains("Evidence hash"));
    }

    #[test]
    fn missing_and_duplicate_inventory_entries_are_rejected() {
        for field in ["artifacts", "evidence_files"] {
            let mut fixture = Fixture::new("1.0.0-rc.1");
            let array = fixture.manifest[field].as_array_mut().unwrap();
            array.push(array[0].clone());
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().unwrap_err().contains("Duplicate"));
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
        let mut fixture = Fixture::new("1.0.0-rc.1");
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
        assert!(fixture.check().unwrap_err().contains("Duplicate ZIP entry"));
    }

    #[test]
    fn zip_symlink_oversized_entry_and_zip64_are_rejected_before_decompression() {
        for kind in 0..3 {
            let mut fixture = Fixture::new("1.0.0-rc.1");
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
        let mut fixture = Fixture::new("1.0.0-rc.1");
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
        assert!(fixture.check().unwrap_err().contains("ZIP64 extra field"));
    }

    #[test]
    fn central_extra_fields_have_bounded_headers_and_payloads() {
        for malformed in [
            vec![0xfe, 0xca, 0],
            vec![0xfe, 0xca, 4, 0, 1],
            vec![0xfe, 0xca, 0, 0, 1],
        ] {
            let mut fixture = Fixture::new("1.0.0-rc.1");
            insert_central_extra(&mut fixture, &malformed);
            assert!(
                fixture
                    .check()
                    .unwrap_err()
                    .contains("Truncated ZIP extra field")
            );
        }
        let mut fixture = Fixture::new("1.0.0-rc.1");
        insert_central_extra(&mut fixture, &[0xfe, 0xca, 0, 0]);
        fixture.check().unwrap();
    }

    #[test]
    fn publication_metadata_is_rejected_inside_the_immutable_manifest() {
        for (field, value) in [
            ("version", json!("1.0.0")),
            ("tag", json!("v1.0.0-rc.1")),
            ("prerelease", json!(false)),
            ("approved_candidate", Value::Null),
            (
                "approved_candidate",
                json!({"tag":"v1.0.0-rc.1", "sha":SHA}),
            ),
            ("release_identifier", json!("1.0.0")),
        ] {
            let mut fixture = Fixture::new("1.0.0");
            fixture.manifest[field] = value;
            fixture.write_manifest_and_checksums();
            assert!(
                fixture.check().unwrap_err().contains("Publication field"),
                "{field}"
            );
        }
    }

    #[test]
    fn public_verifier_rejects_internal_milestones_and_invalid_publication_policy() {
        let fixture = Fixture::new("1.0.0");
        for identifier in ["0.1.0", "0.1.0-rc.1", "0.3.0", "0.10.0", "0.10.0-rc.1"] {
            assert!(
                fixture
                    .check_as(identifier)
                    .unwrap_err()
                    .contains("Internal milestone")
            );
        }
        for publication in [json!(false), json!("true"), json!(1), Value::Null] {
            let mut plan = fixture.plan.clone();
            plan["releases"][3]["publication"] = publication;
            assert!(verify(&plan, "1.0.0", SHA, &fixture.root).is_err());
        }
        let mut plan = fixture.plan.clone();
        plan["releases"][3]
            .as_object_mut()
            .unwrap()
            .remove("publication");
        assert!(verify(&plan, "1.0.0", SHA, &fixture.root).is_err());
        for (pointer, value) in [
            ("/schema_version", json!(1)),
            ("/schema_version", json!(2.0)),
            ("/release_policy/private", json!(true)),
            ("/release_policy/publish_crate", json!(true)),
            ("/release_policy/final_promotion", json!("rebuild")),
            (
                "/release_policy/bundle_change_requires_new_candidate",
                json!(false),
            ),
        ] {
            let mut plan = fixture.plan.clone();
            *plan.pointer_mut(pointer).unwrap() = value;
            assert!(
                verify(&plan, "1.0.0", SHA, &fixture.root).is_err(),
                "{pointer}"
            );
        }
    }

    #[test]
    fn artifact_and_receipt_versions_must_be_the_final_binary_version() {
        for value in [json!("1.0.0-rc.1"), json!("1.0.1"), json!(1), Value::Null] {
            let mut fixture = Fixture::new("1.0.0");
            fixture.manifest["artifact_version"] = value;
            fixture.write_manifest_and_checksums();
            assert!(fixture.check().unwrap_err().contains("identity"));
        }
        let mut fixture = Fixture::new("1.0.0");
        fixture.manifest["schema_version"] = json!(1);
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("identity"));
        let mut fixture = Fixture::new("1.0.0");
        fixture.manifest["gates"][0]["version"] = json!("1.0.0-rc.1");
        fixture.write_manifest_and_checksums();
        assert!(fixture.check().unwrap_err().contains("Invalid gate record"));
        let mut fixture = Fixture::new("1.0.0");
        let (_, bytes) = fixture.entries.first_mut().unwrap();
        let mut receipt: Value = serde_json::from_slice(bytes).unwrap();
        receipt["version"] = json!("1.0.0-rc.1");
        *bytes = serde_json::to_vec(&receipt).unwrap();
        fixture.write_zip(true);
        fixture.refresh();
        assert!(fixture.check().unwrap_err().contains("receipt"));
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
        let fixture = Fixture::new("1.0.0-rc.1");
        assert!(verify(&fixture.plan, "0.2.0-rc.1", SHA, &fixture.root).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;
        let fixture = Fixture::new("1.0.0-rc.1");
        let path = fixture.root.join("runtime-requirements.md");
        fs::remove_file(&path).unwrap();
        symlink("release-notes.md", &path).unwrap();
        assert!(fixture.check().unwrap_err().contains("Link/reparse"));
    }
}
