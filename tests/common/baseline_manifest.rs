//! Immutable internal baseline inventory, validated before opening executables.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, String>;
pub const NAME: &str = "baseline.json";
pub const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const MAX_FILES: usize = 4096;

pub fn digest(path: &Path) -> Result<(u64, String)> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > MAX_FILE_BYTES {
        return Err(format!("bounded regular file required: {}", path.display()));
    }
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    let mut bytes = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        bytes += count as u64;
        if bytes > MAX_FILE_BYTES {
            return Err("file grew beyond the limit".into());
        }
        hash.update(&buffer[..count]);
    }
    if bytes != metadata.len() {
        return Err("file changed during reading".into());
    }
    Ok((bytes, format!("{:x}", hash.finalize())))
}

pub fn safe_relative(value: &str) -> Result<PathBuf> {
    if value.is_empty()
        || value.len() > 512
        || value.starts_with('/')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._/".contains(&byte))
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err("invalid portable relative path".into());
    }
    for part in value.split('/') {
        let base = part.split('.').next().unwrap().to_ascii_uppercase();
        if part.ends_with('.')
            || matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || (base.len() == 4
                && (base.starts_with("COM") || base.starts_with("LPT"))
                && matches!(base.as_bytes()[3], b'1'..=b'9'))
        {
            return Err("reserved path component".into());
        }
    }
    Ok(PathBuf::from(value))
}

pub fn hex(value: &Value, size: usize) -> Result<&str> {
    let value = value.as_str().ok_or("text hash required")?;
    if value.len() != size
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("invalid lowercase hexadecimal hash".into());
    }
    Ok(value)
}

pub fn fields(value: &Value, expected: &[&str]) -> Result<()> {
    let object = value.as_object().ok_or("JSON object required")?;
    if object.len() != expected.len() || object.keys().any(|key| !expected.contains(&key.as_str()))
    {
        return Err("manifest field mismatch".into());
    }
    Ok(())
}

pub fn tree(root: &Path) -> Result<Vec<Value>> {
    fn walk(root: &Path, directory: &Path, files: &mut Vec<Value>) -> Result<()> {
        let metadata = fs::symlink_metadata(directory).map_err(|error| error.to_string())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("real directory required".into());
        }
        for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
            if metadata.file_type().is_symlink() {
                return Err("symlink does not belong in the baseline".into());
            }
            if metadata.is_dir() {
                walk(root, &path, files)?;
            } else {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|error| error.to_string())?
                    .to_str()
                    .ok_or("non-Unicode path")?
                    .replace('\\', "/");
                if relative == NAME {
                    continue;
                }
                safe_relative(&relative)?;
                let (bytes, sha256) = digest(&path)?;
                files.push(json!({"path": relative, "bytes": bytes, "sha256": sha256}));
                if files.len() > MAX_FILES {
                    return Err("inventory exceeded the limit".into());
                }
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    files.sort_unstable_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    Ok(files)
}

pub fn validate(value: &Value, expected_target: &str) -> Result<()> {
    fields(
        value,
        &[
            "schema_version",
            "task",
            "source_sha",
            "source_clean",
            "target",
            "binary_version",
            "toolchain",
            "created_unix_ms",
            "package_root",
            "archive",
            "provenance",
            "files",
            "scenarios",
        ],
    )?;
    if value["schema_version"] != 1
        || value["task"] != "R10"
        || value["source_clean"] != true
        || value["binary_version"] != "0.1.0"
        || value["target"] != expected_target
        || !matches!(
            expected_target,
            "x86_64-pc-windows-msvc" | "x86_64-unknown-linux-gnu"
        )
        || value["created_unix_ms"]
            .as_u64()
            .is_none_or(|number| number == 0)
        || value["toolchain"]
            .as_str()
            .is_none_or(|text| text.is_empty() || text.len() > 4096)
    {
        return Err("invalid internal baseline identity".into());
    }
    hex(&value["source_sha"], 40)?;
    let package_root = value["package_root"]
        .as_str()
        .ok_or("missing package root")?;
    safe_relative(package_root)?;
    let archive = value["archive"].as_str().ok_or("missing package archive")?;
    safe_relative(archive)?;
    let provenance = value["provenance"].as_str().ok_or("missing provenance")?;
    safe_relative(provenance)?;
    let files = value["files"].as_array().ok_or("missing inventory")?;
    if files.is_empty() || files.len() > MAX_FILES {
        return Err("empty or excessive inventory".into());
    }
    let mut paths = BTreeSet::new();
    let mut previous = "";
    for file in files {
        fields(file, &["path", "bytes", "sha256"])?;
        let path = file["path"].as_str().ok_or("missing path")?;
        safe_relative(path)?;
        if path <= previous || path == NAME {
            return Err("duplicate or unsorted inventory".into());
        }
        previous = path;
        if file["bytes"]
            .as_u64()
            .is_none_or(|bytes| bytes > MAX_FILE_BYTES)
        {
            return Err("invalid file size".into());
        }
        hex(&file["sha256"], 64)?;
        paths.insert(path);
    }
    if !paths.contains(archive) || !paths.contains(provenance) {
        return Err("package archive or provenance not inventoried".into());
    }
    let suffix = if expected_target.contains("windows") {
        ".exe"
    } else {
        ""
    };
    for binary in [
        "sider",
        "sider-backup",
        "sider-replica",
        "sider-aof-migrate",
    ] {
        if !paths.contains(format!("{package_root}/{binary}{suffix}").as_str()) {
            return Err("missing required executable".into());
        }
    }
    let scenarios = value["scenarios"].as_array().ok_or("missing scenarios")?;
    if scenarios.len() != 2 {
        return Err("shards1 and shards4 scenarios required".into());
    }
    for (scenario, shards) in scenarios.iter().zip([1, 4]) {
        fields(
            scenario,
            &[
                "shards",
                "routing_version",
                "data_dir",
                "backup_dir",
                "tags",
                "deadlines",
                "expected_state_sha256",
                "observations",
                "configuration",
                "aof",
            ],
        )?;
        if scenario["shards"] != shards || scenario["routing_version"] != 1 {
            return Err("invalid scenario layout".into());
        }
        fields(
            &scenario["configuration"],
            &[
                "max_dataset_bytes",
                "aof_max_record_bytes",
                "aof_sync",
                "aof_compact_after_bytes",
            ],
        )?;
        if scenario["configuration"]
            != json!({"max_dataset_bytes":4*1024*1024,"aof_max_record_bytes":65536,"aof_sync":"always","aof_compact_after_bytes":0})
        {
            return Err("fixture configuration mismatch".into());
        }
        fields(
            &scenario["aof"],
            &[
                "header_version",
                "record_version",
                "role",
                "epoch",
                "sequence",
            ],
        )?;
        if scenario["aof"]["header_version"] != 3
            || scenario["aof"]["record_version"] != 1
            || scenario["aof"]["role"] != "primary"
            || scenario["aof"]["sequence"]
                .as_u64()
                .is_none_or(|sequence| sequence == 0)
        {
            return Err("invalid source AOF metadata".into());
        }
        hex(&scenario["aof"]["epoch"], 32)?;
        for field in ["data_dir", "backup_dir"] {
            let directory = scenario[field]
                .as_str()
                .ok_or("missing scenario directory")?;
            safe_relative(directory)?;
            if !paths
                .iter()
                .any(|path| path.starts_with(&format!("{directory}/")))
            {
                return Err("scenario data missing from inventory".into());
            }
        }
        if scenario["tags"].as_array().is_none_or(|tags| {
            tags.len() != shards as usize
                || tags.iter().any(|tag| {
                    tag.as_str().is_none_or(|tag| {
                        tag.len() > 64 || !tag.starts_with("{r10-") || !tag.ends_with('}')
                    })
                })
        }) {
            return Err("invalid scenario tags".into());
        }
        hex(&scenario["expected_state_sha256"], 64)?;
        let deadlines = scenario["deadlines"]
            .as_array()
            .ok_or("missing deadlines")?;
        if deadlines.len() != 2 {
            return Err("two deadlines required".into());
        }
        for deadline in deadlines {
            fields(deadline, &["key", "unix_ms"])?;
            if deadline["key"]
                .as_str()
                .is_none_or(|key| key.is_empty() || key.len() > 128)
                || deadline["unix_ms"].as_i64().is_none_or(|time| time <= 0)
            {
                return Err("invalid deadline".into());
            }
        }
        fields(
            &scenario["observations"],
            &[
                "data_comparisons",
                "backup_verified",
                "restore_verified",
                "stopped_unix_ms",
                "short_ttl_alive_before_stop",
                "shutdown_method",
                "shutdown_exit_code",
            ],
        )?;
        let observations = &scenario["observations"];
        if observations["data_comparisons"]
            .as_u64()
            .is_none_or(|count| count == 0)
            || observations["backup_verified"] != true
            || observations["restore_verified"] != true
            || observations["short_ttl_alive_before_stop"] != true
            || observations["stopped_unix_ms"]
                .as_i64()
                .is_none_or(|time| time <= 0)
            || !matches!(
                observations["shutdown_method"].as_str(),
                Some("sigterm" | "owned_process_kill")
            )
        {
            return Err("incomplete baseline observations".into());
        }
        let stopped = observations["stopped_unix_ms"].as_i64().unwrap();
        let expected_keys = [
            format!("{}:long", scenario["tags"][0].as_str().unwrap()),
            format!("{}:short", scenario["tags"][0].as_str().unwrap()),
        ];
        if deadlines.iter().zip(expected_keys).any(|(deadline, key)| {
            deadline["key"] != key || deadline["unix_ms"].as_i64().unwrap() <= stopped
        }) || deadlines[0]["unix_ms"].as_i64().unwrap()
            <= deadlines[1]["unix_ms"].as_i64().unwrap()
        {
            return Err("short TTL was not live at shutdown or long TTL is invalid".into());
        }
    }
    Ok(())
}

pub fn verify(root: &Path, expected_hash: &str, target: &str) -> Result<Value> {
    hex(&Value::String(expected_hash.to_owned()), 64)?;
    let (bytes, actual_hash) = digest(&root.join(NAME))?;
    if bytes > 4 * 1024 * 1024 || actual_hash != expected_hash {
        return Err("external manifest hash mismatch".into());
    }
    let value: Value =
        serde_json::from_slice(&fs::read(root.join(NAME)).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    validate(&value, target)?;
    if Value::Array(tree(root)?) != value["files"] {
        return Err("baseline inventory mismatch".into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(root: &Path) -> Value {
        let target = "x86_64-pc-windows-msvc";
        for name in [
            "sider.exe",
            "sider-backup.exe",
            "sider-replica.exe",
            "sider-aof-migrate.exe",
        ] {
            fs::create_dir_all(root.join("package")).unwrap();
            fs::write(
                root.join("package").join(name),
                b"test fixture, not executable",
            )
            .unwrap();
        }
        fs::write(root.join("package.zip"), b"archive fixture").unwrap();
        fs::write(root.join("build-provenance.json"), b"provenance fixture").unwrap();
        let mut scenarios = Vec::new();
        for shards in [1, 4] {
            let data = format!("datasets/shards-{shards}");
            let backup = format!("backups/shards-{shards}");
            for directory in [&data, &backup] {
                fs::create_dir_all(root.join(directory)).unwrap();
                fs::write(root.join(directory).join("data.aof"), b"file fixture").unwrap();
            }
            scenarios.push(json!({"shards":shards,"routing_version":1,"data_dir":data,"backup_dir":backup,"tags":(0..shards).map(|index|format!("{{r10-{index}}}")).collect::<Vec<_>>(),"deadlines":[{"key":"{r10-0}:long","unix_ms":100000},{"key":"{r10-0}:short","unix_ms":1000}],"expected_state_sha256":"a".repeat(64),"configuration":{"max_dataset_bytes":4*1024*1024,"aof_max_record_bytes":65536,"aof_sync":"always","aof_compact_after_bytes":0},"aof":{"header_version":3,"record_version":1,"role":"primary","epoch":"1".repeat(32),"sequence":3},"observations":{"data_comparisons":12,"backup_verified":true,"restore_verified":true,"stopped_unix_ms":100,"short_ttl_alive_before_stop":true,"shutdown_method":"owned_process_kill","shutdown_exit_code":1}}));
        }
        json!({"schema_version":1,"task":"R10","source_sha":"1".repeat(40),"source_clean":true,"target":target,"binary_version":"0.1.0","toolchain":"rustc fixture","created_unix_ms":200,"package_root":"package","archive":"package.zip","provenance":"build-provenance.json","files":tree(root).unwrap(),"scenarios":scenarios})
    }

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "sider-baseline-contract-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&root).unwrap();
            Self(root)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn baseline_paths_reject_traversal_devices_aliases_and_absolute_locations() {
        for path in [
            "",
            "../outside",
            "/absolute",
            "C:/outside",
            "a\\b",
            "a//b",
            "a/./b",
            "NUL",
            "con.txt",
            "a/LPT1",
            "a.",
        ] {
            assert!(safe_relative(path).is_err(), "{path}");
        }
        assert_eq!(
            safe_relative("datasets/shards-4/generation-0001.aof").unwrap(),
            PathBuf::from("datasets/shards-4/generation-0001.aof")
        );
    }

    #[test]
    fn baseline_manifest_rejects_identity_layout_ttl_and_inventory_drift() {
        let directory = Directory::new();
        let valid = fixture(&directory.0);
        assert!(validate(&valid, "x86_64-pc-windows-msvc").is_ok());
        for (pointer, wrong) in [
            ("/schema_version", json!(2)),
            ("/task", json!("R11")),
            ("/source_clean", json!(false)),
            ("/source_sha", json!("short")),
            ("/binary_version", json!("1.0.0")),
            ("/target", json!("x86_64-unknown-linux-gnu")),
            ("/scenarios/1/shards", json!(2)),
            ("/scenarios/0/deadlines/1/unix_ms", json!(99)),
            ("/scenarios/0/aof/epoch", json!("")),
            ("/scenarios/0/observations/restore_verified", json!(false)),
            ("/files/0/path", json!("../outside")),
            ("/files/0/bytes", json!(u64::MAX)),
        ] {
            let mut changed = valid.clone();
            *changed.pointer_mut(pointer).unwrap() = wrong;
            assert!(
                validate(&changed, "x86_64-pc-windows-msvc").is_err(),
                "{pointer}"
            );
        }
        let mut duplicate = valid.clone();
        duplicate["files"][1] = duplicate["files"][0].clone();
        assert!(validate(&duplicate, "x86_64-pc-windows-msvc").is_err());
    }

    #[test]
    fn baseline_external_hash_and_complete_tree_detect_changes_before_consumption() {
        let directory = Directory::new();
        let valid = fixture(&directory.0);
        fs::write(directory.0.join(NAME), serde_json::to_vec(&valid).unwrap()).unwrap();
        let hash = digest(&directory.0.join(NAME)).unwrap().1;
        assert!(verify(&directory.0, &hash, "x86_64-pc-windows-msvc").is_ok());
        assert!(verify(&directory.0, &"f".repeat(64), "x86_64-pc-windows-msvc").is_err());
        let data = directory.0.join("datasets/shards-1/data.aof");
        fs::write(&data, b"changed").unwrap();
        assert!(verify(&directory.0, &hash, "x86_64-pc-windows-msvc").is_err());
        fs::write(&data, b"file fixture").unwrap();
        fs::write(directory.0.join("extra"), b"extra").unwrap();
        assert!(verify(&directory.0, &hash, "x86_64-pc-windows-msvc").is_err());
        fs::remove_file(directory.0.join("extra")).unwrap();
        fs::remove_file(data).unwrap();
        assert!(verify(&directory.0, &hash, "x86_64-pc-windows-msvc").is_err());
    }
}
