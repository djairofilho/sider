//! Congelamento R10 e migração da candidata, com originais somente para leitura.

use super::baseline_manifest as manifest;
#[path = "baseline_package.rs"]
mod package;
#[path = "baseline_scenario.rs"]
mod scenario;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use manifest::Result;
use serde_json::{Value, json};

fn absolute(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(std::env::var_os(name).ok_or_else(|| format!("{name} ausente"))?);
    if !path.is_absolute() {
        return Err(format!("{name} precisa de caminho absoluto"));
    }
    Ok(path)
}
fn number(name: &str, default: u64) -> Result<u64> {
    std::env::var(name).ok().map_or(Ok(default), |value| {
        value.parse().map_err(|_| format!("{name} inválido"))
    })
}
fn source_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
struct PackageInput {
    archive: PathBuf,
    builds: PathBuf,
    provenance: PathBuf,
}
impl PackageInput {
    fn from_env(prefix: &str) -> Result<Self> {
        Ok(Self {
            archive: absolute(&format!("SIDER_{prefix}_PACKAGE"))?,
            builds: absolute(&format!("SIDER_{prefix}_BUILD_DIR"))?,
            provenance: absolute(&format!("SIDER_{prefix}_PROVENANCE"))?,
        })
    }
    fn verify(&self, identity: &(String, String), version: &str) -> Result<()> {
        package::provenance(
            &self.provenance,
            &self.archive,
            &self.builds,
            identity,
            version,
        )?;
        Ok(())
    }
}
fn create(path: &Path) -> Result<()> {
    fs::create_dir(path).map_err(|error| error.to_string())
}
fn write_manifest(path: &Path, value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.join(manifest::NAME))
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    Ok(manifest::digest(&path.join(manifest::NAME))?.1)
}

pub fn freeze_from_env() -> Result<Value> {
    if env!("CARGO_PKG_VERSION") != "0.1.0" {
        return Err("baseline R10 exige pacote interno0.1.0".into());
    }
    let root = source_root();
    let identity = package::identities(&root)?;
    let input = PackageInput::from_env("BASELINE")?;
    let output = absolute("SIDER_INTERNAL_BASELINE_DIR")?;
    let long_ms = number("SIDER_BASELINE_LONG_TTL_MS", 7 * 24 * 60 * 60 * 1000)?;
    if !(24 * 60 * 60 * 1000..=365 * 24 * 60 * 60 * 1000).contains(&long_ms) {
        return Err("TTL longo deve ficar entre1 e365dias".into());
    }
    let result = freeze(&root, &input, &output, &identity, long_ms, 60_000)?;
    if package::identities(&root)? != identity {
        return Err("checkout ou toolchain mudou durante congelamento".into());
    }
    Ok(result)
}

fn freeze(
    root: &Path,
    input: &PackageInput,
    output: &Path,
    identity: &(String, String),
    long_ms: u64,
    short_ms: u64,
) -> Result<Value> {
    input.verify(identity, "0.1.0")?;
    create(output)?;
    let archive_name = if cfg!(windows) {
        "package.zip"
    } else {
        "package.tar.gz"
    };
    fs::copy(&input.archive, output.join(archive_name)).map_err(|error| error.to_string())?;
    fs::copy(&input.provenance, output.join("build-provenance.json"))
        .map_err(|error| error.to_string())?;
    let package = package::extract(
        &output.join(archive_name),
        &output.join("package"),
        "0.1.0",
        root,
    )?;
    package::provenance(
        &output.join("build-provenance.json"),
        &output.join(archive_name),
        &package,
        identity,
        "0.1.0",
    )?;
    package::compare_builds(&package, &input.builds)?;
    package::validate_binaries(&package, "0.1.0")?;
    create(&output.join("datasets"))?;
    create(&output.join("backups"))?;
    let mut scenarios = Vec::new();
    for shards in [1, 4] {
        let data_dir = format!("datasets/shards-{shards}");
        let backup_dir = format!("backups/shards-{shards}");
        let tags = scenario::tags(shards)?;
        let mut node = scenario::Node::start(&package, &output.join(&data_dir), shards, None)?;
        scenario::seed(&node, &tags, long_ms)?;
        let (comparisons, expected) = scenario::check_state(&node, &tags)?;
        scenario::seed_short_ttl(&node, &tags, short_ms)?;
        scenario::export(&package, &node, &output.join(&backup_dir), &identity.0)?;
        let deadlines = scenario::deadlines(&output.join(&backup_dir))?;
        scenario::check_ttl(&node, &deadlines, false)?;
        let status = node.status(&package)?;
        let stopped = node.stop()?;
        scenario::backup_action(&package, "verify", &output.join(&backup_dir), None, shards)?;
        let aof = scenario::aof_metadata(&output.join(&data_dir))?;
        if aof["epoch"] != status["epoch"]
            || aof["sequence"] != status["sequence"]
            || aof["role"] != status["role"]
        {
            return Err("estado AOF diverge do status confirmado antes da parada".into());
        }
        if deadlines
            .iter()
            .filter(|value| value["key"].as_str().unwrap().ends_with(":short"))
            .any(|value| {
                value["unix_ms"].as_i64().unwrap() <= stopped["stopped_unix_ms"].as_i64().unwrap()
            })
        {
            return Err("TTL curto expirou antes de concluir a parada".into());
        }
        let scratch = scenario::Scratch::new()?;
        scenario::backup_action(
            &package,
            "restore",
            &output.join(&backup_dir),
            Some(&scratch.0.join("restored")),
            shards,
        )?;
        let mut restored =
            scenario::Node::start(&package, &scratch.0.join("restored"), shards, None)?;
        if scenario::check_state(&restored, &tags)?.1 != expected {
            return Err("restauração R10 diverge".into());
        }
        restored.stop()?;
        scenarios.push(json!({
            "shards":shards,"routing_version":1,"data_dir":data_dir,"backup_dir":backup_dir,"tags":tags,"deadlines":deadlines,"expected_state_sha256":expected,
            "configuration":{"max_dataset_bytes":scenario::DATASET,"aof_max_record_bytes":scenario::RECORD,"aof_sync":"always","aof_compact_after_bytes":0},
            "aof":aof,
            "observations":{"data_comparisons":comparisons,"backup_verified":true,"restore_verified":true,"short_ttl_alive_before_stop":true,"stopped_unix_ms":stopped["stopped_unix_ms"],"shutdown_method":stopped["shutdown_method"],"shutdown_exit_code":stopped["shutdown_exit_code"]}
        }));
    }
    input.verify(identity, "0.1.0")?;
    if package::identities(root)? != *identity {
        return Err("checkout ou toolchain mudou antes de salvar a baseline".into());
    }
    let value = json!({"schema_version":1,"task":"R10","source_sha":identity.0,"source_clean":true,"target":package::target(),"binary_version":"0.1.0","toolchain":identity.1,"created_unix_ms":scenario::now(),"package_root":"package","archive":archive_name,"provenance":"build-provenance.json","files":manifest::tree(output)?,"scenarios":scenarios});
    manifest::validate(&value, package::target())?;
    let hash = write_manifest(output, &value)?;
    manifest::verify(output, &hash, package::target())?;
    Ok(
        json!({"scope":"internal_baseline","task":"R10","baseline":output,"manifest_sha256":hash,"source_sha":identity.0,"target":package::target(),"scenarios":2}),
    )
}

pub fn migrate_from_env() -> Result<Value> {
    let baseline = absolute("SIDER_INTERNAL_BASELINE_DIR")?;
    let expected = std::env::var("SIDER_INTERNAL_BASELINE_SHA256")
        .map_err(|_| "hash externo da baseline ausente")?;
    let input = PackageInput::from_env("MIGRATION")?;
    let output = absolute("SIDER_MIGRATION_OUTPUT_DIR")?;
    let identity = package::identities(&source_root())?;
    let result = migrate(&baseline, &expected, &input, &output, &identity)?;
    if package::identities(&source_root())? != identity {
        return Err("checkout mudou durante migração".into());
    }
    Ok(result)
}

fn migrate(
    baseline: &Path,
    expected_hash: &str,
    input: &PackageInput,
    output: &Path,
    identity: &(String, String),
) -> Result<Value> {
    input.verify(identity, env!("CARGO_PKG_VERSION"))?;
    let new_sha = &identity.0;
    if fs::symlink_metadata(baseline)
        .map_err(|error| error.to_string())?
        .file_type()
        .is_symlink()
    {
        return Err("baseline não pode ser symlink".into());
    }
    let baseline = baseline.canonicalize().map_err(|error| error.to_string())?;
    let destination_parent = output
        .parent()
        .ok_or("destino sem pai")?
        .canonicalize()
        .map_err(|error| error.to_string())?;
    if destination_parent.starts_with(&baseline) {
        return Err("destino não pode alterar a baseline congelada".into());
    }
    let frozen = manifest::verify(&baseline, expected_hash, package::target())?;
    let old_package = baseline.join(frozen["package_root"].as_str().unwrap());
    package::provenance(
        &baseline.join(frozen["provenance"].as_str().unwrap()),
        &baseline.join(frozen["archive"].as_str().unwrap()),
        &old_package,
        &(
            frozen["source_sha"].as_str().unwrap().to_owned(),
            frozen["toolchain"].as_str().unwrap().to_owned(),
        ),
        "0.1.0",
    )?;
    package::validate_binaries(&old_package, "0.1.0")?;
    for scenario in frozen["scenarios"].as_array().unwrap() {
        for deadline in scenario["deadlines"].as_array().unwrap() {
            let expired = deadline["unix_ms"].as_i64().unwrap() <= scenario::now();
            let short = deadline["key"].as_str().unwrap().ends_with(":short");
            if short != expired {
                return Err("migração exige TTL curto expirado e TTL longo ainda vivo".into());
            }
        }
    }
    create(output)?;
    let current = package::extract(
        &input.archive,
        &output.join("package"),
        env!("CARGO_PKG_VERSION"),
        &source_root(),
    )?;
    package::provenance(
        &input.provenance,
        &input.archive,
        &current,
        identity,
        env!("CARGO_PKG_VERSION"),
    )?;
    package::compare_builds(&current, &input.builds)?;
    package::validate_binaries(&current, env!("CARGO_PKG_VERSION"))?;
    let mut observations = Vec::new();
    let mut comparisons = 0;
    for frozen_case in frozen["scenarios"].as_array().unwrap() {
        let shards = frozen_case["shards"].as_u64().unwrap() as u32;
        let directory = output.join(format!("shards-{shards}"));
        create(&directory)?;
        let tags = frozen_case["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let deadlines = frozen_case["deadlines"].as_array().unwrap();
        let source = baseline.join(frozen_case["data_dir"].as_str().unwrap());
        let copied = directory.join("direct-copy");
        scenario::copy_tree(&source, &copied)?;
        let mut primary = scenario::Node::start(&current, &copied, shards, None)?;
        let (count, state) = scenario::check_state(&primary, &tags)?;
        comparisons += count;
        if state != frozen_case["expected_state_sha256"].as_str().unwrap() {
            return Err("cópia migrada diverge da baseline".into());
        }
        scenario::check_ttl(&primary, deadlines, true)?;
        let mut replica = scenario::Node::start(
            &current,
            &directory.join("new-replica"),
            shards,
            Some(primary.internal),
        )?;
        catch_up(&current, &primary, &replica)?;
        let (count, state) = scenario::check_state(&replica, &tags)?;
        comparisons += count;
        if state != frozen_case["expected_state_sha256"].as_str().unwrap() {
            return Err("nova réplica diverge".into());
        }
        scenario::check_ttl(&replica, deadlines, true)?;
        let key = format!("{}:new-version", tags[0]);
        if !matches!(replica.call(&[b"SET",key.as_bytes(),b"forbidden"])? ,super::wire::Response::Error(error) if error.starts_with(b"READONLY"))
        {
            return Err("nova réplica aceitou escrita".into());
        }
        primary.call(&[b"SET", key.as_bytes(), b"new-version"])?;
        catch_up(&current, &primary, &replica)?;
        if replica.call(&[b"GET", key.as_bytes()])?
            != super::wire::Response::Bulk(Some(b"new-version".to_vec()))
        {
            return Err("delta da nova versão não chegou".into());
        }
        let candidate_backup = directory.join("candidate-backup");
        scenario::export(&current, &primary, &candidate_backup, new_sha)?;
        scenario::backup_action(
            &current,
            "restore",
            &candidate_backup,
            Some(&directory.join("candidate-restored")),
            shards,
        )?;
        let mut candidate_restored = scenario::Node::start(
            &current,
            &directory.join("candidate-restored"),
            shards,
            None,
        )?;
        comparisons += scenario::check_state(&candidate_restored, &tags)?.0;
        if candidate_restored.call(&[b"GET", key.as_bytes()])?
            != super::wire::Response::Bulk(Some(b"new-version".to_vec()))
        {
            return Err("backup novo perdeu escrita da candidata".into());
        }
        scenario::check_ttl(&candidate_restored, deadlines, true)?;
        candidate_restored.stop()?;
        let replica_stop = replica.stop()?;
        let primary_stop = primary.stop()?;
        let restored = directory.join("old-backup-restored");
        scenario::backup_action(
            &old_package,
            "restore",
            &baseline.join(frozen_case["backup_dir"].as_str().unwrap()),
            Some(&restored),
            shards,
        )?;
        let mut old_backup_new_server = scenario::Node::start(&current, &restored, shards, None)?;
        let (count, state) = scenario::check_state(&old_backup_new_server, &tags)?;
        comparisons += count;
        if state != frozen_case["expected_state_sha256"].as_str().unwrap() {
            return Err("backup antigo migrado diverge".into());
        }
        scenario::check_ttl(&old_backup_new_server, deadlines, true)?;
        old_backup_new_server.stop()?;
        reject_dataset(
            &current,
            &source,
            &directory.join("wrong-layout"),
            if shards == 1 { 4 } else { 1 },
            false,
        )?;
        reject_dataset(&current, &source, &directory.join("corrupt"), shards, true)?;
        observations.push(json!({"shards":shards,"direct_copy":true,"old_cli_restore_then_new_server":true,"candidate_backup_restore":true,"new_same_version_replica":true,"short_ttl_expired":true,"long_ttl_preserved":true,"layout_rejected":true,"corruption_rejected":true,"primary_stop":primary_stop,"replica_stop":replica_stop}));
    }
    manifest::verify(&baseline, expected_hash, package::target())?;
    input.verify(identity, env!("CARGO_PKG_VERSION"))?;
    Ok(
        json!({"scope":"extracted_packages_internal_baseline_migration","baseline_source_sha":frozen["source_sha"],"baseline_manifest_sha256":expected_hash,"candidate_source_sha":new_sha,"candidate_archive":package::file_identity(&input.archive)?,"candidate_provenance":package::file_identity(&input.provenance)?,"baseline_version":"0.1.0","candidate_version":env!("CARGO_PKG_VERSION"),"target":package::target(),"data_comparisons":comparisons,"baseline_unchanged":true,"scenarios":observations}),
    )
}

fn catch_up(package: &Path, primary: &scenario::Node, replica: &scenario::Node) -> Result<()> {
    let expected = primary.status(package)?;
    let deadline = Instant::now() + package::TIMEOUT;
    loop {
        let actual = replica.status(package)?;
        if actual["connected"] == true
            && actual["epoch"] == expected["epoch"]
            && actual["sequence"] == expected["sequence"]
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("nova réplica não alcançou o primário".into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn reject_dataset(
    package: &Path,
    source: &Path,
    destination: &Path,
    shards: u32,
    corrupt: bool,
) -> Result<()> {
    scenario::copy_tree(source, destination)?;
    let mut aof = manifest::tree(destination)?
        .into_iter()
        .filter(|file| file["path"].as_str().unwrap().ends_with(".aof"))
        .collect::<Vec<_>>();
    aof.sort_unstable_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    if corrupt {
        let path = destination.join(aof.last().ok_or("AOF ausente")?["path"].as_str().unwrap());
        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        file.write_all(b"BROKEN!!")
            .map_err(|error| error.to_string())?;
    }
    let before = manifest::tree(destination)?;
    let control = scenario::Scratch::new()?;
    let mut child = super::process::OwnedChild::spawn(&mut scenario::server_command(
        package,
        destination,
        shards,
        &control.0,
        None,
    ))?;
    let output = child.wait(package::TIMEOUT)?;
    if output.status.success()
        || output.stderr.is_empty()
        || control.0.join("resp.json").exists()
        || control.0.join("internal.json").exists()
    {
        return Err("dados inválidos produziram prontidão".into());
    }
    if before != manifest::tree(destination)? {
        return Err("recusa modificou dados de entrada".into());
    }
    Ok(())
}

pub fn rehearse_from_env() -> Result<Value> {
    if env!("CARGO_PKG_VERSION") != "0.1.0" {
        return Err("ensaio curto exige a versão interna0.1.0".into());
    }
    let root = source_root();
    let identity = package::identities(&root)?;
    let input = PackageInput::from_env("BASELINE")?;
    let scratch = scenario::Scratch::new()?;
    let baseline = scratch.0.join("baseline");
    let frozen = freeze(&root, &input, &baseline, &identity, 3_600_000, 30_000)?;
    let value = manifest::verify(
        &baseline,
        frozen["manifest_sha256"].as_str().unwrap(),
        package::target(),
    )?;
    let deadline = value["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|scenario| scenario["deadlines"].as_array().unwrap())
        .filter(|deadline| deadline["key"].as_str().unwrap().ends_with(":short"))
        .map(|deadline| deadline["unix_ms"].as_i64().unwrap())
        .max()
        .unwrap();
    while scenario::now() <= deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut result = migrate(
        &baseline,
        frozen["manifest_sha256"].as_str().unwrap(),
        &input,
        &scratch.0.join("migration"),
        &identity,
    )?;
    result["scope"] = json!("same_version_short_rehearsal_not_frozen_baseline");
    if package::identities(&root)? != identity {
        return Err("checkout mudou durante ensaio".into());
    }
    Ok(result)
}
