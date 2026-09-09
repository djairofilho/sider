//! Migração offline explícita, preservando o AOF de origem e sua sequência.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::format::{self, Next, Record};
use super::writer::{DirectoryLock, recover_read_only, sync_directory};
use super::{AofConfig, AofError, DurableLayout, RecoveryMetadata};
use crate::storage::{Clock, Mutation, Store, StoreConfig};

#[derive(Clone, Debug)]
pub struct MigrationOptions {
    pub source: AofConfig,
    pub destination: AofConfig,
    pub source_store: StoreConfig,
    pub destination_store: StoreConfig,
}

#[derive(Clone, Debug)]
pub struct MigrationReport {
    pub source: RecoveryMetadata,
    pub destination_layout: DurableLayout,
    pub sequence: u64,
    pub entries: usize,
    pub destination_shard_usage: Vec<usize>,
}

/// Parser puro compartilhado pelo CLI e pelos testes; caminhos preservam OsString.
pub fn options_from_args(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<MigrationOptions, String> {
    let mut values = BTreeMap::new();
    let mut arguments = arguments.into_iter();
    while let Some(name) = arguments.next() {
        let name = name
            .into_string()
            .map_err(|_| "nome de opção precisa ser UTF-8")?;
        if !matches!(
            name.as_str(),
            "--source"
                | "--source-shards"
                | "--source-routing"
                | "--destination"
                | "--shards"
                | "--routing"
                | "--source-max-dataset-bytes"
                | "--max-dataset-bytes"
                | "--source-max-record-bytes"
                | "--max-record-bytes"
        ) {
            return Err(format!("opção desconhecida: {name}"));
        }
        let value = arguments
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("{name} precisa de um valor"))?;
        if values.insert(name.clone(), value).is_some() {
            return Err(format!("opção repetida: {name}"));
        }
    }
    fn required(values: &mut BTreeMap<String, OsString>, name: &str) -> Result<OsString, String> {
        values
            .remove(name)
            .ok_or_else(|| format!("{name} é obrigatório"))
    }
    fn number(value: OsString, name: &str) -> Result<usize, String> {
        let text = value
            .to_str()
            .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or_else(|| format!("{name} precisa ser inteiro decimal sem sinal"))?;
        text.parse()
            .map_err(|_| format!("{name} excede o tamanho representável"))
    }
    fn layout(
        values: &mut BTreeMap<String, OsString>,
        count: &str,
        routing: &str,
    ) -> Result<DurableLayout, String> {
        let shard_count = u32::try_from(number(required(values, count)?, count)?)
            .map_err(|_| format!("{count} excede u32"))?;
        let routing_version = u32::try_from(number(required(values, routing)?, routing)?)
            .map_err(|_| format!("{routing} excede u32"))?;
        Ok(DurableLayout {
            shard_count,
            routing_version,
        })
    }
    let mut source = AofConfig::new(required(&mut values, "--source")?.into());
    let mut destination = AofConfig::new(required(&mut values, "--destination")?.into());
    source.layout = layout(&mut values, "--source-shards", "--source-routing")?;
    destination.layout = layout(&mut values, "--shards", "--routing")?;
    let mut source_store = StoreConfig::default();
    let mut destination_store = StoreConfig::default();
    for (name, field) in [
        (
            "--source-max-dataset-bytes",
            &mut source_store.max_dataset_bytes,
        ),
        (
            "--max-dataset-bytes",
            &mut destination_store.max_dataset_bytes,
        ),
        (
            "--source-max-record-bytes",
            &mut source.limits.max_record_bytes,
        ),
        (
            "--max-record-bytes",
            &mut destination.limits.max_record_bytes,
        ),
    ] {
        if let Some(value) = values.remove(name) {
            *field = number(value, name)?;
        }
    }
    source.validate().map_err(|error| error.to_string())?;
    destination.validate().map_err(|error| error.to_string())?;
    source_store.validate().map_err(|error| error.to_string())?;
    destination_store
        .validate()
        .map_err(|error| error.to_string())?;
    source
        .layout
        .quota(source_store.max_dataset_bytes, 0)
        .map_err(|error| error.to_string())?;
    destination
        .layout
        .quota(destination_store.max_dataset_bytes, 0)
        .map_err(|error| error.to_string())?;
    Ok(MigrationOptions {
        source,
        destination,
        source_store,
        destination_store,
    })
}

struct FrozenClock {
    now: tokio::time::Instant,
    unix_ms: i64,
}
impl Clock for FrozenClock {
    fn now(&self) -> tokio::time::Instant {
        self.now
    }
    fn unix_millis(&self) -> i64 {
        self.unix_ms
    }
}

struct Destination {
    directory: PathBuf,
    temporary: PathBuf,
    published: PathBuf,
    committed: bool,
}
impl Drop for Destination {
    fn drop(&mut self) {
        if !self.committed {
            // Remove somente nomes criados por esta operação; jamais apaga recursivamente.
            let _ = fs::remove_file(&self.temporary);
            let _ = fs::remove_file(&self.published);
            let _ = fs::remove_file(self.directory.join("writer.lock"));
            let _ = fs::remove_dir(&self.directory);
        }
    }
}

/// Exige o escritor de origem parado e destino inexistente, fora da árvore de origem.
/// Não trunca a origem: uma cauda incompleta é informada no relatório e fica preservada.
pub fn migrate_offline(
    options: MigrationOptions,
    clock: Arc<dyn Clock>,
) -> Result<MigrationReport, AofError> {
    options.source.validate()?;
    options.destination.validate()?;
    options.source_store.validate()?;
    options.destination_store.validate()?;
    options
        .source
        .layout
        .quota(options.source_store.max_dataset_bytes, 0)?;
    options
        .destination
        .layout
        .quota(options.destination_store.max_dataset_bytes, 0)?;
    let source_path = options.source.directory.canonicalize()?;
    let destination_path = destination_path(&options.destination.directory)?;
    if destination_path.starts_with(&source_path) {
        return Err(AofError::Migration(
            "destino precisa ficar fora da árvore de origem",
        ));
    }
    if destination_path.exists() {
        return Err(AofError::Migration("destino já existe"));
    }
    // Um par de relógios mantém a semântica de TTL estável durante leitura, quotas e conferência.
    let clock: Arc<dyn Clock> = Arc::new(FrozenClock {
        now: clock.now(),
        unix_ms: clock.unix_millis(),
    });
    let recovered = recover_read_only(options.source, options.source_store, clock.clone())?;
    let snapshot = recovered.store.snapshot();
    let destination_shard_usage = validate_snapshot(
        &snapshot,
        options.destination.layout,
        options.destination_store,
        clock,
    )?;

    fs::create_dir(&destination_path)?;
    let mut destination = Destination {
        temporary: destination_path.join("migration.tmp"),
        published: destination_path.join("generation-00000000000000000000.aof"),
        directory: destination_path,
        committed: false,
    };
    let lock = DirectoryLock::acquire(&destination.directory, true)?;
    let sequence = recovered.metadata.sequence;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&destination.temporary)?;
    format::write_header_with_layout(&mut file, sequence, options.destination.layout)?;
    let mut digest = 0;
    for mutation in &snapshot {
        let encoded = format::encode(
            &Record::Snapshot(mutation.clone()),
            options.destination.limits,
        )?;
        digest = format::snapshot_digest(digest, &encoded);
        file.write_all(&encoded)?;
    }
    let seal = Record::Seal {
        sequence,
        entries: snapshot.len() as u64,
        digest,
    };
    file.write_all(&format::encode(&seal, options.destination.limits)?)?;
    file.sync_all()?;
    drop(file);
    verify_file(
        &destination.temporary,
        &snapshot,
        &seal,
        &options.destination,
        sequence,
    )?;
    fs::rename(&destination.temporary, &destination.published)?;
    sync_directory(&destination.directory)?;
    if let Some(parent) = destination.directory.parent() {
        sync_directory(parent)?;
    }
    destination.committed = true;
    drop(lock);
    Ok(MigrationReport {
        source: recovered.metadata.clone(),
        destination_layout: options.destination.layout,
        sequence,
        entries: snapshot.len(),
        destination_shard_usage,
    })
}

fn destination_path(path: &Path) -> Result<PathBuf, AofError> {
    let name = path.file_name().ok_or(AofError::Migration(
        "destino precisa nomear um diretório novo",
    ))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(parent.canonicalize()?.join(name))
}

fn validate_snapshot(
    snapshot: &[Mutation],
    layout: DurableLayout,
    config: StoreConfig,
    clock: Arc<dyn Clock>,
) -> Result<Vec<usize>, AofError> {
    let mut usage = vec![0usize; layout.shard_count as usize];
    // Reutiliza a contabilidade real de Store com no máximo uma entrada temporária.
    // Bytes/valores imutáveis continuam compartilhados com a origem.
    let mut entry_store = Store::with_config(
        StoreConfig {
            max_dataset_bytes: isize::MAX as usize,
        },
        clock,
    )?;
    for mutation in snapshot {
        let shard = layout.shard_for(mutation.key())?;
        entry_store.replay(std::slice::from_ref(mutation))?;
        usage[shard] =
            usage[shard]
                .checked_add(entry_store.used_bytes())
                .ok_or(AofError::Migration(
                    "contabilidade excedeu o tamanho representável",
                ))?;
        let quota = layout.quota(config.max_dataset_bytes, shard)?;
        if usage[shard] > quota {
            return Err(AofError::ShardQuota {
                shard,
                used: usage[shard],
                quota,
            });
        }
        entry_store.replay(&[Mutation::Delete {
            key: mutation.key().clone(),
        }])?;
    }
    Ok(usage)
}

fn verify_file(
    path: &Path,
    snapshot: &[Mutation],
    seal: &Record,
    config: &AofConfig,
    sequence: u64,
) -> Result<(), AofError> {
    let mut file = File::open(path)?;
    let header = format::read_header_with_layout(&mut file)?;
    if header.sequence != sequence || header.layout != config.layout {
        return Err(AofError::Migration("cabeçalho do destino divergiu"));
    }
    for mutation in snapshot {
        if format::read_record(&mut file, config.limits)?
            != Next::Record(Record::Snapshot(mutation.clone()))
        {
            return Err(AofError::Migration("snapshot do destino divergiu"));
        }
    }
    if format::read_record(&mut file, config.limits)? != Next::Record(seal.clone())
        || format::read_record(&mut file, config.limits)? != Next::End
    {
        return Err(AofError::Migration("selo ou fim do destino divergiu"));
    }
    Ok(())
}
