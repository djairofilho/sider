//! Parser puro: caminhos são OsString e erros não repetem valores fornecidos.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use super::{Error, ExportOptions, Limits, RestoreOptions};
use crate::persistence::DurableLayout;

pub enum Action {
    Export(ExportOptions),
    Verify {
        source: PathBuf,
        layout: DurableLayout,
        limits: Limits,
    },
    Restore(RestoreOptions),
}

pub fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Action, Error> {
    let mut args = arguments.into_iter();
    let action = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or(Error::Invalid("subcomando ausente"))?;
    if !matches!(action.as_str(), "export" | "verify" | "restore") {
        return Err(Error::Invalid("subcomando desconhecido"));
    }
    let mut values = BTreeMap::new();
    while let Some(name) = args.next() {
        let name = name
            .into_string()
            .map_err(|_| Error::Invalid("nome de opção inválido"))?;
        if !matches!(
            name.as_str(),
            "--source"
                | "--destination"
                | "--source-sha"
                | "--shards"
                | "--routing"
                | "--max-record-bytes"
                | "--max-mutations"
                | "--max-snapshot-bytes"
                | "--max-dataset-bytes"
                | "--timeout-ms"
        ) {
            return Err(Error::Invalid("opção desconhecida"));
        }
        let value = args
            .next()
            .filter(|value| !value.is_empty())
            .ok_or(Error::Invalid("opção sem valor"))?;
        if values.insert(name, value).is_some() {
            return Err(Error::Invalid("opção repetida"));
        }
    }
    fn required(values: &mut BTreeMap<String, OsString>, name: &str) -> Result<OsString, Error> {
        values
            .remove(name)
            .ok_or(Error::Invalid("opção obrigatória ausente"))
    }
    fn number(value: OsString) -> Result<u64, Error> {
        value
            .to_str()
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or(Error::Invalid("inteiro decimal inválido"))?
            .parse()
            .map_err(|_| Error::Invalid("inteiro fora do limite"))
    }
    let mut limits = Limits::default();
    for (name, field) in [
        ("--max-record-bytes", &mut limits.max_record_bytes),
        ("--max-mutations", &mut limits.max_mutations),
        ("--max-dataset-bytes", &mut limits.max_dataset_bytes),
    ] {
        if let Some(value) = values.remove(name) {
            *field = usize::try_from(number(value)?)
                .map_err(|_| Error::Invalid("inteiro fora do limite"))?;
        }
    }
    if let Some(value) = values.remove("--max-snapshot-bytes") {
        limits.max_snapshot_bytes = number(value)?;
    }
    if let Some(value) = values.remove("--timeout-ms") {
        limits.timeout = Duration::from_millis(number(value)?);
    }
    limits.validate()?;
    let source = required(&mut values, "--source")?;
    let result = if action == "export" {
        let source = source
            .to_str()
            .ok_or(Error::Invalid("endereço inválido"))?
            .parse()
            .map_err(|_| Error::Invalid("origem exige IP literal e porta"))?;
        let destination = required(&mut values, "--destination")?.into();
        let source_sha = required(&mut values, "--source-sha")?
            .into_string()
            .map_err(|_| Error::Invalid("SHA inválido"))?;
        super::manifest::require_hex(&source_sha, 40)?;
        Action::Export(ExportOptions {
            source,
            destination,
            source_sha,
            limits,
        })
    } else {
        let layout = DurableLayout {
            shard_count: u32::try_from(number(required(&mut values, "--shards")?)?)
                .map_err(|_| Error::Invalid("quantidade de shards"))?,
            routing_version: u32::try_from(number(required(&mut values, "--routing")?)?)
                .map_err(|_| Error::Invalid("versão de roteamento"))?,
        };
        layout.validate()?;
        layout.quota(limits.max_dataset_bytes, 0)?;
        if action == "restore" {
            Action::Restore(RestoreOptions {
                source: source.into(),
                destination: required(&mut values, "--destination")?.into(),
                layout,
                limits,
            })
        } else {
            Action::Verify {
                source: source.into(),
                layout,
                limits,
            }
        }
    };
    if !values.is_empty() {
        return Err(Error::Invalid("opção incompatível com subcomando"));
    }
    Ok(result)
}
