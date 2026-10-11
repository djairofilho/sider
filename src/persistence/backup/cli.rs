//! Pure parser: paths are OsString and errors do not repeat supplied values.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use super::{Error, ExportOptions, Limits, RestoreOptions};
use crate::persistence::DurableLayout;

pub enum Action {
    Export(ExportOptions),
    ExportMemory(ExportOptions),
    ExportComposed(ExportOptions),
    Verify {
        source: PathBuf,
        layout: DurableLayout,
        limits: Limits,
    },
    VerifyMemory {
        source: PathBuf,
        layout: DurableLayout,
        limits: Limits,
    },
    VerifyComposed {
        source: PathBuf,
        layout: DurableLayout,
        limits: Limits,
    },
    Restore(RestoreOptions),
    RestoreMemory(RestoreOptions),
    RestoreComposed(RestoreOptions),
}

#[derive(Clone, Copy)]
enum Profile {
    Legacy,
    Memory,
    Composed,
}

pub fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Action, Error> {
    let mut args = arguments.into_iter();
    let action = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or(Error::Invalid("missing subcommand"))?;
    if !matches!(action.as_str(), "export" | "verify" | "restore") {
        return Err(Error::Invalid("unknown subcommand"));
    }
    let mut values = BTreeMap::new();
    while let Some(name) = args.next() {
        let name = name
            .into_string()
            .map_err(|_| Error::Invalid("invalid option name"))?;
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
                | "--record-version"
                | "--profile"
        ) {
            return Err(Error::Invalid("unknown option"));
        }
        let value = args
            .next()
            .filter(|value| !value.is_empty())
            .ok_or(Error::Invalid("option without value"))?;
        if values.insert(name, value).is_some() {
            return Err(Error::Invalid("repeated option"));
        }
    }
    fn required(values: &mut BTreeMap<String, OsString>, name: &str) -> Result<OsString, Error> {
        values
            .remove(name)
            .ok_or(Error::Invalid("required option missing"))
    }
    fn number(value: OsString) -> Result<u64, Error> {
        value
            .to_str()
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or(Error::Invalid("invalid decimal integer"))?
            .parse()
            .map_err(|_| Error::Invalid("integer out of range"))
    }
    let mut limits = Limits::default();
    let version = values.remove("--record-version");
    let selected = values.remove("--profile");
    if version.is_some() && selected.is_some() {
        return Err(Error::Invalid(
            "profile and record version are incompatible",
        ));
    }
    let profile = match (version, selected) {
        (None, None) => Profile::Legacy,
        (Some(ref value), None) if value == "1" => Profile::Legacy,
        (Some(ref value), None) if value == "2" => Profile::Memory,
        (None, Some(ref value)) if value == "legacy" => Profile::Legacy,
        (None, Some(ref value)) if value == "memory" => Profile::Memory,
        (None, Some(ref value)) if value == "composed" => Profile::Composed,
        (Some(_), None) => return Err(Error::Invalid("record version must be 1 or 2")),
        _ => {
            return Err(Error::Invalid(
                "profile must be legacy, memory, or composed",
            ));
        }
    };
    for (name, field) in [
        ("--max-record-bytes", &mut limits.max_record_bytes),
        ("--max-mutations", &mut limits.max_mutations),
        ("--max-dataset-bytes", &mut limits.max_dataset_bytes),
    ] {
        if let Some(value) = values.remove(name) {
            *field = usize::try_from(number(value)?)
                .map_err(|_| Error::Invalid("integer out of range"))?;
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
            .ok_or(Error::Invalid("invalid address"))?
            .parse()
            .map_err(|_| Error::Invalid("source requires a literal IP address and port"))?;
        let destination = required(&mut values, "--destination")?.into();
        let source_sha = required(&mut values, "--source-sha")?
            .into_string()
            .map_err(|_| Error::Invalid("invalid SHA"))?;
        super::manifest::require_hex(&source_sha, 40)?;
        let options = ExportOptions {
            source,
            destination,
            source_sha,
            limits,
        };
        match profile {
            Profile::Legacy => Action::Export(options),
            Profile::Memory => Action::ExportMemory(options),
            Profile::Composed => Action::ExportComposed(options),
        }
    } else {
        let layout = DurableLayout {
            shard_count: u32::try_from(number(required(&mut values, "--shards")?)?)
                .map_err(|_| Error::Invalid("shard count"))?,
            routing_version: u32::try_from(number(required(&mut values, "--routing")?)?)
                .map_err(|_| Error::Invalid("routing version"))?,
        };
        layout.validate()?;
        layout.quota(limits.max_dataset_bytes, 0)?;
        if action == "restore" {
            let options = RestoreOptions {
                source: source.into(),
                destination: required(&mut values, "--destination")?.into(),
                layout,
                limits,
            };
            match profile {
                Profile::Legacy => Action::Restore(options),
                Profile::Memory => Action::RestoreMemory(options),
                Profile::Composed => Action::RestoreComposed(options),
            }
        } else {
            match profile {
                Profile::Legacy => Action::Verify {
                    source: source.into(),
                    layout,
                    limits,
                },
                Profile::Memory => Action::VerifyMemory {
                    source: source.into(),
                    layout,
                    limits,
                },
                Profile::Composed => Action::VerifyComposed {
                    source: source.into(),
                    layout,
                    limits,
                },
            }
        }
    };
    if !values.is_empty() {
        return Err(Error::Invalid("option incompatible with subcommand"));
    }
    Ok(result)
}
