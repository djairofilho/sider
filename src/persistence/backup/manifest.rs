use serde_json::{Value, json};

use super::{Error, Limits};
use crate::persistence::{DurableLayout, format};
use crate::replication::Cursor;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub sider_version: String,
    /// Declaração do operador, sem inferir o SHA a partir da conexão ou do binário.
    pub source_sha: String,
    pub cursor: Cursor,
    pub layout: DurableLayout,
    pub source_max_record_bytes: u32,
    pub source_max_mutations: u32,
    pub source_max_snapshot_bytes: u64,
    pub validation_max_dataset_bytes: u64,
    pub entries: u64,
    pub snapshot_bytes: u64,
    pub snapshot_sha256: String,
    pub transport_digest: u32,
    pub received_at_unix_ms: i64,
}

pub(super) fn require_hex(text: &str, size: usize) -> Result<(), Error> {
    if text.len() != size
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Invalid("hash ou identidade hexadecimal"));
    }
    Ok(())
}

impl Manifest {
    pub fn json(&self) -> Value {
        json!({
            "schema_version": 1, "sider_version": self.sider_version,
            "source_sha_declared": self.source_sha,
            "cursor": {"epoch": self.cursor.epoch.iter().map(|byte| format!("{byte:02x}")).collect::<String>(), "sequence": self.cursor.sequence},
            "layout": {"shard_count": self.layout.shard_count, "routing_version": self.layout.routing_version},
            "aof_header_version": format::LAYOUT_VERSION, "record_version": format::VERSION,
            "source_limits": {"max_record_bytes":self.source_max_record_bytes,"max_mutations":self.source_max_mutations,"max_snapshot_bytes":self.source_max_snapshot_bytes},
            "validation_max_dataset_bytes":self.validation_max_dataset_bytes,
            "snapshot":{"file":super::SNAPSHOT,"bytes":self.snapshot_bytes,"sha256":self.snapshot_sha256,"entries":self.entries},
            "transport_digest_crc32": self.transport_digest, "received_at_unix_ms":self.received_at_unix_ms,
        })
    }

    pub(super) fn parse(bytes: &[u8], limits: Limits) -> Result<Self, Error> {
        let value: Value = serde_json::from_slice(bytes)?;
        let allowed = [
            "schema_version",
            "sider_version",
            "source_sha_declared",
            "cursor",
            "layout",
            "aof_header_version",
            "record_version",
            "source_limits",
            "validation_max_dataset_bytes",
            "snapshot",
            "transport_digest_crc32",
            "received_at_unix_ms",
        ];
        if value.as_object().is_none_or(|map| {
            map.len() != allowed.len() || map.keys().any(|key| !allowed.contains(&key.as_str()))
        }) || value["schema_version"] != 1
            || value["aof_header_version"] != format::LAYOUT_VERSION
            || value["record_version"] != format::VERSION
            || value["snapshot"]["file"] != super::SNAPSHOT
        {
            return Err(Error::Invalid("schema do manifesto"));
        }
        let text = |value: &Value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or(Error::Invalid("texto do manifesto"))
        };
        let number = |value: &Value| value.as_u64().ok_or(Error::Invalid("inteiro do manifesto"));
        let small = |value: &Value| {
            u32::try_from(number(value)?).map_err(|_| Error::Invalid("inteiro do manifesto"))
        };
        let epoch = text(&value["cursor"]["epoch"])?;
        require_hex(&epoch, 32)?;
        let epoch: Vec<u8> = (0..16)
            .map(|index| u8::from_str_radix(&epoch[index * 2..index * 2 + 2], 16).unwrap())
            .collect();
        let manifest = Self {
            sider_version: text(&value["sider_version"])?,
            source_sha: text(&value["source_sha_declared"])?,
            cursor: Cursor {
                epoch: epoch.try_into().unwrap(),
                sequence: number(&value["cursor"]["sequence"])?,
            },
            layout: DurableLayout {
                shard_count: small(&value["layout"]["shard_count"])?,
                routing_version: small(&value["layout"]["routing_version"])?,
            },
            source_max_record_bytes: small(&value["source_limits"]["max_record_bytes"])?,
            source_max_mutations: small(&value["source_limits"]["max_mutations"])?,
            source_max_snapshot_bytes: number(&value["source_limits"]["max_snapshot_bytes"])?,
            validation_max_dataset_bytes: number(&value["validation_max_dataset_bytes"])?,
            entries: number(&value["snapshot"]["entries"])?,
            snapshot_bytes: number(&value["snapshot"]["bytes"])?,
            snapshot_sha256: text(&value["snapshot"]["sha256"])?,
            transport_digest: small(&value["transport_digest_crc32"])?,
            received_at_unix_ms: value["received_at_unix_ms"]
                .as_i64()
                .ok_or(Error::Invalid("relógio do manifesto"))?,
        };
        manifest.validate(limits)?;
        Ok(manifest)
    }

    pub(super) fn validate(&self, limits: Limits) -> Result<(), Error> {
        limits.validate()?;
        require_hex(&self.source_sha, 40)?;
        require_hex(&self.snapshot_sha256, 64)?;
        self.layout.validate()?;
        if self.sider_version != env!("CARGO_PKG_VERSION")
            || self.source_max_record_bytes == 0
            || self.source_max_record_bytes as usize > limits.max_record_bytes
            || self.source_max_mutations == 0
            || self.source_max_mutations as usize > limits.max_mutations
            || self.source_max_snapshot_bytes == 0
            || self.source_max_snapshot_bytes > limits.max_snapshot_bytes
            || self.snapshot_bytes < format::LAYOUT_HEADER_BYTES as u64
            || self.snapshot_bytes > limits.max_snapshot_bytes
            || self.entries > self.snapshot_bytes / 12
            || self.validation_max_dataset_bytes == 0
            || self.validation_max_dataset_bytes > isize::MAX as u64
        {
            return Err(Error::Invalid("versão ou limites do manifesto"));
        }
        Ok(())
    }
}
