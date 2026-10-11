//! Complete manifest-last generations with restricted file objects and fixed basenames.

use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path, sync::Arc};

use super::{
    ClientPolicy, ClientSecurity, ClientTransport, Error, InternalPolicy, InternalSecurity,
    InternalTransport,
};

const CLIENT: &[&str] = &["client-policy.json", "client-cert.pem", "client-key.pem"];
const INTERNAL: &[&str] = &[
    "internal-policy.json",
    "internal-cert.pem",
    "internal-key.pem",
    "internal-ca.pem",
    "internal-crl.pem",
];
const MANIFEST: &str = "security-manifest.json";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    generation: u64,
    #[serde(deserialize_with = "unique_files")]
    files: BTreeMap<String, String>,
}
fn unique_files<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    struct Unique;
    impl<'de> Visitor<'de> for Unique {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("unique fixed file digests")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut files = BTreeMap::new();
            while let Some((name, value)) = map.next_entry::<String, String>()? {
                if files.insert(name, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate file"));
                }
            }
            Ok(files)
        }
    }
    deserializer.deserialize_map(Unique)
}

/// An immutable complete generation; policy and TLS consume the same loaded bytes.
pub struct Generation {
    pub number: u64,
    pub client: Option<ClientSecurity>,
    pub internal: Option<InternalSecurity>,
    manifest_bytes: Vec<u8>,
}
impl Generation {
    /// Validates an in-memory client generation supplied by an embedding caller.
    /// Executable deployment still requires the restricted directory loader.
    pub fn client_files(
        number: u64,
        policy: &[u8],
        certificate: &[u8],
        key: &[u8],
    ) -> Result<Self, Error> {
        let files = BTreeMap::from([
            (CLIENT[0].to_owned(), policy.to_vec()),
            (CLIENT[1].to_owned(), certificate.to_vec()),
            (CLIENT[2].to_owned(), key.to_vec()),
        ]);
        let digests: BTreeMap<_, _> = files
            .iter()
            .map(|(name, bytes)| {
                let digest = Sha256::digest(bytes)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                (name, digest)
            })
            .collect();
        let manifest = serde_json::to_vec(
            &serde_json::json!({"version": 1, "generation": number, "files": digests}),
        )
        .map_err(|_| Error::Configuration)?;
        Self::validate(&manifest, &manifest, &files, CLIENT, true, false, false)
    }
    /// Loads a server generation, checking permissions before any listeners open.
    pub fn server(directory: &Path, internal: bool, outgoing: bool) -> Result<Self, Error> {
        Self::load(directory, true, internal, outgoing)
    }
    /// Loads an internal tool generation; client credentials are neither read nor included.
    pub fn tool(directory: &Path) -> Result<Self, Error> {
        Self::load(directory, false, true, true)
    }
    fn load(directory: &Path, client: bool, internal: bool, outgoing: bool) -> Result<Self, Error> {
        // TLS component tests do not establish the Windows filesystem permission
        // contract. Keep deployment disabled pending the platform evidence matrix.
        if cfg!(windows) {
            return Err(Error::PlatformValidation);
        }
        if !directory.is_absolute() || !directory.is_dir() {
            return Err(Error::Configuration);
        }
        let names: Vec<_> = CLIENT
            .iter()
            .filter(|_| client)
            .chain(INTERNAL.iter().filter(|_| internal))
            .copied()
            .collect();
        let files = restricted_files(directory, &names)?;
        let before = files.get(MANIFEST).ok_or(Error::Configuration)?;
        let after = files.get("manifest-after").ok_or(Error::Configuration)?;
        Self::validate(before, after, &files, &names, client, internal, outgoing)
    }
    fn validate(
        before: &[u8],
        after: &[u8],
        files: &BTreeMap<String, Vec<u8>>,
        names: &[&str],
        client: bool,
        internal: bool,
        outgoing: bool,
    ) -> Result<Self, Error> {
        if before != after || before.len() > 16 * 1024 {
            return Err(Error::Configuration);
        }
        let manifest: Manifest =
            serde_json::from_slice(before).map_err(|_| Error::Configuration)?;
        if manifest.version != 1 || manifest.generation == 0 || manifest.files.len() != names.len()
        {
            return Err(Error::Configuration);
        }
        for name in names {
            let digest = manifest.files.get(*name).ok_or(Error::Configuration)?;
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(Error::Configuration);
            }
            let bytes = files.get(*name).ok_or(Error::Configuration)?;
            if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 {
                return Err(Error::Configuration);
            }
            let actual = Sha256::digest(bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            if digest != &actual {
                return Err(Error::Configuration);
            }
        }
        let get = |name| {
            files
                .get(name)
                .map(Vec::as_slice)
                .ok_or(Error::Configuration)
        };
        let client_security = if client {
            Some(ClientSecurity::new(
                ClientPolicy::parse(get("client-policy.json")?)?,
                ClientTransport::new(get("client-cert.pem")?, get("client-key.pem")?)?,
            ))
        } else {
            None
        };
        let internal = if internal {
            let policy = Arc::new(InternalPolicy::parse(
                get("internal-policy.json")?,
                outgoing,
            )?);
            let constructor = if client {
                InternalTransport::new
            } else {
                InternalTransport::tool
            };
            let transport = constructor(
                get("internal-cert.pem")?,
                get("internal-key.pem")?,
                get("internal-ca.pem")?,
                get("internal-crl.pem")?,
                policy.clone(),
            )?;
            Some(InternalSecurity { policy, transport })
        } else {
            None
        };
        Ok(Self {
            number: manifest.generation,
            client: client_security,
            internal,
            manifest_bytes: before.to_vec(),
        })
    }
    /// Rejects reused or older generation numbers, including changed bytes with the active number.
    pub fn replacement_of(&self, active: &Self) -> Result<bool, Error> {
        if self.manifest_bytes == active.manifest_bytes {
            return Ok(false);
        }
        if self.number <= active.number {
            return Err(Error::Configuration);
        }
        Ok(true)
    }
}

#[cfg(unix)]
fn restricted_files(directory: &Path, names: &[&str]) -> Result<BTreeMap<String, Vec<u8>>, Error> {
    use std::{
        fs::{self, File},
        io::Read,
        os::unix::fs::{MetadataExt, OpenOptionsExt},
    };
    let uid = rustix::process::geteuid().as_raw();
    for path in directory.ancestors() {
        let metadata = fs::symlink_metadata(path).map_err(|_| Error::Configuration)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != uid && metadata.uid() != 0
            || metadata.mode() & 0o022 != 0
        {
            return Err(Error::Configuration);
        }
        if path == directory && (metadata.uid() != uid || metadata.mode() & 0o777 != 0o700) {
            return Err(Error::Configuration);
        }
    }
    let read = |name: &str| -> Result<Vec<u8>, Error> {
        let mut file = File::options()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(directory.join(name))
            .map_err(|_| Error::Configuration)?;
        let metadata = file.metadata().map_err(|_| Error::Configuration)?;
        if !metadata.is_file()
            || metadata.uid() != uid
            || metadata.mode() & 0o777 != 0o600
            || metadata.len() > 4 * 1024 * 1024
        {
            return Err(Error::Configuration);
        }
        let mut bytes = Vec::new();
        file.by_ref()
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Configuration)?;
        if bytes.len() > 4 * 1024 * 1024 || bytes.is_empty() {
            return Err(Error::Configuration);
        }
        Ok(bytes)
    };
    let mut files = BTreeMap::new();
    files.insert(MANIFEST.to_owned(), read(MANIFEST)?);
    for name in names {
        files.insert((*name).to_owned(), read(name)?);
    }
    files.insert("manifest-after".to_owned(), read(MANIFEST)?);
    Ok(files)
}

#[cfg(windows)]
fn restricted_files(directory: &Path, names: &[&str]) -> Result<BTreeMap<String, Vec<u8>>, Error> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use std::{
        io::Read,
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    // .NET inspects each opened FileStream's owner and DACL, then reads that same
    // handle. Trusted ancestors are checked before descending; no path is supplied
    // through script syntax. PowerShell runs hidden without user profiles.
    use std::os::windows::process::CommandExt;
    const SCRIPT: &str = include_str!("restricted-files.ps1");
    let encoded = STANDARD.encode(
        SCRIPT
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>(),
    );
    let system = std::env::var_os("SystemRoot").ok_or(Error::Configuration)?;
    let shell = Path::new(&system).join("System32/WindowsPowerShell/v1.0/powershell.exe");
    let mut child = Command::new(shell)
        .args(["-NoProfile", "-NonInteractive", "-EncodedCommand", &encoded])
        .env("SIDER_SECURITY_READ_DIR", directory)
        .env("SIDER_SECURITY_READ_FILES", names.join(","))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .creation_flags(0x08000000)
        .spawn()
        .map_err(|_| Error::Configuration)?;
    let pipe = child.stdout.take().ok_or(Error::Configuration)?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = pipe
            .take(64 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map(|_| bytes);
        let _ = sender.send(result);
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut bytes = None;
    let success = loop {
        if bytes.is_none()
            && let Ok(result) = receiver.try_recv()
        {
            match result {
                Ok(value) if value.len() <= 64 * 1024 * 1024 => bytes = Some(value),
                _ => break false,
            }
        }
        match child.try_wait() {
            Ok(Some(status)) => break status.success(),
            Err(_) => break false,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Ok(None) => break false,
        }
    };
    if !success {
        let _ = child.kill();
    }
    let _ = child.wait();
    reader.join().map_err(|_| Error::Configuration)?;
    if !success {
        return Err(Error::Configuration);
    }
    let bytes = bytes
        .or_else(|| receiver.try_recv().ok().and_then(Result::ok))
        .ok_or(Error::Configuration)?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(Error::Configuration);
    }
    let encoded: BTreeMap<String, String> =
        serde_json::from_slice(&bytes).map_err(|_| Error::Configuration)?;
    encoded
        .into_iter()
        .map(|(name, bytes)| {
            STANDARD
                .decode(bytes)
                .map(|bytes| (name, bytes))
                .map_err(|_| Error::Configuration)
        })
        .collect()
}

#[cfg(not(any(unix, windows)))]
fn restricted_files(_: &Path, _: &[&str]) -> Result<BTreeMap<String, Vec<u8>>, Error> {
    Err(Error::Configuration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn fixture(number: u64) -> (Value, BTreeMap<String, Vec<u8>>) {
        let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let files: BTreeMap<String, Vec<u8>> = BTreeMap::from([
            (
                "client-policy.json".into(),
                br#"{"version":1,"users":[]}"#.to_vec(),
            ),
            (
                "client-cert.pem".into(),
                certificate.cert.pem().into_bytes(),
            ),
            (
                "client-key.pem".into(),
                certificate.key_pair.serialize_pem().into_bytes(),
            ),
        ]);
        let hashes: BTreeMap<_, _> = files
            .iter()
            .map(|(name, bytes)| {
                let digest = Sha256::digest(bytes)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                (name.clone(), digest)
            })
            .collect();
        (
            json!({"version":1,"generation":number,"files":hashes}),
            files,
        )
    }

    fn validate(manifest: &Value, files: &BTreeMap<String, Vec<u8>>) -> Result<Generation, Error> {
        let bytes = serde_json::to_vec(manifest).unwrap();
        Generation::validate(&bytes, &bytes, files, CLIENT, true, false, false)
    }

    #[test]
    fn security_generation_rejects_missing_changed_extra_and_noncanonical_files() {
        let (manifest, files) = fixture(1);
        assert!(validate(&manifest, &files).is_ok());
        let mut missing = files.clone();
        missing.remove("client-key.pem");
        assert!(validate(&manifest, &missing).is_err());
        let mut changed = files.clone();
        changed.get_mut("client-policy.json").unwrap().push(b' ');
        assert!(validate(&manifest, &changed).is_err());
        for mutation in [
            ("generation", json!(0)),
            ("version", json!(2)),
            ("unknown", json!(true)),
        ] {
            let mut invalid = manifest.clone();
            invalid[mutation.0] = mutation.1;
            assert!(validate(&invalid, &files).is_err());
        }
        let mut extra = manifest.clone();
        extra["files"]["../private.pem"] = json!("00".repeat(32));
        assert!(validate(&extra, &files).is_err());
        let mut noncanonical = manifest;
        noncanonical["files"]["client-policy.json"] = json!("A".repeat(64));
        assert!(validate(&noncanonical, &files).is_err());
    }

    #[test]
    fn security_generation_requires_unchanged_manifest_and_monotonic_activation() {
        let (manifest, files) = fixture(4);
        let before = serde_json::to_vec(&manifest).unwrap();
        let mut after = before.clone();
        after.push(b' ');
        assert!(Generation::validate(&before, &after, &files, CLIENT, true, false, false).is_err());
        let active = validate(&manifest, &files).unwrap();
        let changed_same_number =
            Generation::validate(&after, &after, &files, CLIENT, true, false, false).unwrap();
        assert!(changed_same_number.replacement_of(&active).is_err());
        assert!(
            !validate(&manifest, &files)
                .unwrap()
                .replacement_of(&active)
                .unwrap()
        );
        let mut reused = manifest.clone();
        reused["generation"] = json!(3);
        assert!(
            validate(&reused, &files)
                .unwrap()
                .replacement_of(&active)
                .is_err()
        );
        reused["generation"] = json!(5);
        assert!(
            validate(&reused, &files)
                .unwrap()
                .replacement_of(&active)
                .unwrap()
        );
        let duplicate = br#"{"version":1,"generation":1,"files":{"client-policy.json":"a","client-policy.json":"b"}}"#;
        assert!(serde_json::from_slice::<Manifest>(duplicate).is_err());
    }
}
