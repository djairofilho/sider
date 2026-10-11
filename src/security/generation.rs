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
        Self::from_files(number, files, CLIENT, false, false)
    }
    /// Validates a complete in-memory server generation. Client entries are
    /// policy/certificate/key; internal entries are policy/certificate/key/CA/CRL.
    /// This embedding seam never bypasses executable directory permission checks.
    pub fn server_files(
        number: u64,
        client: [&[u8]; 3],
        internal: [&[u8]; 5],
        outgoing: bool,
    ) -> Result<Self, Error> {
        let names: Vec<_> = CLIENT.iter().chain(INTERNAL).copied().collect();
        let files = names
            .iter()
            .copied()
            .zip(client.into_iter().chain(internal))
            .map(|(name, bytes)| (name.to_owned(), bytes.to_vec()))
            .collect();
        Self::from_files(number, files, &names, true, outgoing)
    }
    fn from_files(
        number: u64,
        files: BTreeMap<String, Vec<u8>>,
        names: &[&str],
        internal: bool,
        outgoing: bool,
    ) -> Result<Self, Error> {
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
        Self::validate(
            &manifest, &manifest, &files, names, true, internal, outgoing,
        )
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
    let directory = open_restricted_directory(directory)?;
    restricted_files_at(&directory, names)
}

#[cfg(unix)]
fn open_restricted_directory(directory: &Path) -> Result<std::fs::File, Error> {
    use rustix::fs::{Mode, OFlags};
    use std::{fs, os::unix::fs::MetadataExt};
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
    // Pin the validated directory while loading the manifest and fixed file names.
    let directory = std::fs::File::from(
        rustix::fs::open(
            directory,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| Error::Configuration)?,
    );
    let metadata = directory.metadata().map_err(|_| Error::Configuration)?;
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
        return Err(Error::Configuration);
    }
    Ok(directory)
}

#[cfg(unix)]
fn restricted_files_at(
    directory: &std::fs::File,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<u8>>, Error> {
    use rustix::fs::{Mode, OFlags};
    use std::{fs::File, io::Read, os::unix::fs::MetadataExt};
    let uid = rustix::process::geteuid().as_raw();
    let read = |name: &str| -> Result<Vec<u8>, Error> {
        let mut file = File::from(
            rustix::fs::openat(
                directory,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| Error::Configuration)?,
        );
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
    // The helper checks type, owner, and DACL through opened handles and holds
    // ancestor handles while it reads fixed file names. No path is supplied
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

    #[cfg(unix)]
    #[test]
    fn security_posix_restricted_generation_accepts_exact_modes_and_rejects_symlinks_and_broad_access()
     {
        use std::{
            fs,
            os::unix::fs::{PermissionsExt, symlink},
        };
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        // /tmp and a checkout on a shared mount can have writable ancestors.
        // The service home must have safe ancestors; failure is not skipped.
        let root = fs::canonicalize(std::env::var_os("HOME").expect("service home"))
            .unwrap()
            .join(format!(
                ".sider-security-posix-{}-{}",
                std::process::id(),
                time::OffsetDateTime::now_utc().unix_timestamp_nanos()
            ));
        fs::create_dir(&root).unwrap();
        let directory = Directory(root);
        fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o700)).unwrap();
        let (manifest, files) = fixture(1);
        for (name, bytes) in &files {
            let path = directory.0.join(name);
            fs::write(&path, bytes).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let manifest_path = directory.0.join(MANIFEST);
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(Generation::server(&directory.0, false, false).is_ok());
        let policy = directory.0.join("client-policy.json");
        fs::set_permissions(&policy, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(Generation::server(&directory.0, false, false).is_err());
        fs::set_permissions(&policy, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(&policy, directory.0.join("policy-target")).unwrap();
        symlink("policy-target", &policy).unwrap();
        assert!(Generation::server(&directory.0, false, false).is_err());
        fs::remove_file(&policy).unwrap();
        fs::rename(directory.0.join("policy-target"), &policy).unwrap();
        fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o750)).unwrap();
        assert!(Generation::server(&directory.0, false, false).is_err());
        fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(Generation::server(&directory.0, false, false).is_ok());
        let nested = directory.0.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        for name in CLIENT.iter().copied().chain(std::iter::once(MANIFEST)) {
            let path = nested.join(name);
            fs::copy(directory.0.join(name), &path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(Generation::server(&nested, false, false).is_ok());
        fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o770)).unwrap();
        assert!(Generation::server(&nested, false, false).is_err());
        fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o700)).unwrap();
        let alias = directory.0.join("alias");
        symlink("nested", &alias).unwrap();
        assert!(Generation::server(&alias, false, false).is_err());

        let opened = open_restricted_directory(&nested).unwrap();
        let replacement = directory.0.join("replacement");
        fs::create_dir(&replacement).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
        for name in CLIENT.iter().copied().chain(std::iter::once(MANIFEST)) {
            let path = replacement.join(name);
            fs::copy(nested.join(name), &path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        fs::write(replacement.join("client-policy.json"), b"alternate").unwrap();
        fs::rename(&nested, directory.0.join("moved")).unwrap();
        symlink("replacement", &nested).unwrap();
        let anchored = restricted_files_at(&opened, CLIENT).unwrap();
        assert_eq!(
            anchored.get("client-policy.json"),
            files.get("client-policy.json")
        );
        assert!(Generation::server(&nested, false, false).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn security_windows_restricted_files_accept_private_acl_and_reject_broad_grants() {
        use std::{fs, process::Command};
        struct Directory(std::path::PathBuf, std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let resolved = fs::canonicalize(&self.0).ok();
                let root = fs::canonicalize(&self.1).ok();
                if root.is_some()
                    && resolved.as_ref().and_then(|path| path.parent()) == root.as_deref()
                    && self.0.file_name().is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with("sider-security-windows-")
                    })
                {
                    let _ = fs::remove_dir_all(&self.0);
                }
            }
        }
        fn icacls(path: &Path, args: &[&str]) {
            let output = Command::new("icacls.exe")
                .arg(path)
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success(), "ACL setup failed");
        }
        let identity = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Security.Principal.WindowsIdentity]::GetCurrent().User.Value",
            ])
            .output()
            .unwrap();
        assert!(identity.status.success());
        let sid = String::from_utf8(identity.stdout)
            .unwrap()
            .trim()
            .to_owned();
        assert!(sid.starts_with("S-1-"));
        let system_drive = std::env::var_os("SystemDrive").unwrap();
        let root = std::path::PathBuf::from(format!("{}\\", system_drive.to_string_lossy()));
        let directory = Directory(
            root.join(format!(
                "sider-security-windows-{}-{}",
                std::process::id(),
                time::OffsetDateTime::now_utc().unix_timestamp_nanos()
            )),
            root,
        );
        fs::create_dir(&directory.0).unwrap();
        icacls(&directory.0, &["/grant:r", &format!("*{sid}:(OI)(CI)F")]);
        icacls(&directory.0, &["/inheritance:r"]);
        let (manifest, files) = fixture(1);
        for (name, bytes) in &files {
            fs::write(directory.0.join(name), bytes).unwrap();
        }
        fs::write(
            directory.0.join(MANIFEST),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let read = restricted_files(&directory.0, CLIENT).unwrap();
        assert_eq!(
            read.get("client-policy.json"),
            files.get("client-policy.json")
        );
        assert!(
            Generation::validate(
                read.get(MANIFEST).unwrap(),
                read.get("manifest-after").unwrap(),
                &read,
                CLIENT,
                true,
                false,
                false
            )
            .is_ok()
        );

        icacls(&directory.0, &["/grant", "*S-1-1-0:(OI)(CI)F"]);
        assert!(restricted_files(&directory.0, CLIENT).is_err());
        icacls(&directory.0, &["/remove:g", "*S-1-1-0"]);
        assert!(restricted_files(&directory.0, CLIENT).is_ok());

        let policy = directory.0.join("client-policy.json");
        icacls(&policy, &["/grant", "*S-1-1-0:R"]);
        assert!(restricted_files(&directory.0, CLIENT).is_err());
        icacls(&policy, &["/remove:g", "*S-1-1-0"]);
        assert!(restricted_files(&directory.0, CLIENT).is_ok());

        let policy_target = directory.0.join("policy-target");
        fs::rename(&policy, &policy_target).unwrap();
        let file_symlink = std::os::windows::fs::symlink_file(&policy_target, &policy);
        match file_symlink {
            Ok(()) => {
                use std::os::windows::fs::MetadataExt;
                assert_ne!(
                    fs::symlink_metadata(&policy).unwrap().file_attributes() & 0x400,
                    0
                );
                assert!(restricted_files(&directory.0, CLIENT).is_err());
                fs::remove_file(&policy).unwrap();
            }
            Err(error) => {
                assert_eq!(
                    error.raw_os_error(),
                    Some(1314),
                    "unexpected file symlink failure"
                );
                eprintln!("file symlink case unavailable on this host: {error}");
            }
        }
        fs::rename(policy_target, &policy).unwrap();
        assert!(restricted_files(&directory.0, CLIENT).is_ok());

        let nested = directory.0.join("nested");
        fs::create_dir(&nested).unwrap();
        icacls(&nested, &["/grant:r", &format!("*{sid}:(OI)(CI)F")]);
        icacls(&nested, &["/inheritance:r"]);
        for (name, bytes) in &files {
            fs::write(nested.join(name), bytes).unwrap();
        }
        fs::write(
            nested.join(MANIFEST),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        icacls(&directory.0, &["/grant:r", "*S-1-1-0:(AD)"]);
        assert!(restricted_files(&nested, CLIENT).is_ok());
        icacls(&directory.0, &["/grant:r", "*S-1-1-0:(WD)"]);
        assert!(restricted_files(&nested, CLIENT).is_err());
        icacls(&directory.0, &["/grant:r", "*S-1-1-0:(DC)"]);
        assert!(restricted_files(&nested, CLIENT).is_err());
        icacls(&directory.0, &["/remove:g", "*S-1-1-0"]);
        icacls(&directory.0, &["/grant", "*S-1-1-0:(OI)(CI)(IO)F"]);
        assert!(restricted_files(&nested, CLIENT).is_ok());

        let junction = directory.0.join("junction");
        let created = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType Junction -Path $env:SIDER_TEST_JUNCTION -Target $env:SIDER_TEST_TARGET | Out-Null",
            ])
            .env("SIDER_TEST_JUNCTION", &junction)
            .env("SIDER_TEST_TARGET", &nested)
            .output()
            .unwrap();
        assert!(created.status.success(), "junction setup failed");
        use std::os::windows::fs::MetadataExt;
        assert_ne!(
            fs::symlink_metadata(&junction).unwrap().file_attributes() & 0x400,
            0
        );
        assert!(restricted_files(&junction, CLIENT).is_err());
        fs::remove_dir(&junction).unwrap();
        assert!(restricted_files(&nested, CLIENT).is_ok());
    }
}
