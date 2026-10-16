//! Protected Docker process rehearsal and candidate image gate.
#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;
#[path = "common/wire.rs"]
mod wire;

#[cfg(target_os = "linux")]
mod linux {
    use super::gate_receipt::GateContext;
    use super::process;
    use super::wire;
    use std::cell::RefCell;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output, Stdio};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tokio_rustls::rustls;
    use wire::Response;

    const PASSWORD: &[u8] = b"docker-rehearsal-password";
    const READ_ONLY_PASSWORD: &[u8] = b"docker-readonly-password";
    const TIMEOUT: Duration = Duration::from_secs(25);

    thread_local! {
        static DOCKER_COMMANDS: RefCell<Option<Vec<Value>>> = const { RefCell::new(None) };
    }

    fn record_docker(args: &[&str], duration: Duration, output: &Output) {
        DOCKER_COMMANDS.with(|cell| {
            if let Some(commands) = cell.borrow_mut().as_mut() {
                commands.push(json!({
                    "argv": args,
                    "seconds": duration.as_secs_f64(),
                    "exit_code": output.status.code(),
                    "stdout_bytes": output.stdout.len(),
                    "stdout_sha256": format!("{:x}", Sha256::digest(&output.stdout)),
                    "stderr_bytes": output.stderr.len(),
                    "stderr_sha256": format!("{:x}", Sha256::digest(&output.stderr)),
                }));
            }
        });
    }

    fn save_command_log(directory: &Path) -> Result<Value, String> {
        let commands = DOCKER_COMMANDS.with(|cell| cell.borrow_mut().take());
        let commands = commands.ok_or("Docker command recording was not started")?;
        if commands.is_empty() {
            return Err("Docker image gate recorded no commands".into());
        }
        let bytes = serde_json::to_vec_pretty(&commands).map_err(|error| error.to_string())?;
        let path = directory.join("docker-oci-commands.json");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| error.to_string())?;
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        Ok(json!({
            "file": "docker-oci-commands.json",
            "sha256": format!("{:x}", Sha256::digest(&bytes)),
            "count": commands.len(),
        }))
    }

    fn docker(args: &[&str]) -> Result<String, String> {
        let started = Instant::now();
        let output = process::run(Command::new("docker").args(args), Duration::from_secs(120))
            .map_err(|error| format!("docker {args:?}: {error}"))?;
        record_docker(args, started.elapsed(), &output);
        if !output.status.success() {
            return Err(format!(
                "docker {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        String::from_utf8(output.stdout).map_err(|error| error.to_string())
    }

    fn docker_input(args: &[&str], input: &[u8]) -> Result<String, String> {
        let started = Instant::now();
        let mut child = Command::new("docker")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| error.to_string())?;
        child
            .stdin
            .take()
            .ok_or("missing docker stdin")?
            .write_all(input)
            .map_err(|error| error.to_string())?;
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if child
                .try_wait()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("docker {args:?}: timed out"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child
            .wait_with_output()
            .map_err(|error| error.to_string())?;
        record_docker(args, started.elapsed(), &output);
        if !output.status.success() {
            return Err(format!(
                "docker {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        String::from_utf8(output.stdout).map_err(|error| error.to_string())
    }

    struct Harness {
        root: PathBuf,
        image: String,
        revision: String,
        prefix: String,
        containers: Vec<String>,
        volumes: Vec<String>,
        security: PathBuf,
        cert: rustls::pki_types::CertificateDer<'static>,
        owner: String,
        extra_security: Vec<PathBuf>,
    }

    struct InternalFixture {
        tool: PathBuf,
        replica: PathBuf,
    }

    fn write_generation(
        directory: &Path,
        files: &[(String, Vec<u8>)],
        number: u64,
    ) -> Result<(), String> {
        fs::create_dir_all(directory).map_err(|error| error.to_string())?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
        let mut digests = serde_json::Map::new();
        for (name, bytes) in files {
            let path = directory.join(name);
            fs::write(&path, bytes).map_err(|error| error.to_string())?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
            digests.insert(name.clone(), json!(format!("{:x}", Sha256::digest(bytes))));
        }
        let manifest = directory.join("security-manifest.json");
        fs::write(
            &manifest,
            serde_json::to_vec(&json!({"version":1,"generation":number,"files":digests})).unwrap(),
        )
        .map_err(|error| error.to_string())?;
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600))
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    impl Harness {
        fn new(image: &str) -> Result<Self, String> {
            let inspection: Value = serde_json::from_str(&docker(&["image", "inspect", image])?)
                .map_err(|error| error.to_string())?;
            let image_properties = inspection
                .as_array()
                .and_then(|images| images.first())
                .ok_or("Docker image inspect returned no image")?;
            if image_properties["Os"] != "linux"
                || image_properties["Architecture"] != "amd64"
                || image_properties["Config"]["User"] != "10001:10001"
                || image_properties["Config"]["Labels"]["org.opencontainers.image.version"]
                    != env!("CARGO_PKG_VERSION")
                || !image_properties["Config"]["Env"]
                    .as_array()
                    .is_some_and(|values| {
                        values.contains(&json!("SIDER_SECURITY_PROFILE=protected"))
                    })
            {
                return Err("image platform, user, version, or protected default mismatch".into());
            }
            let revision =
                image_properties["Config"]["Labels"]["org.opencontainers.image.revision"]
                    .as_str()
                    .ok_or("image lacks source revision label")?;
            if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err("invalid source revision label".into());
            }
            let nonce = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
            let prefix = format!("sider-oci-{}-{nonce}", std::process::id());
            let root = std::env::temp_dir().join(&prefix);
            fs::create_dir(&root).map_err(|error| error.to_string())?;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                .map_err(|error| error.to_string())?;
            let metadata = fs::metadata(&root).map_err(|error| error.to_string())?;
            let owner = format!("{}:{}", metadata.uid(), metadata.gid());
            let security = root.join("security");
            fs::create_dir(&security).map_err(|error| error.to_string())?;
            fs::set_permissions(&security, fs::Permissions::from_mode(0o700))
                .map_err(|error| error.to_string())?;
            let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])
                .map_err(|error| error.to_string())?;
            let cert = certified.cert.der().clone();
            let verifier = docker_input(
                &[
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--interactive",
                    "--entrypoint",
                    "/usr/local/bin/sider",
                    image,
                    "--hash-password",
                ],
                PASSWORD,
            )?;
            let read_only_verifier = docker_input(
                &[
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--interactive",
                    "--entrypoint",
                    "/usr/local/bin/sider",
                    image,
                    "--hash-password",
                ],
                READ_ONLY_PASSWORD,
            )?;
            let now = time::OffsetDateTime::now_utc();
            let format = &time::format_description::well_known::Rfc3339;
            let policy = json!({
                "version": 1,
                "users": [{
                    "name": "admin",
                    "credentials": [{
                        "verifier": verifier.trim(),
                        "not_before": (now - time::Duration::minutes(1)).format(format).unwrap(),
                        "expires_at": (now + time::Duration::days(1)).format(format).unwrap()
                    }],
                    "commands": ["GET", "SET", "PING", "MULTI", "EXEC", "MEM.CREATE", "MEM.PUT", "MEM.GET"],
                    "read_keys": {"exact": [], "prefix": [], "all": true},
                    "write_keys": {"exact": [], "prefix": [], "all": true},
                    "publish_channels": {"exact": [], "prefix": [], "all": true},
                    "subscribe_channels": {"exact": [], "prefix": [], "all": true},
                    "info_sections": []
                }, {
                    "name": "reader",
                    "credentials": [{
                        "verifier": read_only_verifier.trim(),
                        "not_before": (now - time::Duration::minutes(1)).format(format).unwrap(),
                        "expires_at": (now + time::Duration::days(1)).format(format).unwrap()
                    }],
                    "commands": ["GET"],
                    "read_keys": {"exact": [], "prefix": [], "all": true},
                    "write_keys": {"exact": [], "prefix": [], "all": false},
                    "publish_channels": {"exact": [], "prefix": [], "all": false},
                    "subscribe_channels": {"exact": [], "prefix": [], "all": false},
                    "info_sections": []
                }]
            });
            let files = [
                ("client-policy.json", serde_json::to_vec(&policy).unwrap()),
                ("client-cert.pem", certified.cert.pem().into_bytes()),
                (
                    "client-key.pem",
                    certified.key_pair.serialize_pem().into_bytes(),
                ),
            ];
            let mut digests = serde_json::Map::new();
            for (name, bytes) in &files {
                let path = security.join(name);
                fs::write(&path, bytes).map_err(|error| error.to_string())?;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                    .map_err(|error| error.to_string())?;
                digests.insert(
                    (*name).into(),
                    json!(format!("{:x}", Sha256::digest(bytes))),
                );
            }
            let manifest = security.join("security-manifest.json");
            fs::write(
                &manifest,
                serde_json::to_vec(&json!({"version":1,"generation":1,"files":digests})).unwrap(),
            )
            .map_err(|error| error.to_string())?;
            fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
            let mount = format!("type=bind,source={},target=/security", security.display());
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "0:0",
                "--mount",
                &mount,
                "--entrypoint",
                "/usr/bin/chown",
                image,
                "-R",
                "10001:10001",
                "/security",
            ])?;
            Ok(Self {
                root,
                image: image.into(),
                revision: revision.into(),
                prefix,
                containers: Vec::new(),
                volumes: Vec::new(),
                security,
                cert,
                owner,
                extra_security: Vec::new(),
            })
        }

        fn chown_security(&self, owner: &str) -> Result<(), String> {
            self.chown_path(&self.security, owner)
        }

        fn chown_path(&self, path: &Path, owner: &str) -> Result<(), String> {
            let mount = format!("type=bind,source={},target=/security", path.display());
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "0:0",
                "--mount",
                &mount,
                "--entrypoint",
                "/usr/bin/chown",
                &self.image,
                "-R",
                owner,
                "/security",
            ])?;
            Ok(())
        }

        fn internal_fixture(&mut self) -> Result<InternalFixture, String> {
            const SERVER: &str = "spiffe://sider/primary-a/server";
            const REPLICA: &str = "spiffe://sider/holder-b/replica";
            const TOOL: &str = "spiffe://sider/holder-c/backup";

            let issuer_key = rcgen::KeyPair::generate().map_err(|error| error.to_string())?;
            let mut issuer_params = rcgen::CertificateParams::new(Vec::<String>::new())
                .map_err(|error| error.to_string())?;
            issuer_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            issuer_params.key_usages = vec![
                rcgen::KeyUsagePurpose::KeyCertSign,
                rcgen::KeyUsagePurpose::CrlSign,
            ];
            let issuer = issuer_params
                .self_signed(&issuer_key)
                .map_err(|error| error.to_string())?;
            let leaf = |uri: &str, serial: u64| -> Result<(String, String), String> {
                let key = rcgen::KeyPair::generate().map_err(|error| error.to_string())?;
                let mut params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()])
                    .map_err(|error| error.to_string())?;
                params.serial_number = Some(serial.into());
                params.subject_alt_names.push(rcgen::SanType::URI(
                    uri.try_into().map_err(|error| format!("{error:?}"))?,
                ));
                params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
                params.extended_key_usages = vec![
                    rcgen::ExtendedKeyUsagePurpose::ClientAuth,
                    rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                ];
                let cert = params
                    .signed_by(&key, &issuer, &issuer_key)
                    .map_err(|error| error.to_string())?;
                Ok((cert.pem(), key.serialize_pem()))
            };
            let server = leaf(SERVER, 1)?;
            let replica = leaf(REPLICA, 2)?;
            let tool = leaf(TOOL, 3)?;
            let now = time::OffsetDateTime::now_utc();
            let crl = rcgen::CertificateRevocationListParams {
                this_update: now - time::Duration::hours(1),
                next_update: now + time::Duration::days(1),
                crl_number: 1_u64.into(),
                issuing_distribution_point: None,
                revoked_certs: Vec::new(),
                key_identifier_method: rcgen::KeyIdMethod::Sha256,
            }
            .signed_by(&issuer, &issuer_key)
            .map_err(|error| error.to_string())?
            .pem()
            .map_err(|error| error.to_string())?;
            let ca = issuer.pem();
            let internal_files = |policy: Value, identity: &(String, String)| {
                vec![
                    (
                        "internal-policy.json".into(),
                        serde_json::to_vec(&policy).unwrap(),
                    ),
                    ("internal-cert.pem".into(), identity.0.as_bytes().to_vec()),
                    ("internal-key.pem".into(), identity.1.as_bytes().to_vec()),
                    ("internal-ca.pem".into(), ca.as_bytes().to_vec()),
                    ("internal-crl.pem".into(), crl.as_bytes().to_vec()),
                ]
            };
            self.chown_security(&self.owner)?;
            let client_files: Vec<_> = ["client-policy.json", "client-cert.pem", "client-key.pem"]
                .into_iter()
                .map(|name| {
                    fs::read(self.security.join(name))
                        .map(|bytes| (name.to_owned(), bytes))
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<_, _>>()?;
            let primary_policy = json!({
                "version":1,"instance_id":"primary-a","principals":[
                    {"uri_san":REPLICA,"roles":["replica"],"upstream_instance":"primary-a"},
                    {"uri_san":TOOL,"roles":["backup","monitor"]}
                ]
            });
            let mut primary_files = client_files.clone();
            primary_files.extend(internal_files(primary_policy, &server));
            write_generation(&self.security, &primary_files, 1)?;
            let tool_directory = self.root.join("tool-security");
            let tool_policy = json!({
                "version":1,"instance_id":"holder-c","principals":[],"upstream_principal":SERVER
            });
            write_generation(&tool_directory, &internal_files(tool_policy, &tool), 1)?;
            self.extra_security.push(tool_directory.clone());
            let replica_directory = self.root.join("replica-security");
            let replica_policy = json!({
                "version":1,"instance_id":"holder-b","principals":[],"upstream_principal":SERVER
            });
            let mut replica_files = client_files;
            replica_files.extend(internal_files(replica_policy, &replica));
            write_generation(&replica_directory, &replica_files, 1)?;
            self.extra_security.push(replica_directory.clone());
            self.chown_security("10001:10001")?;
            self.chown_path(&tool_directory, "10001:10001")?;
            self.chown_path(&replica_directory, "10001:10001")?;
            Ok(InternalFixture {
                tool: tool_directory,
                replica: replica_directory,
            })
        }

        fn remove_admin(&self) -> Result<(), String> {
            self.chown_security(&self.owner)?;
            let policy_path = self.security.join("client-policy.json");
            let mut policy: Value =
                serde_json::from_slice(&fs::read(&policy_path).map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?;
            policy["users"]
                .as_array_mut()
                .ok_or("client policy has no users")?
                .retain(|user| user["name"] != "admin");
            let staged_policy = self.security.join(".next-client-policy.json");
            fs::write(&staged_policy, serde_json::to_vec(&policy).unwrap())
                .map_err(|error| error.to_string())?;
            fs::set_permissions(&staged_policy, fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
            fs::rename(staged_policy, policy_path).map_err(|error| error.to_string())?;
            let mut digests = serde_json::Map::new();
            for name in ["client-policy.json", "client-cert.pem", "client-key.pem"] {
                let bytes =
                    fs::read(self.security.join(name)).map_err(|error| error.to_string())?;
                digests.insert(name.into(), json!(format!("{:x}", Sha256::digest(bytes))));
            }
            let staged_manifest = self.security.join(".next-security-manifest.json");
            fs::write(
                &staged_manifest,
                serde_json::to_vec(&json!({"version":1,"generation":2,"files":digests})).unwrap(),
            )
            .map_err(|error| error.to_string())?;
            fs::set_permissions(&staged_manifest, fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
            self.chown_security("10001:10001")?;
            let mount = format!(
                "type=bind,source={},target=/security",
                self.security.display()
            );
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "0:0",
                "--mount",
                &mount,
                "--entrypoint",
                "/usr/bin/mv",
                &self.image,
                "/security/.next-security-manifest.json",
                "/security/security-manifest.json",
            ])?;
            Ok(())
        }

        fn remove_backup_role(&self) -> Result<(), String> {
            self.chown_security(&self.owner)?;
            let policy_path = self.security.join("internal-policy.json");
            let mut policy: Value =
                serde_json::from_slice(&fs::read(&policy_path).map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?;
            let backup_principal = policy["principals"]
                .as_array_mut()
                .and_then(|principals| {
                    principals
                        .iter_mut()
                        .find(|principal| principal["uri_san"] == "spiffe://sider/holder-c/backup")
                })
                .ok_or("backup principal not found")?;
            backup_principal["roles"] = json!(["monitor"]);
            let staged_policy = self.security.join(".next-internal-policy.json");
            fs::write(&staged_policy, serde_json::to_vec(&policy).unwrap())
                .map_err(|error| error.to_string())?;
            fs::set_permissions(&staged_policy, fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
            fs::rename(staged_policy, policy_path).map_err(|error| error.to_string())?;
            let mut digests = serde_json::Map::new();
            for name in [
                "client-policy.json",
                "client-cert.pem",
                "client-key.pem",
                "internal-policy.json",
                "internal-cert.pem",
                "internal-key.pem",
                "internal-ca.pem",
                "internal-crl.pem",
            ] {
                let bytes =
                    fs::read(self.security.join(name)).map_err(|error| error.to_string())?;
                digests.insert(name.into(), json!(format!("{:x}", Sha256::digest(bytes))));
            }
            let staged_manifest = self.security.join(".next-security-manifest.json");
            fs::write(
                &staged_manifest,
                serde_json::to_vec(&json!({"version":1,"generation":2,"files":digests})).unwrap(),
            )
            .map_err(|error| error.to_string())?;
            fs::set_permissions(&staged_manifest, fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
            self.chown_security("10001:10001")?;
            let mount = format!(
                "type=bind,source={},target=/security",
                self.security.display()
            );
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "0:0",
                "--mount",
                &mount,
                "--entrypoint",
                "/usr/bin/mv",
                &self.image,
                "/security/.next-security-manifest.json",
                "/security/security-manifest.json",
            ])?;
            Ok(())
        }

        fn volume(&mut self, suffix: &str) -> Result<String, String> {
            let name = format!("{}-{suffix}", self.prefix);
            docker(&["volume", "create", &name])?;
            self.volumes.push(name.clone());
            Ok(name)
        }

        fn startup_refuses_security(
            &mut self,
            suffix: &str,
            mount_security: bool,
        ) -> Result<(), String> {
            let name = format!("{}-{suffix}", self.prefix);
            let mount = format!(
                "type=bind,source={},target=/run/sider/security,readonly",
                self.security.display()
            );
            let mut args = vec![
                "run",
                "--detach",
                "--name",
                &name,
                "--read-only",
                "--user",
                "10001:10001",
                "--cap-drop",
                "ALL",
                "--publish",
                "127.0.0.1::6379",
            ];
            if mount_security {
                args.extend(["--mount", &mount]);
            }
            args.push(&self.image);
            docker(&args)?;
            self.containers.push(name.clone());
            let deadline = Instant::now() + TIMEOUT;
            loop {
                let state: Value = serde_json::from_str(&docker(&[
                    "inspect",
                    "--format",
                    "{{json .State}}",
                    &name,
                ])?)
                .map_err(|error| error.to_string())?;
                let port = docker(&["port", &name, "6379/tcp"]).unwrap_or_default();
                if let Ok(address) = port.trim().parse::<SocketAddrV4>()
                    && TcpStream::connect_timeout(&address.into(), Duration::from_millis(100))
                        .is_ok()
                {
                    return Err(format!("{suffix} exposed a client listener"));
                }
                if state["Running"] == false {
                    if state["ExitCode"] == 0 {
                        return Err(format!("{suffix} exited successfully"));
                    }
                    docker(&["rm", "--volumes", &name])?;
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "{suffix} reached a running state without valid security"
                    ));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        fn owned_volume(&mut self, suffix: &str) -> Result<String, String> {
            let volume = self.volume(suffix)?;
            let mount = format!("type=volume,source={volume},target=/backups");
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "0:0",
                "--mount",
                &mount,
                "--entrypoint",
                "/usr/bin/chown",
                &self.image,
                "10001:10001",
                "/backups",
            ])?;
            Ok(volume)
        }

        fn backup_profile(
            &self,
            primary: &str,
            tool_security: &Path,
            volume: &str,
            profile: &str,
        ) -> Result<(), String> {
            let (selector, selector_value, shards) = match profile {
                "memory" => ("--record-version", "2", "1"),
                "composed" => ("--profile", "composed", "4"),
                _ => return Err("unsupported backup profile".into()),
            };
            let network = format!("container:{primary}");
            let security = format!(
                "type=bind,source={},target=/run/sider/security,readonly",
                tool_security.display()
            );
            let destination = format!("type=volume,source={volume},target=/backups");
            docker(&[
                "run",
                "--rm",
                "--network",
                &network,
                "--user",
                "10001:10001",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--mount",
                &security,
                "--mount",
                &destination,
                "--env",
                "SIDER_SECURITY_PROFILE=protected",
                "--env",
                "SIDER_SECURITY_DIR=/run/sider/security",
                "--entrypoint",
                "/usr/local/bin/sider-backup",
                &self.image,
                "export",
                selector,
                selector_value,
                "--source",
                "127.0.0.1:6380",
                "--destination",
                "/backups/cut",
                "--source-sha",
                &self.revision,
            ])
            .map_err(|error| {
                let logs = docker(&["logs", primary]).unwrap_or_else(|failure| failure);
                format!("{error}; primary logs: {logs}")
            })?;
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "10001:10001",
                "--read-only",
                "--mount",
                &destination,
                "--entrypoint",
                "/usr/local/bin/sider-backup",
                &self.image,
                "verify",
                selector,
                selector_value,
                "--source",
                "/backups/cut",
                "--shards",
                shards,
                "--routing",
                "1",
            ])?;
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "10001:10001",
                "--read-only",
                "--mount",
                &destination,
                "--entrypoint",
                "/usr/local/bin/sider-backup",
                &self.image,
                "restore",
                selector,
                selector_value,
                "--source",
                "/backups/cut",
                "--destination",
                "/backups/restored",
                "--shards",
                shards,
                "--routing",
                "1",
            ])?;
            Ok(())
        }

        fn backup_rejected_for_replica_role(
            &self,
            primary: &str,
            replica_security: &Path,
            volume: &str,
            profile: &str,
        ) -> Result<(), String> {
            let (selector, selector_value) = match profile {
                "memory" => ("--record-version", "2"),
                "composed" => ("--profile", "composed"),
                _ => return Err("unsupported backup profile".into()),
            };
            let network = format!("container:{primary}");
            let security = format!(
                "type=bind,source={},target=/run/sider/security,readonly",
                replica_security.display()
            );
            let destination = format!("type=volume,source={volume},target=/backups");
            let path = format!("/backups/denied-replica-{profile}");
            if docker(&[
                "run",
                "--rm",
                "--network",
                &network,
                "--user",
                "10001:10001",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--mount",
                &security,
                "--mount",
                &destination,
                "--env",
                "SIDER_SECURITY_PROFILE=protected",
                "--env",
                "SIDER_SECURITY_DIR=/run/sider/security",
                "--entrypoint",
                "/usr/local/bin/sider-backup",
                &self.image,
                "export",
                selector,
                selector_value,
                "--source",
                "127.0.0.1:6380",
                "--destination",
                &path,
                "--source-sha",
                &self.revision,
            ])
            .is_ok()
            {
                return Err("replica identity was allowed to export a backup".into());
            }
            docker(&[
                "run",
                "--rm",
                "--network",
                "none",
                "--mount",
                &destination,
                "--entrypoint",
                "/usr/bin/test",
                &self.image,
                "!",
                "-e",
                &path,
            ])?;
            Ok(())
        }

        fn backup_denied_after_rotation(
            &self,
            primary: &str,
            tool_security: &Path,
            volume: &str,
        ) -> Result<(), String> {
            let network = format!("container:{primary}");
            let security = format!(
                "type=bind,source={},target=/run/sider/security,readonly",
                tool_security.display()
            );
            let destination = format!("type=volume,source={volume},target=/backups");
            let deadline = Instant::now() + TIMEOUT;
            let mut attempt = 0;
            loop {
                let path = format!("/backups/denied-{attempt}");
                let result = docker(&[
                    "run",
                    "--rm",
                    "--network",
                    &network,
                    "--user",
                    "10001:10001",
                    "--read-only",
                    "--mount",
                    &security,
                    "--mount",
                    &destination,
                    "--env",
                    "SIDER_SECURITY_PROFILE=protected",
                    "--env",
                    "SIDER_SECURITY_DIR=/run/sider/security",
                    "--entrypoint",
                    "/usr/local/bin/sider-backup",
                    &self.image,
                    "export",
                    "--profile",
                    "composed",
                    "--source",
                    "127.0.0.1:6380",
                    "--destination",
                    &path,
                    "--source-sha",
                    &self.revision,
                ]);
                if result.is_err() {
                    docker(&[
                        "run",
                        "--rm",
                        "--network",
                        "none",
                        "--mount",
                        &destination,
                        "--entrypoint",
                        "/usr/bin/test",
                        &self.image,
                        "!",
                        "-e",
                        &path,
                    ])?;
                    let status = docker(&[
                        "run",
                        "--rm",
                        "--network",
                        &network,
                        "--user",
                        "10001:10001",
                        "--read-only",
                        "--mount",
                        &security,
                        "--env",
                        "SIDER_SECURITY_PROFILE=protected",
                        "--env",
                        "SIDER_SECURITY_DIR=/run/sider/security",
                        "--entrypoint",
                        "/usr/local/bin/sider-replica",
                        &self.image,
                        "--addr",
                        "127.0.0.1:6380",
                        "--profile",
                        "composed",
                        "--status",
                    ])?;
                    let status: Value =
                        serde_json::from_str(&status).map_err(|error| error.to_string())?;
                    if status["role"] != "primary" {
                        return Err("monitor role lost access after backup revocation".into());
                    }
                    return Ok(());
                }
                attempt += 1;
                if Instant::now() >= deadline {
                    return Err("backup role removal did not take effect".into());
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }

        fn run(&mut self, suffix: &str, volume: &str, mode: &str) -> Result<String, String> {
            self.run_configured(suffix, volume, mode, false, None)
        }

        fn run_primary(
            &mut self,
            suffix: &str,
            volume: &str,
            mode: &str,
        ) -> Result<String, String> {
            self.run_configured(suffix, volume, mode, true, None)
        }

        fn run_restored(
            &mut self,
            suffix: &str,
            volume: &str,
            mode: &str,
        ) -> Result<String, String> {
            self.run_configured(suffix, volume, mode, true, Some("/var/lib/sider/restored"))
        }

        fn run_configured(
            &mut self,
            suffix: &str,
            volume: &str,
            mode: &str,
            internal: bool,
            aof_dir: Option<&str>,
        ) -> Result<String, String> {
            let name = format!("{}-{suffix}", self.prefix);
            let data = format!("type=volume,source={volume},target=/var/lib/sider");
            let security = format!(
                "type=bind,source={},target=/run/sider/security,readonly",
                self.security.display()
            );
            let (record, shards, composed) = match mode {
                "memory" => ("2", "1", false),
                "composed" => ("1", "4", true),
                _ => return Err("unknown durable mode".into()),
            };
            let record_env = format!("SIDER_AOF_RECORD_VERSION={record}");
            let shards_env = format!("SIDER_SHARDS={shards}");
            let aof_env = aof_dir.map(|directory| format!("SIDER_AOF_DIR={directory}"));
            let mut args = vec![
                "run",
                "--detach",
                "--name",
                &name,
                "--read-only",
                "--user",
                "10001:10001",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--mount",
                &data,
                "--mount",
                &security,
                "--publish",
                "127.0.0.1::6379",
                "--env",
                "SIDER_ADDR=0.0.0.0:6379",
                "--env",
                "SIDER_AOF_SYNC=always",
                "--env",
                "SIDER_SECURITY_PROFILE=protected",
                "--env",
                "SIDER_SECURITY_DIR=/run/sider/security",
                "--env",
                &record_env,
                "--env",
                &shards_env,
            ];
            if composed {
                args.extend(["--env", "SIDER_AOF_COMPOSED=1"]);
            }
            if internal {
                args.extend([
                    "--publish",
                    "127.0.0.1::6381",
                    "--env",
                    "SIDER_REPLICATION_ADDR=127.0.0.1:6380",
                    "--env",
                    "RUST_LOG=debug",
                ]);
            }
            if let Some(ref aof_env) = aof_env {
                args.extend(["--env", aof_env]);
            }
            args.push(&self.image);
            docker(&args)?;
            self.containers.push(name.clone());
            Ok(name)
        }

        fn run_replica(
            &mut self,
            suffix: &str,
            primary: &str,
            volume: &str,
            security: &Path,
            mode: &str,
        ) -> Result<String, String> {
            let name = format!("{}-{suffix}", self.prefix);
            let network = format!("container:{primary}");
            let data = format!("type=volume,source={volume},target=/var/lib/sider");
            let security = format!(
                "type=bind,source={},target=/run/sider/security,readonly",
                security.display()
            );
            let (record, shards, composed) = match mode {
                "memory" => ("SIDER_AOF_RECORD_VERSION=2", "SIDER_SHARDS=1", false),
                "composed" => ("SIDER_AOF_RECORD_VERSION=1", "SIDER_SHARDS=4", true),
                _ => return Err("unsupported replica profile".into()),
            };
            let mut args = vec![
                "run",
                "--detach",
                "--name",
                &name,
                "--network",
                &network,
                "--read-only",
                "--user",
                "10001:10001",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--mount",
                &data,
                "--mount",
                &security,
                "--env",
                "SIDER_ADDR=0.0.0.0:6381",
                "--env",
                "SIDER_AOF_SYNC=always",
                "--env",
                record,
                "--env",
                shards,
                "--env",
                "SIDER_REPLICA_OF=127.0.0.1:6380",
                "--env",
                "SIDER_SECURITY_PROFILE=protected",
                "--env",
                "SIDER_SECURITY_DIR=/run/sider/security",
            ];
            if composed {
                args.extend(["--env", "SIDER_AOF_COMPOSED=1"]);
            }
            args.push(&self.image);
            docker(&args)?;
            self.containers.push(name.clone());
            Ok(name)
        }

        fn stop(&self, name: &str) -> Result<(), String> {
            docker(&["stop", name])?;
            docker(&["rm", name])?;
            Ok(())
        }

        fn assert_logs_redact(&self, name: &str, canaries: &[&[u8]]) -> Result<(), String> {
            let logs = docker(&["logs", name])?;
            for canary in canaries {
                if !canary.is_empty()
                    && logs
                        .as_bytes()
                        .windows(canary.len())
                        .any(|window| window == *canary)
                {
                    return Err(format!(
                        "container {name} logged protected application material"
                    ));
                }
            }
            Ok(())
        }

        fn kill(&self, name: &str) -> Result<(), String> {
            docker(&["kill", "--signal", "KILL", name])?;
            docker(&["rm", name])?;
            Ok(())
        }

        fn address(&self, name: &str) -> Result<SocketAddrV4, String> {
            self.published_address(name, "6379/tcp")
        }

        fn published_address(
            &self,
            name: &str,
            container_port: &str,
        ) -> Result<SocketAddrV4, String> {
            let deadline = Instant::now() + TIMEOUT;
            loop {
                let port = docker(&["port", name, container_port])?;
                if let Ok(address) = port.trim().parse::<SocketAddrV4>()
                    && address.ip().is_loopback()
                    && address.port() > 0
                    && self.connect(address).is_ok()
                {
                    return Ok(address);
                }
                let inspected: Value = serde_json::from_str(&docker(&[
                    "inspect",
                    "--format",
                    "{{json .State}}",
                    name,
                ])?)
                .map_err(|error| error.to_string())?;
                if inspected["Running"] != true || Instant::now() >= deadline {
                    return Err(format!(
                        "container {name} did not become ready: {inspected}"
                    ));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        fn connect(
            &self,
            address: SocketAddrV4,
        ) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, String> {
            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(self.cert.clone())
                .map_err(|error| error.to_string())?;
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_root_certificates(roots)
            .with_no_client_auth();
            let server_name = rustls::pki_types::ServerName::IpAddress(Ipv4Addr::LOCALHOST.into());
            let connection = rustls::ClientConnection::new(Arc::new(config), server_name)
                .map_err(|error| error.to_string())?;
            let socket = TcpStream::connect_timeout(&address.into(), Duration::from_secs(2))
                .map_err(|error| error.to_string())?;
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .map_err(|error| error.to_string())?;
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .map_err(|error| error.to_string())?;
            let mut stream = rustls::StreamOwned::new(connection, socket);
            stream.flush().map_err(|error| error.to_string())?;
            Ok(stream)
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            for name in self.containers.iter().rev() {
                let _ = docker(&["rm", "--force", name]);
            }
            for name in self.volumes.iter().rev() {
                let _ = docker(&["volume", "rm", name]);
            }
            for path in &self.extra_security {
                let _ = self.chown_path(path, &self.owner);
            }
            let _ = self.chown_security(&self.owner);
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn send(
        stream: &mut rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
        args: &[&[u8]],
    ) -> Result<Response, String> {
        let owned: Vec<_> = args.iter().map(|value| value.to_vec()).collect();
        stream
            .write_all(&wire::request(&owned))
            .map_err(|error| error.to_string())?;
        stream.flush().map_err(|error| error.to_string())?;
        wire::read_response(stream)
            .map(|observed| observed.value)
            .map_err(|error| error.to_string())
    }

    fn authenticate(
        stream: &mut rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
    ) -> Result<(), String> {
        if send(stream, &[b"AUTH", b"admin", PASSWORD])? != Response::Simple(b"OK".to_vec()) {
            return Err("authenticated Docker session refused".into());
        }
        Ok(())
    }

    fn run_rehearsal(image: &str) -> Result<(), String> {
        let mut harness = Harness::new(image)?;
        harness.startup_refuses_security("missing-security", false)?;
        harness.chown_security(&harness.owner)?;
        let key = harness.security.join("client-key.pem");
        fs::set_permissions(&key, fs::Permissions::from_mode(0o644))
            .map_err(|error| error.to_string())?;
        harness.startup_refuses_security("broad-key", true)?;
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600))
            .map_err(|error| error.to_string())?;
        harness.chown_security("10001:10001")?;
        let memory_volume = harness.volume("memory")?;
        let composed_volume = harness.volume("composed")?;
        let memory = harness.run("memory-first", &memory_volume, "memory")?;
        let memory_address = harness.address(&memory)?;
        let mut plain = TcpStream::connect(memory_address).map_err(|error| error.to_string())?;
        plain
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(|error| error.to_string())?;
        plain
            .write_all(&wire::request(&[b"PING".to_vec()]))
            .map_err(|error| error.to_string())?;
        let mut bytes = [0; 16];
        if matches!(plain.read(&mut bytes), Ok(count) if bytes[..count].starts_with(b"+PONG")) {
            return Err("plaintext client received PONG".into());
        }
        let mut client = harness.connect(memory_address)?;
        if send(&mut client, &[b"PING"])?
            != Response::Error(b"NOAUTH Authentication required".to_vec())
        {
            return Err("unauthenticated client was admitted".into());
        }
        authenticate(&mut client)?;
        let mut resp3 = harness.connect(memory_address)?;
        authenticate(&mut resp3)?;
        resp3
            .write_all(&wire::request(&[b"HELLO".to_vec(), b"3".to_vec()]))
            .map_err(|error| error.to_string())?;
        resp3.flush().map_err(|error| error.to_string())?;
        let mut marker = [0];
        resp3
            .read_exact(&mut marker)
            .map_err(|error| error.to_string())?;
        if marker != *b"%" {
            return Err("HELLO 3 did not produce a RESP3 map".into());
        }
        let mut read_only = harness.connect(memory_address)?;
        if send(&mut read_only, &[b"AUTH", b"reader", READ_ONLY_PASSWORD])?
            != Response::Simple(b"OK".to_vec())
        {
            return Err("read-only identity could not authenticate".into());
        }
        if send(&mut read_only, &[b"SET", b"forbidden", b"value"])?
            != Response::Error(b"NOPERM Permission denied".to_vec())
        {
            return Err("read-only identity changed the dataset".into());
        }
        if send(&mut client, &[b"SET", b"ordinary", b"value"])? != Response::Simple(b"OK".to_vec())
        {
            return Err("memory profile SET failed".into());
        }
        if send(
            &mut client,
            &[
                b"MEM.CREATE",
                b"collection",
                b"1",
                b"2",
                b"L2SQ",
                b"p",
                b"m",
                b"r",
                b"t",
            ],
        )? != Response::Simple(b"OK".to_vec())
        {
            return Err("memory collection creation failed".into());
        }
        let mut vector = Vec::new();
        vector.extend_from_slice(&1.0_f32.to_le_bytes());
        vector.extend_from_slice(&2.0_f32.to_le_bytes());
        if send(
            &mut client,
            &[
                b"MEM.PUT",
                b"collection",
                b"member",
                b"namespace",
                b"content",
                b"p",
                b"m",
                b"r",
                b"t",
                &vector,
                b"-1",
                b"UPSERT",
                b"0",
                b"0",
            ],
        )? != Response::Integer(1)
        {
            return Err("memory member write failed".into());
        }
        drop(client);
        harness.stop(&memory)?;
        let memory = harness.run("memory-restart", &memory_volume, "memory")?;
        let mut client = harness.connect(harness.address(&memory)?)?;
        authenticate(&mut client)?;
        if send(&mut client, &[b"GET", b"ordinary"])? != Response::Bulk(Some(b"value".to_vec())) {
            return Err("record-2 string did not survive restart".into());
        }
        if !matches!(
            send(&mut client, &[b"MEM.GET", b"collection", b"member"])?,
            Response::Array(Some(_))
        ) {
            return Err("record-2 memory member did not survive restart".into());
        }
        drop(client);
        harness.stop(&memory)?;

        let composed = harness.run("composed-first", &composed_volume, "composed")?;
        let mut client = harness.connect(harness.address(&composed)?)?;
        authenticate(&mut client)?;
        for (args, expected) in [
            (&[b"MULTI".as_slice()][..], Response::Simple(b"OK".to_vec())),
            (
                &[b"SET".as_slice(), b"a", b"left"][..],
                Response::Simple(b"QUEUED".to_vec()),
            ),
            (
                &[b"SET".as_slice(), b"b", b"right"][..],
                Response::Simple(b"QUEUED".to_vec()),
            ),
            (
                &[b"EXEC".as_slice()][..],
                Response::Array(Some(vec![
                    Response::Simple(b"OK".to_vec()),
                    Response::Simple(b"OK".to_vec()),
                ])),
            ),
        ] {
            if send(&mut client, args)? != expected {
                return Err("mixed-owner EXEC failed".into());
            }
        }
        drop(client);
        harness.stop(&composed)?;
        let composed = harness.run("composed-restart", &composed_volume, "composed")?;
        let mut client = harness.connect(harness.address(&composed)?)?;
        authenticate(&mut client)?;
        if send(&mut client, &[b"GET", b"a"])? != Response::Bulk(Some(b"left".to_vec()))
            || send(&mut client, &[b"GET", b"b"])? != Response::Bulk(Some(b"right".to_vec()))
        {
            return Err("mixed-owner commit did not survive restart".into());
        }
        drop(client);
        harness.stop(&composed)?;
        for (suffix, volume, mode) in [
            ("memory-as-composed", &memory_volume, "composed"),
            ("composed-as-memory", &composed_volume, "memory"),
        ] {
            let name = harness.run(suffix, volume, mode)?;
            let deadline = Instant::now() + TIMEOUT;
            loop {
                let state: Value = serde_json::from_str(&docker(&[
                    "inspect",
                    "--format",
                    "{{json .State}}",
                    &name,
                ])?)
                .map_err(|error| error.to_string())?;
                if state["Running"] == false {
                    if state["ExitCode"] == 0 {
                        return Err("wrong durable profile exited successfully".into());
                    }
                    break;
                }
                if Instant::now() >= deadline {
                    return Err("wrong durable profile remained running".into());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        let memory = harness.run("memory-after-refusal", &memory_volume, "memory")?;
        let mut client = harness.connect(harness.address(&memory)?)?;
        authenticate(&mut client)?;
        if send(&mut client, &[b"GET", b"ordinary"])? != Response::Bulk(Some(b"value".to_vec()))
            || send(&mut client, &[b"GET", b"forbidden"])? != Response::Bulk(None)
        {
            return Err("record-2 volume changed after denied access or wrong profile".into());
        }
        harness.remove_admin()?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if send(&mut client, &[b"PING"]).is_err() {
                break;
            }
            if Instant::now() >= deadline {
                return Err("removed administrator session remained active".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        let mut denied = harness.connect(harness.address(&memory)?)?;
        if send(&mut denied, &[b"AUTH", b"admin", PASSWORD])?
            != Response::Error(b"WRONGPASS Authentication failed".to_vec())
        {
            return Err("removed administrator credential remained valid".into());
        }
        let mut reader = harness.connect(harness.address(&memory)?)?;
        if send(&mut reader, &[b"AUTH", b"reader", READ_ONLY_PASSWORD])?
            != Response::Simple(b"OK".to_vec())
            || send(&mut reader, &[b"GET", b"ordinary"])? != Response::Bulk(Some(b"value".to_vec()))
        {
            return Err("surviving reader lost the durable data".into());
        }
        drop(reader);
        drop(denied);
        drop(client);
        harness.assert_logs_redact(
            &memory,
            &[PASSWORD, READ_ONLY_PASSWORD, b"ordinary", b"forbidden"],
        )?;
        harness.stop(&memory)?;
        Ok(())
    }

    fn run_internal_rehearsal(image: &str) -> Result<(), String> {
        let mut harness = Harness::new(image)?;
        let fixture = harness.internal_fixture()?;
        let data = harness.volume("memory-internal")?;
        let backups = harness.owned_volume("memory-backups")?;
        let replica_data = harness.volume("memory-replica")?;
        let primary = harness.run_primary("memory-primary", &data, "memory")?;
        let mut client = harness.connect(harness.address(&primary)?)?;
        authenticate(&mut client)?;
        if send(&mut client, &[b"SET", b"backup-key", b"backup-value"])?
            != Response::Simple(b"OK".to_vec())
        {
            return Err("primary write before backup failed".into());
        }
        harness.backup_rejected_for_replica_role(&primary, &fixture.replica, &backups, "memory")?;
        harness.backup_profile(&primary, &fixture.tool, &backups, "memory")?;
        let replica = harness.run_replica(
            "memory-replica-first",
            &primary,
            &replica_data,
            &fixture.replica,
            "memory",
        )?;
        let replica_address = harness.published_address(&primary, "6381/tcp")?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Ok(mut follower) = harness.connect(replica_address)
                && authenticate(&mut follower).is_ok()
                && send(&mut follower, &[b"GET", b"backup-key"])
                    == Ok(Response::Bulk(Some(b"backup-value".to_vec())))
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("protected follower did not install the primary snapshot".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        harness.stop(&replica)?;
        if send(&mut client, &[b"SET", b"after-disconnect", b"resumed"])?
            != Response::Simple(b"OK".to_vec())
        {
            return Err("primary write during follower disconnect failed".into());
        }
        let replica = harness.run_replica(
            "memory-replica-restarted",
            &primary,
            &replica_data,
            &fixture.replica,
            "memory",
        )?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Ok(mut follower) = harness.connect(replica_address)
                && authenticate(&mut follower).is_ok()
                && send(&mut follower, &[b"GET", b"after-disconnect"])
                    == Ok(Response::Bulk(Some(b"resumed".to_vec())))
            {
                if !matches!(send(&mut follower, &[b"SET", b"read-only", b"denied"]), Ok(Response::Error(error)) if error.starts_with(b"READONLY"))
                {
                    return Err("follower accepted a client write".into());
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err("protected follower did not resume after disconnect".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        harness.stop(&replica)?;
        let restored = harness.run_restored("memory-restored", &backups, "memory")?;
        let mut restored_client = harness.connect(harness.address(&restored)?)?;
        authenticate(&mut restored_client)?;
        if send(&mut restored_client, &[b"GET", b"backup-key"])?
            != Response::Bulk(Some(b"backup-value".to_vec()))
            || send(&mut restored_client, &[b"GET", b"after-disconnect"])? != Response::Bulk(None)
        {
            return Err("restored backup did not represent the captured durable cut".into());
        }
        drop(restored_client);
        harness.stop(&restored)?;
        drop(client);
        harness.assert_logs_redact(&primary, &[PASSWORD, b"backup-key", b"backup-value"])?;
        harness.stop(&primary)?;
        Ok(())
    }

    fn run_composed_internal_rehearsal(image: &str) -> Result<(), String> {
        let mut harness = Harness::new(image)?;
        let fixture = harness.internal_fixture()?;
        let data = harness.volume("composed-internal")?;
        let backups = harness.owned_volume("composed-backups")?;
        let replica_data = harness.volume("composed-replica")?;
        let primary = harness.run_primary("composed-primary", &data, "composed")?;
        let mut client = harness.connect(harness.address(&primary)?)?;
        authenticate(&mut client)?;
        for (args, expected) in [
            (&[b"MULTI".as_slice()][..], Response::Simple(b"OK".to_vec())),
            (
                &[b"SET".as_slice(), b"a", b"left"][..],
                Response::Simple(b"QUEUED".to_vec()),
            ),
            (
                &[b"SET".as_slice(), b"b", b"right"][..],
                Response::Simple(b"QUEUED".to_vec()),
            ),
            (
                &[b"EXEC".as_slice()][..],
                Response::Array(Some(vec![
                    Response::Simple(b"OK".to_vec()),
                    Response::Simple(b"OK".to_vec()),
                ])),
            ),
        ] {
            if send(&mut client, args)? != expected {
                return Err("protected mixed-owner transaction failed".into());
            }
        }
        harness.backup_rejected_for_replica_role(
            &primary,
            &fixture.replica,
            &backups,
            "composed",
        )?;
        harness.backup_profile(&primary, &fixture.tool, &backups, "composed")?;
        let replica = harness.run_replica(
            "composed-replica-first",
            &primary,
            &replica_data,
            &fixture.replica,
            "composed",
        )?;
        let replica_address = harness.published_address(&primary, "6381/tcp")?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Ok(mut follower) = harness.connect(replica_address)
                && authenticate(&mut follower).is_ok()
                && send(&mut follower, &[b"GET", b"a"])
                    == Ok(Response::Bulk(Some(b"left".to_vec())))
                && send(&mut follower, &[b"GET", b"b"])
                    == Ok(Response::Bulk(Some(b"right".to_vec())))
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("protected composed follower did not install both owners".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        harness.stop(&replica)?;
        for (args, expected) in [
            (&[b"MULTI".as_slice()][..], Response::Simple(b"OK".to_vec())),
            (
                &[b"SET".as_slice(), b"a", b"later-left"][..],
                Response::Simple(b"QUEUED".to_vec()),
            ),
            (
                &[b"SET".as_slice(), b"b", b"later-right"][..],
                Response::Simple(b"QUEUED".to_vec()),
            ),
            (
                &[b"EXEC".as_slice()][..],
                Response::Array(Some(vec![
                    Response::Simple(b"OK".to_vec()),
                    Response::Simple(b"OK".to_vec()),
                ])),
            ),
        ] {
            if send(&mut client, args)? != expected {
                return Err("primary mixed transaction during follower disconnect failed".into());
            }
        }
        let replica = harness.run_replica(
            "composed-replica-restarted",
            &primary,
            &replica_data,
            &fixture.replica,
            "composed",
        )?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Ok(mut follower) = harness.connect(replica_address)
                && authenticate(&mut follower).is_ok()
                && send(&mut follower, &[b"GET", b"a"])
                    == Ok(Response::Bulk(Some(b"later-left".to_vec())))
                && send(&mut follower, &[b"GET", b"b"])
                    == Ok(Response::Bulk(Some(b"later-right".to_vec())))
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("protected composed follower did not resume both owners".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        harness.stop(&replica)?;
        let restored = harness.run_restored("composed-restored", &backups, "composed")?;
        let mut restored_client = harness.connect(harness.address(&restored)?)?;
        authenticate(&mut restored_client)?;
        if send(&mut restored_client, &[b"GET", b"a"])? != Response::Bulk(Some(b"left".to_vec()))
            || send(&mut restored_client, &[b"GET", b"b"])?
                != Response::Bulk(Some(b"right".to_vec()))
        {
            return Err("composed backup restored only part of the transaction".into());
        }
        drop(restored_client);
        harness.stop(&restored)?;
        harness.remove_backup_role()?;
        harness.backup_denied_after_rotation(&primary, &fixture.tool, &backups)?;
        if send(&mut client, &[b"GET", b"a"])? != Response::Bulk(Some(b"later-left".to_vec()))
            || send(&mut client, &[b"GET", b"b"])? != Response::Bulk(Some(b"later-right".to_vec()))
        {
            return Err("role revocation changed the composed dataset".into());
        }
        drop(client);
        harness.assert_logs_redact(&primary, &[PASSWORD, b"later-left", b"later-right"])?;
        harness.kill(&primary)?;
        let restarted = harness.run_primary("composed-primary-crash-restart", &data, "composed")?;
        let mut recovered = harness.connect(harness.address(&restarted)?)?;
        authenticate(&mut recovered)?;
        if send(&mut recovered, &[b"GET", b"a"])? != Response::Bulk(Some(b"later-left".to_vec()))
            || send(&mut recovered, &[b"GET", b"b"])?
                != Response::Bulk(Some(b"later-right".to_vec()))
        {
            return Err("composed crash recovery replayed a partial transaction".into());
        }
        drop(recovered);
        harness.stop(&restarted)?;
        Ok(())
    }

    fn anonymous_candidate_pull(context: &GateContext) -> Result<(String, Value), String> {
        let digest = context
            .oci_digest()
            .ok_or("candidate OCI digest is missing")?;
        let image = format!("ghcr.io/djairofilho/sider@{digest}");
        let config = std::env::temp_dir().join(format!(
            "sider-anonymous-docker-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir(&config).map_err(|error| error.to_string())?;
        fs::set_permissions(&config, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
        let started = Instant::now();
        let output = process::run(
            Command::new("docker")
                .env_remove("DOCKER_AUTH_CONFIG")
                .args([
                    "--config",
                    config.to_str().ok_or("non-UTF-8 Docker config")?,
                ])
                .args(["pull", "--platform", "linux/amd64", &image]),
            Duration::from_secs(300),
        );
        let _ = fs::remove_dir_all(&config);
        let output = output?;
        record_docker(
            &[
                "--config",
                "<empty-private-directory>",
                "pull",
                "--platform",
                "linux/amd64",
                &image,
            ],
            started.elapsed(),
            &output,
        );
        if !output.status.success() {
            return Err(format!(
                "anonymous docker pull {image}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let inspection: Value = serde_json::from_str(&docker(&["image", "inspect", &image])?)
            .map_err(|error| error.to_string())?;
        let properties = inspection
            .as_array()
            .and_then(|images| images.first())
            .ok_or("candidate image inspection is empty")?;
        if properties["Os"] != "linux"
            || properties["Architecture"] != "amd64"
            || properties["Config"]["Labels"]["org.opencontainers.image.version"]
                != context.version()
            || properties["Config"]["Labels"]["org.opencontainers.image.revision"] != context.sha()
            || !properties["RepoDigests"]
                .as_array()
                .is_some_and(|digests| digests.contains(&json!(image)))
        {
            return Err("candidate platform, version, revision, or digest mismatch".into());
        }
        let pull = json!({
            "command": format!("docker --config <empty-private-directory> pull --platform linux/amd64 {image}"),
            "stdout": String::from_utf8_lossy(&output.stdout),
            "platform": "linux/amd64",
            "revision": context.sha(),
            "digest": digest,
        });
        Ok((image, pull))
    }

    #[test]
    #[ignore = "requires a clean candidate checkout and public GHCR image digest"]
    fn release_docker_oci_gate() {
        let context = GateContext::from_env("docker_oci").expect("exact candidate context");
        let started = Instant::now();
        DOCKER_COMMANDS.with(|cell| *cell.borrow_mut() = Some(Vec::new()));
        let (image, pull) = anonymous_candidate_pull(&context).expect("anonymous digest pull");
        run_rehearsal(&image).expect("protected profiles, access, rotation, and volumes");
        run_internal_rehearsal(&image).expect("protected memory backup and replica");
        run_composed_internal_rehearsal(&image).expect("protected composed backup and replica");
        let command_log = save_command_log(context.release_dir()).expect("Docker command log");
        context
            .publish(
                4,
                started.elapsed(),
                json!({
                    "anonymous_pull": pull,
                    "command_log": command_log,
                    "scenarios": [
                        "protected_profiles_acl_resp3_rotation_volumes",
                        "protected_memory_backup_restore_replica_reconnect",
                        "protected_composed_atomic_backup_restore_replica_reconnect_role_revocation_crash_recovery"
                    ],
                    "image": image,
                }),
            )
            .expect("Docker image gate receipt");
    }

    #[test]
    #[ignore = "requires a local Linux Docker daemon and SIDER_DOCKER_DEV_IMAGE"]
    fn protected_development_image_profiles() {
        let image = std::env::var("SIDER_DOCKER_DEV_IMAGE").expect("development image name");
        run_rehearsal(&image).expect("protected Docker profiles");
    }

    #[test]
    #[ignore = "requires a local Linux Docker daemon and SIDER_DOCKER_DEV_IMAGE"]
    fn protected_development_image_internal_backup() {
        let image = std::env::var("SIDER_DOCKER_DEV_IMAGE").expect("development image name");
        run_internal_rehearsal(&image).expect("protected Docker internal backup");
    }

    #[test]
    #[ignore = "requires a local Linux Docker daemon and SIDER_DOCKER_DEV_IMAGE"]
    fn protected_development_image_composed_backup() {
        let image = std::env::var("SIDER_DOCKER_DEV_IMAGE").expect("development image name");
        run_composed_internal_rehearsal(&image).expect("protected Docker composed backup");
    }
}
