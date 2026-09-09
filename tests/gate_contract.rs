#![forbid(unsafe_code)]

//! Contratos dos recibos com contexto injetado, sem modificar o ambiente ou Git.

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gate_receipt::{GateContext, Observation};
use serde_json::{Value, json};

const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
const TARGET: &str = "x86_64-unknown-linux-gnu";
const IMAGE: &str =
    "redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    out: PathBuf,
    env: BTreeMap<&'static str, OsString>,
    state: Arc<Mutex<Observation>>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "sider-gate-contract-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        fs::write(root.join("Cargo.toml"), b"fixture").unwrap();
        let out = root.join("output");
        fs::create_dir(&out).unwrap();
        let state = Observation {
            head: SHA.to_owned(),
            status: Vec::new(),
            cargo_metadata: json!({"packages": [{
                "name": "sider", "version": "0.1.0", "publish": [], "license": "MIT",
                "repository": "https://github.com/djairofilho/sider",
                "manifest_path": root.join("Cargo.toml")
            }]}),
            plan: json!({
                "schema_version": 1,
                "repository": "djairofilho/sider",
                "reference": {"image": IMAGE, "platform": "linux/amd64", "redis_version": "8.10.1", "redis_cli_version": "8.10.1"},
                "release_policy": {"private": true, "publish_crate": false, "targets": [TARGET]},
                "releases": [{"version": "0.1.0", "required_gates": ["compatibility"]}]
            }),
            compiler: format!("rustc 1.97.1\nhost: {TARGET}\nrelease: 1.97.1\n"),
            compiled_version: "0.1.0".into(),
            compiled_os: "linux".into(),
            compiled_arch: "x86_64".into(),
            compiled_env: "gnu".into(),
        };
        let env = BTreeMap::from([
            ("SIDER_RELEASE_VERSION", OsString::from("0.1.0")),
            ("SIDER_RELEASE_SHA", OsString::from(SHA)),
            ("SIDER_RELEASE_TARGET", OsString::from(TARGET)),
            ("SIDER_REFERENCE_IMAGE", OsString::from(IMAGE)),
            ("SIDER_RELEASE_DIR", out.as_os_str().to_owned()),
        ]);
        Self {
            root,
            out,
            env,
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn context(&self, gate: &str) -> Result<GateContext, String> {
        let env = self.env.clone();
        let state = Arc::clone(&self.state);
        GateContext::from_observer(
            gate,
            self.root.clone(),
            move |name| env.get(name).cloned(),
            move |_| Ok(state.lock().unwrap().clone()),
        )
    }

    fn change(&self, change: impl FnOnce(&mut Observation)) {
        change(&mut self.state.lock().unwrap());
    }

    fn entries(&self) -> Vec<PathBuf> {
        fs::read_dir(&self.out)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Somente arquivos nos dois diretórios exclusivos criados por esta fixture.
        if let Ok(entries) = fs::read_dir(&self.out) {
            for entry in entries.flatten() {
                let _ = fs::remove_file(entry.path());
            }
        }
        let _ = fs::remove_dir(&self.out);
        let _ = fs::remove_file(self.root.join("Cargo.toml"));
        let _ = fs::remove_dir(&self.root);
    }
}

#[test]
fn valid_context_exposes_exact_values_and_publishes_complete_receipt() {
    let fixture = Fixture::new();
    let context = fixture.context("compatibility").unwrap();
    assert_eq!(context.release_dir(), fixture.out);
    assert_eq!(context.version(), "0.1.0");
    assert_eq!(context.sha(), SHA);
    assert_eq!(context.target(), TARGET);
    context
        .publish(
            48,
            Duration::from_millis(1500),
            json!({"seed": 7, "cli_commands": 5}),
        )
        .unwrap();
    let path = fixture.out.join("receipt-compatibility.json");
    let bytes = fs::read(&path).unwrap();
    assert!(bytes.ends_with(b"\n"));
    let receipt: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipt["schema_version"], 1);
    assert_eq!(receipt["gate"], "compatibility");
    assert_eq!(receipt["sha"], SHA);
    assert_eq!(receipt["version"], "0.1.0");
    assert_eq!(receipt["target"], TARGET);
    assert_eq!(receipt["reference_image"], IMAGE);
    assert_eq!(receipt["status"], "success");
    assert_eq!(receipt["cases"], 48);
    assert_eq!(receipt["duration_seconds"], 1.5);
    assert_eq!(receipt["details"]["seed"], 7);
    assert_eq!(fixture.entries(), [path]);
}

#[test]
fn missing_environment_values_fail_without_fallback() {
    for name in [
        "SIDER_RELEASE_VERSION",
        "SIDER_RELEASE_SHA",
        "SIDER_RELEASE_TARGET",
        "SIDER_REFERENCE_IMAGE",
        "SIDER_RELEASE_DIR",
    ] {
        let mut fixture = Fixture::new();
        fixture.env.remove(name);
        assert!(fixture.context("compatibility").is_err(), "{name}");
        assert!(fixture.entries().is_empty());
    }
}

#[test]
fn unknown_gate_rejects_path_traversal_before_observing() {
    let fixture = Fixture::new();
    for gate in [
        "",
        "../other",
        "native",
        "compatibility/../../other",
        "COMPATIBILITY",
    ] {
        assert!(
            GateContext::from_observer(
                gate,
                fixture.root.clone(),
                |_| None,
                |_| panic!("não deve observar gate desconhecido")
            )
            .is_err()
        );
    }
}

#[test]
fn output_requires_an_existing_absolute_directory() {
    for invalid in [PathBuf::from("relative"), PathBuf::new()] {
        let mut fixture = Fixture::new();
        fixture
            .env
            .insert("SIDER_RELEASE_DIR", invalid.into_os_string());
        assert!(fixture.context("compatibility").is_err());
    }
    let mut fixture = Fixture::new();
    for invalid in [
        fixture.root.join("missing"),
        fixture.root.join("Cargo.toml"),
    ] {
        fixture
            .env
            .insert("SIDER_RELEASE_DIR", invalid.into_os_string());
        assert!(fixture.context("compatibility").is_err());
    }
}

#[test]
fn rejects_malformed_or_mismatched_sha() {
    for sha in [
        "",
        "abc",
        "0123456789ABCDEF0123456789ABCDEF01234567",
        "1123456789abcdef0123456789abcdef01234567",
    ] {
        let mut fixture = Fixture::new();
        fixture.env.insert("SIDER_RELEASE_SHA", sha.into());
        assert!(fixture.context("compatibility").is_err(), "{sha}");
    }
}

#[test]
fn tracked_and_untracked_changes_both_block_publication() {
    for status in [
        b" M src/lib.rs\n".as_slice(),
        b"A  src/new.rs\n",
        b"?? new-file\n",
    ] {
        let fixture = Fixture::new();
        fixture.change(|observed| observed.status = status.to_vec());
        assert!(fixture.context("compatibility").is_err());
        assert!(fixture.entries().is_empty());
    }
}

#[test]
fn rejects_wrong_version_invalid_semver_and_stale_compiled_test() {
    for version in [
        "v0.1.0",
        "0.01.0",
        "0.1",
        "0.1.0-rc.0",
        "0.1.0-rc.01",
        "0.1.0-beta.1",
        "0.1.0+meta",
        "0.2.0",
    ] {
        let mut fixture = Fixture::new();
        fixture.env.insert("SIDER_RELEASE_VERSION", version.into());
        assert!(fixture.context("compatibility").is_err(), "{version}");
    }
    let fixture = Fixture::new();
    fixture.change(|observed| observed.compiled_version = "0.0.0".into());
    assert!(fixture.context("compatibility").is_err());
}

#[test]
fn accepts_registered_release_candidate_with_matching_cargo_version() {
    let mut fixture = Fixture::new();
    fixture
        .env
        .insert("SIDER_RELEASE_VERSION", "0.1.0-rc.2".into());
    fixture.change(|observed| {
        observed.compiled_version = "0.1.0-rc.2".into();
        observed.cargo_metadata["packages"][0]["version"] = json!("0.1.0-rc.2");
    });
    assert_eq!(
        fixture.context("compatibility").unwrap().version(),
        "0.1.0-rc.2"
    );
}

#[test]
fn rejects_non_native_or_mislabeled_target() {
    let mut fixture = Fixture::new();
    fixture
        .env
        .insert("SIDER_RELEASE_TARGET", "x86_64-pc-windows-msvc".into());
    assert!(fixture.context("compatibility").is_err());
    for compiler in [
        "rustc 1.97.1\n",
        "host: aarch64-unknown-linux-gnu\n",
        "host: x86_64-unknown-linux-gnu\nhost: x86_64-unknown-linux-gnu\n",
    ] {
        let fixture = Fixture::new();
        fixture.change(|observed| observed.compiler = compiler.into());
        assert!(fixture.context("compatibility").is_err());
    }
    for (os, arch) in [("windows", "x86_64"), ("linux", "aarch64")] {
        let fixture = Fixture::new();
        fixture.change(|observed| {
            observed.compiled_os = os.into();
            observed.compiled_arch = arch.into();
        });
        assert!(fixture.context("compatibility").is_err());
    }
    let fixture = Fixture::new();
    fixture.change(|observed| observed.compiled_env = "musl".into());
    assert!(fixture.context("compatibility").is_err());
}

#[test]
fn requires_private_policy_registered_gate_and_exact_reference() {
    for (pointer, invalid) in [
        ("/release_policy/private", json!(false)),
        ("/release_policy/publish_crate", json!(true)),
        ("/release_policy/targets", json!([])),
        ("/releases/0/required_gates", json!([])),
        ("/releases/0/version", json!("0.2.0")),
        ("/reference/platform", json!("linux/arm64")),
        ("/reference/redis_cli_version", json!("8.10.10")),
        ("/reference/image", json!("redis:latest")),
    ] {
        let fixture = Fixture::new();
        fixture.change(|observed| *observed.plan.pointer_mut(pointer).unwrap() = invalid);
        assert!(fixture.context("compatibility").is_err(), "{pointer}");
    }
    let mut fixture = Fixture::new();
    fixture
        .env
        .insert("SIDER_REFERENCE_IMAGE", "redis:latest".into());
    assert!(fixture.context("compatibility").is_err());
}

#[test]
fn cargo_metadata_must_identify_the_real_root_and_private_package() {
    for (field, invalid) in [
        ("name", json!("other")),
        ("version", json!("0.2.0")),
        ("publish", Value::Null),
        ("publish", json!(["crates-io"])),
        ("license", json!("UNLICENSED")),
        ("repository", json!("https://github.com/other/repo")),
        ("manifest_path", json!("missing-Cargo.toml")),
    ] {
        let fixture = Fixture::new();
        fixture.change(|observed| observed.cargo_metadata["packages"][0][field] = invalid);
        assert!(fixture.context("compatibility").is_err(), "{field}");
    }
    let fixture = Fixture::new();
    fixture.change(|observed| {
        let duplicate = observed.cargo_metadata["packages"][0].clone();
        observed.cargo_metadata["packages"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
    });
    assert!(fixture.context("compatibility").is_err());
}

#[test]
fn zero_cases_and_oversized_receipts_never_publish() {
    let fixture = Fixture::new();
    let compatibility = fixture.context("compatibility").unwrap();
    assert!(
        compatibility
            .publish(0, Duration::from_secs(1), json!({}))
            .is_err()
    );
    assert!(
        compatibility
            .publish(
                1,
                Duration::from_secs(1),
                json!({"log": "x".repeat(1024 * 1024)})
            )
            .is_err()
    );
    assert!(fixture.entries().is_empty());
    compatibility
        .publish(
            12,
            Duration::from_millis(10),
            json!({"binary_comparisons": 12}),
        )
        .unwrap();
    assert_eq!(
        fixture.entries(),
        [fixture.out.join("receipt-compatibility.json")]
    );
}

#[test]
fn details_cannot_replace_mandatory_receipt_fields() {
    let fixture = Fixture::new();
    fixture
        .context("compatibility")
        .unwrap()
        .publish(
            3,
            Duration::from_secs(1),
            json!({"sha": "wrong", "cases": 0, "status": "not_run"}),
        )
        .unwrap();
    let receipt: Value =
        serde_json::from_slice(&fs::read(fixture.out.join("receipt-compatibility.json")).unwrap())
            .unwrap();
    assert_eq!(receipt["sha"], SHA);
    assert_eq!(receipt["cases"], 3);
    assert_eq!(receipt["status"], "success");
    assert_eq!(receipt["details"]["status"], "not_run");
}

#[test]
fn receipt_file_is_never_overwritten_by_start_or_concurrent_context() {
    let fixture = Fixture::new();
    let first = fixture.context("compatibility").unwrap();
    let second = fixture.context("compatibility").unwrap();
    first
        .publish(48, Duration::from_secs(1), json!({"first": true}))
        .unwrap();
    let path = fixture.out.join("receipt-compatibility.json");
    let before = fs::read(&path).unwrap();
    assert!(fixture.context("compatibility").is_err());
    assert!(
        second
            .publish(99, Duration::from_secs(2), json!({"second": true}))
            .is_err()
    );
    assert_eq!(fs::read(path).unwrap(), before);
    assert_eq!(fixture.entries().len(), 1);
}

#[test]
fn head_or_worktree_change_during_gate_prevents_receipt() {
    for changed_head in [false, true] {
        let fixture = Fixture::new();
        let context = fixture.context("compatibility").unwrap();
        fixture.change(|observed| {
            if changed_head {
                observed.head = "1".repeat(40);
            } else {
                observed.status = b"?? unnoticed.rs\n".to_vec();
            }
        });
        assert!(
            context
                .publish(1, Duration::from_secs(1), json!({}))
                .is_err()
        );
        assert!(fixture.entries().is_empty());
    }
}

#[test]
fn last_revalidation_failure_removes_temporary_without_publishing() {
    let fixture = Fixture::new();
    let env = fixture.env.clone();
    let observed = fixture.state.lock().unwrap().clone();
    let calls = AtomicUsize::new(0);
    let context = GateContext::from_observer(
        "compatibility",
        fixture.root.clone(),
        move |name| env.get(name).cloned(),
        move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) >= 2 {
                Err("checkout mudou na conferência final".into())
            } else {
                Ok(observed.clone())
            }
        },
    )
    .unwrap();
    assert!(
        context
            .publish(1, Duration::from_secs(1), json!({}))
            .is_err()
    );
    assert!(fixture.entries().is_empty());
}

#[test]
fn destination_created_at_final_revalidation_is_preserved() {
    let fixture = Fixture::new();
    let env = fixture.env.clone();
    let observed = fixture.state.lock().unwrap().clone();
    let destination = fixture.out.join("receipt-compatibility.json");
    let path = destination.clone();
    let calls = AtomicUsize::new(0);
    let context = GateContext::from_observer(
        "compatibility",
        fixture.root.clone(),
        move |name| env.get(name).cloned(),
        move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) == 2 {
                fs::write(&path, b"another publisher").unwrap();
            }
            Ok(observed.clone())
        },
    )
    .unwrap();
    assert!(
        context
            .publish(1, Duration::from_secs(1), json!({}))
            .is_err()
    );
    assert_eq!(fs::read(&destination).unwrap(), b"another publisher");
    assert_eq!(fixture.entries(), [destination]);
}

#[cfg(unix)]
fn non_unicode() -> OsString {
    use std::os::unix::ffi::OsStringExt;
    OsString::from_vec(vec![0xff])
}

#[cfg(windows)]
fn non_unicode() -> OsString {
    use std::os::windows::ffi::OsStringExt;
    OsString::from_wide(&[0xd800])
}

#[cfg(any(unix, windows))]
#[test]
fn non_unicode_context_text_is_rejected_without_changing_global_environment() {
    for name in [
        "SIDER_RELEASE_VERSION",
        "SIDER_RELEASE_SHA",
        "SIDER_RELEASE_TARGET",
        "SIDER_REFERENCE_IMAGE",
    ] {
        let mut fixture = Fixture::new();
        fixture.env.insert(name, non_unicode());
        assert!(fixture.context("compatibility").is_err(), "{name}");
    }
}
