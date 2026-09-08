//! Regressões dos processos descartáveis; não dependem de Docker disponível.

#![forbid(unsafe_code)]

#[path = "common/process.rs"]
mod process;
#[allow(dead_code)]
#[path = "common/redis_reference.rs"]
mod redis_reference;
#[path = "common/sider_process.rs"]
mod sider_process;

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use process::{OwnedChild, run};
use sider_process::SiderProcess;

const TIMEOUT: Duration = Duration::from_secs(10);

fn helper(mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "process_child", "--nocapture"])
        .env("SIDER_HARNESS_CHILD", mode);
    command
}

// Chamado normalmente sem efeitos, ou como filho com um modo explicitamente
// injetado. Não há teste ignorado contado como validação desses caminhos.
#[test]
fn process_child() {
    let Some(mode) = std::env::var_os("SIDER_HARNESS_CHILD") else {
        return;
    };
    match mode.to_str().expect("helper mode") {
        "success" => {
            print!("child-output");
            eprint!("child-diagnostic");
        }
        "failure" => {
            std::io::stdout().write_all(b"child-output").unwrap();
            std::io::stderr()
                .write_all(b"\xffbinary-diagnostic")
                .unwrap();
            std::process::exit(7);
        }
        "stall" => loop {
            std::thread::park();
        },
        "flood" => {
            let bytes = vec![b'x'; process::OUTPUT_LIMIT + 1];
            std::io::stdout().write_all(&bytes).unwrap();
        }
        "flood_stderr" => {
            let bytes = vec![b'x'; process::OUTPUT_LIMIT + 1];
            std::io::stderr().write_all(&bytes).unwrap();
        }
        "both_streams" => {
            let stdout = std::thread::spawn(|| {
                std::io::stdout()
                    .write_all(&vec![b'o'; 256 * 1024])
                    .unwrap();
            });
            std::io::stderr()
                .write_all(&vec![b'e'; 256 * 1024])
                .unwrap();
            stdout.join().unwrap();
        }
        "dirty_sider_environment" => {
            let mut sider = SiderProcess::start(
                Path::new(env!("CARGO_BIN_EXE_sider")),
                env!("CARGO_PKG_VERSION"),
            );
            sider.assert_alive();
            assert!(sider.address().ip().is_loopback());
            sider.finish();
        }
        unknown => panic!("unknown helper mode {unknown}"),
    }
}

#[test]
fn process_runner_captures_both_streams_and_preserves_failure_status() {
    let success = run(&mut helper("success"), TIMEOUT).unwrap();
    assert!(success.status.success());
    assert!(
        success
            .stdout
            .windows(b"child-output".len())
            .any(|w| w == b"child-output")
    );
    assert!(
        success
            .stderr
            .windows(b"child-diagnostic".len())
            .any(|w| w == b"child-diagnostic")
    );
    let failure = run(&mut helper("failure"), TIMEOUT).unwrap();
    assert_eq!(failure.status.code(), Some(7));
    assert!(failure.stderr.starts_with(b"\xffbinary-diagnostic"));
}

#[test]
fn process_runner_rejects_excessive_output_instead_of_truncated_success() {
    for mode in ["flood", "flood_stderr"] {
        let error = run(&mut helper(mode), TIMEOUT).unwrap_err();
        assert!(error.contains("limite"), "{mode}: {error}");
    }
}

#[test]
fn process_runner_drains_both_pipes_concurrently_without_deadlock() {
    let output = run(&mut helper("both_streams"), TIMEOUT).unwrap();
    assert!(output.status.success());
    assert!(
        output
            .stdout
            .windows(256 * 1024)
            .any(|w| w.iter().all(|b| *b == b'o'))
    );
    assert_eq!(output.stderr, vec![b'e'; 256 * 1024]);
}

#[test]
fn deadline_fails_and_owned_child_can_be_confirmed_reaped() {
    let mut child = OwnedChild::spawn(&mut helper("stall")).unwrap();
    child.assert_alive().unwrap();
    let start = Instant::now();
    let error = child.wait(Duration::from_millis(50)).unwrap_err();
    assert!(error.contains("prazo"), "{error}");
    let output = child.terminate(TIMEOUT).unwrap();
    assert!(!output.status.success());
    assert!(child.assert_alive().is_err());
    assert!(start.elapsed() < TIMEOUT);
}

#[test]
fn process_runner_reports_missing_executable() {
    let missing = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/sider-does-not-exist-harness");
    assert!(!missing.exists());
    assert!(run(&mut Command::new(&missing), TIMEOUT).is_err());
    assert!(SiderProcess::try_start(&missing, env!("CARGO_PKG_VERSION")).is_err());
}

#[test]
fn sider_rejects_wrong_binary_version_before_launching_server() {
    let result = SiderProcess::try_start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        "not-the-package-version",
    );
    let error = result.err().expect("version mismatch must fail");
    assert!(error.contains("versão"), "{error}");
}

#[test]
fn sider_process_proves_readiness_liveness_and_cleanup() {
    let mut sider = SiderProcess::start(
        Path::new(env!("CARGO_BIN_EXE_sider")),
        env!("CARGO_PKG_VERSION"),
    );
    assert!(sider.address().ip().is_loopback());
    assert_ne!(sider.address().port(), 0);
    sider.assert_alive();
    sider.finish();
}

#[test]
fn sider_clears_inherited_configuration_only_in_its_child() {
    let result = run(
        helper("dirty_sider_environment")
            .env("SIDER_ADDR", "invalid")
            .env("SIDER_MAX_CONNECTIONS", "0")
            .env("SIDER_FRAME_TIMEOUT_MS", "0")
            .env("SIDER_READY_FILE", "nonexistent-parent/unowned-ready.json"),
        TIMEOUT,
    )
    .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
