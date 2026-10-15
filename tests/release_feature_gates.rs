//! Candidate-only feature gates over the existing real-process and native suites.
#![forbid(unsafe_code)]

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "common/process.rs"]
mod process;

use std::process::Command;
use std::time::{Duration, Instant};

use gate_receipt::GateContext;
use serde_json::json;

struct Suite {
    binary: &'static str,
    filter: Option<&'static str>,
    exact: bool,
    ignored: bool,
}

impl Suite {
    const fn all(binary: &'static str) -> Self {
        Self {
            binary,
            filter: None,
            exact: false,
            ignored: false,
        }
    }

    const fn named(binary: &'static str, filter: &'static str) -> Self {
        Self {
            binary,
            filter: Some(filter),
            exact: true,
            ignored: false,
        }
    }

    const fn filtered(binary: &'static str, filter: &'static str) -> Self {
        Self {
            binary,
            filter: Some(filter),
            exact: false,
            ignored: false,
        }
    }

    const fn ignored(binary: &'static str, filter: &'static str) -> Self {
        Self {
            binary,
            filter: Some(filter),
            exact: true,
            ignored: true,
        }
    }
}

fn positive_cases(output: &str) -> Result<u64, String> {
    let summaries: Vec<_> = output
        .lines()
        .filter(|line| line.starts_with("test result: "))
        .collect();
    let [summary] = summaries.as_slice() else {
        return Err("expected exactly one Rust test summary".into());
    };
    let passed = summary
        .strip_prefix("test result: ok. ")
        .and_then(|line| line.split_whitespace().next())
        .and_then(|count| count.parse::<u64>().ok())
        .filter(|count| *count > 0)
        .ok_or("suite has no passed tests")?;
    if !summary.contains(" passed; 0 failed;") {
        return Err("suite contains failed tests".into());
    }
    Ok(passed)
}

fn run_gate(gate: &str, suites: &[Suite]) {
    let context = GateContext::from_env(gate).expect("exact candidate gate context");
    let started = Instant::now();
    let mut total = 0_u64;
    let mut results = Vec::new();
    for suite in suites {
        let mut command = Command::new("cargo");
        command
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("RUST_TEST_THREADS", "1")
            .args(["test", "--locked", "--test", suite.binary]);
        if let Some(filter) = suite.filter {
            command.arg(filter);
        }
        command.args(["--", "--nocapture"]);
        if suite.ignored {
            command.arg("--ignored");
        }
        if suite.exact {
            command.arg("--exact");
        }
        let command_text = format!("{command:?}");
        let output = process::run(&mut command, Duration::from_secs(1200))
            .unwrap_or_else(|error| panic!("{command_text}: {error}"));
        assert!(
            output.status.success(),
            "{command_text}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("UTF-8 Rust test output");
        let cases = positive_cases(&stdout).expect("nonempty successful suite");
        total = total.checked_add(cases).expect("case count overflow");
        results.push(json!({
            "command": command_text,
            "passed": cases,
            "summary": stdout.lines().find(|line| line.starts_with("test result: ")),
        }));
    }
    context
        .publish(total, started.elapsed(), json!({"suites": results}))
        .expect("receipt after completed suites");
}

#[test]
#[ignore = "requires a clean candidate SHA, OCI digest, and new evidence directory"]
fn release_memory_lifecycle_gate() {
    run_gate(
        "memory_lifecycle",
        &[
            Suite::all("agent_memory_tcp"),
            Suite::all("agent_memory_aof"),
            Suite::all("agent_memory_backup"),
            Suite::all("agent_memory_migration"),
            Suite::all("agent_memory_migration_cli"),
            Suite::named(
                "replication_network",
                "memory_replica_process_follows_full_delta_and_reconnects_after_source_restart",
            ),
        ],
    );
}

#[test]
#[ignore = "requires a clean candidate SHA, OCI digest, and new evidence directory"]
fn release_memory_search_gate() {
    run_gate(
        "memory_search",
        &[
            Suite::filtered("agent_memory_search", "exact_search"),
            Suite::filtered("agent_memory_tcp", "memory_tcp_search"),
        ],
    );
}

#[test]
#[ignore = "requires a clean candidate SHA, pinned Redis image, and new evidence directory"]
fn release_resp3_gate() {
    run_gate(
        "resp3",
        &[
            Suite::named("resp3", "resp3_tcp_literal_subset"),
            Suite::ignored("resp3", "resp3_differential_subset"),
        ],
    );
}

#[test]
#[ignore = "requires a clean candidate SHA, OCI digest, and new evidence directory"]
fn release_security_gate() {
    run_gate(
        "security",
        &[
            Suite::all("security"),
            Suite::all("security_internal"),
            Suite::filtered("cli", "security_"),
        ],
    );
}

#[test]
#[ignore = "requires a clean candidate SHA, OCI digest, and new evidence directory"]
fn release_composed_transactions_gate() {
    run_gate(
        "composed_transactions",
        &[
            Suite::all("composed_server"),
            Suite::all("replication_composed_protocol"),
            Suite::all("replication_composed_export"),
            Suite::all("replication_composed_backup"),
            Suite::all("transactions_persistence"),
            Suite::filtered("replication_network", "composed_"),
            Suite::named(
                "security_internal",
                "security_composed_replica_streams_mixed_batch_and_fences_revoked_role",
            ),
        ],
    );
}

#[test]
fn suite_summary_requires_positive_completed_cases() {
    assert_eq!(
        positive_cases("running 2 tests\ntest result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 1.00s\n").unwrap(),
        2
    );
    for output in [
        "running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s",
        "test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
        "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
    ] {
        assert!(positive_cases(output).is_err());
    }
}
