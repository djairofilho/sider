//! Small local tools. No CI, automatic publication or implicit GitHub writes.
#![forbid(unsafe_code)]

mod artifacts;
mod github;
mod plan;
mod sync;

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde_json::{Value, json};

const HELP: &str = "Sider: local Rust tools

Run from the repository root:
  cargo xtask check                         fmt, clippy, database build and tests
  cargo xtask check --tools                 fmt, clippy, xtask tests and plan
  cargo xtask validate                      validate plan, gates, and roadmap
  cargo xtask roadmap [--write]             display or regenerate ROADMAP.md
  cargo xtask sync [--apply] [--json]       preview or synchronize the backlog
  cargo xtask verify-release VERSION SHA DIR verify local asset integrity

Does not run Docker or publish implicitly. External gates remain
mandatory for releases. Only sync --apply writes to GitHub; it does not publish releases.
verify-release does not replace tests, RC approval, or verification on GitHub.";

#[derive(Debug, PartialEq, Eq)]
enum Task {
    Help,
    Check {
        tools: bool,
    },
    Validate,
    Roadmap {
        write: bool,
    },
    Sync {
        apply: bool,
        json: bool,
    },
    Verify {
        version: String,
        sha: String,
        directory: PathBuf,
    },
}

fn parse(args: Vec<OsString>) -> Result<Task, String> {
    let strings: Vec<_> = args.iter().map(|arg| arg.to_str()).collect();
    let task = match strings.as_slice() {
        [] | [Some("help" | "--help" | "-h")] => Task::Help,
        [Some("check")] => Task::Check { tools: false },
        [Some("check"), Some("--tools")] => Task::Check { tools: true },
        [Some("validate")] => Task::Validate,
        [Some("roadmap")] => Task::Roadmap { write: false },
        [Some("roadmap"), Some("--write")] => Task::Roadmap { write: true },
        [Some("sync")] => Task::Sync {
            apply: false,
            json: false,
        },
        [Some("sync"), Some("--apply")] => Task::Sync {
            apply: true,
            json: false,
        },
        [Some("sync"), Some("--json")] => Task::Sync {
            apply: false,
            json: true,
        },
        [Some("sync"), Some("--apply"), Some("--json")]
        | [Some("sync"), Some("--json"), Some("--apply")] => Task::Sync {
            apply: true,
            json: true,
        },
        [Some("verify-release"), Some(version), Some(sha), _] => Task::Verify {
            version: (*version).to_owned(),
            sha: (*sha).to_owned(),
            directory: PathBuf::from(&args[3]),
        },
        _ => return Err(format!("Invalid command or arguments.\n\n{HELP}")),
    };
    Ok(task)
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask under repository")
        .to_owned()
}

fn main() -> ExitCode {
    match parse(std::env::args_os().skip(1).collect()).and_then(|task| execute(task, &root())) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn execute(task: Task, root: &Path) -> Result<(), String> {
    if task == Task::Help {
        println!("{HELP}");
        return Ok(());
    }
    if let Task::Check { tools } = task {
        // Streaming to the terminal avoids buffering test output and preserves Ctrl+C.
        // Cargo reuses its native incremental cache. This is not release evidence.
        run_checks(tools, |args| {
            eprintln!("\n> cargo {}", args.join(" "));
            let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
                .args(args)
                .current_dir(root)
                .status()
                .map_err(|e| format!("start Cargo: {e}"))?;
            if status.success() {
                Ok(())
            } else {
                Err(format!("cargo {} failed: {status}", args.join(" ")))
            }
        })?;
        if tools {
            validate(root)?;
        }
        return Ok(());
    }
    let plan = plan::load(&root.join("releases/plan.json"))?;
    match task {
        Task::Validate => validate(root)?,
        Task::Roadmap { write } => {
            let roadmap = plan::render(&plan)?;
            if write {
                fs::write(root.join("ROADMAP.md"), roadmap).map_err(|e| e.to_string())?;
                println!("ROADMAP.md regenerated from releases/plan.json.");
            } else {
                print!("{roadmap}");
            }
        }
        Task::Sync {
            apply,
            json: detailed,
        } => {
            let report = sync::run(&plan, apply)?;
            if detailed {
                print_json(report)?;
            } else {
                print_json(
                    json!({"apply": report["apply"], "total_changes": report["total_changes"],
                    "counts": report["counts"], "details": "Use sync --json to inspect the planned bodies before --apply."}),
                )?;
            }
        }
        Task::Verify {
            version,
            sha,
            directory,
        } => {
            plan::release_for_version(&plan, &version)?;
            print_json(artifacts::verify(&plan, &version, &sha, &directory)?)?;
        }
        Task::Help | Task::Check { .. } => unreachable!("handled above"),
    }
    Ok(())
}

fn print_json(value: Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
    );
    Ok(())
}

fn validate(root: &Path) -> Result<(), String> {
    let plan = plan::load(&root.join("releases/plan.json"))?;
    let gates: Value = serde_json::from_slice(
        &fs::read(root.join("releases/gates.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("releases/gates.json: {e}"))?;
    validate_gates(&plan, &gates)?;
    let roadmap = fs::read_to_string(root.join("ROADMAP.md")).map_err(|e| e.to_string())?;
    if roadmap.replace("\r\n", "\n") != plan::render(&plan)? {
        return Err("ROADMAP.md is outdated; run cargo xtask roadmap --write".into());
    }
    let releases = plan["releases"].as_array().ok_or("missing releases")?;
    print_json(
        json!({"valid": true, "milestones": releases.len(), "tasks": releases.iter()
        .map(|release| release["tasks"].as_array().map_or(0, Vec::len)).sum::<usize>()}),
    )
}

fn validate_gates(plan: &Value, value: &Value) -> Result<(), String> {
    if value["schema_version"] != json!(1) {
        return Err("gates.schema_version must be 1".into());
    }
    let gates = value["gates"]
        .as_object()
        .ok_or("gates must be an object")?;
    let required: BTreeSet<_> = plan["releases"]
        .as_array()
        .ok_or("missing releases")?
        .iter()
        .flat_map(|release| release["required_gates"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .filter(|name| !matches!(*name, "native" | "tcp_smoke"))
        .collect();
    if gates.keys().map(String::as_str).collect::<BTreeSet<_>>() != required {
        return Err("gates does not match the external gates required by the plan".into());
    }
    for (name, gate) in gates {
        let timeout = gate["timeout_seconds"]
            .as_u64()
            .filter(|n| *n > 0 && *n <= 86_400)
            .ok_or_else(|| format!("{name}: invalid timeout"))?;
        let command = gate
            .get("command")
            .ok_or_else(|| format!("{name}: missing command"))?;
        if !command.is_null()
            && !command.as_array().is_some_and(|args| {
                !args.is_empty()
                    && args.iter().all(|arg| {
                        arg.as_str()
                            .is_some_and(|s| !s.is_empty() && !s.contains('\0'))
                    })
            })
        {
            return Err(format!("{name}: command must be null or a nonempty argv"));
        }
        let required_minimum = match name.as_str() {
            "soak" => plan["release_policy"]["stable_soak_seconds"].as_u64(),
            _ => None,
        };
        if let Some(minimum) = required_minimum
            && !gate["minimum_seconds"]
                .as_u64()
                .is_some_and(|n| n >= minimum && n < timeout)
        {
            return Err(format!(
                "{name}: minimum duration missing, too short, or incompatible with timeout"
            ));
        }
    }
    Ok(())
}

fn run_checks(
    tools: bool,
    mut run: impl FnMut(&[&str]) -> Result<(), String>,
) -> Result<(), String> {
    if tools {
        run(&[
            "fmt",
            "--manifest-path",
            "xtask/Cargo.toml",
            "--all",
            "--check",
        ])?;
        run(&[
            "clippy",
            "--manifest-path",
            "xtask/Cargo.toml",
            "--locked",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ])?;
        run(&["test", "--manifest-path", "xtask/Cargo.toml", "--locked"])?;
    } else {
        run(&["fmt", "--all", "--check"])?;
        // Clippy already checks all targets. No second cargo check is needed here.
        run(&[
            "clippy",
            "--locked",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ])?;
        // The package fixture runs a real binary; don't reuse a stale pre-release executable.
        run(&["build", "--locked", "--bin", "sider"])?;
        run(&["test", "--locked"])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn mutations_are_never_implicit_and_unknown_arguments_fail() {
        assert_eq!(
            parse(args(&["sync"])).unwrap(),
            Task::Sync {
                apply: false,
                json: false
            }
        );
        assert_eq!(
            parse(args(&["roadmap"])).unwrap(),
            Task::Roadmap { write: false }
        );
        assert_eq!(
            parse(args(&["sync", "--apply"])).unwrap(),
            Task::Sync {
                apply: true,
                json: false
            }
        );
        assert_eq!(
            parse(args(&["sync", "--json"])).unwrap(),
            Task::Sync {
                apply: false,
                json: true
            }
        );
        assert_eq!(
            parse(args(&["sync", "--json", "--apply"])).unwrap(),
            Task::Sync {
                apply: true,
                json: true
            }
        );
        for argv in [
            vec!["publish", "--apply"],
            vec!["sync", "--aply"],
            vec!["check", "--apply"],
            vec!["validate", "--write"],
            vec!["verify-release", "0.1.0"],
            vec!["sync", "--apply", "--apply"],
        ] {
            assert!(parse(args(&argv)).is_err(), "{argv:?}");
        }
    }

    #[test]
    fn checks_stop_on_first_failure_and_do_not_run_external_gates() {
        let mut calls = Vec::new();
        assert!(
            run_checks(false, |argv| {
                calls.push(argv.join(" "));
                if calls.len() == 2 {
                    Err("expected failure".into())
                } else {
                    Ok(())
                }
            })
            .is_err()
        );
        assert_eq!(calls.len(), 2);
        for tools in [false, true] {
            calls.clear();
            run_checks(tools, |argv| {
                calls.push(argv.join(" "));
                Ok(())
            })
            .unwrap();
            assert_eq!(calls.len(), if tools { 3 } else { 4 });
            assert!(calls.iter().all(|cmd| !cmd.contains("--ignored")
                && !cmd.contains("docker")
                && !cmd.starts_with("check")));
            assert_eq!(
                calls.iter().any(|cmd| cmd.contains("xtask/Cargo.toml")),
                tools
            );
            if !tools {
                assert_eq!(
                    calls,
                    [
                        "fmt --all --check",
                        "clippy --locked --all-targets -- -D warnings",
                        "build --locked --bin sider",
                        "test --locked",
                    ]
                );
            }
        }
    }

    #[test]
    fn real_gates_and_pending_commands_are_structurally_valid() {
        let plan = plan::load(&root().join("releases/plan.json")).unwrap();
        let gates: Value = serde_json::from_str(include_str!("../../releases/gates.json")).unwrap();
        validate_gates(&plan, &gates).unwrap();
        for broken in [json!([]), json!("cargo test"), json!([""]), json!([3])] {
            let mut bad = gates.clone();
            bad["gates"]["compatibility"]["command"] = broken;
            assert!(validate_gates(&plan, &bad).is_err());
        }
        let mut bad = gates.clone();
        bad["gates"]["soak"]["minimum_seconds"] = json!(1);
        assert!(validate_gates(&plan, &bad).is_err());
        let mut bad = gates.clone();
        bad["gates"]["soak"]["timeout_seconds"] = json!(true);
        assert!(validate_gates(&plan, &bad).is_err());
        let mut bad = gates;
        bad["gates"].as_object_mut().unwrap().remove("crash");
        assert!(validate_gates(&plan, &bad).is_err());
    }
}
