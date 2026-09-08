//! Gate opt-in: libFuzzer real, medido fora do build e com recibo no SHA validado.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

#[path = "common/gate_receipt.rs"]
mod gate_receipt;
#[path = "../fuzz/fuzz_targets/resp_decoder.rs"]
mod harness;
#[path = "common/process.rs"]
mod process;

const NIGHTLY: &str = "nightly-2026-09-07";
const CARGO_FUZZ_VERSION: &str = "cargo-fuzz 0.13.2";
const TARGET: &str = "x86_64-unknown-linux-gnu";
const SEED: u32 = 0x5349_4445;
const MINIMUM_SECONDS: u64 = 900;
const FUZZ_SECONDS: u64 = 901;
const GATE_TIMEOUT: Duration = Duration::from_secs(1200);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(240);
const SEEDS: &str = include_str!("../fuzz/corpus-seeds.json");

#[test]
#[ignore = "exige Linux, nightly fixada, cargo-fuzz e ao menos 900 segundos reais"]
fn release_fuzz_gate() {
    release_fuzz_gate_impl().unwrap_or_else(|error| panic!("gate fuzz recusado: {error}"));
}

fn release_fuzz_gate_impl() -> Result<(), String> {
    let gate_started = Instant::now();
    let context = gate_receipt::GateContext::from_env("fuzz")?;
    if !cfg!(all(target_os = "linux", target_arch = "x86_64")) || context.target() != TARGET {
        return Err("o gate fuzz requer execução nativa Linux x86_64".into());
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let run_dir = create_run_dir(context.release_dir())?;
    let corpus = run_dir.join("corpus");
    let artifacts = run_dir.join("artifacts");
    let seed_count = write_seed_corpus(&corpus)?;
    fs::create_dir(&artifacts).map_err(|error| error.to_string())?;
    let build_dir = build_dir(root)?;
    let root_lock = fs::read(root.join("Cargo.lock")).map_err(|error| error.to_string())?;
    let fuzz_lock = fs::read(root.join("fuzz/Cargo.lock")).map_err(|error| error.to_string())?;

    let mut compiler = Command::new("rustc");
    compiler.args([&format!("+{NIGHTLY}"), "--version", "--verbose"]);
    let compiler = command_text(&mut compiler, gate_started, &run_dir, "nightly")?;
    if !compiler.contains("-nightly") || !compiler.contains(&format!("host: {TARGET}")) {
        return Err("compilador de fuzz não corresponde à nightly e plataforma esperadas".into());
    }
    let mut tool = Command::new("cargo");
    tool.args([&format!("+{NIGHTLY}"), "fuzz", "--version"]);
    let tool = command_text(&mut tool, gate_started, &run_dir, "cargo-fuzz")?;
    if tool.trim() != CARGO_FUZZ_VERSION {
        return Err(format!(
            "cargo-fuzz divergente: esperado {CARGO_FUZZ_VERSION}, recebido {tool:?}"
        ));
    }
    let mut clang = Command::new("clang++");
    clang.arg("--version");
    let clang = command_text(&mut clang, gate_started, &run_dir, "clang")?;

    // cargo-fuzz 0.13.2 não expõe --locked. Metadata valida a resolução congelada,
    // e os dois lockfiles são conferidos byte a byte depois do build instrumentado.
    let mut metadata = cargo(root);
    metadata.args([
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--filter-platform",
        TARGET,
        "--manifest-path",
        "fuzz/Cargo.toml",
    ]);
    command_text(&mut metadata, gate_started, &run_dir, "metadata")?;
    let mut build = cargo(root);
    build
        .args([
            "fuzz",
            "build",
            "resp_decoder",
            "--target",
            TARGET,
            "--sanitizer",
            "address",
            "--debug-assertions",
            "--target-dir",
        ])
        .arg(&build_dir)
        .env("CC", "clang")
        .env("CXX", "clang++");
    command_text(&mut build, gate_started, &run_dir, "build")?;
    if fs::read(root.join("Cargo.lock")).map_err(|error| error.to_string())? != root_lock
        || fs::read(root.join("fuzz/Cargo.lock")).map_err(|error| error.to_string())? != fuzz_lock
    {
        return Err("o build de fuzz modificou um lockfile; nenhum recibo será publicado".into());
    }
    let binary = build_dir.join(TARGET).join("release/resp_decoder");
    if !binary.is_file() {
        return Err(format!(
            "alvo instrumentado não foi produzido: {}",
            binary.display()
        ));
    }
    let args = [
        format!("-max_total_time={FUZZ_SECONDS}"),
        format!("-seed={SEED}"),
        "-print_final_stats=1".into(),
        "-max_len=4096".into(),
        "-timeout=5".into(),
        "-rss_limit_mb=2048".into(),
        "-jobs=0".into(),
        "-workers=1".into(),
        format!("-artifact_prefix={}/", artifacts.display()),
    ];
    println!(
        "libFuzzer: início de {FUZZ_SECONDS} s, seed {SEED}, {seed_count} seeds; logs em {}",
        run_dir.display()
    );
    let mut run = Command::new(&binary);
    run.current_dir(root)
        .arg(&corpus)
        .args(&args)
        .env_remove("ASAN_OPTIONS")
        .env_remove("LSAN_OPTIONS")
        .env_remove("UBSAN_OPTIONS");
    // Não há cargo intermediário aqui: o filho supervisionado é o próprio
    // libFuzzer, incluindo encerramento por timeout, falha ou saída excessiva.
    let remaining = remaining(gate_started)?;
    if remaining < Duration::from_secs(FUZZ_SECONDS + 10) {
        return Err(
            "build consumiu o orçamento do gate; pré-compile antes de tentar novamente".into(),
        );
    }
    let started = Instant::now();
    let output = capture_command(&mut run, remaining, &run_dir, "libfuzzer")?;
    let duration = started.elapsed();
    save_output(&run_dir, "libfuzzer", &output)?;
    if !output.status.success() {
        return Err(format!(
            "libFuzzer falhou: {}; consulte {}",
            output.status,
            run_dir.display()
        ));
    }
    let stderr = std::str::from_utf8(&output.stderr).map_err(|error| error.to_string())?;
    let stdout = std::str::from_utf8(&output.stdout).map_err(|error| error.to_string())?;
    reject_failure_markers(stdout)?;
    let stats = parse_stats(stderr, duration)?;
    context.publish(
        stats.executions,
        duration,
        json!({
            "engine": "libFuzzer",
            "harness": "resp_decoder",
            "sanitizer": "address",
            "nightly": NIGHTLY,
            "compiler": compiler.trim(),
            "cargo_fuzz": tool.trim(),
            "libfuzzer_sys": "0.4.13",
            "cxx": clang.trim(),
            "seed": SEED,
            "seed_files": seed_count,
            "executions": stats.executions,
            "reported_seconds": stats.seconds,
            "duration_seconds": duration.as_secs_f64(),
            "arguments": args,
            "run_directory": run_dir,
            "stdout": "libfuzzer.stdout.log",
            "stderr": "libfuzzer.stderr.log"
        }),
    )?;
    println!(
        "libFuzzer aprovado: {} execuções em {:.3} s",
        stats.executions,
        duration.as_secs_f64()
    );
    Ok(())
}

fn cargo(root: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(root)
        .arg(format!("+{NIGHTLY}"))
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTC_BOOTSTRAP");
    command
}

fn build_dir(root: &Path) -> Result<PathBuf, String> {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let path = target.join("sider-fuzz");
    fs::create_dir_all(&path).map_err(|error| error.to_string())?;
    path.canonicalize().map_err(|error| error.to_string())
}

fn remaining(started: Instant) -> Result<Duration, String> {
    GATE_TIMEOUT
        .checked_sub(started.elapsed())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| "gate fuzz excedeu seu prazo global".into())
}

fn command_text(
    command: &mut Command,
    started: Instant,
    directory: &Path,
    name: &str,
) -> Result<String, String> {
    let output = capture_command(
        command,
        remaining(started)?.min(COMMAND_TIMEOUT),
        directory,
        name,
    )?;
    save_output(directory, name, &output)?;
    if !output.status.success() {
        return Err(format!(
            "{name} falhou: {}; consulte {}",
            output.status,
            directory.display()
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| error.to_string())
}

fn create_run_dir(parent: &Path) -> Result<PathBuf, String> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let path = parent.join(format!("fuzz-run-{}-{nonce}", std::process::id()));
    fs::create_dir(&path).map_err(|error| error.to_string())?;
    Ok(path)
}

fn save_output(directory: &Path, name: &str, output: &Output) -> Result<(), String> {
    for (stream, bytes) in [("stdout", &output.stdout), ("stderr", &output.stderr)] {
        let path = directory.join(format!("{name}.{stream}.log"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        file.write_all(bytes).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn capture_command(
    command: &mut Command,
    timeout: Duration,
    directory: &Path,
    name: &str,
) -> Result<Output, String> {
    match process::run(command, timeout) {
        Ok(output) => Ok(output),
        Err(error) => {
            let diagnostic = format!(
                "Captura de {name} falhou: {error}\n\
                 stdout/stderr completos indisponíveis. O supervisor encerra o filho direto; \
                 corpus e artefatos já gravados são preservados. Nenhum recibo foi publicado.\n"
            );
            let path = directory.join(format!("{name}.capture-failure.log"));
            let saved = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .and_then(|mut file| file.write_all(diagnostic.as_bytes()));
            match saved {
                Ok(()) => Err(format!("{error}; diagnóstico em {}", path.display())),
                Err(logging) => Err(format!(
                    "{error}; também falhou o registro do diagnóstico: {logging}"
                )),
            }
        }
    }
}

fn seeds(source: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
    let source: Value = serde_json::from_str(source).map_err(|error| error.to_string())?;
    let mut names = BTreeSet::new();
    let mut decoded = Vec::new();
    for seed in source.as_array().ok_or("seeds precisam formar um array")? {
        let name = seed
            .get("name")
            .and_then(Value::as_str)
            .ok_or("seed sem nome")?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || !names.insert(name.to_owned())
        {
            return Err("nome de seed vazio, duplicado ou inseguro".into());
        }
        let hex = seed
            .get("hex")
            .and_then(Value::as_str)
            .ok_or("seed sem hex")?;
        if !hex.len().is_multiple_of(2) || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("seed não contém pares hexadecimais".into());
        }
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        for index in (0..hex.len()).step_by(2) {
            bytes.push(
                u8::from_str_radix(&hex[index..index + 2], 16)
                    .map_err(|error| error.to_string())?,
            );
        }
        decoded.push((name.to_owned(), bytes));
    }
    if decoded.is_empty() {
        return Err("corpus de seeds vazio".into());
    }
    Ok(decoded)
}

fn write_seed_corpus(directory: &Path) -> Result<usize, String> {
    fs::create_dir_all(directory).map_err(|error| error.to_string())?;
    let seeds = seeds(SEEDS)?;
    for (name, bytes) in &seeds {
        let path = directory.join(name);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => file.write_all(bytes).map_err(|error| error.to_string())?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(&path).map_err(|error| error.to_string())? != *bytes {
                    return Err(format!("seed existente divergente: {}", path.display()));
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(seeds.len())
}

#[derive(Debug, PartialEq, Eq)]
struct Stats {
    executions: u64,
    seconds: u64,
}

fn reject_failure_markers(text: &str) -> Result<(), String> {
    if [
        "ERROR: AddressSanitizer",
        "SUMMARY: AddressSanitizer",
        "ERROR: LeakSanitizer",
        "SUMMARY: LeakSanitizer",
        "ERROR: libFuzzer",
        "runtime error:",
        "UndefinedBehaviorSanitizer",
        "Sanitizer:DEADLYSIGNAL",
        "panicked at",
    ]
    .iter()
    .any(|marker| text.contains(marker))
    {
        return Err(
            "log de fuzz contém falha do harness, timeout ou diagnóstico de sanitizer".into(),
        );
    }
    Ok(())
}

fn parse_stats(text: &str, duration: Duration) -> Result<Stats, String> {
    reject_failure_markers(text)?;
    if duration < Duration::from_secs(MINIMUM_SECONDS) {
        return Err("libFuzzer não executou por 900 segundos reais".into());
    }
    let mut executions = None;
    let mut done = None;
    let mut seed = None;
    let mut coverage = false;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("stat::number_of_executed_units:")
            && executions
                .replace(
                    value
                        .trim()
                        .parse::<u64>()
                        .map_err(|error| error.to_string())?,
                )
                .is_some()
        {
            return Err("estatística de execuções duplicada".into());
        }
        if let Some(value) = line.strip_prefix("INFO: Seed:")
            && seed
                .replace(
                    value
                        .trim()
                        .parse::<u32>()
                        .map_err(|error| error.to_string())?,
                )
                .is_some()
        {
            return Err("seed de execução duplicado".into());
        }
        if let Some(value) = line.strip_prefix("Done ") {
            let fields = value.split_whitespace().collect::<Vec<_>>();
            if fields.len() != 5
                || fields[1] != "runs"
                || fields[2] != "in"
                || fields[4] != "second(s)"
            {
                return Err("resumo final do libFuzzer inválido".into());
            }
            let counts = (
                fields[0]
                    .parse::<u64>()
                    .map_err(|error| error.to_string())?,
                fields[3]
                    .parse::<u64>()
                    .map_err(|error| error.to_string())?,
            );
            if done.replace(counts).is_some() {
                return Err("resumo final duplicado".into());
            }
        }
        if line.starts_with('#') && line.split_whitespace().any(|word| word == "DONE") {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            coverage |= fields.windows(2).any(|pair| {
                pair[0] == "cov:" && pair[1].parse::<u64>().is_ok_and(|value| value > 0)
            });
        }
    }
    let executions = executions
        .filter(|value| *value > 0)
        .ok_or("estatística de execuções ausente ou zero")?;
    let (done_runs, seconds) = done.ok_or("resumo final ausente")?;
    if done_runs != executions || seconds < MINIMUM_SECONDS || seed != Some(SEED) || !coverage {
        return Err(
            "estatísticas, duração, seed ou cobertura não comprovam o contrato de fuzz".into(),
        );
    }
    Ok(Stats {
        executions,
        seconds,
    })
}

#[test]
#[ignore = "prepara corpus binário local; não executa nem aprova o gate de fuzz"]
fn prepare_fuzz_corpus() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let directory = build_dir(root).unwrap().join("corpus");
    let count = write_seed_corpus(&directory).unwrap();
    println!("{count} seeds preparados em {}", directory.display());
}

#[test]
fn native_seed_regressions_exercise_the_exact_fuzz_harness() {
    for (name, seed) in seeds(SEEDS).unwrap() {
        harness::exercise(&seed);
        assert!(!name.is_empty());
    }
    harness::exercise(&[]);
    harness::exercise(&[0xff; 4096]);
    harness::exercise(&b"+\r\n".repeat(100));
}

#[test]
fn seed_manifest_rejects_ambiguous_or_unsafe_inputs() {
    for source in [
        "[]",
        "{}",
        r#"[{"name":"../bad","hex":"00"}]"#,
        r#"[{"name":"ok","hex":"0"}]"#,
        r#"[{"name":"ok","hex":"xx"}]"#,
        r#"[{"name":"ok","hex":"00"},{"name":"ok","hex":"01"}]"#,
    ] {
        assert!(seeds(source).is_err(), "{source}");
    }
}

fn successful_stats() -> String {
    format!(
        "INFO: Seed: {SEED}\n#1234 DONE cov: 100 ft: 200\nDone 1234 runs in 901 second(s)\nstat::number_of_executed_units: 1234\n"
    )
}

#[test]
fn parses_real_final_stats_and_requires_measured_runtime() {
    assert_eq!(
        parse_stats(&successful_stats(), Duration::from_secs(901)).unwrap(),
        Stats {
            executions: 1234,
            seconds: 901
        }
    );
    assert!(parse_stats(&successful_stats(), Duration::from_millis(899_999)).is_err());
}

#[test]
fn rejects_missing_conflicting_or_failed_fuzz_evidence() {
    let valid = successful_stats();
    for text in [
        String::new(),
        valid.replace("1234", "0"),
        valid.replace("901 second(s)", "899 second(s)"),
        valid.replace(&SEED.to_string(), "1"),
        valid.replace("cov: 100", "cov: 0"),
        valid.replace(
            "stat::number_of_executed_units: 1234",
            "stat::number_of_executed_units: 1235",
        ),
        format!("{valid}stat::number_of_executed_units: 1234\n"),
        format!("{valid}ERROR: libFuzzer: timeout after 5 seconds\n"),
        format!("{valid}ERROR: AddressSanitizer: heap-buffer-overflow\n"),
        format!("{valid}thread panicked at assertion\n"),
    ] {
        assert!(
            parse_stats(&text, Duration::from_secs(901)).is_err(),
            "{text}"
        );
    }
}
