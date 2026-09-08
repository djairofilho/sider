use std::process::Command;

fn sider() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sider"));
    command.env_remove("SIDER_ADDR");
    command
}

#[test]
fn bootstrap_reports_its_stage_and_configured_address() {
    let output = sider()
        .env("SIDER_ADDR", "127.0.0.1:6380")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("127.0.0.1:6380"));
    assert!(stdout.contains("servidor TCP ainda não implementado"));
}

#[test]
fn invalid_configuration_fails_with_a_diagnostic() {
    let output = sider().env("SIDER_ADDR", "invalid").output().unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("SIDER_ADDR")
    );
}

#[test]
fn help_does_not_require_valid_server_configuration() {
    let output = sider()
        .env("SIDER_ADDR", "invalid")
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Uso: sider")
    );
}

#[test]
fn version_matches_the_package_manifest() {
    let output = sider().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("sider {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn unknown_arguments_are_rejected() {
    let output = sider().arg("--unknown").output().unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains("uso:"));
}
