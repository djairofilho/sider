//! Smoke do executável realmente selecionado no diretório de pacote extraído.
//!
//! A extração e os checksums do arquivo compactado pertencem ao empacotamento;
//! este teste não infere essa origem a partir do nome de um diretório.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

#[path = "common/process.rs"]
mod process;
#[path = "common/sider_process.rs"]
mod sider_process;

use sider_process::SiderProcess;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const BINARY_NAME: &str = if cfg!(windows) { "sider.exe" } else { "sider" };
const MIGRATOR_NAME: &str = if cfg!(windows) {
    "sider-aof-migrate.exe"
} else {
    "sider-aof-migrate"
};
const BACKUP_NAME: &str = if cfg!(windows) {
    "sider-backup.exe"
} else {
    "sider-backup"
};
const PACKAGE_FILES: &[&str] = &[
    BINARY_NAME,
    MIGRATOR_NAME,
    BACKUP_NAME,
    "README.md",
    "LICENSE",
];
const DISTRIBUTION_README: &[u8] = include_bytes!("../releases/README.md");
const DISTRIBUTION_LICENSE: &[u8] = include_bytes!("../LICENSE");
const PIPELINE_REQUEST: &[u8] = b"*3\r\n$3\r\nSET\r\n$4\r\n\x00\xff\r\n\r\n$5\r\n\x00\r\n\xffA\r\n\
      *2\r\n$3\r\nGET\r\n$4\r\n\x00\xff\r\n\r\n\
      *3\r\n$3\r\nDEL\r\n$4\r\n\x00\xff\r\n\r\n$4\r\n\x00\xff\r\n\r\n\
      *2\r\n$3\r\nGET\r\n$4\r\n\x00\xff\r\n\r\n\
      *1\r\n$4\r\nPING\r\n";
const PIPELINE_RESPONSE: &[u8] = b"+OK\r\n$5\r\n\x00\r\n\xffA\r\n:1\r\n$-1\r\n+PONG\r\n";

struct ExtractedPackage {
    directory: PathBuf,
    binary: PathBuf,
    binary_bytes: u64,
    migrator: PathBuf,
    migrator_bytes: u64,
    backup: PathBuf,
    backup_bytes: u64,
}

fn select_package(
    value: Option<OsString>,
    readme: &[u8],
    license: &[u8],
) -> Result<ExtractedPackage, String> {
    let directory = PathBuf::from(value.ok_or("SIDER_PACKAGE_DIR ausente")?);
    if !directory.is_absolute() {
        return Err("SIDER_PACKAGE_DIR precisa ser um caminho absoluto não vazio".into());
    }
    let metadata = fs::symlink_metadata(&directory)
        .map_err(|e| format!("inspecionar SIDER_PACKAGE_DIR: {e}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("SIDER_PACKAGE_DIR precisa ser diretório real, não symlink".into());
    }
    let directory = directory.canonicalize().map_err(|e| e.to_string())?;
    let binary = directory.join(BINARY_NAME);
    let binary_bytes = regular_file(&binary)?.len();
    if binary_bytes == 0 {
        return Err("executável do pacote está vazio".into());
    }
    let migrator = directory.join(MIGRATOR_NAME);
    let migrator_bytes = regular_file(&migrator)?.len();
    if migrator_bytes == 0 {
        return Err("migrador do pacote está vazio".into());
    }
    let backup = directory.join(BACKUP_NAME);
    let backup_bytes = regular_file(&backup)?.len();
    if backup_bytes == 0 {
        return Err("CLI de backup do pacote está vazia".into());
    }
    for (name, expected) in [("README.md", readme), ("LICENSE", license)] {
        if expected.is_empty() {
            return Err(format!("{name} do checkout está vazio"));
        }
        verify_contents(&directory.join(name), expected)?;
    }
    Ok(ExtractedPackage {
        directory,
        binary,
        binary_bytes,
        migrator,
        migrator_bytes,
        backup,
        backup_bytes,
    })
}

fn regular_file(path: &Path) -> Result<fs::Metadata, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|e| format!("arquivo obrigatório {}: {e}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "{} precisa ser arquivo regular, não symlink",
            path.display()
        ));
    }
    Ok(metadata)
}

fn verify_contents(path: &Path, expected: &[u8]) -> Result<(), String> {
    let size = u64::try_from(expected.len()).map_err(|_| "documento esperado grande demais")?;
    if regular_file(path)?.len() != size {
        return Err(format!(
            "conteúdo integral de {} diverge do checkout",
            path.display()
        ));
    }
    let mut actual = Vec::new();
    File::open(path)
        .map_err(|e| e.to_string())?
        .take(size.checked_add(1).ok_or("limite de documento inválido")?)
        .read_to_end(&mut actual)
        .map_err(|e| e.to_string())?;
    if actual != expected {
        return Err(format!(
            "conteúdo integral de {} diverge do checkout",
            path.display()
        ));
    }
    Ok(())
}

fn native_target() -> Result<&'static str, String> {
    if cfg!(all(
        target_os = "windows",
        target_arch = "x86_64",
        target_env = "msvc"
    )) {
        Ok("x86_64-pc-windows-msvc")
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        Ok("x86_64-unknown-linux-gnu")
    } else {
        Err("smoke de pacote exige execução nativa Windows MSVC ou Linux GNU x86_64".into())
    }
}

fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| "pipeline do pacote excedeu prazo total de I/O".into())
}

fn pipeline(address: SocketAddr) -> Result<(), String> {
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut stream = TcpStream::connect_timeout(&address, remaining(deadline)?)
        .map_err(|e| format!("conectar ao executável extraído: {e}"))?;
    let mut request = PIPELINE_REQUEST;
    while !request.is_empty() {
        stream
            .set_write_timeout(Some(remaining(deadline)?))
            .map_err(|e| e.to_string())?;
        match stream.write(request) {
            Ok(0) => return Err("escrita do pipeline foi interrompida".into()),
            Ok(count) => request = &request[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("escrever pipeline: {error}")),
        }
    }
    stream
        .shutdown(Shutdown::Write)
        .map_err(|e| format!("half-close: {e}"))?;
    let mut actual = vec![0; PIPELINE_RESPONSE.len()];
    let mut offset = 0;
    while offset < actual.len() {
        stream
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|e| e.to_string())?;
        match stream.read(&mut actual[offset..]) {
            Ok(0) => return Err("resposta do pipeline truncada".into()),
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("ler pipeline: {error}")),
        }
    }
    if actual != PIPELINE_RESPONSE {
        return Err(format!("bytes do pipeline divergentes: {actual:?}"));
    }
    loop {
        stream
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|e| e.to_string())?;
        let mut trailing = [0; 1];
        match stream.read(&mut trailing) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err("resposta excedente depois do pipeline".into()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("EOF limpo após half-close: {error}")),
        }
    }
}

fn smoke(
    package: &ExtractedPackage,
    readme: &[u8],
    license: &[u8],
    version: &str,
) -> Result<Value, String> {
    let target = native_target()?;
    let migrator = process::run(
        std::process::Command::new(&package.migrator).arg("--help"),
        IO_TIMEOUT,
    )?;
    if !migrator.status.success()
        || !migrator
            .stdout
            .starts_with(b"Uso: sider-aof-migrate --source DIR")
    {
        return Err("migrador extraído não executou sua CLI".into());
    }
    let backup = process::run(
        std::process::Command::new(&package.backup).arg("--version"),
        IO_TIMEOUT,
    )?;
    if !backup.status.success() || backup.stdout != format!("sider-backup {version}\n").as_bytes() {
        return Err("versão da CLI de backup extraída diverge".into());
    }
    // Esta é a única origem do executável: não há fallback para um target local.
    let mut sider = SiderProcess::try_start(&package.binary, version)?;
    pipeline(sider.address())?;
    sider.assert_alive();
    sider.finish();
    // Mudanças nos documentos durante o teste também invalidam o resultado.
    let verified = select_package(
        Some(package.directory.clone().into_os_string()),
        readme,
        license,
    )?;
    if verified.binary_bytes != package.binary_bytes
        || verified.migrator_bytes != package.migrator_bytes
        || verified.backup_bytes != package.backup_bytes
    {
        return Err("tamanho do executável mudou durante o smoke".into());
    }
    Ok(json!({
        "schema_version": 1,
        "check": "extracted_package_runs_version_and_tcp",
        "status": "success",
        "version": version,
        "target": target,
        "package_directory": package.directory.to_string_lossy(),
        "binary": BINARY_NAME,
        "binary_bytes": package.binary_bytes,
        "migrator": MIGRATOR_NAME,
        "migrator_bytes": package.migrator_bytes,
        "migrator_help_checked": true,
        "backup": BACKUP_NAME,
        "backup_bytes": package.backup_bytes,
        "backup_version_checked": true,
        "readme_matches_checkout": true,
        "readme_source": "releases/README.md",
        "license_matches_checkout": true,
        "license_source": "LICENSE",
        "version_checked": true,
        "readiness_pid_and_loopback_checked": true,
        "readiness_ping_checked": true,
        "pipeline_commands": 5,
        "binary_key_and_value_checked": true,
        "ordered_responses_checked": true,
        "half_close_eof_checked": true,
        "cleanup_confirmed": true
    }))
}

#[test]
#[ignore = "exige SIDER_PACKAGE_DIR absoluto apontando para um pacote realmente extraído"]
fn extracted_package_runs_version_and_tcp() {
    let package = select_package(
        std::env::var_os("SIDER_PACKAGE_DIR"),
        DISTRIBUTION_README,
        DISTRIBUTION_LICENSE,
    )
    .expect("pacote extraído completo e explicitamente selecionado");
    let result = smoke(
        &package,
        DISTRIBUTION_README,
        DISTRIBUTION_LICENSE,
        env!("CARGO_PKG_VERSION"),
    )
    .expect("executável extraído precisa passar pelo smoke real");
    // Somente este caminho opt-in emite resultado para o manifesto manual.
    // Não cria recibo de gate, não publica release e não infere o SHA do build.
    println!("{result}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            for _ in 0..100 {
                let path = std::env::temp_dir().join(format!(
                    "sider-package-test-{}-{}",
                    std::process::id(),
                    NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
                ));
                match fs::create_dir(&path) {
                    Ok(()) => {
                        let fixture = Self(path);
                        fs::create_dir(fixture.checkout()).unwrap();
                        fs::create_dir(fixture.package()).unwrap();
                        for name in ["README.md", "LICENSE"] {
                            let contents = format!("Documento completo de programação: {name}.\n");
                            fs::write(fixture.checkout().join(name), &contents).unwrap();
                            fs::write(fixture.package().join(name), contents).unwrap();
                        }
                        for name in [BINARY_NAME, MIGRATOR_NAME, BACKUP_NAME] {
                            fs::write(fixture.package().join(name), b"fixture, not executable")
                                .unwrap();
                        }
                        return fixture;
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => panic!("create owned package fixture: {error}"),
                }
            }
            panic!("could not create exclusive package fixture");
        }

        fn checkout(&self) -> PathBuf {
            self.0.join("checkout")
        }
        fn package(&self) -> PathBuf {
            self.0.join("package")
        }
        fn select(&self) -> Result<ExtractedPackage, String> {
            let readme = fs::read(self.checkout().join("README.md")).map_err(|e| e.to_string())?;
            let license = fs::read(self.checkout().join("LICENSE")).map_err(|e| e.to_string())?;
            select_package(Some(self.package().into_os_string()), &readme, &license)
        }

        fn install_real_binary(&self) {
            // Simulação explícita do teste nativo, não evidência de extração.
            fs::copy(
                env!("CARGO_BIN_EXE_sider"),
                self.package().join(BINARY_NAME),
            )
            .unwrap();
            fs::copy(
                env!("CARGO_BIN_EXE_sider-aof-migrate"),
                self.package().join(MIGRATOR_NAME),
            )
            .unwrap();
            fs::copy(
                env!("CARGO_BIN_EXE_sider-backup"),
                self.package().join(BACKUP_NAME),
            )
            .unwrap();
            for (name, contents) in [
                ("README.md", DISTRIBUTION_README),
                ("LICENSE", DISTRIBUTION_LICENSE),
            ] {
                fs::write(self.checkout().join(name), contents).unwrap();
                fs::copy(self.checkout().join(name), self.package().join(name)).unwrap();
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Apenas caminhos conhecidos sob o diretório criado pelo fixture.
            for directory in [self.package(), self.checkout()] {
                for name in PACKAGE_FILES {
                    let path = directory.join(name);
                    let _ = fs::remove_file(&path);
                    let _ = fs::remove_dir(&path);
                }
                let _ = fs::remove_dir(directory);
            }
            let _ = fs::remove_file(self.0.join("not-a-directory"));
            let _ = fs::remove_file(self.0.join("original-binary"));
            let _ = fs::remove_dir(self.0.join("linked-package"));
            let _ = fs::remove_file(self.0.join("linked-package"));
            let _ = fs::remove_dir(&self.0);
        }
    }

    #[test]
    fn selection_requires_explicit_existing_absolute_directory() {
        let fixture = Fixture::new();
        for selected in [
            None,
            Some(OsString::new()),
            Some(OsString::from("package")),
            Some(fixture.0.join("missing").into_os_string()),
        ] {
            assert!(select_package(selected, DISTRIBUTION_README, DISTRIBUTION_LICENSE).is_err());
        }
        let file = fixture.0.join("not-a-directory");
        fs::write(&file, b"file").unwrap();
        assert!(
            select_package(
                Some(file.into_os_string()),
                DISTRIBUTION_README,
                DISTRIBUTION_LICENSE
            )
            .is_err()
        );
        let package = fixture.select().unwrap();
        assert_eq!(
            package.binary,
            fixture.package().canonicalize().unwrap().join(BINARY_NAME)
        );
    }

    #[test]
    fn every_required_file_must_be_present_and_regular() {
        for name in PACKAGE_FILES {
            let fixture = Fixture::new();
            let path = fixture.package().join(name);
            fs::remove_file(&path).unwrap();
            assert!(fixture.select().is_err(), "missing {name}");
            fs::create_dir(&path).unwrap();
            assert!(fixture.select().is_err(), "directory instead of {name}");
        }
    }

    #[test]
    fn documents_require_complete_identical_bytes_and_nonempty_checkout() {
        for name in ["README.md", "LICENSE"] {
            let fixture = Fixture::new();
            let expected = fs::read(fixture.checkout().join(name)).unwrap();
            let mut different = expected.clone();
            different[0] ^= 1;
            for contents in [
                Vec::new(),
                expected[..expected.len() - 1].to_vec(),
                different,
            ] {
                fs::write(fixture.package().join(name), contents).unwrap();
                assert!(fixture.select().is_err(), "nonidentical {name}");
            }
            fs::write(fixture.package().join(name), []).unwrap();
            fs::write(fixture.checkout().join(name), []).unwrap();
            assert!(fixture.select().is_err(), "empty checkout {name}");
            fs::remove_file(fixture.checkout().join(name)).unwrap();
            assert!(fixture.select().is_err(), "missing checkout {name}");
        }
    }

    #[test]
    fn empty_binary_never_falls_back_to_cargo_binary() {
        let fixture = Fixture::new();
        fs::write(fixture.package().join(BINARY_NAME), []).unwrap();
        assert!(fixture.select().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn selection_rejects_symlinked_binary_and_package_directory() {
        use std::os::unix::fs::symlink;
        let fixture = Fixture::new();
        let binary = fixture.package().join(BINARY_NAME);
        let original = fixture.0.join("original-binary");
        fs::rename(&binary, &original).unwrap();
        symlink(&original, &binary).unwrap();
        assert!(fixture.select().is_err());
        let linked = fixture.0.join("linked-package");
        symlink(fixture.package(), &linked).unwrap();
        assert!(
            select_package(
                Some(linked.into_os_string()),
                DISTRIBUTION_README,
                DISTRIBUTION_LICENSE
            )
            .is_err()
        );
    }

    #[test]
    fn native_fixture_executes_same_smoke_without_claiming_archive_extraction() {
        let fixture = Fixture::new();
        fixture.install_real_binary();
        let package = fixture.select().unwrap();
        let result = smoke(
            &package,
            DISTRIBUTION_README,
            DISTRIBUTION_LICENSE,
            env!("CARGO_PKG_VERSION"),
        )
        .unwrap();
        assert_eq!(result["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(result["pipeline_commands"], 5);
        assert_eq!(result["cleanup_confirmed"], true);
        assert!(
            package
                .binary
                .starts_with(fixture.package().canonicalize().unwrap())
        );
    }

    #[test]
    fn wrong_executable_version_produces_no_success_result() {
        let fixture = Fixture::new();
        fixture.install_real_binary();
        let package = fixture.select().unwrap();
        let error = smoke(
            &package,
            DISTRIBUTION_README,
            DISTRIBUTION_LICENSE,
            "0.0.0-not-this-version",
        )
        .unwrap_err();
        assert!(error.contains("versão"), "{error}");
    }
}
