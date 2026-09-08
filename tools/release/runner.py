"""Build and test real packages; never turn missing product gates into success."""

from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import subprocess
import tarfile
import tempfile
import time
import tomllib
import zipfile
from pathlib import Path

from tools.release.policy import TARGETS, git, validate_gate_reports, version_key

MULTIPLATFORM = {"crash", "recovery", "migration"}


def run(argv, *, cwd, env=None, timeout=900):
    subprocess.run(argv, cwd=cwd, env=env, check=True, timeout=timeout)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def file_metadata(path, target):
    path = Path(path)
    return {"name": path.name, "size": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "target": target}


def tcp_smoke(binary, *, timeout=10):
    """Exercise the actual extracted executable, not a test-only server."""
    with tempfile.TemporaryDirectory(prefix="sider-ready-") as temporary, tempfile.TemporaryFile() as output:
        ready_path = Path(temporary) / "ready.json"
        env = {**os.environ, "SIDER_ADDR": "127.0.0.1:0", "SIDER_READY_FILE": str(ready_path)}
        process = subprocess.Popen([str(binary)], env=env, stdout=output, stderr=output)
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise ValueError("Binário empacotado encerrou antes de abrir TCP")
                if not ready_path.exists():
                    time.sleep(0.05)
                    continue
                ready = json.loads(ready_path.read_text(encoding="utf-8"))
                if (ready.get("pid") != process.pid or ready.get("host") != "127.0.0.1"
                        or type(ready.get("port")) is not int or not 1 <= ready["port"] <= 65535):
                    raise ValueError("Sinal de prontidão não corresponde ao processo testado")
                try:
                    connection = socket.create_connection((ready["host"], ready["port"]), timeout=0.2)
                except OSError:
                    time.sleep(0.05)
                    continue
                with connection:
                    connection.settimeout(2)
                    connection.sendall(b"*1\r\n$4\r\nPING\r\n")
                    response = b""
                    while len(response) < 7:
                        chunk = connection.recv(7 - len(response))
                        if not chunk:
                            break
                        response += chunk
                    if response != b"+PONG\r\n":
                        raise ValueError(f"Resposta TCP inválida: {response!r}")
                    if process.poll() is not None:
                        raise ValueError("Servidor encerrou durante o smoke TCP")
                    return
            raise ValueError("Timeout ao iniciar o binário empacotado")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def license_bytes(path):
    path = Path(path)
    if not path.is_file():
        raise ValueError("LICENSE ausente; o pacote exige o texto integral da licença")
    content = path.read_bytes()
    if not content.strip():
        raise ValueError("LICENSE vazio; o pacote exige o texto integral da licença")
    return content


def create_package(binary, readme, license_file, out, version, target):
    """Only three fixed archive members; preserve the complete license bytes."""
    version_key(version)
    if target not in TARGETS:
        raise ValueError("Target não suportado")
    license_content = license_bytes(license_file)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"sider-v{version}-{target}"
    executable = "sider.exe" if target == TARGETS[1] else "sider"
    members = {f"{stem}/{executable}": Path(binary).read_bytes(),
               f"{stem}/README.md": Path(readme).read_bytes(),
               f"{stem}/LICENSE": license_content}
    if target == TARGETS[1]:
        archive = out / f"{stem}.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            for name, content in sorted(members.items()):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                bundle.writestr(info, content)
    else:
        import gzip
        archive = out / f"{stem}.tar.gz"
        with archive.open("wb") as destination, gzip.GzipFile(fileobj=destination, mode="wb", mtime=0, filename="") as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as bundle:
                for name, content in sorted(members.items()):
                    info = tarfile.TarInfo(name)
                    info.size = len(content)
                    info.mode = 0o755 if name.endswith("/sider") else 0o644
                    bundle.addfile(info, io.BytesIO(content))
    return archive, f"{stem}/{executable}"


def inspect_package(archive, executable, version, *, bootstrap=False):
    with tempfile.TemporaryDirectory(prefix="sider-package-") as temp:
        if str(archive).endswith(".zip"):
            with zipfile.ZipFile(archive) as bundle:
                data = bundle.read(executable)
        else:
            with tarfile.open(archive) as bundle:
                member = bundle.getmember(executable)
                if not member.isfile():
                    raise ValueError("Executável empacotado não é arquivo regular")
                data = bundle.extractfile(member).read()
        binary = Path(temp) / Path(executable).name
        binary.write_bytes(data)
        binary.chmod(0o755)
        result = subprocess.check_output([str(binary), "--version"], timeout=10, text=True, encoding="utf-8").strip()
        if result != f"sider {version}":
            raise ValueError("Versão do binário empacotado diverge do manifesto")
        if not bootstrap:
            tcp_smoke(binary)


def build_package(root, out, version, target, *, bootstrap=False):
    root, out = Path(root).resolve(), Path(out).resolve()
    version_key(version)
    package = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))["package"]
    if package["version"] != version or package.get("publish") is not False:
        raise ValueError("Metadados do pacote não correspondem à versão privada")
    if target not in TARGETS:
        raise ValueError("Target não suportado")
    license_bytes(root / "LICENSE")
    sha = git("rev-parse", "HEAD", cwd=root)
    env = {**os.environ, "RUSTDOCFLAGS": "-D warnings"}
    checks = [
        ["cargo", "fmt", "--all", "--", "--check"],
        ["cargo", "check", "--locked", "--all-targets"],
        ["cargo", "clippy", "--locked", "--all-targets", "--", "-D", "warnings"],
        ["cargo", "test", "--locked"],
        ["cargo", "doc", "--locked", "--no-deps"],
        ["cargo", "build", "--locked", "--release", "--target", target],
    ]
    for command in checks:
        run(command, cwd=root, env=env)
    toolchain = tomllib.loads((root / "rust-toolchain.toml").read_text(encoding="utf-8"))["toolchain"]["channel"]
    compiler = subprocess.check_output(["rustc", "--version", "--verbose"], cwd=root, text=True, encoding="utf-8").strip()
    if f"release: {toolchain}" not in compiler.splitlines():
        raise ValueError("Compilador executado diverge da toolchain fixada")
    binary = root / "target" / target / "release" / ("sider.exe" if target == TARGETS[1] else "sider")
    archive, executable = create_package(binary, root / "README.md", root / "LICENSE", out, version, target)
    inspect_package(archive, executable, version, bootstrap=bootstrap)
    report = {
        "schema_version": 1, "version": version, "sha": sha, "target": target,
        "toolchain": toolchain, "compiler": compiler,
        "artifacts": [file_metadata(archive, target)],
        "gates": [{"id": "native", "status": "success", "sha": sha, "target": target},
                  {"id": "tcp_smoke", "status": "not_run" if bootstrap else "success", "sha": sha, "target": target}],
    }
    write_json(out / f"build-{target}.json", report)
    return report


def product_gates(root, out, plan, release, version, target=TARGETS[0]):
    root, out = Path(root).resolve(), Path(out).resolve()
    registry = json.loads((root / "releases/gates.json").read_text(encoding="utf-8"))["gates"]
    sha = git("rev-parse", "HEAD", cwd=root)
    out.mkdir(parents=True, exist_ok=True)
    evidence, artifacts = [], []
    env = {**os.environ, "SIDER_REFERENCE_IMAGE": plan["reference"]["image"],
           "SIDER_RELEASE_VERSION": version, "SIDER_RELEASE_SHA": sha, "SIDER_RELEASE_DIR": str(out),
           "SIDER_RELEASE_TARGET": target}
    if target not in TARGETS:
        raise ValueError("Target não suportado")
    for gate_id in release["required_gates"]:
        if gate_id in {"native", "tcp_smoke"}:
            continue
        if target == TARGETS[1] and gate_id not in MULTIPLATFORM:
            continue
        specification = registry.get(gate_id, {})
        command = specification.get("command")
        if not isinstance(command, list) or not command or not all(isinstance(arg, str) and arg for arg in command):
            raise ValueError(f"Gate de produto ainda não implementado: {gate_id}")
        receipt_path = out / f"receipt-{gate_id}.json"
        if receipt_path.exists():
            raise ValueError(f"Recibo antigo encontrado para {gate_id}; use diretório novo")
        started = time.monotonic()
        run(command, cwd=root, env=env, timeout=specification["timeout_seconds"])
        elapsed = time.monotonic() - started
        if elapsed < specification.get("minimum_seconds", 0):
            raise ValueError(f"Gate {gate_id} terminou antes da duração mínima")
        # A command that accidentally matches zero tests must not be accepted.
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (receipt.get("sha") != sha or receipt.get("status") != "success"
                or type(receipt.get("cases")) is not int or receipt["cases"] < 1):
            raise ValueError(f"Gate {gate_id} não forneceu recibo de testes executados")
        evidence.append({"id": gate_id, "status": "success", "sha": sha, "target": target, "duration_seconds": elapsed,
                         "cases": receipt["cases"], "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest()})
        if gate_id == "docker":
            archive = out / f"sider-v{version}-linux-amd64-image.tar.gz"
            if not archive.is_file() or archive.stat().st_size == 0:
                raise ValueError("Gate Docker não produziu a imagem exportada")
            artifacts.append(file_metadata(archive, "linux/amd64"))
    report = {"version": version, "sha": sha, "gates": evidence, "artifacts": artifacts}
    write_json(out / f"product-gates-{target}.json", report)
    return report


def assemble(root, out, release, version, candidate=None):
    root, out = Path(root).resolve(), Path(out).resolve()
    sha = git("rev-parse", "HEAD", cwd=root)
    reports = [json.loads((out / f"build-{target}.json").read_text(encoding="utf-8")) for target in TARGETS]
    products = [json.loads((out / f"product-gates-{target}.json").read_text(encoding="utf-8")) for target in TARGETS]
    for report in [*reports, *products]:
        if report.get("sha") != sha or report.get("version") != version:
            raise ValueError("Artefatos de outro SHA ou versão")
    gates = [g for p in products for g in p["gates"] if g["id"] not in MULTIPLATFORM]
    for gate_id in MULTIPLATFORM & set(release["required_gates"]):
        entries = [g for p in products for g in p["gates"] if g["id"] == gate_id]
        if len(entries) != 2 or any(g["status"] != "success" or g["sha"] != sha for g in entries):
            raise ValueError(f"Gate {gate_id} não aprovado nas duas plataformas")
        gates.append({"id": gate_id, "sha": sha, "status": "success",
                      "targets": [g["target"] for g in entries], "evidence": entries})
    for gate_id in ("native", "tcp_smoke"):
        entries = [g for report in reports for g in report["gates"] if g["id"] == gate_id]
        if len(entries) != 2 or any(g["status"] != "success" or g["sha"] != sha for g in entries):
            raise ValueError(f"Gate {gate_id} ausente ou não aprovado nas duas plataformas")
        gates.append({"id": gate_id, "sha": sha, "status": "success", "targets": [g["target"] for g in entries]})
    validate_gate_reports(release, gates, sha)
    toolchains = {r["toolchain"] for r in reports}
    if len(toolchains) != 1:
        raise ValueError("Toolchains divergentes")
    manifest = {"schema_version": 1, "version": version, "sha": sha,
                "toolchain": toolchains.pop(), "required_gates": release["required_gates"], "gates": gates,
                "compilers": {r["target"]: r["compiler"] for r in reports},
                "artifacts": [a for report in [*reports, *products] for a in report["artifacts"]],
                "source_runs": [{"repository": os.environ.get("GITHUB_REPOSITORY"),
                                 "run_id": os.environ.get("GITHUB_RUN_ID")}]}
    if candidate:
        manifest["candidate"] = candidate
    return manifest
