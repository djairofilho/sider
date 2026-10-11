#!/usr/bin/env python3
"""Exercise the reference client against an extracted development Sider binary."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import platform
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path

import reference

ROOT = Path(__file__).resolve().parents[2]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def jsonify(value):
    if isinstance(value, bytes):
        return {"bulk_hex": value.hex()}
    if isinstance(value, list):
        return [jsonify(item) for item in value]
    return value


class Server:
    def __init__(self, binary: Path, directory: Path) -> None:
        self.binary = binary
        self.directory = directory
        self.child: subprocess.Popen | None = None
        self.log = None

    def start(self) -> int:
        ready = self.directory / "ready.json"
        ready.unlink(missing_ok=True)
        env = {name: value for name, value in os.environ.items()
               if not name.upper().startswith("SIDER_")}
        env.update(SIDER_ADDR="127.0.0.1:0", SIDER_READY_FILE=str(ready),
                   SIDER_SECURITY_PROFILE="legacy-loopback",
                   SIDER_AOF_DIR=str(self.directory / "aof"),
                   SIDER_AOF_RECORD_VERSION="2", SIDER_AOF_SYNC="always",
                   SIDER_AOF_COMPACT_AFTER_BYTES="0", SIDER_SHARDS="1")
        self.log = (self.directory / "server.log").open("ab")
        self.child = subprocess.Popen([str(self.binary)], env=env,
                                      stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while not ready.exists():
            if self.child.poll() is not None:
                raise RuntimeError("extracted server exited before readiness: " +
                                   (self.directory / "server.log").read_text(errors="replace"))
            if time.monotonic() > deadline:
                raise TimeoutError("extracted server readiness")
            time.sleep(0.01)
        state = json.loads(ready.read_text())
        assert state["pid"] == self.child.pid and state["host"] == "127.0.0.1"
        return state["port"]

    def stop(self) -> None:
        if self.child is not None and self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=5)
        if self.log is not None:
            self.log.close()
        self.child, self.log = None, None


def exercise(binary: Path) -> dict:
    assert git("status", "--porcelain") == "", "run from a clean checkout"
    transcript: list[dict] = []
    stage = "preflight"

    def record(args: tuple[bytes, ...], result: object) -> None:
        transcript.append({"stage": stage, "request_bulk_hex": [arg.hex() for arg in args],
                           "reply": jsonify(result)})

    with tempfile.TemporaryDirectory(prefix="sider-memory-extracted-") as temp:
        directory = Path(temp)
        name = "sider.exe" if os.name == "nt" else "sider"
        archive = directory / "development-binary.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
            output.write(binary, arcname=name)
        extracted_dir = directory / "extracted"
        extracted_dir.mkdir()
        with zipfile.ZipFile(archive) as source:
            assert source.namelist() == [name]
            extracted = Path(source.extract(name, extracted_dir))
        extracted.chmod(binary.stat().st_mode)
        original_hash = sha256(binary)
        assert sha256(extracted) == original_hash
        version = subprocess.check_output([str(extracted), "--version"], text=True).strip()
        server = Server(extracted, directory)
        try:
            port = server.start()
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                reference.run("127.0.0.1", port, True, False, durable=True, record=record)
            if "Sider commands sent: 0" not in output.getvalue():
                raise AssertionError("provider failure did not stop before Sider writes")
            client = reference.Sider("127.0.0.1", port, record=record)
            try:
                assert client.command(b"MEM.INFO", reference.COLLECTION) is None
            finally:
                client.close()
            preflight_output = output.getvalue()
            stage = "ingest_and_lifecycle"
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                reference.run("127.0.0.1", port, False, False, durable=True, record=record)
            lifecycle_output = output.getvalue()
            server.stop()
            port = server.start()
            stage = "after_recovery"
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                reference.check_recovered("127.0.0.1", port, record=record)
            recovery_output = output.getvalue()
        finally:
            server.stop()
        return {"schema": "sider-memory-reference-result/1",
                "source_sha": git("rev-parse", "HEAD"),
                "source_binary_sha256": original_hash,
                "archive_sha256": sha256(archive),
                "extracted_binary_sha256": sha256(extracted),
                "version": version, "platform": platform.platform(),
                "python": platform.python_version(),
                "configuration": {"security_profile": "legacy-loopback",
                                  "aof_record_version": 2, "aof_sync": "always",
                                  "shards": 1, "address": "127.0.0.1:0"},
                "preflight_output": preflight_output,
                "lifecycle_output": lifecycle_output,
                "recovery_output": recovery_output,
                "transcript": transcript,
                "assertions": ["provider failure sent zero commands",
                               "collection absent after provider failure",
                               "filtered source retrieval",
                               "replacement and provenance",
                               "TTL expiry and deletion",
                               "record-2 AOF recovery"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = exercise(args.binary.resolve(strict=True))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
