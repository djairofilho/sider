import json
import hashlib
import subprocess
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from tools.release.runner import assemble, build_package, create_package, product_gates, tcp_smoke, TARGETS


SHA = "a" * 40
VERSION = "0.3.0-rc.1"


class RunnerTests(unittest.TestCase):
    def test_archives_have_fixed_members_and_deterministic_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary, readme, license_file = root / "binary", root / "README.md", root / "LICENSE"
            binary.write_bytes(b"fake binary for archive inspection only")
            readme.write_text("Sider", encoding="utf-8")
            license_file.write_bytes((Path(__file__).resolve().parents[2] / "LICENSE").read_bytes())
            for target in TARGETS:
                with self.subTest(target=target):
                    archive, executable = create_package(binary, readme, license_file, root, "0.1.0-rc.1", target)
                    first = archive.read_bytes()
                    again, _ = create_package(binary, readme, license_file, root, "0.1.0-rc.1", target)
                    self.assertEqual(first, again.read_bytes())
                    directory = executable.rsplit("/", 1)[0]
                    expected = {executable: binary.read_bytes(),
                                f"{directory}/README.md": readme.read_bytes(),
                                f"{directory}/LICENSE": license_file.read_bytes()}
                    if target == TARGETS[1]:
                        with zipfile.ZipFile(archive) as package:
                            self.assertEqual(package.namelist(), sorted(expected))
                            self.assertEqual({name: package.read(name) for name in package.namelist()}, expected)
                    else:
                        with tarfile.open(archive) as package:
                            self.assertEqual(package.getnames(), sorted(expected))
                            self.assertTrue(all(member.isfile() for member in package.getmembers()))
                            self.assertEqual({name: package.extractfile(name).read() for name in package.getnames()}, expected)

    def test_missing_or_empty_license_blocks_archives_without_output(self):
        for target in TARGETS:
            for content in (None, b"", b" \r\n\t"):
                with self.subTest(target=target, content=content), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    binary, readme, license_file = root / "binary", root / "README.md", root / "LICENSE"
                    binary.write_bytes(b"fake binary for archive inspection only")
                    readme.write_text("Sider", encoding="utf-8")
                    if content is not None:
                        license_file.write_bytes(content)
                    with self.assertRaisesRegex(ValueError, "LICENSE (ausente|vazio)"):
                        create_package(binary, readme, license_file, root / "out", VERSION, target)
                    self.assertFalse((root / "out").exists())

    def test_missing_or_empty_root_license_blocks_before_build(self):
        for content in (None, b""):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                (root / "Cargo.toml").write_text(
                    f'[package]\nversion = "{VERSION}"\npublish = false\n', encoding="utf-8")
                if content is not None:
                    (root / "LICENSE").write_bytes(content)
                with patch("tools.release.runner.run") as run, patch("tools.release.runner.git") as git:
                    with self.assertRaisesRegex(ValueError, "LICENSE (ausente|vazio)"):
                        build_package(root, root / "out", VERSION, TARGETS[1], bootstrap=True)
                run.assert_not_called()
                git.assert_not_called()
                self.assertFalse((root / "out").exists())

    def test_missing_gate_fails_instead_of_reporting_success(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "releases").mkdir()
            (root / "releases/gates.json").write_text(json.dumps({"gates": {"compatibility": {"command": None}}}), encoding="utf-8")
            with patch("tools.release.runner.git", return_value="a" * 40), self.assertRaisesRegex(ValueError, "não implementado"):
                product_gates(root, root / "out", {"reference": {"image": "redis@sha256:test"}},
                              {"required_gates": ["native", "compatibility"]}, "0.1.0-rc.1")
            self.assertFalse((root / f"out/product-gates-{TARGETS[0]}.json").exists())


class TcpReadinessTests(unittest.TestCase):
    def process(self, ready=None, *, poll=None):
        process = Mock(pid=421, poll=Mock(return_value=poll))

        def spawn(argv, *, env, **kwargs):
            self.assertEqual(env["SIDER_ADDR"], "127.0.0.1:0")
            self.assertIn("SIDER_READY_FILE", env)
            if ready is not None:
                Path(env["SIDER_READY_FILE"]).write_text(json.dumps(ready), encoding="utf-8")
            return process

        return process, spawn

    def connection(self, response=None):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.recv.side_effect = response or [b"+PO", b"NG\r\n"]
        return connection

    def test_ping_uses_address_and_pid_emitted_by_spawned_process(self):
        process, spawn = self.process({"pid": 421, "host": "127.0.0.1", "port": 54231})
        connection = self.connection()
        with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                patch("tools.release.runner.socket.create_connection", return_value=connection) as connect:
            tcp_smoke(Path("sider.exe"))
        connect.assert_called_once_with(("127.0.0.1", 54231), timeout=0.2)
        connection.sendall.assert_called_once_with(b"*1\r\n$4\r\nPING\r\n")
        self.assertEqual(connection.recv.call_count, 2)
        process.terminate.assert_called_once()
        process.wait.assert_called_once_with(timeout=5)

    def test_wrong_pid_or_invalid_address_never_connects(self):
        invalid_signals = [
            {"pid": 422, "host": "127.0.0.1", "port": 1234},
            {"pid": 421, "host": "0.0.0.0", "port": 1234},
            {"pid": 421, "host": "127.0.0.1", "port": 0},
            {"pid": 421, "host": "127.0.0.1", "port": 65536},
            {"pid": 421, "host": "127.0.0.1", "port": True},
        ]
        for ready in invalid_signals:
            with self.subTest(ready=ready):
                process, spawn = self.process(ready)
                with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                        patch("tools.release.runner.socket.create_connection") as connect:
                    with self.assertRaisesRegex(ValueError, "prontidão"):
                        tcp_smoke(Path("sider.exe"))
                connect.assert_not_called()
                process.terminate.assert_called_once()

    def test_missing_readiness_times_out_without_guessing_a_port(self):
        process, spawn = self.process()
        with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                patch("tools.release.runner.socket.create_connection") as connect, \
                patch("tools.release.runner.time.monotonic", side_effect=[0, 0, 2]), \
                patch("tools.release.runner.time.sleep"):
            with self.assertRaisesRegex(ValueError, "Timeout"):
                tcp_smoke(Path("sider.exe"), timeout=1)
        connect.assert_not_called()
        process.terminate.assert_called_once()

    def test_bootstrap_exit_before_readiness_is_not_success(self):
        process, spawn = self.process(poll=0)
        with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                patch("tools.release.runner.socket.create_connection") as connect:
            with self.assertRaisesRegex(ValueError, "encerrou antes"):
                tcp_smoke(Path("sider.exe"))
        connect.assert_not_called()
        process.terminate.assert_not_called()

    def test_exit_during_ping_rejects_even_a_correct_reply(self):
        process, spawn = self.process({"pid": 421, "host": "127.0.0.1", "port": 54231})
        process.poll.side_effect = [None, 0, 0]
        with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                patch("tools.release.runner.socket.create_connection", return_value=self.connection()):
            with self.assertRaisesRegex(ValueError, "durante o smoke"):
                tcp_smoke(Path("sider.exe"))

    def test_invalid_ping_reply_rejects_and_cleans_up_process(self):
        process, spawn = self.process({"pid": 421, "host": "127.0.0.1", "port": 54231})
        with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                patch("tools.release.runner.socket.create_connection", return_value=self.connection([b"+BAD\r\n", b""])):
            with self.assertRaisesRegex(ValueError, "Resposta TCP inválida"):
                tcp_smoke(Path("sider.exe"))
        process.terminate.assert_called_once()

    def test_unresponsive_process_is_killed_after_termination_timeout(self):
        process, spawn = self.process({"pid": 421, "host": "127.0.0.1", "port": 54231})
        process.wait.side_effect = [subprocess.TimeoutExpired("sider", 5), 0]
        with patch("tools.release.runner.subprocess.Popen", side_effect=spawn), \
                patch("tools.release.runner.socket.create_connection", return_value=self.connection()):
            tcp_smoke(Path("sider.exe"))
        process.kill.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)


class ProductGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="sider-gate-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.out = self.root / "out"
        (self.root / "releases").mkdir()
        self.plan = {"reference": {"image": "redis:8.10.1@sha256:" + "b" * 64}}
        self.release = {"required_gates": ["native", "compatibility", "tcp_smoke"]}
        self.specification = {"command": ["python", "test-gate.py"], "timeout_seconds": 30}

    def execute(self, *, target=TARGETS[0]):
        (self.root / "releases/gates.json").write_text(json.dumps({"gates": {"compatibility": self.specification}}), encoding="utf-8")
        with patch("tools.release.runner.git", return_value=SHA):
            return product_gates(self.root, self.out, self.plan, self.release, VERSION, target)

    def receipt(self, cases=2, status="success", sha=SHA):
        def write_receipt(argv, *, cwd, env, timeout):
            self.assertEqual(argv, self.specification["command"])
            self.assertEqual(cwd, self.root)
            self.assertEqual(env["SIDER_REFERENCE_IMAGE"], self.plan["reference"]["image"])
            self.assertEqual(env["SIDER_RELEASE_SHA"], SHA)
            self.assertEqual(env["SIDER_RELEASE_VERSION"], VERSION)
            self.assertEqual(timeout, 30)
            Path(env["SIDER_RELEASE_DIR"], "receipt-compatibility.json").write_text(
                json.dumps({"sha": sha, "status": status, "cases": cases, "scenario": "Expiração e recuperação"}, ensure_ascii=False),
                encoding="utf-8",
            )
        return write_receipt

    def test_old_receipt_prevents_executing_or_reusing_gate(self):
        self.out.mkdir()
        (self.out / "receipt-compatibility.json").write_text('{"sha":"stale"}', encoding="utf-8")
        with patch("tools.release.runner.run") as run:
            with self.assertRaisesRegex(ValueError, "Recibo antigo"):
                self.execute()
        run.assert_not_called()
        self.assertFalse((self.out / f"product-gates-{TARGETS[0]}.json").exists())

    def test_zero_boolean_or_missing_cases_never_count_as_evidence(self):
        for cases in (0, -1, True, None):
            with self.subTest(cases=cases):
                receipt = self.out / "receipt-compatibility.json"
                if receipt.exists():
                    receipt.unlink()
                with patch("tools.release.runner.run", side_effect=self.receipt(cases=cases)):
                    with self.assertRaisesRegex(ValueError, "recibo de testes"):
                        self.execute()
                self.assertFalse((self.out / f"product-gates-{TARGETS[0]}.json").exists())

    def test_wrong_sha_or_non_success_receipt_rejected(self):
        for status, sha in (("skipped", SHA), ("failure", SHA), ("success", "b" * 40)):
            with self.subTest(status=status, sha=sha):
                receipt = self.out / "receipt-compatibility.json"
                if receipt.exists():
                    receipt.unlink()
                with patch("tools.release.runner.run", side_effect=self.receipt(status=status, sha=sha)):
                    with self.assertRaisesRegex(ValueError, "recibo de testes"):
                        self.execute()

    def test_success_requires_receipt_and_records_its_hash(self):
        with patch("tools.release.runner.run", side_effect=self.receipt()), \
                patch("tools.release.runner.time.monotonic", side_effect=[100, 102]):
            result = self.execute()
        receipt = self.out / "receipt-compatibility.json"
        gate = result["gates"][0]
        self.assertEqual(gate["cases"], 2)
        self.assertEqual(gate["duration_seconds"], 2)
        self.assertEqual(gate["receipt_sha256"], hashlib.sha256(receipt.read_bytes()).hexdigest())
        self.assertIn("Expiração e recuperação", receipt.read_text(encoding="utf-8"))
        self.assertEqual(json.loads((self.out / f"product-gates-{TARGETS[0]}.json").read_text(encoding="utf-8")), result)

    def test_command_success_without_receipt_still_fails(self):
        with patch("tools.release.runner.run"):
            with self.assertRaises(FileNotFoundError):
                self.execute()
        self.assertFalse((self.out / f"product-gates-{TARGETS[0]}.json").exists())

    def test_failed_command_propagates_without_success_report(self):
        with patch("tools.release.runner.run", side_effect=subprocess.CalledProcessError(1, ["test-gate"])):
            with self.assertRaises(subprocess.CalledProcessError):
                self.execute()
        self.assertFalse((self.out / f"product-gates-{TARGETS[0]}.json").exists())

    def test_duration_cannot_be_satisfied_by_early_exit(self):
        self.specification["minimum_seconds"] = 900
        with patch("tools.release.runner.run", side_effect=self.receipt()), \
                patch("tools.release.runner.time.monotonic", side_effect=[0, 899]):
            with self.assertRaisesRegex(ValueError, "duração mínima"):
                self.execute()

    def test_linux_only_gate_is_not_reported_as_windows_success(self):
        self.specification["command"] = None
        with patch("tools.release.runner.run") as run:
            result = self.execute(target=TARGETS[1])
        run.assert_not_called()
        self.assertEqual(result["gates"], [])


class AssembleGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="sider-assemble-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.release = {"required_gates": ["native", "tcp_smoke", "compatibility", "fuzz", "crash", "recovery", "migration"]}
        self.files = {}
        for target in TARGETS:
            self.files[f"build-{target}.json"] = {
                "version": VERSION, "sha": SHA, "target": target, "toolchain": "1.97.1", "compiler": "rustc 1.97.1",
                "artifacts": [{"name": f"sider-{target}.zip", "target": target, "size": 10, "sha256": "b" * 64}],
                "gates": [{"id": gate, "sha": SHA, "target": target, "status": "success"} for gate in ("native", "tcp_smoke")],
            }
            gates = ["crash", "recovery", "migration"] + (["compatibility", "fuzz"] if target == TARGETS[0] else [])
            self.files[f"product-gates-{target}.json"] = {
                "version": VERSION, "sha": SHA, "artifacts": [],
                "gates": [{"id": gate, "sha": SHA, "target": target, "status": "success", "duration_seconds": 900} for gate in gates],
            }

    def execute(self):
        for filename, content in self.files.items():
            (self.root / filename).write_text(json.dumps(content), encoding="utf-8")
        with patch("tools.release.runner.git", return_value=SHA):
            return assemble(self.root, self.root, self.release, VERSION)

    def test_combines_native_and_persistence_evidence_from_both_targets(self):
        result = self.execute()
        indexed = {gate["id"]: gate for gate in result["gates"]}
        self.assertEqual(set(indexed), set(self.release["required_gates"]))
        for gate in ("native", "tcp_smoke", "crash", "recovery", "migration"):
            self.assertEqual(set(indexed[gate]["targets"]), set(TARGETS))
        self.assertEqual(indexed["fuzz"]["duration_seconds"], 900)
        self.assertEqual(len(result["artifacts"]), 2)
        self.assertEqual(set(result["compilers"]), set(TARGETS))

    def test_duplicate_target_cannot_replace_other_platform(self):
        self.files[f"product-gates-{TARGETS[1]}.json"]["gates"][0]["target"] = TARGETS[0]
        with self.assertRaisesRegex(ValueError, "Linux e Windows"):
            self.execute()

    def test_missing_windows_crash_gate_fails(self):
        report = self.files[f"product-gates-{TARGETS[1]}.json"]
        report["gates"] = [gate for gate in report["gates"] if gate["id"] != "crash"]
        with self.assertRaisesRegex(ValueError, "crash"):
            self.execute()

    def test_bootstrap_tcp_not_run_blocks_assembly(self):
        self.files[f"build-{TARGETS[1]}.json"]["gates"][1]["status"] = "not_run"
        with self.assertRaisesRegex(ValueError, "tcp_smoke"):
            self.execute()

    def test_artifacts_from_another_sha_or_version_are_rejected(self):
        for field, value in (("sha", "b" * 40), ("version", "0.3.0-rc.2")):
            with self.subTest(field=field):
                report = self.files[f"product-gates-{TARGETS[0]}.json"]
                original = report[field]
                report[field] = value
                with self.assertRaisesRegex(ValueError, "outro SHA ou versão"):
                    self.execute()
                report[field] = original

    def test_toolchain_mismatch_rejected(self):
        self.files[f"build-{TARGETS[1]}.json"]["toolchain"] = "1.98.0"
        with self.assertRaisesRegex(ValueError, "Toolchains divergentes"):
            self.execute()


if __name__ == "__main__":
    unittest.main()
