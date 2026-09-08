"""Prévia de versão e preparação sem alterações locais ou chamadas de escrita."""

from __future__ import annotations

import copy
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from tools.release.prepare import prepare_release, version_edits


class ReadOnlyPlanningClient:
    repo = "djairofilho/sider"

    def __init__(self):
        self.calls = []
        self.task_state = "closed"

    def repo_path(self, suffix):
        return f"/repos/{self.repo}" + ("/" + suffix.lstrip("/") if suffix else "")

    def request(self, method, path, body=None):
        self.calls.append((method, path))
        if method == "GET" and path == self.repo_path(""):
            return {"private": True}
        raise AssertionError(f"Chamada não autorizada no teste: {method} {path}")

    def paginate(self, path):
        self.calls.append(("GET", path))
        gate = {"number": 7, "state": "open", "milestone": {"number": 1},
                "body": "<!-- sider:task R01-GATE -->"}
        task = {"number": 6, "state": self.task_state, "milestone": {"number": 1},
                "body": "<!-- sider:task R01-01 -->"}
        if path == self.repo_path("/milestones?state=all"):
            return [{"number": 1, "title": "v0.1.0", "state": "open"}]
        if path == self.repo_path("/issues?state=all"):
            return [task, gate]
        if path == self.repo_path("/issues?state=open&milestone=1"):
            return [gate] + ([task] if self.task_state == "open" else [])
        if path == self.repo_path("/releases"):
            return []
        raise AssertionError(f"Leitura inesperada no teste: {path}")


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.cargo = ('[package]\nname = "sider"\nversion = "0.1.0"\npublish = false\n'
                      '\n[dependencies.bytes]\nversion = "1.10.1"\n')
        self.lock = ('version = 4\n\n[[package]]\nname = "bytes"\nversion = "1.10.1"\n'
                     '\n[[package]]\nname = "sider"\nversion = "0.1.0"\ndependencies = ["bytes"]\n')
        self.changelog = "# Changelog\n\n## [Unreleased]\n\n## [0.0.1]\n\nRegistro histórico.\n"
        for name, content in [("Cargo.toml", self.cargo), ("Cargo.lock", self.lock), ("CHANGELOG.md", self.changelog)]:
            (self.root / name).write_text(content, encoding="utf-8", newline="\n")
        self.version = "0.1.0-rc.1"
        self.notes = f"# Sider v{self.version}\n\nValidação de publicação, programação e memória.\n"
        self.release = {"id": "R01", "version": "0.1.0", "depends_on": [],
                        "tasks": [{"id": "R01-01"}], "gate": {"id": "R01-GATE"}}
        self.plan = {"repository": "djairofilho/sider", "releases": [self.release]}
        self.client = ReadOnlyPlanningClient()

    def snapshot(self):
        return {str(path.relative_to(self.root)): path.read_bytes() for path in self.root.rglob("*") if path.is_file()}

    def test_version_preview_changes_only_sider_and_preserves_existing_files(self):
        before = self.snapshot()
        edits = version_edits(self.root, self.version, self.notes)
        self.assertEqual(set(edits), {"Cargo.toml", "Cargo.lock", "CHANGELOG.md", f"releases/notes/v{self.version}.md"})
        cargo = tomllib.loads(edits["Cargo.toml"])
        lock = tomllib.loads(edits["Cargo.lock"])
        self.assertEqual(cargo["package"]["version"], self.version)
        self.assertFalse(cargo["package"]["publish"])
        self.assertEqual(cargo["dependencies"]["bytes"]["version"], "1.10.1")
        versions = {package["name"]: package["version"] for package in lock["package"]}
        self.assertEqual(versions, {"sider": self.version, "bytes": "1.10.1"})
        self.assertIn("## [Unreleased]", edits["CHANGELOG.md"])
        self.assertIn(f"## [{self.version}]", edits["CHANGELOG.md"])
        self.assertIn("## [0.0.1]\n\nRegistro histórico.", edits["CHANGELOG.md"])
        self.assertEqual(edits[f"releases/notes/v{self.version}.md"], self.notes)
        self.assertEqual(self.snapshot(), before)

    def test_notes_must_be_for_exact_version_and_complete(self):
        before = self.snapshot()
        for notes in ["# Sider v0.2.0\n\nOutro alvo.\n", self.notes + "<!-- pending -->\n", ""]:
            with self.subTest(notes=notes), self.assertRaisesRegex(ValueError, "Notas"):
                version_edits(self.root, self.version, notes)
        self.assertEqual(self.snapshot(), before)

    def test_duplicate_version_cannot_be_added_to_changelog(self):
        path = self.root / "CHANGELOG.md"
        path.write_text(self.changelog + f"\n## [{self.version}]\n", encoding="utf-8")
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, "já contém"):
            version_edits(self.root, self.version, self.notes)
        self.assertEqual(self.snapshot(), before)

    def test_missing_package_lock_or_unreleased_header_fails_without_edits(self):
        changes = [
            ("Cargo.toml", '[package]\nname = "sider"\n'),
            ("Cargo.lock", 'version = 4\n\n[[package]]\nname = "bytes"\nversion = "1.10.1"\n'),
            ("CHANGELOG.md", "# Changelog\n"),
        ]
        for filename, content in changes:
            with self.subTest(filename=filename):
                path = self.root / filename
                previous = path.read_bytes()
                path.write_text(content, encoding="utf-8")
                before = self.snapshot()
                with self.assertRaises(ValueError):
                    version_edits(self.root, self.version, self.notes)
                self.assertEqual(self.snapshot(), before)
                path.write_bytes(previous)

    def test_dry_run_reads_real_readiness_but_never_creates_branch_or_pr(self):
        before = self.snapshot()
        with patch("tools.release.prepare.git", return_value="a" * 40) as mocked_git, patch("tools.release.prepare.run") as run:
            result = prepare_release(self.root, self.plan, self.release, self.version, self.notes, self.client)
        self.assertEqual(result["mode"], "dry-run")
        self.assertEqual(result["branch"], "chore/release-v0.1.0-rc.1")
        self.assertEqual(result["gate_issue"], 7)
        self.assertEqual(result["milestone_number"], 1)
        self.assertEqual(len(result["files"]), 4)
        run.assert_not_called()
        self.assertTrue(all(call.args[0] == "rev-parse" for call in mocked_git.call_args_list))
        self.assertTrue(all(method == "GET" for method, _ in self.client.calls))
        self.assertEqual(self.snapshot(), before)

    def test_dry_run_blocks_incomplete_backlog_before_git_or_version_preview(self):
        self.client.task_state = "open"
        before = self.snapshot()
        with patch("tools.release.prepare.git") as mocked_git, patch("tools.release.prepare.run") as run:
            with self.assertRaisesRegex(ValueError, "Tarefas ainda não concluídas"):
                prepare_release(self.root, self.plan, self.release, self.version, self.notes, self.client)
        mocked_git.assert_not_called()
        run.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_final_dry_run_requires_candidate_validation(self):
        final_version = "0.1.0"
        notes = "# Sider v0.1.0\n\nVersão final verificada.\n"
        before = self.snapshot()
        with patch("tools.release.prepare.git", return_value="a" * 40), patch("tools.release.prepare.candidate_for", side_effect=ValueError("Candidata ausente")) as candidate, patch("tools.release.prepare.run") as run:
            with self.assertRaisesRegex(ValueError, "Candidata ausente"):
                prepare_release(self.root, copy.deepcopy(self.plan), self.release, final_version, notes, self.client)
        candidate.assert_called_once_with(final_version, "a" * 40, self.client, self.root.resolve())
        run.assert_not_called()
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
