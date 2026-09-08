"""Preflight testa o evento e arquivos reais com Git e GitHub simulados."""

from __future__ import annotations

import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from tools.release.cli import preflight


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.version = "0.1.0-rc.1"
        self.notes = f"# Sider v{self.version}\n\nValidação reproduzível.\n"
        (self.root / "releases/notes").mkdir(parents=True)
        (self.root / "Cargo.toml").write_text(f'[package]\nname = "sider"\nversion = "{self.version}"\npublish = false\n', encoding="utf-8")
        (self.root / "Cargo.lock").write_text(f'version = 4\n\n[[package]]\nname = "sider"\nversion = "{self.version}"\n', encoding="utf-8")
        (self.root / "CHANGELOG.md").write_text(f"# Changelog\n\n## [{self.version}]\n", encoding="utf-8")
        self.notes_path = self.root / f"releases/notes/v{self.version}.md"
        self.notes_path.write_text(self.notes, encoding="utf-8", newline="\n")
        repository = {"full_name": "djairofilho/sider"}
        self.event = {"action": "closed", "repository": repository, "pull_request": {
            "number": 12, "merged": True, "merge_commit_sha": "a" * 40,
            "head": {"ref": f"chore/release-v{self.version}", "repo": repository, "sha": "b" * 40},
            "base": {"ref": "main", "repo": repository}, "labels": [{"name": "type:release"}],
        }}
        self.plan = {"repository": repository["full_name"], "releases": [{"id": "R01", "version": "0.1.0"}]}
        self.client = Mock()
        self.client.repo_path.side_effect = lambda suffix: "/repos/djairofilho/sider" + suffix
        self.client.paginate.return_value = []
        self.tracking = {"gate_issue": 7, "milestone_number": 1}

    def test_merged_preflight_selects_exact_merge_commit_and_runs_source_validation(self):
        with patch("tools.release.cli.git", return_value="a" * 40) as mocked_git, patch("tools.release.cli.readiness", return_value=self.tracking) as readiness, patch("tools.release.cli.candidate_for", return_value=None) as candidate:
            result = preflight(self.root, self.plan, self.client, self.event)
        self.assertEqual(result, {"version": self.version, "sha": "a" * 40, "pr": 12,
                                  "gate_issue": 7, "milestone_number": 1, "candidate": None,
                                  "published": False, "merged": True})
        mocked_git.assert_any_call("rev-parse", "HEAD", cwd=self.root)
        mocked_git.assert_any_call("diff", "--quiet", cwd=self.root)
        mocked_git.assert_any_call("diff", "--cached", "--quiet", cwd=self.root)
        readiness.assert_called_once_with(self.plan, self.plan["releases"][0], self.client)
        candidate.assert_called_once_with(self.version, "a" * 40, self.client, self.root)
        self.client.request.assert_not_called()

    def test_checkout_of_latest_main_instead_of_merge_sha_is_rejected_early(self):
        with patch("tools.release.cli.git", return_value="c" * 40), patch("tools.release.cli.validate_version_files") as validate_files, patch("tools.release.cli.readiness") as readiness:
            with self.assertRaisesRegex(ValueError, "Checkout não corresponde"):
                preflight(self.root, self.plan, self.client, self.event)
        validate_files.assert_not_called()
        readiness.assert_not_called()
        self.client.paginate.assert_not_called()

    def test_bad_or_pending_notes_are_rejected_before_remote_readiness(self):
        for notes in ["# Sider v0.2.0\n\nVersão errada.\n", self.notes + "<!-- pending -->\n"]:
            with self.subTest(notes=notes):
                self.notes_path.write_text(notes, encoding="utf-8", newline="\n")
                with patch("tools.release.cli.git", return_value="a" * 40), patch("tools.release.cli.readiness") as readiness, patch("tools.release.cli.candidate_for") as candidate:
                    with self.assertRaisesRegex(ValueError, "Notas de release"):
                        preflight(self.root, self.plan, self.client, self.event)
                readiness.assert_not_called()
                candidate.assert_not_called()
        self.client.paginate.assert_not_called()

    def test_missing_notes_or_wrong_lock_version_is_rejected(self):
        self.notes_path.unlink()
        with patch("tools.release.cli.git", return_value="a" * 40), patch("tools.release.cli.readiness") as readiness:
            with self.assertRaises(FileNotFoundError):
                preflight(self.root, self.plan, self.client, self.event)
        self.notes_path.write_text(self.notes, encoding="utf-8")
        lock_path = self.root / "Cargo.lock"
        lock_path.write_text(lock_path.read_text(encoding="utf-8").replace(self.version, "0.1.0"), encoding="utf-8")
        with patch("tools.release.cli.git", return_value="a" * 40), patch("tools.release.cli.readiness") as readiness:
            with self.assertRaisesRegex(ValueError, "Cargo.toml, Cargo.lock"):
                preflight(self.root, self.plan, self.client, self.event)
        readiness.assert_not_called()

    def test_incomplete_milestone_stops_before_candidate_or_publication_lookup(self):
        with patch("tools.release.cli.git", return_value="a" * 40), patch("tools.release.cli.readiness", side_effect=ValueError("Milestone contém bloqueios abertos")), patch("tools.release.cli.candidate_for") as candidate:
            with self.assertRaisesRegex(ValueError, "bloqueios abertos"):
                preflight(self.root, self.plan, self.client, self.event)
        candidate.assert_not_called()
        self.client.paginate.assert_not_called()
        self.client.request.assert_not_called()

    def test_closed_unmerged_or_foreign_pr_cannot_reach_repository_validation(self):
        invalid_events = []
        unmerged = copy.deepcopy(self.event)
        unmerged["pull_request"]["merged"] = False
        invalid_events.append(unmerged)
        foreign = copy.deepcopy(self.event)
        foreign["pull_request"]["head"]["repo"] = {"full_name": "elsewhere/sider"}
        invalid_events.append(foreign)
        for event in invalid_events:
            with self.subTest(event=event), patch("tools.release.cli.git") as mocked_git, patch("tools.release.cli.readiness") as readiness:
                with self.assertRaises(ValueError):
                    preflight(self.root, self.plan, self.client, event)
                mocked_git.assert_not_called()
                readiness.assert_not_called()

    def test_open_pr_validation_uses_head_sha_and_never_marks_it_merged(self):
        event = copy.deepcopy(self.event)
        event.update(action="synchronize")
        event["pull_request"]["merged"] = False
        with patch("tools.release.cli.git", return_value="b" * 40), patch("tools.release.cli.readiness", return_value=self.tracking), patch("tools.release.cli.candidate_for", return_value=None):
            result = preflight(self.root, self.plan, self.client, event)
        self.assertEqual(result["sha"], "b" * 40)
        self.assertFalse(result["merged"])

    def test_published_detection_excludes_drafts_and_other_versions(self):
        for release, expected in [
            ({"tag_name": "v" + self.version, "draft": False}, True),
            ({"tag_name": "v" + self.version, "draft": True}, False),
            ({"tag_name": "v0.2.0-rc.1", "draft": False}, False),
        ]:
            with self.subTest(release=release):
                self.client.paginate.return_value = [release]
                with patch("tools.release.cli.git", return_value="a" * 40), patch("tools.release.cli.readiness", return_value=self.tracking), patch("tools.release.cli.candidate_for", return_value=None):
                    result = preflight(self.root, self.plan, self.client, self.event)
                self.assertEqual(result["published"], expected)


if __name__ == "__main__":
    unittest.main()
