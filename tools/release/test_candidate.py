"""Candidate promotion never blesses functional changes or dependency updates."""

import subprocess
import unittest
from unittest.mock import Mock, patch

from tools.release.policy import candidate_for


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.client.paginate.return_value = [
            {"tag_name": "v0.1.0-rc.1", "draft": False, "prerelease": True},
        ]

    @staticmethod
    def git_fixture(*args, **kwargs):
        if args[0] == "rev-list":
            return "a" * 40
        if args[0] == "merge-base":
            return ""
        if args[0] == "diff":
            return "Cargo.toml\nCargo.lock\nCHANGELOG.md\nreleases/notes/v0.1.0.md"
        if args[0] == "show":
            version = "0.1.0-rc.1" if args[1].startswith("a" * 40) else "0.1.0"
            if args[1].endswith("Cargo.toml"):
                return f'[package]\nname="sider"\nversion="{version}"\npublish=false\n'
            return f'[[package]]\nname="sider"\nversion="{version}"\n'
        raise AssertionError(args)

    def test_final_allows_only_version_and_notes(self):
        with patch("tools.release.policy.git", side_effect=self.git_fixture):
            result = candidate_for("0.1.0", "b" * 40, self.client, ".")
        self.assertEqual(result, {"tag": "v0.1.0-rc.1", "sha": "a" * 40})

    def test_functional_change_requires_another_rc(self):
        def source(*args, **kwargs):
            return "src/main.rs" if args[0] == "diff" else self.git_fixture(*args, **kwargs)
        with patch("tools.release.policy.git", side_effect=source), self.assertRaisesRegex(ValueError, "funcional"):
            candidate_for("0.1.0", "b" * 40, self.client, ".")

    def test_dependency_change_in_lock_is_not_version_only(self):
        def source(*args, **kwargs):
            result = self.git_fixture(*args, **kwargs)
            if args[0] == "show" and args[1].startswith("b" * 40) and args[1].endswith("Cargo.lock"):
                result += '\n[[package]]\nname="dependency"\nversion="2.0.0"\n'
            return result
        with patch("tools.release.policy.git", side_effect=source), self.assertRaisesRegex(ValueError, "Dependências"):
            candidate_for("0.1.0", "b" * 40, self.client, ".")

    def test_candidate_must_be_ancestor(self):
        def source(*args, **kwargs):
            if args[0] == "merge-base":
                raise subprocess.CalledProcessError(1, ["git", *args])
            return self.git_fixture(*args, **kwargs)
        with patch("tools.release.policy.git", side_effect=source), self.assertRaises(subprocess.CalledProcessError):
            candidate_for("0.1.0", "b" * 40, self.client, ".")

    def test_cannot_publish_an_older_rc_after_a_newer_one(self):
        self.client.paginate.return_value[0]["tag_name"] = "v0.1.0-rc.2"
        with self.assertRaisesRegex(ValueError, "avança"):
            candidate_for("0.1.0-rc.1", "b" * 40, self.client, ".")

    def test_final_without_published_candidate_is_blocked(self):
        self.client.paginate.return_value[0]["draft"] = True
        with self.assertRaisesRegex(ValueError, "candidata publicada"):
            candidate_for("0.1.0", "b" * 40, self.client, ".")


if __name__ == "__main__":
    unittest.main()
