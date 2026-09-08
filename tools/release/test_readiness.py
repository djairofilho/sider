"""Elegibilidade de milestones contra uma API GitHub falsa."""

import copy
import unittest
from urllib.parse import parse_qs, urlsplit

from .policy import readiness
from .sync import sync_plan
from .test_sync import FakeGitHub, example_plan


class ReadinessGitHub(FakeGitHub):
    def __init__(self):
        super().__init__()
        self.releases = []

    def paginate(self, path):
        parts = urlsplit(path)
        if parts.path == self.repo_path("/releases"):
            self.calls.append(("GET", path, None))
            return copy.deepcopy(self.releases)
        rows = super().paginate(path)
        if parts.path == self.repo_path("/issues"):
            query = parse_qs(parts.query)
            if query.get("state", ["all"])[0] != "all":
                rows = [row for row in rows if row.get("state") == query["state"][0]]
            if "milestone" in query:
                rows = [row for row in rows if str((row.get("milestone") or {}).get("number")) == query["milestone"][0]]
        return rows


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.plan = example_plan()
        self.client = ReadinessGitHub()
        sync_plan(self.plan, self.client, apply=True)
        for issue in self.client.issues:
            if "-GATE]" not in issue["title"]:
                issue["state"] = "closed"

    def check(self, release=0):
        return readiness(self.plan, self.plan["releases"][release], self.client)

    def test_ready_is_read_only_and_returns_exact_gate(self):
        writes = len(self.client.writes)
        result = self.check()
        self.assertEqual(result, {"gate_issue": self.client.issue("R01-GATE")["number"], "milestone_number": 1})
        self.assertEqual(len(self.client.writes), writes)

    def test_open_functional_task_blocks(self):
        self.client.issue("R01-01")["state"] = "open"
        with self.assertRaisesRegex(ValueError, "R01-01"):
            self.check()

    def test_open_human_issue_without_marker_blocks(self):
        self.client.issues.append({"number": 88, "title": "Corrupção identificada", "body": "Relato humano", "state": "open", "milestone": {"number": 1}})
        with self.assertRaisesRegex(ValueError, "bloqueios abertos"):
            self.check()

    def test_open_pr_in_milestone_does_not_count_as_extra_issue(self):
        self.client.issues.append({"number": 89, "title": "PR de release", "body": "", "state": "open", "milestone": {"number": 1}, "pull_request": {"url": "pr"}})
        self.check()

    def test_preceding_gate_requires_final_publication(self):
        self.client.issue("R01-GATE")["state"] = "closed"
        with self.assertRaisesRegex(ValueError, "anterior ainda não publicada"):
            self.check(1)
        self.client.releases = [{"tag_name": "v0.1.0", "draft": False, "prerelease": True}]
        with self.assertRaisesRegex(ValueError, "anterior ainda não publicada"):
            self.check(1)
        self.client.releases[0]["prerelease"] = False
        self.check(1)

    def test_wrong_gate_milestone_blocks(self):
        self.client.issue("R01-GATE")["milestone"] = {"number": 2}
        with self.assertRaisesRegex(ValueError, "milestone incorreto"):
            self.check()

    def test_public_repository_blocks(self):
        self.client.private = False
        with self.assertRaisesRegex(ValueError, "privado"):
            self.check()

    def test_duplicate_issue_identity_blocks(self):
        duplicate = copy.deepcopy(self.client.issue("R01-01"))
        duplicate["number"] = 90
        self.client.issues.append(duplicate)
        with self.assertRaises(ValueError):
            self.check()

    def test_multiple_task_markers_cannot_share_one_closed_issue(self):
        gate = self.client.issue("R01-GATE")
        self.client.issues.remove(gate)
        self.client.issue("R01-01")["body"] += "\n<!-- sider:task R01-GATE -->"
        with self.assertRaises(ValueError):
            self.check()


if __name__ == "__main__":
    unittest.main()
