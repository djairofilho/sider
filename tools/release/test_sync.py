"""Reconciliação testada com API em memória, sem escrever no GitHub."""

import copy
import unittest
from unittest.mock import patch

from .github import GitHubError
from .sync import START, sync_plan


SHA = "a" * 40


def example_plan():
    releases = []
    for number in (1, 2):
        identifier = f"R{number:02}"
        releases.append({
            "id": identifier, "version": f"0.{number}.0", "title": f"Versão {number}",
            "depends_on": ["R01"] if number == 2 else [], "scope": ["Escopo de teste"],
            "required_gates": ["native", "compatibility", "fuzz", "tcp_smoke"],
            "tasks": [{"id": f"{identifier}-01", "title": "Implementar operação", "area": "storage",
                       "objective": "Comportamento binário reproduzível.",
                       "deliverables": ["Implementação com dados binários"], "tests": ["Regressão de limite"],
                       "acceptance": ["Estado e resposta esperados"],
                       "depends_on": ["B00-01"] if number == 1 else []}],
            "gate": {"id": f"{identifier}-GATE", "title": "Validar e publicar", "acceptance": ["Publicação confirmada"]},
        })
    return {"schema_version": 1, "repository": "djairofilho/sider", "releases": releases,
            "contracts": {"decisions": ["Dados binários"], "after_1_0": ["Cluster"],
                          "sources": ["https://redis.io/"], "commands_added": {}},
            "release_policy": {"private": True, "publish_crate": False, "candidate_required": True,
                               "merge_strategy": "merge", "release_branch_prefix": "chore/release-v",
                               "release_label": "type:release", "linux_runner": "ubuntu-24.04",
                               "docker_since": "0.10.0", "patch_requires_manifest_entry": True,
                               "functional_change_requires_new_candidate": True,
                               "candidate_fuzz_seconds": 900, "stable_soak_seconds": 3600,
                               "targets": ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"]},
            "reference": {"redis_version": "8.10.1", "redis_cli_version": "8.10.1", "platform": "linux/amd64",
                          "image": "redis:8.10.1@sha256:" + "b" * 64},
            "bootstrap": [{"id": "B00-01", "title": "Fundação", "status": "completed",
                           "evidence": [f"https://github.com/djairofilho/sider/commit/{SHA}",
                                        "https://github.com/djairofilho/sider/actions/runs/7"]}]}


class FakeGitHub:
    repo = "djairofilho/sider"

    def __init__(self):
        self.private = True
        self.milestones = []
        self.issues = []
        self.labels = []
        self.calls = []
        self.run = {"status": "completed", "conclusion": "success", "head_sha": SHA}

    def repo_path(self, suffix):
        return f"/repos/{self.repo}{suffix}"

    def paginate(self, path):
        self.calls.append(("GET", path, None))
        category = path.removeprefix(self.repo_path("/")).split("?", 1)[0]
        return copy.deepcopy(getattr(self, category))

    def request(self, method, path, body=None):
        self.calls.append((method, path, copy.deepcopy(body)))
        suffix = path.removeprefix(self.repo_path(""))
        if method == "GET":
            if not suffix:
                return {"private": self.private}
            if suffix.startswith("/commits/"):
                return {"sha": SHA}
            if suffix.startswith("/actions/runs/"):
                return copy.deepcopy(self.run)
        if method == "POST" and suffix == "/labels":
            self.labels.append(copy.deepcopy(body))
            return copy.deepcopy(body)
        if method == "POST" and suffix == "/milestones":
            milestone = {**copy.deepcopy(body), "number": len(self.milestones) + 1}
            self.milestones.append(milestone)
            return copy.deepcopy(milestone)
        if method == "POST" and suffix == "/issues":
            issue = {**copy.deepcopy(body), "number": len(self.issues) + 1, "state": "open", "comments": []}
            issue["html_url"] = f"https://github.com/{self.repo}/issues/{issue['number']}"
            issue["milestone"] = {"number": body["milestone"]}
            issue["labels"] = [{"name": name} for name in body["labels"]]
            self.issues.append(issue)
            return copy.deepcopy(issue)
        if method == "PATCH":
            category, number = suffix.strip("/").split("/")
            rows = getattr(self, category)
            row = next(row for row in rows if row["number"] == int(number))
            for key, value in body.items():
                if key == "milestone":
                    row[key] = {"number": value}
                elif key == "labels":
                    row[key] = [{"name": name} for name in value]
                else:
                    row[key] = copy.deepcopy(value)
            return copy.deepcopy(row)
        raise AssertionError((method, path, body))

    @property
    def writes(self):
        return [call for call in self.calls if call[0] != "GET"]

    def issue(self, identifier):
        return next(issue for issue in self.issues if f"<!-- sider:task {identifier} -->" in issue["body"])


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.plan = example_plan()
        self.client = FakeGitHub()

    def apply(self):
        return sync_plan(self.plan, self.client, apply=True)

    def test_dry_run_reports_complete_work_without_mutation(self):
        report = sync_plan(self.plan, self.client)
        self.assertGreater(report["total_changes"], 0)
        self.assertEqual(report["counts"]["milestone_created"], 2)
        self.assertEqual(report["counts"]["issue_created"], 5)
        self.assertEqual(report["counts"]["bootstrap_closed"], 1)
        self.assertFalse(self.client.writes)
        self.assertFalse(self.client.issues)
        self.assertEqual(len([call for call in self.client.calls if "per_page=100" in call[1]]), 3)

    def test_second_apply_has_zero_changes_and_no_duplicate_ids(self):
        self.apply()
        initial_writes = len(self.client.writes)
        report = self.apply()
        self.assertEqual(report["total_changes"], 0)
        self.assertEqual(report["counts"], {})
        self.assertEqual(len(self.client.writes), initial_writes)
        self.assertEqual(len(self.client.milestones), 2)
        self.assertEqual(len(self.client.issues), 5)
        self.assertEqual(self.client.issue("B00-01")["state"], "closed")
        self.assertEqual(self.client.issue("R01-01")["state"], "open")
        self.assertEqual(self.client.issue("R01-GATE")["state"], "open")

    def test_human_text_comments_and_labels_survive_managed_update(self):
        self.apply()
        issue = self.client.issue("R01-01")
        before, after = "Nota humana: revisão técnica.\n\n", "\n\nNão apagar este checklist."
        issue["body"] = before + issue["body"] + after
        issue["comments"] = ["Discussão humana intacta"]
        issue["labels"].append({"name": "priority:high"})
        self.plan["releases"][0]["tasks"][0]["objective"] = "Novo objetivo com acentuação."
        self.apply()
        updated = self.client.issue("R01-01")
        self.assertTrue(updated["body"].startswith(before))
        self.assertTrue(updated["body"].endswith(after))
        self.assertIn("Novo objetivo com acentuação.", updated["body"])
        self.assertEqual(updated["comments"], ["Discussão humana intacta"])
        self.assertIn({"name": "priority:high"}, updated["labels"])
        self.assertFalse(any("/comments" in call[1] for call in self.client.calls))

    def test_dependency_links_and_blocking_follow_real_issue_state(self):
        self.apply()
        task = self.client.issue("R01-01")
        gate = self.client.issue("R01-GATE")
        later = self.client.issue("R02-01")
        self.assertNotIn({"name": "status:blocked"}, task["labels"])
        self.assertIn({"name": "status:blocked"}, gate["labels"])
        self.assertIn({"name": "status:blocked"}, later["labels"])
        self.assertIn(f"[R01-GATE]({gate['html_url']})", later["body"])
        task["state"] = "closed"
        self.apply()
        self.assertNotIn({"name": "status:blocked"}, gate["labels"])
        self.assertEqual(gate["state"], "open")
        self.assertEqual(self.client.milestones[0]["state"], "open")
        gate["state"] = "closed"
        self.apply()
        self.assertNotIn({"name": "status:blocked"}, later["labels"])
        self.assertEqual(task["state"], "closed")

    def test_duplicate_marker_rejected_before_writes(self):
        self.apply()
        duplicate = copy.deepcopy(self.client.issue("R01-01"))
        duplicate["number"] = 77
        self.client.issues.append(duplicate)
        count = len(self.client.writes)
        with self.assertRaisesRegex(ValueError, "duplicado"):
            self.apply()
        self.assertEqual(len(self.client.writes), count)

    def test_ambiguous_block_rejected_without_mutation(self):
        self.apply()
        self.client.issue("R01-01")["body"] += "\n" + START
        count = len(self.client.writes)
        with self.assertRaisesRegex(ValueError, "Bloco gerenciado"):
            self.apply()
        self.assertEqual(len(self.client.writes), count)

    def test_reserved_id_without_marker_is_not_silently_adopted(self):
        self.client.issues.append({"number": 1, "title": "[R01-01] Issue humana", "body": "Texto humano"})
        with self.assertRaisesRegex(ValueError, "reservado"):
            self.apply()
        self.assertFalse(self.client.writes)

    def test_prs_do_not_claim_task_identity(self):
        self.client.issues.append({"number": 9, "title": "[R01-01] PR", "body": "", "pull_request": {"url": "pr"}})
        self.apply()
        self.assertEqual(len(self.client.issues), 6)

    def test_bootstrap_requires_passing_ci_and_matching_commit(self):
        for run in ({"status": "in_progress", "conclusion": None, "head_sha": SHA},
                    {"status": "completed", "conclusion": "failure", "head_sha": SHA},
                    {"status": "completed", "conclusion": "success", "head_sha": "b" * 40}):
            with self.subTest(run=run):
                self.client.run = run
                with self.assertRaises(ValueError):
                    self.apply()
                self.assertFalse(self.client.writes)

    def test_bootstrap_rejects_unrecognized_evidence(self):
        self.plan["bootstrap"][0]["evidence"] = ["https://evil.example/claim"]
        with self.assertRaisesRegex(ValueError, "Evidência"):
            self.apply()
        self.assertFalse(self.client.writes)

    def test_duplicate_milestone_and_public_repo_rejected(self):
        self.client.milestones = [{"number": 1, "title": "v0.1.0"}, {"number": 2, "title": "v0.1.0"}]
        with self.assertRaisesRegex(ValueError, "milestone duplicado"):
            self.apply()
        self.client.private = False
        with self.assertRaisesRegex(ValueError, "privado"):
            self.apply()
        self.assertFalse(self.client.writes)

    def test_existing_milestone_human_description_preserved(self):
        self.client.milestones.append({"number": 1, "title": "v0.1.0", "description": "Data acordada em reunião.", "state": "open"})
        self.apply()
        self.assertTrue(self.client.milestones[0]["description"].startswith("Data acordada em reunião.\n\n" + START))
        self.assertEqual(self.apply()["total_changes"], 0)

    def test_lost_create_response_resumes_without_duplicate_issue(self):
        original_request = self.client.request
        failed = False

        def lose_response(method, path, body=None):
            nonlocal failed
            result = original_request(method, path, body)
            if method == "POST" and path.endswith("/issues") and not failed:
                failed = True
                raise GitHubError("Resposta perdida depois da criação")
            return result

        with patch.object(self.client, "request", side_effect=lose_response):
            with self.assertRaises(GitHubError):
                self.apply()
        self.apply()
        self.assertEqual(len(self.client.issues), 5)
        self.assertEqual(self.apply()["total_changes"], 0)

    def test_milestone_id_cannot_change_version_silently(self):
        self.client.milestones.append({"number": 1, "title": "v0.2.0",
                                       "description": START + "\n<!-- sider:release R01 -->\n<!-- sider:managed:end -->"})
        with self.assertRaisesRegex(ValueError, "versão divergente"):
            self.apply()
        self.assertFalse(self.client.writes)


if __name__ == "__main__":
    unittest.main()
