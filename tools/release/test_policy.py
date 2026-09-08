import copy
import unittest

from tools.release.policy import TARGETS, event_context, validate_gate_reports, version_key


class PolicyTests(unittest.TestCase):
    def event(self):
        repo = {"full_name": "djairofilho/sider"}
        return {"action": "closed", "repository": repo, "pull_request": {
            "number": 10, "merged": True, "merge_commit_sha": "a" * 40,
            "head": {"ref": "chore/release-v0.1.0-rc.1", "repo": repo},
            "base": {"ref": "main", "repo": repo}, "labels": [{"name": "type:release"}]}}

    def test_merged_release_selects_merge_sha(self):
        self.assertEqual(event_context(self.event(), "djairofilho/sider")["sha"], "a" * 40)

    def test_closed_unmerged_fork_wrong_branch_or_label_rejected(self):
        for alter in (
            lambda e: e["pull_request"].update(merged=False),
            lambda e: e["pull_request"]["head"].update(repo={"full_name": "fork/sider"}),
            lambda e: e["pull_request"]["head"].update(ref="ci/release-lifecycle"),
            lambda e: e["pull_request"].update(labels=[]),
            lambda e: e["pull_request"]["base"].update(ref="other"),
        ):
            event = copy.deepcopy(self.event())
            alter(event)
            with self.assertRaises(ValueError):
                event_context(event, "djairofilho/sider")

    def test_semver_orders_rc_numerically_and_final_last(self):
        versions = ["0.10.0", "0.1.0-rc.10", "0.1.0", "0.1.0-rc.2"]
        self.assertEqual(sorted(versions, key=version_key), ["0.1.0-rc.2", "0.1.0-rc.10", "0.1.0", "0.10.0"])
        for invalid in ("v0.1.0", "0.01.0", "0.1.0-rc.0", "0.1.0;echo hi"):
            with self.assertRaises(ValueError):
                version_key(invalid)

    def test_gates_require_all_platforms_exact_sha_and_duration(self):
        reports = [
            {"id": "native", "status": "success", "sha": "a" * 40, "targets": list(TARGETS)},
            {"id": "fuzz", "status": "success", "sha": "a" * 40, "duration_seconds": 900},
        ]
        release = {"required_gates": ["native", "fuzz"]}
        validate_gate_reports(release, reports, "a" * 40)
        for mutation in (
            lambda x: x.pop(),
            lambda x: x[0].update(status="skipped"),
            lambda x: x[1].update(duration_seconds=899),
            lambda x: x[1].update(sha="b" * 40),
            lambda x: x[0].update(targets=[TARGETS[0]]),
        ):
            altered = copy.deepcopy(reports)
            mutation(altered)
            with self.assertRaises(ValueError):
                validate_gate_reports(release, altered, "a" * 40)


if __name__ == "__main__":
    unittest.main()
