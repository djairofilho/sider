"""Regressões do manifesto, dependências e geração determinística do roadmap."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

from tools.release.plan import (
    DEFAULT_PLAN, DEFAULT_ROADMAP, load_plan, release_for_version,
    render_roadmap, validate_plan,
)


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.plan = load_plan(DEFAULT_PLAN)

    def test_all_deliveries_are_scheduled_without_claiming_completion(self):
        self.assertEqual(len(self.plan["releases"]), 11)
        self.assertEqual(sum(len(r["tasks"]) for r in self.plan["releases"]), 50)
        self.assertEqual(len(self.plan["bootstrap"]), 1)
        for release in self.plan["releases"]:
            for task in release["tasks"]:
                self.assertNotEqual(task.get("status"), "completed")

    def test_round_trip_and_utf8(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.json"
            path.write_text(json.dumps(self.plan, ensure_ascii=False), encoding="utf-8")
            self.assertEqual(load_plan(path), self.plan)

    def test_generated_roadmap_matches_committed_projection(self):
        self.assertEqual(DEFAULT_ROADMAP.read_text(encoding="utf-8"), render_roadmap(self.plan))

    def test_roadmap_is_deterministic_and_does_not_mutate_source(self):
        before = copy.deepcopy(self.plan)
        self.assertEqual(render_roadmap(self.plan), render_roadmap(self.plan))
        self.assertEqual(before, self.plan)

    def test_automation_is_deferred_without_removing_product_evidence(self):
        policy = self.plan["release_policy"]
        self.assertFalse(policy["ci_enabled"])
        self.assertFalse(policy["automatic_publication"])
        self.assertEqual(policy["automation_resume_after"], "1.0.0")
        text = render_roadmap(self.plan)
        self.assertIn("desativadas até e incluindo a 1.0", text)
        self.assertIn("não dispara publicação", text)
        self.assertIn("verificação manual registrada", text)
        self.assertIn("15 minutos de fuzz", text)
        self.assertIn("uma hora de carga contínua", text)
        self.assertIn("CI multiplataforma", self.plan["bootstrap"][0]["title"])

    def test_only_bootstrap_is_checked_and_accents_are_valid(self):
        text = render_roadmap(self.plan)
        self.assertEqual(text.count("[x]"), 1)
        self.assertIn("Replicação", text)
        for broken in ("\u00c3", "\u00c2", "\ufffd", "\\`", "\x07"):
            self.assertNotIn(broken, text)

    def test_rc_and_final_resolve_same_entry(self):
        expected = self.plan["releases"][0]
        for version in ("0.1.0", "v0.1.0", "0.1.0-rc.1", "v0.1.0-rc.12"):
            with self.subTest(version=version):
                self.assertIs(release_for_version(self.plan, version), expected)

    def test_missing_patch_never_falls_back_to_minor(self):
        for version in ("0.3.1", "0.3.1-rc.1", "2.0.0"):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "sem milestone"):
                release_for_version(self.plan, version)

    def test_patch_resolves_only_explicit_entry(self):
        patch = copy.deepcopy(self.plan["releases"][2])
        patch["version"] = "0.3.1"
        self.plan["releases"].append(patch)
        self.assertIs(release_for_version(self.plan, "0.3.1-rc.2"), patch)

    def test_version_parser_rejects_ambiguous_or_unsupported_forms(self):
        for version in ("0.1", "01.1.0", "0.1.0-rc.0", "0.1.0-rc.01", "0.1.0-beta.1", "0.1.0+meta", " 0.1.0", "0.1.0\n", None):
            with self.subTest(version=version), self.assertRaises(ValueError):
                release_for_version(self.plan, version)

    def test_duplicate_task_id_rejected(self):
        tasks = self.plan["releases"][0]["tasks"]
        tasks[1]["id"] = tasks[0]["id"]
        with self.assertRaisesRegex(ValueError, "ID duplicado"):
            validate_plan(self.plan)

    def test_missing_dependency_rejected(self):
        self.plan["releases"][0]["tasks"][0]["depends_on"] = ["R99-01"]
        with self.assertRaisesRegex(ValueError, "inexistente"):
            validate_plan(self.plan)

    def test_task_dependency_cycle_rejected(self):
        self.plan["releases"][0]["tasks"][0]["depends_on"] = ["R01-02"]
        with self.assertRaisesRegex(ValueError, "Ciclo"):
            validate_plan(self.plan)

    def test_release_dependency_cycle_rejected(self):
        self.plan["releases"][0]["depends_on"] = ["R02"]
        with self.assertRaisesRegex(ValueError, "Ciclo"):
            validate_plan(self.plan)

    def test_dependency_order_rejected_even_without_cycle(self):
        tasks = self.plan["releases"][0]["tasks"]
        tasks[0]["depends_on"] = ["R01-02"]
        tasks[1]["depends_on"] = []
        with self.assertRaisesRegex(ValueError, "ordem de execução"):
            validate_plan(self.plan)

    def test_task_cannot_depend_on_release_identifier(self):
        self.plan["releases"][1]["tasks"][0]["depends_on"] = ["R01"]
        with self.assertRaisesRegex(ValueError, "tarefa deve depender"):
            validate_plan(self.plan)

    def test_gate_cannot_share_task_id(self):
        self.plan["releases"][0]["gate"]["id"] = "R01-01"
        with self.assertRaisesRegex(ValueError, "ID inválido"):
            validate_plan(self.plan)

    def test_missing_malformed_or_duplicate_dependencies_rejected(self):
        for deps in (None, "R01-GATE", ["R01-GATE", "R01-GATE"], [5]):
            plan = copy.deepcopy(self.plan)
            plan["releases"][1]["tasks"][0]["depends_on"] = deps
            with self.subTest(deps=deps), self.assertRaises(ValueError):
                validate_plan(plan)

    def test_semantic_version_order_uses_numbers(self):
        validate_plan(self.plan)
        releases = self.plan["releases"]
        releases[1], releases[2] = releases[2], releases[1]
        with self.assertRaisesRegex(ValueError, "ordem semântica"):
            validate_plan(self.plan)

    def test_duplicate_release_version_rejected(self):
        self.plan["releases"][1]["version"] = "0.1.0"
        with self.assertRaisesRegex(ValueError, "Versão duplicada"):
            validate_plan(self.plan)

    def test_release_manifest_cannot_store_prerelease_as_base(self):
        self.plan["releases"][0]["version"] = "0.1.0-rc.1"
        with self.assertRaisesRegex(ValueError, "Versão-base inválida"):
            validate_plan(self.plan)

    def test_missing_required_evidence_gate_rejected(self):
        for index, gate in ((0, "fuzz"), (2, "migration"), (8, "replication"), (10, "soak")):
            plan = copy.deepcopy(self.plan)
            plan["releases"][index]["required_gates"].remove(gate)
            with self.subTest(gate=gate), self.assertRaises(ValueError):
                validate_plan(plan)

    def test_unknown_evidence_gate_rejected(self):
        self.plan["releases"][0]["required_gates"].append("pretend_pass")
        with self.assertRaisesRegex(ValueError, "desconhecido"):
            validate_plan(self.plan)

    def test_issue_spec_requires_every_nonempty_section(self):
        for field in ("title", "area", "objective", "deliverables", "tests", "acceptance"):
            plan = copy.deepcopy(self.plan)
            del plan["releases"][0]["tasks"][0][field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_plan(plan)

    def test_bootstrap_cannot_claim_completion_without_evidence(self):
        self.plan["bootstrap"][0]["evidence"] = []
        with self.assertRaisesRegex(ValueError, "evidence"):
            validate_plan(self.plan)

    def test_schema_version_and_record_types(self):
        for plan in ([], {}, {"schema_version": True}, {"schema_version": 2}):
            with self.subTest(plan=plan), self.assertRaises(ValueError):
                validate_plan(plan)

    def test_redis_reference_must_be_pinned_and_consistent(self):
        for field, value in (("image", "redis:latest"), ("redis_cli_version", "8.10.0"), ("redis_version", "8.10.0")):
            plan = copy.deepcopy(self.plan)
            plan["reference"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_plan(plan)

    def test_fixed_privacy_and_release_policies_cannot_silently_drift(self):
        for field, value in (("private", False), ("publish_crate", True), ("candidate_required", False), ("candidate_fuzz_seconds", 899), ("stable_soak_seconds", 3599), ("targets", []), ("ci_enabled", True), ("automatic_publication", True), ("automation_resume_after", "0.10.0")):
            plan = copy.deepcopy(self.plan)
            plan["release_policy"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_plan(plan)


if __name__ == "__main__":
    unittest.main()
