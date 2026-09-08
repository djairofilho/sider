"""Simulações de falhas da publicação; não acessam o GitHub nem criam releases."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

try:
    from .publish import PublicationError, canonical_json, prepare_assets, publish_release, reconcile_published, resolve_tag
except ImportError:
    from publish import PublicationError, canonical_json, prepare_assets, publish_release, reconcile_published, resolve_tag


class ApiError(RuntimeError):
    def __init__(self, status: int = 503):
        self.status = status
        super().__init__(f"HTTP {status}")


class FakeGitHub:
    """Servidor em memória, incluindo respostas perdidas após gravar a alteração."""

    def __init__(self):
        self.private = True
        self.releases = []
        self.tags = {}
        self.annotated_tags = {}
        self.blobs = {}
        self.issues = [{"number": 7, "state": "open", "milestone": {"number": 1}}]
        self.comments = []
        self.milestone = {"number": 1, "state": "open"}
        self.mutations = []
        self.faults = {}
        self.next_asset = 1
        self.starter_failure = False

    def repo_path(self, suffix):
        return "/repos/owner/sider" + ("/" + suffix if suffix else "")

    def fail_once(self, event, *, after=False):
        self.faults[event] = "after" if after else "before"

    def _fault(self, event, point):
        if self.faults.get(event) == point:
            del self.faults[event]
            raise ApiError()

    def _release(self, number):
        return next(release for release in self.releases if release["id"] == number)

    def request(self, method, path, body=None):
        route = path.removeprefix("/repos/owner/sider").lstrip("/")
        if method == "GET" and not route:
            return {"private": self.private}
        if method == "GET" and route.startswith("git/ref/tags/"):
            tag = route.removeprefix("git/ref/tags/")
            if tag not in self.tags:
                raise ApiError(404)
            return {"object": copy.deepcopy(self.tags[tag])}
        if method == "GET" and route.startswith("git/tags/"):
            return {"object": copy.deepcopy(self.annotated_tags[route.split("/")[-1]])}
        if method == "POST" and route == "git/refs":
            self._fault("tag", "before")
            tag = body["ref"].removeprefix("refs/tags/")
            if tag in self.tags:
                raise ApiError(422)
            self.tags[tag] = {"type": "commit", "sha": body["sha"]}
            self.mutations.append(("tag", tag))
            self._fault("tag", "after")
            return {"object": copy.deepcopy(self.tags[tag])}
        if method == "POST" and route == "releases":
            self._fault("create", "before")
            release = dict(body, id=len(self.releases) + 1, assets=[])
            release["upload_url"] = f"https://uploads.github.com/releases/{release['id']}"
            self.releases.append(release)
            self.mutations.append(("create", release["id"]))
            self._fault("create", "after")
            return copy.deepcopy(release)
        if route.startswith("releases/assets/") and method == "DELETE":
            asset_id = int(route.split("/")[-1])
            for release in self.releases:
                release["assets"] = [asset for asset in release["assets"] if asset["id"] != asset_id]
            self.mutations.append(("delete", asset_id))
            return None
        if route.startswith("releases/"):
            release = self._release(int(route.split("/")[1]))
            if method == "GET":
                return copy.deepcopy(release)
            if method == "PATCH":
                self._fault("publish", "before")
                release.update(body)
                self.mutations.append(("publish", release["id"]))
                self._fault("publish", "after")
                return copy.deepcopy(release)
        if route == "issues/7/comments" and method == "POST":
            self._fault("comment", "before")
            comment = dict(body, id=len(self.comments) + 1)
            self.comments.append(comment)
            self.mutations.append(("comment", comment["id"]))
            self._fault("comment", "after")
            return copy.deepcopy(comment)
        if route.startswith("issues/"):
            issue = next(issue for issue in self.issues if issue["number"] == int(route.split("/")[-1]))
            if method == "GET":
                return copy.deepcopy(issue)
            if method == "PATCH":
                self._fault("close_issue", "before")
                issue.update(body)
                self.mutations.append(("close_issue", issue["number"]))
                self._fault("close_issue", "after")
                return copy.deepcopy(issue)
        if route.startswith("milestones/"):
            if method == "GET":
                return copy.deepcopy(self.milestone)
            if method == "PATCH":
                self._fault("close_milestone", "before")
                self.milestone.update(body)
                self.mutations.append(("close_milestone", self.milestone["number"]))
                self._fault("close_milestone", "after")
                return copy.deepcopy(self.milestone)
        raise AssertionError((method, path, body))

    def paginate(self, path):
        route = path.removeprefix("/repos/owner/sider/")
        if route == "releases":
            return copy.deepcopy(self.releases)
        if route.startswith("releases/") and route.endswith("/assets"):
            return copy.deepcopy(self._release(int(route.split("/")[1]))["assets"])
        if route == "issues?milestone=1&state=open":
            return copy.deepcopy([issue for issue in self.issues if issue["state"] == "open"])
        if route == "issues/7/comments":
            return copy.deepcopy(self.comments)
        raise AssertionError(path)

    def upload(self, upload_url, name, data, content_type):
        self._fault("upload", "before")
        release = self._release(int(upload_url.split("/")[-1]))
        asset_id = self.next_asset
        self.next_asset += 1
        asset = {
            "id": asset_id, "name": name, "size": len(data), "state": "uploaded",
            "url": f"https://api.github.com/repos/owner/sider/releases/assets/{asset_id}",
        }
        if self.starter_failure:
            asset.update(state="starter", size=0)
        release["assets"].append(asset)
        self.blobs[asset["url"]] = data
        self.mutations.append(("upload", name))
        if self.starter_failure:
            self.starter_failure = False
            raise ApiError()
        self._fault("upload", "after")
        return copy.deepcopy(asset)

    def download(self, url):
        return self.blobs[url]


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.client = FakeGitHub()
        self.manifest = {
            "schema_version": 1, "version": "0.1.0-rc.1", "sha": "a" * 40,
            "toolchain": "1.97.1", "required_gates": ["ci-linux", "ci-windows", "fuzz"],
            "gates": [{"id": name, "status": "success", "sha": "a" * 40, "duration_seconds": 900}
                      for name in ["ci-linux", "ci-windows", "fuzz"]],
            "artifacts": [], "source_runs": [123],
        }
        for name, target in [("sider-linux.tar.gz", "x86_64-unknown-linux-gnu"), ("sider-windows.zip", "x86_64-pc-windows-msvc")]:
            data = f"fixture:{target}".encode()
            (self.directory / name).write_bytes(data)
            self.manifest["artifacts"].append({"name": name, "target": target, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
        self.notes = "# Sider 0.1\n\nValidação de publicação com acentuação.\n"

    def publish(self, manifest=None):
        return publish_release(self.client, manifest or self.manifest, self.directory, self.notes, gate_issue=7, milestone_number=1)

    def final_manifest(self):
        self.publish()
        manifest = copy.deepcopy(self.manifest)
        manifest.update(version="0.1.0", candidate={"tag": "v0.1.0-rc.1", "sha": "a" * 40})
        return manifest

    def test_pure_preparation_produces_deterministic_checksums_and_utf8(self):
        assets = prepare_assets(self.manifest, self.directory, self.notes)
        self.assertEqual(assets["release-manifest.json"], canonical_json(self.manifest))
        for line in assets["SHA256SUMS"].decode().splitlines():
            digest, name = line.split("  ")
            self.assertEqual(digest, hashlib.sha256(assets[name]).hexdigest())
        self.assertEqual(self.client.mutations, [])
        self.assertEqual(len(list(self.directory.iterdir())), 2)

    def test_rc_published_once_keeps_tracking_open(self):
        release = self.publish()
        self.assertFalse(release["draft"])
        self.assertTrue(release["prerelease"])
        self.assertEqual(release["make_latest"], "false")
        self.assertEqual(self.client.issues[0]["state"], "open")
        self.assertEqual(self.client.milestone["state"], "open")
        before = list(self.client.mutations)
        self.publish()
        self.assertEqual(self.client.mutations, before)
        self.assertEqual(len(release["assets"]), 5)
        self.assertEqual(len(self.client.comments), 1)

    def test_final_closes_issue_and_milestone_only_after_publication(self):
        release = self.publish(self.final_manifest())
        self.assertFalse(release["prerelease"])
        self.assertEqual(release["make_latest"], "legacy")
        events = [event for event, _ in self.client.mutations]
        self.assertEqual(events[-4:], ["publish", "comment", "close_issue", "close_milestone"])
        self.assertEqual(self.client.milestone["state"], "closed")

    def test_response_lost_after_each_remote_write_is_reconciled(self):
        for event in ["tag", "create", "upload", "publish", "comment"]:
            with self.subTest(event=event):
                self.client = FakeGitHub()
                self.client.fail_once(event, after=True)
                self.assertFalse(self.publish()["draft"])
                self.assertEqual(len(self.client.releases), 1)
                self.assertEqual(len(self.client.releases[0]["assets"]), 5)

    def test_failure_before_create_upload_or_publish_resumes_without_duplicates(self):
        for event in ["create", "upload", "publish"]:
            with self.subTest(event=event):
                self.client = FakeGitHub()
                self.client.fail_once(event)
                with self.assertRaises(ApiError):
                    self.publish()
                self.assertFalse(self.publish()["draft"])
                self.assertEqual(len(self.client.releases), 1)
                self.assertEqual(len(self.client.releases[0]["assets"]), 5)

    def test_empty_starter_upload_is_removed_only_from_draft(self):
        self.client.starter_failure = True
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertTrue(self.client.releases[0]["draft"])
        self.publish()
        self.assertEqual(sum(event == "delete" for event, _ in self.client.mutations), 1)

    def test_tracking_failure_retries_without_republishing(self):
        manifest = self.final_manifest()
        self.client.fail_once("close_milestone")
        with self.assertRaises(ApiError):
            self.publish(manifest)
        publications = sum(event == "publish" for event, _ in self.client.mutations)
        self.assertEqual(self.client.issues[0]["state"], "closed")
        self.publish(manifest)
        self.assertEqual(sum(event == "publish" for event, _ in self.client.mutations), publications)
        self.assertEqual(self.client.milestone["state"], "closed")

    def test_lost_issue_close_response_retries_without_republishing(self):
        manifest = self.final_manifest()
        self.client.fail_once("close_issue", after=True)
        with self.assertRaises(ApiError):
            self.publish(manifest)
        self.publish(manifest)
        self.assertEqual(sum(event == "publish" for event, _ in self.client.mutations), 2)

    def test_gate_missing_failed_cancelled_skipped_or_wrong_sha_prevents_all_writes(self):
        mutations = [lambda m: m["gates"].pop(), lambda m: m.update(required_gates=[])]
        mutations += [lambda m, status=status: m["gates"][0].update(status=status) for status in ["failure", "cancelled", "skipped"]]
        mutations.append(lambda m: m["gates"][0].update(sha="b" * 40))
        mutations.append(lambda m: m["gates"][-1].update(duration_seconds=899))
        mutations.append(lambda m: m["gates"].append(copy.deepcopy(m["gates"][0])))
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                manifest = copy.deepcopy(self.manifest)
                mutate(manifest)
                with self.assertRaises(PublicationError):
                    self.publish(manifest)
                self.assertEqual(self.client.mutations, [])

    def test_invalid_artifact_metadata_or_content_prevents_all_writes(self):
        for changed in [{"name": "../escape"}, {"name": "SHA256SUMS"}, {"size": 999}, {"sha256": "b" * 64}, {"target": "unsupported"}]:
            with self.subTest(changed=changed):
                manifest = copy.deepcopy(self.manifest)
                manifest["artifacts"][0].update(changed)
                with self.assertRaises(PublicationError):
                    self.publish(manifest)
                self.assertEqual(self.client.mutations, [])

    def test_docker_archive_required_since_010(self):
        self.manifest["version"] = "0.10.0-rc.1"
        with self.assertRaisesRegex(PublicationError, "Docker"):
            self.publish()
        self.assertEqual(self.client.mutations, [])

    def test_recovery_and_migration_gates_require_both_platforms(self):
        for gate_id in ["native", "tcp_smoke", "crash", "recovery", "migration"]:
            with self.subTest(gate=gate_id):
                manifest = copy.deepcopy(self.manifest)
                manifest["required_gates"].append(gate_id)
                manifest["gates"].append({"id": gate_id, "sha": manifest["sha"], "status": "success", "targets": ["x86_64-unknown-linux-gnu"]})
                with self.assertRaisesRegex(PublicationError, "Linux e Windows"):
                    self.publish(manifest)
                self.assertEqual(self.client.mutations, [])

    def test_bad_version_and_final_without_candidate_are_blocked(self):
        for version in ["0.0.0", "01.1.0", "0.1.0-rc.0", "0.1.0+build", "0.1.0"]:
            with self.subTest(version=version):
                self.manifest["version"] = version
                with self.assertRaises(PublicationError):
                    self.publish()
                self.assertEqual(self.client.mutations, [])

    def test_public_repository_or_incomplete_milestone_is_blocked(self):
        self.client.private = False
        with self.assertRaises(PublicationError):
            self.publish()
        self.client.private = True
        self.client.issues.append({"number": 8, "state": "open", "milestone": {"number": 1}})
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, [])

    def test_existing_tag_different_sha_is_not_replaced(self):
        self.client.tags["v0.1.0-rc.1"] = {"type": "commit", "sha": "b" * 40}
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, [])

    def test_issue_without_milestone_or_pull_request_is_rejected(self):
        for update in [{"milestone": None}, {"pull_request": {"url": "https://example.invalid"}}]:
            with self.subTest(update=update):
                self.client = FakeGitHub()
                self.client.issues[0].update(update)
                with self.assertRaises(PublicationError):
                    self.publish()
                self.assertEqual(self.client.mutations, [])

    def test_published_release_with_deleted_tag_is_not_repaired_automatically(self):
        self.publish()
        self.client.tags.clear()
        before = list(self.client.mutations)
        with self.assertRaisesRegex(PublicationError, "perdeu sua tag"):
            self.publish()
        self.assertEqual(self.client.mutations, before)

    def test_existing_commitish_does_not_hide_changed_tag(self):
        self.publish()
        self.client.tags["v0.1.0-rc.1"] = {"type": "commit", "sha": "b" * 40}
        self.assertEqual(self.client.releases[0]["target_commitish"], "a" * 40)
        before = list(self.client.mutations)
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, before)

    def test_annotated_tag_is_resolved_to_commit(self):
        self.client.tags["v0.1.0-rc.1"] = {"type": "tag", "sha": "c" * 40}
        self.client.annotated_tags["c" * 40] = {"type": "tag", "sha": "d" * 40}
        self.client.annotated_tags["d" * 40] = {"type": "commit", "sha": "a" * 40}
        self.publish()
        self.assertEqual(resolve_tag(self.client, "v0.1.0-rc.1"), "a" * 40)
        self.assertFalse(any(event == "tag" for event, _ in self.client.mutations))

    def test_cyclic_annotated_tag_is_rejected(self):
        self.client.tags["v0.1.0-rc.1"] = {"type": "tag", "sha": "c" * 40}
        self.client.annotated_tags["c" * 40] = {"type": "tag", "sha": "c" * 40}
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, [])

    def test_duplicate_release_or_asset_is_rejected_without_writes(self):
        self.publish()
        before = list(self.client.mutations)
        self.client.releases.append(copy.deepcopy(self.client.releases[0]))
        with self.assertRaises(PublicationError):
            self.publish()
        self.client.releases.pop()
        self.client.releases[0]["assets"].append(copy.deepcopy(self.client.releases[0]["assets"][0]))
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, before)

    def test_mismatched_remote_asset_is_never_replaced(self):
        self.client.fail_once("publish")
        with self.assertRaises(ApiError):
            self.publish()
        self.client.blobs[self.client.releases[0]["assets"][0]["url"]] = b"modified"
        before = list(self.client.mutations)
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, before)

    def test_published_notes_or_manifest_changes_are_not_overwritten(self):
        self.publish()
        before = list(self.client.mutations)
        self.notes += "Alteração.\n"
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, before)
        self.notes = self.client.releases[0]["body"]
        self.manifest["source_runs"] = [456]
        with self.assertRaises(PublicationError):
            self.publish()
        self.assertEqual(self.client.mutations, before)

    def test_final_rejects_unpublished_or_wrong_candidate_manifest(self):
        manifest = self.final_manifest()
        before = list(self.client.mutations)
        self.client.releases[0]["draft"] = True
        with self.assertRaises(PublicationError):
            self.publish(manifest)
        self.client.releases[0]["draft"] = False
        asset = next(asset for asset in self.client.releases[0]["assets"] if asset["name"] == "release-manifest.json")
        candidate = json.loads(self.client.blobs[asset["url"]])
        candidate["sha"] = "b" * 40
        self.client.blobs[asset["url"]] = canonical_json(candidate)
        with self.assertRaises(PublicationError):
            self.publish(manifest)
        self.assertEqual(self.client.mutations, before)

    def test_reconcile_uses_published_artifacts_without_a_new_build(self):
        manifest = self.final_manifest()
        self.client.fail_once("close_milestone")
        with self.assertRaises(ApiError):
            self.publish(manifest)
        for artifact in manifest["artifacts"]:
            (self.directory / artifact["name"]).write_bytes(b"new build is intentionally different")
        before = list(self.client.mutations)
        result = reconcile_published(self.client, "0.1.0", "a" * 40, gate_issue=7, milestone_number=1, required_gates=manifest["required_gates"])
        self.assertFalse(result["draft"])
        self.assertEqual(self.client.mutations[len(before):], [("close_milestone", 1)])

    def test_reconcile_published_rc_is_read_only_after_tracking_record_exists(self):
        self.publish()
        before = list(self.client.mutations)
        reconcile_published(self.client, self.manifest["version"], self.manifest["sha"], gate_issue=7, milestone_number=1, required_gates=self.manifest["required_gates"])
        self.assertEqual(self.client.mutations, before)

    def test_reconcile_blocks_wrong_gates_sha_and_tampered_artifacts(self):
        self.publish()
        before = list(self.client.mutations)
        for sha, gates in [("b" * 40, self.manifest["required_gates"]), ("a" * 40, ["different-gate"])]:
            with self.subTest(sha=sha, gates=gates):
                with self.assertRaises(PublicationError):
                    reconcile_published(self.client, self.manifest["version"], sha, gate_issue=7, milestone_number=1, required_gates=gates)
        for name in ["SHA256SUMS", "release-notes.md", "sider-windows.zip"]:
            with self.subTest(name=name):
                asset = next(asset for asset in self.client.releases[0]["assets"] if asset["name"] == name)
                previous = self.client.blobs[asset["url"]]
                self.client.blobs[asset["url"]] = b"x" * len(previous)
                with self.assertRaises(PublicationError):
                    reconcile_published(self.client, self.manifest["version"], self.manifest["sha"], gate_issue=7, milestone_number=1, required_gates=self.manifest["required_gates"])
                self.client.blobs[asset["url"]] = previous
        self.assertEqual(self.client.mutations, before)

    def test_reconcile_blocks_unsafe_remote_asset_names(self):
        self.publish()
        before = list(self.client.mutations)
        self.client.releases[0]["assets"][0]["name"] = "../escape"
        with self.assertRaises(PublicationError):
            reconcile_published(self.client, self.manifest["version"], self.manifest["sha"], gate_issue=7, milestone_number=1)
        self.assertEqual(self.client.mutations, before)

    def test_reconcile_requires_notes_to_match_versioned_source_when_provided(self):
        self.publish()
        before = list(self.client.mutations)
        with self.assertRaisesRegex(PublicationError, "Notas publicadas"):
            reconcile_published(self.client, self.manifest["version"], self.manifest["sha"], gate_issue=7, milestone_number=1, notes="Notas diferentes.")
        self.assertEqual(self.client.mutations, before)

    def test_human_comments_are_preserved_and_publication_record_is_not_duplicated(self):
        self.client.comments.append({"id": 99, "body": "Minha revisão manual."})
        self.publish()
        self.publish()
        self.assertEqual(len(self.client.comments), 2)
        self.assertEqual(self.client.comments[0]["body"], "Minha revisão manual.")


if __name__ == "__main__":
    unittest.main()
