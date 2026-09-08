"""Read-only release eligibility, version and evidence checks."""

from __future__ import annotations

import re
import subprocess
import tomllib
from pathlib import Path

VERSION = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-rc\.([1-9]\d*))?\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
TARGETS = ("x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc")


def version_key(version):
    match = VERSION.fullmatch(version)
    if not match:
        raise ValueError(f"Versão inválida: {version}")
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), 1 if rc is None else 0, int(rc or 0))


def base_version(version):
    version_key(version)
    return version.split("-rc.")[0]


def event_context(event, repository, *, merged=True):
    pr = event.get("pull_request", {})
    head = pr.get("head", {})
    branch = head.get("ref", "")
    prefix = "chore/release-v"
    if (
        event.get("repository", {}).get("full_name") != repository
        or head.get("repo", {}).get("full_name") != repository
        or pr.get("base", {}).get("repo", {}).get("full_name") != repository
        or pr.get("base", {}).get("ref") != "main"
        or not branch.startswith(prefix)
        or "type:release" not in [label.get("name") for label in pr.get("labels", [])]
    ):
        raise ValueError("Evento não pertence a um PR de release elegível")
    if merged and (event.get("action") != "closed" or pr.get("merged") is not True):
        raise ValueError("Publicação exige PR integrado, não apenas fechado")
    version = branch[len(prefix):]
    version_key(version)
    sha = pr.get("merge_commit_sha") if merged else head.get("sha")
    if not isinstance(sha, str) or not SHA.fullmatch(sha):
        raise ValueError("SHA do PR inválido")
    return {"version": version, "sha": sha, "pr": pr["number"]}


def git(*args, cwd=None):
    return subprocess.check_output(["git", *args], cwd=cwd, text=True, encoding="utf-8").strip()


def validate_version_files(root, version):
    version_key(version)
    root = Path(root)
    cargo = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    packages = [p for p in lock["package"] if p["name"] == "sider"]
    if cargo["package"]["version"] != version or len(packages) != 1 or packages[0]["version"] != version:
        raise ValueError("Cargo.toml, Cargo.lock e versão da release divergem")
    if cargo["package"].get("publish") is not False:
        raise ValueError("A publicação no crates.io deve continuar desabilitada")
    notes = root / "releases" / "notes" / f"v{version}.md"
    text = notes.read_text(encoding="utf-8")
    if not text.startswith(f"# Sider v{version}\n") or "<!-- pending -->" in text:
        raise ValueError("Notas de release ausentes, pendentes ou com versão divergente")
    if f"## [{version}]" not in (root / "CHANGELOG.md").read_text(encoding="utf-8"):
        raise ValueError("Versão ausente no changelog")
    return text


def task_index(issues):
    indexed = {}
    for issue in issues:
        if "pull_request" in issue:
            continue
        matches = re.findall(r"<!-- sider:task ([A-Z0-9-]+) -->", issue.get("body") or "")
        if len(matches) > 1:
            raise ValueError("Uma issue não pode representar várias tarefas")
        for task_id in matches:
            if task_id in indexed:
                raise ValueError(f"ID de issue duplicado: {task_id}")
            indexed[task_id] = issue
    return indexed


def readiness(plan, release, client):
    repo = client.request("GET", client.repo_path(""))
    if repo.get("private") is not True:
        raise ValueError("A automação exige repositório privado")
    milestones = [m for m in client.paginate(client.repo_path("/milestones?state=all"))
                  if m["title"] == f"v{release['version']}"]
    if len(milestones) != 1:
        raise ValueError("Milestone ausente ou duplicado; execute sync primeiro")
    indexed = task_index(client.paginate(client.repo_path("/issues?state=all")))
    ids = [task["id"] for task in release["tasks"]]
    ids += [f"{dep}-GATE" for dep in release.get("depends_on", [])]
    blocked = [task_id for task_id in ids if indexed.get(task_id, {}).get("state") != "closed"]
    if blocked:
        raise ValueError("Tarefas ainda não concluídas: " + ", ".join(blocked))
    gate = indexed.get(release["gate"]["id"])
    if not gate or (gate.get("milestone") or {}).get("number") != milestones[0]["number"]:
        raise ValueError("Issue de publicação ausente ou no milestone incorreto")
    published = client.paginate(client.repo_path("/releases"))
    by_id = {r["id"]: r for r in plan["releases"]}
    for dep in release.get("depends_on", []):
        tag = "v" + by_id[dep]["version"]
        if not any(r["tag_name"] == tag and not r["draft"] and not r["prerelease"] for r in published):
            raise ValueError(f"Release anterior ainda não publicada: {tag}")
    # A new blocking issue in this milestone must also prevent publication.
    extras = [i for i in indexed.values() if (i.get("milestone") or {}).get("number") == milestones[0]["number"]
              and i["number"] != gate["number"] and i.get("state") != "closed"]
    if extras:
        raise ValueError("Milestone contém issues abertas")
    all_issues = client.paginate(client.repo_path(f"/issues?state=open&milestone={milestones[0]['number']}"))
    if any(i["number"] != gate["number"] and "pull_request" not in i for i in all_issues):
        raise ValueError("Milestone contém bloqueios abertos fora do manifesto")
    return {"gate_issue": gate["number"], "milestone_number": milestones[0]["number"]}


def candidate_for(version, sha, client, root):
    """Validate SemVer progression and a final's functional equivalence to its RC."""
    releases = [r for r in client.paginate(client.repo_path("/releases")) if not r["draft"]]
    existing = [r for r in releases if r["tag_name"] == "v" + version]
    if not existing:
        for release in releases:
            tag = release["tag_name"].removeprefix("v")
            if VERSION.fullmatch(tag) and base_version(tag) == base_version(version) and version_key(tag) >= version_key(version):
                raise ValueError(f"Versão não avança o histórico publicado: {tag}")
    if "-rc." in version:
        return None
    candidates = [r for r in releases if r["tag_name"].startswith(f"v{version}-rc.") and r["prerelease"]]
    if not candidates:
        raise ValueError("Versão final exige candidata publicada")
    candidate = max(candidates, key=lambda r: version_key(r["tag_name"][1:]))
    candidate_sha = git("rev-list", "-n", "1", candidate["tag_name"], cwd=root)
    git("merge-base", "--is-ancestor", candidate_sha, sha, cwd=root)
    changed = git("diff", "--name-only", candidate_sha, sha, cwd=root).splitlines()
    allowed = {"Cargo.toml", "Cargo.lock", "CHANGELOG.md", f"releases/notes/v{version}.md"}
    if set(changed) - allowed:
        raise ValueError("Mudança funcional após a candidata; publique outra RC")
    for filename in ("Cargo.toml", "Cargo.lock"):
        previous = tomllib.loads(git("show", f"{candidate_sha}:{filename}", cwd=root))
        current = tomllib.loads(git("show", f"{sha}:{filename}", cwd=root))
        if filename == "Cargo.toml":
            previous["package"]["version"] = current["package"]["version"]
        else:
            for document in (previous, current):
                for package in document["package"]:
                    if package["name"] == "sider":
                        package["version"] = "<release-version>"
        if previous != current:
            raise ValueError("Dependências ou contrato de build mudaram após a candidata")
    return {"tag": candidate["tag_name"], "sha": candidate_sha}


def validate_gate_reports(release, reports, sha):
    indexed = {}
    for gate in reports:
        gate_id = gate.get("id")
        if gate_id in indexed:
            raise ValueError(f"Evidência duplicada: {gate_id}")
        if gate.get("status") != "success" or gate.get("sha") != sha:
            raise ValueError(f"Evidência ausente, ignorada ou inválida: {gate_id}")
        if gate_id == "fuzz" and gate.get("duration_seconds", 0) < 900:
            raise ValueError("Fuzz deve executar por pelo menos 900 segundos")
        if gate_id == "soak" and gate.get("duration_seconds", 0) < 3600:
            raise ValueError("Ensaio de carga deve executar por pelo menos 3600 segundos")
        if gate_id in {"native", "tcp_smoke", "crash", "recovery", "migration"} and set(gate.get("targets", [])) != set(TARGETS):
            raise ValueError(f"Gate {gate_id} exige Linux e Windows")
        indexed[gate_id] = gate
    if set(indexed) != set(release["required_gates"]):
        raise ValueError("Evidências não correspondem aos gates cumulativos do milestone")
    return list(indexed.values())
