"""Prepare a release PR only after its implementation backlog is complete."""

from __future__ import annotations

import re
from pathlib import Path

from tools.release.policy import candidate_for, git, readiness, validate_version_files, version_key
from tools.release.runner import run


def version_edits(root, version, notes):
    """Pure preview of the only four files a release preparation may change."""
    root = Path(root)
    version_key(version)
    if not notes.startswith(f"# Sider v{version}\n") or "<!-- pending -->" in notes:
        raise ValueError("Notas devem começar com o título da versão e estar concluídas")
    cargo = (root / "Cargo.toml").read_text(encoding="utf-8")
    cargo, count = re.subn(r'(?m)^version = "[^"]+"$', f'version = "{version}"', cargo, count=1)
    if count != 1:
        raise ValueError("Versão do pacote não encontrada")
    lock = (root / "Cargo.lock").read_text(encoding="utf-8")
    lock, count = re.subn(r'(\[\[package\]\]\nname = "sider"\nversion = ")[^"]+("\n)',
                          lambda match: match[1] + version + match[2], lock)
    if count != 1:
        raise ValueError("Pacote sider não encontrado no lockfile")
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    if f"## [{version}]" in changelog:
        raise ValueError("Changelog já contém esta versão; retome a branch existente")
    if "## [Unreleased]\n" not in changelog:
        raise ValueError("Changelog sem seção Unreleased")
    changelog = changelog.replace("## [Unreleased]\n", f"## [Unreleased]\n\n## [{version}]\n\n" + notes.split("\n", 1)[1].strip() + "\n", 1)
    return {"Cargo.toml": cargo, "Cargo.lock": lock, "CHANGELOG.md": changelog,
            f"releases/notes/v{version}.md": notes}


def prepare_release(root, plan, release, version, notes, client, *, apply=False):
    root = Path(root).resolve()
    version_key(version)
    tracking = readiness(plan, release, client)
    sha = git("rev-parse", "HEAD", cwd=root)
    if apply:
        if git("status", "--porcelain", cwd=root):
            raise ValueError("A preparação exige worktree limpa")
        if git("branch", "--show-current", cwd=root) != "main":
            raise ValueError("Atualize main e execute a preparação a partir dela")
        remote = git("remote", "get-url", "origin", cwd=root)
        if remote not in {f"https://github.com/{client.repo}.git", f"git@github.com:{client.repo}.git"}:
            raise ValueError("origin diverge do repositório privado do manifesto")
        git("fetch", "origin", "main", "--tags", cwd=root)
        if git("rev-parse", "origin/main", cwd=root) != sha:
            raise ValueError("main local não corresponde a origin/main")
    candidate_for(version, sha, client, root)
    branch = f"chore/release-v{version}"
    edits = version_edits(root, version, notes)
    result = {"mode": "apply" if apply else "dry-run", "branch": branch,
              "files": list(edits), **tracking}
    if not apply:
        return result
    if git("branch", "--list", branch, cwd=root) or git("ls-remote", "--heads", "origin", branch, cwd=root):
        raise ValueError("Branch de release já existe; retome o PR existente")
    run(["git", "switch", "-c", branch], cwd=root)
    for filename, content in edits.items():
        path = root / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
    # Cargo validates the mechanically updated lock without refreshing dependencies.
    run(["cargo", "metadata", "--locked", "--offline", "--format-version", "1", "--no-deps"], cwd=root)
    validate_version_files(root, version)
    run(["git", "add", "--", *edits], cwd=root)
    run(["git", "commit", "-m", f"chore(release): prepare v{version}"], cwd=root)
    run(["git", "push", "-u", "origin", branch], cwd=root)
    body = (f"Preparação de `v{version}`.\n\n"
            f"Relacionada à issue #{tracking['gate_issue']}. Não fecha o milestone no merge.\n\n"
            "A publicação exige preflight, testes cumulativos, artefatos extraídos e checksums. "
            "Integrar por merge commit com a conta do usuário, após validação manual. "
            "CI e publicação automática estão adiadas até depois da 1.0; o merge não publica a release.\n")
    pr = client.request("POST", client.repo_path("/pulls"), {
        "title": f"chore(release): prepare v{version}", "head": branch, "base": "main", "body": body,
    })
    client.request("POST", client.repo_path(f"/issues/{pr['number']}/labels"), {"labels": ["type:release"]})
    result["pull_request"] = pr["html_url"]
    return result
