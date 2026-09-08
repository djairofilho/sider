"""Reconciliação idempotente do manifesto com milestones e issues do GitHub."""

from __future__ import annotations

import argparse
from collections import Counter
import copy
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote

from .github import GitHubClient, GitHubError
from .plan import DEFAULT_PLAN, load_plan, validate_plan


START = "<!-- sider:managed:start -->"
END = "<!-- sider:managed:end -->"
TASK_MARKER = re.compile(r"<!-- sider:task ([A-Z][A-Z0-9-]*) -->")
RELEASE_MARKER = re.compile(r"<!-- sider:release (R[0-9]{2}) -->")
MANAGED_TYPES = {"type:task", "type:release", "type:bootstrap"}


def managed_body(existing: str | None, generated: str) -> str:
    """Atualiza só o bloco gerenciado, preservando literalmente o texto humano."""
    body = existing or ""
    if START not in body and END not in body:
        return body + ("\n\n" if body else "") + generated
    if body.count(START) != 1 or body.count(END) != 1:
        raise ValueError("Bloco gerenciado ausente, duplicado ou ambíguo")
    first, last = body.index(START), body.index(END)
    if first > last:
        raise ValueError("Bloco gerenciado fora de ordem")
    return body[:first] + generated + body[last + len(END):]


def _index_issues(issues: list[dict]) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for issue in issues:
        if "pull_request" in issue:
            continue
        body = issue.get("body") or ""
        markers = TASK_MARKER.findall(body)
        if "sider:task" in body and len(markers) != 1:
            raise ValueError(f"Marcador de tarefa ambíguo na issue #{issue['number']}")
        if not markers:
            continue
        task_id = markers[0]
        if task_id in indexed:
            raise ValueError(f"Identificador duplicado no GitHub: {task_id}")
        if body.count(START) != 1 or body.count(END) != 1:
            raise ValueError(f"Bloco gerenciado inválido na issue #{issue['number']}")
        managed_body(body, START + END)
        indexed[task_id] = issue
    return indexed


def _label_names(issue: dict) -> list[str]:
    return [label if isinstance(label, str) else label["name"] for label in issue.get("labels", [])]


def _prove_bootstrap(item: dict, client: Any, cache: dict[str, Any]) -> None:
    """Exige commits existentes e CI concluída no SHA de um desses commits."""
    commit_shas: set[str] = set()
    runs: list[dict] = []
    prefix = re.escape(f"https://github.com/{client.repo}/")
    for evidence in item.get("evidence", []):
        commit = re.fullmatch(prefix + r"commit/([0-9a-f]{7,40})", evidence)
        run = re.fullmatch(prefix + r"actions/runs/([0-9]+)", evidence)
        if not commit and not run:
            raise ValueError(f"Evidência bootstrap não reconhecida: {item['id']}")
        suffix = f"/commits/{commit[1]}" if commit else f"/actions/runs/{run[1]}"
        if suffix not in cache:
            cache[suffix] = client.request("GET", client.repo_path(suffix))
        result = cache[suffix]
        if commit:
            sha = result.get("sha", "")
            if not sha.startswith(commit[1]):
                raise ValueError(f"SHA de evidência divergente: {item['id']}")
            commit_shas.add(sha)
        else:
            if result.get("status") != "completed" or result.get("conclusion") != "success":
                raise ValueError(f"CI bootstrap não aprovada: {item['id']}")
            runs.append(result)
    if not commit_shas or not runs or any(run.get("head_sha") not in commit_shas for run in runs):
        raise ValueError(f"Bootstrap exige commit e CI aprovada no mesmo SHA: {item['id']}")


def _descriptors(plan: dict) -> list[dict]:
    result = []
    first_version = plan["releases"][0]["version"]
    release_ids = {release["id"]: release for release in plan["releases"]}
    for bootstrap in plan.get("bootstrap", []):
        result.append({**bootstrap, "kind": "bootstrap", "version": first_version,
                       "area": "foundation", "depends_on": []})
    for release in plan["releases"]:
        previous = [release_ids[dependency]["gate"]["id"] for dependency in release.get("depends_on", [])]
        for task in release["tasks"]:
            result.append({**task, "kind": "task", "version": release["version"],
                           "depends_on": list(dict.fromkeys(task.get("depends_on", []) + previous))})
        result.append({**release["gate"], "kind": "release", "version": release["version"],
                       "area": "release", "required_gates": release.get("required_gates", []),
                       "depends_on": [task["id"] for task in release["tasks"]] + previous})
    return result


def _paragraph_list(values: list[str]) -> str:
    return "\n".join(f"- {value}" for value in values) or "- Nenhum."


def _render(item: dict, issues: dict[str, dict], repo: str) -> str:
    lines = [START, f"<!-- sider:task {item['id']} -->", "", "## Objetivo", "",
             item.get("objective") or item["title"], "", f"Versão: `v{item['version']}`."]
    if item["kind"] == "bootstrap":
        lines += ["", "## Entregáveis", "", "- Fundação existente registrada pelos commits vinculados.",
                  "", "## Testes exigidos", "", "- CI concluída com sucesso no SHA de um commit vinculado.",
                  "", "## Evidências do bootstrap", "", _paragraph_list(item.get("evidence", []))]
    elif item["kind"] == "release":
        lines += ["", "## Entregáveis", "", "- Publicar uma candidata e a versão final com evidências e artefatos verificados.",
                  "- Até e incluindo a 1.0, verificar e publicar manualmente; CI e publicação automática ficam para depois da 1.0.",
                  "- Manter esta issue e o milestone abertos até a publicação final confirmada.",
                  "", "## Testes exigidos", "", _paragraph_list(item.get("required_gates", []))]
    else:
        lines += ["", "## Entregáveis", "", _paragraph_list(item["deliverables"]),
                  "", "## Testes exigidos", "", _paragraph_list(item["tests"])]
    lines += ["", "## Dependências", ""]
    if item["depends_on"]:
        for dependency in item["depends_on"]:
            issue = issues.get(dependency, {})
            url = issue.get("html_url") or f"https://github.com/{repo}/issues?q={quote(dependency)}"
            checked = "x" if issue.get("state") == "closed" else " "
            lines.append(f"- [{checked}] [{dependency}]({url})")
    else:
        lines.append("- Nenhuma.")
    lines += ["", "## Critério de conclusão", "", _paragraph_list(item.get("acceptance", [
        "Bootstrap comprovado pelos commits e pela CI vinculada."
    ])), "", END]
    return "\n".join(lines)


def sync_plan(plan: dict, client: Any, apply: bool = False) -> dict:
    """Planeja por padrão; apply=True aplica apenas os campos gerenciados.

    Não fecha tarefas funcionais nem a issue de publicação. O único fechamento
    automático aqui é o bootstrap com evidências verificadas na API.
    """
    validate_plan(plan)
    if plan["repository"] != client.repo:
        raise ValueError("Repositório do cliente diverge do manifesto")
    repository = client.request("GET", client.repo_path(""))
    if not repository.get("private"):
        raise ValueError("O backlog Sider deve permanecer em repositório privado")
    milestones = client.paginate(client.repo_path("/milestones?state=all&per_page=100"))
    raw_issues = client.paginate(client.repo_path("/issues?state=all&per_page=100"))
    existing_labels = client.paginate(client.repo_path("/labels?per_page=100"))
    issues = _index_issues(raw_issues)
    items = _descriptors(plan)
    ids = {item["id"] for item in items}
    for issue in raw_issues:
        if "pull_request" in issue or TASK_MARKER.search(issue.get("body") or ""):
            continue
        title_id = re.match(r"\[([A-Z][A-Z0-9-]*)\]", issue.get("title", ""))
        if title_id and title_id[1] in ids:
            raise ValueError(f"Issue #{issue['number']} usa ID reservado sem marcador gerenciado")
    milestone_index: dict[str, dict] = {}
    release_index: dict[str, str] = {}
    expected_versions = {release["id"]: "v" + release["version"] for release in plan["releases"]}
    for milestone in milestones:
        title = milestone["title"]
        if title in milestone_index:
            raise ValueError(f"Título de milestone duplicado: {title}")
        milestone_index[title] = milestone
        description = milestone.get("description") or ""
        markers = RELEASE_MARKER.findall(description)
        if "sider:release" in description and len(markers) != 1:
            raise ValueError(f"Marcador de milestone ambíguo: {title}")
        if markers:
            identifier = markers[0]
            if identifier in release_index:
                raise ValueError(f"ID de milestone duplicado: {identifier}")
            if identifier in expected_versions and expected_versions[identifier] != title:
                raise ValueError(f"ID de milestone com versão divergente: {identifier}")
            release_index[identifier] = title
            if title in expected_versions.values() and expected_versions.get(identifier) != title:
                raise ValueError(f"Milestone reservado com ID divergente: {title}")
    # Todas as colisões e evidências são verificadas antes da primeira escrita.
    evidence_cache: dict[str, Any] = {}
    for item in items:
        if item["kind"] == "bootstrap" and item.get("status") == "completed":
            _prove_bootstrap(item, client, evidence_cache)
    for release in plan["releases"]:
        milestone = milestone_index.get("v" + release["version"])
        if milestone:
            managed_body(milestone.get("description"), START + END)

    changes: list[dict] = []

    def mutate(kind: str, method: str, path: str, body: dict, identifier: str) -> Any:
        changes.append({"kind": kind, "id": identifier, "method": method, "path": path, "body": body})
        return client.request(method, path, body) if apply else None

    known_labels = {label["name"] for label in existing_labels}
    definitions = {
        "type:task": ("1d76db", "Entrega funcional do plano de releases"),
        "type:release": ("5319e7", "Validação e publicação de uma versão"),
        "type:bootstrap": ("0e8a16", "Fundação já comprovada por commits e CI"),
        "status:blocked": ("d93f0b", "Aguarda uma dependência ainda aberta"),
        "compatibility:breaking": ("b60205", "Mudança incompatível a documentar nas notas"),
    }
    for area in sorted({item["area"] for item in items}):
        definitions["area:" + area] = ("c5def5", f"Área de responsabilidade: {area}")
    for name, (color, description) in definitions.items():
        if name not in known_labels:
            mutate("label_created", "POST", client.repo_path("/labels"),
                   {"name": name, "color": color, "description": description}, name)
    for release in plan["releases"]:
        title = "v" + release["version"]
        milestone = milestone_index.get(title)
        generated = "\n".join([START, f"<!-- sider:release {release['id']} -->", "", release["title"], "",
                                "CI e publicação automática desativadas até e incluindo a 1.0; verificações e publicação manuais.", "",
                                _paragraph_list(release["gate"]["acceptance"]), "", END])
        description = managed_body(milestone.get("description") if milestone else None, generated)
        if milestone is None:
            body = {"title": title, "description": description, "state": "open"}
            created = mutate("milestone_created", "POST", client.repo_path("/milestones"), body, title)
            milestone_index[title] = created or {**body, "number": None}
        elif description != milestone.get("description"):
            mutate("milestone_updated", "PATCH", client.repo_path(f"/milestones/{milestone['number']}"),
                   {"description": description}, title)
            milestone["description"] = description

    def desired_labels(item: dict, issue: dict) -> list[str]:
        human = [name for name in _label_names(issue)
                 if name not in MANAGED_TYPES and name != "status:blocked" and not name.startswith("area:")]
        labels = human + ["type:" + item["kind"], "area:" + item["area"]]
        if any(issues.get(dep, {}).get("state") != "closed" for dep in item["depends_on"]):
            labels.append("status:blocked")
        return sorted(set(labels))

    # Cria todos os IDs antes de resolver os links, inclusive dependências futuras.
    for item in items:
        if item["id"] in issues:
            continue
        milestone_number = milestone_index["v" + item["version"]]["number"]
        body = {"title": f"[{item['id']}] {item['title']}", "body": _render(item, issues, client.repo),
                "labels": desired_labels(item, {}), "milestone": milestone_number}
        created = mutate("issue_created", "POST", client.repo_path("/issues"), body, item["id"])
        issues[item["id"]] = created or {**copy.deepcopy(body), "number": None, "state": "open",
                                        "milestone": {"number": milestone_number}}
    for item in items:
        if item["kind"] != "bootstrap" or item.get("status") != "completed":
            continue
        issue = issues[item["id"]]
        if issue.get("state") != "closed":
            mutate("bootstrap_closed", "PATCH", client.repo_path(f"/issues/{issue['number']}"),
                   {"state": "closed", "state_reason": "completed"}, item["id"])
            issue["state"] = "closed"
    for item in items:
        issue = issues[item["id"]]
        desired = {"title": f"[{item['id']}] {item['title']}",
                   "body": managed_body(issue.get("body"), _render(item, issues, client.repo)),
                   "labels": desired_labels(item, issue),
                   "milestone": milestone_index["v" + item["version"]]["number"]}
        current = {"title": issue.get("title"), "body": issue.get("body"),
                   "labels": sorted(_label_names(issue)),
                   "milestone": (issue.get("milestone") or {}).get("number")}
        update = {key: value for key, value in desired.items() if value != current[key]}
        if update:
            mutate("issue_updated", "PATCH", client.repo_path(f"/issues/{issue['number']}"), update, item["id"])
    return {"apply": apply, "total_changes": len(changes), "counts": dict(Counter(c["kind"] for c in changes)),
            "changes": changes, "issues": {key: issue.get("html_url") for key, issue in issues.items()},
            "milestones": {key: milestone.get("number") for key, milestone in milestone_index.items()}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--apply", action="store_true", help="Aplica o plano; sem esta opção, apenas simula")
    parser.add_argument("--json", action="store_true", help="Exibe todas as alterações previstas em JSON")
    args = parser.parse_args()
    try:
        plan = load_plan(args.plan)
        client = GitHubClient(repo=plan["repository"], allow_writes=args.apply)
        report = sync_plan(plan, client, apply=args.apply)
        output = report if args.json else {key: report[key] for key in ("apply", "total_changes", "counts")}
        print(json.dumps(output, ensure_ascii=False, indent=2))
    except (ValueError, OSError, GitHubError) as error:
        parser.exit(1, f"Erro: {error}\n")


if __name__ == "__main__":
    main()
