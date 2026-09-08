"""Manifesto de releases e projeção Markdown, sem dependências externas."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PLAN = ROOT / "releases" / "plan.json"
DEFAULT_ROADMAP = ROOT / "ROADMAP.md"
VERSION = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)")
RELEASE_VERSION = re.compile(r"v?((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))(?:-rc\.([1-9]\d*))?")
GATES = (
    "native", "compatibility", "fuzz", "tcp_smoke", "crash", "recovery",
    "migration", "sharding", "types", "sorted_sets", "transactions",
    "pubsub", "replication", "docker", "soak", "benchmarks",
)


def _strings(value: Any, name: str, *, empty: bool = False) -> list[str]:
    if not isinstance(value, list) or (not value and not empty):
        raise ValueError(f"{name}: lista {'não vazia ' if not empty else ''}obrigatória")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{name}: valores devem ser textos não vazios")
    if len(set(value)) != len(value):
        raise ValueError(f"{name}: valores duplicados")
    return value


def _text(record: dict[str, Any], key: str, context: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}.{key}: texto não vazio obrigatório")
    return value


def load_plan(path: str | Path = DEFAULT_PLAN) -> dict[str, Any]:
    """Lê UTF-8 e rejeita um manifesto inválido antes de qualquer ação externa."""
    plan = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_plan(plan)
    return plan


def validate_plan(plan: dict[str, Any]) -> None:
    """Valida estrutura, IDs, versões, cobertura de gates e ordem topológica."""
    if not isinstance(plan, dict) or type(plan.get("schema_version")) is not int or plan["schema_version"] != 1:
        raise ValueError("schema_version deve ser 1")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", _text(plan, "repository", "plan")):
        raise ValueError("repository deve ter formato owner/repo")
    reference = plan.get("reference")
    if not isinstance(reference, dict):
        raise ValueError("reference obrigatória")
    for field in ("redis_version", "redis_cli_version", "image", "platform"):
        _text(reference, field, "reference")
    if not re.fullmatch(r"redis:[^@]+@sha256:[a-f0-9]{64}", reference["image"]):
        raise ValueError("reference.image deve fixar tag e digest sha256")
    if reference["redis_version"] != reference["redis_cli_version"]:
        raise ValueError("Redis e redis-cli devem usar a mesma versão")
    if not reference["image"].startswith(f"redis:{reference['redis_version']}@"):
        raise ValueError("reference.image diverge da versão Redis")
    contracts = plan.get("contracts")
    if not isinstance(contracts, dict):
        raise ValueError("contracts obrigatório")
    for field in ("decisions", "after_1_0", "sources"):
        _strings(contracts.get(field), f"contracts.{field}")
    commands = contracts.get("commands_added")
    if not isinstance(commands, dict):
        raise ValueError("contracts.commands_added deve ser um objeto")
    for version, forms in commands.items():
        if not isinstance(version, str) or not VERSION.fullmatch(version):
            raise ValueError("contracts.commands_added tem versão inválida")
        _strings(forms, f"commands_added.{version}")
    policy = plan.get("release_policy")
    if not isinstance(policy, dict):
        raise ValueError("release_policy obrigatória")
    expected_policy = {
        "private": True, "publish_crate": False, "candidate_required": True,
        "merge_strategy": "merge", "release_branch_prefix": "chore/release-v",
        "release_label": "type:release", "linux_runner": "ubuntu-24.04",
        "docker_since": "0.10.0", "patch_requires_manifest_entry": True,
        "functional_change_requires_new_candidate": True,
    }
    for field, expected in expected_policy.items():
        if type(policy.get(field)) is not type(expected) or policy[field] != expected:
            raise ValueError(f"release_policy.{field} diverge do contrato suportado")
    for field, minimum in (("candidate_fuzz_seconds", 900), ("stable_soak_seconds", 3600)):
        if type(policy.get(field)) is not int or policy[field] < minimum:
            raise ValueError(f"release_policy.{field} deve ser inteiro >= {minimum}")
    if policy.get("targets") != ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"]:
        raise ValueError("release_policy.targets diverge das plataformas suportadas")
    releases = plan.get("releases")
    bootstrap = plan.get("bootstrap")
    if not isinstance(releases, list) or not releases:
        raise ValueError("releases: lista não vazia obrigatória")
    if not isinstance(bootstrap, list):
        raise ValueError("bootstrap: lista obrigatória")
    nodes: dict[str, list[str]] = {}
    positions: dict[str, int] = {}

    def register(record: Any, pattern: str, dependencies: list[str]) -> str:
        if not isinstance(record, dict):
            raise ValueError("Cada registro deve ser um objeto")
        identifier = _text(record, "id", "registro")
        if not re.fullmatch(pattern, identifier):
            raise ValueError(f"ID inválido: {identifier}")
        if identifier in nodes:
            raise ValueError(f"ID duplicado: {identifier}")
        _text(record, "title", identifier)
        nodes[identifier] = dependencies
        positions[identifier] = len(positions)
        return identifier

    for item in bootstrap:
        identifier = register(item, r"B\d{2}-\d{2}", [])
        if item.get("status") != "completed":
            raise ValueError(f"{identifier}: bootstrap deve conter somente entregas comprovadas")
        _strings(item.get("evidence"), f"{identifier}.evidence")
    previous_version: tuple[int, ...] | None = None
    previous_gates: set[str] = set()
    release_ids: set[str] = set()
    task_ids: set[str] = set()
    gate_ids: set[str] = set()
    versions: set[str] = set()
    for release in releases:
        if not isinstance(release, dict):
            raise ValueError("Cada release deve ser um objeto")
        deps = _strings(release.get("depends_on"), "release.depends_on", empty=True)
        identifier = register(release, r"R\d{2}", deps)
        release_ids.add(identifier)
        version = _text(release, "version", identifier)
        if not VERSION.fullmatch(version):
            raise ValueError(f"Versão-base inválida: {version}")
        if version in versions:
            raise ValueError(f"Versão duplicada: {version}")
        versions.add(version)
        numbers = tuple(map(int, version.split(".")))
        if previous_version is not None and numbers <= previous_version:
            raise ValueError("As releases devem estar em ordem semântica crescente")
        previous_version = numbers
        _strings(release.get("scope"), f"{identifier}.scope")
        required = set(_strings(release.get("required_gates"), f"{identifier}.required_gates"))
        if required - set(GATES):
            raise ValueError(f"{identifier}: gate de evidência desconhecido")
        if not previous_gates <= required or not set(GATES[:4]) <= required:
            raise ValueError(f"{identifier}: gates obrigatórios devem ser cumulativos")
        thresholds = {
            (0, 3, 0): {"crash", "recovery", "migration"},
            (0, 4, 0): {"sharding"}, (0, 5, 0): {"types"},
            (0, 6, 0): {"sorted_sets"}, (0, 7, 0): {"transactions"},
            (0, 8, 0): {"pubsub"}, (0, 9, 0): {"replication"},
            (0, 10, 0): {"docker"}, (1, 0, 0): {"soak", "benchmarks"},
        }
        for threshold, gates in thresholds.items():
            if numbers >= threshold and not gates <= required:
                raise ValueError(f"{identifier}: faltam gates da capacidade {threshold}")
        previous_gates = required
        tasks = release.get("tasks")
        if not isinstance(tasks, list) or not tasks:
            raise ValueError(f"{identifier}.tasks: lista não vazia obrigatória")
        own_tasks = []
        for task in tasks:
            if not isinstance(task, dict):
                raise ValueError(f"{identifier}: tarefa deve ser um objeto")
            task_deps = _strings(task.get("depends_on"), "task.depends_on", empty=True)
            task_id = register(task, rf"{identifier}-\d{{2}}", task_deps)
            task_ids.add(task_id)
            own_tasks.append(task_id)
            for field in ("area", "objective"):
                _text(task, field, task_id)
            for field in ("deliverables", "tests", "acceptance"):
                _strings(task.get(field), f"{task_id}.{field}")
        gate = release.get("gate")
        gate_id = register(gate, rf"{identifier}-GATE", own_tasks + [f"{dep}-GATE" for dep in deps])
        gate_ids.add(gate_id)
        _strings(gate.get("acceptance"), f"{gate_id}.acceptance")
    for identifier, deps in nodes.items():
        for dependency in deps:
            if dependency not in nodes:
                raise ValueError(f"{identifier}: dependência inexistente {dependency}")
        if identifier in release_ids and not set(deps) <= release_ids:
            raise ValueError(f"{identifier}: release depende apenas de releases")
        if identifier in task_ids and set(deps) & release_ids:
            raise ValueError(f"{identifier}: tarefa deve depender de tarefa, bootstrap ou gate")
    if set(commands) - versions:
        raise ValueError("commands_added referencia versão sem release")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(identifier: str) -> None:
        if identifier in visiting:
            raise ValueError(f"Ciclo de dependências em {identifier}")
        if identifier in visited:
            return
        visiting.add(identifier)
        for dependency in nodes[identifier]:
            visit(dependency)
        visiting.remove(identifier)
        visited.add(identifier)

    for identifier in nodes:
        visit(identifier)
    for identifier, deps in nodes.items():
        if any(positions[dependency] >= positions[identifier] for dependency in deps):
            raise ValueError(f"{identifier}: dependência fora da ordem de execução")


def release_for_version(plan: dict[str, Any], version: str) -> dict[str, Any]:
    """Resolve uma final ou RC; patches exigem sua própria entrada no manifesto."""
    if not isinstance(version, str) or (match := RELEASE_VERSION.fullmatch(version)) is None:
        raise ValueError(f"Versão de release inválida: {version}")
    base = match.group(1)
    for release in plan["releases"]:
        if release["version"] == base:
            return release
    raise ValueError(f"Versão {base} sem milestone no manifesto")


def render_roadmap(plan: dict[str, Any]) -> str:
    """Gera uma visão determinística; o estado externo das issues não altera o arquivo."""
    validate_plan(plan)
    releases = plan["releases"]
    count = sum(len(release["tasks"]) + 1 for release in releases) + len(plan["bootstrap"])
    lines = [
        "# Roadmap de releases do Sider", "",
        "<!-- Gerado por python -m tools.release.plan --write; editar releases/plan.json. -->", "",
        "Este roteiro organiza as entregas até a 1.0. O bootstrap é a única capacidade",
        "concluída nesta linha de base; as funcionalidades do banco permanecem planejadas.",
        "O estado operacional das tarefas está nas issues do GitHub, sem duplicar o estado neste arquivo.", "",
        f"São {len(releases)} milestones e {count} issues: um bootstrap, tarefas funcionais e um gate de publicação por versão.",
        "O repositório e os artefatos permanecem privados; a crate usa `publish = false`.", "",
        "## Índice", "",
        "- [Sequência de versões](#sequência-de-versões)",
        "- [Bootstrap comprovado](#bootstrap-comprovado)",
        "- [Contratos transversais](#contratos-transversais)",
        "- [Tarefas por versão](#tarefas-por-versão)",
        "- [Execução e publicação](#execução-e-publicação)", "",
        "## Sequência de versões", "",
        "| Milestone | Entrega | Tarefas | Depende de |",
        "| --- | --- | --- | --- |",
    ]
    for release in releases:
        deps = ", ".join(release["depends_on"]) or "Bootstrap"
        lines.append(f"| `{release['version']}` | {release['title']} | {len(release['tasks'])} + publicação | {deps} |")
    lines += ["", "## Bootstrap comprovado", ""]
    for item in plan["bootstrap"]:
        lines += [f"- [x] `{item['id']}`: {item['title']}."]
        lines += [f"  [Evidência {index}]({url})" for index, url in enumerate(item["evidence"], 1)]
    lines += ["", "## Contratos transversais", ""]
    for text in plan["contracts"]["decisions"]:
        lines.append(f"- {text}")
    lines += ["", "Fora do escopo até a 1.0: " + "; ".join(plan["contracts"]["after_1_0"]) + ".", ""]
    reference = plan["reference"]
    lines += [
        f"Referência fixada: Redis e `redis-cli` {reference['redis_version']}, plataforma `{reference['platform']}`.", "",
        "```text", reference["image"], "```", "",
        "A imagem fixada é uma entrada da suíte; só uma execução registrada constitui evidência de compatibilidade.", "",
        "## Tarefas por versão", "",
        "Objetivos, entregáveis, testes e critérios completos de cada issue estão no",
        "[manifesto versionado](releases/plan.json). As dependências indicam a ordem de execução.", "",
    ]
    for release in releases:
        lines += [f"### {release['version']} — {release['title']}", ""]
        lines += [f"- {text}" for text in release["scope"]]
        lines += ["", "| ID | Entrega | Dependências |", "| --- | --- | --- |"]
        for task in release["tasks"]:
            lines.append(f"| `{task['id']}` | {task['title']} | {', '.join(task['depends_on']) or 'Nenhuma'} |")
        gate = release["gate"]
        lines += [f"| `{gate['id']}` | {gate['title']} | Todas as tarefas da versão e os gates anteriores |", "",
                  "Evidências obrigatórias: " + ", ".join(f"`{item}`" for item in release["required_gates"]) + ".", "",
                  "Critérios para publicação:", ""]
        lines += [f"- {text}" for text in gate["acceptance"]]
        lines.append("")
    lines += [
        "## Execução e publicação", "",
        "1. Selecione a próxima issue desbloqueada do milestone atual e implemente em branch própria.",
        "2. Inclua testes e evidências; mantenha código compilável e commits atômicos em cada etapa.",
        "3. Integre o PR vinculado à issue por merge commit após validação.",
        "4. Atualize compatibilidade e notas; prepare `v<versão>-rc.1` quando as tarefas funcionais terminarem.",
        "5. O merge do PR `chore/release-v<versão>`, com label `type:release`, valida o SHA exato e publica os pacotes.",
        "6. Mudança funcional após a RC exige outra RC; a final recompila e testa o mesmo conteúdo funcional aprovado.",
        "7. Encerre o milestone somente após conferir a publicação final e seus artefatos.", "",
        "O fluxo completo e os comandos de preparação estão no [guia de releases](docs/releases.md).",
        "Cada candidata exige pelo menos 15 minutos de fuzz; a 1.0 acrescenta uma hora de carga contínua.",
        "As evidências são cumulativas. Teste ausente, ignorado, cancelado ou sem relatório bloqueia a publicação.",
        "Na primeira versão AOF, migração valida fixtures do formato inicial; nas seguintes, testa a versão anterior suportada.", "",
        "Os pacotes são Linux GNU x86_64 (`.tar.gz`, Ubuntu 24.04) e Windows MSVC x86_64 (`.zip`).",
        "Desde a 0.10, uma imagem Docker Linux amd64 exportada acompanha a release privada.",
        "Checksums SHA-256, manifesto de build e notas acompanham os binários testados depois da extração.", "",
        "Patches, como `0.3.1`, precisam de registro próprio no manifesto e de milestone criado quando necessário.",
        "Patches também têm candidata. Novas capacidades entram em minor; incompatibilidades antes da 1.0",
        "são restritas às minors e descritas nas notas. Não há datas artificiais.", "",
        "Reexecuções retomam drafts e uploads incompletos. Tag com SHA divergente ou artefato publicado diferente",
        "interrompe o fluxo. Uma release publicada não é sobrescrita.", "",
        "Para validar a fonte e a projeção sem alterar arquivos:", "",
        "```sh", "python -m tools.release.plan --check", "```", "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--roadmap", type=Path, default=DEFAULT_ROADMAP)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--check", action="store_true", help="Valida manifesto e ROADMAP sem escrever")
    actions.add_argument("--write", action="store_true", help="Regenera o ROADMAP a partir do manifesto")
    args = parser.parse_args()
    try:
        plan = load_plan(args.plan)
        rendered = render_roadmap(plan)
        if args.write:
            args.roadmap.write_text(rendered, encoding="utf-8", newline="\n")
        elif args.check and (not args.roadmap.exists() or args.roadmap.read_text(encoding="utf-8") != rendered):
            raise ValueError("ROADMAP.md está desatualizado; execute python -m tools.release.plan --write")
        print(f"Manifesto válido: {len(plan['releases'])} releases")
    except (ValueError, OSError) as error:
        parser.exit(1, f"Erro: {error}\n")


if __name__ == "__main__":
    main()
