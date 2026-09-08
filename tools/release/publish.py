"""Publicação retomável de releases privadas, sem sobrescrever artefatos."""

from __future__ import annotations

import hashlib
import json
import math
import re
import tempfile
from pathlib import Path
from typing import Any

VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.([1-9][0-9]*))?")
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
ASSET_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
BINARY_TARGETS = {"x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"}
MULTIPLATFORM_GATES = {"native", "tcp_smoke", "crash", "recovery", "migration"}
MANIFEST_NAME = "release-manifest.json"
CHECKSUMS_NAME = "SHA256SUMS"
NOTES_NAME = "release-notes.md"


class PublicationError(ValueError):
    """Estado local ou remoto incompatível com a publicação solicitada."""


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PublicationError(message)


def _version(version: Any) -> re.Match[str]:
    match = VERSION.fullmatch(version) if isinstance(version, str) else None
    _require(match is not None, "Versão inválida; use X.Y.Z ou X.Y.Z-rc.N.")
    assert match is not None
    _require(tuple(map(int, match.group(1, 2, 3))) >= (0, 1, 0), "O bootstrap não é uma release funcional.")
    return match


def prepare_assets(manifest: dict, artifacts_dir: Path | str, notes: str) -> dict[str, bytes]:
    """Valida toda a entrada e retorna os uploads; não escreve no disco nem na API.

    O chamador deve obter required_gates do plano e evidências da execução real.
    Esta fronteira exige completude e consistência, sem afirmar autenticidade de
    resultados fornecidos por um chamador arbitrário.
    """
    _require(manifest.get("schema_version") == 1, "Schema de manifesto não suportado.")
    version = _version(manifest.get("version"))
    sha = manifest.get("sha")
    _require(isinstance(sha, str) and SHA.fullmatch(sha) is not None, "SHA de commit inválido.")
    _require(isinstance(manifest.get("toolchain"), str) and bool(manifest["toolchain"].strip()), "Toolchain ausente.")
    _require(isinstance(notes, str) and bool(notes.strip()), "Notas da release ausentes.")
    required = manifest.get("required_gates")
    _require(isinstance(required, list) and bool(required), "Lista de gates obrigatórios ausente.")
    _require(all(isinstance(item, str) and item for item in required), "ID de gate inválido.")
    _require(len(required) == len(set(required)), "Gate obrigatório duplicado.")
    gates = manifest.get("gates")
    _require(isinstance(gates, list), "Resultados dos gates ausentes.")
    seen_gates: set[str] = set()
    for gate in gates:
        _require(isinstance(gate, dict), "Resultado de gate inválido.")
        name = gate.get("id")
        _require(isinstance(name, str) and bool(name) and name not in seen_gates, "ID de gate ausente ou duplicado.")
        seen_gates.add(name)
        _require(gate.get("status") == "success" and gate.get("sha") == sha, f"Gate {name} falhou, foi ignorado ou pertence a outro SHA.")
    _require(set(required) <= seen_gates, "Gate obrigatório sem resultado.")
    for gate in gates:
        if gate["id"] in {"fuzz", "soak"}:
            minimum = 900 if gate["id"] == "fuzz" else 3600
            duration = gate.get("duration_seconds")
            _require(type(duration) in {int, float} and math.isfinite(duration) and duration >= minimum, f"Gate {gate['id']} não comprova a duração mínima.")
        if gate["id"] in MULTIPLATFORM_GATES:
            targets = gate.get("targets")
            _require(isinstance(targets, list) and set(targets) == BINARY_TARGETS, f"Gate {gate['id']} não cobre Linux e Windows.")
    if version.group(4) is None:
        candidate = manifest.get("candidate")
        _require(isinstance(candidate, dict), "Versão final exige uma candidata publicada.")
        tag = candidate.get("tag")
        base = ".".join(version.group(1, 2, 3))
        _require(isinstance(tag, str) and re.fullmatch(rf"v{re.escape(base)}-rc\.[1-9][0-9]*", tag) is not None, "Candidata de outra versão-base ou tag inválida.")
        _require(isinstance(candidate.get("sha"), str) and SHA.fullmatch(candidate["sha"]) is not None, "SHA da candidata inválido.")

    artifacts = manifest.get("artifacts")
    _require(isinstance(artifacts, list) and bool(artifacts), "Artefatos ausentes.")
    uploads: dict[str, bytes] = {}
    names = {MANIFEST_NAME.casefold(), CHECKSUMS_NAME.casefold(), NOTES_NAME.casefold()}
    targets: set[str] = set()
    directory = Path(artifacts_dir).resolve(strict=True)
    _require(directory.is_dir(), "Diretório de artefatos inválido.")
    for artifact in artifacts:
        _require(isinstance(artifact, dict), "Descrição de artefato inválida.")
        name = artifact.get("name")
        _require(isinstance(name, str) and ASSET_NAME.fullmatch(name) is not None, "Nome de artefato inseguro.")
        _require(name.casefold() not in names, "Nome de artefato duplicado ou reservado.")
        names.add(name.casefold())
        target = artifact.get("target")
        _require(isinstance(target, str) and bool(target), "Target ausente.")
        targets.add(target)
        size, digest = artifact.get("size"), artifact.get("sha256")
        _require(type(size) is int and size > 0, "Tamanho de artefato inválido.")
        _require(isinstance(digest, str) and DIGEST.fullmatch(digest) is not None, "SHA-256 de artefato inválido.")
        path = directory / name
        _require(not path.is_symlink() and path.is_file() and path.resolve().parent == directory, "Artefato deve ser um arquivo regular dentro do diretório de uploads.")
        data = path.read_bytes()
        _require(len(data) == size and hashlib.sha256(data).hexdigest() == digest, f"Tamanho ou checksum divergente: {name}.")
        uploads[name] = data
    _require(BINARY_TARGETS <= targets, "São obrigatórios os pacotes Linux GNU e Windows MSVC x86_64.")
    if tuple(map(int, version.group(1, 2, 3))) >= (0, 10, 0):
        _require("linux/amd64" in targets, "A partir da 0.10 é obrigatório o arquivo da imagem Docker Linux amd64.")
    uploads[MANIFEST_NAME] = canonical_json(manifest)
    uploads[NOTES_NAME] = notes.encode("utf-8")
    uploads[CHECKSUMS_NAME] = "".join(
        f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in sorted(uploads.items())
    ).encode("utf-8")
    return uploads


def _find_release(client: Any, tag: str) -> dict | None:
    matches = [release for release in client.paginate(client.repo_path("releases")) if release.get("tag_name") == tag]
    _require(len(matches) <= 1, f"Múltiplas releases encontradas para {tag}.")
    return matches[0] if matches else None


def resolve_tag(client: Any, tag: str) -> str | None:
    """Resolve tags leves e anotadas até o commit real, sem confiar em commitish."""
    try:
        reference = client.request("GET", client.repo_path(f"git/ref/tags/{tag}"))
    except Exception as error:
        if getattr(error, "status", None) == 404:
            return None
        raise
    obj = reference["object"]
    visited: set[str] = set()
    while obj.get("type") == "tag":
        sha = obj.get("sha")
        _require(isinstance(sha, str) and sha not in visited and len(visited) < 16, "Tag anotada cíclica ou profunda demais.")
        visited.add(sha)
        obj = client.request("GET", client.repo_path(f"git/tags/{sha}"))["object"]
    _require(obj.get("type") == "commit" and isinstance(obj.get("sha"), str) and SHA.fullmatch(obj["sha"]) is not None, "A tag não aponta para um commit válido.")
    return obj["sha"]


def _ensure_tag(client: Any, tag: str, sha: str, *, existing_release: bool) -> None:
    found = resolve_tag(client, tag)
    if found is None:
        _require(not existing_release, "Release existente perdeu sua tag; é necessária investigação.")
        try:
            client.request("POST", client.repo_path("git/refs"), {"ref": f"refs/tags/{tag}", "sha": sha})
        except Exception:
            # Uma resposta pode desaparecer depois da criação; conferir antes de repetir.
            if resolve_tag(client, tag) != sha:
                raise
        found = resolve_tag(client, tag)
    _require(found == sha, f"A tag {tag} aponta para outro SHA; nenhuma referência foi sobrescrita.")


def _assets(client: Any, release: dict) -> dict[str, dict]:
    assets = client.paginate(client.repo_path(f"releases/{release['id']}/assets"))
    result: dict[str, dict] = {}
    for asset in assets:
        name = asset["name"]
        _require(name not in result, f"Asset remoto duplicado: {name}.")
        result[name] = asset
    return result


def _verify_asset(client: Any, asset: dict, expected: bytes) -> None:
    _require(asset.get("state") == "uploaded", f"Upload incompleto: {asset['name']}.")
    _require(asset.get("size") == len(expected), f"Tamanho remoto divergente: {asset['name']}.")
    downloaded = client.download(asset["url"])
    _require(hashlib.sha256(downloaded).digest() == hashlib.sha256(expected).digest(), f"Checksum remoto divergente: {asset['name']}.")


def _verify_metadata(release: dict, tag: str, notes: str, prerelease: bool) -> None:
    _require(release.get("tag_name") == tag, "Tag da release divergente.")
    _require(release.get("body") == notes, "Notas remotas divergentes; não serão sobrescritas.")
    _require(release.get("prerelease") is prerelease, "Classificação RC/final divergente.")


def _verify_complete(client: Any, release: dict, uploads: dict[str, bytes]) -> None:
    assets = _assets(client, release)
    _require(set(assets) == set(uploads), "Conjunto de assets remoto incompleto ou inesperado.")
    for name, data in uploads.items():
        _verify_asset(client, assets[name], data)


def _candidate_is_published(client: Any, candidate: dict) -> None:
    release = _find_release(client, candidate["tag"])
    _require(release is not None and release.get("draft") is False and release.get("prerelease") is True, "Candidata não está publicada como prerelease.")
    _require(resolve_tag(client, candidate["tag"]) == candidate["sha"], "Tag da candidata diverge do SHA aprovado.")
    assets = _assets(client, release)
    _require(MANIFEST_NAME in assets, "Candidata sem manifesto de evidências.")
    asset = assets[MANIFEST_NAME]
    _require(asset.get("state") == "uploaded", "Manifesto da candidata incompleto.")
    try:
        manifest = json.loads(client.download(asset["url"]))
    except (ValueError, UnicodeError) as error:
        raise PublicationError("Manifesto remoto da candidata inválido.") from error
    _require(manifest.get("version") == candidate["tag"][1:] and manifest.get("sha") == candidate["sha"], "Manifesto remoto da candidata divergente.")


def _open_items(client: Any, milestone_number: int) -> list[dict]:
    return client.paginate(client.repo_path(f"issues?milestone={milestone_number}&state=open"))


def _check_tracking(client: Any, gate_issue: int, milestone_number: int, *, allow_other_items: bool) -> None:
    issue = client.request("GET", client.repo_path(f"issues/{gate_issue}"))
    milestone = issue.get("milestone") or {}
    _require("pull_request" not in issue and milestone.get("number") == milestone_number, "Issue de publicação não pertence ao milestone informado.")
    if not allow_other_items:
        others = [item for item in _open_items(client, milestone_number) if item["number"] != gate_issue]
        _require(not others, "Milestone contém itens abertos além da publicação.")


def _close_tracking(client: Any, gate_issue: int, milestone_number: int) -> None:
    issue_path = client.repo_path(f"issues/{gate_issue}")
    if client.request("GET", issue_path)["state"] != "closed":
        client.request("PATCH", issue_path, {"state": "closed", "state_reason": "completed"})
    _require(not _open_items(client, milestone_number), "Release publicada; milestone ainda contém itens abertos.")
    milestone_path = client.repo_path(f"milestones/{milestone_number}")
    if client.request("GET", milestone_path)["state"] != "closed":
        client.request("PATCH", milestone_path, {"state": "closed"})


def _record_publication(client: Any, gate_issue: int, tag: str, sha: str, prerelease: bool) -> None:
    marker = f"<!-- sider:publication {tag} {sha} -->"
    kind = "Candidata" if prerelease else "Versão final"
    body = f"{marker}\n{kind} `{tag}` publicada e verificada no commit `{sha}`.\n"
    path = client.repo_path(f"issues/{gate_issue}/comments")

    def existing() -> bool:
        matches = [comment for comment in client.paginate(path) if marker in (comment.get("body") or "")]
        _require(len(matches) <= 1, "Registro de publicação duplicado na issue.")
        if matches:
            _require(matches[0]["body"] == body, "Registro de publicação alterado; comentário será preservado.")
        return bool(matches)

    if not existing():
        try:
            client.request("POST", path, {"body": body})
        except Exception:
            if not existing():
                raise
        _require(existing(), "Registro de publicação não confirmado na issue.")


def reconcile_published(
    client: Any,
    version: str,
    sha: str,
    *,
    gate_issue: int,
    milestone_number: int,
    required_gates: list[str] | None = None,
    notes: str | None = None,
) -> dict:
    """Revalida os bytes publicados e retoma apenas o registro/fechamento do backlog.

    Não recompila nem substitui a proveniência por uma execução nova. A CLI deve
    fornecer required_gates a partir do plano versionado no commit da release.
    Arquivos temporários recebem somente nomes simples verificados e são limpos
    pelo TemporaryDirectory, fora do repositório.
    """
    _version(version)
    _require(isinstance(sha, str) and SHA.fullmatch(sha) is not None, "SHA de commit inválido.")
    _require(client.request("GET", client.repo_path("")).get("private") is True, "O repositório deve permanecer privado.")
    release = _find_release(client, f"v{version}")
    _require(release is not None and release.get("draft") is False, "A retomada exige uma release já publicada.")
    _require(resolve_tag(client, f"v{version}") == sha, "Tag publicada diverge do SHA solicitado.")
    assets = _assets(client, release)
    _require(MANIFEST_NAME in assets, "Release publicada sem manifesto.")
    raw_manifest = client.download(assets[MANIFEST_NAME]["url"])
    try:
        manifest = json.loads(raw_manifest)
    except (ValueError, UnicodeError) as error:
        raise PublicationError("Manifesto publicado inválido.") from error
    _require(isinstance(manifest, dict) and manifest.get("version") == version and manifest.get("sha") == sha, "Manifesto publicado diverge da versão ou SHA solicitado.")
    _require(raw_manifest == canonical_json(manifest), "Manifesto publicado não tem a serialização canônica esperada.")
    if required_gates is not None:
        recorded = manifest.get("required_gates")
        _require(isinstance(recorded, list) and set(recorded) == set(required_gates), "Gates publicados divergem do plano versionado.")
    # O parser de nomes antecede qualquer gravação no diretório temporário.
    for name in assets:
        _require(isinstance(name, str) and ASSET_NAME.fullmatch(name) is not None, "Nome de asset remoto inseguro.")
    if notes is not None:
        _require(release.get("body") == notes, "Notas publicadas divergem do commit da release.")
    else:
        notes = release.get("body")
    with tempfile.TemporaryDirectory(prefix="sider-release-reconcile-") as temporary:
        directory = Path(temporary)
        for name, asset in assets.items():
            data = raw_manifest if name == MANIFEST_NAME else client.download(asset["url"])
            _require(asset.get("state") == "uploaded" and asset.get("size") == len(data), f"Asset publicado incompleto: {name}.")
            (directory / name).write_bytes(data)
        expected = prepare_assets(manifest, directory, notes)
        _require(set(assets) == set(expected), "Conjunto de assets publicado diverge do manifesto.")
        for name, data in expected.items():
            _require((directory / name).read_bytes() == data, f"Bytes publicados divergentes: {name}.")
        return publish_release(client, manifest, directory, notes, gate_issue=gate_issue, milestone_number=milestone_number)


def publish_release(
    client: Any,
    manifest: dict,
    artifacts_dir: Path | str,
    notes: str,
    *,
    gate_issue: int,
    milestone_number: int,
) -> dict:
    """Publica ou retoma uma execução previamente validada pelo preflight.

    Nunca substitui tags, assets completos ou releases publicadas. É seguro chamar
    novamente com entradas idênticas após falha de rede. A publicação somente
    ocorre depois de baixar e conferir todos os uploads. O fechamento do backlog
    é posterior, para que uma falha nessa etapa não exija republicação.
    """
    uploads = prepare_assets(manifest, artifacts_dir, notes)
    _require(type(gate_issue) is int and gate_issue > 0 and type(milestone_number) is int and milestone_number > 0, "Issue e milestone devem ser números positivos.")
    _require(client.request("GET", client.repo_path("")).get("private") is True, "O repositório deve permanecer privado.")
    version, sha = manifest["version"], manifest["sha"]
    tag = f"v{version}"
    prerelease = _version(version).group(4) is not None
    release = _find_release(client, tag)
    # Uma final já publicada deve poder retomar somente o fechamento do backlog.
    already_published = release is not None and release.get("draft") is False
    _check_tracking(client, gate_issue, milestone_number, allow_other_items=already_published)
    if not prerelease:
        _candidate_is_published(client, manifest["candidate"])
    _ensure_tag(client, tag, sha, existing_release=release is not None)
    if release is None:
        try:
            release = client.request("POST", client.repo_path("releases"), {
                "tag_name": tag, "target_commitish": sha, "name": f"Sider {tag}",
                "body": notes, "draft": True, "prerelease": prerelease,
                "make_latest": "false", "generate_release_notes": False,
            })
        except Exception:
            release = _find_release(client, tag)
            if release is None:
                raise
    _verify_metadata(release, tag, notes, prerelease)
    if release.get("draft") is True:
        assets = _assets(client, release)
        _require(set(assets) <= set(uploads), "Draft contém assets inesperados; nenhuma alteração realizada.")
        for name, data in uploads.items():
            asset = assets.get(name)
            if asset is not None and asset.get("state") == "starter" and asset.get("size") == 0:
                client.request("DELETE", client.repo_path(f"releases/assets/{asset['id']}"))
                asset = None
            if asset is not None:
                _verify_asset(client, asset, data)
                continue
            try:
                content_type = "application/json" if name == MANIFEST_NAME else "application/octet-stream"
                client.upload(release["upload_url"], name, data, content_type)
            except Exception:
                uploaded = _assets(client, release).get(name)
                if uploaded is None:
                    raise
                _verify_asset(client, uploaded, data)
        _verify_complete(client, release, uploads)
        _require(resolve_tag(client, tag) == sha, "Tag alterada durante os uploads.")
        _check_tracking(client, gate_issue, milestone_number, allow_other_items=False)
        release_path = client.repo_path(f"releases/{release['id']}")
        try:
            client.request("PATCH", release_path, {
                "draft": False, "prerelease": prerelease,
                "make_latest": "false" if prerelease else "legacy",
            })
        except Exception:
            remote = client.request("GET", release_path)
            if remote.get("draft") is not False:
                raise
        release = client.request("GET", release_path)
    else:
        release = client.request("GET", client.repo_path(f"releases/{release['id']}"))
    _require(release.get("draft") is False, "Release permaneceu como draft.")
    _verify_metadata(release, tag, notes, prerelease)
    _require(resolve_tag(client, tag) == sha, "Tag publicada diverge do commit validado.")
    _verify_complete(client, release, uploads)
    _record_publication(client, gate_issue, tag, sha, prerelease)
    if not prerelease:
        _close_tracking(client, gate_issue, milestone_number)
    return release
