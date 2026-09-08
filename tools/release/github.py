"""Cliente GitHub restrito, com escrita explícita e JSON UTF-8."""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class GitHubError(RuntimeError):
    """Erro HTTP ou de transporte, sem expor credenciais ou URLs assinadas."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def _check_url(url: str, hosts: set[str]) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in hosts
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise GitHubError("Destino HTTPS do GitHub não permitido")
    return url


class _SafeRedirects(HTTPRedirectHandler):
    def __init__(self, hosts: set[str]):
        super().__init__()
        self.hosts = hosts

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _check_url(newurl, self.hosts)
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and urlsplit(req.full_url).netloc != urlsplit(newurl).netloc:
            redirected.remove_header("Authorization")
            redirected.remove_header("Proxy-authorization")
        return redirected


class GitHubClient:
    """Usa GH_TOKEN/GITHUB_TOKEN ou a sessão da CLI; leitura por padrão.

    Escritas não são repetidas automaticamente após falha de transporte. O chamador
    deve reler os recursos e reconciliar a operação usando seus identificadores.
    """

    def __init__(
        self,
        repo: str = "djairofilho/sider",
        allow_writes: bool = False,
        token: str | None = None,
    ):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
            raise ValueError("Repositório deve usar o formato owner/repo")
        self.repo = repo
        self.allow_writes = allow_writes
        self._token = token
        self._token_loaded = token is not None

    def repo_path(self, suffix: str) -> str:
        return f"/repos/{self.repo}" + ("/" + suffix.lstrip("/") if suffix else "")

    def _credential(self) -> str:
        if not self._token_loaded:
            self._token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
            if not self._token:
                try:
                    result = subprocess.run(
                        ["gh", "auth", "token", "--hostname", "github.com"],
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        timeout=30,
                        check=False,
                    )
                except (OSError, subprocess.TimeoutExpired):
                    raise GitHubError("Não foi possível consultar a autenticação da CLI") from None
                if result.returncode != 0:
                    raise GitHubError("Autentique com gh auth login ou forneça GH_TOKEN")
                self._token = result.stdout.strip()
            self._token_loaded = True
        if not self._token:
            raise GitHubError("Token GitHub ausente")
        return self._token

    def _api_url(self, path: str) -> str:
        if path.startswith("/") and not path.startswith("//"):
            path = "https://api.github.com" + path
        return _check_url(path, {"api.github.com"})

    def _send(
        self,
        method: str,
        url: str,
        data: bytes | None = None,
        content_type: str | None = None,
        binary: bool = False,
        hosts: set[str] | None = None,
    ) -> tuple[bytes, Any]:
        method = method.upper()
        if method != "GET" and not self.allow_writes:
            raise GitHubError("Escrita GitHub exige allow_writes=True")
        allowed = hosts or {"api.github.com"}
        _check_url(url, allowed)
        headers = {
            "Accept": "application/octet-stream" if binary else "application/vnd.github+json",
            "User-Agent": "sider-release-tools",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if urlsplit(url).hostname in {"api.github.com", "uploads.github.com"}:
            headers["Authorization"] = "Bearer " + self._credential()
        if content_type:
            headers["Content-Type"] = content_type
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with build_opener(_SafeRedirects(allowed)).open(request, timeout=60) as response:
                return response.read(), response.headers
        except HTTPError as error:
            raise GitHubError(f"GitHub retornou HTTP {error.code}", error.code) from None
        except (URLError, TimeoutError, OSError):
            raise GitHubError("Falha de transporte ao acessar GitHub; releia o estado antes de repetir") from None

    @staticmethod
    def _json(data: bytes) -> Any:
        if not data:
            return None
        try:
            return json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise GitHubError("Resposta GitHub não contém JSON UTF-8 válido") from None

    def request(self, method: str, path: str, body: Any = None) -> Any:
        method = method.upper()
        if method not in {"GET", "POST", "PATCH", "PUT", "DELETE"}:
            raise ValueError("Método HTTP não suportado")
        if method == "GET" and body is not None:
            raise ValueError("GET não pode enviar um corpo")
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        response, _ = self._send(
            method, self._api_url(path), data, "application/json; charset=utf-8" if data else None
        )
        return self._json(response)

    def paginate(self, path: str) -> list[Any]:
        url = self._api_url(path)
        parsed = urlsplit(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.setdefault("per_page", "100")
        url = urlunsplit(parsed._replace(query=urlencode(query)))
        items: list[Any] = []
        visited: set[str] = set()
        while url:
            if url in visited or len(visited) >= 1000:
                raise GitHubError("Paginação GitHub circular ou excessiva")
            visited.add(url)
            data, headers = self._send("GET", self._api_url(url))
            page = self._json(data)
            if not isinstance(page, list):
                raise GitHubError("Endpoint paginado não retornou uma lista")
            items.extend(page)
            links = re.findall(r'<([^>]+)>;\s*rel="([^"]+)"', headers.get("Link", ""))
            next_links = [link for link, relation in links if relation == "next"]
            if len(next_links) > 1:
                raise GitHubError("Paginação GitHub contém links next duplicados")
            url = self._api_url(next_links[0]) if next_links else ""
        return items

    def upload(self, upload_url: str, name: str, data: bytes, content_type: str) -> Any:
        url = upload_url.split("{", 1)[0]
        _check_url(url, {"uploads.github.com"})
        parsed = urlsplit(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["name"] = name
        url = urlunsplit(parsed._replace(query=urlencode(query)))
        response, _ = self._send(
            "POST", url, data, content_type, hosts={"uploads.github.com"}
        )
        return self._json(response)

    def download(self, url: str) -> bytes:
        url = self._api_url(url)
        response, _ = self._send(
            "GET",
            url,
            binary=True,
            hosts={
                "api.github.com",
                "release-assets.githubusercontent.com",
                "objects.githubusercontent.com",
            },
        )
        return response
