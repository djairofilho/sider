"""Testes sem rede do transporte e da proteção de credenciais."""

import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.request import Request

from .github import GitHubClient, GitHubError, _SafeRedirects


class Response(io.BytesIO):
    def __init__(self, value, headers=None):
        super().__init__(value if isinstance(value, bytes) else json.dumps(value).encode("utf-8"))
        self.headers = headers or {}


class GitHubClientTests(unittest.TestCase):
    def client(self, **kwargs):
        return GitHubClient(token="secret-test-token", **kwargs)

    def test_write_requires_explicit_opt_in_before_any_network(self):
        with patch("tools.release.github.build_opener") as opener:
            with self.assertRaisesRegex(GitHubError, "allow_writes"):
                self.client().request("POST", "/repos/djairofilho/sider/issues", {"title": "Título"})
            with self.assertRaisesRegex(GitHubError, "allow_writes"):
                self.client().upload("https://uploads.github.com/repos/a/b/releases/1/assets{?name,label}", "a", b"x", "text/plain")
            opener.assert_not_called()

    def test_utf8_json_and_api_headers(self):
        opener = Mock()
        opener.open.return_value = Response({"body": "Publicação"})
        with patch("tools.release.github.build_opener", return_value=opener):
            result = self.client(allow_writes=True).request("POST", "/repos/djairofilho/sider/issues", {"body": "Publicação `sider`"})
        request = opener.open.call_args.args[0]
        self.assertEqual(request.data.decode("utf-8"), '{"body": "Publicação `sider`"}')
        self.assertEqual(request.get_header("Authorization"), "Bearer secret-test-token")
        self.assertIn("utf-8", request.get_header("Content-type"))
        self.assertEqual(result["body"], "Publicação")
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 60)

    def test_untrusted_urls_and_get_body_rejected(self):
        for url in ("https://evil.example/api", "http://api.github.com/repos/a/b", "//evil.example/x", "https://user:pass@api.github.com/x"):
            with self.subTest(url=url), self.assertRaises(GitHubError):
                self.client().request("GET", url)
        with self.assertRaises(ValueError):
            self.client().request("GET", "/repos/a/b", {})

    def test_pagination_uses_link_and_keeps_metadata(self):
        opener = Mock()
        opener.open.side_effect = [
            Response([{"number": 1, "body": "acentuação"}], {"Link": '<https://api.github.com/repos/a/b/issues?page=2>; rel="next"'}),
            Response([{"number": 2}]),
        ]
        with patch("tools.release.github.build_opener", return_value=opener):
            rows = self.client().paginate("/repos/a/b/issues?state=all")
        self.assertEqual(rows, [{"number": 1, "body": "acentuação"}, {"number": 2}])
        self.assertIn("per_page=100", opener.open.call_args_list[0].args[0].full_url)
        self.assertIn("page=2", opener.open.call_args_list[1].args[0].full_url)

    def test_pagination_cannot_exfiltrate_token(self):
        opener = Mock()
        opener.open.return_value = Response([], {"Link": '<https://evil.example/token>; rel="next"'})
        with patch("tools.release.github.build_opener", return_value=opener):
            with self.assertRaises(GitHubError):
                self.client().paginate("/repos/a/b/issues")
        self.assertEqual(opener.open.call_count, 1)

    def test_pagination_cycle_and_non_list_rejected(self):
        for responses in (
            [Response([], {"Link": '<https://api.github.com/repos/a/b/issues?per_page=100>; rel="next"'})],
            [Response({"items": []})],
        ):
            opener = Mock()
            opener.open.side_effect = responses
            with patch("tools.release.github.build_opener", return_value=opener), self.assertRaises(GitHubError):
                self.client().paginate("/repos/a/b/issues")

    def test_download_uses_binary_accept(self):
        opener = Mock()
        opener.open.return_value = Response(b"\x00\xffarchive")
        with patch("tools.release.github.build_opener", return_value=opener):
            value = self.client().download("https://api.github.com/repos/a/b/releases/assets/1")
        self.assertEqual(value, b"\x00\xffarchive")
        self.assertEqual(opener.open.call_args.args[0].get_header("Accept"), "application/octet-stream")

    def test_upload_encodes_filename_without_corrupting_data(self):
        opener = Mock()
        opener.open.return_value = Response({"id": 1})
        with patch("tools.release.github.build_opener", return_value=opener):
            self.client(allow_writes=True).upload(
                "https://uploads.github.com/repos/a/b/releases/1/assets{?name,label}", "sider final.zip", b"\xff", "application/zip"
            )
        request = opener.open.call_args.args[0]
        self.assertTrue(request.full_url.endswith("?name=sider+final.zip"))
        self.assertEqual(request.data, b"\xff")

    def test_redirect_drops_auth_on_asset_host(self):
        redirects = _SafeRedirects({"api.github.com", "release-assets.githubusercontent.com"})
        request = Request("https://api.github.com/x", headers={"Authorization": "Bearer secret", "Accept": "application/octet-stream"})
        changed = redirects.redirect_request(request, None, 302, "Found", {}, "https://release-assets.githubusercontent.com/asset")
        self.assertIsNone(changed.get_header("Authorization"))
        self.assertEqual(changed.get_header("Accept"), "application/octet-stream")
        same = redirects.redirect_request(request, None, 302, "Found", {}, "https://api.github.com/y")
        self.assertEqual(same.get_header("Authorization"), "Bearer secret")
        with self.assertRaises(GitHubError):
            redirects.redirect_request(request, None, 302, "Found", {}, "https://evil.example/asset")

    def test_error_status_and_secrets_not_exposed(self):
        for failure, status in ((HTTPError("secret-url", 404, "secret-test-token", {}, None), 404), (URLError("secret-test-token"), None)):
            opener = Mock()
            opener.open.side_effect = failure
            with patch("tools.release.github.build_opener", return_value=opener):
                with self.assertRaises(GitHubError) as caught:
                    self.client().request("GET", "/repos/a/b")
            self.assertEqual(caught.exception.status, status)
            self.assertNotIn("secret", str(caught.exception))

    def test_auth_uses_env_or_captured_cli_output(self):
        with patch.dict("os.environ", {"GH_TOKEN": "env-token"}, clear=True), patch("tools.release.github.subprocess.run") as run:
            self.assertEqual(GitHubClient()._credential(), "env-token")
            run.assert_not_called()
        with patch.dict("os.environ", {}, clear=True), patch("tools.release.github.subprocess.run", return_value=SimpleNamespace(returncode=0, stdout="cli-token\n")) as run:
            self.assertEqual(GitHubClient()._credential(), "cli-token")
            self.assertTrue(run.call_args.kwargs["capture_output"])


if __name__ == "__main__":
    unittest.main()
