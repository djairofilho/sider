"""Credential-free tests for preflight and RESP framing in reference.py."""

import math
import unittest
from unittest.mock import patch

import reference


class FailingProvider:
    provider, model, revision, transform = reference.IDENTITY

    def embed(self, text: bytes):
        raise RuntimeError("provider unavailable")


class ReferenceTests(unittest.TestCase):
    def test_complete_fixture_preflights_before_any_client_exists(self):
        prepared = reference.prepare(reference.FixtureEmbeddingProvider(), reference.SOURCES)
        self.assertEqual(len(prepared), 3)
        self.assertEqual(prepared[0][1], b"\x00\x00\x00\x00\x00\x00\x00\x00")

    def test_provider_failure_stops_preflight(self):
        with self.assertRaisesRegex(RuntimeError, "provider unavailable"):
            reference.prepare(FailingProvider(), reference.SOURCES)

    def test_requested_provider_failure_does_not_open_sider_connection(self):
        with patch.object(reference, "Sider") as client:
            reference.run("127.0.0.1", 6379, fail_provider=True, check_empty=False)
        client.assert_not_called()

    def test_non_finite_provider_output_is_rejected_before_connection(self):
        class NonFiniteProvider:
            provider, model, revision, transform = reference.IDENTITY

            def embed(self, text: bytes):
                return (math.nan, 0.0)

        with self.assertRaisesRegex(ValueError, "invalid two-dimensional vector"):
            reference.prepare(NonFiniteProvider(), reference.SOURCES)

    def test_resp_request_uses_binary_safe_bulk_framing(self):
        self.assertEqual(
            reference.encode((b"ECHO", b"a\x00\r\n\xff")),
            b"*2\r\n$4\r\nECHO\r\n$5\r\na\x00\r\n\xff\r\n",
        )


if __name__ == "__main__":
    unittest.main()
