#!/usr/bin/env python3
"""Deterministic, credential-free client for the sider-memory/1 TCP commands."""

from __future__ import annotations

import argparse
import math
import socket
import struct
import sys
import time
from dataclasses import dataclass
from typing import Protocol


IDENTITY = (b"fixture", b"two-axis-demo", b"1", b"l2sq-f32le-v1")
COLLECTION = b"memory:{agent}:reference"
QUERY = (0.0, 0.0)


@dataclass(frozen=True)
class Source:
    member_id: bytes
    namespace: bytes
    content: bytes
    category: bytes
    vector_input: tuple[float, float]
    source: bytes
    revision: bytes


SOURCES = (
    Source(b"policy-1", b"team-a", b"Keep the signing key offline.", b"policy", (0.0, 0.0), b"handbook://security", b"rev-7"),
    Source(b"note-2", b"team-a", b"Rotate service credentials monthly.", b"note", (1.0, 0.0), b"notes://rotation", b"rev-2"),
    Source(b"policy-3", b"team-b", b"Back up encrypted recovery material.", b"policy", (0.2, 0.0), b"handbook://recovery", b"rev-3"),
)


class EmbeddingProvider(Protocol):
    provider: bytes
    model: bytes
    revision: bytes
    transform: bytes

    def embed(self, text: bytes) -> tuple[float, ...]: ...


class FixtureEmbeddingProvider:
    """Stable toy mapping from an explicit fixture vector; no model is called."""

    provider = IDENTITY[0]
    model = IDENTITY[1]
    revision = IDENTITY[2]
    transform = IDENTITY[3]

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    def embed(self, text: bytes) -> tuple[float, ...]:
        if self.fail:
            raise RuntimeError("fixture provider failure requested")
        source = next((item for item in SOURCES if item.content == text), None)
        if source is None:
            raise ValueError("content is not present in the deterministic fixture")
        return source.vector_input


def prepare(provider: EmbeddingProvider, sources: tuple[Source, ...]) -> list[tuple[Source, bytes]]:
    """Resolve all vectors before the caller opens a Sider connection."""
    identity = (provider.provider, provider.model, provider.revision, provider.transform)
    if identity != IDENTITY:
        raise ValueError("provider identity differs from the collection contract")
    prepared: list[tuple[Source, bytes]] = []
    for source in sources:
        vector = provider.embed(source.content)
        if len(vector) != 2 or not all(math.isfinite(value) for value in vector):
            raise ValueError("provider returned an invalid two-dimensional vector")
        prepared.append((source, struct.pack("<2f", *vector)))
    return prepared


def encode(args: tuple[bytes, ...]) -> bytes:
    output = [b"*" + str(len(args)).encode("ascii") + b"\r\n"]
    for arg in args:
        output.extend((b"$", str(len(arg)).encode("ascii"), b"\r\n", arg, b"\r\n"))
    return b"".join(output)


def read_line(stream: socket.socket) -> bytes:
    data = bytearray()
    while not data.endswith(b"\r\n"):
        chunk = stream.recv(1)
        if not chunk:
            raise ConnectionError("Sider closed the connection")
        data.extend(chunk)
    return bytes(data[:-2])


def read_exact(stream: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = stream.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Sider closed the connection")
        data.extend(chunk)
    return bytes(data)


def read_response(stream: socket.socket):
    prefix = read_exact(stream, 1)
    if prefix in (b"+", b"-", b":"):
        line = read_line(stream)
        if prefix == b"-":
            raise RuntimeError(line.decode("ascii", "replace"))
        return line if prefix == b"+" else int(line)
    if prefix == b"$":
        size = int(read_line(stream))
        if size == -1:
            return None
        value = read_exact(stream, size)
        if read_exact(stream, 2) != b"\r\n":
            raise ValueError("invalid bulk response terminator")
        return value
    if prefix == b"*":
        count = int(read_line(stream))
        if count == -1:
            return None
        return [read_response(stream) for _ in range(count)]
    raise ValueError(f"unsupported RESP2 response type: {prefix!r}")


class Sider:
    def __init__(self, host: str, port: int, timeout: float = 5.0) -> None:
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.socket.settimeout(timeout)

    def command(self, *args: bytes):
        self.socket.sendall(encode(tuple(args)))
        return read_response(self.socket)

    def close(self) -> None:
        self.socket.close()


def ingest(client: Sider, prepared: list[tuple[Source, bytes]]) -> None:
    client.command(b"MEM.CREATE", COLLECTION, b"1", b"2", b"L2SQ", *IDENTITY)
    for source, vector in prepared:
        provenance = (
            b"embedding_model", IDENTITY[1],
            b"embedding_provider", IDENTITY[0],
            b"embedding_revision", IDENTITY[2],
            b"embedding_transform", IDENTITY[3],
            b"source", source.source,
            b"source_revision", source.revision,
        )
        result = client.command(
            b"MEM.PUT", COLLECTION, source.member_id, source.namespace, source.content,
            *IDENTITY, vector, b"-1", b"UPSERT", b"1", b"category", b"BYTES",
            source.category, b"6", *provenance,
        )
        if result != 1:
            raise RuntimeError(f"unexpected MEM.PUT result: {result!r}")


def run(host: str, port: int, fail_provider: bool, check_empty: bool) -> None:
    if check_empty:
        client = Sider(host, port)
        try:
            info = client.command(b"MEM.INFO", COLLECTION)
            print("MEM.INFO after restart:", info)
            if info is not None:
                raise AssertionError("in-memory collection unexpectedly survived restart")
        finally:
            client.close()
        return
    provider = FixtureEmbeddingProvider(fail=fail_provider)
    try:
        prepared = prepare(provider, SOURCES)
    except Exception as exc:
        print(f"preflight failed: {exc}; Sider commands sent: 0")
        if fail_provider:
            return
        raise

    client = Sider(host, port)
    try:
        ingest(client, prepared)
        print("ingested: 3 records")

        query = struct.pack("<2f", *QUERY)
        hits = client.command(
            b"MEM.SEARCH", COLLECTION, *IDENTITY, query, b"5", b"team-a",
            b"1", b"category", b"BYTES", b"policy",
        )
        print("filtered search:", hits)
        if [hit[0] for hit in hits] != [b"policy-1"]:
            raise AssertionError(f"unexpected filtered results: {hits!r}")

        record = client.command(b"MEM.GET", COLLECTION, b"policy-1")
        print("source attribution:", record[5])
        expected_provenance = [
            [b"embedding_model", IDENTITY[1]],
            [b"embedding_provider", IDENTITY[0]],
            [b"embedding_revision", IDENTITY[2]],
            [b"embedding_transform", IDENTITY[3]],
            [b"source", b"handbook://security"],
            [b"source_revision", b"rev-7"],
        ]
        if record[5] != expected_provenance:
            raise AssertionError(f"unexpected provenance: {record[5]!r}")

        replacement = client.command(
            b"MEM.PUT", COLLECTION, b"policy-1", b"team-a", b"Signing keys stay offline; rotate access quarterly.",
            *IDENTITY, struct.pack("<2f", 0.0, 0.1), b"-1", b"UPSERT", b"1",
            b"category", b"BYTES", b"policy", b"6",
            b"embedding_model", IDENTITY[1], b"embedding_provider", IDENTITY[0],
            b"embedding_revision", IDENTITY[2], b"embedding_transform", IDENTITY[3],
            b"source", b"handbook://security", b"source_revision", b"rev-8",
        )
        updated = client.command(b"MEM.GET", COLLECTION, b"policy-1")
        print("replacement:", replacement, updated[1], updated[5])
        if replacement != 1 or updated[1] != b"Signing keys stay offline; rotate access quarterly.":
            raise AssertionError("full replacement did not update the record")
        expected_provenance[-1] = [b"source_revision", b"rev-8"]
        if updated[5] != expected_provenance:
            raise AssertionError("replacement provenance was not stored")

        ttl_result = client.command(
            b"MEM.PUT", COLLECTION, b"temporary", b"team-a", b"short lived", *IDENTITY,
            struct.pack("<2f", 0.1, 0.1), b"150", b"UPSERT", b"0", b"0",
        )
        pttl = client.command(b"MEM.PTTL", COLLECTION, b"temporary")
        time.sleep(0.2)
        expired = client.command(b"MEM.GET", COLLECTION, b"temporary")
        print("expiry:", ttl_result, "initial PTTL positive:", pttl > 0, "GET after 200 ms:", expired)
        if ttl_result != 1 or pttl <= 0 or expired is not None:
            raise AssertionError("member TTL did not expire the temporary record")

        deleted = client.command(b"MEM.DEL", COLLECTION, b"note-2")
        absent = client.command(b"MEM.GET", COLLECTION, b"note-2")
        print("deletion:", deleted, "GET after DEL:", absent)
        if deleted != 1 or absent is not None:
            raise AssertionError("MEM.DEL did not remove the requested record")
        print("restart/recovery: in-memory mode only; restart loses records (AOF record-2 activation is a later stage)")
    finally:
        client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--fail-provider", action="store_true", help="exercise zero-write preflight failure")
    parser.add_argument("--check-empty", action="store_true", help="verify collection is absent after in-memory restart")
    args = parser.parse_args()
    run(args.host, args.port, args.fail_provider, args.check_empty)
    return 0


if __name__ == "__main__":
    sys.exit(main())
