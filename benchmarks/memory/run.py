#!/usr/bin/env python3
"""Reproducible development-checkout memory search corpus and loopback load run."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import platform
import random
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "examples" / "agent-memory"))
from reference import Sider  # noqa: E402 - shared RESP2 client in this repository

CORPUS = Path(__file__).with_name("corpus-v1.json")
LOAD_KEY = b"memory:{corpus}:load"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def vector(values: list[float]) -> bytes:
    return struct.pack("<" + "f" * len(values), *values)


def metadata_args(metadata: dict[str, list[str]]) -> list[bytes]:
    result = [str(len(metadata)).encode()]
    for name, (kind, value) in sorted(metadata.items()):
        result.extend((name.encode(), kind.encode(), value.encode()))
    return result


def create(client: Sider, key: bytes, identity: list[bytes]) -> None:
    assert client.command(b"MEM.CREATE", key, b"1", b"2", b"L2SQ", *identity) == b"OK"


def put(client: Sider, key: bytes, identity: list[bytes], record: dict) -> None:
    result = client.command(
        b"MEM.PUT", key, bytes.fromhex(record["id_hex"]), record["namespace"].encode(),
        record["name"].encode(), *identity, vector(record["vector"]),
        str(record.get("ttl_ms", -1)).encode(), b"UPSERT",
        *metadata_args(record["metadata"]), b"0",
    )
    assert result == 1, (record["name"], result)


def search(client: Sider, key: bytes, identity: list[bytes], query: dict) -> list[list[bytes]]:
    return client.command(
        b"MEM.SEARCH", key, *identity, vector(query["vector"]),
        str(query["k"]).encode(), query["namespace"].encode(),
        *metadata_args(query["filters"]),
    )


def eligible(records: list[dict], query: dict) -> list[dict]:
    return [r for r in records if not r.get("expired") and not r.get("deleted")
            and r["namespace"] == query["namespace"]
            and all(r["metadata"].get(field) == value
                    for field, value in query["filters"].items())]


def oracle(records: list[dict], query: dict) -> list[list[bytes]]:
    q = struct.unpack("<2f", vector(query["vector"]))
    ranked = []
    for record in eligible(records, query):
        values = struct.unpack("<2f", vector(record["vector"]))
        distance = sum((float(a) - float(b)) ** 2 for a, b in zip(q, values))
        ranked.append((distance, bytes.fromhex(record["id_hex"])))
    ranked.sort(key=lambda row: (row[0], row[1]))
    return [[member_id, struct.pack("<d", distance)]
            for distance, member_id in ranked[:query["k"]]]


def check_queries(client: Sider, key: bytes, identity: list[bytes],
                  records: list[dict], queries: list[dict], baseline: bool) -> list[dict]:
    observations = []
    for query in queries:
        allowed = [r["id_hex"] for r in eligible(records, query)]
        if baseline:
            assert allowed == query["eligible_hex_after_expiry"], query["name"]
        wanted = oracle(records, query)
        started = time.perf_counter_ns()
        actual = search(client, key, identity, query)
        elapsed = time.perf_counter_ns() - started
        assert actual == wanted, (query["name"], actual, wanted)
        ids = [row[0].hex() for row in actual]
        relevant = set(query["relevant_hex"])
        observations.append({
            "query": query["name"], "eligible_hex": allowed,
            "expected_ids_hex": [row[0].hex() for row in wanted],
            "actual_ids_hex": ids,
            "actual_distance_f64le_hex": [row[1].hex() for row in actual],
            "exact_recall_at_k": 1.0 if wanted else None,
            "judged_relevance_precision_at_k":
                len(set(ids) & relevant) / len(ids) if ids else None,
            "latency_ns": elapsed,
        })
    return observations


def info(client: Sider, *sections: bytes) -> dict[str, int | str]:
    reply = client.command(b"INFO", *sections)
    result = {}
    for line in reply.decode("ascii").splitlines():
        if ":" in line and not line.startswith("#"):
            name, value = line.split(":", 1)
            try:
                result[name] = int(value)
            except ValueError:
                result[name] = value
    return result


def rss_bytes(pid: int) -> int | None:
    if sys.platform.startswith("linux"):
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        return None
    if sys.platform == "win32":
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t)]
        process = ctypes.windll.kernel32.OpenProcess(0x1000 | 0x0400, False, pid)
        if not process:
            return None
        try:
            counters = Counters()
            counters.cb = ctypes.sizeof(counters)
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                    process, ctypes.byref(counters), counters.cb):
                return int(counters.WorkingSetSize)
        finally:
            ctypes.windll.kernel32.CloseHandle(process)
    return None


def hardware() -> dict:
    cpu = platform.processor() or os.environ.get("PROCESSOR_IDENTIFIER", "")
    memory = None
    if sys.platform.startswith("linux"):
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                memory = int(line.split()[1]) * 1024
                break
    elif sys.platform == "win32":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong),
                        ("total_physical", ctypes.c_ulonglong),
                        ("available_physical", ctypes.c_ulonglong),
                        ("total_page", ctypes.c_ulonglong),
                        ("available_page", ctypes.c_ulonglong),
                        ("total_virtual", ctypes.c_ulonglong),
                        ("available_virtual", ctypes.c_ulonglong),
                        ("available_extended", ctypes.c_ulonglong)]
        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            memory = int(status.total_physical)
    return {"cpu_model": cpu or "pending", "logical_cpus": os.cpu_count(),
            "system_memory_bytes": memory}


class Server:
    def __init__(self, binary: Path, directory: Path) -> None:
        self.binary, self.directory = binary, directory
        self.child: subprocess.Popen | None = None
        self.log = None

    def start(self) -> Sider:
        ready = self.directory / "ready.json"
        ready.unlink(missing_ok=True)
        env = {key: value for key, value in os.environ.items()
               if not key.upper().startswith("SIDER_")}
        env.update(SIDER_SECURITY_PROFILE="legacy-loopback", SIDER_ADDR="127.0.0.1:0",
                   SIDER_READY_FILE=str(ready), SIDER_AOF_DIR=str(self.directory / "aof"),
                   SIDER_AOF_RECORD_VERSION="2", SIDER_AOF_SYNC="always",
                   SIDER_AOF_COMPACT_AFTER_BYTES="0", SIDER_SHARDS="1")
        self.log = (self.directory / "server.log").open("ab")
        self.child = subprocess.Popen([str(self.binary)], cwd=ROOT, env=env,
                                      stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while not ready.exists():
            if self.child.poll() is not None:
                raise RuntimeError(f"server exited: {(self.directory / 'server.log').read_text(errors='replace')}")
            if time.monotonic() > deadline:
                raise TimeoutError("server readiness")
            time.sleep(0.01)
        state = json.loads(ready.read_text())
        assert state["pid"] == self.child.pid and state["host"] == "127.0.0.1"
        return Sider("127.0.0.1", state["port"])

    def stop(self) -> None:
        if self.child is not None and self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=5)
        if self.log is not None:
            self.log.close()
        self.child, self.log = None, None


def percentile(samples: list[int], percent: int) -> int:
    ranked = sorted(samples)
    return ranked[max(0, (len(ranked) * percent + 99) // 100 - 1)]


def run(binary: Path, output: Path, load_count: int, repetitions: int) -> dict:
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    assert corpus["version"] == "sider-memory-corpus/1"
    assert 1 <= load_count <= 1024 and repetitions >= 1
    assert git("status", "--porcelain") == "", "run from a clean checkout"
    sha = git("rev-parse", "HEAD")
    identity = [value.encode() for value in corpus["identity"]]
    records = json.loads(json.dumps(corpus["records"]))
    quality_key = corpus["collection"].encode()
    observations: dict = {}
    with tempfile.TemporaryDirectory(prefix="sider-memory-corpus-") as temporary:
        server = Server(binary, Path(temporary))
        try:
            client = server.start()
            create(client, quality_key, identity)
            for record in records:
                put(client, quality_key, identity, record)
            time.sleep(0.25)
            for record in records:
                if "ttl_ms" in record:
                    record["expired"] = True
                    assert client.command(b"MEM.PTTL", quality_key,
                                          bytes.fromhex(record["id_hex"])) == -2
            observations["cold_after_ingest"] = check_queries(
                client, quality_key, identity, records, corpus["queries"], True)
            changed = next(r for r in records if r["name"] == "policy-b")
            changed["vector"] = [0.01, 0.0]
            put(client, quality_key, identity, changed)
            removed = next(r for r in records if r["name"] == "negative-policy")
            assert client.command(b"MEM.DEL", quality_key,
                                  bytes.fromhex(removed["id_hex"])) == 1
            removed["deleted"] = True
            observations["after_update_delete_expiry"] = check_queries(
                client, quality_key, identity, records, corpus["queries"], False)

            create(client, LOAD_KEY, identity)
            generator = random.Random(corpus["seed"])
            load_records = []
            for index in range(load_count):
                record = {"name": f"load-{index:04d}", "id_hex": f"{index:04x}",
                          "namespace": "team-a" if index % 2 else "team-b",
                          "vector": [generator.uniform(-1, 1), generator.uniform(-1, 1)],
                          "metadata": {"category": ["BYTES", "policy" if index % 4 else "note"]}}
                put(client, LOAD_KEY, identity, record)
                load_records.append(record)
            load_queries = [dict(query, k=10) for query in corpus["queries"]]
            observations["load_oracle"] = check_queries(
                client, LOAD_KEY, identity, load_records, load_queries, False)
            expected_load = [oracle(load_records, query) for query in load_queries]
            observations["before_load_info"] = info(client, b"stats", b"memory", b"persistence")
            latencies: list[int] = []
            rss_samples: list[int] = []
            queue_samples: list[dict] = []
            stop = threading.Event()
            pid = server.child.pid

            def sample() -> None:
                observer = Sider("127.0.0.1", client.socket.getpeername()[1])
                try:
                    while not stop.is_set():
                        rss = rss_bytes(pid)
                        if rss is not None:
                            rss_samples.append(rss)
                        metrics = info(observer, b"stats", b"persistence")
                        queue_samples.append({name: metrics.get(name) for name in
                                              ("worker_queue_used", "aof_queue_used")})
                        stop.wait(0.01)
                finally:
                    observer.close()

            monitor = threading.Thread(target=sample, daemon=True)
            monitor.start()
            start = time.perf_counter_ns()
            for _ in range(repetitions):
                for query, wanted in zip(load_queries, expected_load):
                    begun = time.perf_counter_ns()
                    actual = search(client, LOAD_KEY, identity, query)
                    latencies.append(time.perf_counter_ns() - begun)
                    assert actual == wanted
            elapsed = time.perf_counter_ns() - start
            stop.set()
            monitor.join(timeout=5)
            assert not monitor.is_alive(), "monitor did not stop"
            observations["after_load_info"] = info(client, b"stats", b"memory", b"persistence")
            observations["load"] = {
                "latency_ns_samples": latencies, "elapsed_ns": elapsed,
                "throughput_queries_per_second": len(latencies) * 1e9 / elapsed,
                "p50_ns": percentile(latencies, 50), "p95_ns": percentile(latencies, 95),
                "p99_ns": percentile(latencies, 99),
                "rss_bytes_samples": rss_samples, "queue_samples": queue_samples,
                "rss_max_observed_bytes": max(rss_samples) if rss_samples else None,
                "worker_queue_max_observed": max((s["worker_queue_used"] or 0 for s in queue_samples), default=None),
                "aof_queue_max_observed": max((s["aof_queue_used"] or 0 for s in queue_samples), default=None),
            }
            client.close()
            server.stop()
            client = server.start()
            observations["cold_after_recovery"] = check_queries(
                client, quality_key, identity, records, corpus["queries"], False)
            observations["recovered_load_oracle"] = check_queries(
                client, LOAD_KEY, identity, load_records, load_queries, False)
            observations["after_recovery_info"] = info(client, b"stats", b"memory", b"persistence")
            client.close()
        finally:
            server.stop()
    return {
        "schema": "sider-memory-corpus-result/1", "source_sha": sha,
        "binary_sha256": digest(binary), "binary_path": str(binary),
        "corpus_version": corpus["version"], "corpus_sha256": digest(CORPUS),
        "seed": corpus["seed"], "platform": platform.platform(),
        "uname": platform.uname()._asdict(), "hardware": hardware(),
        "python": platform.python_version(), "load_records": load_count,
        "repetitions": repetitions, "searches_per_repetition": len(corpus["queries"]),
        "configuration": {"security_profile": "legacy-loopback", "address": "127.0.0.1:0",
                          "aof_record_version": 2, "aof_sync": "always", "shards": 1,
                          "aof_compaction_after_bytes": 0, "serial_clients": 1,
                          "queue_observer_clients": 1},
        "load_records_input": load_records, "observations": observations,
    }


def report(raw: dict, raw_name: str) -> str:
    load = raw["observations"]["load"]
    info_after = raw["observations"]["after_load_info"]
    judged = raw["observations"]["cold_after_ingest"]
    precision = [row["judged_relevance_precision_at_k"] for row in judged
                 if row["judged_relevance_precision_at_k"] is not None]
    return f"""# Memory search corpus result

Source SHA: `{raw['source_sha']}`. Binary SHA-256: `{raw['binary_sha256']}`.
Corpus: `{raw['corpus_version']}` (`{raw['corpus_sha256']}`); seed `{raw['seed']}`.
Platform: `{raw['platform']}`; CPU: `{raw['hardware']['cpu_model']}`;
logical CPUs: `{raw['hardware']['logical_cpus']}`;
system memory: `{raw['hardware']['system_memory_bytes']}` bytes.
Python: `{raw['python']}`. Raw inputs and observations: [{raw_name}]({raw_name}).

The run used {raw['load_records']} generated load records, one serial loopback
client, {raw['repetitions']} repetitions of {raw['searches_per_repetition']} query shapes,
one queue observer, one shard, and AOF record 2 with `always` sync.
The first query after ingestion and the first after recovery are recorded separately
from the repeated warm load. Updates, deletion, TTL expiry, and recovery were checked.

| Observation | Value |
| --- | ---: |
| Exact oracle recall on tested nonempty cases | 1.0 |
| Mean judged relevance precision@k on nonempty fixture queries | {statistics.mean(precision):.3f} |
| Serial query throughput | {load['throughput_queries_per_second']:.1f} queries/s |
| p50 / p95 / p99 round-trip latency | {load['p50_ns']/1e6:.3f} / {load['p95_ns']/1e6:.3f} / {load['p99_ns']/1e6:.3f} ms |
| Maximum sampled server RSS | {load['rss_max_observed_bytes'] if load['rss_max_observed_bytes'] is not None else 'pending'} bytes |
| Dataset logical charge after load | {info_after.get('dataset_logical_bytes')} bytes |
| Maximum sampled worker / AOF queue depth | {load['worker_queue_max_observed'] if load['worker_queue_max_observed'] is not None else 'pending'} / {load['aof_queue_max_observed'] if load['aof_queue_max_observed'] is not None else 'pending'} |

Exact correctness compares returned IDs and binary64 distance bytes with an
independent Python oracle on each query. Relevance judgments are fixture labels,
not geometric truth; their precision is reported separately. RSS and queues are
periodic samples and can miss peaks. This one machine and toy dataset establish
neither a service latency guarantee nor a general relevance claim. Runs without
RSS or queue samples remain pending for those measurements.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--load-records", type=int, default=256)
    parser.add_argument("--repetitions", type=int, default=100)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    raw = run(binary, args.output_dir, args.load_records, args.repetitions)
    raw_path = args.output_dir / "raw.json"
    raw_path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path = args.output_dir / "report.md"
    report_path.write_text(report(raw, raw_path.name), encoding="utf-8")
    print(report_path)


if __name__ == "__main__":
    main()
