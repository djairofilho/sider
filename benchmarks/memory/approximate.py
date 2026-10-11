#!/usr/bin/env python3
"""Screen derived grid indexes against exact search on corpus v1."""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import random
import statistics
import subprocess
import time
import tracemalloc
from pathlib import Path

from run import CORPUS, eligible, oracle, percentile, rss_bytes

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "docs" / "evidence" / "issue-112-corpus-e2475bd"
CENTERS = (-0.75, -0.25, 0.25, 0.75)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def make_records(count: int, seed: int) -> list[dict]:
    generator = random.Random(seed)
    records = []
    for index in range(count):
        records.append({"name": f"load-{index:04d}", "id_hex": f"{index:04x}",
                        "namespace": "team-a" if index % 2 else "team-b",
                        "vector": [generator.uniform(-1, 1), generator.uniform(-1, 1)],
                        "metadata": {"category": ["BYTES", "policy" if index % 4 else "note"]}})
    return records


class Grid:
    """Non-production, fixed-centroid two-axis screening prototype."""

    def __init__(self, records: list[dict], probes: int) -> None:
        self.probes = probes
        self.cells: dict[tuple[int, int], list[dict]] = {}
        for record in records:
            x, y = record["vector"]
            cell = (min(range(4), key=lambda i: abs(x - CENTERS[i])),
                    min(range(4), key=lambda i: abs(y - CENTERS[i])))
            self.cells.setdefault(cell, []).append(record)

    def search(self, query: dict) -> list[list[bytes]]:
        x, y = query["vector"]
        ranked_cells = sorted(
            ((cx - x) ** 2 + (cy - y) ** 2, ix, iy)
            for ix, cx in enumerate(CENTERS) for iy, cy in enumerate(CENTERS)
        )
        candidates = [record for _, ix, iy in ranked_cells[:self.probes]
                      for record in self.cells.get((ix, iy), ())]
        return oracle(candidates, query)

    def serialized_size(self) -> int:
        payload = [[ix, iy, [r["id_hex"] for r in rows]]
                   for (ix, iy), rows in sorted(self.cells.items())]
        return len(json.dumps(payload, separators=(",", ":")).encode())

    def layout_hash(self) -> str:
        payload = [[ix, iy, [r["id_hex"] for r in rows]]
                   for (ix, iy), rows in sorted(self.cells.items())]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


class XWindow:
    """Non-production sorted-axis window with a fixed candidate budget."""

    def __init__(self, records: list[dict], width: int = 64) -> None:
        self.width = width
        self.records = sorted(records, key=lambda record: (record["vector"][0], record["id_hex"]))
        self.positions = [record["vector"][0] for record in self.records]

    def search(self, query: dict) -> list[list[bytes]]:
        x = query["vector"][0]
        right = bisect.bisect_left(self.positions, x)
        left = right - 1
        candidates = []
        while len(candidates) < self.width and (left >= 0 or right < len(self.records)):
            if left >= 0 and (right == len(self.records) or
                              abs(self.positions[left] - x) <= abs(self.positions[right] - x)):
                candidates.append(self.records[left])
                left -= 1
            else:
                candidates.append(self.records[right])
                right += 1
        return oracle(candidates, query)

    def serialized_size(self) -> int:
        return len(json.dumps([(r["id_hex"], r["vector"][0]) for r in self.records],
                              separators=(",", ":")).encode())

    def layout_hash(self) -> str:
        payload = [(r["id_hex"], r["vector"][0]) for r in self.records]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def recall(actual: list[list[bytes]], wanted: list[list[bytes]]) -> float | None:
    if not wanted:
        return None
    return len({row[0] for row in actual} & {row[0] for row in wanted}) / len(wanted)


def measure(records: list[dict], queries: list[dict], repetitions: int) -> dict:
    methods = {"exact": lambda: None, "grid_1_probe": lambda: Grid(records, 1),
               "grid_4_probes": lambda: Grid(records, 4),
               "x_window_64": lambda: XWindow(records)}
    expected = [oracle(records, query) for query in queries]
    result = {}
    for name, build in methods.items():
        builds = []
        allocations = []
        index = None
        for _ in range(3):
            tracemalloc.start()
            begun = time.perf_counter_ns()
            index = build()
            builds.append(time.perf_counter_ns() - begun)
            allocations.append(tracemalloc.get_traced_memory()[1])
            tracemalloc.stop()
        query_result = [oracle(records, query) if index is None else index.search(query)
                        for query in queries]
        per_query_recall = [recall(actual, wanted)
                            for actual, wanted in zip(query_result, expected)]
        latencies = []
        rss_samples = []
        began_all = time.perf_counter_ns()
        for _ in range(repetitions):
            for query in queries:
                begun = time.perf_counter_ns()
                actual = oracle(records, query) if index is None else index.search(query)
                latencies.append(time.perf_counter_ns() - begun)
                assert actual == query_result[queries.index(query)]
            resident = rss_bytes(os.getpid())
            if resident is not None:
                rss_samples.append(resident)
        elapsed = time.perf_counter_ns() - began_all
        result[name] = {
            "build_ns_samples": builds, "peak_python_allocation_bytes_samples": allocations,
            "index_serialized_bytes": index.serialized_size() if index else 0,
            "index_layout_sha256": index.layout_hash() if index else None,
            "query_ids_hex": [[row[0].hex() for row in rows] for rows in query_result],
            "per_query_recall_at_k": per_query_recall,
            "mean_nonempty_recall_at_k": statistics.mean(x for x in per_query_recall if x is not None),
            "latency_ns_samples": latencies, "elapsed_ns": elapsed,
            "p50_ns": percentile(latencies, 50), "p95_ns": percentile(latencies, 95),
            "p99_ns": percentile(latencies, 99),
            "throughput_queries_per_second": len(latencies) * 1e9 / elapsed,
            "rss_bytes_samples": rss_samples,
            "max_sampled_process_rss_bytes": max(rss_samples) if rss_samples else None,
        }
    return result


def lifecycle(corpus: dict, queries: list[dict]) -> dict:
    records = json.loads(json.dumps(corpus["records"]))
    results = {}
    stages = ["ingest", "ttl_elapsed", "replacement", "deletion",
              "aof_recovered", "backup_restored", "replica_installed"]
    for stage in stages:
        if stage == "ttl_elapsed":
            for record in records:
                if "ttl_ms" in record:
                    record["expired"] = True
        elif stage == "replacement":
            next(r for r in records if r["name"] == "policy-b")["vector"] = [0.01, 0.0]
        elif stage == "deletion":
            next(r for r in records if r["name"] == "negative-policy")["deleted"] = True
        state = json.loads(json.dumps(records))
        wanted = [oracle(state, query) for query in queries]
        candidates = {}
        for name, build in (("grid_1_probe", lambda: Grid(state, 1)),
                            ("grid_4_probes", lambda: Grid(state, 4)),
                            ("x_window_64", lambda: XWindow(state))):
            begun = time.perf_counter_ns()
            index = build()
            build_ns = time.perf_counter_ns() - begun
            candidates[name] = {
                "rebuild_ns": build_ns, "layout_sha256": index.layout_hash(),
                "recall_at_k": [recall(index.search(query), rows)
                                for query, rows in zip(queries, wanted)],
            }
        results[stage] = candidates
    return results


def run(output: Path, repetitions: int) -> dict:
    assert git("status", "--porcelain") == "", "run from a clean checkout"
    assert repetitions >= 1
    corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    baseline = {}
    for platform_name in ("windows", "linux"):
        path = EVIDENCE / platform_name / "raw.json"
        source = json.loads(path.read_text(encoding="utf-8"))
        assert source["corpus_sha256"] == digest(CORPUS)
        assert source["load_records_input"] == make_records(256, corpus["seed"])
        baseline[platform_name] = {
            "file": str(path.relative_to(ROOT)).replace("\\", "/"),
            "sha256": digest(path), "server_binary_sha256": source["binary_sha256"],
            "source_sha": source["source_sha"],
            "server_serial_p99_ns": source["observations"]["load"]["p99_ns"],
            "server_concurrent_p99_ns": source["observations"]["concurrent_load"]["p99_ns"],
        }
    queries = [dict(query, k=10) for query in corpus["queries"]]
    observations = {str(size): measure(make_records(size, corpus["seed"]), queries, repetitions)
                    for size in (256, 1024)}
    return {"schema": "sider-memory-index-experiment/1",
            "source_sha": git("rev-parse", "HEAD"), "corpus_sha256": digest(CORPUS),
            "seed": corpus["seed"], "platform": __import__("platform").platform(),
            "python": __import__("platform").python_version(),
            "cpu_count": os.cpu_count(), "repetitions": repetitions,
            "prototype": "fixed 4x4 centroid grid with one/four probes; sorted x-axis window of 64 candidates; derived state",
            "baseline": baseline, "observations": observations,
            "lifecycle_rebuild": lifecycle(corpus, queries),
            "limitations": [
                "Python in-process timings are not Sider TCP or Rust timings.",
                "The grid is an offline screening prototype, not installed in Sider.",
                "AOF, backup and replica stages rebuild from equivalent corpus records; no indexed Sider process is exercised.",
                "Whole-process RSS samples do not isolate index memory or establish a peak.",
            ]}


def report(raw: dict) -> str:
    lines = ["# Derived-index screening", "",
             f"Source SHA: `{raw['source_sha']}`; corpus SHA-256: `{raw['corpus_sha256']}`.",
             f"Platform: `{raw['platform']}`; Python: `{raw['python']}`; logical CPUs: `{raw['cpu_count']}`.",
             f"Seed: `{raw['seed']}`; {raw['repetitions']} repetitions of five query shapes per method and size.",
             "Raw results: [raw.json](raw.json).", "",
             "| Records | Method | Mean recall@10 | p50/p95/p99, ms | Queries/s | Serialized index bytes | Median build, ms | Max sampled process RSS, bytes |",
             "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for size, methods in raw["observations"].items():
        for name, values in methods.items():
            builds = sorted(values["build_ns_samples"])
            lines.append(f"| {size} | {name} | {values['mean_nonempty_recall_at_k']:.3f} | "
                         f"{values['p50_ns']/1e6:.3f}/{values['p95_ns']/1e6:.3f}/{values['p99_ns']/1e6:.3f} | "
                         f"{values['throughput_queries_per_second']:.0f} | {values['index_serialized_bytes']} | "
                         f"{builds[1]/1e6:.3f} | {values['max_sampled_process_rss_bytes']} |")
    lines.extend(["", "The exact method is the quality oracle. Index recall is computed against its top-k",
                  "IDs after the same namespace and metadata filters. Grid storage is a serialized",
                  "prototype size; Python allocation peaks and lifecycle rebuild timings are in",
                  "the raw file. Sider server baselines from #112 are referenced there with exact",
                  "SHA and binary identity, but these in-process timings are not directly comparable.",
                  "The prototype does not implement Sider persistence or replication. A later",
                  "production index would need independent durable and replica validation.", ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--repetitions", type=int, default=100)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    raw = run(args.output_dir, args.repetitions)
    (args.output_dir / "raw.json").write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    (args.output_dir / "report.md").write_text(report(raw), encoding="utf-8")
    print(args.output_dir / "report.md")


if __name__ == "__main__":
    main()
