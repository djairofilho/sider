#!/usr/bin/env python3
"""Independent reference evaluator for sider-memory-corpus/1 (stdlib only)."""
import json
import math
import struct
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load_vectors(path, dimension):
    raw = path.read_bytes()
    width = 4 * dimension
    if len(raw) % width:
        raise ValueError("vector file length is not a whole number of vectors")
    return [struct.unpack("<" + "f" * dimension, raw[i:i + width])
            for i in range(0, len(raw), width)]


def distance(query, vector):
    # Explicit coordinate order and separate binary64 operations implement the
    # contract's no-fused-operation squared-L2 oracle.
    total = 0.0
    for q, x in zip(query, vector):
        delta = float(q) - float(x)
        total = total + delta * delta
    return total


def eligible(record, query, now):
    if record["deadline_ms"] is not None and record["deadline_ms"] <= now:
        return False
    if record["namespace"] != query["namespace"]:
        return False
    for predicate in query["metadata"]:
        stored = record["metadata"].get(predicate["field"])
        if stored is None or stored["type"] != predicate["type"]:
            return False
        if stored["value"] != predicate["value"]:
            return False
    return True


def main():
    corpus = json.loads((HERE / "corpus.json").read_text(encoding="utf-8"))
    expected = json.loads((HERE / "expected.json").read_text(encoding="utf-8"))
    vectors = load_vectors(HERE / corpus["vector_file"], corpus["dimension"])
    used_indices = {r["vector_index"] for r in corpus["records"]}
    used_indices.update(op["vector_index"] for op in corpus["operations"]
                        if "vector_index" in op)
    used_indices.update(q["vector_index"] for q in corpus["queries"])
    if any(index < 0 or index >= len(vectors) for index in used_indices):
        raise ValueError("a fixture references a vector outside vectors.f32le")
    if any(not math.isfinite(coordinate)
           for index in used_indices for coordinate in vectors[index]):
        raise ValueError("a used vector contains a non-finite coordinate")
    records = {r["id"]: r.copy() for r in corpus["records"]}
    for op in corpus["operations"]:
        if op["op"] == "update":
            records[op["id"]] = {k: v for k, v in op.items() if k != "op"}
        elif op["op"] == "delete":
            records.pop(op["id"], None)
        elif op["op"] != "recover":
            raise ValueError("unknown operation: " + op["op"])

    actual = {}
    for query in corpus["queries"]:
        qv = vectors[query["vector_index"]]
        candidates = [(distance(qv, vectors[r["vector_index"]]), rid)
                      for rid, r in records.items()
                      if eligible(r, query, corpus["clock_unix_ms"])]
        if any(not math.isfinite(score) for score, _ in candidates):
            raise ValueError("a candidate produced a non-finite distance")
        candidates.sort(key=lambda item: (item[0], item[1].encode("utf-8")))
        eligible_ids = sorted((rid for rid, r in records.items()
                               if eligible(r, query, corpus["clock_unix_ms"])),
                              key=lambda rid: rid.encode("utf-8"))
        actual[query["name"]] = {
            "eligible_ids": eligible_ids,
            "ranked_ids": [rid for _, rid in candidates[:query["k"]]],
        }
    if actual != expected["results"]:
        raise SystemExit("oracle mismatch\nactual=" + json.dumps(actual, indent=2))
    print("PASS: corpus inputs and independent exact oracle match expected.json")
    print("queries=" + str(len(corpus["queries"])) + " records_after_recovery=" + str(len(records)))


if __name__ == "__main__":
    main()
