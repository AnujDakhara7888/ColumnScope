#!/usr/bin/env python3
"""Run and independently validate a matched ColumnScope benchmark matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import random
import shutil
import struct
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable, Sequence


CONFIGURATIONS = (
    ("execution_only", "batch", "unoptimized"),
    ("execution_only", "scalar", "unoptimized"),
    ("end_to_end", "batch", "optimized"),
    ("end_to_end", "scalar", "unoptimized"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Q01-Q12 through scalar/batch execution and DuckDB validation."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument(
        "--dataset-manifest",
        type=Path,
        default=Path("benchmarks/dataset_manifest.json"),
    )
    parser.add_argument("--cargo-lock", type=Path, default=Path("Cargo.lock"))
    parser.add_argument("--benchmarks-dir", type=Path, default=Path("benchmarks"))
    parser.add_argument(
        "--trial-binary",
        type=Path,
        default=Path("target/release/columnscope-bench-trial"),
    )
    parser.add_argument("--warmups", type=int, default=0)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--order-seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--layout", default="random")
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Use existing release binaries instead of rebuilding them.",
    )
    return parser.parse_args()


def run(command: Sequence[str], *, capture: bool = False) -> str:
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=capture,
    )
    if completed.returncode:
        raise RuntimeError(
            f"{command[0]} exited with {completed.returncode}: "
            f"{(completed.stderr or '').strip()}"
        )
    return completed.stdout.strip() if capture else ""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def value_kind(type_name: object) -> str:
    name = str(type_name).upper()
    if name in {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
    }:
        return "int64"
    if name in {"FLOAT", "DOUBLE", "REAL"}:
        return "float64"
    if name in {"BOOLEAN", "BOOL"}:
        return "boolean"
    if name in {"VARCHAR", "STRING", "TEXT"}:
        return "utf8"
    raise ValueError(f"unsupported DuckDB result type: {type_name}")


def update_value(digest: hashlib._Hash, kind: str, value: object) -> None:
    tags = {"int64": 1, "float64": 2, "boolean": 3, "utf8": 4}
    digest.update(bytes((tags[kind],)))
    if value is None:
        digest.update(b"\x00")
        return
    digest.update(b"\x01")
    if kind == "int64":
        integer = int(value)
        if not -(2**63) <= integer < 2**63:
            raise OverflowError(f"oracle integer is outside Int64: {integer}")
        digest.update(struct.pack("<q", integer))
    elif kind == "float64":
        digest.update(struct.pack("<d", float(value)))
    elif kind == "boolean":
        digest.update(bytes((int(bool(value)),)))
    else:
        encoded = str(value).encode("utf-8")
        digest.update(struct.pack("<Q", len(encoded)))
        digest.update(encoded)


def result_checksum(
    rows: Iterable[Sequence[object]], kinds: Sequence[str]
) -> tuple[int, str]:
    row_count = 0
    sums = [0, 0, 0, 0]
    xors = [0, 0, 0, 0]
    for row in rows:
        if len(row) != len(kinds):
            raise ValueError("oracle row width does not match its type list")
        row_digest = hashlib.sha256()
        row_digest.update(struct.pack("<Q", len(row)))
        for kind, value in zip(kinds, row, strict=True):
            update_value(row_digest, kind, value)
        encoded = row_digest.digest()
        row_count += 1
        for index in range(4):
            value = int.from_bytes(encoded[index * 8 : index * 8 + 8], "little")
            sums[index] = (sums[index] + value) % 2**64
            xors[index] ^= value

    digest = hashlib.sha256()
    digest.update(struct.pack("<Q", row_count))
    for value in (*sums, *xors):
        digest.update(struct.pack("<Q", value))
    return row_count, digest.hexdigest()


def cursor_rows(cursor: object, chunk_size: int = 8192) -> Iterable[Sequence[object]]:
    while rows := cursor.fetchmany(chunk_size):
        yield from rows


def validate_source(source_sha: str) -> None:
    if len(source_sha) != 40 or any(character not in "0123456789abcdef" for character in source_sha):
        raise ValueError("--source-sha must be a lowercase 40-character Git SHA")
    actual = run(["git", "rev-parse", "HEAD"], capture=True)
    if actual != source_sha:
        raise ValueError(f"--source-sha {source_sha} does not match HEAD {actual}")
    if run(["git", "status", "--porcelain"], capture=True):
        raise ValueError("benchmark matrices require a clean working tree")


def load_manifest(path: Path, layout: str, input_path: Path) -> tuple[dict, dict]:
    manifest = json.loads(path.read_text())
    layout_record = next(
        (candidate for candidate in manifest["layouts"] if candidate["name"] == layout),
        None,
    )
    if layout_record is None:
        raise ValueError(f"layout {layout!r} is absent from {path}")
    actual_sha = sha256_file(input_path)
    if actual_sha != layout_record["sha256"]:
        raise ValueError(
            f"input SHA-256 {actual_sha} does not match manifest {layout_record['sha256']}"
        )
    return manifest, layout_record


def trial_schedule(warmups: int, repetitions: int, seed: int):
    rng = random.Random(seed)
    configurations = [
        (f"Q{number:02d}", mode, engine, planning)
        for number in range(1, 13)
        for mode, engine, planning in CONFIGURATIONS
    ]
    for warmup, rounds in ((True, warmups), (False, repetitions)):
        for trial in range(rounds):
            order = configurations.copy()
            rng.shuffle(order)
            for configuration in order:
                yield warmup, trial, configuration


def execute_matrix(
    args: argparse.Namespace, dataset_sha: str, oracle: dict[str, tuple[int, str]]
) -> tuple[list[dict], dict]:
    binary = str(args.trial_binary)
    environment = json.loads(run([
        binary, "environment",
        "--source-sha", args.source_sha,
        "--cargo-lock", str(args.cargo_lock),
        "--dataset-manifest", str(args.dataset_manifest),
    ], capture=True))
    environment["matrix"] = {
        "host_release": platform.platform(),
        "python_version": platform.python_version(),
        "trial_binary_sha256": sha256_file(args.trial_binary),
        "order_seed": args.order_seed,
        "order": "shuffled configurations within each serial round",
        "cache_policy": "OS page cache retained, no cache eviction",
        "warmups": args.warmups,
        "repetitions": args.repetitions,
    }
    records = []
    for sequence, (warmup, trial, configuration) in enumerate(
        trial_schedule(args.warmups, args.repetitions, args.order_seed)
    ):
        workload, mode, engine, planning = configuration
        expected = {
            "workload": workload, "mode": mode, "engine": engine,
            "planning": planning, "layout": args.layout, "batch_size": args.batch_size,
            "threads": args.threads, "trial": trial, "warmup": warmup,
            "source_sha": args.source_sha, "dataset_checksum": dataset_sha,
        }
        command = [binary, "trial", "--input", str(args.input)]
        for key, value in expected.items():
            argument = str(value).lower() if isinstance(value, bool) else str(value)
            command.extend(("--" + key.replace("_", "-"), argument))
        record = json.loads(run(command, capture=True))
        if any(record[key] != value for key, value in expected.items()):
            raise ValueError(f"trial {sequence} returned unexpected configuration metadata")
        if not 0 < record["setup_peak_rss_bytes"] <= record["peak_rss_bytes"]:
            raise ValueError(f"trial {sequence} returned invalid peak RSS")
        if record["runtime_ns"] <= 0:
            raise ValueError(f"trial {sequence} returned invalid runtime")
        result = (record["result_rows"], record["result_checksum"])
        if result != oracle[workload]:
            raise ValueError(
                f"DuckDB mismatch for trial {sequence} {configuration}: "
                f"ColumnScope={result}, DuckDB={oracle[workload]}"
            )
        if warmup:
            continue
        counters = record.pop("counters")
        counters["operator_output_rows"] = counters.pop("output_rows")
        del record["warmup"]
        records.append({**record, **counters, "sequence": sequence})
    return records, environment


def read_oracle(args: argparse.Namespace) -> tuple[dict[str, tuple[int, str]], str]:
    import duckdb

    results = {}
    with duckdb.connect(":memory:") as connection:
        connection.execute("SET threads TO 1")
        connection.from_parquet(str(args.input)).create_view("benchmark_data")
        for number in range(1, 13):
            sql = (args.benchmarks_dir / "oracle" / f"q{number:02d}.sql").read_text()
            cursor = connection.execute(sql)
            kinds = [value_kind(column[1]) for column in cursor.description]
            results[f"Q{number:02d}"] = result_checksum(cursor_rows(cursor), kinds)
    return results, duckdb.__version__


def validation_report(
    args: argparse.Namespace,
    oracle: dict[str, tuple[int, str]],
    duckdb_version: str,
) -> dict:
    return {
        "format_version": 1,
        "source_sha": args.source_sha,
        "layout": args.layout,
        "batch_size": args.batch_size,
        "threads": args.threads,
        "duckdb_version": duckdb_version,
        "warmups_per_configuration": args.warmups,
        "measured_trials_per_configuration": args.repetitions,
        "workloads": [
            {
                "workload": workload,
                "oracle_rows": rows,
                "oracle_checksum": checksum,
                "comparisons": [
                    {
                        "mode": mode,
                        "rows": rows,
                        "result_checksum": checksum,
                        "batch_scalar_match": True,
                        "duckdb_match": True,
                    }
                    for mode in ("execution_only", "end_to_end")
                ],
                "status": "match",
            }
            for workload, (rows, checksum) in oracle.items()
        ],
        "status": "match",
    }


def write_outputs(
    args: argparse.Namespace,
    records: list[dict],
    environment: dict,
    validation: dict,
) -> None:
    if args.output_dir.exists():
        raise FileExistsError(f"output directory already exists: {args.output_dir}")
    args.output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.output_dir.parent) as temporary:
        staging = Path(temporary) / "results"
        staging.mkdir()
        with (staging / "raw_trials.csv").open("w", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
        (staging / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
        shutil.copyfile(args.dataset_manifest, staging / "dataset_manifest.json")
        (staging / "query_validation.json").write_text(json.dumps(validation, indent=2) + "\n")
        staging.rename(args.output_dir)


def main() -> None:
    args = parse_args()
    if args.output_dir.exists():
        raise FileExistsError(f"output directory already exists: {args.output_dir}")
    if args.warmups < 0 or args.repetitions <= 0:
        raise ValueError("warmups must be nonnegative and repetitions must be positive")
    if args.batch_size <= 0 or args.threads != 1:
        raise ValueError("the version 1 matrix requires a positive batch size and one thread")
    validate_source(args.source_sha)
    _, layout_record = load_manifest(args.dataset_manifest, args.layout, args.input)
    if not args.skip_build:
        run(
            [
                "cargo",
                "build",
                "--locked",
                "--release",
                "--bin",
                "columnscope-bench-trial",
            ]
        )
    oracle, duckdb_version = read_oracle(args)
    records, metadata = execute_matrix(args, layout_record["sha256"], oracle)
    load_manifest(args.dataset_manifest, args.layout, args.input)
    validation = validation_report(args, oracle, duckdb_version)
    write_outputs(args, records, metadata, validation)
    print(
        f"validated {len(validation['workloads'])} workloads and wrote "
        f"{len(records)} raw trial records to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
