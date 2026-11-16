#!/usr/bin/env python3
"""Run and independently validate the first complete ColumnScope matrix."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
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
        "--runner-binary",
        type=Path,
        default=Path("target/release/columnscope-bench-runner"),
    )
    parser.add_argument(
        "--trial-binary",
        type=Path,
        default=Path("target/release/columnscope-bench-trial"),
    )
    parser.add_argument("--warmups", type=int, default=0)
    parser.add_argument("--repetitions", type=int, default=1)
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
        check=True,
        text=True,
        capture_output=capture,
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


def execute_matrix(args: argparse.Namespace, dataset_sha: str) -> tuple[list[dict], dict]:
    all_records: list[dict] = []
    fieldnames: list[str] | None = None
    environment: dict | None = None
    with tempfile.TemporaryDirectory(prefix="columnscope-matrix-") as temporary:
        temporary_root = Path(temporary)
        for number in range(1, 13):
            workload = f"Q{number:02d}"
            for mode, engine, planning in CONFIGURATIONS:
                configuration = temporary_root / f"{workload}-{mode}-{engine}"
                run(
                    [
                        str(args.runner_binary),
                        "--trial-binary",
                        str(args.trial_binary),
                        "--input",
                        str(args.input),
                        "--dataset-manifest",
                        str(args.dataset_manifest),
                        "--cargo-lock",
                        str(args.cargo_lock),
                        "--workload",
                        workload,
                        "--engine",
                        engine,
                        "--mode",
                        mode,
                        "--planning",
                        planning,
                        "--layout",
                        args.layout,
                        "--batch-size",
                        str(args.batch_size),
                        "--threads",
                        str(args.threads),
                        "--warmups",
                        str(args.warmups),
                        "--repetitions",
                        str(args.repetitions),
                        "--source-sha",
                        args.source_sha,
                        "--output-dir",
                        str(configuration),
                    ]
                )
                with (configuration / "raw_trials.csv").open(newline="") as source:
                    reader = csv.DictReader(source)
                    if fieldnames is None:
                        fieldnames = list(reader.fieldnames or [])
                    elif fieldnames != reader.fieldnames:
                        raise ValueError("trial CSV schemas changed within the matrix")
                    records = list(reader)
                if len(records) != args.repetitions:
                    raise ValueError(f"{workload} {mode} {engine} emitted the wrong trial count")
                for record in records:
                    if record["dataset_checksum"] != dataset_sha:
                        raise ValueError("trial dataset checksum does not match the manifest")
                    if record["source_sha"] != args.source_sha:
                        raise ValueError("trial source SHA does not match the requested revision")
                all_records.extend(records)

                candidate_environment = json.loads(
                    (configuration / "environment.json").read_text()
                )
                if environment is None:
                    environment = candidate_environment
                elif environment != candidate_environment:
                    raise ValueError("environment metadata changed within the matrix")

    if fieldnames is None or environment is None:
        raise ValueError("matrix did not produce records")
    return all_records, {"fieldnames": fieldnames, "environment": environment}


def record_result(records: list[dict], workload: str, mode: str, engine: str) -> tuple[int, str]:
    selected = [
        record
        for record in records
        if record["workload"] == workload
        and record["mode"] == mode
        and record["engine"] == engine
    ]
    if not selected:
        raise ValueError(f"missing records for {workload} {mode} {engine}")
    values = {(int(record["result_rows"]), record["result_checksum"]) for record in selected}
    if len(values) != 1:
        raise ValueError(f"result drift across repetitions for {workload} {mode} {engine}")
    return values.pop()


def validate_oracle(args: argparse.Namespace, records: list[dict]) -> dict:
    import duckdb

    connection = duckdb.connect(":memory:")
    connection.execute("SET threads TO 1")
    connection.from_parquet(str(args.input)).create_view("benchmark_data")
    validations = []
    try:
        for number in range(1, 13):
            workload = f"Q{number:02d}"
            sql = (args.benchmarks_dir / "oracle" / f"q{number:02d}.sql").read_text()
            cursor = connection.execute(sql)
            kinds = [value_kind(column[1]) for column in cursor.description]
            oracle_rows, oracle_checksum = result_checksum(cursor_rows(cursor), kinds)
            comparisons = []
            for mode in ("execution_only", "end_to_end"):
                batch = record_result(records, workload, mode, "batch")
                scalar = record_result(records, workload, mode, "scalar")
                if batch != scalar:
                    raise ValueError(f"scalar/batch mismatch for {workload} {mode}")
                if batch != (oracle_rows, oracle_checksum):
                    raise ValueError(
                        f"DuckDB mismatch for {workload} {mode}: "
                        f"ColumnScope={batch}, DuckDB={(oracle_rows, oracle_checksum)}"
                    )
                comparisons.append(
                    {
                        "mode": mode,
                        "rows": batch[0],
                        "result_checksum": batch[1],
                        "batch_scalar_match": True,
                        "duckdb_match": True,
                    }
                )
            validations.append(
                {
                    "workload": workload,
                    "oracle_rows": oracle_rows,
                    "oracle_checksum": oracle_checksum,
                    "comparisons": comparisons,
                    "status": "match",
                }
            )
    finally:
        connection.close()
    return {
        "format_version": 1,
        "source_sha": args.source_sha,
        "layout": args.layout,
        "batch_size": args.batch_size,
        "threads": args.threads,
        "duckdb_version": duckdb.__version__,
        "workloads": validations,
        "status": "match",
    }


def write_outputs(
    args: argparse.Namespace,
    records: list[dict],
    metadata: dict,
    validation: dict,
) -> None:
    if args.output_dir.exists():
        raise FileExistsError(f"output directory already exists: {args.output_dir}")
    args.output_dir.mkdir(parents=True)
    with (args.output_dir / "raw_trials.csv").open("w", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=metadata["fieldnames"])
        writer.writeheader()
        writer.writerows(records)
    (args.output_dir / "environment.json").write_text(
        json.dumps(metadata["environment"], indent=2) + "\n"
    )
    shutil.copyfile(args.dataset_manifest, args.output_dir / "dataset_manifest.json")
    (args.output_dir / "query_validation.json").write_text(
        json.dumps(validation, indent=2) + "\n"
    )


def main() -> None:
    args = parse_args()
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
                "--bin",
                "columnscope-bench-runner",
            ]
        )
    records, metadata = execute_matrix(args, layout_record["sha256"])
    validation = validate_oracle(args, records)
    write_outputs(args, records, metadata, validation)
    print(
        f"validated {len(validation['workloads'])} workloads and wrote "
        f"{len(records)} raw trial records to {args.output_dir}"
    )


if __name__ == "__main__":
    main()
