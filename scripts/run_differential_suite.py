#!/usr/bin/env python3
"""Run seeded small-data differential checks against DuckDB."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_benchmark_matrix import (
    cursor_rows,
    result_checksum,
    value_kind,
)


DEFAULT_SEED = 20261207
DEFAULT_CASES = 120
BOUNDARY_ROWS = (0, 1, 8191, 8192, 8193)
FIELD_TYPES = (
    ("id", "int64"),
    ("value", "int64"),
    ("metric", "float64"),
    ("flag", "boolean"),
    ("group_key", "utf8"),
)
CONFIGURATIONS = (
    ("batch", "unoptimized"),
    ("batch", "optimized"),
    ("scalar", "unoptimized"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate seeded ColumnScope plans against DuckDB."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--cases", type=int, default=DEFAULT_CASES)
    parser.add_argument(
        "--trial-binary",
        type=Path,
        default=Path("target/release/columnscope-bench-trial"),
    )
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args()


def run(command: Sequence[str], *, capture: bool = False) -> str:
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        detail = f": {stderr}" if stderr else ""
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}{detail}"
        )
    if not capture and completed.stdout:
        print(completed.stdout, end="")
    return completed.stdout.strip() if capture else ""


def validate_source(source_sha: str) -> None:
    if len(source_sha) != 40 or any(
        character not in "0123456789abcdef" for character in source_sha
    ):
        raise ValueError("--source-sha must be a lowercase 40-character Git SHA")
    actual = run(["git", "rev-parse", "HEAD"], capture=True)
    if actual != source_sha:
        raise ValueError(f"--source-sha {source_sha} does not match HEAD {actual}")
    if run(["git", "status", "--porcelain"], capture=True):
        raise ValueError("differential suites require a clean working tree")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def column(name: str) -> dict:
    return {"type": "column", "name": name}


def literal(kind: str, value: object) -> dict:
    return {"type": "literal", "literal": {"type": kind, "value": value}}


def binary(kind: str, left: dict, right: dict) -> dict:
    return {"type": kind, "left": left, "right": right}


def filtered_input(predicate: dict | None) -> dict:
    scan = {"type": "scan"}
    if predicate is None:
        return scan
    return {"type": "filter", "input": scan, "predicate": predicate}


def project_plan(
    input_schema: dict,
    predicate: dict,
    expressions: list[tuple[str, str]],
) -> dict:
    return {
        "format_version": 1,
        "input_schema": input_schema,
        "plan": {
            "type": "project",
            "input": filtered_input(predicate),
            "expressions": [
                {"alias": alias, "expr": column(name)} for alias, name in expressions
            ],
        },
    }


def aggregate_plan(
    input_schema: dict,
    predicate: dict | None,
    group_by: list[str],
    aggregates: list[tuple[str, str, str | None]],
) -> dict:
    values = []
    for function, alias, name in aggregates:
        value = {"function": function, "alias": alias}
        if name is not None:
            value["expr"] = column(name)
        values.append(value)
    return {
        "format_version": 1,
        "input_schema": input_schema,
        "plan": {
            "type": "aggregate",
            "input": filtered_input(predicate),
            "group_by": [
                {"alias": name, "expr": column(name)} for name in group_by
            ],
            "aggregates": values,
        },
    }


def case_definition(
    case_index: int,
    case_rng: random.Random,
    input_schema: dict,
) -> tuple[str, dict, str]:
    threshold = case_rng.randint(-45, 45)
    metric_threshold = case_rng.randint(-80, 80) / 4.0
    group_literal = case_rng.choice(["", "a", "b", "c"])
    template = case_index % 12

    if template == 0:
        predicate = binary("lt", column("value"), literal("int64", threshold))
        plan = project_plan(
            input_schema,
            predicate,
            [("id", "id"), ("value", "value")],
        )
        sql = f"SELECT id, value FROM case_data WHERE value < {threshold}"
        return "int_lt_project", plan, sql
    if template == 1:
        predicate = binary("gt_eq", column("value"), literal("int64", threshold))
        plan = project_plan(
            input_schema,
            predicate,
            [("id", "id"), ("group_key", "group_key")],
        )
        sql = f"SELECT id, group_key FROM case_data WHERE value >= {threshold}"
        return "int_gte_project", plan, sql
    if template == 2:
        predicate = binary(
            "and",
            binary("eq", column("flag"), literal("boolean", True)),
            binary("lt_eq", column("value"), literal("int64", threshold)),
        )
        plan = project_plan(
            input_schema,
            predicate,
            [("id", "id"), ("metric", "metric")],
        )
        sql = (
            "SELECT id, metric FROM case_data "
            f"WHERE flag = TRUE AND value <= {threshold}"
        )
        return "boolean_and_project", plan, sql
    if template == 3:
        predicate = binary(
            "or",
            {"type": "is_null", "expr": column("value")},
            binary("eq", column("flag"), literal("boolean", False)),
        )
        plan = project_plan(
            input_schema,
            predicate,
            [("id", "id"), ("flag", "flag")],
        )
        sql = "SELECT id, flag FROM case_data WHERE value IS NULL OR flag = FALSE"
        return "null_or_project", plan, sql
    if template == 4:
        predicate = {
            "type": "not",
            "expr": binary(
                "gt",
                column("metric"),
                literal("float64", metric_threshold),
            ),
        }
        plan = project_plan(
            input_schema,
            predicate,
            [("id", "id"), ("metric", "metric")],
        )
        sql = (
            "SELECT id, metric FROM case_data "
            f"WHERE NOT (metric > {metric_threshold})"
        )
        return "float_not_project", plan, sql
    if template == 5:
        predicate = binary(
            "not_eq",
            column("group_key"),
            literal("utf8", group_literal),
        )
        plan = project_plan(
            input_schema,
            predicate,
            [("id", "id"), ("group_key", "group_key")],
        )
        sql = (
            "SELECT id, group_key FROM case_data "
            f"WHERE group_key <> {sql_string(group_literal)}"
        )
        return "utf8_neq_project", plan, sql
    if template == 6:
        plan = aggregate_plan(input_schema, None, [], aggregate_set())
        sql = (
            "SELECT COUNT(*), COUNT(value), SUM(value), "
            "MIN(value), MAX(value), AVG(value) FROM case_data"
        )
        return "global_aggregate", plan, sql
    if template == 7:
        aggregates = [
            ("count_star", "rows", None),
            ("count", "value_count", "value"),
            ("sum", "value_sum", "value"),
        ]
        plan = aggregate_plan(input_schema, None, ["group_key"], aggregates)
        sql = (
            "SELECT group_key, COUNT(*), COUNT(value), SUM(value) "
            "FROM case_data GROUP BY group_key"
        )
        return "utf8_group_aggregate", plan, sql
    if template == 8:
        aggregates = [
            ("count_star", "rows", None),
            ("min", "value_min", "value"),
            ("max", "value_max", "value"),
        ]
        plan = aggregate_plan(input_schema, None, ["flag"], aggregates)
        sql = (
            "SELECT flag, COUNT(*), MIN(value), MAX(value) "
            "FROM case_data GROUP BY flag"
        )
        return "boolean_group_aggregate", plan, sql
    if template == 9:
        aggregates = [
            ("count_star", "rows", None),
            ("sum", "value_sum", "value"),
        ]
        plan = aggregate_plan(
            input_schema,
            None,
            ["group_key", "flag"],
            aggregates,
        )
        sql = (
            "SELECT group_key, flag, COUNT(*), SUM(value) "
            "FROM case_data GROUP BY group_key, flag"
        )
        return "composite_group_aggregate", plan, sql
    if template == 10:
        predicate = binary(
            "gt",
            column("metric"),
            literal("float64", metric_threshold),
        )
        aggregates = [
            ("count_star", "rows", None),
            ("sum", "value_sum", "value"),
        ]
        plan = aggregate_plan(
            input_schema,
            predicate,
            ["group_key"],
            aggregates,
        )
        sql = (
            "SELECT group_key, COUNT(*), SUM(value) FROM case_data "
            f"WHERE metric > {metric_threshold} GROUP BY group_key"
        )
        return "filtered_group_aggregate", plan, sql

    predicate = {"type": "is_not_null", "expr": column("value")}
    aggregates = [
        ("count_star", "rows", None),
        ("sum", "value_sum", "value"),
    ]
    plan = aggregate_plan(input_schema, predicate, [], aggregates)
    sql = "SELECT COUNT(*), SUM(value) FROM case_data WHERE value IS NOT NULL"
    return "not_null_global_aggregate", plan, sql


def aggregate_set() -> list[tuple[str, str, str | None]]:
    return [
        ("count_star", "rows", None),
        ("count", "value_count", "value"),
        ("sum", "value_sum", "value"),
        ("min", "value_min", "value"),
        ("max", "value_max", "value"),
        ("avg", "value_avg", "value"),
    ]


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def row_count_for(case_index: int, case_rng: random.Random) -> int:
    if case_index < len(BOUNDARY_ROWS):
        return BOUNDARY_ROWS[case_index]
    return case_rng.randint(0, 127)


def batch_size_for(case_index: int, case_rng: random.Random) -> int:
    if case_index < len(BOUNDARY_ROWS):
        return 8192
    return case_rng.choice([1, 2, 7, 31, 8192])


def generate_rows(row_count: int, case_rng: random.Random) -> list[tuple]:
    groups = ["", "a", "b", "c"]
    rows = []
    for row in range(row_count):
        value = None if case_rng.random() < 0.2 else case_rng.randint(-50, 50)
        metric = None if case_rng.random() < 0.2 else case_rng.randint(-100, 100) / 4.0
        flag = None if case_rng.random() < 0.2 else bool(case_rng.getrandbits(1))
        group_key = None if case_rng.random() < 0.2 else case_rng.choice(groups)
        rows.append((row, value, metric, flag, group_key))
    return rows


def schema_for_rows(rows: list[tuple]) -> dict:
    return {
        "fields": [
            {
                "name": name,
                "data_type": data_type,
                "nullable": any(row[index] is None for row in rows),
            }
            for index, (name, data_type) in enumerate(FIELD_TYPES)
        ]
    }


def write_case(connection: object, path: Path, rows: list[tuple]) -> None:
    connection.execute("DROP TABLE IF EXISTS case_data")
    connection.execute(
        "CREATE TABLE case_data("
        "id BIGINT, value BIGINT, metric DOUBLE, "
        "flag BOOLEAN, group_key VARCHAR)"
    )
    if rows:
        connection.executemany("INSERT INTO case_data VALUES (?, ?, ?, ?, ?)", rows)
    escaped = str(path).replace("'", "''")
    connection.execute(
        f"COPY case_data TO '{escaped}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
    )


def execute_trial(
    args: argparse.Namespace,
    plan_path: Path,
    input_path: Path,
    dataset_checksum: str,
    case_id: str,
    engine: str,
    planning: str,
    batch_size: int,
) -> dict:
    output = run(
        [
            str(args.trial_binary),
            "plan-trial",
            "--plan",
            str(plan_path),
            "--input",
            str(input_path),
            "--workload",
            case_id,
            "--engine",
            engine,
            "--mode",
            "end_to_end",
            "--planning",
            planning,
            "--layout",
            "differential",
            "--batch-size",
            str(batch_size),
            "--threads",
            "1",
            "--trial",
            "0",
            "--warmup",
            "false",
            "--source-sha",
            args.source_sha,
            "--dataset-checksum",
            dataset_checksum,
        ],
        capture=True,
    )
    return json.loads(output)


def oracle_result(connection: object, sql: str) -> tuple[int, str]:
    cursor = connection.execute(sql)
    kinds = [value_kind(column_info[1]) for column_info in cursor.description]
    return result_checksum(cursor_rows(cursor), kinds)


def run_suite(args: argparse.Namespace) -> dict:
    import duckdb

    suite_rng = random.Random(args.seed)
    connection = duckdb.connect(":memory:")
    connection.execute("SET threads TO 1")
    cases = []
    try:
        with tempfile.TemporaryDirectory(
            prefix="columnscope-differential-"
        ) as temporary:
            root = Path(temporary)
            for case_index in range(args.cases):
                case_seed = suite_rng.getrandbits(64)
                case_rng = random.Random(case_seed)
                row_count = row_count_for(case_index, case_rng)
                rows = generate_rows(row_count, case_rng)
                template, plan, sql = case_definition(
                    case_index,
                    case_rng,
                    schema_for_rows(rows),
                )
                case_id = f"D{case_index:03d}"
                input_path = root / f"{case_id}.parquet"
                plan_path = root / f"{case_id}.json"
                write_case(connection, input_path, rows)
                plan_path.write_text(json.dumps(plan, indent=2) + "\n")
                dataset_checksum = sha256_file(input_path)
                oracle_rows, oracle_checksum = oracle_result(connection, sql)
                batch_size = batch_size_for(case_index, case_rng)
                comparisons = []
                for engine, planning in CONFIGURATIONS:
                    record = execute_trial(
                        args,
                        plan_path,
                        input_path,
                        dataset_checksum,
                        case_id,
                        engine,
                        planning,
                        batch_size,
                    )
                    actual = (record["result_rows"], record["result_checksum"])
                    expected = (oracle_rows, oracle_checksum)
                    if actual != expected:
                        raise ValueError(
                            f"{case_id} {engine}/{planning} mismatch: "
                            f"ColumnScope={actual}, DuckDB={expected}"
                        )
                    comparisons.append(
                        {
                            "engine": engine,
                            "planning": planning,
                            "rows": record["result_rows"],
                            "result_checksum": record["result_checksum"],
                            "duckdb_match": True,
                        }
                    )
                cases.append(
                    {
                        "case": case_id,
                        "seed": case_seed,
                        "template": template,
                        "input_rows": row_count,
                        "batch_size": batch_size,
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
        "suite_seed": args.seed,
        "case_count": len(cases),
        "source_sha": args.source_sha,
        "duckdb_version": duckdb.__version__,
        "boundary_rows": list(BOUNDARY_ROWS),
        "configurations": [
            {"engine": engine, "planning": planning}
            for engine, planning in CONFIGURATIONS
        ],
        "cases": cases,
        "status": "match",
    }


def main() -> None:
    args = parse_args()
    if args.cases < 100:
        raise ValueError("--cases must be at least 100")
    if args.output.exists():
        raise FileExistsError(f"output already exists: {args.output}")
    validate_source(args.source_sha)
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
    result = run_suite(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        f"validated {result['case_count']} seeded cases against DuckDB "
        f"with {len(CONFIGURATIONS)} engine/planning configurations"
    )


if __name__ == "__main__":
    main()
