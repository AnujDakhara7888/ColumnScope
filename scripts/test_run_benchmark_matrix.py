import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from scripts.run_benchmark_matrix import execute_matrix, result_checksum, trial_schedule, value_kind


class ResultChecksumTests(unittest.TestCase):
    def test_checksum_is_order_independent_and_duplicate_sensitive(self):
        kinds = ["int64", "utf8", "float64", "boolean"]
        first = (7, "north", 1.5, True)
        second = (-2, None, None, False)

        forward = result_checksum([first, second], kinds)
        reverse = result_checksum([second, first], kinds)
        duplicated = result_checksum([first, second, second], kinds)

        self.assertEqual(forward, reverse)
        self.assertNotEqual(forward, duplicated)
        self.assertEqual(forward[0], 2)

    def test_duckdb_types_map_to_columnscope_sink_types(self):
        self.assertEqual(value_kind("BIGINT"), "int64")
        self.assertEqual(value_kind("HUGEINT"), "int64")
        self.assertEqual(value_kind("DOUBLE"), "float64")
        self.assertEqual(value_kind("BOOLEAN"), "boolean")
        self.assertEqual(value_kind("VARCHAR"), "utf8")

    def test_unsupported_oracle_type_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unsupported DuckDB result type"):
            value_kind("DECIMAL(18,2)")


class TrialScheduleTests(unittest.TestCase):
    def test_seeded_rounds_cover_each_configuration_once(self):
        schedule = list(trial_schedule(3, 10, 42))
        self.assertEqual(schedule, list(trial_schedule(3, 10, 42)))
        self.assertNotEqual(schedule, list(trial_schedule(3, 10, 43)))
        self.assertEqual(len(schedule), 624)
        configurations = {configuration for _, _, configuration in schedule[:48]}
        self.assertEqual(len(configurations), 48)
        for round_index in range(13):
            records = schedule[round_index * 48 : (round_index + 1) * 48]
            phase = (round_index < 3, round_index if round_index < 3 else round_index - 3)
            self.assertEqual({(warmup, trial) for warmup, trial, _ in records}, {phase})
            self.assertEqual({configuration for _, _, configuration in records}, configurations)

    def test_oracle_mismatch_stops_during_warmup(self):
        import json

        args = Namespace(
            trial_binary=Path("unused"),
            cargo_lock=Path("Cargo.lock"),
            dataset_manifest=Path("manifest.json"),
            input=Path("input.parquet"),
            source_sha="source",
            order_seed=42,
            warmups=3,
            repetitions=10,
            layout="random",
            batch_size=8192,
            threads=1,
        )
        _, _, (workload, mode, engine, planning) = next(trial_schedule(3, 10, 42))
        mismatch = {
            "workload": workload,
            "mode": mode,
            "engine": engine,
            "planning": planning,
            "layout": "random",
            "batch_size": 8192,
            "threads": 1,
            "trial": 0,
            "warmup": True,
            "source_sha": "source",
            "dataset_checksum": "dataset",
            "runtime_ns": 1,
            "setup_peak_rss_bytes": 1,
            "peak_rss_bytes": 2,
            "result_rows": 1,
            "result_checksum": "wrong",
        }
        module = "scripts.run_benchmark_matrix"
        with (
            patch(f"{module}.sha256_file", return_value="binary"),
            patch(f"{module}.run", side_effect=["{}", json.dumps(mismatch)]) as run,
            self.assertRaisesRegex(ValueError, "DuckDB mismatch for trial 0"),
        ):
            execute_matrix(args, "dataset", {workload: (1, "expected")})
        self.assertEqual(run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
