import random
import unittest

from scripts.run_differential_suite import (
    BOUNDARY_ROWS,
    aggregate_set,
    batch_size_for,
    case_definition,
    generate_rows,
    row_count_for,
    schema_for_rows,
)


class DifferentialSuiteTests(unittest.TestCase):
    def test_all_twelve_templates_are_deterministic(self):
        schema = schema_for_rows([])
        first = [
            case_definition(index, random.Random(index), schema)
            for index in range(12)
        ]
        second = [
            case_definition(index, random.Random(index), schema)
            for index in range(12)
        ]

        self.assertEqual(first, second)
        self.assertEqual(len({value[0] for value in first}), 12)

    def test_required_row_boundaries_are_first(self):
        actual = tuple(
            row_count_for(index, random.Random(index))
            for index in range(len(BOUNDARY_ROWS))
        )
        self.assertEqual(actual, BOUNDARY_ROWS)

    def test_required_row_boundaries_use_default_batch_size(self):
        actual = tuple(
            batch_size_for(index, random.Random(index))
            for index in range(len(BOUNDARY_ROWS))
        )
        self.assertEqual(actual, (8192,) * len(BOUNDARY_ROWS))

    def test_generated_rows_are_reproducible_and_finite(self):
        first = generate_rows(100, random.Random(42))
        second = generate_rows(100, random.Random(42))

        self.assertEqual(first, second)
        self.assertTrue(
            all(row[2] is None or (row[2] * 4).is_integer() for row in first)
        )

    def test_schema_nullability_matches_observed_values(self):
        rows = [(0, 1, None, True, "a"), (1, None, 1.5, False, "b")]
        schema = schema_for_rows(rows)

        self.assertEqual(
            [field["nullable"] for field in schema["fields"]],
            [False, True, True, False, False],
        )

    def test_global_aggregate_set_covers_all_supported_functions(self):
        self.assertEqual(
            [value[0] for value in aggregate_set()],
            ["count_star", "count", "sum", "min", "max", "avg"],
        )


if __name__ == "__main__":
    unittest.main()
