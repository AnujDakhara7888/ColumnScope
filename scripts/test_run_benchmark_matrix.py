import unittest

from scripts.run_benchmark_matrix import result_checksum, value_kind


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


if __name__ == "__main__":
    unittest.main()
