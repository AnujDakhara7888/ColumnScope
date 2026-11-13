# Frozen benchmark inputs

ColumnScope uses twelve fixed semantic query families. Each workload has a
versioned engine plan in [`workloads/`](workloads/) and equivalent DuckDB SQL in
[`oracle/`](oracle/). Optimizer flags, physical layout, and batch size are
experimental configurations of these queries, not additional workloads.

| ID | Query family | Primary behavior |
|---|---|---|
| Q01 | Scan and consume all eight columns | Wide scan |
| Q02 | Project `id`, `amount_cents` | Narrow access |
| Q03 | Project `id` where `selectivity_key < 10` | Exact 0.1% filter |
| Q04 | Project `id` where `selectivity_key < 100` | Exact 1% filter |
| Q05 | Project `id` where `selectivity_key < 1000` | Exact 10% filter |
| Q06 | Project `id` where `selectivity_key < 5000` | Exact 50% filter |
| Q07 | Project `id` where `selectivity_key < 9000` | Exact 90% filter |
| Q08 | Global count/count/sum | Integer aggregation and nulls |
| Q09 | Count/sum by `region` | Low-cardinality grouping |
| Q10 | Average/min/max `metric` by `category` | Floating aggregation |
| Q11 | Count/sum by `customer_id` | 100,000-key grouping |
| Q12 | 10% key filter and active rows, grouped by region/category | Filtered two-key aggregation |

## Dataset contract

The generator creates exactly 1,000,000 logical rows from seed 42:

- unique `id` values from 0 through 999,999;
- exactly 100,000 `customer_id` values;
- eight deliberately skewed `region` values with target weights
  35/20/15/10/8/6/4/2 percent;
- 32 `category` values;
- nullable, bounded `amount_cents` and nullable, finite `metric` values;
- nullable `active` values;
- `selectivity_key = id % 10,000`, making Q03–Q07 selectivity exact.

Nullable fields use independent deterministic 1-in-20 null decisions. Amounts
are bounded to ±100,000 cents, keeping the one-million-row sum inside Int64.

Two one-file Snappy Parquet layouts contain identical logical rows. `random`
uses a seeded Fisher–Yates permutation. `clustered` orders rows by
`selectivity_key`, then by occurrence. Both use 65,536-row groups and 8,192-row
generation batches. This isolates physical clustering when row-group pruning is
measured.

Generate the files outside the repository:

```sh
cargo run --locked --release --bin generate-benchmark-data -- \
  artifacts/columnscope/datasets
```

Generated Parquet data is intentionally ignored by Git. The committed
[`dataset_manifest.json`](dataset_manifest.json) records parameters, physical
file sizes and SHA-256 values, and a layout-independent logical-row checksum.
The test suite regenerates the files, validates all one million unique IDs and
all 100,000 customer keys, compares both layouts' logical checksums, checks
Parquet compression and row groups, and verifies the exact selectivity counts.

## Evidence boundary

These files define reproducible inputs and correctness contracts. They are not
performance results. Runtime and peak-memory claims require the separate
fresh-process measurement harness and accepted result parity checks described
in [`docs/BENCHMARKS.md`](../docs/BENCHMARKS.md).
