# Correctness validation

ColumnScope checks execution results against DuckDB before using the same
workloads for performance analysis. The oracle runs independently over the
generated Parquet input; DuckDB is not linked into the Rust engine or used by
the production execution path.

## Seeded differential suite

The seeded suite generates nullable Int64, Float64, Boolean, and Utf8 data,
writes a fresh Parquet file for each case, and constructs a versioned
ColumnScope plan plus equivalent DuckDB SQL. Each case is executed through:

- batch execution with unoptimized planning;
- batch execution with optimized planning; and
- scalar execution with unoptimized planning.

All three outputs must match the DuckDB row count and typed result-multiset
digest exactly. The digest is insensitive to row order but sensitive to
duplicates, nulls, value types, and floating-point bit patterns. A mismatch or
failed child process stops the run, and the output file is written only after
every case succeeds.

The twelve plan families cover integer and floating-point predicates, Kleene
Boolean combinations, `NOT`, null tests, Utf8 comparison, projection, all six
aggregate forms, nullable single-key and two-key grouping, and filtered global
or grouped aggregation. The first five cases use 0, 1, 8,191, 8,192, and 8,193
rows to cross the default batch boundary. Remaining cases use deterministic
small inputs, with each case seed retained for reproduction.

Create the pinned oracle environment and run the suite against a clean source
revision:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

.venv/bin/python scripts/run_differential_suite.py \
  --output /tmp/columnscope-differential.json \
  --source-sha "$(git rev-parse HEAD)"
```

The default seed is `20261207` and the default case count is 120. The driver
rejects fewer than 100 cases, a dirty worktree, a source revision other than
`HEAD`, and an existing output path.

## Observed result

The retained [differential result](../benchmarks/results/differential/README.md)
records 120 passing cases and 360 passing ColumnScope-to-DuckDB comparisons on
source revision `9f012a7115e8a365dbabc7397ea508bae22e1294`. Each plan family
appears ten times. The run used DuckDB 2.0.0, seed `20261207`, and the five
documented batch-boundary row counts. No mismatch was observed.

The Rust integration suite separately exercises corrupt Parquet input, schema
and plan rejection, no-match and all-match filters, null-only aggregate
groups, empty input, integer overflow across batches, non-finite floating
input, group-state exhaustion, invalid output sizes, sticky failures, and
worker-thread execution. See [Testing](TESTING.md) for the complete observed
coverage.

## Evidence boundary

The differential inputs are intentionally small and provide correctness
coverage, not runtime or memory evidence. They use one Parquet file per case;
multi-file snapshot execution is covered by the separate storage-integration
work. The suite does not cover SQL parsing, joins, nested types, disk spill, or
distributed execution because those features are outside the current engine
scope.

The differential suite uses quarter-step floating-point input and integer
aggregate arguments, so exact typed digests are appropriate for these cases.
Future floating-point aggregate inputs that can differ by valid evaluation
order require the documented absolute `1e-9` plus relative `1e-8` tolerance
instead of bitwise equality.
