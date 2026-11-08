# `columnscope`

This crate contains the ColumnScope execution engine and command-line tools.
The current API establishes the Arrow/Parquet storage boundary:

- `fixture_batch` builds a deterministic nullable batch;
- `write_fixture` writes it as Snappy-compressed Parquet;
- `read_parquet_batches` decodes with an explicit positive batch size;
- `summarize_batches` consumes decoded output and reports observable counts;
- `QueryPlan::from_json` parses a strict versioned logical plan;
- `bind_query` resolves columns, validates expressions, and infers output schema;
- `SnapshotDescriptor::from_json` validates immutable file metadata and schema
  fingerprints;
- `ParquetScan` implements the `BatchOperator` pull interface and reports
  cumulative pull, output-batch, and output-row counters;
- `evaluate` applies bound typed expressions to Arrow arrays;
- `FilterExec` and `ProjectExec` stream filtered and projected batches;
- `HashAggregateExec` computes global, single-key, or two-key COUNT, SUM, MIN,
  MAX, and AVG results under explicit state and output-batch limits.

Run the verified storage example from the workspace root:

```sh
cargo run --bin parquet-fixture -- /tmp/columnscope-fixture.parquet
```

The JSON contracts are documented in [Plan format](../../docs/PLAN_FORMAT.md)
and [Snapshot descriptor](../../docs/SNAPSHOT_DESCRIPTOR.md). Scan, filter,
projection, and bounded composite aggregation physical operators are
implemented; automatic physical planning remains unimplemented. See the
[workspace README](../../README.md) for current limitations.
