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
- `ScalarFilterProject` and `ScalarAggregate` provide an independent
  row-at-a-time comparison path over the same decoded Arrow batches.
- `plan_query` lowers a bound tree in unoptimized mode, checks every physical
  schema, and retains a stable explain tree beside the live operator root.
- `PlanningMode::Optimized` safely moves filters through projections, remaps
  column indexes, projects Parquet columns, and prunes impossible numeric row
  groups while retaining residual filters.

Run the verified storage example from the workspace root:

```sh
cargo run --bin parquet-fixture -- /tmp/columnscope-fixture.parquet
```

The JSON contracts are documented in [Plan format](../../docs/PLAN_FORMAT.md)
and [Snapshot descriptor](../../docs/SNAPSHOT_DESCRIPTOR.md). Scan, filter,
projection, and bounded composite aggregation physical operators are
implemented and connected by unoptimized and optimized physical planning. See
the [workspace README](../../README.md) for current limitations.
