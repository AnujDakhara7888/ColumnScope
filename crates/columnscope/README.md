# `columnscope`

This crate contains the ColumnScope execution engine and command-line tools.
The current API establishes the Arrow/Parquet storage boundary:

- `fixture_batch` builds a deterministic nullable batch;
- `write_fixture` writes it as Snappy-compressed Parquet;
- `read_parquet_batches` decodes with an explicit positive batch size;
- `summarize_batches` consumes decoded output and reports observable counts.
- `QueryPlan::from_json` parses a strict versioned logical plan.
- `bind_query` resolves columns, validates expressions, and infers output schema.
- `SnapshotDescriptor::from_json` validates immutable file metadata and schema
  fingerprints.

Run the verified storage example from the workspace root:

```sh
cargo run --bin parquet-fixture -- /tmp/columnscope-fixture.parquet
```

The JSON contracts are documented in [Plan format](../../docs/PLAN_FORMAT.md)
and [Snapshot descriptor](../../docs/SNAPSHOT_DESCRIPTOR.md). Physical execution
operators remain unimplemented. See the [workspace README](../../README.md) for
current limitations.
