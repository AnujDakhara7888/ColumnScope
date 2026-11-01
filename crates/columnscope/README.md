# `columnscope`

This crate contains the ColumnScope execution engine and command-line tools.
The current API establishes the Arrow/Parquet storage boundary:

- `fixture_batch` builds a deterministic nullable batch;
- `write_fixture` writes it as Snappy-compressed Parquet;
- `read_parquet_batches` decodes with an explicit positive batch size;
- `summarize_batches` consumes decoded output and reports observable counts.

Run the verified storage example from the workspace root:

```sh
cargo run --bin parquet-fixture -- /tmp/columnscope-fixture.parquet
```

The query plan and execution operator APIs will be documented here when they
exist. See the [workspace README](../../README.md) for current limitations.
