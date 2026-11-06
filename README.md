# ColumnScope

[![CI](https://github.com/AnujDakhara7888/ColumnScope/actions/workflows/ci.yml/badge.svg)](https://github.com/AnujDakhara7888/ColumnScope/actions/workflows/ci.yml)

ColumnScope is a small columnar query engine being built in Rust to make query
execution strategy visible and measurable. It will execute typed logical plans
over Parquet, lower them into explicit physical operators, process Arrow arrays
in batches, and compare that path with an independently inspectable scalar
baseline.

The project focuses on a deliberately narrow question: how do batching,
projection pruning, filter placement, and conservative row-group pruning affect
correctness, runtime, and memory for the same query semantics?

## Current capabilities

The storage foundation and first physical operator are implemented and
verified:

- a pinned Rust workspace using Arrow and Parquet 60.1.0;
- a Python 3.14.5 development environment with DuckDB 2.0.0 reserved for the
  independent correctness oracle;
- a deterministic nullable Arrow `RecordBatch` containing Int64, Float64,
  Boolean, and Utf8 columns;
- Snappy-compressed Parquet write and batched decode;
- explicit rejection of zero-sized decode batches and propagation of corrupt
  input errors;
- versioned JSON logical plans with schema, name, alias, and expression type
  validation before execution;
- a validated version-1 pinned snapshot descriptor with canonical schema
  fingerprints and normalized relative object keys;
- a pull-based Parquet scan operator with a stable output schema, cumulative
  pull/batch/row counters, and repeatable end-of-stream behavior;
- vectorized typed comparisons, Kleene Boolean/null evaluation, batch filtering,
  and named expression projection over Arrow arrays;
- an independent row-at-a-time scan/filter/project path over the same decoded
  Arrow buffers, with a common typed result collector for exact comparisons;
- a checked DuckDB oracle fixture for the first filter/project pipeline;
- a compatibility consumer that counts every decoded batch, column, and row;
- Linux CI for formatting, strict Clippy linting, and tests.

Aggregation, logical-to-physical planning, optimization, live SnapshotStore
integration, and performance results are not yet implemented. Physical
filter/project and scalar pipelines currently require manual assembly from
bound plans. The repository does not claim complete query execution or
benchmark behavior before those components have executable evidence.

## Quick start

Install [Rustup](https://rustup.rs/), then clone the repository. The checked-in
toolchain file selects Rust 1.96.0 automatically.

```sh
cargo run --bin parquet-fixture -- /tmp/columnscope-fixture.parquet
```

A successful run writes the fixture, decodes it in two-row batches, consumes
the output, and prints a summary similar to:

```text
wrote and decoded 4 rows in 2 batches (4 columns): /tmp/columnscope-fixture.parquet
```

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

The current suite covers Parquet values/nulls, decode errors, scan lifecycle and
counters, typed expression semantics, vectorized/scalar parity,
filter/project execution, logical plan binding, and pinned snapshot validation.
See [Testing](docs/TESTING.md) for the latest observed counts. DuckDB supplies
checked expected results only and is not used by either execution path.

## Design

ColumnScope owns plan validation, execution orchestration, expression
semantics, grouping, optimization rules, metrics, and the scalar comparison
path. Apache Arrow supplies column buffers and primitive array operations;
Apache Parquet supplies file encoding and decoding. DuckDB will be used only as
an independent correctness oracle, never as the production query path.

See [Plan format](docs/PLAN_FORMAT.md), [Snapshot descriptor](docs/SNAPSHOT_DESCRIPTOR.md),
[Architecture](docs/ARCHITECTURE.md), [Testing](docs/TESTING.md), and
[Benchmarks](docs/BENCHMARKS.md) for the public contracts and evidence boundaries.

## Scope and limitations

The intended first release supports scans, filters, projections, and grouped or
global aggregation over Int64, Float64, Boolean, and Utf8 columns. SQL parsing,
joins, nested types, distributed execution, disk spill, and a web service are
outside its scope. Output ordering is not guaranteed unless a caller sorts
outside query execution.
