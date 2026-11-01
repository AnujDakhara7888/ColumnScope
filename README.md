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

The storage foundation is implemented and verified:

- a pinned Rust workspace using Arrow and Parquet 60.1.0;
- a Python 3.14.5 development environment with DuckDB 2.0.0 reserved for the
  independent correctness oracle;
- a deterministic nullable Arrow `RecordBatch` containing Int64, Float64,
  Boolean, and Utf8 columns;
- Snappy-compressed Parquet write and batched decode;
- explicit rejection of zero-sized decode batches and propagation of corrupt
  input errors;
- a consumer that counts every decoded batch, column, and row;
- Linux CI for formatting, strict Clippy linting, and tests.

The logical plan, expression evaluator, physical query operators, optimizer,
scalar baseline, SnapshotStore adapter, and performance results are not yet
implemented. The repository does not claim query-engine or benchmark behavior
before those components have executable evidence.

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

The initial storage slice has four integration tests covering value/schema/null
preservation, partial batching and complete consumption, invalid batch size,
and corrupt Parquet input. The last local verification completed with 4 passed,
0 failed, and 0 ignored tests. The pinned DuckDB package was also installed and
imported successfully under Python 3.14.5; it is not used by the production
Rust path.

## Design

ColumnScope owns plan validation, execution orchestration, expression
semantics, grouping, optimization rules, metrics, and the scalar comparison
path. Apache Arrow supplies column buffers and primitive array operations;
Apache Parquet supplies file encoding and decoding. DuckDB will be used only as
an independent correctness oracle, never as the production query path.

See [Architecture](docs/ARCHITECTURE.md), [Testing](docs/TESTING.md), and
[Benchmarks](docs/BENCHMARKS.md) for the public design and evidence boundaries.

## Scope and limitations

The intended first release supports scans, filters, projections, and grouped or
global aggregation over Int64, Float64, Boolean, and Utf8 columns. SQL parsing,
joins, nested types, distributed execution, disk spill, and a web service are
outside its scope. Output ordering is not guaranteed unless a caller sorts
outside query execution.
