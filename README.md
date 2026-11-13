# ColumnScope

[![CI](https://github.com/AnujDakhara7888/ColumnScope/actions/workflows/ci.yml/badge.svg)](https://github.com/AnujDakhara7888/ColumnScope/actions/workflows/ci.yml)

ColumnScope is a small columnar query engine built in Rust to make query
execution strategy visible and measurable. It executes typed logical plans
over Parquet, lowers them into explicit physical operators, processes Arrow
arrays in batches, and compares that path with an independently inspectable
scalar baseline.

The project focuses on a deliberately narrow question: how do batching,
projection pruning, filter placement, and conservative row-group pruning affect
correctness, runtime, and memory for the same query semantics?

## Current capabilities

The storage foundation and comparison execution paths are implemented and
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
- global, single-key, and two-key hash aggregation for COUNT, SUM, MIN, MAX,
  and AVG, with an explicit retained-state budget and bounded output batches;
- an independent row-at-a-time filter/project and aggregation path over the
  same decoded Arrow buffers, with matching null, overflow, and failure rules;
- twelve frozen plan/SQL workload pairs and a deterministic generator for
  identical random and clustered one-million-row Parquet layouts;
- unoptimized logical-to-physical planning with recursive schema checks and a
  stable physical explain tree;
- an opt-in optimizer for safe Filter-through-Project movement, Parquet column
  projection, and conservative numeric row-group pruning with residual filters;
- checked DuckDB oracle fixtures for filter/project and grouped aggregation;
- a compatibility consumer that counts every decoded batch, column, and row;
- Linux CI for formatting, strict Clippy linting, and tests.

Live SnapshotStore integration and performance results are not yet implemented.
Unoptimized mode preserves the bound logical tree;
optimized mode applies only the documented pushdowns and keeps every filter as
a residual correctness check. Hash aggregation is blocking and fails rather
than spilling when its configured state budget is exceeded. The repository
does not claim benchmark behavior before the remaining measurement components
have executable evidence.

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

To lower a versioned plan into physical operators, inspect it, and pull result
batches:

```rust
use columnscope::bind::bind_query;
use columnscope::logical::QueryPlan;
use columnscope::planner::{PlannerOptions, PlanningMode, plan_query};

let parsed = QueryPlan::from_json(&std::fs::read_to_string("plan.json")?)?;
let bound = bind_query(parsed)?;
let options = PlannerOptions {
    mode: PlanningMode::Optimized,
    ..PlannerOptions::default()
};
let mut query = plan_query(bound, "input.parquet", options)?;

print!("{}", query.explain());
while let Some(batch) = query.root.next_batch()? {
    // Consume each Arrow RecordBatch.
}
```

The default planner remains explicitly unoptimized for comparison. Optimized
explain output additionally reports filter movement, decoded columns, selected
row groups, and residual filters.

To reproduce the benchmark inputs without committing generated Parquet files:

```sh
cargo run --locked --release --bin generate-benchmark-data -- \
  artifacts/columnscope/datasets
```

See the [frozen benchmark inputs](benchmarks/README.md) for the dataset contract,
checksums, and Q01–Q12 definitions. No runtime result is claimed until the
measurement harness and result validation are complete.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

The current suite covers Parquet values/nulls, decode errors, scan lifecycle and
counters, typed expression semantics, filter/project execution, scalar/vector
aggregation parity and failure boundaries, physical planning/schema checks,
logical plan binding, pinned snapshot validation, frozen workload contracts,
and million-row layout identity. See
[Testing](docs/TESTING.md) for the latest observed counts. DuckDB supplies
checked expected results only and is not used by the production Rust path.

## Design

ColumnScope owns plan validation, execution orchestration, expression
semantics, grouping, optimization rules, metrics, and the scalar comparison
path. Apache Arrow supplies column buffers and primitive array operations;
Apache Parquet supplies file encoding and decoding. DuckDB will be used only as
an independent correctness oracle, never as the production query path.

See [Plan format](docs/PLAN_FORMAT.md), [Snapshot descriptor](docs/SNAPSHOT_DESCRIPTOR.md),
[Architecture](docs/ARCHITECTURE.md), [Testing](docs/TESTING.md),
[frozen benchmark inputs](benchmarks/README.md), and [Benchmarks](docs/BENCHMARKS.md)
for the public contracts and evidence boundaries.

## Scope and limitations

The intended first release supports scans, filters, projections, and grouped or
global aggregation over Int64, Float64, Boolean, and Utf8 columns. SQL parsing,
joins, nested types, distributed execution, disk spill, and a web service are
outside its scope. Output ordering is not guaranteed unless a caller sorts
outside query execution.
