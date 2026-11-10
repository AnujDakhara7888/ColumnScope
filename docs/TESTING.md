# Testing

ColumnScope uses layered tests so storage, semantics, optimization, and
performance evidence can fail independently.

## Current automated coverage

| Test | Observed result |
|---|---|
| Parquet round trip preserves schema, values, and nulls | pass |
| Batch size 2 yields two batches and the sink consumes all 4 rows | pass |
| Batch size 0 is rejected before opening input | pass |
| Corrupt Parquet input returns an error | pass |
| Pull scan reports 2 batches/4 rows and repeatable end-of-stream | pass |
| Empty Parquet input preserves its schema and emits no batches | pass |
| An opened scan remains consumable through its owned file handle | pass |
| Parquet scan moves to a worker thread and streams to completion | pass |
| Filter/project output matches the checked DuckDB fixture | pass |
| Typed comparisons and unary predicates preserve null semantics | pass |
| Kleene Boolean evaluation matches SQL three-valued logic | pass |
| Projection failure is terminal and cannot restart its child | pass |
| No-match/all-match filters and a worker-thread pipeline complete | pass |
| Grouped COUNT/SUM/MIN/MAX/AVG matches the DuckDB oracle | pass |
| Global integer and floating aggregates return expected values | pass |
| Empty global/grouped inputs follow SQL identity and null behavior | pass |
| Null keys group together and null aggregate values are skipped | pass |
| Integer SUM overflow across batches fails terminally | pass |
| Non-finite Float64 aggregate input fails before output | pass |
| Two-key groups preserve the full tuple, including nullable key parts | pass |
| Aggregate state budget fails before output and remains terminal | pass |
| Five groups drain as bounded result batches of 2, 2, and 1 rows | pass |
| Zero-sized aggregate output batches are rejected at construction | pass |
| Grouped results are batch-boundary independent on a worker thread | pass |
| Unoptimized planning preserves Project → Filter → Scan order | pass |
| Planned filter/project execution completes on a worker thread | pass |
| Aggregate scan, state, and output limits appear in explain output | pass |
| Scan, intermediate, and query-output schema mismatches fail planning | pass |
| Invalid scan batch size fails before input is opened | pass |
| Filter/project plan parses, binds aliases, and infers nullability | pass |
| All six aggregate forms infer documented output types | pass |
| Unknown fields, versions, columns, aliases, and implicit coercion fail | pass |
| Pinned snapshot canonical fingerprint and JSON round trip validate | pass |
| Unsafe/duplicate object keys and schema mismatches fail | pass |

The tests use temporary directories and do not depend on checked-in generated
data. Run the complete current suite with:

```sh
cargo test --locked --workspace
```

Formatting and lints are release gates as well:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

CI runs all three commands on Linux with the checked-in Rust toolchain.

The oracle environment is pinned separately:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

DuckDB 2.0.0 has been installed and imported successfully with Python 3.14.5.
Checked filter/project and grouped-aggregate results are stored as small JSON
oracle fixtures.

The latest local workspace run completed with 49 passed, 0 failed, and 0
ignored integration tests. Both oracle fixtures were independently regenerated
with DuckDB 2.0.0. The grouped fixture matched three region groups, including a
null-only aggregate group. Composite nullable keys, terminal state-budget
failure, and three partial output batches were also exercised. The production
path does not invoke DuckDB. The planner tests also executed the preserved
unoptimized tree, checked explain/resource details, and rejected mismatched
schemas before execution. Randomized differential cases and benchmark
validation await later components.

## Correctness strategy for engine operators

As operators are implemented, optimized, unoptimized, and scalar results will
be compared with DuckDB over identical Parquet inputs. Integer, string, and
boolean results must match exactly. Floating aggregates will use a documented
absolute-plus-relative tolerance. Result multisets will be compared outside
the measured interval because the engine does not promise output order.

Boundary coverage will include empty input, null-only columns, predicate nulls,
multi-file scans, no/all matches, integer overflow, aggregation memory limits,
and row counts around the default 8,192-row batch boundary. Randomized cases
will retain their seeds for reproduction.

The fixed filter/project and grouped-aggregate plans have oracle parity.
Randomized-query results are not claimed yet.
