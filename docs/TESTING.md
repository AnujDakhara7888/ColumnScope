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

The future oracle environment is pinned separately:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

DuckDB 2.0.0 has been installed and imported successfully with Python 3.14.5.
No oracle query is claimed until the query-plan harness exists.

The latest local workspace run completed with 20 passed, 0 failed, and 0
ignored integration tests. Physical operator, oracle, concurrency, and recovery
tests are not present yet because those components have not been implemented.

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

No operator-oracle or randomized-query result is claimed yet.
