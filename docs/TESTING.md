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
| Scalar filter/project matches vector execution on shared Arrow buffers | pass |
| Scalar global and filtered two-key aggregates match vector results | pass |
| Scalar empty, nullable, overflow, non-finite, and state-limit behavior matches | pass |
| Scalar aggregation is batch-boundary independent on a worker thread | pass |
| Unoptimized planning preserves Project → Filter → Scan order | pass |
| Planned filter/project execution completes on a worker thread | pass |
| Aggregate scan, state, and output limits appear in explain output | pass |
| Scan, intermediate, and query-output schema mismatches fail planning | pass |
| Invalid scan batch size fails before input is opened | pass |
| Optimized and unoptimized results match across three row groups | pass |
| Projection pushdown decodes 2 of 3 columns and remaps expressions | pass |
| Numeric statistics prune only provably impossible row groups | pass |
| Residual filtering retains partial-group row correctness | pass |
| Filter safely moves below Project; unsupported NOT keeps all groups | pass |
| COUNT(*) preserves 12 rows with a zero-column scan projection | pass |
| Filter/project plan parses, binds aliases, and infers nullability | pass |
| All six aggregate forms infer documented output types | pass |
| Unknown fields, versions, columns, aliases, and implicit coercion fail | pass |
| Pinned snapshot canonical fingerprint and JSON round trip validate | pass |
| Unsafe/duplicate object keys and schema mismatches fail | pass |
| Two-file snapshot scan preserves descriptor order and sticky end-of-stream | pass |
| Missing later files and size/row-count/schema mismatches fail before output | pass |
| Explicit SHA-256 verification accepts matching files and rejects mismatch | pass |
| Snapshot optimized projection/pruning matches unoptimized output | pass |
| Snapshot checker reports the pinned version, files, rows, and checksum mode | pass |
| Q01–Q12 plans bind and pair with equivalent oracle SQL | pass |
| Generated layouts match committed physical SHA-256 values | pass |
| Both layouts contain the same one million unique logical rows | pass |
| Cluster order, 100,000 customer keys, and exact selectivity counts validate | pass |
| Execution-only scalar and batch Q03 trials share result counts/checksums | pass |
| End-to-end optimized batch and scalar Q08/Q09 trials share result checksums | pass |
| Unsupported benchmark planning/thread combinations fail before input open | pass |
| Benchmark environment metadata binds source, lockfile, and dataset hashes | pass |
| All 48 initial Q01–Q12 mode/engine records match DuckDB result multisets | pass |
| All 120 seeded cases match DuckDB in three execution/planning configurations | pass |
| Row counts 0/1/8191/8192/8193 cross the default batch boundary | pass |

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

The benchmark and differential digest helpers have focused Python coverage:

```sh
python3 -m unittest \
  scripts/test_run_benchmark_matrix.py \
  scripts/test_run_differential_suite.py
```

The nine focused tests cover output-order independence, duplicate sensitivity,
DuckDB-to-ColumnScope type mapping, deterministic plan families and data,
physical Parquet nullability, required boundary sizes, and aggregate coverage.
The complete matrix additionally verifies all actual million-row workload
outputs against independently executed DuckDB SQL before retaining raw
samples.

The oracle environment is pinned separately:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

DuckDB 2.0.0 has been installed and imported successfully with Python 3.14.5.
Checked filter/project and grouped-aggregate results are stored as small JSON
oracle fixtures.

The latest local workspace run completed with 83 passed, 0 failed, and 0
ignored integration tests. Both oracle fixtures were independently regenerated
with DuckDB 2.0.0. The grouped fixture matched three region groups, including a
null-only aggregate group. Composite nullable keys, terminal state-budget
failure, and three partial output batches were also exercised. The production
path does not invoke DuckDB. The planner tests also executed the preserved
unoptimized tree, checked explain/resource details, and rejected mismatched
schemas before execution. The scalar comparison consumed the same decoded
Arrow batches as the vector path and matched unordered result rows for empty,
global, filtered, nullable composite-key, and all supported aggregate cases.
The frozen workload test regenerated both one-million-row layouts, matched the
committed physical and logical checksums, checked 16 Snappy row groups per
file, and observed exact Q03–Q07 counts of 1,000, 10,000, 100,000, 500,000,
and 900,000 rows. All twelve equivalent SQL files executed against DuckDB; the
observed result-row counts for Q01–Q12 were 1,000,000, 1,000,000, 1,000,
10,000, 100,000, 500,000, 900,000, 1, 8, 32, 100,000, and 256. Randomized
differential coverage retained 120 cases and 360 comparisons: optimized batch,
unoptimized batch, and scalar output all matched DuckDB. The complete repeated
timed benchmark matrix awaits later measurement work.

The harness tests use a small deterministic Parquet fixture. They validate the
timing-mode paths, scalar/batch result parity, counters, process RSS fields,
configuration rejection, and provenance capture. Release runner smoke checks
are not reported as performance measurements.

Optimizer parity uses a separate twelve-row fixture written as three four-row
groups. A `metric >= 12` predicate pruned the `[0, 3]` group, retained the
partially matching `[10, 13]` group for residual evaluation, and retained the
`[20, 23]` group. Optimized and unoptimized execution both returned IDs 6–11.
An unsupported `NOT` predicate retained all three groups.

## Correctness strategy for engine operators

Optimized, unoptimized, and scalar results are compared over identical inputs,
with DuckDB supplying independently executed results. Integer, string, Boolean,
null, and the currently generated floating values match exactly. Result
multisets are compared outside measured intervals because the engine does not
promise output order. Floating aggregates with valid evaluation-order variance
use absolute `1e-9` plus relative `1e-8` tolerance.

Current boundary coverage includes empty input, null-only aggregate groups,
predicate nulls, no/all matches, integer overflow, aggregation memory limits,
and row counts around the default 8,192-row batch boundary. Differential case
seeds are retained for reproduction. The separate snapshot-storage integration
checks ordered multi-file input, fail-before-output metadata/schema validation,
explicit checksums, optimizer parity, version pinning, and replay stability.

The fixed filter/project and grouped-aggregate plans and the retained seeded
differential cases have oracle parity. See
[Correctness validation](CORRECTNESS.md) for method, result, and limitations.
