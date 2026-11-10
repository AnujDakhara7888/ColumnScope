# Architecture

ColumnScope separates query meaning from execution strategy so that optimizer
and batching effects can be tested without changing semantics.

## Intended execution path

```text
versioned JSON plan
        |
        v
parse -> bind/type-check -> logical plan -> rule optimizer -> physical plan
                                                        |
                                                        v
ParquetScan -> FilterExec -> ProjectExec -> HashAggregateExec -> result sink
```

Logical nodes describe scans, filters, projections, and aggregation. Binding
resolves names against the input schema and rejects invalid types before
execution. The physical layer uses a pull interface that returns at most one
Arrow `RecordBatch` at a time and exposes operator metrics.

`plan_query` recursively lowers the bound tree into `ParquetScan`, `FilterExec`,
`ProjectExec`, and `HashAggregateExec`. The current `Unoptimized` mode applies
no rewrites: operator nesting exactly preserves the logical tree. Construction
checks the Parquet fields and every operator output against the corresponding
bound schema, so a mismatch fails before a runnable query is returned. A stable
explain tree records operator properties and schemas separately from the live
operator objects.

`Optimized` mode applies two logical/physical boundaries. A Filter above a
Project moves below it only after the predicate's projected columns are
substituted with their bound source expressions. Required-column analysis then
rewrites every surviving column index and supplies a root-column projection to
the Parquet reader. COUNT(*) can therefore preserve row counts without decoding
any data column.

For a residual predicate directly above the scan, exact Int64 and Float64
row-group min/max statistics may exclude a group only when the comparison is
provably impossible. AND and OR combine these conservative decisions; missing,
inexact, non-finite, type-mismatched, and unsupported statistics keep the row
group. The original Filter remains in the physical tree and evaluates every
decoded row, including rows in partially matching groups. Explain output reports
decoded/source columns and selected/total row groups.

A separate scalar executor will evaluate the same supported semantics one row
at a time over the same decoded Arrow buffers. It is a comparison path, not an
alternate storage stack.

## Implemented plan and storage contracts

A deterministic Arrow batch is written with the Apache Parquet writer using
Snappy compression and decoded through `ParquetRecordBatchReaderBuilder` with a
caller-selected batch size. `ParquetScan` wraps that decoder behind the
`BatchOperator` contract. Each pull returns one batch, a decode error, or an
end-of-stream marker without introducing custom row materialization.

Every operator exposes its output schema, lifecycle state, and cumulative pull,
output-batch, and output-row counters. Successful end-of-stream is sticky, so
repeated pulls remain `Ok(None)` and never reopen or restart the input. A decode
failure moves the operator to a terminal failed state because resuming from an
uncertain Parquet position could duplicate or omit rows. The scan owns its open
file handle for its lifetime; a small compatibility sink consumes all output and
counts rows.

`FilterExec` evaluates a bound Boolean expression into an Arrow validity-aware
mask and uses Arrow's batch filter kernel. Mask nulls do not pass the filter,
matching SQL `WHERE`. `ProjectExec` evaluates named bound expressions into
arrays and creates a batch with the inferred output schema. Comparisons operate
only on binder-approved equal types; Boolean `AND` and `OR` use Kleene
three-valued logic. Both operators preserve the pull lifecycle and become
terminal after an evaluation or child error.

`HashAggregateExec` is a blocking operator: it consumes streaming input batches,
evaluates aggregate expressions as Arrow arrays, and updates one global state
or a hash table keyed by one or two typed values. COUNT ignores null expression
values, numeric SUM/MIN/MAX/AVG skip nulls, and an empty global input returns
COUNT 0 with nullable aggregates set to null. Integer SUM is checked; overflow
and non-finite Float64 input are terminal errors.

Each new group receives a deterministic retained-state charge covering its key
container, owned Utf8 bytes, aggregate-state vector, and a fixed hash-entry
allowance. The operator fails before inserting a group that would exceed the
configured budget; the charge is an enforcement metric, not a claim about exact
allocator RSS. Once input is exhausted, the hash table is consumed directly
into caller-sized result batches. There is no result-wide row buffer or hidden
sort, so grouped output order is deliberately unspecified. Disk spill is not
implemented.

Arrow owns array buffers and validity masks. Parquet owns encoding, compression,
and decoding. ColumnScope will own the control flow and semantics above that
boundary; these library capabilities are not presented as custom engine work.

Plan format version 1 is a strict serde contract. A plan document owns its input
schema and a recursive Scan/Filter/Project/Aggregate tree. Binding resolves each
column name to an input position, validates expression and aggregate types,
checks aliases, and infers the schema after every node. Planning consumes that
bound tree and builds the matching physical hierarchy without parsing or name
lookup inside operator loops. Keeping binding and physical construction
separate makes the unoptimized baseline explicit and leaves rewrite decisions
inspectable.

The snapshot boundary accepts an immutable version-1 descriptor. It validates
the canonical schema fingerprint, file-level schema agreement, lowercase
SHA-256 values, unique normalized relative object keys, and nonnegative sizes,
row counts, and versions. It never discovers files by listing a directory.

## Planned semantic boundaries

- Types: nullable Int64, Float64, Boolean, and Utf8.
- Predicates: typed comparisons and three-valued boolean logic.
- Aggregates: COUNT, SUM, MIN, MAX, and AVG, globally or grouped by one or two
  non-floating keys.
- Integer overflow: checked and returned as an error.
- Float input: finite values only, with documented comparison tolerances for
  aggregate validation.
- Memory: scan/filter/project stream batches; hash aggregation has an explicit
  state budget and fails instead of spilling.
- Optimization: projection pruning, safe filter movement, and conservative
  numeric row-group pruning with a residual filter retained for correctness.

These execution boundaries describe the design target. Parsing, binding,
vectorized expression evaluation, scan/filter/project execution, and bounded
global or composite-key aggregation are available through an unoptimized
physical planner or the documented pushdown/pruning mode. Page-index pruning,
predicate evaluation inside the Parquet decoder, and cost-based rules are not.
