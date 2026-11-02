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
will resolve names against the input schema and reject invalid types before
execution where possible. The physical layer will use a pull interface that
returns one Arrow `RecordBatch` at a time and exposes operator metrics.

A separate scalar executor will evaluate the same supported semantics one row
at a time over the same decoded Arrow buffers. It is a comparison path, not an
alternate storage stack.

## Implemented plan and storage contracts

A deterministic
Arrow batch is written with the Apache Parquet writer using Snappy compression
and decoded through `ParquetRecordBatchReaderBuilder` with a caller-selected
batch size. The reader returns `RecordBatch` values without introducing custom
row materialization. A small sink consumes all decoded batches and counts rows.

Arrow owns array buffers and validity masks. Parquet owns encoding, compression,
and decoding. ColumnScope will own the control flow and semantics above that
boundary; these library capabilities are not presented as custom engine work.

Plan format version 1 is a strict serde contract. A plan document owns its input
schema and a recursive Scan/Filter/Project/Aggregate tree. Binding resolves each
column name to an input position, validates expression and aggregate types,
checks aliases, and infers the schema after every node. The bound tree is not
executable yet; keeping it separate prevents parsing or name lookup from being
hidden inside future operator loops.

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

These execution boundaries describe the design target. Parsing and binding are
available; evaluation and physical execution are not.
