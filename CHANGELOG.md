# Changelog

All notable changes to ColumnScope will be documented here.

## Unreleased

### Added

- Pinned Rust 1.96.0 workspace with Arrow and Parquet 60.1.0.
- Deterministic nullable Arrow fixture and Snappy Parquet round trip.
- Batched decode consumer and tests for values, nulls, boundaries, and corrupt
  input.
- Linux CI for formatting, strict Clippy linting, and workspace tests.
- Strict version-1 JSON plans for scans, filters, projections, and aggregation,
  with typed literals, expression binding, alias checks, and schema inference.
- Version-1 pinned snapshot descriptors with canonical SHA-256 schema
  fingerprints and safe relative file metadata.
- Pull-based Parquet scan operator with stable schema, lifecycle state,
  cumulative counters, and sticky end-of-stream behavior.
- Vectorized typed comparison, Boolean/null expression evaluation, streaming
  filter and projection operators, and a checked DuckDB oracle fixture.
- Global, single-key, and two-key COUNT/SUM/MIN/MAX/AVG hash aggregation with
  checked overflow, empty/null semantics, an explicit state budget, bounded
  result batches, and grouped DuckDB oracle parity.
- Unoptimized logical-to-physical planning with per-operator schema checks,
  physical resource propagation, and stable explain output.
- Opt-in Filter-through-Project and Parquet projection pushdown, conservative
  numeric row-group pruning, retained residual filters, and optimizer parity
  coverage.
- Independent scalar filter/project and bounded aggregation execution over
  shared Arrow buffers, with vector parity across nullable composite groups,
  empty inputs, batch boundaries, overflow, and terminal failures.
- Twelve frozen benchmark plan/SQL pairs and a deterministic one-million-row
  generator for checksum-verified random and selectivity-key-clustered Parquet
  layouts.
- Release-only fresh-process benchmark trials with execution-only and
  end-to-end timing boundaries, order-independent result checksums, process
  peak-RSS capture, raw CSV samples, and source/environment provenance.
- A complete Q01–Q12 matrix driver that verifies manifest/source provenance,
  runs scalar and batch configurations, independently executes DuckDB SQL, and
  publishes raw artifacts only after every result multiset matches.
- The first complete 48-record million-row matrix, with exact DuckDB parity for
  every workload in execution-only and end-to-end modes, retained as a
  single-sample correctness baseline with full provenance.
- A deterministic 120-case differential suite covering twelve query families,
  three execution/planning configurations, nullable data, and row counts around
  the 8,192-row batch boundary, with all 360 comparisons matched to DuckDB.
