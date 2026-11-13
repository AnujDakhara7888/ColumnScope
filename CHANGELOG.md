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
