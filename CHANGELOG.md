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
