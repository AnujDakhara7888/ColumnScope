# Decision 0001: consume immutable snapshot descriptors

Status: accepted

## Context

A query must observe one stable set of Parquet files even if another writer
commits concurrently. Directory listing cannot distinguish committed data from
temporary, abandoned, or newer files and can change between batches.

## Decision

ColumnScope accepts a versioned immutable descriptor containing the table and
commit identity, canonical schema, schema fingerprint, and complete file list.
The caller supplies the storage root and credentials separately. ColumnScope
never lists a data prefix to construct a snapshot and never refreshes the
descriptor while an execution is in progress.

The shared schema is serialized as a deliberately small JSON contract rather
than a Rust path dependency, allowing storage and query components to release
independently. Version 1 supports only ColumnScope's four flat scalar types.

## Consequences

Queries can pin and replay a committed version deterministically. Missing files,
invalid paths, checksum failures, or schema mismatches become explicit errors
instead of silently reducing the result. Whole-file checksum verification has a
real I/O cost and must be reported separately from query execution timing.
