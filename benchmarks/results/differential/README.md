# Seeded differential result

This directory retains the first complete small-data differential correctness
run. It is a validation artifact, not a performance result.

## Provenance

- ColumnScope source: `9f012a7115e8a365dbabc7397ea508bae22e1294`
- suite seed: `20261207`
- DuckDB: 2.0.0
- cases: 120
- ColumnScope comparisons: 360
- configurations: batch/unoptimized, batch/optimized, scalar/unoptimized
- required row counts: 0, 1, 8,191, 8,192, and 8,193, each decoded with an
  8,192-row batch size

Each of the twelve query templates appears ten times. Every configuration
matched DuckDB's output row count and order-independent typed multiset digest
for every case. The per-case seeds, input sizes, batch sizes, oracle digests,
and engine digests are retained in
[`query_validation.json`](query_validation.json).

The inputs are deterministic, nullable, and deliberately small. This result
does not establish a speedup, memory reduction, or behavior for multi-file
snapshots.
