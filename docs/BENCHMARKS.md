# Benchmarks

No query performance results are published yet. Vectorized scan, filter,
projection, bounded composite aggregation, conservative pushdown/pruning, and
an independent scalar comparison path exist. The deterministic million-row
dataset and twelve query families are frozen, but the fresh-process timing/RSS
harness is not implemented. Storage, optimizer, scalar-parity, workload, and
oracle checks are correctness evidence, not benchmarks.

## Frozen inputs

The [benchmark input contract](../benchmarks/README.md) defines Q01–Q12, their
versioned plans and equivalent DuckDB SQL, and the deterministic dataset. The
generator emits random-order and selectivity-key-clustered layouts with the
same one million logical rows, one Snappy Parquet file per layout, and
65,536-row groups. The committed manifest records generation parameters and
verified physical and logical SHA-256 values; generated Parquet files remain
outside Git.

## Measurement design

The benchmark suite uses the frozen deterministic one-million-row dataset and
twelve fixed query families covering wide and projected scans, a selectivity
curve, global aggregation, low- and high-cardinality grouping, and a combined
filter/two-key aggregation pipeline.

Two comparisons will be reported separately:

1. Execution-only: scalar and batched executors receive the same decoded Arrow
   buffers.
2. End-to-end: both paths include Parquet decode and output consumption with
   matched input settings.

Optimizer ablations will change one factor at a time. Random-order and clustered
physical layouts will be reported separately so output selectivity is not
mistaken for row-group pruning.

Each accepted timing result must first pass result-multiset validation. Raw
samples will retain runtime, process peak RSS, output counts/checksums, source
revision, dataset checksum, batch size, layout, flags, trial number, hardware,
OS, compiler, and cache policy. Reports will include median and p95 without a
minimum required speedup; regressions and negative results will remain visible.
