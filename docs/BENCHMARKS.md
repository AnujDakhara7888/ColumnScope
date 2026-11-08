# Benchmarks

No query performance results are published yet. Vectorized scan, filter,
projection, and bounded composite aggregation exist, but the scalar aggregate
comparison, fixed million-row workload, and timing/RSS harness are not
implemented. The four-row storage and oracle checks are correctness evidence,
not benchmarks.

## Measurement design

The benchmark suite will use a deterministic one-million-row dataset and twelve
fixed query families covering wide and projected scans, a selectivity curve,
global aggregation, low- and high-cardinality grouping, and a combined
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
