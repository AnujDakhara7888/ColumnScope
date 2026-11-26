# Matched Q01–Q12 measurements

The matrix covers twelve query families on the frozen one-million-row random
layout, using 8,192-row batches and one execution thread. Each query ran as
execution-only batch and scalar, and as end-to-end optimized batch and
unoptimized scalar. DuckDB 2.0.0 supplied independently evaluated expected
rows and typed multiset digests. All 144 warmups and 480 measured trials matched.

Each serial round shuffled the 48 configurations with order seed 42. Every
trial started a fresh process. Three warmup rounds preceded ten measured
rounds. The operating system page cache was retained. The measured source is
`2b6a49843c63c8a2ef2f9efc94f2d59e7544e9fe`. The host was an Apple M4 with
16 GiB RAM on macOS 26.6.2. CPU model and memory were verified separately
with `sysctl` because the trial's host probe could not read those fields.

- [Raw trials](raw_trials.csv) record runtime, whole-process peak RSS, setup
  peak RSS, result digests, counters, and launch position. Positions 0–143
  are the warmups and are excluded from the CSV.
- [Environment](environment.json) records toolchain, binary and source
  provenance, and the measurement protocol.
- [Dataset manifest](dataset_manifest.json) identifies the frozen Parquet input.
- [Result validation](query_validation.json) records the DuckDB comparisons.

Execution-only measurements start after Parquet decode. In end-to-end mode
the comparison includes both execution strategy and scan optimization.
Q01 uses the same scan consumer under both engine labels. Peak RSS includes
the entire process, including input buffers and runtime libraries.

See the [reproduction command](../../../docs/BENCHMARKS.md#complete-matrix-validation).
