# Initial complete benchmark matrix

This directory retains the first complete Q01–Q12 correctness and measurement
pass. It is a baseline artifact, not a performance report.

## Configuration

- Source revision: `9d3b5a4bbedc0c991fc9f486b202a99cfb8aaadc`
- Dataset: one-million-row random layout, seed 42, Snappy Parquet
- Dataset SHA-256:
  `c4ad8701e4723cbf3199767a4eb2602597d50974d941a0681f231e9850a70d32`
- Batch size: 8,192 rows
- Threads: 1
- Measurements: 0 warmups and 1 measured repetition per configuration
- Configurations per workload: execution-only batch/scalar and end-to-end
  optimized batch/unoptimized scalar
- Oracle: DuckDB 2.0.0 with one thread
- Host: Apple silicon macOS; complete details are in `environment.json`

The run produced 48 raw records under 48 distinct process IDs. All twelve
workloads matched DuckDB exactly for output row count and the canonical typed
multiset digest in both modes. Observed Q01–Q12 result counts were 1,000,000;
1,000,000; 1,000; 10,000; 100,000; 500,000; 900,000; 1; 8; 32; 100,000;
and 256.

## Files

- `raw_trials.csv` contains the unaggregated fresh-process measurements.
- `query_validation.json` records DuckDB, scalar, and batch parity for every
  workload and mode.
- `environment.json` records source, build, compiler, operating system, CPU,
  memory, lockfile, and dataset provenance.
- `dataset_manifest.json` is the exact manifest used by the run.

## Limitations

One repetition cannot characterize runtime variance, cache effects, or p95
behavior. No speedup, regression, or memory comparison should be inferred from
these samples. Q01 is a direct scan/consume workload, so its scalar route uses
the common batch input consumer rather than a row-at-a-time expression loop.
Repeated matched trials, Linux measurements, ablations, summaries, and charts
remain separate later work.
