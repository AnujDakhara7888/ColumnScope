# Benchmarks

No query performance conclusions are published yet. Vectorized scan, filter,
projection, bounded composite aggregation, conservative pushdown/pruning, and
an independent scalar comparison path exist. The deterministic million-row
dataset, twelve query families, and first complete 48-record validation matrix
are retained. This single-sample pass is correctness and completeness evidence,
not a runtime or memory comparison.

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

## Measurement harness

`columnscope-bench-runner` launches a new release-mode
`columnscope-bench-trial` process for every warmup and measured repetition. It
writes measured samples to `raw_trials.csv`, records the build and host in
`environment.json`, and copies the dataset manifest beside the samples. Warmup
records are validated but excluded from the CSV.

The two timing boundaries are explicit:

- `execution_only` decodes the entire Parquet input before starting the timer.
  Scalar and batch trials then consume the same Arrow representation with
  unoptimized planning. The setup high-water RSS is reported separately.
- `end_to_end` starts the timer before physical planning, file opening,
  Parquet decode, execution, and result consumption. Batch trials may select
  optimized or unoptimized planning; the scalar path is unoptimized.

Both modes include an order-independent, duplicate-sensitive result sink in
the timed region. Each output row is hashed without sorting or retaining the
result set; four wrapping sums and four XOR accumulators form the final
checksum. Accepted comparisons require equal output row counts and checksums.

Peak RSS is the process high-water mark reported by `getrusage`. Linux values
are normalized from KiB to bytes; macOS values are already bytes. This measures
the whole trial process rather than engine allocations alone. Version 1 trials
require one execution thread and reject unsupported flag combinations before
opening the input.

After generating the frozen inputs, run one configuration with:

```sh
cargo build --locked --release \
  --bin columnscope-bench-trial --bin columnscope-bench-runner

target/release/columnscope-bench-runner \
  --trial-binary target/release/columnscope-bench-trial \
  --input artifacts/columnscope/datasets/random.parquet \
  --dataset-manifest benchmarks/dataset_manifest.json \
  --cargo-lock Cargo.lock \
  --workload Q03 --engine batch --mode end_to_end \
  --planning optimized --layout random \
  --batch-size 8192 --threads 1 \
  --warmups 1 --repetitions 5 \
  --source-sha "$(git rev-parse HEAD)" \
  --output-dir artifacts/columnscope/results/q03-batch-optimized
```

The runner executes one configuration at a time so an experiment matrix stays
explicit. Generated datasets and ad hoc result directories stay outside
version control; small accepted evidence sets may be retained with their
provenance. No runtime or memory comparison is accepted or published yet.

## Complete matrix validation

The matrix driver runs all twelve workloads through four configurations:
execution-only batch and scalar, plus end-to-end optimized batch and
unoptimized scalar. It requires the one-million-row input to match the selected
manifest layout, rebuilds the release binaries, and refuses a dirty tree or a
source revision other than `HEAD`.

Create a pinned Python environment and run the initial one-sample matrix with:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

.venv/bin/python scripts/run_benchmark_matrix.py \
  --input artifacts/columnscope/datasets/random.parquet \
  --output-dir artifacts/columnscope/results/initial \
  --source-sha "$(git rev-parse HEAD)" \
  --warmups 0 --repetitions 1
```

DuckDB executes each paired SQL query with one thread. Its actual typed output
rows pass through the same documented order-independent multiset digest as the
ColumnScope sink. The driver requires equal row counts and digests for DuckDB,
scalar execution, and batch execution in both modes before publishing any
files. It writes `raw_trials.csv`, `environment.json`,
`dataset_manifest.json`, and `query_validation.json` only after every workload
passes. The output directory must not already exist.

This initial matrix is a completeness and correctness gate. A single sample is
not sufficient for a runtime or memory comparison; the later matched trial set
will use warmups and repeated measurements before reporting medians or p95.

## Observed initial matrix

The retained [initial result set](../benchmarks/results/initial/README.md) ran
Q01–Q12 over the one-million-row random layout at batch size 8,192 and one
thread. Its 48 measured records used distinct child process IDs. Every record
had positive runtime and RSS fields, one source revision, and the committed
random-layout checksum.

DuckDB 2.0.0, batch execution, and scalar execution produced the same row count
and typed multiset digest for every workload in both timing modes. Q01–Q12
returned 1,000,000; 1,000,000; 1,000; 10,000; 100,000; 500,000; 900,000; 1;
8; 32; 100,000; and 256 rows. The environment and raw samples are retained,
but their single timings are deliberately not summarized as performance
results.
