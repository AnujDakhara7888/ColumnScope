use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use columnscope::aggregate::AggregateOptions;
use columnscope::benchmark::{FROZEN_WORKLOADS, benchmark_schema};
use columnscope::measurement::{
    BenchmarkEngine, BenchmarkMode, BenchmarkPlanning, TrialConfig, capture_environment, run_trial,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::tempdir;

const SOURCE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DATASET_SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[test]
fn end_to_end_ablation_stages_keep_the_same_result() {
    let directory = tempdir().expect("create trial directory");
    let input = directory.path().join("fixture.parquet");
    write_benchmark_fixture(&input);
    let mut checksums = Vec::new();
    for stage in [
        "unoptimized",
        "projection_only",
        "filter_pushdown",
        "optimized",
    ] {
        let planning = stage.parse().expect("parse ablation stage");
        let record = run_trial(config(
            &input,
            "Q03",
            BenchmarkEngine::Batch,
            BenchmarkMode::EndToEnd,
            planning,
        ))
        .expect("run ablation trial");
        assert_eq!(record.result_rows, 2);
        assert_eq!(record.flags, format!("planning={stage}"));
        assert_eq!(serde_json::to_value(record.planning).unwrap(), stage);
        checksums.push(record.result_checksum);
    }
    assert!(checksums.iter().all(|checksum| checksum == &checksums[0]));
}

#[test]
fn execution_only_scalar_and_batch_trials_share_predecoded_results() {
    let directory = tempdir().expect("create trial directory");
    let input = directory.path().join("fixture.parquet");
    write_benchmark_fixture(&input);

    let batch = run_trial(config(
        &input,
        "Q03",
        BenchmarkEngine::Batch,
        BenchmarkMode::ExecutionOnly,
        BenchmarkPlanning::Unoptimized,
    ))
    .expect("run batch trial");
    let scalar = run_trial(config(
        &input,
        "Q03",
        BenchmarkEngine::Scalar,
        BenchmarkMode::ExecutionOnly,
        BenchmarkPlanning::Unoptimized,
    ))
    .expect("run scalar trial");

    assert_eq!(batch.result_rows, 2);
    assert_eq!(batch.result_checksum, scalar.result_checksum);
    assert_eq!(batch.source_sha, SOURCE_SHA);
    assert_eq!(batch.dataset_checksum, DATASET_SHA);
    assert!(batch.runtime_ns > 0);
    assert!(scalar.runtime_ns > 0);
    assert!(batch.peak_rss_bytes >= batch.setup_peak_rss_bytes);
    assert!(scalar.peak_rss_bytes >= scalar.setup_peak_rss_bytes);
    assert_eq!(batch.counters.output_rows, 2);
    assert_eq!(scalar.counters.input_rows, 6);
    assert_eq!(scalar.counters.output_rows, 2);
}

#[test]
fn end_to_end_optimized_batch_and_scalar_aggregates_have_equal_sinks() {
    let directory = tempdir().expect("create trial directory");
    let input = directory.path().join("fixture.parquet");
    write_benchmark_fixture(&input);

    for workload in ["Q08", "Q09"] {
        let batch = run_trial(config(
            &input,
            workload,
            BenchmarkEngine::Batch,
            BenchmarkMode::EndToEnd,
            BenchmarkPlanning::Optimized,
        ))
        .expect("run optimized batch trial");
        let scalar = run_trial(config(
            &input,
            workload,
            BenchmarkEngine::Scalar,
            BenchmarkMode::EndToEnd,
            BenchmarkPlanning::Unoptimized,
        ))
        .expect("run scalar trial");

        assert_eq!(batch.result_rows, scalar.result_rows, "{workload} rows");
        assert_eq!(
            batch.result_checksum, scalar.result_checksum,
            "{workload} checksum"
        );
        assert!(batch.counters.pull_calls > 0);
        assert_eq!(scalar.counters.input_rows, 6);
    }
}

#[test]
fn invalid_trial_combinations_are_rejected_before_execution() {
    let missing = Path::new("missing.parquet");
    let scalar_optimized = config(
        missing,
        "Q03",
        BenchmarkEngine::Scalar,
        BenchmarkMode::EndToEnd,
        BenchmarkPlanning::Optimized,
    );
    assert!(
        run_trial(scalar_optimized)
            .expect_err("scalar optimization should fail")
            .to_string()
            .contains("scalar trials")
    );

    let optimized_memory = config(
        missing,
        "Q03",
        BenchmarkEngine::Batch,
        BenchmarkMode::ExecutionOnly,
        BenchmarkPlanning::Optimized,
    );
    assert!(
        run_trial(optimized_memory)
            .expect_err("execution-only optimization should fail")
            .to_string()
            .contains("predecoded")
    );

    let mut multiple_threads = config(
        missing,
        "Q03",
        BenchmarkEngine::Batch,
        BenchmarkMode::EndToEnd,
        BenchmarkPlanning::Unoptimized,
    );
    multiple_threads.threads = 2;
    assert!(
        run_trial(multiple_threads)
            .expect_err("multiple threads should fail")
            .to_string()
            .contains("one thread")
    );
}

#[test]
fn environment_metadata_records_source_toolchain_and_dataset_provenance() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let environment = capture_environment(
        SOURCE_SHA.to_owned(),
        root.join("Cargo.lock"),
        root.join("benchmarks/dataset_manifest.json"),
    )
    .expect("capture environment");

    assert_eq!(environment.source_sha, SOURCE_SHA);
    assert_eq!(environment.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(environment.cargo_lock_sha256.len(), 64);
    assert_eq!(environment.dataset_manifest_sha256.len(), 64);
    assert_eq!(environment.dataset_logical_rows_sha256.len(), 64);
    assert!(!environment.rustc_version.is_empty());
    assert!(!environment.operating_system.is_empty());
    assert!(!environment.architecture.is_empty());
    assert!(environment.logical_cpus > 0);
}

fn config(
    input: &Path,
    workload: &str,
    engine: BenchmarkEngine,
    mode: BenchmarkMode,
    planning: BenchmarkPlanning,
) -> TrialConfig {
    let workload = FROZEN_WORKLOADS
        .iter()
        .find(|candidate| candidate.id == workload)
        .expect("known workload");
    TrialConfig {
        workload: workload.id.to_owned(),
        plan_json: workload.plan_json.to_owned(),
        input: input.to_owned(),
        engine,
        mode,
        planning,
        layout: "fixture".to_owned(),
        batch_size: 2,
        threads: 1,
        trial: 0,
        warmup: false,
        source_sha: SOURCE_SHA.to_owned(),
        dataset_checksum: DATASET_SHA.to_owned(),
        aggregate: AggregateOptions::default(),
    }
}

fn write_benchmark_fixture(path: &Path) {
    let batch = RecordBatch::try_new(
        benchmark_schema(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 10, 20, 20, 30, 30])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "north", "north", "south", "south", "east", "east",
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "category_00",
                "category_00",
                "category_01",
                "category_01",
                "category_02",
                "category_02",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(5),
                None,
                Some(7),
                Some(-2),
                Some(9),
                None,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(2.0),
                Some(4.0),
                Some(8.0),
                Some(16.0),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(true),
                Some(false),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0, 5, 10, 100, 1_000, 9_000])) as ArrayRef,
        ],
    )
    .expect("build benchmark fixture");
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(3))
        .build();
    let file = std::fs::File::create(path).expect("create benchmark fixture");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .expect("open benchmark writer");
    writer.write(&batch).expect("write benchmark fixture");
    writer.close().expect("close benchmark fixture");
}
