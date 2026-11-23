use std::fs::{self, File};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use columnscope::bind::bind_query;
use columnscope::error::{ExecutionError, SnapshotError};
use columnscope::execution::{BatchOperator, OperatorMetrics, OperatorState};
use columnscope::expr::{Expr, NamedExpr, ScalarValue};
use columnscope::fixture_batch;
use columnscope::logical::{LogicalPlan, QueryPlan};
use columnscope::planner::{PlannerOptions, PlanningMode, plan_snapshot_query};
use columnscope::snapshot::{ChecksumAlgorithm, FileChecksum, SnapshotDescriptor, SnapshotFile};
use columnscope::snapshot_scan::{SnapshotScan, verify_snapshot_checksums};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const DESCRIPTOR: &str = include_str!("../../../fixtures/snapshots/descriptor_v1.json");

fn assert_send<T: Send>() {}

fn write_batch(path: &std::path::Path, batch: &RecordBatch) {
    let mut writer = ArrowWriter::try_new(
        File::create(path).expect("create Parquet part"),
        batch.schema(),
        None,
    )
    .expect("create Parquet writer");
    writer.write(batch).expect("write Parquet part");
    writer.close().expect("close Parquet part");
}

fn snapshot_file(
    path: &std::path::Path,
    object_key: &str,
    row_count: u64,
    schema_fingerprint: &str,
) -> SnapshotFile {
    let bytes = fs::read(path).expect("read Parquet part");
    SnapshotFile {
        object_key: object_key.to_owned(),
        byte_size: bytes.len() as u64,
        row_count,
        checksum: FileChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        },
        schema_fingerprint: schema_fingerprint.to_owned(),
    }
}

fn two_part_fixture() -> (
    tempfile::TempDir,
    SnapshotDescriptor,
    RecordBatch,
    std::path::PathBuf,
) {
    let directory = tempdir().expect("create snapshot root");
    let data = directory.path().join("data");
    fs::create_dir(&data).expect("create data directory");
    let batch = fixture_batch().expect("build fixture batch");
    let first_path = data.join("part-00000.parquet");
    let second_path = data.join("part-00001.parquet");
    write_batch(&first_path, &batch.slice(0, 2));
    write_batch(&second_path, &batch.slice(2, 2));

    let mut descriptor =
        SnapshotDescriptor::from_json(DESCRIPTOR).expect("parse descriptor fixture");
    descriptor.files = vec![
        snapshot_file(
            &first_path,
            "data/part-00000.parquet",
            2,
            &descriptor.schema_fingerprint,
        ),
        snapshot_file(
            &second_path,
            "data/part-00001.parquet",
            2,
            &descriptor.schema_fingerprint,
        ),
    ];
    (directory, descriptor, batch, second_path)
}

#[test]
fn snapshot_scan_reads_exact_files_in_descriptor_order() {
    assert_send::<SnapshotScan>();
    let (directory, descriptor, expected, _) = two_part_fixture();
    let mut scan =
        SnapshotScan::try_new(directory.path(), &descriptor, 1).expect("open pinned snapshot");

    assert_eq!(scan.file_count(), 2);
    assert_eq!(scan.schema(), expected.schema());
    let mut ids = Vec::new();
    while let Some(batch) = scan.next_batch().expect("pull snapshot batch") {
        ids.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 id")
                .values(),
        );
    }

    assert_eq!(ids, vec![0, 1, 2, 3]);
    assert_eq!(scan.state(), OperatorState::Exhausted);
    assert_eq!(
        scan.metrics(),
        OperatorMetrics {
            pull_calls: 5,
            output_batches: 4,
            output_rows: 4,
        }
    );
    assert!(scan.next_batch().expect("repeat end-of-stream").is_none());
    assert_eq!(scan.metrics().pull_calls, 5);
}

#[test]
fn empty_snapshot_preserves_schema_and_has_sticky_end_of_stream() {
    let directory = tempdir().expect("create snapshot root");
    let mut descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    descriptor.files.clear();
    let expected = Arc::new(descriptor.schema.to_arrow().unwrap());
    let mut scan =
        SnapshotScan::try_new(directory.path(), &descriptor, 8_192).expect("open empty snapshot");

    assert_eq!(scan.schema(), expected);
    assert!(scan.next_batch().unwrap().is_none());
    assert!(scan.next_batch().unwrap().is_none());
    assert_eq!(scan.state(), OperatorState::Exhausted);
    assert_eq!(
        scan.metrics(),
        OperatorMetrics {
            pull_calls: 1,
            output_batches: 0,
            output_rows: 0,
        }
    );
}

#[test]
fn every_file_is_opened_and_validated_before_any_output() {
    let (directory, mut descriptor, _, second_path) = two_part_fixture();
    fs::remove_file(second_path).expect("remove later snapshot part");

    let error = match SnapshotScan::try_new(directory.path(), &descriptor, 2) {
        Ok(_) => panic!("missing later file must fail construction"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ExecutionError::Snapshot(SnapshotError::FileAccess { object_key, .. })
            if object_key == "data/part-00001.parquet"
    ));

    descriptor.files[1].object_key = "data/unlisted.parquet".to_owned();
    assert!(SnapshotScan::try_new(directory.path(), &descriptor, 2).is_err());
}

#[test]
fn declared_size_and_row_count_must_match_parquet_metadata() {
    let (directory, descriptor, _, _) = two_part_fixture();
    let mut wrong_size = descriptor.clone();
    wrong_size.files[0].byte_size += 1;
    let error = SnapshotScan::try_new(directory.path(), &wrong_size, 2)
        .err()
        .expect("wrong size must fail");
    assert!(matches!(
        error,
        ExecutionError::Snapshot(SnapshotError::FileSizeMismatch { .. })
    ));

    let mut wrong_rows = descriptor;
    wrong_rows.files[0].row_count += 1;
    let error = SnapshotScan::try_new(directory.path(), &wrong_rows, 2)
        .err()
        .expect("wrong row count must fail");
    assert!(matches!(
        error,
        ExecutionError::Snapshot(SnapshotError::FileRowCountMismatch { .. })
    ));
}

#[test]
fn every_physical_file_must_match_the_descriptor_schema() {
    let (directory, mut descriptor, batch, second_path) = two_part_fixture();
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[1] = Field::new("renamed_metric", DataType::Float64, true);
    let mismatched = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        batch.slice(2, 2).columns().to_vec(),
    )
    .expect("build mismatched batch");
    write_batch(&second_path, &mismatched);
    descriptor.files[1] = snapshot_file(
        &second_path,
        "data/part-00001.parquet",
        2,
        &descriptor.schema_fingerprint,
    );

    let error = SnapshotScan::try_new(directory.path(), &descriptor, 2)
        .err()
        .expect("physical schema mismatch must fail");
    assert!(matches!(
        error,
        ExecutionError::Snapshot(SnapshotError::PhysicalFileSchemaMismatch { object_key })
            if object_key == "data/part-00001.parquet"
    ));
}

#[test]
fn checksum_validation_is_explicit_and_covers_every_file() {
    let (directory, descriptor, _, _) = two_part_fixture();
    verify_snapshot_checksums(directory.path(), &descriptor).expect("checksums should match");

    let mut mismatched = descriptor;
    mismatched.files[1].checksum.value = "0".repeat(64);
    assert_eq!(
        verify_snapshot_checksums(directory.path(), &mismatched).unwrap_err(),
        SnapshotError::FileChecksumMismatch {
            object_key: "data/part-00001.parquet".to_owned(),
        }
    );
}

#[test]
fn zstandard_snapshot_file_decodes_through_the_pinned_adapter() {
    let directory = tempdir().expect("create snapshot root");
    let data = directory.path().join("data");
    fs::create_dir(&data).expect("create data directory");
    let path = data.join("part-zstd.parquet");
    let batch = fixture_batch().expect("build fixture batch");
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).expect("create Zstandard part"),
        batch.schema(),
        Some(properties),
    )
    .expect("create Zstandard writer");
    writer.write(&batch).expect("write Zstandard part");
    writer.close().expect("close Zstandard part");

    let mut descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    descriptor.files = vec![snapshot_file(
        &path,
        "data/part-zstd.parquet",
        4,
        &descriptor.schema_fingerprint,
    )];
    let mut scan =
        SnapshotScan::try_new(directory.path(), &descriptor, 2).expect("open Zstandard snapshot");
    let mut rows = 0;
    while let Some(batch) = scan.next_batch().expect("decode Zstandard batch") {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 4);
}

#[test]
fn optimized_snapshot_plan_matches_unoptimized_and_explains_pushdowns() {
    let (directory, descriptor, _, _) = two_part_fixture();
    let query = QueryPlan {
        format_version: 1,
        input_schema: descriptor.schema.clone(),
        plan: LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan),
                predicate: Expr::Gt {
                    left: Box::new(Expr::Column {
                        name: "metric".to_owned(),
                    }),
                    right: Box::new(Expr::Literal {
                        literal: ScalarValue::Float64 { value: Some(100.0) },
                    }),
                },
            }),
            expressions: vec![NamedExpr {
                alias: "id".to_owned(),
                expr: Expr::Column {
                    name: "id".to_owned(),
                },
            }],
        },
    };
    let unoptimized = plan_snapshot_query(
        bind_query(query.clone()).expect("bind unoptimized snapshot plan"),
        directory.path(),
        &descriptor,
        PlannerOptions {
            scan_batch_rows: 2,
            ..PlannerOptions::default()
        },
    )
    .expect("plan unoptimized snapshot query");
    let optimized = plan_snapshot_query(
        bind_query(query).expect("bind optimized snapshot plan"),
        directory.path(),
        &descriptor,
        PlannerOptions {
            mode: PlanningMode::Optimized,
            scan_batch_rows: 2,
            ..PlannerOptions::default()
        },
    )
    .expect("plan optimized snapshot query");

    let explain = optimized.explain();
    assert!(explain.contains("SnapshotScan [source=events@2, commit=commit-0002, files=2,"));
    assert!(explain.contains("batch_rows=2, columns=2/4, row_groups=0/2, pruned_row_groups=2]"));
    assert_eq!(unoptimized.mode, PlanningMode::Unoptimized);
    let optimized_schema = optimized.root.schema();
    let unoptimized_schema = unoptimized.root.schema();
    let mut optimized_root = optimized.root;
    let mut unoptimized_root = unoptimized.root;
    let mut optimized_rows = 0;
    while let Some(batch) = optimized_root.next_batch().unwrap() {
        optimized_rows += batch.num_rows();
    }
    let mut unoptimized_rows = 0;
    while let Some(batch) = unoptimized_root.next_batch().unwrap() {
        unoptimized_rows += batch.num_rows();
    }
    assert_eq!((optimized_rows, unoptimized_rows), (0, 0));
    assert_eq!(optimized_schema.as_ref(), unoptimized_schema.as_ref());
}
