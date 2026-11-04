use std::fs::File;

use columnscope::error::ExecutionError;
use columnscope::execution::{BatchOperator, OperatorMetrics, OperatorState};
use columnscope::scan::ParquetScan;
use columnscope::{fixture_batch, write_fixture};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn assert_send<T: Send>() {}

#[test]
fn scan_pulls_batches_and_counts_only_returned_output() {
    assert_send::<ParquetScan>();

    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");

    let expected_schema = fixture_batch().expect("build fixture").schema();
    let mut scan = ParquetScan::try_new(&path, 2).expect("open scan");
    assert_eq!(scan.schema(), expected_schema);
    assert_eq!(scan.state(), OperatorState::Ready);
    assert_eq!(scan.metrics(), OperatorMetrics::default());

    let first = scan.next_batch().expect("pull first batch").unwrap();
    let second = scan.next_batch().expect("pull second batch").unwrap();
    assert_eq!([first.num_rows(), second.num_rows()], [2, 2]);
    assert_eq!(
        scan.metrics(),
        OperatorMetrics {
            pull_calls: 2,
            output_batches: 2,
            output_rows: 4,
        }
    );
    assert_eq!(scan.state(), OperatorState::Ready);

    assert!(scan.next_batch().expect("reach end-of-stream").is_none());
    assert_eq!(scan.state(), OperatorState::Exhausted);
    assert!(scan.next_batch().expect("repeat end-of-stream").is_none());
    assert_eq!(
        scan.metrics(),
        OperatorMetrics {
            pull_calls: 3,
            output_batches: 2,
            output_rows: 4,
        }
    );
}

#[test]
fn empty_parquet_stream_has_schema_and_sticky_end_of_stream() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("empty.parquet");
    let schema = fixture_batch().expect("build fixture").schema();
    let writer = ArrowWriter::try_new(
        File::create(&path).expect("create empty Parquet file"),
        schema.clone(),
        None,
    )
    .expect("create writer");
    writer.close().expect("close empty Parquet file");

    let mut scan = ParquetScan::try_new(&path, 8_192).expect("open empty scan");
    assert_eq!(scan.schema(), schema);
    assert!(scan.next_batch().expect("pull empty scan").is_none());
    assert!(scan.next_batch().expect("repeat empty scan").is_none());
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
fn invalid_batch_size_wins_over_missing_input() {
    let error = match ParquetScan::try_new("does-not-exist.parquet", 0) {
        Ok(_) => panic!("zero batch size should fail"),
        Err(error) => error,
    };
    assert!(matches!(error, ExecutionError::InvalidBatchSize));
}

#[test]
fn scan_owns_open_file_handle_after_construction() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let mut scan = ParquetScan::try_new(&path, 3).expect("open scan");
    std::fs::remove_file(&path).expect("unlink fixture after opening scan");

    let mut batches = Vec::new();
    while let Some(batch) = scan
        .next_batch()
        .expect("consume scan from its open handle")
    {
        batches.push(batch);
    }
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        4
    );
    assert_eq!(scan.state(), OperatorState::Exhausted);
}

#[test]
fn scan_can_move_to_a_worker_thread_and_stream_to_completion() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let mut scan = ParquetScan::try_new(&path, 3).expect("open scan");

    let (rows, metrics, state) = std::thread::spawn(move || {
        let mut rows = 0;
        while let Some(batch) = scan.next_batch().expect("pull on worker thread") {
            rows += batch.num_rows();
        }
        (rows, scan.metrics(), scan.state())
    })
    .join()
    .expect("worker thread should not panic");

    assert_eq!(rows, 4);
    assert_eq!(metrics.output_batches, 2);
    assert_eq!(metrics.output_rows, 4);
    assert_eq!(state, OperatorState::Exhausted);
}
