use std::fs;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use columnscope::{fixture_batch, read_parquet_batches, summarize_batches, write_fixture};
use tempfile::tempdir;

#[test]
fn parquet_round_trip_preserves_schema_values_and_nulls() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");

    let batches = read_parquet_batches(&path, 8_192).expect("decode fixture");
    assert_eq!(batches.len(), 1);
    let actual = &batches[0];
    let expected = fixture_batch().expect("build expected fixture");
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.num_rows(), 4);

    let ids = actual
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[0, 1, 2, 3]);

    let metric = actual
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(metric.value(0), 1.5);
    assert!(metric.is_null(1));
    assert_eq!(metric.value(2), -2.0);
    assert_eq!(metric.value(3), 4.25);

    let active = actual
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(active.value(0));
    assert!(!active.value(1));
    assert!(active.is_null(2));
    assert!(active.value(3));

    let region = actual
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        region.iter().collect::<Vec<_>>(),
        vec![Some("north"), Some("south"), Some("west"), Some("north"),]
    );
}

#[test]
fn decoder_honors_small_batch_size_and_consumer_counts_every_row() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");

    let batches = read_parquet_batches(&path, 2).expect("decode fixture");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [2, 2]
    );
    assert_eq!(
        summarize_batches(&batches).expect("summarize batches"),
        columnscope::FixtureSummary {
            batches: 2,
            columns: 4,
            rows: 4,
        }
    );
}

#[test]
fn zero_batch_size_is_rejected_before_opening_input() {
    let error = read_parquet_batches("does-not-exist.parquet", 0).unwrap_err();
    assert!(error.to_string().contains("batch size must be positive"));
}

#[test]
fn invalid_parquet_is_reported_as_an_error() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("invalid.parquet");
    fs::write(&path, b"not parquet").expect("write invalid fixture");

    let error = read_parquet_batches(&path, 8_192).unwrap_err();
    assert!(!error.to_string().is_empty());
}
