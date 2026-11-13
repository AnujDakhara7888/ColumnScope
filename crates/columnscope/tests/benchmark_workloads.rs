use std::fs::File;

use arrow::array::Int64Array;
use columnscope::benchmark::{
    BENCHMARK_ROW_COUNT, DatasetManifest, FROZEN_WORKLOADS, generate_benchmark_datasets,
    validate_benchmark_layout,
};
use columnscope::bind::bind_query;
use columnscope::expr::{BoundExprKind, ScalarValue};
use columnscope::logical::{AggregateFunction, BoundLogicalPlan, LogicalPlan, QueryPlan};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use tempfile::tempdir;

const COMMITTED_MANIFEST: &str = include_str!("../../../benchmarks/dataset_manifest.json");

#[test]
fn all_twelve_frozen_plans_bind_and_have_matching_oracle_sql() {
    let expected_ids = (1..=12).map(|id| format!("Q{id:02}")).collect::<Vec<_>>();
    assert_eq!(
        FROZEN_WORKLOADS
            .iter()
            .map(|workload| workload.id)
            .collect::<Vec<_>>(),
        expected_ids
    );

    for workload in FROZEN_WORKLOADS {
        let plan = QueryPlan::from_json(workload.plan_json)
            .unwrap_or_else(|error| panic!("{} plan must parse: {error}", workload.id));
        assert_eq!(plan.input_schema.fields.len(), 8, "{} schema", workload.id);
        bind_query(plan).unwrap_or_else(|error| panic!("{} plan must bind: {error}", workload.id));
        assert!(workload.oracle_sql.contains("FROM benchmark_data"));
        assert!(workload.oracle_sql.trim_end().ends_with(';'));
    }
}

#[test]
fn frozen_workload_shapes_match_the_twelve_query_contracts() {
    assert!(matches!(plan("Q01").plan, LogicalPlan::Scan));
    let LogicalPlan::Project { expressions, .. } = plan("Q02").plan else {
        panic!("Q02 must project")
    };
    assert_eq!(
        expressions
            .iter()
            .map(|expression| expression.alias.as_str())
            .collect::<Vec<_>>(),
        ["id", "amount_cents"]
    );

    for (id, threshold) in [
        ("Q03", 10),
        ("Q04", 100),
        ("Q05", 1_000),
        ("Q06", 5_000),
        ("Q07", 9_000),
    ] {
        let bound = bind_query(plan(id)).expect("bind selectivity workload");
        let BoundLogicalPlan::Project { input, .. } = bound.plan else {
            panic!("{id} must project")
        };
        let BoundLogicalPlan::Filter { predicate, .. } = *input else {
            panic!("{id} must filter")
        };
        let BoundExprKind::Lt(left, right) = predicate.kind else {
            panic!("{id} must use less-than")
        };
        assert!(matches!(left.kind, BoundExprKind::Column { index: 7, .. }));
        assert_eq!(
            right.kind,
            BoundExprKind::Literal(ScalarValue::Int64 {
                value: Some(threshold)
            })
        );
    }

    let expected_aggregates = [
        ("Q08", 0, 3),
        ("Q09", 1, 2),
        ("Q10", 1, 3),
        ("Q11", 1, 2),
        ("Q12", 2, 2),
    ];
    for (id, expected_groups, expected_functions) in expected_aggregates {
        let bound = bind_query(plan(id)).expect("bind aggregate workload");
        let BoundLogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } = bound.plan
        else {
            panic!("{id} must aggregate")
        };
        assert_eq!(group_by.len(), expected_groups, "{id} group keys");
        assert_eq!(aggregates.len(), expected_functions, "{id} aggregates");
    }

    let bound = bind_query(plan("Q10")).expect("bind Q10");
    let BoundLogicalPlan::Aggregate { aggregates, .. } = bound.plan else {
        unreachable!()
    };
    assert_eq!(
        aggregates
            .iter()
            .map(|aggregate| aggregate.function)
            .collect::<Vec<_>>(),
        [
            AggregateFunction::Avg,
            AggregateFunction::Min,
            AggregateFunction::Max
        ]
    );
}

#[test]
fn million_row_layouts_match_the_committed_manifest_and_logical_rows() {
    let directory = tempdir().expect("create dataset directory");
    let actual = generate_benchmark_datasets(directory.path()).expect("generate datasets");
    let expected = DatasetManifest::from_json(COMMITTED_MANIFEST).expect("parse manifest");
    assert_eq!(actual, expected);

    let random = validate_benchmark_layout(directory.path().join("random.parquet"))
        .expect("validate random layout");
    let clustered = validate_benchmark_layout(directory.path().join("clustered.parquet"))
        .expect("validate clustered layout");
    assert_eq!(random.row_count, BENCHMARK_ROW_COUNT);
    assert_eq!(clustered.row_count, BENCHMARK_ROW_COUNT);
    assert_eq!(random.customer_cardinality, 100_000);
    assert_eq!(clustered.customer_cardinality, 100_000);
    assert_eq!(random.region_cardinality, 8);
    assert_eq!(clustered.region_cardinality, 8);
    assert_eq!(random.category_cardinality, 32);
    assert_eq!(clustered.category_cardinality, 32);
    assert_eq!(random.logical_rows_sha256, expected.logical_rows_sha256);
    assert_eq!(clustered.logical_rows_sha256, expected.logical_rows_sha256);
    assert!(!random.selectivity_key_monotonic);
    assert!(clustered.selectivity_key_monotonic);

    for layout in &expected.layouts {
        let file = File::open(directory.path().join(&layout.file)).expect("open generated layout");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read metadata");
        let metadata = builder.metadata();
        assert_eq!(
            metadata
                .row_groups()
                .iter()
                .map(|row_group| row_group.num_rows())
                .sum::<i64>(),
            BENCHMARK_ROW_COUNT as i64
        );
        assert_eq!(metadata.num_row_groups(), 16);
        assert!(metadata.row_groups().iter().all(|row_group| {
            row_group
                .columns()
                .iter()
                .all(|column| column.compression() == Compression::SNAPPY)
        }));
    }

    let file = File::open(directory.path().join("random.parquet")).expect("open random layout");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("open reader")
        .build()
        .expect("build reader");
    let mut counts = [0usize; 5];
    let thresholds = [10, 100, 1_000, 5_000, 9_000];
    for batch in reader {
        let batch = batch.expect("decode selectivity column");
        let key = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("selectivity key is Int64");
        for value in key.values() {
            for (index, threshold) in thresholds.iter().enumerate() {
                counts[index] += usize::from(*value < *threshold);
            }
        }
    }
    assert_eq!(counts, [1_000, 10_000, 100_000, 500_000, 900_000]);
}

fn plan(id: &str) -> QueryPlan {
    let workload = FROZEN_WORKLOADS
        .iter()
        .find(|workload| workload.id == id)
        .expect("known workload");
    QueryPlan::from_json(workload.plan_json).expect("parse workload")
}
