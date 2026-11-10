use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use columnscope::bind::bind_query;
use columnscope::error::ExecutionError;
use columnscope::execution::BatchOperator;
use columnscope::expr::{Expr, NamedExpr, ScalarValue};
use columnscope::logical::{AggregateExpr, LogicalPlan, QueryPlan};
use columnscope::planner::{PlannedQuery, PlannerOptions, PlanningMode, plan_query};
use columnscope::scan::{ParquetScan, ScanPushdowns};
use columnscope::schema::{FieldDef, ScalarType, SchemaDef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tempfile::tempdir;

fn source_schema() -> SchemaDef {
    SchemaDef {
        fields: vec![
            FieldDef {
                name: "id".into(),
                data_type: ScalarType::Int64,
                nullable: false,
            },
            FieldDef {
                name: "metric".into(),
                data_type: ScalarType::Float64,
                nullable: false,
            },
            FieldDef {
                name: "label".into(),
                data_type: ScalarType::Utf8,
                nullable: false,
            },
        ],
    }
}

fn write_row_group_fixture(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Float64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let metrics = vec![
        0.0, 1.0, 2.0, 3.0, 10.0, 11.0, 12.0, 13.0, 20.0, 21.0, 22.0, 23.0,
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..12)) as ArrayRef,
            Arc::new(Float64Array::from(metrics)) as ArrayRef,
            Arc::new(StringArray::from(vec!["unused"; 12])) as ArrayRef,
        ],
    )
    .expect("build row-group fixture");
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(4))
        .build();
    let file = File::create(path).expect("create Parquet fixture");
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).expect("open writer");
    writer.write(&batch).expect("write fixture batch");
    let metadata = writer.close().expect("close fixture writer");
    assert_eq!(metadata.num_row_groups(), 3);
}

fn filtered_projection_plan(predicate: Expr) -> QueryPlan {
    QueryPlan {
        format_version: 1,
        input_schema: source_schema(),
        plan: LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan),
                predicate,
            }),
            expressions: vec![NamedExpr {
                alias: "id".into(),
                expr: column("id"),
            }],
        },
    }
}

fn metric_gte(value: f64) -> Expr {
    Expr::GtEq {
        left: Box::new(column("metric")),
        right: Box::new(float_literal(value)),
    }
}

fn plan(path: &std::path::Path, query: QueryPlan, mode: PlanningMode) -> PlannedQuery {
    plan_query(
        bind_query(query).expect("bind query"),
        path,
        PlannerOptions {
            mode,
            scan_batch_rows: 2,
            ..PlannerOptions::default()
        },
    )
    .expect("plan query")
}

fn collect_ids(mut query: PlannedQuery) -> Vec<i64> {
    let mut rows = Vec::new();
    while let Some(batch) = query.root.next_batch().expect("execute query") {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| ids.value(row)));
    }
    rows
}

#[test]
fn optimized_projection_and_pruning_match_unoptimized_results() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("row-groups.parquet");
    write_row_group_fixture(&path);
    let query = filtered_projection_plan(metric_gte(12.0));
    let unoptimized = plan(&path, query.clone(), PlanningMode::Unoptimized);
    let optimized = plan(&path, query, PlanningMode::Optimized);
    let explain = optimized.explain();

    assert!(explain.starts_with("mode=optimized [filter_pushdowns=0]\n"));
    assert!(explain.contains("FilterExec [predicate=Boolean!, residual=true]"));
    assert!(explain.contains(
        "ParquetScan [source=row-groups.parquet, batch_rows=2, columns=2/3, row_groups=2/3, pruned_row_groups=1] schema=[id:Int64!, metric:Float64!]"
    ));

    let expected = vec![6, 7, 8, 9, 10, 11];
    assert_eq!(collect_ids(unoptimized), expected);
    assert_eq!(collect_ids(optimized), expected);
}

#[test]
fn filter_moves_below_projection_and_remains_residual() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("row-groups.parquet");
    write_row_group_fixture(&path);
    let query = QueryPlan {
        format_version: 1,
        input_schema: source_schema(),
        plan: LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan),
                expressions: vec![
                    NamedExpr {
                        alias: "event_id".into(),
                        expr: column("id"),
                    },
                    NamedExpr {
                        alias: "score".into(),
                        expr: column("metric"),
                    },
                ],
            }),
            predicate: Expr::GtEq {
                left: Box::new(column("score")),
                right: Box::new(float_literal(12.0)),
            },
        },
    };
    let unoptimized = plan(&path, query.clone(), PlanningMode::Unoptimized);
    assert!(unoptimized.explain().contains(
        "FilterExec [predicate=Boolean!] schema=[event_id:Int64!, score:Float64!]\n  ProjectExec"
    ));
    let optimized = plan(&path, query, PlanningMode::Optimized);
    assert!(optimized.explain().contains(
        "ProjectExec [expressions=2] schema=[event_id:Int64!, score:Float64!]\n  FilterExec [predicate=Boolean!, residual=true]"
    ));
    assert_eq!(optimized.filter_pushdowns, 1);

    let expected = vec![6, 7, 8, 9, 10, 11];
    assert_eq!(collect_ids(unoptimized), expected);
    assert_eq!(
        std::thread::spawn(move || collect_ids(optimized))
            .join()
            .expect("worker should not panic"),
        expected
    );
}

#[test]
fn conservative_or_pruning_keeps_each_possible_row_group() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("row-groups.parquet");
    write_row_group_fixture(&path);
    let predicate = Expr::Or {
        left: Box::new(Expr::Lt {
            left: Box::new(column("metric")),
            right: Box::new(float_literal(1.0)),
        }),
        right: Box::new(Expr::Gt {
            left: Box::new(column("metric")),
            right: Box::new(float_literal(22.0)),
        }),
    };
    let query = filtered_projection_plan(predicate);
    let unoptimized = plan(&path, query.clone(), PlanningMode::Unoptimized);
    let optimized = plan(&path, query, PlanningMode::Optimized);
    assert!(
        optimized
            .explain()
            .contains("row_groups=2/3, pruned_row_groups=1")
    );
    assert_eq!(collect_ids(unoptimized), vec![0, 11]);
    assert_eq!(collect_ids(optimized), vec![0, 11]);
}

#[test]
fn unsupported_not_predicate_disables_pruning_but_not_projection() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("row-groups.parquet");
    write_row_group_fixture(&path);
    let predicate = Expr::Not {
        expr: Box::new(metric_gte(12.0)),
    };
    let query = filtered_projection_plan(predicate);
    let unoptimized = plan(&path, query.clone(), PlanningMode::Unoptimized);
    let optimized = plan(&path, query, PlanningMode::Optimized);
    assert!(
        optimized
            .explain()
            .contains("columns=2/3, row_groups=3/3, pruned_row_groups=0")
    );
    assert_eq!(collect_ids(unoptimized), vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(collect_ids(optimized), vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn count_star_can_scan_row_counts_without_decoding_columns() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("row-groups.parquet");
    write_row_group_fixture(&path);
    let query = QueryPlan {
        format_version: 1,
        input_schema: source_schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![AggregateExpr::CountStar {
                alias: "rows".into(),
            }],
        },
    };
    let mut optimized = plan(&path, query, PlanningMode::Optimized);
    assert!(
        optimized
            .explain()
            .contains("columns=0/3, row_groups=3/3, pruned_row_groups=0")
    );
    let output = optimized
        .root
        .next_batch()
        .expect("execute count")
        .expect("count output");
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        12
    );
}

#[test]
fn invalid_scan_projection_is_rejected_before_decode() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("row-groups.parquet");
    write_row_group_fixture(&path);
    assert!(matches!(
        ParquetScan::try_new_with_pushdowns(
            &path,
            2,
            ScanPushdowns {
                projection: Some(vec![0, 0]),
                pruning_predicate: None,
            }
        ),
        Err(ExecutionError::DuplicateProjectionColumn(0))
    ));
    assert!(matches!(
        ParquetScan::try_new_with_pushdowns(
            path,
            2,
            ScanPushdowns {
                projection: Some(vec![3]),
                pruning_predicate: None,
            }
        ),
        Err(ExecutionError::ProjectionOutOfBounds {
            index: 3,
            columns: 3
        })
    ));

    let scan = ParquetScan::try_new_with_pushdowns(
        directory.path().join("row-groups.parquet"),
        2,
        ScanPushdowns {
            projection: Some(vec![1, 0]),
            pruning_predicate: None,
        },
    )
    .expect("normalize projection order");
    assert_eq!(scan.schema().field(0).name(), "id");
    assert_eq!(scan.schema().field(1).name(), "metric");
}

fn column(name: &str) -> Expr {
    Expr::Column {
        name: name.to_owned(),
    }
}

fn float_literal(value: f64) -> Expr {
    Expr::Literal {
        literal: ScalarValue::Float64 { value: Some(value) },
    }
}
