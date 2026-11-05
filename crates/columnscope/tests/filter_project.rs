use std::path::PathBuf;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use columnscope::bind::{bind_expr, bind_query};
use columnscope::error::ExecutionError;
use columnscope::evaluate::evaluate;
use columnscope::execution::{BatchOperator, OperatorState};
use columnscope::expr::{BoundExpr, BoundExprKind, Expr, ExprType, ScalarValue};
use columnscope::logical::{BoundLogicalPlan, QueryPlan};
use columnscope::operators::{FilterExec, ProjectExec};
use columnscope::scan::ParquetScan;
use columnscope::schema::ScalarType;
use columnscope::{fixture_batch, write_fixture};
use serde::Deserialize;
use tempfile::tempdir;

#[derive(Debug, Deserialize, PartialEq)]
struct OracleFixture {
    columns: Vec<String>,
    rows: Vec<(i64, String)>,
}

fn plan_fixture() -> QueryPlan {
    QueryPlan::from_json(include_str!(
        "../../../fixtures/plans/filter_project_v1.json"
    ))
    .expect("parse plan fixture")
}

fn physical_filter_project(path: PathBuf, batch_size: usize) -> ProjectExec {
    let bound = bind_query(plan_fixture()).expect("bind plan fixture");
    let BoundLogicalPlan::Project {
        input, expressions, ..
    } = bound.plan
    else {
        panic!("fixture root should be Project")
    };
    let BoundLogicalPlan::Filter { predicate, .. } = *input else {
        panic!("fixture project input should be Filter")
    };
    let scan = ParquetScan::try_new(path, batch_size).expect("open fixture scan");
    let filter = FilterExec::try_new(Box::new(scan), predicate).expect("build filter");
    ProjectExec::new(Box::new(filter), expressions)
}

#[test]
fn filter_project_matches_the_checked_duckdb_oracle() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");

    let mut project = physical_filter_project(path, 2);
    let expected: OracleFixture = serde_json::from_str(include_str!(
        "../../../fixtures/oracles/filter_project_v1.json"
    ))
    .expect("parse oracle fixture");
    let actual_columns = project
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut actual_rows = Vec::new();
    while let Some(batch) = project.next_batch().expect("pull project") {
        assert_eq!(batch.num_columns(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let areas = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual_rows.push((ids.value(row), areas.value(row).to_owned()));
        }
    }

    assert_eq!(actual_columns, expected.columns);
    assert_eq!(actual_rows, expected.rows);
    assert_eq!(project.metrics().output_batches, 2);
    assert_eq!(project.metrics().output_rows, 2);
    assert_eq!(project.state(), OperatorState::Exhausted);
    assert!(
        project
            .next_batch()
            .expect("repeat end-of-stream")
            .is_none()
    );
}

#[test]
fn vectorized_boolean_and_null_semantics_match_sql_where() {
    let batch = fixture_batch().expect("build fixture");
    let schema = plan_fixture().input_schema;
    let expression = Expr::Or {
        left: Box::new(Expr::And {
            left: Box::new(Expr::Column {
                name: "active".to_owned(),
            }),
            right: Box::new(Expr::GtEq {
                left: Box::new(Expr::Column {
                    name: "metric".to_owned(),
                }),
                right: Box::new(Expr::Literal {
                    literal: ScalarValue::Float64 { value: Some(4.0) },
                }),
            }),
        }),
        right: Box::new(Expr::IsNull {
            expr: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
        }),
    };
    let bound = bind_expr(expression, &schema).expect("bind expression");
    let actual = evaluate(&bound, &batch).expect("evaluate expression");
    let actual = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        actual.iter().collect::<Vec<_>>(),
        [Some(false), Some(true), Some(false), Some(true)]
    );

    let null_comparison = bind_expr(
        Expr::Eq {
            left: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Float64 { value: None },
            }),
        },
        &schema,
    )
    .expect("bind null comparison");
    let actual = evaluate(&null_comparison, &batch).expect("evaluate null comparison");
    let actual = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(actual.iter().all(|value| value.is_none()));
}

#[test]
fn all_comparison_and_unary_forms_evaluate_with_bound_types() {
    let batch = fixture_batch().expect("build fixture");
    let schema = plan_fixture().input_schema;
    let expressions = [
        Expr::NotEq {
            left: Box::new(Expr::Column {
                name: "region".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Utf8 {
                    value: Some("north".to_owned()),
                },
            }),
        },
        Expr::LtEq {
            left: Box::new(Expr::Column {
                name: "id".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Int64 { value: Some(1) },
            }),
        },
        Expr::Gt {
            left: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Float64 { value: Some(0.0) },
            }),
        },
        Expr::Not {
            expr: Box::new(Expr::Column {
                name: "active".to_owned(),
            }),
        },
        Expr::IsNotNull {
            expr: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
        },
    ];
    let expected = [
        vec![Some(false), Some(true), Some(true), Some(false)],
        vec![Some(true), Some(true), Some(false), Some(false)],
        vec![Some(true), None, Some(false), Some(true)],
        vec![Some(false), Some(true), None, Some(false)],
        vec![Some(true), Some(false), Some(true), Some(true)],
    ];

    for (expression, expected) in expressions.into_iter().zip(expected) {
        let bound = bind_expr(expression, &schema).expect("bind expression");
        let actual = evaluate(&bound, &batch).expect("evaluate expression");
        let actual = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(actual.iter().collect::<Vec<_>>(), expected);
    }
}

#[test]
fn project_failure_is_terminal_and_does_not_restart_the_scan() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let scan = ParquetScan::try_new(path, 2).expect("open scan");
    let invalid = BoundExpr {
        kind: BoundExprKind::Column {
            index: 99,
            name: "missing".to_owned(),
        },
        result: ExprType {
            data_type: ScalarType::Int64,
            nullable: false,
        },
    };
    let mut project = ProjectExec::new(
        Box::new(scan),
        vec![columnscope::logical::BoundNamedExpr {
            alias: "missing".to_owned(),
            expr: invalid,
        }],
    );

    assert!(matches!(
        project.next_batch(),
        Err(ExecutionError::ColumnOutOfBounds {
            index: 99,
            columns: 4
        })
    ));
    assert_eq!(project.state(), OperatorState::Failed);
    assert!(matches!(
        project.next_batch(),
        Err(ExecutionError::OperatorFailed)
    ));
    assert_eq!(project.metrics().pull_calls, 1);
    assert_eq!(project.metrics().output_batches, 0);
}

#[test]
fn filter_project_pipeline_can_run_on_a_worker_thread() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let mut project = physical_filter_project(path, 3);

    let rows = std::thread::spawn(move || {
        let mut rows = 0;
        while let Some(batch) = project.next_batch().expect("pull pipeline") {
            rows += batch.num_rows();
        }
        rows
    })
    .join()
    .expect("worker should not panic");
    assert_eq!(rows, 2);
}

#[test]
fn literal_predicates_cover_no_match_and_all_match_batches() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let schema = plan_fixture().input_schema;

    for (value, expected_rows) in [(false, 0), (true, 4)] {
        let predicate = bind_expr(
            Expr::Literal {
                literal: ScalarValue::Boolean { value: Some(value) },
            },
            &schema,
        )
        .expect("bind literal predicate");
        let scan = ParquetScan::try_new(&path, 8_192).expect("open scan");
        let mut filter = FilterExec::try_new(Box::new(scan), predicate).expect("build filter");
        let batch = filter.next_batch().expect("pull filter").unwrap();
        assert_eq!(batch.num_rows(), expected_rows);
        assert!(filter.next_batch().expect("finish filter").is_none());
        assert_eq!(filter.metrics().output_rows, expected_rows);
    }
}
