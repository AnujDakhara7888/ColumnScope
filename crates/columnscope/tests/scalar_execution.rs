use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use columnscope::bind::{bind_expr, bind_query};
use columnscope::error::ExecutionError;
use columnscope::execution::{BatchOperator, OperatorMetrics, OperatorState};
use columnscope::expr::{BoundExpr, BoundExprKind, Expr, ExprType, ScalarValue};
use columnscope::logical::{BoundLogicalPlan, BoundNamedExpr, QueryPlan};
use columnscope::operators::{FilterExec, ProjectExec};
use columnscope::scalar::{
    ScalarFilterProject, collect_batch_result, collect_scalar_result, evaluate_scalar,
};
use columnscope::scan::ParquetScan;
use columnscope::schema::{FieldDef, ScalarType, SchemaDef};
use columnscope::{fixture_batch, read_parquet_batches, write_fixture};
use tempfile::tempdir;

struct BatchInput {
    batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
}

impl BatchInput {
    fn new(batches: Vec<RecordBatch>, fallback_schema: SchemaRef) -> Self {
        let schema = batches.first().map_or(fallback_schema, RecordBatch::schema);
        Self {
            batches: batches.into(),
            schema,
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
        }
    }
}

impl BatchOperator for BatchInput {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }
        self.metrics.pull_calls += 1;
        match self.batches.pop_front() {
            Some(batch) => {
                self.metrics.output_batches += 1;
                self.metrics.output_rows += batch.num_rows();
                Ok(Some(batch))
            }
            None => {
                self.state = OperatorState::Exhausted;
                Ok(None)
            }
        }
    }

    fn metrics(&self) -> OperatorMetrics {
        self.metrics
    }

    fn state(&self) -> OperatorState {
        self.state
    }
}

fn plan_fixture() -> QueryPlan {
    QueryPlan::from_json(include_str!(
        "../../../fixtures/plans/filter_project_v1.json"
    ))
    .expect("parse plan fixture")
}

fn bound_filter_project() -> (BoundExpr, Vec<BoundNamedExpr>) {
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
    (predicate, expressions)
}

#[test]
fn scalar_and_vectorized_paths_match_on_shared_arrow_buffers() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let batches = read_parquet_batches(&path, 2).expect("decode shared batches");
    let schema = batches[0].schema();
    let (predicate, projections) = bound_filter_project();

    let vector_input = BatchInput::new(batches.clone(), schema.clone());
    let filter = FilterExec::try_new(Box::new(vector_input), predicate.clone())
        .expect("build vector filter");
    let mut project = ProjectExec::new(Box::new(filter), projections.clone());
    let vector_result = collect_batch_result(&mut project).expect("collect vector result");

    let scalar_input = BatchInput::new(batches, schema);
    let mut scalar =
        ScalarFilterProject::try_new(Box::new(scalar_input), Some(predicate), projections)
            .expect("build scalar pipeline");
    let scalar_result = collect_scalar_result(&mut scalar).expect("collect scalar result");

    assert_eq!(scalar_result, vector_result);
    assert_eq!(
        scalar_result.rows,
        [
            vec![
                ScalarValue::Int64 { value: Some(0) },
                ScalarValue::Utf8 {
                    value: Some("north".to_owned())
                }
            ],
            vec![
                ScalarValue::Int64 { value: Some(3) },
                ScalarValue::Utf8 {
                    value: Some("north".to_owned())
                }
            ]
        ]
    );
    assert_eq!(scalar.metrics().input_batches, 2);
    assert_eq!(scalar.metrics().input_rows, 4);
    assert_eq!(scalar.metrics().output_rows, 2);
    assert_eq!(scalar.state(), OperatorState::Exhausted);
    assert!(scalar.next_row().expect("repeat end-of-stream").is_none());
}

#[test]
fn complete_expression_grammar_matches_vectorized_evaluation() {
    let batch = fixture_batch().expect("build fixture");
    let schema = plan_fixture().input_schema;
    let expressions = vec![
        Expr::Eq {
            left: Box::new(Expr::Column {
                name: "region".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Utf8 {
                    value: Some("north".to_owned()),
                },
            }),
        },
        Expr::NotEq {
            left: Box::new(Expr::Column {
                name: "id".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Int64 { value: Some(1) },
            }),
        },
        Expr::Lt {
            left: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Float64 { value: Some(2.0) },
            }),
        },
        Expr::LtEq {
            left: Box::new(Expr::Column {
                name: "id".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Int64 { value: Some(2) },
            }),
        },
        Expr::Gt {
            left: Box::new(Expr::Column {
                name: "id".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Int64 { value: Some(0) },
            }),
        },
        Expr::GtEq {
            left: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Float64 { value: Some(1.5) },
            }),
        },
        Expr::And {
            left: Box::new(Expr::Column {
                name: "active".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Boolean { value: Some(true) },
            }),
        },
        Expr::Or {
            left: Box::new(Expr::Column {
                name: "active".to_owned(),
            }),
            right: Box::new(Expr::Literal {
                literal: ScalarValue::Boolean { value: Some(false) },
            }),
        },
        Expr::Not {
            expr: Box::new(Expr::Column {
                name: "active".to_owned(),
            }),
        },
        Expr::IsNull {
            expr: Box::new(Expr::Column {
                name: "metric".to_owned(),
            }),
        },
        Expr::IsNotNull {
            expr: Box::new(Expr::Column {
                name: "active".to_owned(),
            }),
        },
    ];
    let projections = expressions
        .into_iter()
        .enumerate()
        .map(|(index, expr)| BoundNamedExpr {
            alias: format!("e{index}"),
            expr: bind_expr(expr, &schema).expect("bind expression"),
        })
        .collect::<Vec<_>>();

    let input_schema = batch.schema();
    let mut vectorized = ProjectExec::new(
        Box::new(BatchInput::new(vec![batch.clone()], input_schema.clone())),
        projections.clone(),
    );
    let vector_result = collect_batch_result(&mut vectorized).expect("collect vector result");
    let mut scalar = ScalarFilterProject::try_new(
        Box::new(BatchInput::new(vec![batch], input_schema)),
        None,
        projections,
    )
    .expect("build scalar pipeline");
    let scalar_result = collect_scalar_result(&mut scalar).expect("collect scalar result");
    assert_eq!(scalar_result, vector_result);
}

#[test]
fn full_kleene_truth_tables_match_vectorized_evaluation() {
    let left = vec![
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        Some(false),
        None,
        None,
        None,
    ];
    let right = vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ];
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Boolean, true),
        Field::new("right", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(BooleanArray::from(left)) as ArrayRef,
            Arc::new(BooleanArray::from(right)) as ArrayRef,
        ],
    )
    .expect("build truth-table batch");
    let bind_schema = SchemaDef {
        fields: vec![
            FieldDef {
                name: "left".to_owned(),
                data_type: ScalarType::Boolean,
                nullable: true,
            },
            FieldDef {
                name: "right".to_owned(),
                data_type: ScalarType::Boolean,
                nullable: true,
            },
        ],
    };
    let column = |name: &str| Expr::Column {
        name: name.to_owned(),
    };
    let projections = vec![
        BoundNamedExpr {
            alias: "and_result".to_owned(),
            expr: bind_expr(
                Expr::And {
                    left: Box::new(column("left")),
                    right: Box::new(column("right")),
                },
                &bind_schema,
            )
            .expect("bind AND"),
        },
        BoundNamedExpr {
            alias: "or_result".to_owned(),
            expr: bind_expr(
                Expr::Or {
                    left: Box::new(column("left")),
                    right: Box::new(column("right")),
                },
                &bind_schema,
            )
            .expect("bind OR"),
        },
    ];

    let mut vectorized = ProjectExec::new(
        Box::new(BatchInput::new(vec![batch.clone()], arrow_schema.clone())),
        projections.clone(),
    );
    let vector_result = collect_batch_result(&mut vectorized).expect("collect vector result");
    let mut scalar = ScalarFilterProject::try_new(
        Box::new(BatchInput::new(vec![batch], arrow_schema)),
        None,
        projections,
    )
    .expect("build scalar pipeline");
    let scalar_result = collect_scalar_result(&mut scalar).expect("collect scalar result");
    assert_eq!(scalar_result, vector_result);

    let expected_and = [
        Some(true),
        Some(false),
        None,
        Some(false),
        Some(false),
        Some(false),
        None,
        Some(false),
        None,
    ];
    let expected_or = [
        Some(true),
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        None,
        Some(true),
        None,
        None,
    ];
    for (index, row) in scalar_result.rows.iter().enumerate() {
        assert_eq!(
            row,
            &[
                ScalarValue::Boolean {
                    value: expected_and[index]
                },
                ScalarValue::Boolean {
                    value: expected_or[index]
                }
            ]
        );
    }
}

#[test]
fn empty_input_has_schema_and_sticky_end_of_stream() {
    let schema = fixture_batch().expect("build fixture").schema();
    let (_, projections) = bound_filter_project();
    let input = BatchInput::new(Vec::new(), schema);
    let mut scalar = ScalarFilterProject::try_new(Box::new(input), None, projections)
        .expect("build scalar pipeline");

    assert!(scalar.next_row().expect("finish empty input").is_none());
    assert!(scalar.next_row().expect("repeat end-of-stream").is_none());
    assert_eq!(scalar.state(), OperatorState::Exhausted);
    assert_eq!(scalar.metrics().pull_calls, 1);
    assert_eq!(scalar.metrics().input_batches, 0);
    assert_eq!(scalar.metrics().output_rows, 0);
    assert_eq!(scalar.schema().fields().len(), 2);
}

#[test]
fn scalar_failure_is_terminal_and_rejects_nonfinite_input() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "metric",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef],
    )
    .expect("build nonfinite batch");
    let projection = BoundNamedExpr {
        alias: "metric".to_owned(),
        expr: BoundExpr {
            kind: BoundExprKind::Column {
                index: 0,
                name: "metric".to_owned(),
            },
            result: ExprType {
                data_type: ScalarType::Float64,
                nullable: false,
            },
        },
    };
    let input = BatchInput::new(vec![batch], schema);
    let mut scalar = ScalarFilterProject::try_new(Box::new(input), None, vec![projection])
        .expect("build scalar pipeline");

    assert!(matches!(
        scalar.next_row(),
        Err(ExecutionError::NonFiniteFloatInput)
    ));
    assert_eq!(scalar.state(), OperatorState::Failed);
    assert!(matches!(
        scalar.next_row(),
        Err(ExecutionError::OperatorFailed)
    ));
    assert_eq!(scalar.metrics().output_rows, 0);
}

#[test]
fn scalar_pipeline_moves_to_a_worker_thread() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let (predicate, projections) = bound_filter_project();
    let scan = ParquetScan::try_new(path, 3).expect("open scan");
    let scalar = ScalarFilterProject::try_new(Box::new(scan), Some(predicate), projections)
        .expect("build scalar Parquet pipeline");

    let result = std::thread::spawn(move || {
        let mut scalar = scalar;
        collect_scalar_result(&mut scalar)
    })
    .join()
    .expect("worker should not panic")
    .expect("collect scalar result");
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn row_bounds_are_checked_before_expression_evaluation() {
    let batch = fixture_batch().expect("build fixture");
    let expression = bind_expr(
        Expr::Column {
            name: "id".to_owned(),
        },
        &plan_fixture().input_schema,
    )
    .expect("bind expression");
    assert!(matches!(
        evaluate_scalar(&expression, &batch, 4),
        Err(ExecutionError::RowOutOfBounds { index: 4, rows: 4 })
    ));
}
