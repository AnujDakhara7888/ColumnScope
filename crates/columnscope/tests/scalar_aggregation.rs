use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use columnscope::aggregate::{AggregateOptions, HashAggregateExec};
use columnscope::bind::bind_query;
use columnscope::error::ExecutionError;
use columnscope::execution::{BatchOperator, OperatorMetrics, OperatorState};
use columnscope::expr::{BoundExpr, Expr};
use columnscope::logical::{
    AggregateExpr, BoundAggregateExpr, BoundLogicalPlan, BoundNamedExpr, LogicalPlan, QueryPlan,
};
use columnscope::operators::FilterExec;
use columnscope::scalar::{
    ScalarAggregate, ScalarResult, collect_batch_result, collect_scalar_aggregate_result,
};
use columnscope::schema::{FieldDef, ScalarType, SchemaDef};

struct BatchInput {
    batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
}

impl BatchInput {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
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

#[test]
fn scalar_and_vector_aggregates_match_for_filtered_composite_groups() {
    let batch = shared_batch();
    let schema = batch.schema();
    let batches = vec![batch.slice(0, 2), batch.slice(2, 4)];
    let (predicate, group_by, aggregates) = bound_parts(filtered_grouped_plan());

    let vector_input = BatchInput::new(batches.clone(), schema.clone());
    let filter = FilterExec::try_new(
        Box::new(vector_input),
        predicate.clone().expect("filtered plan has a predicate"),
    )
    .expect("build vector filter");
    let mut vector =
        HashAggregateExec::try_new(Box::new(filter), group_by.clone(), aggregates.clone())
            .expect("build vector aggregate");
    let vector_result = collect_batch_result(&mut vector).expect("collect vector aggregate");

    let scalar_input = BatchInput::new(batches, schema);
    let mut scalar = ScalarAggregate::try_new(
        Box::new(scalar_input),
        predicate,
        group_by,
        aggregates,
        AggregateOptions::default(),
    )
    .expect("build scalar aggregate");
    let scalar_result =
        collect_scalar_aggregate_result(&mut scalar).expect("collect scalar aggregate");

    assert_same_unordered_rows(&scalar_result, &vector_result);
    assert_eq!(scalar.metrics().input_batches, 2);
    assert_eq!(scalar.metrics().input_rows, 6);
    assert_eq!(scalar.metrics().output_rows, 5);
    assert!(scalar.peak_state_bytes() > 0);
    assert_eq!(scalar.state(), OperatorState::Exhausted);
    assert!(scalar.next_row().expect("repeat end-of-stream").is_none());
}

#[test]
fn empty_global_and_grouped_results_match_vector_execution() {
    let schema = shared_arrow_schema();
    for plan in [global_plan(), filtered_grouped_plan()] {
        let (predicate, group_by, aggregates) = bound_parts(plan);
        let mut vector_input: Box<dyn BatchOperator> =
            Box::new(BatchInput::new(Vec::new(), schema.clone()));
        if let Some(predicate) = predicate.clone() {
            vector_input = Box::new(
                FilterExec::try_new(vector_input, predicate).expect("build empty vector filter"),
            );
        }
        let mut vector =
            HashAggregateExec::try_new(vector_input, group_by.clone(), aggregates.clone())
                .expect("build empty vector aggregate");
        let vector_result = collect_batch_result(&mut vector).expect("collect vector result");

        let scalar_input = BatchInput::new(Vec::new(), schema.clone());
        let mut scalar = ScalarAggregate::try_new(
            Box::new(scalar_input),
            predicate,
            group_by,
            aggregates,
            AggregateOptions::default(),
        )
        .expect("build empty scalar aggregate");
        let scalar_result =
            collect_scalar_aggregate_result(&mut scalar).expect("collect scalar result");

        assert_same_unordered_rows(&scalar_result, &vector_result);
    }
}

#[test]
fn scalar_aggregation_is_batch_boundary_independent_and_send() {
    let batch = shared_batch();
    let schema = batch.schema();
    let run = |batch_rows: usize| {
        let batches = (0..batch.num_rows())
            .step_by(batch_rows)
            .map(|offset| batch.slice(offset, batch_rows.min(batch.num_rows() - offset)))
            .collect::<Vec<_>>();
        let (predicate, group_by, aggregates) = bound_parts(filtered_grouped_plan());
        let input = BatchInput::new(batches, schema.clone());
        let aggregate = ScalarAggregate::try_new(
            Box::new(input),
            predicate,
            group_by,
            aggregates,
            AggregateOptions::default(),
        )
        .expect("build scalar aggregate");
        std::thread::spawn(move || {
            let mut aggregate = aggregate;
            collect_scalar_aggregate_result(&mut aggregate).expect("collect on worker")
        })
        .join()
        .expect("worker should not panic")
    };

    let one_row = run(1);
    let one_batch = run(6);
    assert_same_unordered_rows(&one_row, &one_batch);
}

#[test]
fn scalar_overflow_and_state_budget_fail_before_output_and_stay_failed() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batches = [i64::MAX, 1]
        .into_iter()
        .map(|value| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
            )
            .expect("build overflow batch")
        })
        .collect::<Vec<_>>();
    let (_, group_by, aggregates) = bound_parts(integer_sum_plan());
    let mut overflow = ScalarAggregate::try_new(
        Box::new(BatchInput::new(batches, schema)),
        None,
        group_by,
        aggregates,
        AggregateOptions::default(),
    )
    .expect("build overflowing aggregate");
    assert!(matches!(
        overflow.next_row(),
        Err(ExecutionError::IntegerOverflow("SUM"))
    ));
    assert_eq!(overflow.metrics().output_rows, 0);
    assert_eq!(overflow.state(), OperatorState::Failed);
    assert!(matches!(
        overflow.next_row(),
        Err(ExecutionError::OperatorFailed)
    ));

    let batch = shared_batch();
    let schema = batch.schema();
    let (predicate, group_by, aggregates) = bound_parts(filtered_grouped_plan());
    let mut bounded = ScalarAggregate::try_new(
        Box::new(BatchInput::new(vec![batch], schema)),
        predicate,
        group_by,
        aggregates,
        AggregateOptions {
            state_budget_bytes: 1,
            output_batch_rows: 8_192,
        },
    )
    .expect("build bounded aggregate");
    assert!(matches!(
        bounded.next_row(),
        Err(ExecutionError::AggregateStateBudgetExceeded {
            limit: 1,
            attempted
        }) if attempted > 1
    ));
    assert_eq!(bounded.metrics().output_rows, 0);
    assert_eq!(bounded.state(), OperatorState::Failed);
    assert!(matches!(
        bounded.next_row(),
        Err(ExecutionError::OperatorFailed)
    ));
}

#[test]
fn scalar_nonfinite_input_and_invalid_output_size_match_vector_errors() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "metric",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![1.0, f64::INFINITY])) as ArrayRef],
    )
    .expect("build nonfinite batch");
    let (_, group_by, aggregates) = bound_parts(float_sum_plan());
    let mut scalar = ScalarAggregate::try_new(
        Box::new(BatchInput::new(vec![batch], schema.clone())),
        None,
        group_by.clone(),
        aggregates.clone(),
        AggregateOptions::default(),
    )
    .expect("build nonfinite aggregate");
    assert!(matches!(
        scalar.next_row(),
        Err(ExecutionError::NonFiniteAggregate("input"))
    ));
    assert_eq!(scalar.metrics().output_rows, 0);
    assert_eq!(scalar.state(), OperatorState::Failed);

    assert!(matches!(
        ScalarAggregate::try_new(
            Box::new(BatchInput::new(Vec::new(), schema)),
            None,
            group_by,
            aggregates,
            AggregateOptions {
                state_budget_bytes: 1_024,
                output_batch_rows: 0,
            },
        ),
        Err(ExecutionError::InvalidAggregateOutputBatchSize)
    ));
}

fn bound_parts(
    plan: QueryPlan,
) -> (
    Option<BoundExpr>,
    Vec<BoundNamedExpr>,
    Vec<BoundAggregateExpr>,
) {
    let bound = bind_query(plan).expect("bind aggregate plan");
    let BoundLogicalPlan::Aggregate {
        input,
        group_by,
        aggregates,
        ..
    } = bound.plan
    else {
        panic!("plan root should be Aggregate")
    };
    let predicate = match *input {
        BoundLogicalPlan::Filter { predicate, .. } => Some(predicate),
        BoundLogicalPlan::Scan { .. } => None,
        _ => panic!("aggregate input should be Scan or Filter"),
    };
    (predicate, group_by, aggregates)
}

fn assert_same_unordered_rows(left: &ScalarResult, right: &ScalarResult) {
    assert_eq!(left.schema, right.schema);
    let normalize = |result: &ScalarResult| {
        let mut rows = result
            .rows
            .iter()
            .map(|row| format!("{row:?}"))
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    assert_eq!(normalize(left), normalize(right));
}

fn shared_batch() -> RecordBatch {
    RecordBatch::try_new(
        shared_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(2.0),
                Some(4.0),
                None,
                Some(8.0),
            ])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("north"),
                Some("north"),
                Some("north"),
                Some("south"),
                None,
                None,
            ])) as ArrayRef,
        ],
    )
    .expect("build shared batch")
}

fn shared_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

fn shared_schema() -> SchemaDef {
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
                nullable: true,
            },
            FieldDef {
                name: "active".into(),
                data_type: ScalarType::Boolean,
                nullable: true,
            },
            FieldDef {
                name: "region".into(),
                data_type: ScalarType::Utf8,
                nullable: true,
            },
        ],
    }
}

fn filtered_grouped_plan() -> QueryPlan {
    QueryPlan {
        format_version: 1,
        input_schema: shared_schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan),
                predicate: Expr::GtEq {
                    left: Box::new(column("id")),
                    right: Box::new(Expr::Literal {
                        literal: columnscope::expr::ScalarValue::Int64 { value: Some(1) },
                    }),
                },
            }),
            group_by: vec![
                named("region", column("region")),
                named("active", column("active")),
            ],
            aggregates: vec![
                AggregateExpr::CountStar {
                    alias: "rows".into(),
                },
                AggregateExpr::Count {
                    alias: "metric_count".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Sum {
                    alias: "metric_sum".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Min {
                    alias: "metric_min".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Max {
                    alias: "metric_max".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Avg {
                    alias: "metric_avg".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Sum {
                    alias: "id_sum".into(),
                    expr: column("id"),
                },
                AggregateExpr::Min {
                    alias: "id_min".into(),
                    expr: column("id"),
                },
                AggregateExpr::Max {
                    alias: "id_max".into(),
                    expr: column("id"),
                },
                AggregateExpr::Avg {
                    alias: "id_avg".into(),
                    expr: column("id"),
                },
            ],
        },
    }
}

fn global_plan() -> QueryPlan {
    QueryPlan {
        format_version: 1,
        input_schema: shared_schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![
                AggregateExpr::CountStar {
                    alias: "rows".into(),
                },
                AggregateExpr::Count {
                    alias: "count".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Sum {
                    alias: "sum".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Min {
                    alias: "min".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Max {
                    alias: "max".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Avg {
                    alias: "avg".into(),
                    expr: column("metric"),
                },
            ],
        },
    }
}

fn integer_sum_plan() -> QueryPlan {
    single_column_sum_plan("id", ScalarType::Int64)
}

fn float_sum_plan() -> QueryPlan {
    single_column_sum_plan("metric", ScalarType::Float64)
}

fn single_column_sum_plan(name: &str, data_type: ScalarType) -> QueryPlan {
    QueryPlan {
        format_version: 1,
        input_schema: SchemaDef {
            fields: vec![FieldDef {
                name: name.into(),
                data_type,
                nullable: false,
            }],
        },
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![AggregateExpr::Sum {
                alias: "sum".into(),
                expr: column(name),
            }],
        },
    }
}

fn named(alias: &str, expr: Expr) -> columnscope::expr::NamedExpr {
    columnscope::expr::NamedExpr {
        alias: alias.into(),
        expr,
    }
}

fn column(name: &str) -> Expr {
    Expr::Column {
        name: name.to_owned(),
    }
}
