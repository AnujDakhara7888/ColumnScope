use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use columnscope::aggregate::HashAggregateExec;
use columnscope::bind::bind_query;
use columnscope::error::ExecutionError;
use columnscope::execution::{BatchOperator, OperatorMetrics, OperatorState};
use columnscope::expr::{Expr, NamedExpr};
use columnscope::logical::{AggregateExpr, BoundLogicalPlan, LogicalPlan, QueryPlan};
use columnscope::scan::ParquetScan;
use columnscope::schema::{FieldDef, ScalarType, SchemaDef};
use columnscope::{fixture_batch, write_fixture};
use serde::Deserialize;
use tempfile::tempdir;

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct AggregateRow {
    region: String,
    rows: i64,
    metric_count: i64,
    metric_sum: Option<f64>,
    metric_min: Option<f64>,
    metric_max: Option<f64>,
    metric_avg: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AggregateOracle {
    rows: Vec<AggregateRow>,
}

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

fn aggregate_fixture_plan() -> QueryPlan {
    QueryPlan::from_json(include_str!("../../../fixtures/plans/aggregate_v1.json"))
        .expect("parse aggregate fixture")
}

fn aggregate_exec(
    input: Box<dyn BatchOperator>,
    plan: QueryPlan,
) -> Result<HashAggregateExec, ExecutionError> {
    let bound = bind_query(plan).expect("bind aggregate plan");
    let BoundLogicalPlan::Aggregate {
        group_by,
        aggregates,
        ..
    } = bound.plan
    else {
        panic!("plan root should be Aggregate")
    };
    HashAggregateExec::try_new(input, group_by, aggregates)
}

fn grouped_rows(batch: &RecordBatch) -> Vec<AggregateRow> {
    let region = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rows = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let count = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let min = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let max = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let avg = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let optional = |array: &Float64Array, row| (!array.is_null(row)).then(|| array.value(row));
    let mut result = (0..batch.num_rows())
        .map(|row| AggregateRow {
            region: region.value(row).to_owned(),
            rows: rows.value(row),
            metric_count: count.value(row),
            metric_sum: optional(sum, row),
            metric_min: optional(min, row),
            metric_max: optional(max, row),
            metric_avg: optional(avg, row),
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.region.cmp(&right.region));
    result
}

#[test]
fn grouped_aggregates_match_the_checked_duckdb_oracle() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let scan = ParquetScan::try_new(path, 2).expect("open scan");
    let mut aggregate =
        aggregate_exec(Box::new(scan), aggregate_fixture_plan()).expect("build aggregate");
    let output = aggregate.next_batch().expect("execute aggregate").unwrap();
    let mut expected: AggregateOracle =
        serde_json::from_str(include_str!("../../../fixtures/oracles/aggregate_v1.json"))
            .expect("parse aggregate oracle");
    expected
        .rows
        .sort_by(|left, right| left.region.cmp(&right.region));

    assert_eq!(grouped_rows(&output), expected.rows);
    assert_eq!(aggregate.metrics().output_batches, 1);
    assert_eq!(aggregate.metrics().output_rows, 3);
    assert!(aggregate.next_batch().expect("finish aggregate").is_none());
    assert_eq!(aggregate.state(), OperatorState::Exhausted);
    assert!(
        aggregate
            .next_batch()
            .expect("repeat end-of-stream")
            .is_none()
    );
}

#[test]
fn global_integer_and_float_aggregates_have_expected_types_and_values() {
    let batch = fixture_batch().expect("build fixture");
    let plan = QueryPlan {
        format_version: 1,
        input_schema: fixture_schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![
                AggregateExpr::CountStar {
                    alias: "rows".into(),
                },
                AggregateExpr::Count {
                    alias: "metric_count".into(),
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
                AggregateExpr::Sum {
                    alias: "metric_sum".into(),
                    expr: column("metric"),
                },
                AggregateExpr::Avg {
                    alias: "metric_avg".into(),
                    expr: column("metric"),
                },
            ],
        },
    };
    let schema = batch.schema();
    let input = BatchInput::new(vec![batch], schema);
    let mut aggregate = aggregate_exec(Box::new(input), plan).expect("build aggregate");
    let output = aggregate.next_batch().expect("execute aggregate").unwrap();

    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6
    );
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        output
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        output
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.5
    );
    assert_eq!(
        output
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        3.75
    );
    assert_eq!(
        output
            .column(7)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.25
    );
}

#[test]
fn empty_input_returns_global_identity_and_no_group_rows() {
    let arrow_schema = fixture_batch().expect("build fixture").schema();
    let global_plan = QueryPlan {
        format_version: 1,
        input_schema: fixture_schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![
                AggregateExpr::CountStar {
                    alias: "rows".into(),
                },
                AggregateExpr::Sum {
                    alias: "sum".into(),
                    expr: column("id"),
                },
                AggregateExpr::Min {
                    alias: "min".into(),
                    expr: column("id"),
                },
                AggregateExpr::Max {
                    alias: "max".into(),
                    expr: column("id"),
                },
                AggregateExpr::Avg {
                    alias: "avg".into(),
                    expr: column("id"),
                },
            ],
        },
    };
    let input = BatchInput::new(Vec::new(), arrow_schema.clone());
    let mut global = aggregate_exec(Box::new(input), global_plan).expect("build global aggregate");
    let output = global
        .next_batch()
        .expect("execute global aggregate")
        .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    for column in 1..5 {
        assert!(output.column(column).is_null(0));
    }

    let input = BatchInput::new(Vec::new(), arrow_schema);
    let mut grouped =
        aggregate_exec(Box::new(input), aggregate_fixture_plan()).expect("build grouped aggregate");
    let output = grouped
        .next_batch()
        .expect("execute grouped aggregate")
        .unwrap();
    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.num_columns(), 7);
}

#[test]
fn nullable_keys_form_a_group_and_null_values_are_skipped() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("metric", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![None, None, Some("east")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(2), None, Some(4)])) as ArrayRef,
        ],
    )
    .expect("build nullable group batch");
    let plan = QueryPlan {
        format_version: 1,
        input_schema: SchemaDef {
            fields: vec![
                FieldDef {
                    name: "region".into(),
                    data_type: ScalarType::Utf8,
                    nullable: true,
                },
                FieldDef {
                    name: "metric".into(),
                    data_type: ScalarType::Int64,
                    nullable: true,
                },
            ],
        },
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![NamedExpr {
                alias: "region".into(),
                expr: column("region"),
            }],
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
            ],
        },
    };
    let input = BatchInput::new(vec![batch], schema);
    let mut aggregate = aggregate_exec(Box::new(input), plan).expect("build aggregate");
    let output = aggregate.next_batch().expect("execute aggregate").unwrap();
    let keys = output
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rows = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = output
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = output
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let null_row = (0..output.num_rows())
        .find(|row| keys.is_null(*row))
        .unwrap();
    assert_eq!(rows.value(null_row), 2);
    assert_eq!(counts.value(null_row), 1);
    assert_eq!(sums.value(null_row), 2);
}

#[test]
fn integer_sum_overflow_is_terminal_across_batch_boundaries() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batches = [i64::MAX, 1]
        .into_iter()
        .map(|value| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let plan = QueryPlan {
        format_version: 1,
        input_schema: SchemaDef {
            fields: vec![FieldDef {
                name: "id".into(),
                data_type: ScalarType::Int64,
                nullable: false,
            }],
        },
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![AggregateExpr::Sum {
                alias: "sum".into(),
                expr: column("id"),
            }],
        },
    };
    let input = BatchInput::new(batches, schema);
    let mut aggregate = aggregate_exec(Box::new(input), plan).expect("build aggregate");
    assert!(matches!(
        aggregate.next_batch(),
        Err(ExecutionError::IntegerOverflow("SUM"))
    ));
    assert_eq!(aggregate.state(), OperatorState::Failed);
    assert!(matches!(
        aggregate.next_batch(),
        Err(ExecutionError::OperatorFailed)
    ));
    assert_eq!(aggregate.metrics().output_batches, 0);
}

#[test]
fn nonfinite_float_input_is_rejected_before_output() {
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
    let plan = QueryPlan {
        format_version: 1,
        input_schema: SchemaDef {
            fields: vec![FieldDef {
                name: "metric".into(),
                data_type: ScalarType::Float64,
                nullable: false,
            }],
        },
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![AggregateExpr::Sum {
                alias: "sum".into(),
                expr: column("metric"),
            }],
        },
    };
    let input = BatchInput::new(vec![batch], schema);
    let mut aggregate = aggregate_exec(Box::new(input), plan).expect("build aggregate");
    assert!(matches!(
        aggregate.next_batch(),
        Err(ExecutionError::NonFiniteAggregate("input"))
    ));
    assert_eq!(aggregate.state(), OperatorState::Failed);
    assert_eq!(aggregate.metrics().output_batches, 0);
}

#[test]
fn composite_keys_are_explicitly_deferred() {
    let batch = fixture_batch().expect("build fixture");
    let schema = batch.schema();
    let plan = QueryPlan {
        format_version: 1,
        input_schema: fixture_schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![
                NamedExpr {
                    alias: "id".into(),
                    expr: column("id"),
                },
                NamedExpr {
                    alias: "region".into(),
                    expr: column("region"),
                },
            ],
            aggregates: vec![AggregateExpr::CountStar {
                alias: "rows".into(),
            }],
        },
    };
    let bound = bind_query(plan).expect("bind composite aggregate");
    let BoundLogicalPlan::Aggregate {
        group_by,
        aggregates,
        ..
    } = bound.plan
    else {
        panic!("plan root should be Aggregate")
    };
    let input = BatchInput::new(vec![batch], schema);
    assert!(matches!(
        HashAggregateExec::try_new(Box::new(input), group_by, aggregates),
        Err(ExecutionError::UnsupportedGroupKeyCount(2))
    ));
}

#[test]
fn grouped_results_are_batch_boundary_independent_and_send() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");

    let run = |batch_size| {
        let scan = ParquetScan::try_new(&path, batch_size).expect("open scan");
        let aggregate =
            aggregate_exec(Box::new(scan), aggregate_fixture_plan()).expect("build aggregate");
        std::thread::spawn(move || {
            let mut aggregate = aggregate;
            let output = aggregate.next_batch().expect("execute aggregate").unwrap();
            grouped_rows(&output)
        })
        .join()
        .expect("worker should not panic")
    };
    assert_eq!(run(1), run(8_192));
}

fn column(name: &str) -> Expr {
    Expr::Column {
        name: name.to_owned(),
    }
}

fn fixture_schema() -> SchemaDef {
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
                nullable: false,
            },
        ],
    }
}
