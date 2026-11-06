//! Independent row-at-a-time execution over decoded Arrow buffers.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::ExecutionError;
use crate::execution::{BatchOperator, OperatorState};
use crate::expr::{BoundExpr, BoundExprKind, ScalarValue};
use crate::logical::BoundNamedExpr;

/// Counters for the scalar scan/filter/project path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScalarMetrics {
    /// Calls made while the pipeline is active, including the terminal call.
    pub pull_calls: usize,
    /// Arrow batches received from the input operator.
    pub input_batches: usize,
    /// Rows available across received input batches.
    pub input_rows: usize,
    /// Rows that passed the predicate and were projected.
    pub output_rows: usize,
}

/// Materialized result used to compare execution paths outside timed regions.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarResult {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<ScalarValue>>,
}

/// Row-at-a-time filter/project execution over batches from a physical input.
pub struct ScalarFilterProject {
    input: Box<dyn BatchOperator>,
    predicate: Option<BoundExpr>,
    projections: Vec<BoundNamedExpr>,
    schema: SchemaRef,
    current: Option<RecordBatch>,
    row_index: usize,
    metrics: ScalarMetrics,
    state: OperatorState,
}

impl ScalarFilterProject {
    pub fn try_new(
        input: Box<dyn BatchOperator>,
        predicate: Option<BoundExpr>,
        projections: Vec<BoundNamedExpr>,
    ) -> Result<Self, ExecutionError> {
        if let Some(predicate) = &predicate
            && predicate.result.data_type != crate::schema::ScalarType::Boolean
        {
            return Err(ExecutionError::ExpressionType {
                expected: "Boolean",
                actual: predicate.result.data_type.to_string(),
            });
        }
        let fields = projections
            .iter()
            .map(|named| {
                Field::new(
                    &named.alias,
                    named.expr.result.data_type.into(),
                    named.expr.result.nullable,
                )
            })
            .collect::<Vec<_>>();
        Ok(Self {
            input,
            predicate,
            projections,
            schema: Arc::new(Schema::new(fields)),
            current: None,
            row_index: 0,
            metrics: ScalarMetrics::default(),
            state: OperatorState::Ready,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn metrics(&self) -> ScalarMetrics {
        self.metrics
    }

    pub fn state(&self) -> OperatorState {
        self.state
    }

    /// Returns the next projected row, or `None` after sticky end-of-stream.
    pub fn next_row(&mut self) -> Result<Option<Vec<ScalarValue>>, ExecutionError> {
        match self.state {
            OperatorState::Exhausted => return Ok(None),
            OperatorState::Failed => return Err(ExecutionError::OperatorFailed),
            OperatorState::Ready => {}
        }
        self.metrics.pull_calls = match self.metrics.pull_calls.checked_add(1) {
            Some(value) => value,
            None => return self.fail(ExecutionError::CounterOverflow),
        };

        loop {
            if let Some(batch) = &self.current
                && self.row_index < batch.num_rows()
            {
                let row = self.row_index;
                self.row_index += 1;
                let passes = match &self.predicate {
                    Some(predicate) => match evaluate_scalar(predicate, batch, row) {
                        Ok(ScalarValue::Boolean { value }) => value == Some(true),
                        Ok(value) => {
                            return self.fail(ExecutionError::ExpressionType {
                                expected: "Boolean",
                                actual: value.data_type().to_string(),
                            });
                        }
                        Err(error) => return self.fail(error),
                    },
                    None => true,
                };
                if !passes {
                    continue;
                }

                let projected = match self
                    .projections
                    .iter()
                    .map(|named| evaluate_scalar(&named.expr, batch, row))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(projected) => projected,
                    Err(error) => return self.fail(error),
                };
                self.metrics.output_rows = match self.metrics.output_rows.checked_add(1) {
                    Some(value) => value,
                    None => return self.fail(ExecutionError::CounterOverflow),
                };
                return Ok(Some(projected));
            }

            match self.input.next_batch() {
                Ok(Some(batch)) => {
                    let input_batches = match self.metrics.input_batches.checked_add(1) {
                        Some(value) => value,
                        None => return self.fail(ExecutionError::CounterOverflow),
                    };
                    let input_rows = match self.metrics.input_rows.checked_add(batch.num_rows()) {
                        Some(value) => value,
                        None => return self.fail(ExecutionError::CounterOverflow),
                    };
                    self.metrics.input_batches = input_batches;
                    self.metrics.input_rows = input_rows;
                    self.current = Some(batch);
                    self.row_index = 0;
                }
                Ok(None) => {
                    self.current = None;
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
                Err(error) => return self.fail(error),
            }
        }
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

/// Collects all rows from the scalar path using its public pull interface.
pub fn collect_scalar_result(
    pipeline: &mut ScalarFilterProject,
) -> Result<ScalarResult, ExecutionError> {
    let schema = pipeline.schema();
    let mut rows = Vec::new();
    while let Some(row) = pipeline.next_row()? {
        rows.push(row);
    }
    Ok(ScalarResult { schema, rows })
}

/// Collects a batch operator into the same typed row representation.
pub fn collect_batch_result(
    operator: &mut dyn BatchOperator,
) -> Result<ScalarResult, ExecutionError> {
    let schema = operator.schema();
    let mut rows = Vec::new();
    while let Some(batch) = operator.next_batch()? {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|column| value_at(&batch, column, row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(ScalarResult { schema, rows })
}

/// Evaluates one bound expression at a single row.
pub fn evaluate_scalar(
    expr: &BoundExpr,
    batch: &RecordBatch,
    row: usize,
) -> Result<ScalarValue, ExecutionError> {
    if row >= batch.num_rows() {
        return Err(ExecutionError::RowOutOfBounds {
            index: row,
            rows: batch.num_rows(),
        });
    }
    match &expr.kind {
        BoundExprKind::Column { index, .. } => value_at(batch, *index, row),
        BoundExprKind::Literal(value) => Ok(value.clone()),
        BoundExprKind::Eq(left, right) => comparison(left, right, batch, row, |ordering| {
            ordering == Ordering::Equal
        }),
        BoundExprKind::NotEq(left, right) => comparison(left, right, batch, row, |ordering| {
            ordering != Ordering::Equal
        }),
        BoundExprKind::Lt(left, right) => comparison(left, right, batch, row, |ordering| {
            ordering == Ordering::Less
        }),
        BoundExprKind::LtEq(left, right) => comparison(left, right, batch, row, |ordering| {
            ordering != Ordering::Greater
        }),
        BoundExprKind::Gt(left, right) => comparison(left, right, batch, row, |ordering| {
            ordering == Ordering::Greater
        }),
        BoundExprKind::GtEq(left, right) => comparison(left, right, batch, row, |ordering| {
            ordering != Ordering::Less
        }),
        BoundExprKind::And(left, right) => boolean_binary(left, right, batch, row, kleene_and),
        BoundExprKind::Or(left, right) => boolean_binary(left, right, batch, row, kleene_or),
        BoundExprKind::Not(inner) => match evaluate_scalar(inner, batch, row)? {
            ScalarValue::Boolean { value } => Ok(ScalarValue::Boolean {
                value: value.map(|value| !value),
            }),
            value => Err(ExecutionError::ExpressionType {
                expected: "Boolean",
                actual: value.data_type().to_string(),
            }),
        },
        BoundExprKind::IsNull(inner) => Ok(ScalarValue::Boolean {
            value: Some(evaluate_scalar(inner, batch, row)?.is_null()),
        }),
        BoundExprKind::IsNotNull(inner) => Ok(ScalarValue::Boolean {
            value: Some(!evaluate_scalar(inner, batch, row)?.is_null()),
        }),
    }
}

fn comparison<F>(
    left: &BoundExpr,
    right: &BoundExpr,
    batch: &RecordBatch,
    row: usize,
    predicate: F,
) -> Result<ScalarValue, ExecutionError>
where
    F: FnOnce(Ordering) -> bool,
{
    let left = evaluate_scalar(left, batch, row)?;
    let right = evaluate_scalar(right, batch, row)?;
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Boolean { value: None });
    }
    let ordering = scalar_ordering(&left, &right)?;
    Ok(ScalarValue::Boolean {
        value: Some(predicate(ordering)),
    })
}

fn scalar_ordering(left: &ScalarValue, right: &ScalarValue) -> Result<Ordering, ExecutionError> {
    match (left, right) {
        (ScalarValue::Int64 { value: Some(left) }, ScalarValue::Int64 { value: Some(right) }) => {
            Ok(left.cmp(right))
        }
        (
            ScalarValue::Float64 { value: Some(left) },
            ScalarValue::Float64 { value: Some(right) },
        ) => left
            .partial_cmp(right)
            .ok_or(ExecutionError::NonFiniteFloatInput),
        (
            ScalarValue::Boolean { value: Some(left) },
            ScalarValue::Boolean { value: Some(right) },
        ) => Ok(left.cmp(right)),
        (ScalarValue::Utf8 { value: Some(left) }, ScalarValue::Utf8 { value: Some(right) }) => {
            Ok(left.cmp(right))
        }
        _ => Err(ExecutionError::ExpressionType {
            expected: "matching non-null scalar types",
            actual: format!("{} and {}", left.data_type(), right.data_type()),
        }),
    }
}

fn boolean_binary<F>(
    left: &BoundExpr,
    right: &BoundExpr,
    batch: &RecordBatch,
    row: usize,
    function: F,
) -> Result<ScalarValue, ExecutionError>
where
    F: FnOnce(Option<bool>, Option<bool>) -> Option<bool>,
{
    let left = evaluate_scalar(left, batch, row)?;
    let right = evaluate_scalar(right, batch, row)?;
    match (left, right) {
        (ScalarValue::Boolean { value: left }, ScalarValue::Boolean { value: right }) => {
            Ok(ScalarValue::Boolean {
                value: function(left, right),
            })
        }
        (left, right) => Err(ExecutionError::ExpressionType {
            expected: "Boolean",
            actual: format!("{} and {}", left.data_type(), right.data_type()),
        }),
    }
}

fn kleene_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn kleene_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn value_at(batch: &RecordBatch, column: usize, row: usize) -> Result<ScalarValue, ExecutionError> {
    if row >= batch.num_rows() {
        return Err(ExecutionError::RowOutOfBounds {
            index: row,
            rows: batch.num_rows(),
        });
    }
    let array = batch
        .columns()
        .get(column)
        .ok_or(ExecutionError::ColumnOutOfBounds {
            index: column,
            columns: batch.num_columns(),
        })?;
    let is_null = array.is_null(row);
    match array.data_type() {
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(ScalarValue::Int64 {
                value: (!is_null).then(|| array.value(row)),
            })
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let value = (!is_null).then(|| array.value(row));
            if value.is_some_and(|value| !value.is_finite()) {
                return Err(ExecutionError::NonFiniteFloatInput);
            }
            Ok(ScalarValue::Float64 { value })
        }
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(ScalarValue::Boolean {
                value: (!is_null).then(|| array.value(row)),
            })
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(ScalarValue::Utf8 {
                value: (!is_null).then(|| array.value(row).to_owned()),
            })
        }
        data_type => Err(ExecutionError::UnsupportedArrowType(data_type.to_string())),
    }
}
