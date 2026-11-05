//! Vectorized evaluation of bound expressions over Arrow batches.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow::record_batch::RecordBatch;

use crate::error::ExecutionError;
use crate::expr::{BoundExpr, BoundExprKind, ScalarValue};

/// Evaluates one bound expression for every row in `batch`.
pub fn evaluate(expr: &BoundExpr, batch: &RecordBatch) -> Result<ArrayRef, ExecutionError> {
    match &expr.kind {
        BoundExprKind::Column { index, .. } => {
            batch
                .columns()
                .get(*index)
                .cloned()
                .ok_or(ExecutionError::ColumnOutOfBounds {
                    index: *index,
                    columns: batch.num_columns(),
                })
        }
        BoundExprKind::Literal(literal) => Ok(literal_array(literal, batch.num_rows())),
        BoundExprKind::Eq(left, right) => compare(left, right, batch, eq),
        BoundExprKind::NotEq(left, right) => compare(left, right, batch, neq),
        BoundExprKind::Lt(left, right) => compare(left, right, batch, lt),
        BoundExprKind::LtEq(left, right) => compare(left, right, batch, lt_eq),
        BoundExprKind::Gt(left, right) => compare(left, right, batch, gt),
        BoundExprKind::GtEq(left, right) => compare(left, right, batch, gt_eq),
        BoundExprKind::And(left, right) => boolean_binary(left, right, batch, and_kleene),
        BoundExprKind::Or(left, right) => boolean_binary(left, right, batch, or_kleene),
        BoundExprKind::Not(inner) => {
            let value = evaluate_boolean(inner, batch)?;
            Ok(Arc::new(not(&value)?) as ArrayRef)
        }
        BoundExprKind::IsNull(inner) => {
            let value = evaluate(inner, batch)?;
            Ok(Arc::new(is_null(value.as_ref())?) as ArrayRef)
        }
        BoundExprKind::IsNotNull(inner) => {
            let value = evaluate(inner, batch)?;
            Ok(Arc::new(is_not_null(value.as_ref())?) as ArrayRef)
        }
    }
}

fn compare<F>(
    left: &BoundExpr,
    right: &BoundExpr,
    batch: &RecordBatch,
    kernel: F,
) -> Result<ArrayRef, ExecutionError>
where
    F: FnOnce(
        &dyn arrow::array::Datum,
        &dyn arrow::array::Datum,
    ) -> Result<BooleanArray, arrow::error::ArrowError>,
{
    let left = evaluate(left, batch)?;
    let right = evaluate(right, batch)?;
    let left_datum: &dyn arrow::array::Datum = &left.as_ref();
    let right_datum: &dyn arrow::array::Datum = &right.as_ref();
    Ok(Arc::new(kernel(left_datum, right_datum)?) as ArrayRef)
}

fn boolean_binary<F>(
    left: &BoundExpr,
    right: &BoundExpr,
    batch: &RecordBatch,
    kernel: F,
) -> Result<ArrayRef, ExecutionError>
where
    F: FnOnce(&BooleanArray, &BooleanArray) -> Result<BooleanArray, arrow::error::ArrowError>,
{
    let left = evaluate_boolean(left, batch)?;
    let right = evaluate_boolean(right, batch)?;
    Ok(Arc::new(kernel(&left, &right)?) as ArrayRef)
}

fn evaluate_boolean(expr: &BoundExpr, batch: &RecordBatch) -> Result<BooleanArray, ExecutionError> {
    let value = evaluate(expr, batch)?;
    value
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| ExecutionError::ExpressionType {
            expected: "Boolean",
            actual: value.data_type().to_string(),
        })
}

fn literal_array(literal: &ScalarValue, rows: usize) -> ArrayRef {
    match literal {
        ScalarValue::Int64 { value } => Arc::new(Int64Array::from(vec![*value; rows])) as ArrayRef,
        ScalarValue::Float64 { value } => {
            Arc::new(Float64Array::from(vec![*value; rows])) as ArrayRef
        }
        ScalarValue::Boolean { value } => {
            Arc::new(BooleanArray::from(vec![*value; rows])) as ArrayRef
        }
        ScalarValue::Utf8 { value } => Arc::new(StringArray::from(
            std::iter::repeat_n(value.as_deref(), rows).collect::<Vec<_>>(),
        )) as ArrayRef,
    }
}
