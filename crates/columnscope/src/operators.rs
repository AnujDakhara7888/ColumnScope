//! Streaming filter and projection physical operators.

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::ExecutionError;
use crate::evaluate::evaluate;
use crate::execution::{BatchOperator, OperatorMetrics, OperatorState};
use crate::expr::BoundExpr;
use crate::logical::BoundNamedExpr;

/// Applies a nullable Boolean predicate to every input batch.
///
/// Predicate nulls are treated as non-matches, matching SQL `WHERE` behavior.
pub struct FilterExec {
    input: Box<dyn BatchOperator>,
    predicate: BoundExpr,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
}

impl FilterExec {
    pub fn try_new(
        input: Box<dyn BatchOperator>,
        predicate: BoundExpr,
    ) -> Result<Self, ExecutionError> {
        if predicate.result.data_type != crate::schema::ScalarType::Boolean {
            return Err(ExecutionError::ExpressionType {
                expected: "Boolean",
                actual: predicate.result.data_type.to_string(),
            });
        }
        let schema = input.schema();
        Ok(Self {
            input,
            predicate,
            schema,
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
        })
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

impl BatchOperator for FilterExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        match self.state {
            OperatorState::Exhausted => return Ok(None),
            OperatorState::Failed => return Err(ExecutionError::OperatorFailed),
            OperatorState::Ready => {}
        }
        if let Err(error) = self.metrics.record_pull() {
            return self.fail(error);
        }

        let input = match self.input.next_batch() {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
            Err(error) => return self.fail(error),
        };
        let predicate = match evaluate(&self.predicate, &input) {
            Ok(value) => value,
            Err(error) => return self.fail(error),
        };
        let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
            return self.fail(ExecutionError::ExpressionType {
                expected: "Boolean",
                actual: predicate.data_type().to_string(),
            });
        };
        let output = match filter_record_batch(&input, predicate) {
            Ok(batch) => batch,
            Err(error) => return self.fail(error.into()),
        };
        if let Err(error) = self.metrics.record_output(output.num_rows()) {
            return self.fail(error);
        }
        Ok(Some(output))
    }

    fn metrics(&self) -> OperatorMetrics {
        self.metrics
    }

    fn state(&self) -> OperatorState {
        self.state
    }
}

/// Evaluates named expressions into a new batch without row materialization.
pub struct ProjectExec {
    input: Box<dyn BatchOperator>,
    expressions: Vec<BoundNamedExpr>,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
}

impl ProjectExec {
    pub fn new(input: Box<dyn BatchOperator>, expressions: Vec<BoundNamedExpr>) -> Self {
        let fields = expressions
            .iter()
            .map(|named| {
                Field::new(
                    &named.alias,
                    named.expr.result.data_type.into(),
                    named.expr.result.nullable,
                )
            })
            .collect::<Vec<_>>();
        Self {
            input,
            expressions,
            schema: Arc::new(Schema::new(fields)),
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
        }
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

impl BatchOperator for ProjectExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        match self.state {
            OperatorState::Exhausted => return Ok(None),
            OperatorState::Failed => return Err(ExecutionError::OperatorFailed),
            OperatorState::Ready => {}
        }
        if let Err(error) = self.metrics.record_pull() {
            return self.fail(error);
        }

        let input = match self.input.next_batch() {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
            Err(error) => return self.fail(error),
        };
        let columns = match self
            .expressions
            .iter()
            .map(|named| evaluate(&named.expr, &input))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(columns) => columns,
            Err(error) => return self.fail(error),
        };
        let options = RecordBatchOptions::new().with_row_count(Some(input.num_rows()));
        let output = match RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)
        {
            Ok(batch) => batch,
            Err(error) => return self.fail(error.into()),
        };
        if let Err(error) = self.metrics.record_output(output.num_rows()) {
            return self.fail(error);
        }
        Ok(Some(output))
    }

    fn metrics(&self) -> OperatorMetrics {
        self.metrics
    }

    fn state(&self) -> OperatorState {
        self.state
    }
}
