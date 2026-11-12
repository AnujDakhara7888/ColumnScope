//! Independent row-at-a-time execution over decoded Arrow buffers.

use std::cmp::Ordering;
use std::collections::{HashMap, hash_map};
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::aggregate::AggregateOptions;
use crate::error::ExecutionError;
use crate::execution::{BatchOperator, OperatorState};
use crate::expr::{BoundExpr, BoundExprKind, ScalarValue};
use crate::logical::{AggregateFunction, BoundAggregateExpr, BoundNamedExpr};
use crate::schema::ScalarType;

const HASH_ENTRY_OVERHEAD_BYTES: usize = 32;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ScalarGroupPart {
    Int64(Option<i64>),
    Boolean(Option<bool>),
    Utf8(Option<String>),
}

impl ScalarGroupPart {
    fn from_value(value: ScalarValue) -> Result<Self, ExecutionError> {
        match value {
            ScalarValue::Int64 { value } => Ok(Self::Int64(value)),
            ScalarValue::Boolean { value } => Ok(Self::Boolean(value)),
            ScalarValue::Utf8 { value } => Ok(Self::Utf8(value)),
            ScalarValue::Float64 { .. } => Err(ExecutionError::UnsupportedAggregateType(
                "Float64 group key".to_owned(),
            )),
        }
    }

    fn into_value(self) -> ScalarValue {
        match self {
            Self::Int64(value) => ScalarValue::Int64 { value },
            Self::Boolean(value) => ScalarValue::Boolean { value },
            Self::Utf8(value) => ScalarValue::Utf8 { value },
        }
    }

    fn owned_bytes(&self) -> usize {
        match self {
            Self::Utf8(Some(value)) => value.len(),
            Self::Int64(_) | Self::Boolean(_) | Self::Utf8(None) => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ScalarGroupKey(Vec<ScalarGroupPart>);

impl ScalarGroupKey {
    fn retained_bytes(&self, aggregate_count: usize) -> Result<usize, ExecutionError> {
        let key_parts = self
            .0
            .capacity()
            .checked_mul(size_of::<ScalarGroupPart>())
            .ok_or(ExecutionError::AggregateStateSizeOverflow)?;
        let strings = self.0.iter().try_fold(0usize, |total, part| {
            total
                .checked_add(part.owned_bytes())
                .ok_or(ExecutionError::AggregateStateSizeOverflow)
        })?;
        let aggregate_states = aggregate_count
            .checked_mul(size_of::<ScalarAggregateState>())
            .ok_or(ExecutionError::AggregateStateSizeOverflow)?;
        [
            HASH_ENTRY_OVERHEAD_BYTES,
            size_of::<ScalarGroupKey>(),
            key_parts,
            strings,
            size_of::<Vec<ScalarAggregateState>>(),
            aggregate_states,
        ]
        .into_iter()
        .try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or(ExecutionError::AggregateStateSizeOverflow)
        })
    }
}

#[derive(Debug, Clone)]
enum ScalarAggregateState {
    Count(i64),
    IntSum(Option<i64>),
    FloatSum(Option<f64>),
    Min {
        data_type: ScalarType,
        value: Option<ScalarValue>,
    },
    Max {
        data_type: ScalarType,
        value: Option<ScalarValue>,
    },
    Avg {
        sum: f64,
        count: i64,
    },
}

impl ScalarAggregateState {
    fn new(aggregate: &BoundAggregateExpr) -> Self {
        match aggregate.function {
            AggregateFunction::CountStar | AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum => match aggregate.data_type {
                ScalarType::Int64 => Self::IntSum(None),
                ScalarType::Float64 => Self::FloatSum(None),
                _ => unreachable!("binder restricts SUM to numeric types"),
            },
            AggregateFunction::Min => Self::Min {
                data_type: aggregate.data_type,
                value: None,
            },
            AggregateFunction::Max => Self::Max {
                data_type: aggregate.data_type,
                value: None,
            },
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn update(
        &mut self,
        aggregate: &BoundAggregateExpr,
        value: Option<ScalarValue>,
    ) -> Result<(), ExecutionError> {
        match self {
            Self::Count(count) => {
                if aggregate.function == AggregateFunction::CountStar
                    || value.as_ref().is_some_and(|value| !value.is_null())
                {
                    *count = count
                        .checked_add(1)
                        .ok_or(ExecutionError::IntegerOverflow("COUNT"))?;
                }
            }
            Self::IntSum(sum) => {
                if let Some(ScalarValue::Int64 { value: Some(value) }) = value {
                    *sum = Some(match *sum {
                        Some(current) => current
                            .checked_add(value)
                            .ok_or(ExecutionError::IntegerOverflow("SUM"))?,
                        None => value,
                    });
                }
            }
            Self::FloatSum(sum) => {
                if let Some(ScalarValue::Float64 { value: Some(value) }) = value {
                    ensure_finite_aggregate(value, "SUM")?;
                    let next = sum.unwrap_or(0.0) + value;
                    ensure_finite_aggregate(next, "SUM")?;
                    *sum = Some(next);
                }
            }
            Self::Min { value: current, .. } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    let replace = match current.as_ref() {
                        Some(current) => scalar_ordering(&value, current)? == Ordering::Less,
                        None => true,
                    };
                    if replace {
                        *current = Some(value);
                    }
                }
            }
            Self::Max { value: current, .. } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    let replace = match current.as_ref() {
                        Some(current) => scalar_ordering(&value, current)? == Ordering::Greater,
                        None => true,
                    };
                    if replace {
                        *current = Some(value);
                    }
                }
            }
            Self::Avg { sum, count } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    let numeric = match value {
                        ScalarValue::Int64 { value: Some(value) } => value as f64,
                        ScalarValue::Float64 { value: Some(value) } => value,
                        value => {
                            return Err(ExecutionError::UnsupportedAggregateType(
                                value.data_type().to_string(),
                            ));
                        }
                    };
                    ensure_finite_aggregate(numeric, "AVG")?;
                    let next = *sum + numeric;
                    ensure_finite_aggregate(next, "AVG")?;
                    *sum = next;
                    *count = count
                        .checked_add(1)
                        .ok_or(ExecutionError::IntegerOverflow("AVG count"))?;
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> ScalarValue {
        match self {
            Self::Count(value) => ScalarValue::Int64 { value: Some(value) },
            Self::IntSum(value) => ScalarValue::Int64 { value },
            Self::FloatSum(value) => ScalarValue::Float64 { value },
            Self::Min { data_type, value } | Self::Max { data_type, value } => {
                value.unwrap_or_else(|| null_scalar(data_type))
            }
            Self::Avg { sum, count } => ScalarValue::Float64 {
                value: (count > 0).then(|| sum / count as f64),
            },
        }
    }
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

/// Row-at-a-time global or composite-key aggregation over decoded Arrow input.
pub struct ScalarAggregate {
    input: Box<dyn BatchOperator>,
    predicate: Option<BoundExpr>,
    group_by: Vec<BoundNamedExpr>,
    aggregates: Vec<BoundAggregateExpr>,
    schema: SchemaRef,
    state_budget_bytes: usize,
    remaining_groups: Option<hash_map::IntoIter<ScalarGroupKey, Vec<ScalarAggregateState>>>,
    prepared: bool,
    metrics: ScalarMetrics,
    state: OperatorState,
    peak_state_bytes: usize,
}

impl ScalarAggregate {
    pub fn try_new(
        input: Box<dyn BatchOperator>,
        predicate: Option<BoundExpr>,
        group_by: Vec<BoundNamedExpr>,
        aggregates: Vec<BoundAggregateExpr>,
        options: AggregateOptions,
    ) -> Result<Self, ExecutionError> {
        if let Some(predicate) = &predicate
            && predicate.result.data_type != ScalarType::Boolean
        {
            return Err(ExecutionError::ExpressionType {
                expected: "Boolean",
                actual: predicate.result.data_type.to_string(),
            });
        }
        if group_by.len() > 2 {
            return Err(ExecutionError::UnsupportedGroupKeyCount(group_by.len()));
        }
        if options.output_batch_rows == 0 {
            return Err(ExecutionError::InvalidAggregateOutputBatchSize);
        }
        let fields = group_by
            .iter()
            .map(|named| {
                Field::new(
                    &named.alias,
                    named.expr.result.data_type.into(),
                    named.expr.result.nullable,
                )
            })
            .chain(aggregates.iter().map(|aggregate| {
                Field::new(
                    &aggregate.alias,
                    aggregate.data_type.into(),
                    aggregate.nullable,
                )
            }))
            .collect::<Vec<_>>();
        Ok(Self {
            input,
            predicate,
            group_by,
            aggregates,
            schema: Arc::new(Schema::new(fields)),
            state_budget_bytes: options.state_budget_bytes,
            remaining_groups: None,
            prepared: false,
            metrics: ScalarMetrics::default(),
            state: OperatorState::Ready,
            peak_state_bytes: 0,
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

    pub fn peak_state_bytes(&self) -> usize {
        self.peak_state_bytes
    }

    /// Returns one completed aggregate row, or sticky end-of-stream.
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
        if !self.prepared
            && let Err(error) = self.prepare()
        {
            return self.fail(error);
        }

        let Some((key, states)) = self
            .remaining_groups
            .as_mut()
            .expect("prepared scalar aggregation owns its groups")
            .next()
        else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };
        let mut row = key
            .0
            .into_iter()
            .map(ScalarGroupPart::into_value)
            .collect::<Vec<_>>();
        row.extend(states.into_iter().map(ScalarAggregateState::finish));
        self.metrics.output_rows = match self.metrics.output_rows.checked_add(1) {
            Some(value) => value,
            None => return self.fail(ExecutionError::CounterOverflow),
        };
        Ok(Some(row))
    }

    fn prepare(&mut self) -> Result<(), ExecutionError> {
        let mut groups = HashMap::<ScalarGroupKey, Vec<ScalarAggregateState>>::new();
        let mut state_bytes = 0usize;
        if self.group_by.is_empty() {
            insert_scalar_group(
                &mut groups,
                ScalarGroupKey(Vec::new()),
                &self.aggregates,
                self.state_budget_bytes,
                &mut state_bytes,
            )?;
        }

        while let Some(batch) = self.input.next_batch()? {
            self.metrics.input_batches = self
                .metrics
                .input_batches
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow)?;
            self.metrics.input_rows = self
                .metrics
                .input_rows
                .checked_add(batch.num_rows())
                .ok_or(ExecutionError::CounterOverflow)?;
            for row in 0..batch.num_rows() {
                if !self.row_passes(&batch, row)? {
                    continue;
                }
                let key = ScalarGroupKey(
                    self.group_by
                        .iter()
                        .map(|group| {
                            evaluate_scalar(&group.expr, &batch, row)
                                .and_then(ScalarGroupPart::from_value)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let states = insert_scalar_group(
                    &mut groups,
                    key,
                    &self.aggregates,
                    self.state_budget_bytes,
                    &mut state_bytes,
                )?;
                for (state, aggregate) in states.iter_mut().zip(&self.aggregates) {
                    let value = aggregate
                        .expr
                        .as_ref()
                        .map(|expression| {
                            evaluate_scalar(expression, &batch, row).map_err(|error| match error {
                                ExecutionError::NonFiniteFloatInput => {
                                    ExecutionError::NonFiniteAggregate("input")
                                }
                                error => error,
                            })
                        })
                        .transpose()?;
                    state.update(aggregate, value)?;
                }
            }
        }
        self.peak_state_bytes = state_bytes;
        self.remaining_groups = Some(groups.into_iter());
        self.prepared = true;
        Ok(())
    }

    fn row_passes(&self, batch: &RecordBatch, row: usize) -> Result<bool, ExecutionError> {
        match &self.predicate {
            None => Ok(true),
            Some(predicate) => match evaluate_scalar(predicate, batch, row)? {
                ScalarValue::Boolean { value } => Ok(value == Some(true)),
                value => Err(ExecutionError::ExpressionType {
                    expected: "Boolean",
                    actual: value.data_type().to_string(),
                }),
            },
        }
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

/// Collects scalar aggregate rows using its public row pull interface.
pub fn collect_scalar_aggregate_result(
    aggregate: &mut ScalarAggregate,
) -> Result<ScalarResult, ExecutionError> {
    let schema = aggregate.schema();
    let mut rows = Vec::new();
    while let Some(row) = aggregate.next_row()? {
        rows.push(row);
    }
    Ok(ScalarResult { schema, rows })
}

fn insert_scalar_group<'a>(
    groups: &'a mut HashMap<ScalarGroupKey, Vec<ScalarAggregateState>>,
    key: ScalarGroupKey,
    aggregates: &[BoundAggregateExpr],
    budget: usize,
    state_bytes: &mut usize,
) -> Result<&'a mut Vec<ScalarAggregateState>, ExecutionError> {
    match groups.entry(key) {
        hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        hash_map::Entry::Vacant(entry) => {
            let charge = entry.key().retained_bytes(aggregates.len())?;
            let attempted = state_bytes
                .checked_add(charge)
                .ok_or(ExecutionError::AggregateStateSizeOverflow)?;
            if attempted > budget {
                return Err(ExecutionError::AggregateStateBudgetExceeded {
                    limit: budget,
                    attempted,
                });
            }
            *state_bytes = attempted;
            Ok(entry.insert(aggregates.iter().map(ScalarAggregateState::new).collect()))
        }
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

fn ensure_finite_aggregate(value: f64, operation: &'static str) -> Result<(), ExecutionError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(ExecutionError::NonFiniteAggregate(operation))
    }
}

fn null_scalar(data_type: ScalarType) -> ScalarValue {
    match data_type {
        ScalarType::Int64 => ScalarValue::Int64 { value: None },
        ScalarType::Float64 => ScalarValue::Float64 { value: None },
        ScalarType::Boolean => ScalarValue::Boolean { value: None },
        ScalarType::Utf8 => ScalarValue::Utf8 { value: None },
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
