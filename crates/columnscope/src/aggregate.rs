//! Blocking hash aggregation over streaming Arrow input batches.

use std::cmp::Ordering;
use std::collections::{HashMap, hash_map};
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::ExecutionError;
use crate::evaluate::evaluate;
use crate::execution::{BatchOperator, OperatorMetrics, OperatorState};
use crate::expr::ScalarValue;
use crate::logical::{AggregateFunction, BoundAggregateExpr, BoundNamedExpr};
use crate::schema::ScalarType;

const HASH_ENTRY_OVERHEAD_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyPart {
    Int64(Option<i64>),
    Boolean(Option<bool>),
    Utf8(Option<String>),
}

impl GroupKeyPart {
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
struct GroupKey(Vec<GroupKeyPart>);

impl GroupKey {
    fn retained_bytes(&self, aggregate_count: usize) -> Result<usize, ExecutionError> {
        let key_parts = self
            .0
            .capacity()
            .checked_mul(size_of::<GroupKeyPart>())
            .ok_or(ExecutionError::AggregateStateSizeOverflow)?;
        let strings = self.0.iter().try_fold(0usize, |total, part| {
            total
                .checked_add(part.owned_bytes())
                .ok_or(ExecutionError::AggregateStateSizeOverflow)
        })?;
        let aggregate_states = aggregate_count
            .checked_mul(size_of::<AggregateState>())
            .ok_or(ExecutionError::AggregateStateSizeOverflow)?;
        [
            HASH_ENTRY_OVERHEAD_BYTES,
            size_of::<GroupKey>(),
            key_parts,
            strings,
            size_of::<Vec<AggregateState>>(),
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
enum AggregateState {
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

impl AggregateState {
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
                    ensure_finite(value, "SUM")?;
                    let next = sum.unwrap_or(0.0) + value;
                    ensure_finite(next, "SUM")?;
                    *sum = Some(next);
                }
            }
            Self::Min { value: current, .. } => {
                if let Some(value) = value.filter(|value| !value.is_null())
                    && current
                        .as_ref()
                        .is_none_or(|current| scalar_ordering(&value, current) == Ordering::Less)
                {
                    *current = Some(value);
                }
            }
            Self::Max { value: current, .. } => {
                if let Some(value) = value.filter(|value| !value.is_null())
                    && current
                        .as_ref()
                        .is_none_or(|current| scalar_ordering(&value, current) == Ordering::Greater)
                {
                    *current = Some(value);
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
                    ensure_finite(numeric, "AVG")?;
                    let next = *sum + numeric;
                    ensure_finite(next, "AVG")?;
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
                value.unwrap_or_else(|| null_value(data_type))
            }
            Self::Avg { sum, count } => ScalarValue::Float64 {
                value: (count > 0).then(|| sum / count as f64),
            },
        }
    }
}

/// Resource limits for blocking hash aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateOptions {
    /// Maximum deterministic charge for retained keys and aggregate states.
    pub state_budget_bytes: usize,
    /// Maximum number of aggregate rows returned by one pull.
    pub output_batch_rows: usize,
}

impl Default for AggregateOptions {
    fn default() -> Self {
        Self {
            state_budget_bytes: 64 * 1024 * 1024,
            output_batch_rows: 8_192,
        }
    }
}

/// Global, single-key, or two-key grouped hash aggregation.
///
/// The operator consumes its input on the first pull, retains only the hash
/// table, and drains that table directly into bounded output batches. Grouped
/// output follows hash-table iteration order and is never implicitly sorted.
pub struct HashAggregateExec {
    input: Box<dyn BatchOperator>,
    group_by: Vec<BoundNamedExpr>,
    aggregates: Vec<BoundAggregateExpr>,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
    options: AggregateOptions,
    prepared: bool,
    remaining_groups: Option<hash_map::IntoIter<GroupKey, Vec<AggregateState>>>,
    empty_output_pending: bool,
    peak_state_bytes: usize,
}

impl HashAggregateExec {
    pub fn try_new(
        input: Box<dyn BatchOperator>,
        group_by: Vec<BoundNamedExpr>,
        aggregates: Vec<BoundAggregateExpr>,
    ) -> Result<Self, ExecutionError> {
        Self::try_new_with_options(input, group_by, aggregates, AggregateOptions::default())
    }

    pub fn try_new_with_options(
        input: Box<dyn BatchOperator>,
        group_by: Vec<BoundNamedExpr>,
        aggregates: Vec<BoundAggregateExpr>,
        options: AggregateOptions,
    ) -> Result<Self, ExecutionError> {
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
            group_by,
            aggregates,
            schema: Arc::new(Schema::new(fields)),
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
            options,
            prepared: false,
            remaining_groups: None,
            empty_output_pending: false,
            peak_state_bytes: 0,
        })
    }

    /// Peak deterministic retained-state charge observed while building groups.
    pub fn peak_state_bytes(&self) -> usize {
        self.peak_state_bytes
    }

    fn prepare(&mut self) -> Result<(), ExecutionError> {
        let mut groups = HashMap::<GroupKey, Vec<AggregateState>>::new();
        let mut state_bytes = 0usize;
        if self.group_by.is_empty() {
            insert_group(
                &mut groups,
                GroupKey(Vec::new()),
                &self.aggregates,
                self.options.state_budget_bytes,
                &mut state_bytes,
            )?;
        }

        while let Some(batch) = self.input.next_batch()? {
            let group_values = self
                .group_by
                .iter()
                .map(|group| evaluate(&group.expr, &batch))
                .collect::<Result<Vec<_>, _>>()?;
            let aggregate_values = self
                .aggregates
                .iter()
                .map(|aggregate| {
                    aggregate
                        .expr
                        .as_ref()
                        .map(|expr| evaluate(expr, &batch))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;

            for row in 0..batch.num_rows() {
                let parts = group_values
                    .iter()
                    .map(|values| array_value(values, row).and_then(GroupKeyPart::from_value))
                    .collect::<Result<Vec<_>, _>>()?;
                let key = GroupKey(parts);
                let states = insert_group(
                    &mut groups,
                    key,
                    &self.aggregates,
                    self.options.state_budget_bytes,
                    &mut state_bytes,
                )?;
                for ((state, aggregate), values) in states
                    .iter_mut()
                    .zip(&self.aggregates)
                    .zip(&aggregate_values)
                {
                    let value = values
                        .as_ref()
                        .map(|values| array_value(values, row))
                        .transpose()?;
                    state.update(aggregate, value)?;
                }
            }
        }

        self.peak_state_bytes = state_bytes;
        self.empty_output_pending = groups.is_empty();
        self.remaining_groups = Some(groups.into_iter());
        self.prepared = true;
        Ok(())
    }

    fn next_output_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        let remaining = self
            .remaining_groups
            .as_mut()
            .expect("prepared aggregation owns its group iterator");
        let row_count = remaining.len().min(self.options.output_batch_rows);
        if row_count == 0 && !self.empty_output_pending {
            return Ok(None);
        }
        let mut columns =
            vec![Vec::<ScalarValue>::with_capacity(row_count); self.schema.fields().len()];
        for _ in 0..row_count {
            let (key, states) = remaining
                .next()
                .expect("iterator length was checked before aggregate output");
            let mut index = 0;
            for part in key.0 {
                columns[index].push(part.into_value());
                index += 1;
            }
            for state in states {
                columns[index].push(state.finish());
                index += 1;
            }
        }
        let arrays = columns
            .iter()
            .zip(self.schema.fields())
            .map(|(values, field)| values_to_array(values, field.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let output = RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)?;
        self.empty_output_pending = false;
        Ok(Some(output))
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

impl BatchOperator for HashAggregateExec {
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
        if !self.prepared
            && let Err(error) = self.prepare()
        {
            return self.fail(error);
        }
        let output = match self.next_output_batch() {
            Ok(output) => output,
            Err(error) => return self.fail(error),
        };
        match output {
            Some(output) => {
                if let Err(error) = self.metrics.record_output(output.num_rows()) {
                    return self.fail(error);
                }
                Ok(Some(output))
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

fn insert_group<'a>(
    groups: &'a mut HashMap<GroupKey, Vec<AggregateState>>,
    key: GroupKey,
    aggregates: &[BoundAggregateExpr],
    budget: usize,
    state_bytes: &mut usize,
) -> Result<&'a mut Vec<AggregateState>, ExecutionError> {
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
            Ok(entry.insert(new_states(aggregates)))
        }
    }
}

fn new_states(aggregates: &[BoundAggregateExpr]) -> Vec<AggregateState> {
    aggregates.iter().map(AggregateState::new).collect()
}

fn ensure_finite(value: f64, operation: &'static str) -> Result<(), ExecutionError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(ExecutionError::NonFiniteAggregate(operation))
    }
}

fn scalar_ordering(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Int64 { value: Some(left) }, ScalarValue::Int64 { value: Some(right) }) => {
            left.cmp(right)
        }
        (
            ScalarValue::Float64 { value: Some(left) },
            ScalarValue::Float64 { value: Some(right) },
        ) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        _ => unreachable!("binder and evaluator provide matching numeric values"),
    }
}

fn null_value(data_type: ScalarType) -> ScalarValue {
    match data_type {
        ScalarType::Int64 => ScalarValue::Int64 { value: None },
        ScalarType::Float64 => ScalarValue::Float64 { value: None },
        ScalarType::Boolean => ScalarValue::Boolean { value: None },
        ScalarType::Utf8 => ScalarValue::Utf8 { value: None },
    }
}

fn array_value(array: &ArrayRef, row: usize) -> Result<ScalarValue, ExecutionError> {
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
                return Err(ExecutionError::NonFiniteAggregate("input"));
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
        data_type => Err(ExecutionError::UnsupportedAggregateType(
            data_type.to_string(),
        )),
    }
}

fn values_to_array(
    values: &[ScalarValue],
    data_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
    match data_type {
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Int64 { value } => Ok(*value),
                    value => Err(ExecutionError::UnsupportedAggregateType(
                        value.data_type().to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Float64 { value } => Ok(*value),
                    value => Err(ExecutionError::UnsupportedAggregateType(
                        value.data_type().to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Boolean { value } => Ok(*value),
                    value => Err(ExecutionError::UnsupportedAggregateType(
                        value.data_type().to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )) as ArrayRef),
        DataType::Utf8 => {
            let strings = values
                .iter()
                .map(|value| match value {
                    ScalarValue::Utf8 { value } => Ok(value.as_deref()),
                    value => Err(ExecutionError::UnsupportedAggregateType(
                        value.data_type().to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StringArray::from(strings)) as ArrayRef)
        }
        data_type => Err(ExecutionError::UnsupportedAggregateType(
            data_type.to_string(),
        )),
    }
}
