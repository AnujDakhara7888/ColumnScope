use thiserror::Error;

/// Errors produced while constructing or pulling physical operators.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("batch size must be positive")]
    InvalidBatchSize,
    #[error("failed to access Parquet input: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to open Parquet input: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("failed to decode an Arrow batch: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("operator output counters overflowed")]
    CounterOverflow,
    #[error("operator cannot resume after an earlier pull failure")]
    OperatorFailed,
    #[error("physical expression expected {expected}, found {actual}")]
    ExpressionType {
        expected: &'static str,
        actual: String,
    },
    #[error("physical column index {index} is outside a {columns}-column batch")]
    ColumnOutOfBounds { index: usize, columns: usize },
    #[error("scan projection column {index} is outside a {columns}-column schema")]
    ProjectionOutOfBounds { index: usize, columns: usize },
    #[error("scan projection contains duplicate column {0}")]
    DuplicateProjectionColumn(usize),
    #[error("aggregate execution supports at most two group keys, found {0}")]
    UnsupportedGroupKeyCount(usize),
    #[error("aggregate output batch size must be positive")]
    InvalidAggregateOutputBatchSize,
    #[error(
        "aggregate state budget exceeded: limit is {limit} bytes, next group would require {attempted} total bytes"
    )]
    AggregateStateBudgetExceeded { limit: usize, attempted: usize },
    #[error("aggregate state size accounting overflowed")]
    AggregateStateSizeOverflow,
    #[error("integer overflow while computing {0}")]
    IntegerOverflow(&'static str),
    #[error("non-finite Float64 value encountered while computing {0}")]
    NonFiniteAggregate(&'static str),
    #[error("aggregate received unsupported value type {0}")]
    UnsupportedAggregateType(String),
    #[error("bound schema for {operator} is invalid: {reason}")]
    InvalidBoundSchema {
        operator: &'static str,
        reason: String,
    },
    #[error("physical schema mismatch at {operator}: expected {expected}, found {actual}")]
    PhysicalSchemaMismatch {
        operator: &'static str,
        expected: String,
        actual: String,
    },
    #[error("optimizer invariant failed: {0}")]
    OptimizerInvariant(String),
    #[error("row index {index} is outside a {rows}-row batch")]
    RowOutOfBounds { index: usize, rows: usize },
    #[error("unsupported Arrow type in scalar execution: {0}")]
    UnsupportedArrowType(String),
    #[error("non-finite Float64 input is not supported")]
    NonFiniteFloatInput,
}

/// Errors produced while parsing, validating, or binding a query contract.
#[derive(Debug, Error, PartialEq)]
pub enum PlanError {
    #[error("invalid JSON plan: {0}")]
    InvalidJson(String),
    #[error("unsupported plan format version {found}; expected version 1")]
    UnsupportedPlanVersion { found: u32 },
    #[error("schema contains an empty field name")]
    EmptyFieldName,
    #[error("schema contains duplicate field `{0}`")]
    DuplicateField(String),
    #[error("column `{0}` does not exist")]
    UnknownColumn(String),
    #[error("output name cannot be empty")]
    EmptyOutputName,
    #[error("output contains duplicate name `{0}`")]
    DuplicateOutput(String),
    #[error("{operator} requires {expected}, found {actual}")]
    InvalidType {
        operator: &'static str,
        expected: &'static str,
        actual: String,
    },
    #[error("{operator} requires matching operand types, found {left} and {right}")]
    TypeMismatch {
        operator: &'static str,
        left: String,
        right: String,
    },
    #[error("non-finite Float64 literal is not supported")]
    NonFiniteFloat,
    #[error("grouped aggregation supports at most two keys, found {0}")]
    TooManyGroupKeys(usize),
    #[error("aggregate plans require at least one group key or aggregate expression")]
    EmptyAggregate,
}

/// Errors produced while validating a pinned snapshot descriptor.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SnapshotError {
    #[error("invalid snapshot descriptor JSON: {0}")]
    InvalidJson(String),
    #[error("unsupported snapshot format version {found}; expected version 1")]
    UnsupportedVersion { found: u32 },
    #[error("snapshot field `{0}` cannot be empty")]
    EmptyField(&'static str),
    #[error("object key `{0}` must be a normalized relative path")]
    InvalidObjectKey(String),
    #[error("snapshot contains duplicate object key `{0}`")]
    DuplicateObjectKey(String),
    #[error("{field} must be a lowercase 64-character SHA-256 value")]
    InvalidSha256 { field: &'static str },
    #[error("descriptor schema fingerprint does not match its canonical schema")]
    SchemaFingerprintMismatch,
    #[error("file `{object_key}` uses a different schema fingerprint")]
    FileSchemaMismatch { object_key: String },
    #[error("invalid descriptor schema: {0}")]
    InvalidSchema(String),
}
