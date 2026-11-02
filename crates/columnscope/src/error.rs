use thiserror::Error;

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
