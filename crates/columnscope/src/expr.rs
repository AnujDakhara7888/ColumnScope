use serde::{Deserialize, Serialize};

use crate::schema::ScalarType;

/// A typed scalar literal. Null is represented by `value: null` on a variant,
/// so its type remains explicit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScalarValue {
    Int64 { value: Option<i64> },
    Float64 { value: Option<f64> },
    Boolean { value: Option<bool> },
    Utf8 { value: Option<String> },
}

impl ScalarValue {
    pub fn data_type(&self) -> ScalarType {
        match self {
            Self::Int64 { .. } => ScalarType::Int64,
            Self::Float64 { .. } => ScalarType::Float64,
            Self::Boolean { .. } => ScalarType::Boolean,
            Self::Utf8 { .. } => ScalarType::Utf8,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            Self::Int64 { value } => value.is_none(),
            Self::Float64 { value } => value.is_none(),
            Self::Boolean { value } => value.is_none(),
            Self::Utf8 { value } => value.is_none(),
        }
    }
}

/// Unbound expression syntax supported by plan format version 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expr {
    Column { name: String },
    Literal { literal: ScalarValue },
    Eq { left: Box<Expr>, right: Box<Expr> },
    NotEq { left: Box<Expr>, right: Box<Expr> },
    Lt { left: Box<Expr>, right: Box<Expr> },
    LtEq { left: Box<Expr>, right: Box<Expr> },
    Gt { left: Box<Expr>, right: Box<Expr> },
    GtEq { left: Box<Expr>, right: Box<Expr> },
    And { left: Box<Expr>, right: Box<Expr> },
    Or { left: Box<Expr>, right: Box<Expr> },
    Not { expr: Box<Expr> },
    IsNull { expr: Box<Expr> },
    IsNotNull { expr: Box<Expr> },
}

/// Expression with an output name used by projections and group keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedExpr {
    pub alias: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExprType {
    pub data_type: ScalarType,
    pub nullable: bool,
}

/// Bound expression with column names resolved to stable input positions.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    pub result: ExprType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExprKind {
    Column { index: usize, name: String },
    Literal(ScalarValue),
    Eq(Box<BoundExpr>, Box<BoundExpr>),
    NotEq(Box<BoundExpr>, Box<BoundExpr>),
    Lt(Box<BoundExpr>, Box<BoundExpr>),
    LtEq(Box<BoundExpr>, Box<BoundExpr>),
    Gt(Box<BoundExpr>, Box<BoundExpr>),
    GtEq(Box<BoundExpr>, Box<BoundExpr>),
    And(Box<BoundExpr>, Box<BoundExpr>),
    Or(Box<BoundExpr>, Box<BoundExpr>),
    Not(Box<BoundExpr>),
    IsNull(Box<BoundExpr>),
    IsNotNull(Box<BoundExpr>),
}
