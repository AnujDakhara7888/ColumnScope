use serde::{Deserialize, Serialize};

use crate::error::PlanError;
use crate::expr::{BoundExpr, Expr, NamedExpr};
use crate::schema::{ScalarType, SchemaDef};

pub const PLAN_FORMAT_VERSION: u32 = 1;

/// Versioned query-plan document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryPlan {
    pub format_version: u32,
    pub input_schema: SchemaDef,
    pub plan: LogicalPlan,
}

impl QueryPlan {
    pub fn from_json(json: &str) -> Result<Self, PlanError> {
        let plan: Self = serde_json::from_str(json)
            .map_err(|error| PlanError::InvalidJson(error.to_string()))?;
        if plan.format_version != PLAN_FORMAT_VERSION {
            return Err(PlanError::UnsupportedPlanVersion {
                found: plan.format_version,
            });
        }
        plan.input_schema.validate()?;
        Ok(plan)
    }
}

/// Logical plan nodes supported by format version 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalPlan {
    Scan,
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        expressions: Vec<NamedExpr>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<NamedExpr>,
        aggregates: Vec<AggregateExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "function", rename_all = "snake_case", deny_unknown_fields)]
pub enum AggregateExpr {
    CountStar { alias: String },
    Count { alias: String, expr: Expr },
    Sum { alias: String, expr: Expr },
    Min { alias: String, expr: Expr },
    Max { alias: String, expr: Expr },
    Avg { alias: String, expr: Expr },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundQueryPlan {
    pub plan: BoundLogicalPlan,
    pub output_schema: SchemaDef,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundLogicalPlan {
    Scan {
        schema: SchemaDef,
    },
    Filter {
        input: Box<BoundLogicalPlan>,
        predicate: BoundExpr,
        schema: SchemaDef,
    },
    Project {
        input: Box<BoundLogicalPlan>,
        expressions: Vec<BoundNamedExpr>,
        schema: SchemaDef,
    },
    Aggregate {
        input: Box<BoundLogicalPlan>,
        group_by: Vec<BoundNamedExpr>,
        aggregates: Vec<BoundAggregateExpr>,
        schema: SchemaDef,
    },
}

impl BoundLogicalPlan {
    pub fn schema(&self) -> &SchemaDef {
        match self {
            Self::Scan { schema }
            | Self::Filter { schema, .. }
            | Self::Project { schema, .. }
            | Self::Aggregate { schema, .. } => schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundNamedExpr {
    pub alias: String,
    pub expr: BoundExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAggregateExpr {
    pub alias: String,
    pub function: AggregateFunction,
    pub expr: Option<BoundExpr>,
    pub data_type: ScalarType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
}
