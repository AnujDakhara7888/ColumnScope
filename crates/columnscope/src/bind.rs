use std::collections::HashSet;

use crate::error::PlanError;
use crate::expr::{BoundExpr, BoundExprKind, Expr, ExprType, ScalarValue};
use crate::logical::{
    AggregateExpr, AggregateFunction, BoundAggregateExpr, BoundLogicalPlan, BoundNamedExpr,
    BoundQueryPlan, LogicalPlan, QueryPlan,
};
use crate::schema::{FieldDef, ScalarType, SchemaDef};

pub fn bind_query(plan: QueryPlan) -> Result<BoundQueryPlan, PlanError> {
    if plan.format_version != crate::logical::PLAN_FORMAT_VERSION {
        return Err(PlanError::UnsupportedPlanVersion {
            found: plan.format_version,
        });
    }
    plan.input_schema.validate()?;
    let bound = bind_plan(plan.plan, &plan.input_schema)?;
    let output_schema = bound.schema().clone();
    Ok(BoundQueryPlan {
        plan: bound,
        output_schema,
    })
}

fn bind_plan(plan: LogicalPlan, input_schema: &SchemaDef) -> Result<BoundLogicalPlan, PlanError> {
    match plan {
        LogicalPlan::Scan => Ok(BoundLogicalPlan::Scan {
            schema: input_schema.clone(),
        }),
        LogicalPlan::Filter { input, predicate } => {
            let input = bind_plan(*input, input_schema)?;
            let schema = input.schema().clone();
            let predicate = bind_expr(predicate, &schema)?;
            require_type("filter predicate", &predicate, ScalarType::Boolean)?;
            Ok(BoundLogicalPlan::Filter {
                input: Box::new(input),
                predicate,
                schema,
            })
        }
        LogicalPlan::Project { input, expressions } => {
            let input = bind_plan(*input, input_schema)?;
            let input_output = input.schema().clone();
            let mut names = HashSet::with_capacity(expressions.len());
            let mut fields = Vec::with_capacity(expressions.len());
            let mut bound_expressions = Vec::with_capacity(expressions.len());
            for named in expressions {
                validate_output_name(&named.alias, &mut names)?;
                let expr = bind_expr(named.expr, &input_output)?;
                fields.push(FieldDef {
                    name: named.alias.clone(),
                    data_type: expr.result.data_type,
                    nullable: expr.result.nullable,
                });
                bound_expressions.push(BoundNamedExpr {
                    alias: named.alias,
                    expr,
                });
            }
            Ok(BoundLogicalPlan::Project {
                input: Box::new(input),
                expressions: bound_expressions,
                schema: SchemaDef { fields },
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => bind_aggregate(*input, group_by, aggregates, input_schema),
    }
}

fn bind_aggregate(
    input: LogicalPlan,
    group_by: Vec<crate::expr::NamedExpr>,
    aggregates: Vec<AggregateExpr>,
    root_schema: &SchemaDef,
) -> Result<BoundLogicalPlan, PlanError> {
    if group_by.len() > 2 {
        return Err(PlanError::TooManyGroupKeys(group_by.len()));
    }
    if group_by.is_empty() && aggregates.is_empty() {
        return Err(PlanError::EmptyAggregate);
    }

    let input = bind_plan(input, root_schema)?;
    let input_output = input.schema().clone();
    let mut names = HashSet::with_capacity(group_by.len() + aggregates.len());
    let mut fields = Vec::with_capacity(group_by.len() + aggregates.len());
    let mut bound_groups = Vec::with_capacity(group_by.len());
    for named in group_by {
        validate_output_name(&named.alias, &mut names)?;
        let expr = bind_expr(named.expr, &input_output)?;
        if !matches!(
            expr.result.data_type,
            ScalarType::Int64 | ScalarType::Utf8 | ScalarType::Boolean
        ) {
            return Err(PlanError::InvalidType {
                operator: "group key",
                expected: "Int64, Utf8, or Boolean",
                actual: expr.result.data_type.to_string(),
            });
        }
        fields.push(FieldDef {
            name: named.alias.clone(),
            data_type: expr.result.data_type,
            nullable: expr.result.nullable,
        });
        bound_groups.push(BoundNamedExpr {
            alias: named.alias,
            expr,
        });
    }

    let mut bound_aggregates = Vec::with_capacity(aggregates.len());
    for aggregate in aggregates {
        let bound = bind_aggregate_expr(aggregate, &input_output, &mut names)?;
        fields.push(FieldDef {
            name: bound.alias.clone(),
            data_type: bound.data_type,
            nullable: bound.nullable,
        });
        bound_aggregates.push(bound);
    }

    Ok(BoundLogicalPlan::Aggregate {
        input: Box::new(input),
        group_by: bound_groups,
        aggregates: bound_aggregates,
        schema: SchemaDef { fields },
    })
}

fn bind_aggregate_expr(
    aggregate: AggregateExpr,
    schema: &SchemaDef,
    names: &mut HashSet<String>,
) -> Result<BoundAggregateExpr, PlanError> {
    let (alias, function, expression) = match aggregate {
        AggregateExpr::CountStar { alias } => (alias, AggregateFunction::CountStar, None),
        AggregateExpr::Count { alias, expr } => (alias, AggregateFunction::Count, Some(expr)),
        AggregateExpr::Sum { alias, expr } => (alias, AggregateFunction::Sum, Some(expr)),
        AggregateExpr::Min { alias, expr } => (alias, AggregateFunction::Min, Some(expr)),
        AggregateExpr::Max { alias, expr } => (alias, AggregateFunction::Max, Some(expr)),
        AggregateExpr::Avg { alias, expr } => (alias, AggregateFunction::Avg, Some(expr)),
    };
    validate_output_name(&alias, names)?;

    let expr = expression.map(|expr| bind_expr(expr, schema)).transpose()?;
    let (data_type, nullable) = match function {
        AggregateFunction::CountStar | AggregateFunction::Count => (ScalarType::Int64, false),
        AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
            let expression = expr.as_ref().expect("non-count aggregate has expression");
            require_numeric("aggregate", expression)?;
            (expression.result.data_type, true)
        }
        AggregateFunction::Avg => {
            let expression = expr.as_ref().expect("AVG has expression");
            require_numeric("AVG", expression)?;
            (ScalarType::Float64, true)
        }
    };

    Ok(BoundAggregateExpr {
        alias,
        function,
        expr,
        data_type,
        nullable,
    })
}

pub fn bind_expr(expr: Expr, schema: &SchemaDef) -> Result<BoundExpr, PlanError> {
    match expr {
        Expr::Column { name } => {
            let (index, field) = schema
                .field(&name)
                .ok_or_else(|| PlanError::UnknownColumn(name.clone()))?;
            Ok(BoundExpr {
                kind: BoundExprKind::Column { index, name },
                result: ExprType {
                    data_type: field.data_type,
                    nullable: field.nullable,
                },
            })
        }
        Expr::Literal { literal } => {
            if let ScalarValue::Float64 { value: Some(value) } = literal
                && !value.is_finite()
            {
                return Err(PlanError::NonFiniteFloat);
            }
            let result = ExprType {
                data_type: literal.data_type(),
                nullable: literal.is_null(),
            };
            Ok(BoundExpr {
                kind: BoundExprKind::Literal(literal),
                result,
            })
        }
        Expr::Eq { left, right } => {
            bind_comparison("=", *left, *right, schema, false, BoundExprKind::Eq)
        }
        Expr::NotEq { left, right } => {
            bind_comparison("!=", *left, *right, schema, false, BoundExprKind::NotEq)
        }
        Expr::Lt { left, right } => {
            bind_comparison("<", *left, *right, schema, true, BoundExprKind::Lt)
        }
        Expr::LtEq { left, right } => {
            bind_comparison("<=", *left, *right, schema, true, BoundExprKind::LtEq)
        }
        Expr::Gt { left, right } => {
            bind_comparison(">", *left, *right, schema, true, BoundExprKind::Gt)
        }
        Expr::GtEq { left, right } => {
            bind_comparison(">=", *left, *right, schema, true, BoundExprKind::GtEq)
        }
        Expr::And { left, right } => {
            bind_boolean_binary("AND", *left, *right, schema, BoundExprKind::And)
        }
        Expr::Or { left, right } => {
            bind_boolean_binary("OR", *left, *right, schema, BoundExprKind::Or)
        }
        Expr::Not { expr } => {
            let expr = bind_expr(*expr, schema)?;
            require_type("NOT", &expr, ScalarType::Boolean)?;
            let result = expr.result;
            Ok(BoundExpr {
                kind: BoundExprKind::Not(Box::new(expr)),
                result,
            })
        }
        Expr::IsNull { expr } => bind_null_test(*expr, schema, true),
        Expr::IsNotNull { expr } => bind_null_test(*expr, schema, false),
    }
}

fn bind_comparison<F>(
    operator: &'static str,
    left: Expr,
    right: Expr,
    schema: &SchemaDef,
    ordered: bool,
    constructor: F,
) -> Result<BoundExpr, PlanError>
where
    F: FnOnce(Box<BoundExpr>, Box<BoundExpr>) -> BoundExprKind,
{
    let left = bind_expr(left, schema)?;
    let right = bind_expr(right, schema)?;
    if left.result.data_type != right.result.data_type {
        return Err(PlanError::TypeMismatch {
            operator,
            left: left.result.data_type.to_string(),
            right: right.result.data_type.to_string(),
        });
    }
    if ordered && left.result.data_type == ScalarType::Boolean {
        return Err(PlanError::InvalidType {
            operator,
            expected: "Int64, Float64, or Utf8",
            actual: ScalarType::Boolean.to_string(),
        });
    }
    let nullable = left.result.nullable || right.result.nullable;
    Ok(BoundExpr {
        kind: constructor(Box::new(left), Box::new(right)),
        result: ExprType {
            data_type: ScalarType::Boolean,
            nullable,
        },
    })
}

fn bind_boolean_binary<F>(
    operator: &'static str,
    left: Expr,
    right: Expr,
    schema: &SchemaDef,
    constructor: F,
) -> Result<BoundExpr, PlanError>
where
    F: FnOnce(Box<BoundExpr>, Box<BoundExpr>) -> BoundExprKind,
{
    let left = bind_expr(left, schema)?;
    let right = bind_expr(right, schema)?;
    require_type(operator, &left, ScalarType::Boolean)?;
    require_type(operator, &right, ScalarType::Boolean)?;
    let nullable = left.result.nullable || right.result.nullable;
    Ok(BoundExpr {
        kind: constructor(Box::new(left), Box::new(right)),
        result: ExprType {
            data_type: ScalarType::Boolean,
            nullable,
        },
    })
}

fn bind_null_test(expr: Expr, schema: &SchemaDef, is_null: bool) -> Result<BoundExpr, PlanError> {
    let expr = bind_expr(expr, schema)?;
    let kind = if is_null {
        BoundExprKind::IsNull(Box::new(expr))
    } else {
        BoundExprKind::IsNotNull(Box::new(expr))
    };
    Ok(BoundExpr {
        kind,
        result: ExprType {
            data_type: ScalarType::Boolean,
            nullable: false,
        },
    })
}

fn require_type(
    operator: &'static str,
    expr: &BoundExpr,
    expected: ScalarType,
) -> Result<(), PlanError> {
    if expr.result.data_type != expected {
        return Err(PlanError::InvalidType {
            operator,
            expected: match expected {
                ScalarType::Int64 => "Int64",
                ScalarType::Float64 => "Float64",
                ScalarType::Boolean => "Boolean",
                ScalarType::Utf8 => "Utf8",
            },
            actual: expr.result.data_type.to_string(),
        });
    }
    Ok(())
}

fn require_numeric(operator: &'static str, expr: &BoundExpr) -> Result<(), PlanError> {
    if !expr.result.data_type.is_numeric() {
        return Err(PlanError::InvalidType {
            operator,
            expected: "Int64 or Float64",
            actual: expr.result.data_type.to_string(),
        });
    }
    Ok(())
}

fn validate_output_name(name: &str, names: &mut HashSet<String>) -> Result<(), PlanError> {
    if name.is_empty() {
        return Err(PlanError::EmptyOutputName);
    }
    if !names.insert(name.to_owned()) {
        return Err(PlanError::DuplicateOutput(name.to_owned()));
    }
    Ok(())
}
