//! Correctness-preserving logical rewrites and scan pushdown analysis.

use std::collections::BTreeSet;

use crate::error::ExecutionError;
use crate::expr::{BoundExpr, BoundExprKind, ExprType};
use crate::logical::{BoundAggregateExpr, BoundLogicalPlan, BoundNamedExpr, BoundQueryPlan};
use crate::schema::SchemaDef;

/// Observable optimizer decisions consumed by physical planning.
#[derive(Debug, Clone, PartialEq)]
pub struct OptimizedQuery {
    pub query: BoundQueryPlan,
    pub scan_projection: Vec<usize>,
    pub source_columns: usize,
    pub pruning_predicate: Option<BoundExpr>,
    pub filter_pushdowns: usize,
}

/// Pushes safe filters through projections and removes unused scan columns.
///
/// Filters remain in the returned tree as residual correctness checks. The
/// pruning predicate is a copy used only to reject impossible row groups.
pub fn optimize_query(query: BoundQueryPlan) -> Result<OptimizedQuery, ExecutionError> {
    let mut filter_pushdowns = 0;
    let pushed = push_filters(query.plan, &mut filter_pushdowns)?;
    let pruning_predicate = find_scan_predicate(&pushed);
    let required = (0..pushed.schema().fields.len()).collect::<BTreeSet<_>>();
    let pruned = prune_columns(pushed, &required)?;
    Ok(OptimizedQuery {
        query: BoundQueryPlan {
            output_schema: query.output_schema,
            plan: pruned.plan,
        },
        scan_projection: pruned.scan_projection,
        source_columns: pruned.source_columns,
        pruning_predicate,
        filter_pushdowns,
    })
}

fn push_filters(
    plan: BoundLogicalPlan,
    pushdown_count: &mut usize,
) -> Result<BoundLogicalPlan, ExecutionError> {
    match plan {
        BoundLogicalPlan::Scan { .. } => Ok(plan),
        BoundLogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => {
            let input = push_filters(*input, pushdown_count)?;
            push_filter_node(input, predicate, schema, pushdown_count)
        }
        BoundLogicalPlan::Project {
            input,
            expressions,
            schema,
        } => Ok(BoundLogicalPlan::Project {
            input: Box::new(push_filters(*input, pushdown_count)?),
            expressions,
            schema,
        }),
        BoundLogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => Ok(BoundLogicalPlan::Aggregate {
            input: Box::new(push_filters(*input, pushdown_count)?),
            group_by,
            aggregates,
            schema,
        }),
    }
}

fn push_filter_node(
    input: BoundLogicalPlan,
    predicate: BoundExpr,
    output_schema: SchemaDef,
    pushdown_count: &mut usize,
) -> Result<BoundLogicalPlan, ExecutionError> {
    match input {
        BoundLogicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            let substituted = substitute_projection_columns(predicate, &expressions)?;
            let filter_schema = input.schema().clone();
            let pushed_filter =
                push_filter_node(*input, substituted, filter_schema, pushdown_count)?;
            *pushdown_count = pushdown_count.checked_add(1).ok_or_else(|| {
                ExecutionError::OptimizerInvariant("filter pushdown count overflowed".to_owned())
            })?;
            Ok(BoundLogicalPlan::Project {
                input: Box::new(pushed_filter),
                expressions,
                schema,
            })
        }
        input => Ok(BoundLogicalPlan::Filter {
            input: Box::new(input),
            predicate,
            schema: output_schema,
        }),
    }
}

fn substitute_projection_columns(
    expression: BoundExpr,
    projections: &[BoundNamedExpr],
) -> Result<BoundExpr, ExecutionError> {
    transform_expression(expression, &mut |index, _name, _result| {
        projections
            .get(index)
            .map(|projection| projection.expr.clone())
            .ok_or_else(|| {
                ExecutionError::OptimizerInvariant(format!(
                    "projection expression index {index} is out of bounds"
                ))
            })
    })
}

struct PrunedPlan {
    plan: BoundLogicalPlan,
    mapping: Vec<Option<usize>>,
    scan_projection: Vec<usize>,
    source_columns: usize,
}

fn prune_columns(
    plan: BoundLogicalPlan,
    required: &BTreeSet<usize>,
) -> Result<PrunedPlan, ExecutionError> {
    match plan {
        BoundLogicalPlan::Scan { schema } => {
            validate_required(required, schema.fields.len())?;
            let scan_projection = required.iter().copied().collect::<Vec<_>>();
            let mapping = index_mapping(schema.fields.len(), &scan_projection);
            let fields = scan_projection
                .iter()
                .map(|index| schema.fields[*index].clone())
                .collect();
            Ok(PrunedPlan {
                plan: BoundLogicalPlan::Scan {
                    schema: SchemaDef { fields },
                },
                mapping,
                scan_projection,
                source_columns: schema.fields.len(),
            })
        }
        BoundLogicalPlan::Filter {
            input, predicate, ..
        } => {
            let input_columns = input.schema().fields.len();
            validate_required(required, input_columns)?;
            let mut child_required = required.clone();
            collect_columns(&predicate, &mut child_required);
            let child = prune_columns(*input, &child_required)?;
            let predicate = remap_expression(predicate, &child.mapping)?;
            let schema = child.plan.schema().clone();
            Ok(PrunedPlan {
                plan: BoundLogicalPlan::Filter {
                    input: Box::new(child.plan),
                    predicate,
                    schema,
                },
                mapping: child.mapping,
                scan_projection: child.scan_projection,
                source_columns: child.source_columns,
            })
        }
        BoundLogicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            validate_required(required, expressions.len())?;
            let selected = required.iter().copied().collect::<Vec<_>>();
            let mut child_required = BTreeSet::new();
            for index in &selected {
                collect_columns(&expressions[*index].expr, &mut child_required);
            }
            let child = prune_columns(*input, &child_required)?;
            let expressions = selected
                .iter()
                .map(|index| {
                    let named = &expressions[*index];
                    Ok(BoundNamedExpr {
                        alias: named.alias.clone(),
                        expr: remap_expression(named.expr.clone(), &child.mapping)?,
                    })
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?;
            let fields = selected
                .iter()
                .map(|index| schema.fields[*index].clone())
                .collect();
            Ok(PrunedPlan {
                plan: BoundLogicalPlan::Project {
                    input: Box::new(child.plan),
                    expressions,
                    schema: SchemaDef { fields },
                },
                mapping: index_mapping(schema.fields.len(), &selected),
                scan_projection: child.scan_projection,
                source_columns: child.source_columns,
            })
        }
        BoundLogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let mut child_required = BTreeSet::new();
            for group in &group_by {
                collect_columns(&group.expr, &mut child_required);
            }
            for aggregate in &aggregates {
                if let Some(expression) = &aggregate.expr {
                    collect_columns(expression, &mut child_required);
                }
            }
            let child = prune_columns(*input, &child_required)?;
            let group_by = group_by
                .into_iter()
                .map(|named| {
                    Ok(BoundNamedExpr {
                        alias: named.alias,
                        expr: remap_expression(named.expr, &child.mapping)?,
                    })
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?;
            let aggregates = aggregates
                .into_iter()
                .map(|aggregate| remap_aggregate(aggregate, &child.mapping))
                .collect::<Result<Vec<_>, _>>()?;
            let output_columns = schema.fields.len();
            Ok(PrunedPlan {
                plan: BoundLogicalPlan::Aggregate {
                    input: Box::new(child.plan),
                    group_by,
                    aggregates,
                    schema,
                },
                mapping: (0..output_columns).map(Some).collect(),
                scan_projection: child.scan_projection,
                source_columns: child.source_columns,
            })
        }
    }
}

fn remap_aggregate(
    aggregate: BoundAggregateExpr,
    mapping: &[Option<usize>],
) -> Result<BoundAggregateExpr, ExecutionError> {
    Ok(BoundAggregateExpr {
        expr: aggregate
            .expr
            .map(|expression| remap_expression(expression, mapping))
            .transpose()?,
        ..aggregate
    })
}

fn remap_expression(
    expression: BoundExpr,
    mapping: &[Option<usize>],
) -> Result<BoundExpr, ExecutionError> {
    transform_expression(expression, &mut |index, name, result| {
        let index = mapping.get(index).copied().flatten().ok_or_else(|| {
            ExecutionError::OptimizerInvariant(format!(
                "column `{name}` at index {index} was not retained"
            ))
        })?;
        Ok(BoundExpr {
            kind: BoundExprKind::Column { index, name },
            result,
        })
    })
}

fn transform_expression<F>(
    expression: BoundExpr,
    replace_column: &mut F,
) -> Result<BoundExpr, ExecutionError>
where
    F: FnMut(usize, String, ExprType) -> Result<BoundExpr, ExecutionError>,
{
    let result = expression.result;
    let kind = match expression.kind {
        BoundExprKind::Column { index, name } => {
            return replace_column(index, name, result);
        }
        BoundExprKind::Literal(value) => BoundExprKind::Literal(value),
        BoundExprKind::Eq(left, right) => BoundExprKind::Eq(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::NotEq(left, right) => BoundExprKind::NotEq(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::Lt(left, right) => BoundExprKind::Lt(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::LtEq(left, right) => BoundExprKind::LtEq(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::Gt(left, right) => BoundExprKind::Gt(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::GtEq(left, right) => BoundExprKind::GtEq(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::And(left, right) => BoundExprKind::And(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::Or(left, right) => BoundExprKind::Or(
            Box::new(transform_expression(*left, replace_column)?),
            Box::new(transform_expression(*right, replace_column)?),
        ),
        BoundExprKind::Not(expression) => {
            BoundExprKind::Not(Box::new(transform_expression(*expression, replace_column)?))
        }
        BoundExprKind::IsNull(expression) => {
            BoundExprKind::IsNull(Box::new(transform_expression(*expression, replace_column)?))
        }
        BoundExprKind::IsNotNull(expression) => {
            BoundExprKind::IsNotNull(Box::new(transform_expression(*expression, replace_column)?))
        }
    };
    Ok(BoundExpr { kind, result })
}

fn collect_columns(expression: &BoundExpr, columns: &mut BTreeSet<usize>) {
    match &expression.kind {
        BoundExprKind::Column { index, .. } => {
            columns.insert(*index);
        }
        BoundExprKind::Literal(_) => {}
        BoundExprKind::Eq(left, right)
        | BoundExprKind::NotEq(left, right)
        | BoundExprKind::Lt(left, right)
        | BoundExprKind::LtEq(left, right)
        | BoundExprKind::Gt(left, right)
        | BoundExprKind::GtEq(left, right)
        | BoundExprKind::And(left, right)
        | BoundExprKind::Or(left, right) => {
            collect_columns(left, columns);
            collect_columns(right, columns);
        }
        BoundExprKind::Not(expression)
        | BoundExprKind::IsNull(expression)
        | BoundExprKind::IsNotNull(expression) => collect_columns(expression, columns),
    }
}

fn find_scan_predicate(plan: &BoundLogicalPlan) -> Option<BoundExpr> {
    match plan {
        BoundLogicalPlan::Scan { .. } => None,
        BoundLogicalPlan::Filter {
            input, predicate, ..
        } => match input.as_ref() {
            BoundLogicalPlan::Scan { .. } => Some(predicate.clone()),
            BoundLogicalPlan::Filter { .. } => find_scan_predicate(input).map(|inner| {
                let result = ExprType {
                    data_type: predicate.result.data_type,
                    nullable: predicate.result.nullable || inner.result.nullable,
                };
                BoundExpr {
                    kind: BoundExprKind::And(Box::new(predicate.clone()), Box::new(inner)),
                    result,
                }
            }),
            _ => find_scan_predicate(input),
        },
        BoundLogicalPlan::Project { input, .. } | BoundLogicalPlan::Aggregate { input, .. } => {
            find_scan_predicate(input)
        }
    }
}

fn index_mapping(columns: usize, selected: &[usize]) -> Vec<Option<usize>> {
    let mut mapping = vec![None; columns];
    for (new_index, old_index) in selected.iter().enumerate() {
        mapping[*old_index] = Some(new_index);
    }
    mapping
}

fn validate_required(required: &BTreeSet<usize>, columns: usize) -> Result<(), ExecutionError> {
    if let Some(index) = required.iter().find(|index| **index >= columns) {
        Err(ExecutionError::OptimizerInvariant(format!(
            "required column index {index} is outside a {columns}-column schema"
        )))
    } else {
        Ok(())
    }
}
