//! Unoptimized logical-to-physical planning and stable explain output.

use std::fmt::{self, Write};
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;

use crate::aggregate::{AggregateOptions, HashAggregateExec};
use crate::error::ExecutionError;
use crate::execution::BatchOperator;
use crate::logical::{BoundLogicalPlan, BoundQueryPlan};
use crate::operators::{FilterExec, ProjectExec};
use crate::optimizer::optimize_query;
use crate::scan::{ParquetScan, ScanPushdowns};
use crate::schema::SchemaDef;

/// Planning behavior selected for a physical query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanningMode {
    /// Preserve the bound logical tree without applying rewrite rules.
    Unoptimized,
    /// Apply safe filter/projection pushdown and conservative row-group pruning.
    Optimized,
}

impl fmt::Display for PlanningMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unoptimized => formatter.write_str("unoptimized"),
            Self::Optimized => formatter.write_str("optimized"),
        }
    }
}

/// Physical resource settings applied while lowering a bound query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerOptions {
    pub mode: PlanningMode,
    pub scan_batch_rows: usize,
    pub aggregate: AggregateOptions,
}

impl Default for PlannerOptions {
    fn default() -> Self {
        Self {
            mode: PlanningMode::Unoptimized,
            scan_batch_rows: 8_192,
            aggregate: AggregateOptions::default(),
        }
    }
}

/// Operator kinds shown in a physical explain tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorKind {
    ParquetScan,
    FilterExec,
    ProjectExec,
    HashAggregateExec,
}

impl fmt::Display for PhysicalOperatorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ParquetScan => "ParquetScan",
            Self::FilterExec => "FilterExec",
            Self::ProjectExec => "ProjectExec",
            Self::HashAggregateExec => "HashAggregateExec",
        };
        formatter.write_str(name)
    }
}

/// One node in a stable, inspectable physical plan description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainNode {
    pub operator: PhysicalOperatorKind,
    pub schema: SchemaDef,
    pub properties: Vec<(String, String)>,
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    fn render(&self, depth: usize, output: &mut String) {
        let _ = write!(output, "{}{}", "  ".repeat(depth), self.operator);
        if !self.properties.is_empty() {
            let properties = self
                .properties
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(output, " [{properties}]");
        }
        let _ = writeln!(output, " schema={}", format_schema(&self.schema));
        for child in &self.children {
            child.render(depth + 1, output);
        }
    }
}

/// A live physical operator tree and its immutable explain description.
pub struct PlannedQuery {
    pub root: Box<dyn BatchOperator>,
    pub mode: PlanningMode,
    pub explain_root: ExplainNode,
    pub filter_pushdowns: usize,
}

impl PlannedQuery {
    /// Renders the selected planning mode and physical operator tree.
    pub fn explain(&self) -> String {
        let mut output = if self.mode == PlanningMode::Optimized {
            format!(
                "mode={} [filter_pushdowns={}]\n",
                self.mode, self.filter_pushdowns
            )
        } else {
            format!("mode={}\n", self.mode)
        };
        self.explain_root.render(0, &mut output);
        output
    }
}

/// Lowers a bound logical plan into a live physical operator tree.
///
/// Every constructed operator schema is checked against its bound logical
/// schema before the query is returned. The current mode performs no rewrites.
pub fn plan_query(
    query: BoundQueryPlan,
    parquet_path: impl AsRef<Path>,
    options: PlannerOptions,
) -> Result<PlannedQuery, ExecutionError> {
    ensure_schema_defs("QueryOutput", &query.output_schema, query.plan.schema())?;
    let (query, context) = match options.mode {
        PlanningMode::Unoptimized => (
            query,
            BuildContext {
                options,
                scan_projection: None,
                pruning_predicate: None,
                filter_pushdowns: 0,
            },
        ),
        PlanningMode::Optimized => {
            let optimized = optimize_query(query)?;
            let context = BuildContext {
                options,
                scan_projection: Some(optimized.scan_projection),
                pruning_predicate: optimized.pruning_predicate,
                filter_pushdowns: optimized.filter_pushdowns,
            };
            (optimized.query, context)
        }
    };
    ensure_schema_defs("QueryOutput", &query.output_schema, query.plan.schema())?;
    let (root, explain_root) = build_node(query.plan, parquet_path.as_ref(), &context)?;
    ensure_physical_schema("QueryOutput", &query.output_schema, root.schema())?;
    Ok(PlannedQuery {
        root,
        mode: options.mode,
        explain_root,
        filter_pushdowns: context.filter_pushdowns,
    })
}

struct BuildContext {
    options: PlannerOptions,
    scan_projection: Option<Vec<usize>>,
    pruning_predicate: Option<crate::expr::BoundExpr>,
    filter_pushdowns: usize,
}

fn build_node(
    plan: BoundLogicalPlan,
    parquet_path: &Path,
    context: &BuildContext,
) -> Result<(Box<dyn BatchOperator>, ExplainNode), ExecutionError> {
    match plan {
        BoundLogicalPlan::Scan { schema } => {
            let scan = ParquetScan::try_new_with_pushdowns(
                parquet_path,
                context.options.scan_batch_rows,
                ScanPushdowns {
                    projection: context.scan_projection.clone(),
                    pruning_predicate: context.pruning_predicate.clone(),
                },
            )?;
            ensure_physical_schema("ParquetScan", &schema, scan.schema())?;
            let mut properties = vec![
                (
                    "source".to_owned(),
                    display_path(parquet_path).to_string_lossy().into_owned(),
                ),
                (
                    "batch_rows".to_owned(),
                    context.options.scan_batch_rows.to_string(),
                ),
            ];
            if context.options.mode == PlanningMode::Optimized {
                properties.extend([
                    (
                        "columns".to_owned(),
                        format!("{}/{}", scan.projected_columns(), scan.source_columns()),
                    ),
                    (
                        "row_groups".to_owned(),
                        format!("{}/{}", scan.selected_row_groups(), scan.total_row_groups()),
                    ),
                    (
                        "pruned_row_groups".to_owned(),
                        (scan.total_row_groups() - scan.selected_row_groups()).to_string(),
                    ),
                ]);
            }
            let explain = ExplainNode {
                operator: PhysicalOperatorKind::ParquetScan,
                schema,
                properties,
                children: Vec::new(),
            };
            Ok((Box::new(scan), explain))
        }
        BoundLogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => {
            let predicate_type = predicate.result;
            let (input, child) = build_node(*input, parquet_path, context)?;
            let operator = FilterExec::try_new(input, predicate)?;
            ensure_physical_schema("FilterExec", &schema, operator.schema())?;
            let explain = ExplainNode {
                operator: PhysicalOperatorKind::FilterExec,
                schema,
                properties: {
                    let mut properties = vec![(
                        "predicate".to_owned(),
                        format!(
                            "{}{}",
                            predicate_type.data_type,
                            if predicate_type.nullable { "?" } else { "!" }
                        ),
                    )];
                    if context.options.mode == PlanningMode::Optimized {
                        properties.push(("residual".to_owned(), "true".to_owned()));
                    }
                    properties
                },
                children: vec![child],
            };
            Ok((Box::new(operator), explain))
        }
        BoundLogicalPlan::Project {
            input,
            expressions,
            schema,
        } => {
            let expression_count = expressions.len();
            let (input, child) = build_node(*input, parquet_path, context)?;
            let operator = ProjectExec::new(input, expressions);
            ensure_physical_schema("ProjectExec", &schema, operator.schema())?;
            let explain = ExplainNode {
                operator: PhysicalOperatorKind::ProjectExec,
                schema,
                properties: vec![("expressions".to_owned(), expression_count.to_string())],
                children: vec![child],
            };
            Ok((Box::new(operator), explain))
        }
        BoundLogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let group_count = group_by.len();
            let aggregate_count = aggregates.len();
            let (input, child) = build_node(*input, parquet_path, context)?;
            let operator = HashAggregateExec::try_new_with_options(
                input,
                group_by,
                aggregates,
                context.options.aggregate,
            )?;
            ensure_physical_schema("HashAggregateExec", &schema, operator.schema())?;
            let explain = ExplainNode {
                operator: PhysicalOperatorKind::HashAggregateExec,
                schema,
                properties: vec![
                    ("groups".to_owned(), group_count.to_string()),
                    ("aggregates".to_owned(), aggregate_count.to_string()),
                    (
                        "state_budget_bytes".to_owned(),
                        context.options.aggregate.state_budget_bytes.to_string(),
                    ),
                    (
                        "output_batch_rows".to_owned(),
                        context.options.aggregate.output_batch_rows.to_string(),
                    ),
                ],
                children: vec![child],
            };
            Ok((Box::new(operator), explain))
        }
    }
}

fn ensure_schema_defs(
    operator: &'static str,
    expected: &SchemaDef,
    actual: &SchemaDef,
) -> Result<(), ExecutionError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ExecutionError::PhysicalSchemaMismatch {
            operator,
            expected: format_schema(expected),
            actual: format_schema(actual),
        })
    }
}

fn ensure_physical_schema(
    operator: &'static str,
    expected: &SchemaDef,
    actual: SchemaRef,
) -> Result<(), ExecutionError> {
    let expected_arrow =
        expected
            .to_arrow()
            .map_err(|error| ExecutionError::InvalidBoundSchema {
                operator,
                reason: error.to_string(),
            })?;
    let fields_match = expected_arrow.fields().len() == actual.fields().len()
        && expected_arrow
            .fields()
            .iter()
            .zip(actual.fields())
            .all(|(expected, actual)| {
                expected.name() == actual.name()
                    && expected.data_type() == actual.data_type()
                    && expected.is_nullable() == actual.is_nullable()
            });
    if fields_match {
        Ok(())
    } else {
        Err(ExecutionError::PhysicalSchemaMismatch {
            operator,
            expected: format_schema(expected),
            actual: format!("{actual:?}"),
        })
    }
}

fn display_path(path: &Path) -> PathBuf {
    path.file_name().map_or_else(
        || path.to_path_buf(),
        |file_name| PathBuf::from(file_name.to_owned()),
    )
}

fn format_schema(schema: &SchemaDef) -> String {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            format!(
                "{}:{}{}",
                field.name,
                field.data_type,
                if field.nullable { "?" } else { "!" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{fields}]")
}
