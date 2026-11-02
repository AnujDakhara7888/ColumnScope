use columnscope::bind::{bind_expr, bind_query};
use columnscope::error::PlanError;
use columnscope::expr::{BoundExprKind, Expr, NamedExpr, ScalarValue};
use columnscope::logical::{AggregateExpr, BoundLogicalPlan, LogicalPlan, QueryPlan};
use columnscope::schema::{FieldDef, ScalarType, SchemaDef};

const FILTER_PROJECT: &str = include_str!("../../../fixtures/plans/filter_project_v1.json");
const AGGREGATE: &str = include_str!("../../../fixtures/plans/aggregate_v1.json");

#[test]
fn parses_and_binds_filter_projection_with_aliases() {
    let parsed = QueryPlan::from_json(FILTER_PROJECT).expect("parse plan fixture");
    let bound = bind_query(parsed).expect("bind plan fixture");
    assert_eq!(
        bound.output_schema,
        SchemaDef {
            fields: vec![
                FieldDef {
                    name: "event_id".into(),
                    data_type: ScalarType::Int64,
                    nullable: false,
                },
                FieldDef {
                    name: "area".into(),
                    data_type: ScalarType::Utf8,
                    nullable: false,
                },
            ],
        }
    );

    let BoundLogicalPlan::Project { input, .. } = bound.plan else {
        panic!("expected project")
    };
    let BoundLogicalPlan::Filter { predicate, .. } = *input else {
        panic!("expected filter")
    };
    assert_eq!(predicate.result.data_type, ScalarType::Boolean);
    assert!(predicate.result.nullable);
}

#[test]
fn aggregate_fixture_infers_count_and_nullable_numeric_outputs() {
    let bound = bind_query(QueryPlan::from_json(AGGREGATE).unwrap()).unwrap();
    let types = bound
        .output_schema
        .fields
        .iter()
        .map(|field| (field.name.as_str(), field.data_type, field.nullable))
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        vec![
            ("region", ScalarType::Utf8, false),
            ("rows", ScalarType::Int64, false),
            ("metric_count", ScalarType::Int64, false),
            ("metric_sum", ScalarType::Float64, true),
            ("metric_min", ScalarType::Float64, true),
            ("metric_max", ScalarType::Float64, true),
            ("metric_avg", ScalarType::Float64, true),
        ]
    );
}

#[test]
fn rejects_unsupported_plan_version_before_binding() {
    let json = FILTER_PROJECT.replacen("\"format_version\": 1", "\"format_version\": 2", 1);
    assert_eq!(
        QueryPlan::from_json(&json).unwrap_err(),
        PlanError::UnsupportedPlanVersion { found: 2 }
    );
}

#[test]
fn rejects_unknown_json_fields() {
    let json = FILTER_PROJECT.replacen(
        "\"format_version\": 1",
        "\"format_version\": 1, \"extra\": true",
        1,
    );
    assert!(matches!(
        QueryPlan::from_json(&json),
        Err(PlanError::InvalidJson(_))
    ));
}

#[test]
fn rejects_missing_columns_and_implicit_numeric_coercion() {
    let schema = schema();
    assert_eq!(
        bind_expr(
            Expr::Column {
                name: "missing".into()
            },
            &schema
        )
        .unwrap_err(),
        PlanError::UnknownColumn("missing".into())
    );

    let comparison = Expr::Eq {
        left: Box::new(Expr::Column { name: "id".into() }),
        right: Box::new(Expr::Literal {
            literal: ScalarValue::Float64 { value: Some(1.0) },
        }),
    };
    assert!(matches!(
        bind_expr(comparison, &schema),
        Err(PlanError::TypeMismatch { .. })
    ));
}

#[test]
fn null_tests_are_non_nullable_booleans() {
    let bound = bind_expr(
        Expr::IsNull {
            expr: Box::new(Expr::Column {
                name: "metric".into(),
            }),
        },
        &schema(),
    )
    .unwrap();
    assert_eq!(bound.result.data_type, ScalarType::Boolean);
    assert!(!bound.result.nullable);
    assert!(matches!(bound.kind, BoundExprKind::IsNull(_)));
}

#[test]
fn rejects_non_boolean_filter_and_duplicate_projection_aliases() {
    let non_boolean = QueryPlan {
        format_version: 1,
        input_schema: schema(),
        plan: LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan),
            predicate: Expr::Column { name: "id".into() },
        },
    };
    assert!(matches!(
        bind_query(non_boolean),
        Err(PlanError::InvalidType {
            operator: "filter predicate",
            ..
        })
    ));

    let json = FILTER_PROJECT.replacen("\"alias\": \"area\"", "\"alias\": \"event_id\"", 1);
    assert_eq!(
        bind_query(QueryPlan::from_json(&json).unwrap()).unwrap_err(),
        PlanError::DuplicateOutput("event_id".into())
    );
}

#[test]
fn rejects_nonfinite_programmatic_float_literal() {
    let error = bind_expr(
        Expr::Literal {
            literal: ScalarValue::Float64 {
                value: Some(f64::INFINITY),
            },
        },
        &schema(),
    )
    .unwrap_err();
    assert_eq!(error, PlanError::NonFiniteFloat);
}

#[test]
fn binds_the_complete_boolean_and_comparison_grammar() {
    let column = || Expr::Column { name: "id".into() };
    let literal = || Expr::Literal {
        literal: ScalarValue::Int64 { value: Some(2) },
    };
    let comparisons = vec![
        Expr::Eq {
            left: Box::new(column()),
            right: Box::new(literal()),
        },
        Expr::NotEq {
            left: Box::new(column()),
            right: Box::new(literal()),
        },
        Expr::Lt {
            left: Box::new(column()),
            right: Box::new(literal()),
        },
        Expr::LtEq {
            left: Box::new(column()),
            right: Box::new(literal()),
        },
        Expr::Gt {
            left: Box::new(column()),
            right: Box::new(literal()),
        },
        Expr::GtEq {
            left: Box::new(column()),
            right: Box::new(literal()),
        },
    ];
    for expression in comparisons {
        let bound = bind_expr(expression, &schema()).unwrap();
        assert_eq!(bound.result.data_type, ScalarType::Boolean);
        assert!(!bound.result.nullable);
    }

    let boolean = || Expr::Column {
        name: "active".into(),
    };
    let true_literal = || Expr::Literal {
        literal: ScalarValue::Boolean { value: Some(true) },
    };
    for expression in [
        Expr::And {
            left: Box::new(boolean()),
            right: Box::new(true_literal()),
        },
        Expr::Or {
            left: Box::new(boolean()),
            right: Box::new(true_literal()),
        },
        Expr::Not {
            expr: Box::new(boolean()),
        },
        Expr::IsNotNull {
            expr: Box::new(boolean()),
        },
    ] {
        assert_eq!(
            bind_expr(expression, &schema()).unwrap().result.data_type,
            ScalarType::Boolean
        );
    }

    let typed_null = bind_expr(
        Expr::Literal {
            literal: ScalarValue::Utf8 { value: None },
        },
        &schema(),
    )
    .unwrap();
    assert_eq!(typed_null.result.data_type, ScalarType::Utf8);
    assert!(typed_null.result.nullable);
}

#[test]
fn rejects_duplicate_schema_fields_and_excess_group_keys() {
    let mut duplicate_schema = schema();
    duplicate_schema
        .fields
        .push(duplicate_schema.fields[0].clone());
    assert_eq!(
        bind_query(QueryPlan {
            format_version: 1,
            input_schema: duplicate_schema,
            plan: LogicalPlan::Scan,
        })
        .unwrap_err(),
        PlanError::DuplicateField("id".into())
    );

    let group_by = ["first", "second", "third"]
        .into_iter()
        .map(|alias| NamedExpr {
            alias: alias.into(),
            expr: Expr::Column { name: "id".into() },
        })
        .collect();
    let plan = QueryPlan {
        format_version: 1,
        input_schema: schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by,
            aggregates: vec![AggregateExpr::CountStar {
                alias: "rows".into(),
            }],
        },
    };
    assert_eq!(
        bind_query(plan).unwrap_err(),
        PlanError::TooManyGroupKeys(3)
    );
}

#[test]
fn rejects_non_numeric_sum_and_float_group_keys() {
    let sum = QueryPlan {
        format_version: 1,
        input_schema: schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![],
            aggregates: vec![AggregateExpr::Sum {
                alias: "sum".into(),
                expr: Expr::Column {
                    name: "region".into(),
                },
            }],
        },
    };
    assert!(matches!(
        bind_query(sum),
        Err(PlanError::InvalidType {
            operator: "aggregate",
            ..
        })
    ));

    let group = QueryPlan {
        format_version: 1,
        input_schema: schema(),
        plan: LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan),
            group_by: vec![NamedExpr {
                alias: "metric".into(),
                expr: Expr::Column {
                    name: "metric".into(),
                },
            }],
            aggregates: vec![],
        },
    };
    assert!(matches!(
        bind_query(group),
        Err(PlanError::InvalidType {
            operator: "group key",
            ..
        })
    ));
}

fn schema() -> SchemaDef {
    SchemaDef {
        fields: vec![
            FieldDef {
                name: "id".into(),
                data_type: ScalarType::Int64,
                nullable: false,
            },
            FieldDef {
                name: "metric".into(),
                data_type: ScalarType::Float64,
                nullable: true,
            },
            FieldDef {
                name: "active".into(),
                data_type: ScalarType::Boolean,
                nullable: true,
            },
            FieldDef {
                name: "region".into(),
                data_type: ScalarType::Utf8,
                nullable: false,
            },
        ],
    }
}
