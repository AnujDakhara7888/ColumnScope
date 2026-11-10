use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use columnscope::aggregate::AggregateOptions;
use columnscope::bind::bind_query;
use columnscope::error::ExecutionError;
use columnscope::execution::OperatorState;
use columnscope::logical::{BoundLogicalPlan, QueryPlan};
use columnscope::planner::{PlannerOptions, PlanningMode, plan_query};
use columnscope::schema::ScalarType;
use columnscope::write_fixture;
use tempfile::tempdir;

fn fixture_plan(name: &str) -> QueryPlan {
    let json = match name {
        "filter_project" => include_str!("../../../fixtures/plans/filter_project_v1.json"),
        "aggregate" => include_str!("../../../fixtures/plans/aggregate_v1.json"),
        _ => panic!("unknown fixture plan"),
    };
    QueryPlan::from_json(json).expect("parse fixture plan")
}

#[test]
fn unoptimized_planner_preserves_tree_and_executes_on_a_worker() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let bound = bind_query(fixture_plan("filter_project")).expect("bind fixture plan");
    let planned = plan_query(
        bound,
        &path,
        PlannerOptions {
            scan_batch_rows: 2,
            ..PlannerOptions::default()
        },
    )
    .expect("plan physical query");

    assert_eq!(planned.mode, PlanningMode::Unoptimized);
    assert_eq!(
        planned.explain(),
        concat!(
            "mode=unoptimized\n",
            "ProjectExec [expressions=2] schema=[event_id:Int64!, area:Utf8!]\n",
            "  FilterExec [predicate=Boolean?] schema=[id:Int64!, metric:Float64?, active:Boolean?, region:Utf8!]\n",
            "    ParquetScan [source=fixture.parquet, batch_rows=2] schema=[id:Int64!, metric:Float64?, active:Boolean?, region:Utf8!]\n"
        )
    );

    let rows = std::thread::spawn(move || {
        let mut root = planned.root;
        let mut rows = Vec::new();
        while let Some(batch) = root.next_batch().expect("execute planned query") {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let regions = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            rows.extend(
                (0..batch.num_rows()).map(|row| (ids.value(row), regions.value(row).to_owned())),
            );
        }
        assert_eq!(root.state(), OperatorState::Exhausted);
        rows
    })
    .join()
    .expect("worker should not panic");
    assert_eq!(rows, vec![(0, "north".into()), (3, "north".into())]);
}

#[test]
fn aggregate_options_are_lowered_and_explained() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let bound = bind_query(fixture_plan("aggregate")).expect("bind aggregate plan");
    let mut planned = plan_query(
        bound,
        &path,
        PlannerOptions {
            mode: PlanningMode::Unoptimized,
            scan_batch_rows: 1,
            aggregate: AggregateOptions {
                state_budget_bytes: 4_096,
                output_batch_rows: 2,
            },
        },
    )
    .expect("plan aggregate");

    assert!(planned.explain().starts_with(concat!(
        "mode=unoptimized\n",
        "HashAggregateExec [groups=1, aggregates=6, state_budget_bytes=4096, output_batch_rows=2] "
    )));
    assert!(planned.explain().contains(
        "  ParquetScan [source=fixture.parquet, batch_rows=1] schema=[id:Int64!, metric:Float64?, active:Boolean?, region:Utf8!]"
    ));

    let mut batch_sizes = Vec::new();
    while let Some(batch) = planned
        .root
        .next_batch()
        .expect("execute planned aggregate")
    {
        batch_sizes.push(batch.num_rows());
    }
    assert_eq!(batch_sizes, vec![2, 1]);
}

#[test]
fn parquet_schema_mismatch_fails_during_planning() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let mut query = fixture_plan("filter_project");
    query.input_schema.fields[0].nullable = true;
    let bound = bind_query(query).expect("bind altered input contract");

    assert!(matches!(
        plan_query(bound, path, PlannerOptions::default()),
        Err(ExecutionError::PhysicalSchemaMismatch {
            operator: "ParquetScan",
            ..
        })
    ));
}

#[test]
fn intermediate_schema_mismatch_identifies_the_operator() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let mut bound = bind_query(fixture_plan("filter_project")).expect("bind fixture plan");
    let BoundLogicalPlan::Project { input, .. } = &mut bound.plan else {
        panic!("fixture root should be Project")
    };
    let BoundLogicalPlan::Filter { schema, .. } = input.as_mut() else {
        panic!("project input should be Filter")
    };
    schema.fields[0].nullable = true;

    assert!(matches!(
        plan_query(bound, path, PlannerOptions::default()),
        Err(ExecutionError::PhysicalSchemaMismatch {
            operator: "FilterExec",
            ..
        })
    ));
}

#[test]
fn query_output_schema_is_checked_before_opening_input() {
    let mut bound = bind_query(fixture_plan("filter_project")).expect("bind fixture plan");
    bound.output_schema.fields[0].data_type = ScalarType::Utf8;

    assert!(matches!(
        plan_query(
            bound,
            "/path/that/is/not/opened.parquet",
            PlannerOptions::default()
        ),
        Err(ExecutionError::PhysicalSchemaMismatch {
            operator: "QueryOutput",
            ..
        })
    ));
}

#[test]
fn invalid_scan_batch_size_is_rejected_before_opening_input() {
    let bound = bind_query(fixture_plan("filter_project")).expect("bind fixture plan");
    assert!(matches!(
        plan_query(
            bound,
            "/path/that/is/not/opened.parquet",
            PlannerOptions {
                scan_batch_rows: 0,
                ..PlannerOptions::default()
            }
        ),
        Err(ExecutionError::InvalidBatchSize)
    ));
}

#[test]
fn explain_schema_matches_the_live_root_schema() {
    let directory = tempdir().expect("create temporary directory");
    let path = directory.path().join("fixture.parquet");
    write_fixture(&path).expect("write fixture");
    let bound = bind_query(fixture_plan("filter_project")).expect("bind fixture plan");
    let planned = plan_query(bound, path, PlannerOptions::default()).expect("plan query");
    let explained = Arc::new(
        planned
            .explain_root
            .schema
            .to_arrow()
            .expect("convert explained schema"),
    );
    assert_eq!(planned.root.schema(), explained);
}
