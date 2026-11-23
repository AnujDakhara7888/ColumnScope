use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;

use columnscope::aggregate::AggregateOptions;
use columnscope::benchmark::FROZEN_WORKLOADS;
use columnscope::measurement::{
    BenchmarkEngine, BenchmarkMode, BenchmarkPlanning, TrialConfig, capture_environment, run_trial,
};

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    if cfg!(debug_assertions) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark trials require a --release binary",
        )
        .into());
    }
    let mut arguments = env::args().skip(1);
    let command = arguments.next().ok_or_else(|| invalid("missing command"))?;
    let values = parse_pairs(arguments.collect())?;
    match command.as_str() {
        "trial" => {
            let workload_id = required(&values, "workload")?.to_ascii_uppercase();
            let workload = FROZEN_WORKLOADS
                .iter()
                .find(|workload| workload.id == workload_id)
                .ok_or_else(|| invalid("workload must be Q01 through Q12"))?;
            let record = run_trial(trial_config(
                &values,
                workload.id.to_owned(),
                workload.plan_json.to_owned(),
            )?)?;
            println!("{}", serde_json::to_string(&record)?);
        }
        "plan-trial" => {
            let plan_json = fs::read_to_string(required(&values, "plan")?)?;
            let record = run_trial(trial_config(
                &values,
                required(&values, "workload")?.to_owned(),
                plan_json,
            )?)?;
            println!("{}", serde_json::to_string(&record)?);
        }
        "environment" => {
            let environment = capture_environment(
                required(&values, "source-sha")?.to_owned(),
                required(&values, "cargo-lock")?,
                required(&values, "dataset-manifest")?,
            )?;
            println!("{}", serde_json::to_string_pretty(&environment)?);
        }
        _ => {
            return Err(invalid("command must be `trial`, `plan-trial`, or `environment`").into());
        }
    }
    Ok(())
}

fn trial_config(
    values: &HashMap<String, String>,
    workload: String,
    plan_json: String,
) -> Result<TrialConfig, Box<dyn Error + Send + Sync>> {
    Ok(TrialConfig {
        workload,
        plan_json,
        input: PathBuf::from(required(values, "input")?),
        engine: required(values, "engine")?.parse::<BenchmarkEngine>()?,
        mode: required(values, "mode")?.parse::<BenchmarkMode>()?,
        planning: required(values, "planning")?.parse::<BenchmarkPlanning>()?,
        layout: required(values, "layout")?.to_owned(),
        batch_size: parse_usize(values, "batch-size")?,
        threads: parse_usize(values, "threads")?,
        trial: parse_usize(values, "trial")?,
        warmup: required(values, "warmup")?.parse::<bool>()?,
        source_sha: required(values, "source-sha")?.to_owned(),
        dataset_checksum: required(values, "dataset-checksum")?.to_owned(),
        aggregate: AggregateOptions {
            state_budget_bytes: parse_optional_usize(
                values,
                "state-budget-bytes",
                AggregateOptions::default().state_budget_bytes,
            )?,
            output_batch_rows: parse_optional_usize(
                values,
                "output-batch-rows",
                AggregateOptions::default().output_batch_rows,
            )?,
        },
    })
}

fn parse_pairs(arguments: Vec<String>) -> Result<HashMap<String, String>, io::Error> {
    if !arguments.len().is_multiple_of(2) {
        return Err(invalid("arguments must be --name value pairs"));
    }
    let mut values = HashMap::new();
    for pair in arguments.chunks_exact(2) {
        let name = pair[0]
            .strip_prefix("--")
            .ok_or_else(|| invalid("argument names must start with --"))?;
        if values.insert(name.to_owned(), pair[1].clone()).is_some() {
            return Err(invalid(&format!("duplicate argument --{name}")));
        }
    }
    Ok(values)
}

fn required<'a>(values: &'a HashMap<String, String>, name: &str) -> Result<&'a str, io::Error> {
    values
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| invalid(&format!("missing --{name}")))
}

fn parse_usize(values: &HashMap<String, String>, name: &str) -> Result<usize, io::Error> {
    required(values, name)?
        .parse()
        .map_err(|_| invalid(&format!("--{name} must be a nonnegative integer")))
}

fn parse_optional_usize(
    values: &HashMap<String, String>,
    name: &str,
    default: usize,
) -> Result<usize, io::Error> {
    values.get(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| invalid(&format!("--{name} must be a nonnegative integer")))
    })
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
