use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use columnscope::benchmark::DatasetManifest;
use columnscope::measurement::TrialRecord;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    if cfg!(debug_assertions) {
        return Err(invalid("benchmark runner requires a --release binary").into());
    }
    let values = parse_pairs(env::args().skip(1).collect())?;
    let trial_binary = PathBuf::from(required(&values, "trial-binary")?);
    let input = PathBuf::from(required(&values, "input")?);
    let manifest_path = PathBuf::from(required(&values, "dataset-manifest")?);
    let cargo_lock = PathBuf::from(required(&values, "cargo-lock")?);
    let output_directory = PathBuf::from(required(&values, "output-dir")?);
    let layout = required(&values, "layout")?;
    let source_sha = required(&values, "source-sha")?;
    let warmups = parse_usize(&values, "warmups")?;
    let repetitions = parse_usize(&values, "repetitions")?;
    if repetitions == 0 {
        return Err(invalid("--repetitions must be positive").into());
    }

    let manifest_json = fs::read_to_string(&manifest_path)?;
    let manifest = DatasetManifest::from_json(&manifest_json)?;
    let dataset_checksum = manifest
        .layouts
        .iter()
        .find(|candidate| candidate.name == layout)
        .ok_or_else(|| invalid("--layout is absent from the dataset manifest"))?
        .sha256
        .as_str();

    fs::create_dir_all(&output_directory)?;
    let environment = run_child(
        &trial_binary,
        [
            "environment",
            "--source-sha",
            source_sha,
            "--cargo-lock",
            path_text(&cargo_lock)?,
            "--dataset-manifest",
            path_text(&manifest_path)?,
        ],
    )?;
    let environment_json: serde_json::Value = serde_json::from_slice(&environment.stdout)?;
    fs::write(
        output_directory.join("environment.json"),
        format!("{}\n", serde_json::to_string_pretty(&environment_json)?),
    )?;
    fs::write(
        output_directory.join("dataset_manifest.json"),
        manifest_json,
    )?;

    for trial in 0..warmups {
        let record = execute_trial(
            &trial_binary,
            &values,
            &input,
            source_sha,
            dataset_checksum,
            trial,
            true,
        )?;
        if !record.warmup {
            return Err(invalid("warmup child did not identify itself as warmup").into());
        }
    }

    let mut records = Vec::with_capacity(repetitions);
    for trial in 0..repetitions {
        let record = execute_trial(
            &trial_binary,
            &values,
            &input,
            source_sha,
            dataset_checksum,
            trial,
            false,
        )?;
        if record.warmup {
            return Err(invalid("measured child identified itself as warmup").into());
        }
        records.push(record);
    }
    write_csv(output_directory.join("raw_trials.csv"), &records)?;
    println!(
        "wrote {} measured fresh-process trial(s) to {}",
        records.len(),
        output_directory.display()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_trial(
    trial_binary: &Path,
    values: &HashMap<String, String>,
    input: &Path,
    source_sha: &str,
    dataset_checksum: &str,
    trial: usize,
    warmup: bool,
) -> Result<TrialRecord, Box<dyn Error + Send + Sync>> {
    let trial_text = trial.to_string();
    let warmup_text = warmup.to_string();
    let output = run_child(
        trial_binary,
        [
            "trial",
            "--input",
            path_text(input)?,
            "--workload",
            required(values, "workload")?,
            "--engine",
            required(values, "engine")?,
            "--mode",
            required(values, "mode")?,
            "--planning",
            required(values, "planning")?,
            "--layout",
            required(values, "layout")?,
            "--batch-size",
            required(values, "batch-size")?,
            "--threads",
            required(values, "threads")?,
            "--trial",
            &trial_text,
            "--warmup",
            &warmup_text,
            "--source-sha",
            source_sha,
            "--dataset-checksum",
            dataset_checksum,
            "--state-budget-bytes",
            values
                .get("state-budget-bytes")
                .map_or("67108864", String::as_str),
            "--output-batch-rows",
            values
                .get("output-batch-rows")
                .map_or("8192", String::as_str),
        ],
    )?;
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn run_child<'a>(
    binary: &Path,
    arguments: impl IntoIterator<Item = &'a str>,
) -> Result<Output, io::Error> {
    let output = Command::new(binary).args(arguments).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{} exited with {}: {}",
            binary.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

fn write_csv(path: impl AsRef<Path>, records: &[TrialRecord]) -> io::Result<()> {
    let mut file = File::create(path)?;
    writeln!(
        file,
        "workload,mode,layout,engine,flags,batch_size,threads,trial,runtime_ns,peak_rss_bytes,setup_peak_rss_bytes,result_rows,result_checksum,source_sha,seed,dataset_checksum,process_id,pull_calls,input_batches,input_rows,output_batches,operator_output_rows"
    )?;
    for record in records {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.workload,
            record.mode,
            record.layout,
            record.engine,
            record.flags,
            record.batch_size,
            record.threads,
            record.trial,
            record.runtime_ns,
            record.peak_rss_bytes,
            record.setup_peak_rss_bytes,
            record.result_rows,
            record.result_checksum,
            record.source_sha,
            record.seed,
            record.dataset_checksum,
            record.process_id,
            record.counters.pull_calls,
            record.counters.input_batches,
            record.counters.input_rows,
            record.counters.output_batches,
            record.counters.output_rows,
        )?;
    }
    Ok(())
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

fn path_text(path: &Path) -> Result<&str, io::Error> {
    path.to_str()
        .ok_or_else(|| invalid("benchmark paths must be valid UTF-8"))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
