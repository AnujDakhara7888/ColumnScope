//! Fresh-process benchmark trials, result consumption, and provenance capture.

use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::time::Instant;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::aggregate::{AggregateOptions, HashAggregateExec};
use crate::benchmark::{BENCHMARK_SEED, DatasetManifest};
use crate::bind::bind_query;
use crate::error::ExecutionError;
use crate::execution::{BatchOperator, OperatorMetrics, OperatorState};
use crate::expr::ScalarValue;
use crate::logical::{BoundLogicalPlan, BoundQueryPlan, QueryPlan};
use crate::operators::{FilterExec, ProjectExec};
use crate::planner::{PlannerOptions, PlanningMode, plan_query};
use crate::scalar::{ScalarAggregate, ScalarFilterProject, ScalarMetrics};
use crate::scan::ParquetScan;
use crate::{BoxError, read_parquet_batches};

pub const TRIAL_FORMAT_VERSION: u32 = 1;
pub const ENVIRONMENT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkEngine {
    Batch,
    Scalar,
}

impl FromStr for BenchmarkEngine {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "batch" => Ok(Self::Batch),
            "scalar" => Ok(Self::Scalar),
            _ => Err(invalid_input("engine must be `batch` or `scalar`")),
        }
    }
}

impl fmt::Display for BenchmarkEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch => formatter.write_str("batch"),
            Self::Scalar => formatter.write_str("scalar"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkMode {
    ExecutionOnly,
    EndToEnd,
}

impl FromStr for BenchmarkMode {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "execution_only" => Ok(Self::ExecutionOnly),
            "end_to_end" => Ok(Self::EndToEnd),
            _ => Err(invalid_input(
                "mode must be `execution_only` or `end_to_end`",
            )),
        }
    }
}

impl fmt::Display for BenchmarkMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionOnly => formatter.write_str("execution_only"),
            Self::EndToEnd => formatter.write_str("end_to_end"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkPlanning {
    Unoptimized,
    ProjectionOnly,
    FilterPushdown,
    Optimized,
}

impl BenchmarkPlanning {
    fn planner_mode(self) -> PlanningMode {
        match self {
            Self::Unoptimized => PlanningMode::Unoptimized,
            Self::ProjectionOnly => PlanningMode::ProjectionOnly,
            Self::FilterPushdown => PlanningMode::FilterPushdown,
            Self::Optimized => PlanningMode::Optimized,
        }
    }
}

impl FromStr for BenchmarkPlanning {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unoptimized" => Ok(Self::Unoptimized),
            "projection_only" => Ok(Self::ProjectionOnly),
            "filter_pushdown" => Ok(Self::FilterPushdown),
            "optimized" => Ok(Self::Optimized),
            _ => Err(invalid_input(
                "planning must be `unoptimized`, `projection_only`, `filter_pushdown`, or `optimized`",
            )),
        }
    }
}

impl fmt::Display for BenchmarkPlanning {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.planner_mode().fmt(formatter)
    }
}

#[derive(Debug, Clone)]
pub struct TrialConfig {
    pub workload: String,
    pub plan_json: String,
    pub input: PathBuf,
    pub engine: BenchmarkEngine,
    pub mode: BenchmarkMode,
    pub planning: BenchmarkPlanning,
    pub layout: String,
    pub batch_size: usize,
    pub threads: usize,
    pub trial: usize,
    pub warmup: bool,
    pub source_sha: String,
    pub dataset_checksum: String,
    pub aggregate: AggregateOptions,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrialCounters {
    pub pull_calls: usize,
    pub input_batches: usize,
    pub input_rows: usize,
    pub output_batches: usize,
    pub output_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrialRecord {
    pub format_version: u32,
    pub workload: String,
    pub mode: BenchmarkMode,
    pub layout: String,
    pub engine: BenchmarkEngine,
    pub planning: BenchmarkPlanning,
    pub flags: String,
    pub batch_size: usize,
    pub threads: usize,
    pub trial: usize,
    pub warmup: bool,
    pub runtime_ns: u128,
    pub peak_rss_bytes: u64,
    pub setup_peak_rss_bytes: u64,
    pub result_rows: usize,
    pub result_checksum: String,
    pub source_sha: String,
    pub seed: u64,
    pub dataset_checksum: String,
    pub process_id: u32,
    pub counters: TrialCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentMetadata {
    pub format_version: u32,
    pub source_sha: String,
    pub package_version: String,
    pub build_profile: String,
    pub operating_system: String,
    pub architecture: String,
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub total_memory_bytes: Option<u64>,
    pub rustc_version: String,
    pub cargo_lock_sha256: String,
    pub dataset_manifest_sha256: String,
    pub dataset_logical_rows_sha256: String,
}

/// Executes one measured configuration. Callers should run this once per process.
pub fn run_trial(config: TrialConfig) -> Result<TrialRecord, BoxError> {
    validate_config(&config)?;
    let parsed = QueryPlan::from_json(&config.plan_json)?;
    let bound = bind_query(parsed)?;

    let (setup_peak_rss_bytes, runtime_ns, peak_rss_bytes, sink, counters) = match config.mode {
        BenchmarkMode::ExecutionOnly => {
            let batches = read_parquet_batches(&config.input, config.batch_size)?;
            let setup_peak = peak_rss_bytes()?;
            let started = Instant::now();
            let (sink, counters) = match config.engine {
                BenchmarkEngine::Batch => run_batch_memory(bound, batches, config.aggregate)?,
                BenchmarkEngine::Scalar => run_scalar_memory(bound, batches, config.aggregate)?,
            };
            let runtime = started.elapsed().as_nanos();
            (setup_peak, runtime, peak_rss_bytes()?, sink, counters)
        }
        BenchmarkMode::EndToEnd => {
            let setup_peak = peak_rss_bytes()?;
            let started = Instant::now();
            let (sink, counters) = match config.engine {
                BenchmarkEngine::Batch => run_batch_parquet(
                    bound,
                    &config.input,
                    config.batch_size,
                    config.aggregate,
                    config.planning,
                )?,
                BenchmarkEngine::Scalar => {
                    run_scalar_parquet(bound, &config.input, config.batch_size, config.aggregate)?
                }
            };
            let runtime = started.elapsed().as_nanos();
            (setup_peak, runtime, peak_rss_bytes()?, sink, counters)
        }
    };

    Ok(TrialRecord {
        format_version: TRIAL_FORMAT_VERSION,
        workload: config.workload,
        mode: config.mode,
        layout: config.layout,
        engine: config.engine,
        planning: config.planning,
        flags: format!("planning={}", config.planning),
        batch_size: config.batch_size,
        threads: config.threads,
        trial: config.trial,
        warmup: config.warmup,
        runtime_ns,
        peak_rss_bytes,
        setup_peak_rss_bytes,
        result_rows: sink.rows,
        result_checksum: sink.finish(),
        source_sha: config.source_sha,
        seed: BENCHMARK_SEED,
        dataset_checksum: config.dataset_checksum,
        process_id: std::process::id(),
        counters,
    })
}

pub fn capture_environment(
    source_sha: String,
    cargo_lock: impl AsRef<Path>,
    dataset_manifest: impl AsRef<Path>,
) -> Result<EnvironmentMetadata, BoxError> {
    validate_sha256("source SHA", &source_sha)?;
    let manifest_json = fs::read_to_string(dataset_manifest.as_ref())?;
    let manifest = DatasetManifest::from_json(&manifest_json)?;
    let rustc_version = command_output("rustc", &["-Vv"])?;
    Ok(EnvironmentMetadata {
        format_version: ENVIRONMENT_FORMAT_VERSION,
        source_sha,
        package_version: env!("CARGO_PKG_VERSION").to_owned(),
        build_profile: if cfg!(debug_assertions) {
            "debug".to_owned()
        } else {
            "release".to_owned()
        },
        operating_system: command_output("uname", &["-sr"])
            .unwrap_or_else(|_| format!("{} {}", std::env::consts::OS, std::env::consts::FAMILY)),
        architecture: std::env::consts::ARCH.to_owned(),
        cpu_model: cpu_model().unwrap_or_else(|| "unknown".to_owned()),
        logical_cpus: std::thread::available_parallelism()?.get(),
        total_memory_bytes: total_memory_bytes(),
        rustc_version,
        cargo_lock_sha256: file_sha256(cargo_lock.as_ref())?,
        dataset_manifest_sha256: file_sha256(dataset_manifest.as_ref())?,
        dataset_logical_rows_sha256: manifest.logical_rows_sha256,
    })
}

fn validate_config(config: &TrialConfig) -> Result<(), BoxError> {
    if config.batch_size == 0 {
        return Err(invalid_input("batch size must be positive").into());
    }
    if config.threads != 1 {
        return Err(invalid_input("version 1 benchmark trials require one thread").into());
    }
    validate_sha256("source SHA", &config.source_sha)?;
    validate_sha256("dataset checksum", &config.dataset_checksum)?;
    if config.engine == BenchmarkEngine::Scalar && config.planning != BenchmarkPlanning::Unoptimized
    {
        return Err(invalid_input("scalar trials do not apply batch optimizer rules").into());
    }
    if config.mode == BenchmarkMode::ExecutionOnly
        && config.planning != BenchmarkPlanning::Unoptimized
    {
        return Err(invalid_input(
            "execution-only trials use predecoded input and unoptimized planning",
        )
        .into());
    }
    Ok(())
}

fn validate_sha256(name: &str, value: &str) -> Result<(), BoxError> {
    if value.len() == 40 && name == "source SHA" && value.bytes().all(is_lower_hex) {
        return Ok(());
    }
    if value.len() == 64 && value.bytes().all(is_lower_hex) {
        return Ok(());
    }
    Err(invalid_input(&format!(
        "{name} must be a lowercase hexadecimal Git or SHA-256 value"
    ))
    .into())
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn run_batch_memory(
    bound: BoundQueryPlan,
    batches: Vec<RecordBatch>,
    aggregate: AggregateOptions,
) -> Result<(ResultSink, TrialCounters), BoxError> {
    let schema = batches
        .first()
        .ok_or_else(|| invalid_input("execution-only benchmark input must contain rows"))?
        .schema();
    let input = Box::new(MemoryInput::new(batches, schema));
    let mut root = build_vector_node(bound.plan, input, aggregate)?;
    consume_batch_operator(root.as_mut())
}

fn run_batch_parquet(
    bound: BoundQueryPlan,
    input: &Path,
    batch_size: usize,
    aggregate: AggregateOptions,
    planning: BenchmarkPlanning,
) -> Result<(ResultSink, TrialCounters), BoxError> {
    let mut query = plan_query(
        bound,
        input,
        PlannerOptions {
            mode: planning.planner_mode(),
            scan_batch_rows: batch_size,
            aggregate,
        },
    )?;
    consume_batch_operator(query.root.as_mut())
}

fn run_scalar_memory(
    bound: BoundQueryPlan,
    batches: Vec<RecordBatch>,
    aggregate: AggregateOptions,
) -> Result<(ResultSink, TrialCounters), BoxError> {
    let schema = batches
        .first()
        .ok_or_else(|| invalid_input("execution-only benchmark input must contain rows"))?
        .schema();
    run_scalar_plan(
        bound.plan,
        Box::new(MemoryInput::new(batches, schema)),
        aggregate,
    )
}

fn run_scalar_parquet(
    bound: BoundQueryPlan,
    input: &Path,
    batch_size: usize,
    aggregate: AggregateOptions,
) -> Result<(ResultSink, TrialCounters), BoxError> {
    let scan = ParquetScan::try_new(input, batch_size)?;
    run_scalar_plan(bound.plan, Box::new(scan), aggregate)
}

fn build_vector_node(
    plan: BoundLogicalPlan,
    input: Box<dyn BatchOperator>,
    aggregate: AggregateOptions,
) -> Result<Box<dyn BatchOperator>, ExecutionError> {
    match plan {
        BoundLogicalPlan::Scan { .. } => Ok(input),
        BoundLogicalPlan::Filter {
            input: child,
            predicate,
            ..
        } => Ok(Box::new(FilterExec::try_new(
            build_vector_node(*child, input, aggregate)?,
            predicate,
        )?)),
        BoundLogicalPlan::Project {
            input: child,
            expressions,
            ..
        } => Ok(Box::new(ProjectExec::new(
            build_vector_node(*child, input, aggregate)?,
            expressions,
        ))),
        BoundLogicalPlan::Aggregate {
            input: child,
            group_by,
            aggregates,
            ..
        } => Ok(Box::new(HashAggregateExec::try_new_with_options(
            build_vector_node(*child, input, aggregate)?,
            group_by,
            aggregates,
            aggregate,
        )?)),
    }
}

fn run_scalar_plan(
    plan: BoundLogicalPlan,
    input: Box<dyn BatchOperator>,
    aggregate_options: AggregateOptions,
) -> Result<(ResultSink, TrialCounters), BoxError> {
    match plan {
        BoundLogicalPlan::Scan { .. } => {
            let mut input = input;
            consume_batch_operator(input.as_mut())
        }
        BoundLogicalPlan::Project {
            input: child,
            expressions,
            ..
        } => {
            let predicate = scalar_input_predicate(*child)?;
            let mut pipeline = ScalarFilterProject::try_new(input, predicate, expressions)?;
            let mut sink = ResultSink::default();
            while let Some(row) = pipeline.next_row()? {
                sink.consume_scalar_row(&row);
            }
            Ok((sink, scalar_counters(pipeline.metrics())))
        }
        BoundLogicalPlan::Aggregate {
            input: child,
            group_by,
            aggregates,
            ..
        } => {
            let predicate = scalar_input_predicate(*child)?;
            let mut aggregation = ScalarAggregate::try_new(
                input,
                predicate,
                group_by,
                aggregates,
                aggregate_options,
            )?;
            let mut sink = ResultSink::default();
            while let Some(row) = aggregation.next_row()? {
                sink.consume_scalar_row(&row);
            }
            Ok((sink, scalar_counters(aggregation.metrics())))
        }
        BoundLogicalPlan::Filter { .. } => Err(invalid_input(
            "frozen scalar workloads require Filter below Project or Aggregate",
        )
        .into()),
    }
}

fn scalar_input_predicate(
    plan: BoundLogicalPlan,
) -> Result<Option<crate::expr::BoundExpr>, BoxError> {
    match plan {
        BoundLogicalPlan::Scan { .. } => Ok(None),
        BoundLogicalPlan::Filter {
            input, predicate, ..
        } if matches!(*input, BoundLogicalPlan::Scan { .. }) => Ok(Some(predicate)),
        _ => Err(invalid_input("unsupported frozen scalar input shape").into()),
    }
}

fn consume_batch_operator(
    operator: &mut dyn BatchOperator,
) -> Result<(ResultSink, TrialCounters), BoxError> {
    let mut sink = ResultSink::default();
    while let Some(batch) = operator.next_batch()? {
        sink.consume_batch(&batch)?;
    }
    let metrics = operator.metrics();
    Ok((
        sink,
        TrialCounters {
            pull_calls: metrics.pull_calls,
            output_batches: metrics.output_batches,
            output_rows: metrics.output_rows,
            ..TrialCounters::default()
        },
    ))
}

fn scalar_counters(metrics: ScalarMetrics) -> TrialCounters {
    TrialCounters {
        pull_calls: metrics.pull_calls,
        input_batches: metrics.input_batches,
        input_rows: metrics.input_rows,
        output_rows: metrics.output_rows,
        ..TrialCounters::default()
    }
}

struct MemoryInput {
    batches: VecDeque<RecordBatch>,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
}

impl MemoryInput {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            batches: batches.into(),
            schema,
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
        }
    }
}

impl BatchOperator for MemoryInput {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        if self.state == OperatorState::Exhausted {
            return Ok(None);
        }
        self.metrics.pull_calls += 1;
        match self.batches.pop_front() {
            Some(batch) => {
                self.metrics.output_batches += 1;
                self.metrics.output_rows += batch.num_rows();
                Ok(Some(batch))
            }
            None => {
                self.state = OperatorState::Exhausted;
                Ok(None)
            }
        }
    }

    fn metrics(&self) -> OperatorMetrics {
        self.metrics
    }

    fn state(&self) -> OperatorState {
        self.state
    }
}

#[derive(Debug, Clone, Default)]
struct ResultSink {
    rows: usize,
    sums: [u64; 4],
    xors: [u64; 4],
}

impl ResultSink {
    fn consume_batch(&mut self, batch: &RecordBatch) -> Result<(), BoxError> {
        for row in 0..batch.num_rows() {
            let mut digest = Sha256::new();
            digest.update((batch.num_columns() as u64).to_le_bytes());
            for column in batch.columns() {
                update_array_value(&mut digest, column.as_ref(), row)?;
            }
            self.consume_digest(digest.finalize().into());
        }
        Ok(())
    }

    fn consume_scalar_row(&mut self, row: &[ScalarValue]) {
        let mut digest = Sha256::new();
        digest.update((row.len() as u64).to_le_bytes());
        for value in row {
            update_scalar_value(&mut digest, value);
        }
        self.consume_digest(digest.finalize().into());
    }

    fn consume_digest(&mut self, digest: [u8; 32]) {
        self.rows += 1;
        for index in 0..4 {
            let start = index * 8;
            let value = u64::from_le_bytes(digest[start..start + 8].try_into().unwrap());
            self.sums[index] = self.sums[index].wrapping_add(value);
            self.xors[index] ^= value;
        }
    }

    fn finish(&self) -> String {
        let mut digest = Sha256::new();
        digest.update((self.rows as u64).to_le_bytes());
        for value in self.sums.into_iter().chain(self.xors) {
            digest.update(value.to_le_bytes());
        }
        hex_digest(digest.finalize())
    }
}

fn update_array_value(digest: &mut Sha256, array: &dyn Array, row: usize) -> Result<(), BoxError> {
    match array.data_type() {
        DataType::Int64 => {
            digest.update([1]);
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            update_optional_i64(digest, (!array.is_null(row)).then(|| array.value(row)));
        }
        DataType::Float64 => {
            digest.update([2]);
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            update_optional_f64(digest, (!array.is_null(row)).then(|| array.value(row)));
        }
        DataType::Boolean => {
            digest.update([3]);
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            update_optional_bool(digest, (!array.is_null(row)).then(|| array.value(row)));
        }
        DataType::Utf8 => {
            digest.update([4]);
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            update_optional_str(digest, (!array.is_null(row)).then(|| array.value(row)));
        }
        data_type => {
            return Err(invalid_input(&format!(
                "benchmark sink does not support Arrow type {data_type}"
            ))
            .into());
        }
    }
    Ok(())
}

fn update_scalar_value(digest: &mut Sha256, value: &ScalarValue) {
    match value {
        ScalarValue::Int64 { value } => {
            digest.update([1]);
            update_optional_i64(digest, *value);
        }
        ScalarValue::Float64 { value } => {
            digest.update([2]);
            update_optional_f64(digest, *value);
        }
        ScalarValue::Boolean { value } => {
            digest.update([3]);
            update_optional_bool(digest, *value);
        }
        ScalarValue::Utf8 { value } => {
            digest.update([4]);
            update_optional_str(digest, value.as_deref());
        }
    }
}

fn update_optional_i64(digest: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_le_bytes());
        }
        None => digest.update([0]),
    }
}

fn update_optional_f64(digest: &mut Sha256, value: Option<f64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_bits().to_le_bytes());
        }
        None => digest.update([0]),
    }
}

fn update_optional_bool(digest: &mut Sha256, value: Option<bool>) {
    match value {
        Some(value) => digest.update([1, u8::from(value)]),
        None => digest.update([0]),
    }
}

fn update_optional_str(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
        None => digest.update([0]),
    }
}

pub fn peak_rss_bytes() -> io::Result<u64> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: `usage` points to writable storage for one `rusage`, and
        // `getrusage` initializes it on a successful return.
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful call above initialized the structure.
        let usage = unsafe { usage.assume_init() };
        #[cfg(target_os = "macos")]
        return Ok(usage.ru_maxrss as u64);
        #[cfg(not(target_os = "macos"))]
        return Ok((usage.ru_maxrss as u64).saturating_mul(1_024));
    }
    #[cfg(not(unix))]
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "peak RSS capture currently supports Unix targets",
    ))
}

fn file_sha256(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        digest.update(&buffer[..bytes]);
    }
    Ok(hex_digest(digest.finalize()))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn command_output(program: &str, arguments: &[&str]) -> io::Result<String> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{program} exited with {}",
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn cpu_model() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        fs::read_to_string("/proc/cpuinfo")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: ").map(str::to_owned))
    }
    #[cfg(target_os = "macos")]
    {
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"]).ok()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    None
}

fn total_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let value = fs::read_to_string("/proc/meminfo")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        Some(value.saturating_mul(1_024))
    }
    #[cfg(target_os = "macos")]
    {
        command_output("sysctl", &["-n", "hw.memsize"])
            .ok()?
            .parse()
            .ok()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    None
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
