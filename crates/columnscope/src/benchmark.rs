//! Deterministic data generation for the frozen benchmark workloads.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::BoxError;

pub const BENCHMARK_FORMAT_VERSION: u32 = 1;
pub const BENCHMARK_ROW_COUNT: usize = 1_000_000;
pub const BENCHMARK_SEED: u64 = 42;
pub const BENCHMARK_BATCH_ROWS: usize = 8_192;
pub const BENCHMARK_ROW_GROUP_ROWS: usize = 65_536;
pub const BENCHMARK_FILE_COUNT: usize = 1;

const CUSTOMER_COUNT: u64 = 100_000;
const SELECTIVITY_CARDINALITY: u64 = 10_000;
const NULL_DENOMINATOR: u64 = 20;
const REGIONS: [&str; 8] = [
    "north", "south", "east", "west", "central", "coastal", "mountain", "remote",
];
const REGION_THRESHOLDS: [u64; 8] = [35, 55, 70, 80, 88, 94, 98, 100];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrozenWorkload {
    pub id: &'static str,
    pub plan_json: &'static str,
    pub oracle_sql: &'static str,
}

macro_rules! workload {
    ($id:literal, $file:literal) => {
        FrozenWorkload {
            id: $id,
            plan_json: include_str!(concat!("../../../benchmarks/workloads/", $file, ".json")),
            oracle_sql: include_str!(concat!("../../../benchmarks/oracle/", $file, ".sql")),
        }
    };
}

pub const FROZEN_WORKLOADS: [FrozenWorkload; 12] = [
    workload!("Q01", "q01"),
    workload!("Q02", "q02"),
    workload!("Q03", "q03"),
    workload!("Q04", "q04"),
    workload!("Q05", "q05"),
    workload!("Q06", "q06"),
    workload!("Q07", "q07"),
    workload!("Q08", "q08"),
    workload!("Q09", "q09"),
    workload!("Q10", "q10"),
    workload!("Q11", "q11"),
    workload!("Q12", "q12"),
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub format_version: u32,
    pub row_count: usize,
    pub seed: u64,
    pub file_count: usize,
    pub batch_rows: usize,
    pub row_group_rows: usize,
    pub compression: String,
    pub customer_cardinality: u64,
    pub selectivity_cardinality: u64,
    pub nullable_probability: String,
    pub region_weights_percent: Vec<u64>,
    pub logical_rows_sha256: String,
    pub layouts: Vec<DatasetLayoutManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLayoutManifest {
    pub name: String,
    pub file: String,
    pub sha256: String,
    pub byte_size: u64,
    pub row_groups: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutValidation {
    pub row_count: usize,
    pub customer_cardinality: usize,
    pub region_cardinality: usize,
    pub category_cardinality: usize,
    pub logical_rows_sha256: String,
    pub selectivity_key_monotonic: bool,
}

impl DatasetManifest {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let mut json = serde_json::to_string_pretty(self)?;
        json.push('\n');
        Ok(json)
    }
}

/// Generates both physical layouts and a manifest in `output_directory`.
pub fn generate_benchmark_datasets(
    output_directory: impl AsRef<Path>,
) -> Result<DatasetManifest, BoxError> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;

    let mut random_ids = (0..BENCHMARK_ROW_COUNT as u64).collect::<Vec<_>>();
    shuffle(&mut random_ids, BENCHMARK_SEED);
    let clustered_ids = clustered_ids();

    let random = write_layout(output_directory, "random", &random_ids)?;
    let clustered = write_layout(output_directory, "clustered", &clustered_ids)?;
    let manifest = DatasetManifest {
        format_version: BENCHMARK_FORMAT_VERSION,
        row_count: BENCHMARK_ROW_COUNT,
        seed: BENCHMARK_SEED,
        file_count: BENCHMARK_FILE_COUNT,
        batch_rows: BENCHMARK_BATCH_ROWS,
        row_group_rows: BENCHMARK_ROW_GROUP_ROWS,
        compression: "snappy".to_owned(),
        customer_cardinality: CUSTOMER_COUNT,
        selectivity_cardinality: SELECTIVITY_CARDINALITY,
        nullable_probability: "1/20".to_owned(),
        region_weights_percent: vec![35, 20, 15, 10, 8, 6, 4, 2],
        logical_rows_sha256: logical_rows_sha256(BENCHMARK_ROW_COUNT),
        layouts: vec![random, clustered],
    };
    fs::write(
        output_directory.join("dataset_manifest.json"),
        manifest.to_json()?,
    )?;
    Ok(manifest)
}

pub fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, true),
        Field::new("metric", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("selectivity_key", DataType::Int64, false),
    ]))
}

/// Validates a generated layout and computes its order-independent logical identity.
pub fn validate_benchmark_layout(path: impl AsRef<Path>) -> Result<LayoutValidation, BoxError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(BENCHMARK_BATCH_ROWS)
        .build()?;
    let mut digests = vec![None; BENCHMARK_ROW_COUNT];
    let mut customers = vec![false; CUSTOMER_COUNT as usize];
    let mut regions = [false; REGIONS.len()];
    let mut categories = [false; 32];
    let mut row_count = 0usize;
    let mut previous_key = None;
    let mut selectivity_key_monotonic = true;
    for batch in reader {
        let batch = batch?;
        if batch.schema() != benchmark_schema() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "benchmark layout schema does not match the frozen schema",
            )
            .into());
        }
        for row in 0..batch.num_rows() {
            let decoded = DecodedBenchmarkRow::at(&batch, row);
            let id = usize::try_from(decoded.id)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative benchmark id"))?;
            if id >= BENCHMARK_ROW_COUNT || digests[id].is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "benchmark ids must be unique and within the frozen range",
                )
                .into());
            }
            if decoded.selectivity_key != decoded.id % SELECTIVITY_CARDINALITY as i64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "selectivity_key does not match id modulo 10,000",
                )
                .into());
            }
            let customer = usize::try_from(decoded.customer_id)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative customer_id"))?;
            if customer >= customers.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "customer_id is outside the frozen 100,000-value domain",
                )
                .into());
            }
            customers[customer] = true;
            let region = REGIONS
                .iter()
                .position(|region| *region == decoded.region)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "region is outside the frozen domain",
                    )
                })?;
            regions[region] = true;
            let category = decoded
                .category
                .strip_prefix("category_")
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value < categories.len())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "category is outside the frozen 32-value domain",
                    )
                })?;
            categories[category] = true;
            if decoded
                .amount_cents
                .is_some_and(|value| value.abs() > 100_000)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "amount_cents is outside the frozen bounded range",
                )
                .into());
            }
            if decoded.metric.is_some_and(|value| !value.is_finite()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric contains a non-finite value",
                )
                .into());
            }
            if previous_key.is_some_and(|previous| decoded.selectivity_key < previous) {
                selectivity_key_monotonic = false;
            }
            previous_key = Some(decoded.selectivity_key);
            digests[id] = Some(decoded.digest());
            row_count += 1;
        }
    }
    if row_count != BENCHMARK_ROW_COUNT || digests.iter().any(Option::is_none) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "benchmark layout does not contain exactly one million unique rows",
        )
        .into());
    }
    let mut logical = Sha256::new();
    for digest in digests {
        logical.update(digest.expect("all ids checked above"));
    }
    Ok(LayoutValidation {
        row_count,
        customer_cardinality: customers.into_iter().filter(|present| *present).count(),
        region_cardinality: regions.into_iter().filter(|present| *present).count(),
        category_cardinality: categories.into_iter().filter(|present| *present).count(),
        logical_rows_sha256: hex_digest(logical.finalize()),
        selectivity_key_monotonic,
    })
}

fn write_layout(
    output_directory: &Path,
    name: &str,
    ids: &[u64],
) -> Result<DatasetLayoutManifest, BoxError> {
    let path = output_directory.join(format!("{name}.parquet"));
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(BENCHMARK_ROW_GROUP_ROWS))
        .build();
    let file = File::create(&path)?;
    let schema = benchmark_schema();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))?;
    for chunk in ids.chunks(BENCHMARK_BATCH_ROWS) {
        writer.write(&batch_for_ids(schema.clone(), chunk)?)?;
    }
    let metadata = writer.close()?;
    let byte_size = fs::metadata(&path)?.len();
    Ok(DatasetLayoutManifest {
        name: name.to_owned(),
        file: path
            .file_name()
            .expect("layout path has a file name")
            .to_string_lossy()
            .into_owned(),
        sha256: file_sha256(&path)?,
        byte_size,
        row_groups: metadata.num_row_groups(),
    })
}

fn batch_for_ids(schema: SchemaRef, ids: &[u64]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let rows = ids
        .iter()
        .copied()
        .map(BenchmarkRow::new)
        .collect::<Vec<_>>();
    let id = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    let customer_id = rows.iter().map(|row| row.customer_id).collect::<Vec<_>>();
    let region = rows.iter().map(|row| row.region).collect::<Vec<_>>();
    let category = rows
        .iter()
        .map(|row| row.category.as_str())
        .collect::<Vec<_>>();
    let amount_cents = rows.iter().map(|row| row.amount_cents).collect::<Vec<_>>();
    let metric = rows.iter().map(|row| row.metric).collect::<Vec<_>>();
    let active = rows.iter().map(|row| row.active).collect::<Vec<_>>();
    let selectivity_key = rows
        .iter()
        .map(|row| row.selectivity_key)
        .collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(id)),
        Arc::new(Int64Array::from(customer_id)),
        Arc::new(StringArray::from(region)),
        Arc::new(StringArray::from(category)),
        Arc::new(Int64Array::from(amount_cents)),
        Arc::new(Float64Array::from(metric)),
        Arc::new(BooleanArray::from(active)),
        Arc::new(Int64Array::from(selectivity_key)),
    ];
    RecordBatch::try_new(schema, columns)
}

#[derive(Debug)]
struct BenchmarkRow {
    id: i64,
    customer_id: i64,
    region: &'static str,
    category: String,
    amount_cents: Option<i64>,
    metric: Option<f64>,
    active: Option<bool>,
    selectivity_key: i64,
}

impl BenchmarkRow {
    fn new(id: u64) -> Self {
        let region_bucket = mixed(id, 1) % 100;
        let region_index = REGION_THRESHOLDS
            .iter()
            .position(|threshold| region_bucket < *threshold)
            .expect("final region threshold covers every bucket");
        let amount_hash = mixed(id, 4);
        let metric_hash = mixed(id, 5);
        let active_hash = mixed(id, 6);
        let amount_cents = if amount_hash.is_multiple_of(NULL_DENOMINATOR) {
            None
        } else {
            Some((amount_hash % 200_001) as i64 - 100_000)
        };
        let metric = if metric_hash.is_multiple_of(NULL_DENOMINATOR) {
            None
        } else {
            Some(((metric_hash % 2_000_001) as i64 - 1_000_000) as f64 / 1_000.0)
        };
        let active = if active_hash.is_multiple_of(NULL_DENOMINATOR) {
            None
        } else {
            Some(!(active_hash / NULL_DENOMINATOR).is_multiple_of(5))
        };
        Self {
            id: id as i64,
            customer_id: ((id.wrapping_mul(48_271) + BENCHMARK_SEED) % CUSTOMER_COUNT) as i64,
            region: REGIONS[region_index],
            category: format!("category_{:02}", mixed(id, 2) % 32),
            amount_cents,
            metric,
            active,
            selectivity_key: (id % SELECTIVITY_CARDINALITY) as i64,
        }
    }

    fn digest(&self) -> [u8; 32] {
        digest_fields(
            self.id,
            self.customer_id,
            self.region,
            &self.category,
            self.amount_cents,
            self.metric,
            self.active,
            self.selectivity_key,
        )
    }
}

struct DecodedBenchmarkRow<'a> {
    id: i64,
    customer_id: i64,
    region: &'a str,
    category: &'a str,
    amount_cents: Option<i64>,
    metric: Option<f64>,
    active: Option<bool>,
    selectivity_key: i64,
}

impl<'a> DecodedBenchmarkRow<'a> {
    fn at(batch: &'a RecordBatch, row: usize) -> Self {
        use arrow::array::Array;

        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let customer_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let region = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let category = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amount = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let metric = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let active = batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let key = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        Self {
            id: id.value(row),
            customer_id: customer_id.value(row),
            region: region.value(row),
            category: category.value(row),
            amount_cents: (!amount.is_null(row)).then(|| amount.value(row)),
            metric: (!metric.is_null(row)).then(|| metric.value(row)),
            active: (!active.is_null(row)).then(|| active.value(row)),
            selectivity_key: key.value(row),
        }
    }

    fn digest(&self) -> [u8; 32] {
        digest_fields(
            self.id,
            self.customer_id,
            self.region,
            self.category,
            self.amount_cents,
            self.metric,
            self.active,
            self.selectivity_key,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn digest_fields(
    id: i64,
    customer_id: i64,
    region: &str,
    category: &str,
    amount_cents: Option<i64>,
    metric: Option<f64>,
    active: Option<bool>,
    selectivity_key: i64,
) -> [u8; 32] {
    let mut checksum = Sha256::new();
    checksum.update(id.to_le_bytes());
    checksum.update(customer_id.to_le_bytes());
    checksum.update((region.len() as u64).to_le_bytes());
    checksum.update(region.as_bytes());
    checksum.update((category.len() as u64).to_le_bytes());
    checksum.update(category.as_bytes());
    update_optional_i64(&mut checksum, amount_cents);
    match metric {
        Some(value) => {
            checksum.update([1]);
            checksum.update(value.to_bits().to_le_bytes());
        }
        None => checksum.update([0]),
    }
    match active {
        Some(value) => checksum.update([1, u8::from(value)]),
        None => checksum.update([0]),
    }
    checksum.update(selectivity_key.to_le_bytes());
    checksum.finalize().into()
}

fn update_optional_i64(checksum: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            checksum.update([1]);
            checksum.update(value.to_le_bytes());
        }
        None => checksum.update([0]),
    }
}

fn logical_rows_sha256(row_count: usize) -> String {
    let mut checksum = Sha256::new();
    for id in 0..row_count as u64 {
        checksum.update(BenchmarkRow::new(id).digest());
    }
    hex_digest(checksum.finalize())
}

fn file_sha256(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut checksum = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        checksum.update(&buffer[..bytes]);
    }
    Ok(hex_digest(checksum.finalize()))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn clustered_ids() -> Vec<u64> {
    let mut ids = Vec::with_capacity(BENCHMARK_ROW_COUNT);
    for key in 0..SELECTIVITY_CARDINALITY {
        for occurrence in 0..BENCHMARK_ROW_COUNT as u64 / SELECTIVITY_CARDINALITY {
            ids.push(occurrence * SELECTIVITY_CARDINALITY + key);
        }
    }
    ids
}

fn shuffle(values: &mut [u64], seed: u64) {
    let mut state = seed;
    for index in (1..values.len()).rev() {
        let selected = (splitmix64(&mut state) % (index as u64 + 1)) as usize;
        values.swap(index, selected);
    }
}

fn mixed(id: u64, domain: u64) -> u64 {
    let mut state = BENCHMARK_SEED
        ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ domain.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    splitmix64(&mut state)
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub fn manifest_path(output_directory: impl AsRef<Path>) -> PathBuf {
    output_directory.as_ref().join("dataset_manifest.json")
}
