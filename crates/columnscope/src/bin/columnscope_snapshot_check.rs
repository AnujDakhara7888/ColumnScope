use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;

use columnscope::execution::BatchOperator;
use columnscope::snapshot::SnapshotDescriptor;
use columnscope::snapshot_scan::{SnapshotScan, verify_snapshot_checksums};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct SnapshotCheck {
    format_version: u32,
    table_id: String,
    version: u64,
    commit_id: String,
    files: usize,
    expected_rows: u64,
    scanned_rows: u64,
    output_batches: usize,
    checksum_verification: &'static str,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let values = parse_pairs(env::args().skip(1).collect())?;
    let storage_root = PathBuf::from(required(&values, "storage-root")?);
    let descriptor_path = PathBuf::from(required(&values, "descriptor")?);
    let batch_size = parse_usize(&values, "batch-size")?;
    let verify_checksums = required(&values, "verify-checksums")?.parse::<bool>()?;
    let descriptor = SnapshotDescriptor::from_json(&fs::read_to_string(descriptor_path)?)?;

    if verify_checksums {
        verify_snapshot_checksums(&storage_root, &descriptor)?;
    }
    let expected_rows = descriptor.files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.row_count)
            .ok_or_else(|| invalid("snapshot row count overflowed"))
    })?;
    let mut scan = SnapshotScan::try_new(storage_root, &descriptor, batch_size)?;
    let mut scanned_rows = 0_u64;
    while let Some(batch) = scan.next_batch()? {
        scanned_rows = scanned_rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| invalid("scanned row count overflowed"))?;
    }
    if scanned_rows != expected_rows {
        return Err(invalid(&format!(
            "snapshot produced {scanned_rows} rows, expected {expected_rows}"
        ))
        .into());
    }
    let record = SnapshotCheck {
        format_version: 1,
        table_id: descriptor.table_id,
        version: descriptor.version,
        commit_id: descriptor.commit_id,
        files: descriptor.files.len(),
        expected_rows,
        scanned_rows,
        output_batches: scan.metrics().output_batches,
        checksum_verification: if verify_checksums {
            "performed"
        } else {
            "skipped"
        },
    };
    println!("{}", serde_json::to_string_pretty(&record)?);
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

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
