use std::env;
use std::error::Error;
use std::path::PathBuf;

use columnscope::{read_parquet_batches, summarize_batches, write_fixture};

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("columnscope-fixture.parquet"));

    write_fixture(&path)?;
    let batches = read_parquet_batches(&path, 2)?;
    let summary = summarize_batches(&batches)?;

    println!(
        "wrote and decoded {} rows in {} batches ({} columns): {}",
        summary.rows,
        summary.batches,
        summary.columns,
        path.display()
    );
    Ok(())
}
