use std::env;
use std::error::Error;
use std::path::PathBuf;

use columnscope::benchmark::generate_benchmark_datasets;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/columnscope/datasets"));
    let manifest = generate_benchmark_datasets(&output)?;
    println!(
        "generated {} rows in {} layouts: {}",
        manifest.row_count,
        manifest.layouts.len(),
        output.display()
    );
    for layout in &manifest.layouts {
        println!(
            "{}: {} bytes, {} row groups, sha256 {}",
            layout.name, layout.byte_size, layout.row_groups, layout.sha256
        );
    }
    println!("logical rows sha256: {}", manifest.logical_rows_sha256);
    Ok(())
}
