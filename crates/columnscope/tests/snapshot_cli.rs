use std::fs;
use std::process::Command;

use columnscope::snapshot::SnapshotDescriptor;
use tempfile::tempdir;

const DESCRIPTOR: &str = include_str!("../../../fixtures/snapshots/descriptor_v1.json");

#[test]
fn snapshot_check_reports_an_empty_pinned_version() {
    let directory = tempdir().expect("create snapshot root");
    let descriptor_path = directory.path().join("descriptor.json");
    let mut descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    descriptor.version = 0;
    descriptor.commit_id = "commit-0000".to_owned();
    descriptor.files.clear();
    fs::write(
        &descriptor_path,
        serde_json::to_vec_pretty(&descriptor).unwrap(),
    )
    .expect("write descriptor");

    let output = Command::new(env!("CARGO_BIN_EXE_columnscope-snapshot-check"))
        .args([
            "--storage-root",
            directory.path().to_str().unwrap(),
            "--descriptor",
            descriptor_path.to_str().unwrap(),
            "--batch-size",
            "8192",
            "--verify-checksums",
            "true",
        ])
        .output()
        .expect("run snapshot checker");

    assert!(
        output.status.success(),
        "snapshot checker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let record: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(record["version"], 0);
    assert_eq!(record["files"], 0);
    assert_eq!(record["expected_rows"], 0);
    assert_eq!(record["scanned_rows"], 0);
    assert_eq!(record["checksum_verification"], "performed");
}
