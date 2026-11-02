use columnscope::error::SnapshotError;
use columnscope::snapshot::{SnapshotDescriptor, canonical_schema_json, schema_fingerprint};

const DESCRIPTOR: &str = include_str!("../../../fixtures/snapshots/descriptor_v1.json");

#[test]
fn fixture_has_a_valid_canonical_schema_fingerprint() {
    let descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).expect("valid descriptor fixture");
    assert_eq!(descriptor.version, 2);
    assert_eq!(descriptor.files.len(), 1);
    assert_eq!(
        schema_fingerprint(&descriptor.schema).unwrap(),
        "8bd4d7479180be8f27b16b60b0e00eadbc0c6e9777e16dae39625048cb5345ca"
    );
    assert_eq!(
        canonical_schema_json(&descriptor.schema).unwrap(),
        "{\"fields\":[{\"name\":\"id\",\"data_type\":\"int64\",\"nullable\":false},{\"name\":\"metric\",\"data_type\":\"float64\",\"nullable\":true},{\"name\":\"active\",\"data_type\":\"boolean\",\"nullable\":true},{\"name\":\"region\",\"data_type\":\"utf8\",\"nullable\":false}]}"
    );
}

#[test]
fn descriptor_round_trip_preserves_the_contract() {
    let descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    let encoded = serde_json::to_string(&descriptor).unwrap();
    assert_eq!(SnapshotDescriptor::from_json(&encoded).unwrap(), descriptor);
}

#[test]
fn rejects_parent_paths_absolute_paths_and_duplicate_keys() {
    for invalid in [
        "../part.parquet",
        "/data/part.parquet",
        "data//part.parquet",
        "data\\\\part.parquet",
    ] {
        let json = DESCRIPTOR.replace("data/part-00000.parquet", invalid);
        let result = SnapshotDescriptor::from_json(&json);
        assert!(
            matches!(result, Err(SnapshotError::InvalidObjectKey(_))),
            "expected invalid object key for {invalid:?}, got {result:?}"
        );
    }

    let mut descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    descriptor.files.push(descriptor.files[0].clone());
    assert_eq!(
        descriptor.validate().unwrap_err(),
        SnapshotError::DuplicateObjectKey("data/part-00000.parquet".into())
    );
}

#[test]
fn rejects_schema_and_file_fingerprint_mismatches() {
    let mut descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    descriptor.schema.fields[0].nullable = true;
    assert_eq!(
        descriptor.validate().unwrap_err(),
        SnapshotError::SchemaFingerprintMismatch
    );

    let mut descriptor = SnapshotDescriptor::from_json(DESCRIPTOR).unwrap();
    descriptor.files[0].schema_fingerprint = "b".repeat(64);
    assert_eq!(
        descriptor.validate().unwrap_err(),
        SnapshotError::FileSchemaMismatch {
            object_key: "data/part-00000.parquet".into(),
        }
    );
}

#[test]
fn rejects_negative_versions_and_unknown_fields() {
    let negative = DESCRIPTOR.replacen("\"version\": 2", "\"version\": -1", 1);
    assert!(matches!(
        SnapshotDescriptor::from_json(&negative),
        Err(SnapshotError::InvalidJson(_))
    ));

    let unknown = DESCRIPTOR.replacen(
        "\"format_version\": 1",
        "\"format_version\": 1, \"extra\": true",
        1,
    );
    assert!(matches!(
        SnapshotDescriptor::from_json(&unknown),
        Err(SnapshotError::InvalidJson(_))
    ));
}
