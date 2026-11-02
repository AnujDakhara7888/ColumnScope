use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::SnapshotError;
use crate::schema::SchemaDef;

pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Immutable file set exported by a committed SnapshotStore version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDescriptor {
    pub format_version: u32,
    pub table_id: String,
    pub version: u64,
    pub commit_id: String,
    pub schema: SchemaDef,
    pub schema_fingerprint: String,
    pub files: Vec<SnapshotFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotFile {
    pub object_key: String,
    pub byte_size: u64,
    pub row_count: u64,
    pub checksum: FileChecksum,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    Sha256,
}

impl SnapshotDescriptor {
    pub fn from_json(json: &str) -> Result<Self, SnapshotError> {
        let descriptor: Self = serde_json::from_str(json)
            .map_err(|error| SnapshotError::InvalidJson(error.to_string()))?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotError::UnsupportedVersion {
                found: self.format_version,
            });
        }
        require_nonempty("table_id", &self.table_id)?;
        require_nonempty("commit_id", &self.commit_id)?;
        self.schema
            .validate()
            .map_err(|error| SnapshotError::InvalidSchema(error.to_string()))?;

        if !is_sha256(&self.schema_fingerprint) {
            return Err(SnapshotError::InvalidSha256 {
                field: "schema_fingerprint",
            });
        }
        let actual = schema_fingerprint(&self.schema)
            .map_err(|error| SnapshotError::InvalidSchema(error.to_string()))?;
        if self.schema_fingerprint != actual {
            return Err(SnapshotError::SchemaFingerprintMismatch);
        }

        let mut keys = HashSet::with_capacity(self.files.len());
        for file in &self.files {
            if !is_normal_relative_key(&file.object_key) {
                return Err(SnapshotError::InvalidObjectKey(file.object_key.clone()));
            }
            if !keys.insert(file.object_key.as_str()) {
                return Err(SnapshotError::DuplicateObjectKey(file.object_key.clone()));
            }
            if !is_sha256(&file.checksum.value) {
                return Err(SnapshotError::InvalidSha256 {
                    field: "files[].checksum.value",
                });
            }
            if file.schema_fingerprint != self.schema_fingerprint {
                return Err(SnapshotError::FileSchemaMismatch {
                    object_key: file.object_key.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Canonical schema bytes are compact UTF-8 JSON in declared field order.
pub fn canonical_schema_json(schema: &SchemaDef) -> Result<String, serde_json::Error> {
    serde_json::to_string(schema)
}

pub fn schema_fingerprint(schema: &SchemaDef) -> Result<String, serde_json::Error> {
    let canonical = canonical_schema_json(schema)?;
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn require_nonempty(field: &'static str, value: &str) -> Result<(), SnapshotError> {
    if value.is_empty() {
        return Err(SnapshotError::EmptyField(field));
    }
    Ok(())
}

fn is_normal_relative_key(key: &str) -> bool {
    !key.is_empty()
        && !key.starts_with('/')
        && !key.contains('\\')
        && key
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
