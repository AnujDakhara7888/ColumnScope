//! Pull-based execution over the exact files in a pinned snapshot descriptor.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};

use crate::error::{ExecutionError, SnapshotError};
use crate::execution::{BatchOperator, OperatorMetrics, OperatorState};
use crate::scan::ParquetScan;
use crate::snapshot::SnapshotDescriptor;

/// Sequential scan over the immutable file order in one descriptor.
///
/// Every listed file is opened and its metadata is checked before the operator
/// is returned, so a missing or incompatible later file cannot produce a
/// plausible partial result.
pub struct SnapshotScan {
    scans: Vec<ParquetScan>,
    current: usize,
    schema: SchemaRef,
    metrics: OperatorMetrics,
    state: OperatorState,
}

impl SnapshotScan {
    /// Opens exactly the descriptor's files and validates size, row count, and
    /// physical schema. Whole-file checksums are deliberately a separate pass.
    pub fn try_new(
        storage_root: impl AsRef<Path>,
        descriptor: &SnapshotDescriptor,
        batch_size: usize,
    ) -> Result<Self, ExecutionError> {
        if batch_size == 0 {
            return Err(ExecutionError::InvalidBatchSize);
        }
        descriptor.validate()?;
        let schema = Arc::new(
            descriptor
                .schema
                .to_arrow()
                .map_err(|error| SnapshotError::InvalidSchema(error.to_string()))?,
        );
        let mut scans = Vec::with_capacity(descriptor.files.len());
        for snapshot_file in &descriptor.files {
            let path = storage_root.as_ref().join(&snapshot_file.object_key);
            let metadata = std::fs::metadata(&path).map_err(|error| SnapshotError::FileAccess {
                object_key: snapshot_file.object_key.clone(),
                reason: error.to_string(),
            })?;
            if metadata.len() != snapshot_file.byte_size {
                return Err(SnapshotError::FileSizeMismatch {
                    object_key: snapshot_file.object_key.clone(),
                    expected: snapshot_file.byte_size,
                    actual: metadata.len(),
                }
                .into());
            }
            let scan = ParquetScan::try_new(&path, batch_size).map_err(|error| {
                SnapshotError::FileAccess {
                    object_key: snapshot_file.object_key.clone(),
                    reason: error.to_string(),
                }
            })?;
            if scan.source_rows() != snapshot_file.row_count {
                return Err(SnapshotError::FileRowCountMismatch {
                    object_key: snapshot_file.object_key.clone(),
                    expected: snapshot_file.row_count,
                    actual: scan.source_rows(),
                }
                .into());
            }
            if scan.source_schema().as_ref() != schema.as_ref() {
                return Err(SnapshotError::PhysicalFileSchemaMismatch {
                    object_key: snapshot_file.object_key.clone(),
                }
                .into());
            }
            scans.push(scan);
        }
        Ok(Self {
            scans,
            current: 0,
            schema,
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
        })
    }

    pub fn file_count(&self) -> usize {
        self.scans.len()
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

impl BatchOperator for SnapshotScan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecutionError> {
        match self.state {
            OperatorState::Exhausted => return Ok(None),
            OperatorState::Failed => return Err(ExecutionError::OperatorFailed),
            OperatorState::Ready => {}
        }
        if let Err(error) = self.metrics.record_pull() {
            return self.fail(error);
        }
        while let Some(scan) = self.scans.get_mut(self.current) {
            match scan.next_batch() {
                Ok(Some(batch)) => {
                    if let Err(error) = self.metrics.record_output(batch.num_rows()) {
                        return self.fail(error);
                    }
                    return Ok(Some(batch));
                }
                Ok(None) => self.current += 1,
                Err(error) => return self.fail(error),
            }
        }
        self.state = OperatorState::Exhausted;
        Ok(None)
    }

    fn metrics(&self) -> OperatorMetrics {
        self.metrics
    }

    fn state(&self) -> OperatorState {
        self.state
    }
}

/// Verifies every declared SHA-256 in a separately measurable whole-file pass.
pub fn verify_snapshot_checksums(
    storage_root: impl AsRef<Path>,
    descriptor: &SnapshotDescriptor,
) -> Result<(), SnapshotError> {
    descriptor.validate()?;
    for snapshot_file in &descriptor.files {
        let path = storage_root.as_ref().join(&snapshot_file.object_key);
        let mut source = File::open(path).map_err(|error| SnapshotError::FileAccess {
            object_key: snapshot_file.object_key.clone(),
            reason: error.to_string(),
        })?;
        let mut digest = Sha256::new();
        let mut bytes = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|error| SnapshotError::FileAccess {
                    object_key: snapshot_file.object_key.clone(),
                    reason: error.to_string(),
                })?;
            if read == 0 {
                break;
            }
            bytes =
                bytes
                    .checked_add(read as u64)
                    .ok_or_else(|| SnapshotError::FileSizeMismatch {
                        object_key: snapshot_file.object_key.clone(),
                        expected: snapshot_file.byte_size,
                        actual: u64::MAX,
                    })?;
            digest.update(&buffer[..read]);
        }
        if bytes != snapshot_file.byte_size {
            return Err(SnapshotError::FileSizeMismatch {
                object_key: snapshot_file.object_key.clone(),
                expected: snapshot_file.byte_size,
                actual: bytes,
            });
        }
        let actual = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if actual != snapshot_file.checksum.value {
            return Err(SnapshotError::FileChecksumMismatch {
                object_key: snapshot_file.object_key.clone(),
            });
        }
    }
    Ok(())
}
