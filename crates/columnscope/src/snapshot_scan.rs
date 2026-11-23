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
use crate::scan::{ParquetScan, ScanPushdowns};
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
    source_columns: usize,
    projected_columns: usize,
    total_row_groups: usize,
    selected_row_groups: usize,
}

impl SnapshotScan {
    /// Opens exactly the descriptor's files and validates size, row count, and
    /// physical schema. Whole-file checksums are deliberately a separate pass.
    pub fn try_new(
        storage_root: impl AsRef<Path>,
        descriptor: &SnapshotDescriptor,
        batch_size: usize,
    ) -> Result<Self, ExecutionError> {
        Self::try_new_with_pushdowns(
            storage_root,
            descriptor,
            batch_size,
            ScanPushdowns::default(),
        )
    }

    /// Opens a pinned snapshot with identical projection and pruning settings
    /// applied independently to every listed Parquet file.
    pub fn try_new_with_pushdowns(
        storage_root: impl AsRef<Path>,
        descriptor: &SnapshotDescriptor,
        batch_size: usize,
        pushdowns: ScanPushdowns,
    ) -> Result<Self, ExecutionError> {
        if batch_size == 0 {
            return Err(ExecutionError::InvalidBatchSize);
        }
        descriptor.validate()?;
        let source_schema = Arc::new(
            descriptor
                .schema
                .to_arrow()
                .map_err(|error| SnapshotError::InvalidSchema(error.to_string()))?,
        );
        let schema = projected_schema(&source_schema, pushdowns.projection.as_deref())?;
        let source_columns = source_schema.fields().len();
        let projected_columns = schema.fields().len();
        let mut scans = Vec::with_capacity(descriptor.files.len());
        let mut total_row_groups = 0_usize;
        let mut selected_row_groups = 0_usize;
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
            let scan = ParquetScan::try_new_with_pushdowns(&path, batch_size, pushdowns.clone())
                .map_err(|error| SnapshotError::FileAccess {
                    object_key: snapshot_file.object_key.clone(),
                    reason: error.to_string(),
                })?;
            if scan.source_rows() != snapshot_file.row_count {
                return Err(SnapshotError::FileRowCountMismatch {
                    object_key: snapshot_file.object_key.clone(),
                    expected: snapshot_file.row_count,
                    actual: scan.source_rows(),
                }
                .into());
            }
            if scan.source_schema().as_ref() != source_schema.as_ref() {
                return Err(SnapshotError::PhysicalFileSchemaMismatch {
                    object_key: snapshot_file.object_key.clone(),
                }
                .into());
            }
            total_row_groups = total_row_groups
                .checked_add(scan.total_row_groups())
                .ok_or(ExecutionError::CounterOverflow)?;
            selected_row_groups = selected_row_groups
                .checked_add(scan.selected_row_groups())
                .ok_or(ExecutionError::CounterOverflow)?;
            scans.push(scan);
        }
        Ok(Self {
            scans,
            current: 0,
            schema,
            metrics: OperatorMetrics::default(),
            state: OperatorState::Ready,
            source_columns,
            projected_columns,
            total_row_groups,
            selected_row_groups,
        })
    }

    pub fn file_count(&self) -> usize {
        self.scans.len()
    }

    pub fn source_columns(&self) -> usize {
        self.source_columns
    }

    pub fn projected_columns(&self) -> usize {
        self.projected_columns
    }

    pub fn total_row_groups(&self) -> usize {
        self.total_row_groups
    }

    pub fn selected_row_groups(&self) -> usize {
        self.selected_row_groups
    }

    fn fail<T>(&mut self, error: ExecutionError) -> Result<T, ExecutionError> {
        self.state = OperatorState::Failed;
        Err(error)
    }
}

fn projected_schema(
    source: &SchemaRef,
    projection: Option<&[usize]>,
) -> Result<SchemaRef, ExecutionError> {
    let Some(projection) = projection else {
        return Ok(source.clone());
    };
    let mut projection = projection.to_vec();
    let mut seen = std::collections::HashSet::with_capacity(projection.len());
    for &index in &projection {
        if index >= source.fields().len() {
            return Err(ExecutionError::ProjectionOutOfBounds {
                index,
                columns: source.fields().len(),
            });
        }
        if !seen.insert(index) {
            return Err(ExecutionError::DuplicateProjectionColumn(index));
        }
    }
    projection.sort_unstable();
    Ok(Arc::new(source.project(&projection)?))
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
