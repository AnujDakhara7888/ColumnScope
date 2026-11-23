# Pinned snapshot descriptor

A snapshot descriptor is an immutable list of files for one committed table
version. ColumnScope consumes that list exactly once and does not refresh HEAD
or infer files by listing a storage prefix during a query.

Version 1 contains:

```json
{
  "format_version": 1,
  "table_id": "events",
  "version": 2,
  "commit_id": "commit-0002",
  "schema": { "fields": [] },
  "schema_fingerprint": "<lowercase sha256>",
  "files": [
    {
      "object_key": "data/part-00000.parquet",
      "byte_size": 1310,
      "row_count": 4,
      "checksum": { "algorithm": "sha256", "value": "<lowercase sha256>" },
      "schema_fingerprint": "<same schema fingerprint>"
    }
  ]
}
```

The canonical schema encoding is compact UTF-8 JSON produced from the `fields`
array in declared order, with each field written as `name`, `data_type`, then
`nullable`. The schema fingerprint is lowercase SHA-256 of those exact bytes.
Field order is significant because it defines physical column positions.

Object keys must be unique normalized relative paths using `/`. Absolute paths,
empty components, `.`, `..`, and backslashes are rejected. Versions, byte sizes,
and row counts are unsigned. Each file must declare the descriptor's schema
fingerprint.

`SnapshotScan::try_new` resolves each key beneath a caller-supplied storage
root, opens every file before returning an operator, and verifies its declared
byte size, Parquet row count, and physical Arrow schema. It then scans files in
descriptor order and never lists the storage directory. Missing or incompatible
later files therefore fail before any earlier file can produce a partial
result. Empty descriptors preserve their declared schema.

`verify_snapshot_checksums` is a separate whole-file pass. The
`columnscope-snapshot-check` command reports whether that pass was performed,
then scans the descriptor and requires the observed row count to equal the sum
of declared file rows:

```sh
target/release/columnscope-snapshot-check \
  --storage-root ./demo-table \
  --descriptor ./snapshot-v2.json \
  --batch-size 8192 \
  --verify-checksums true
```

Keeping checksum I/O explicit prevents it from being hidden inside benchmark
timings. Planning through `plan_snapshot_query` applies the same column
projection and conservative row-group pruning to every listed file and reports
aggregate counts in the physical explain tree.

## Observed SnapshotStore integration

A live local integration run used SnapshotStore source
`2032deab713466a3fff82881822ebc8854b05c29` to create a
table, ingest a four-row batch, and export version 1. ColumnScope verified the
exported file checksum and scanned one file, two output batches, and four rows.
After a second committed batch, the pinned version-1 descriptor still returned
four rows while version 2 returned two files, four batches, and eight rows. A
replay of the second batch ID reported `replayed: true`, kept the table at
version 2, and the fresh version-2 scan remained eight rows.

The run also exposed a codec boundary: SnapshotStore currently rejects
Snappy-compressed ingest input, and its committed files use Zstandard.
ColumnScope therefore enables both Snappy and Zstandard decoding; the live test
used uncompressed ingest input and consumed SnapshotStore's Zstandard output.
This is a verified local-filesystem integration, not an S3 result.
The scan currently keeps all listed files open to preserve fail-before-output
construction, so descriptors with very large file counts may reach the process
file-descriptor limit. Bounded handle pooling is not yet implemented.

See the validated [version-1 fixture](../fixtures/snapshots/descriptor_v1.json).
