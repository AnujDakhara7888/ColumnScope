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
fingerprint. File checksum verification will be a separately measured adapter
step; this contract validates its algorithm and representation but does not read
file contents.

See the validated [version-1 fixture](../fixtures/snapshots/descriptor_v1.json).
