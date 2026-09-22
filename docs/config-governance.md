# Configuration Governance

Status: active

## Principles

- No environment-variable dependency for storage maintenance behavior.
- All runtime knobs are explicit and validated at startup.
- Effective configuration is printed at process start for audit/debug.

## Storage maintenance config

Path: `src/node.rs`

Type:

- `StorageMaintenanceConfig`

Validation:

- `StorageMaintenanceConfig::validate()`
- Enforced by `StorageNode::open_with_maintenance(...)` before opening storage.
- Invalid config returns `InvalidInput` and startup fails fast.

Effective config rendering:

- `StorageMaintenanceConfig::render_json()`

## storage_server startup behavior

Path: `src/bin/storage_server.rs`

- CLI arguments build `StorageMaintenanceConfig`.
- Startup prints:
  - `storage_server effective_config: <json>`
- Then server starts with `start_with_dir_and_maintenance(...)`.

## Covered invalid cases

- zero checkpoint interval
- zero checkpoint max dirty pages
- zero checkpoint max dirty bytes
- zero WAL max bytes
- zero WAL max segments
- zero MVCC GC batch interval
- zero WAL group-commit max batches

WAL group commit tuning knobs:

- `wal_group_commit_max_batches`
- `wal_group_commit_wait_us`

Recommended presets:

- low-latency profile:
  - `wal_group_commit_max_batches = 16`
  - `wal_group_commit_wait_us = 50`
- high-throughput profile:
  - `wal_group_commit_max_batches = 128`
  - `wal_group_commit_wait_us = 400`

Example (low latency):

```bash
cargo run --release --bin storage_server -- \
  --addr 127.0.0.1:50051 \
  --dir ./data/storage-50051 \
  --wal-group-max-batches 16 \
  --wal-group-wait-us 50
```

Example (high throughput):

```bash
cargo run --release --bin storage_server -- \
  --addr 127.0.0.1:50051 \
  --dir ./data/storage-50051 \
  --wal-group-max-batches 128 \
  --wal-group-wait-us 400
```
