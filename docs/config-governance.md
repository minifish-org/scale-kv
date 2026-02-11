# Configuration Governance

Status: active

## Principles

- No environment-variable dependency for storage maintenance behavior.
- All runtime knobs are explicit and validated at startup.
- Effective configuration is printed at process start for audit/debug.

## Storage maintenance config

Path: `/Users/yusp/work/scale-kv/src/node.rs`

Type:

- `StorageMaintenanceConfig`

Validation:

- `StorageMaintenanceConfig::validate()`
- Enforced by `StorageNode::open_with_maintenance(...)` before opening storage.
- Invalid config returns `InvalidInput` and startup fails fast.

Effective config rendering:

- `StorageMaintenanceConfig::render_json()`

## storage_server startup behavior

Path: `/Users/yusp/work/scale-kv/src/bin/storage_server.rs`

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
