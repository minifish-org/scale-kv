# Metrics and Observability

This document describes the current structured metrics output for storage nodes.

## StorageNode metrics snapshot API

Path: `src/node.rs`

- `StorageNode::metrics_snapshot() -> StorageMetricsSnapshot`
- `StorageMetricsSnapshot::render_json() -> String`
- `StorageMetricsSnapshot::render_prometheus() -> String`

Current fields:

- WAL:
  - `wal_segments`
  - `wal_bytes`
  - `wal_backpressure_count`
- Checkpoint:
  - `checkpoint_runs`
  - `checkpoint_total_duration_ms`
  - `checkpoint_last_duration_ms`
- MVCC/GC:
  - `gc_runs`
  - `gc_versions_removed_total`
  - `gc_last_duration_ms`
  - `mvcc_keys`
  - `mvcc_versions`
  - `mvcc_avg_versions_per_key`
  - `active_reads`
- Cache:
  - `page_cache_resident_pages`
  - `page_cache_hits`
  - `page_cache_misses`

## storage_server structured output

Path: `src/bin/storage_server.rs`

Flags:

- `--metrics-interval-secs <u64>`
  - `0` (default): disabled
  - `>0`: emit periodic metrics
- `--metrics-format <json|prometheus|prom>`
  - default: `json`

Example:

```bash
cargo run --bin storage_server -- \
  --addr 127.0.0.1:50051 \
  --dir ./data/storage-50051 \
  --metrics-interval-secs 10 \
  --metrics-format prometheus
```
