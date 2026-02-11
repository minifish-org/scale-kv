# Fault Drills and Recovery Validation

This document tracks automated failure/recovery drills for storage.

## Current automated drills

Path: `/Users/yusp/work/scale-kv/src/node.rs`

- `test_wal_backpressure_under_tight_byte_limit`
  - Uses `max_wal_bytes=1` to force deterministic WAL backpressure.
  - Verifies write returns `WouldBlock`.
  - Verifies `wal_backpressure_count` metric increments.

- `test_restart_preserves_data_and_durable_lsn_monotonic`
  - Writes one WAL-backed page batch.
  - Reopens storage on same directory.
  - Verifies:
    - data is recoverable after restart
    - `durable_lsn` is monotonic across restart.

## Existing quorum/failure coverage

Path: `/Users/yusp/work/scale-kv/tests/quorum_commit_e2e.rs`

- `test_quorum_commit_with_one_ahead_node`
  - Exercises partial storage inconsistency where one node is ahead.
  - Verifies commit can still succeed with quorum and healthy nodes advance.
- `test_quorum_commit_succeeds_with_one_backpressured_node`
  - One node is configured with aggressive WAL backpressure.
  - Verifies quorum=2 commit still succeeds and completes within timeout.
- `test_quorum_commit_fails_when_quorum_requires_backpressured_node`
  - Same fault model, but quorum=3.
  - Verifies commit fails with quorum-not-reached.

## Next drills to add

- Disk full during checkpoint file sync.
- Restart ordering matrix (staggered restart of multiple replicas).
