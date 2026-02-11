# Capacity Model (Single Compute)

Status: draft-v1

This model is for quick planning and guardrail tuning, not exact forecasting.

## Inputs

- `K`: key cardinality
- `V`: average value bytes
- `vpk`: average versions per key (MVCC depth)
- `R`: reads/sec
- `W`: writes/sec
- `P`: page size bytes (`16384`)
- `C`: cache hit ratio (`0..1`)

## Memory model (first-order)

Approximate MVCC logical footprint:

- `mvcc_bytes ~= K * vpk * (KEY_SIZE + V + version_overhead)`

With default constants:

- `KEY_SIZE = 16`
- `version_overhead` (commit_lsn + metadata + allocator slack) use `32..96` as planning range.

Suggested safety:

- keep process RSS target below `70%` of node memory
- reserve `15..25%` for transient spikes (checkpoint, GC, allocator fragmentation)

## WAL model (steady state)

Per-write WAL amplification factor `A_wal` depends on page churn and batching.

- `wal_bytes_per_sec ~= W * avg_wal_record_bytes * A_wal`

Operational threshold guidance:

- `max_wal_bytes >= wal_bytes_per_sec * checkpoint_interval_secs * 2`
- `max_wal_segments` sized so segment rotation does not dominate IO

Backpressure policy:

- if WAL exceeds threshold, force checkpoint + truncate
- if still above threshold, reject with `WouldBlock` until pressure drops
- tune WAL group commit:
  - increase `wal_group_commit_max_batches` for throughput
  - increase `wal_group_commit_wait_us` modestly to improve fsync amortization (at latency cost)

Practical starting points:

- latency-sensitive:
  - `wal_group_commit_max_batches=16`
  - `wal_group_commit_wait_us=50`
- throughput-sensitive:
  - `wal_group_commit_max_batches=128`
  - `wal_group_commit_wait_us=400`

Tuning loop:

1. Fix workload mix (`read_ratio`, `concurrency`).
2. Sweep `wal_group_commit_wait_us` upward until p99 hits SLO bound.
3. Sweep `wal_group_commit_max_batches` upward for extra throughput.
4. Stop when throughput gain flattens or p99 exceeds SLO.

## Cache model

Working set pages:

- `working_set_pages ~= hot_bytes / P`

To keep hit ratio stable:

- `page_cache_pages >= working_set_pages * 1.2`

Track and alert:

- cache hit ratio = `hits / (hits + misses)`
- sustained hit ratio drop + WAL growth usually indicates under-sized cache or write-heavy churn

## GC tuning model

GC lag risk increases with:

- high `vpk`
- long-lived active reads
- low GC cadence

Tuning:

- decrease `mvcc_gc_every_wal_batches` for more frequent cleanup when `vpk` grows
- enforce read timeout/abort policy for stuck long readers

## Baseline workflow

1. Run `/Users/yusp/work/scale-kv/scripts/run_perf_baseline.sh`.
2. Record throughput and p95/p99 for each matrix point.
3. Collect metrics snapshot:
   - WAL bytes/segments
   - cache hit/miss
   - MVCC versions and versions/key
   - backpressure count
4. Fit capacity envelope:
   - max stable throughput before p99 or backpressure inflects
   - required cache pages for target hit ratio
   - WAL and GC thresholds for steady-state operation
