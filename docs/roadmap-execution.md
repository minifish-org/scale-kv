# Scale-KV Roadmap Execution Plan

Status: Draft  
Target: single-compute Aurora-style KV, production-hardening path

## Phase 0: API Surface Freeze

Goal: finalize stable external API and internal boundary.

Tasks:

- Finalize `docs/api-v1-draft.md`.
- Mark internal APIs with doc comments / hidden visibility where possible.
- Add API contract tests for stable behaviors.

Exit criteria:

- API list approved.
- No undocumented external API additions.

## Phase 1: Metrics and Observability (P1)

Goal: expose minimum operational telemetry.

Tasks:

- Add metrics counters/gauges:
  - WAL: bytes, segments, backpressure count
  - MVCC: versions count, versions removed, active reads
  - Checkpoint/GC: run count + duration
  - Cache: hit/miss + resident pages
- Add metrics export endpoint (or structured scrape output).
- Add baseline dashboard/alert rules.

Exit criteria:

- Operators can identify WAL pressure, GC stall, cache inefficiency from metrics.

## Phase 2: Failure Drills and Recovery Validation

Goal: verify system behavior under realistic faults.

Tasks:

- Add fault tests:
  - disk full during WAL append/checkpoint
  - network jitter/timeout/partial storage unavailability
  - restart sequence and replay correctness
- Add deterministic recovery assertions:
  - durable_lsn monotonicity
  - data correctness after replay/restart

Exit criteria:

- Fault matrix is automated and reproducible.

## Phase 3: Auth, Ops Interface, Config Governance

Goal: production-safe control plane.

Tasks:

- Authentication + authorization model for admin ops.
- Explicit ops APIs for safe maintenance actions.
- Configuration schema and startup validation.
- Effective-config reporting on startup.

Exit criteria:

- Unauthorized ops are rejected.
- Invalid config fails fast with actionable errors.

## Phase 4: Performance Baseline and Capacity Model

Goal: predictable scaling envelope.

Tasks:

- Define benchmark matrix (R/W ratio, concurrency, key cardinality, value size).
- Produce baseline report (throughput + P95/P99 + WAL/cache/GC behavior).
- Write sizing guide:
  - page cache sizing
  - WAL thresholds
  - MVCC GC tuning

Exit criteria:

- Capacity planning document available and reproducible.

## Working Agreement

- Use one check path everywhere:
  - `./scripts/check.sh`
- CI and local pre-commit must run same checks.
- API changes must update Phase 0 contract docs first.

