# Performance Baseline

Status: active

## Benchmark runner

Path: `/Users/yusp/work/scale-kv/src/bin/workload_bench.rs`

CLI:

- `--addr <host:port>`
- `--records <usize>`
- `--ops <usize>`
- `--concurrency <usize>`
- `--read-ratio <0..100>`

Output:

- one JSON line with:
  - throughput: `throughput_ops_per_sec`
  - latency: `p50_us`, `p95_us`, `p99_us`
  - operation split: `read_ops`, `write_ops`
  - run metadata: `records`, `ops`, `concurrency`, `read_ratio`

## Baseline matrix automation

Path: `/Users/yusp/work/scale-kv/scripts/run_perf_baseline.sh`

Default matrix:

- read ratio: `100, 80, 50, 20, 0`
- concurrency: `1, 4, 16, 32`
- records: `2000`
- ops per run: `4000`
- intended for CI/dev reproducibility, not absolute max throughput

Artifacts:

- benchmark result JSONL:
  - `artifacts/perf-baseline-<timestamp>.jsonl`
- storage server log:
  - `artifacts/storage-server-<timestamp>.log`

## How to run

```bash
./scripts/run_perf_baseline.sh
```

Optional overrides:

- `ADDR=127.0.0.1:50051`
- `DATA_DIR=./data/perf-baseline`
- `RECORDS=200000`
- `OPS=300000`
- `OUT_DIR=./artifacts`

Example:

```bash
ADDR=127.0.0.1:50071 RECORDS=50000 OPS=100000 ./scripts/run_perf_baseline.sh
```
