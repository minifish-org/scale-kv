# Performance Baseline Snapshot (2026-02-12)

Command:

```bash
OUT_DIR=/tmp/scale-kv-baseline RECORDS=200 OPS=200 ./scripts/run_perf_baseline.sh
```

Environment:

- local machine run
- single storage server
- quick baseline scale (`records=200`, `ops=200`)

## Report

# Performance Baseline Report

- source: `/tmp/scale-kv-baseline/perf-baseline-20260212-002620.jsonl`
- rows: `20`
- workload: `records=200`, `ops=200`

## Matrix

| read_ratio | concurrency | throughput_ops_per_sec | p50_us | p95_us | p99_us | read_ops | write_ops |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 16.86 | 59143 | 65272 | 78405 | 0 | 200 |
| 0 | 4 | 16.34 | 241914 | 264072 | 275669 | 0 | 200 |
| 0 | 16 | 20.74 | 966387 | 1025597 | 1044987 | 0 | 200 |
| 0 | 32 | 23.86 | 810567 | 2018849 | 2037023 | 0 | 200 |
| 20 | 1 | 64.30 | 20046 | 24769 | 27222 | 54 | 146 |
| 20 | 4 | 53.61 | 90437 | 108993 | 155239 | 42 | 158 |
| 20 | 16 | 25.69 | 498553 | 997190 | 1013038 | 37 | 163 |
| 20 | 32 | 18.00 | 1745816 | 2983560 | 3037775 | 32 | 168 |
| 50 | 1 | 105.12 | 33 | 22718 | 26059 | 107 | 93 |
| 50 | 4 | 93.65 | 16113 | 91255 | 93759 | 100 | 100 |
| 50 | 16 | 81.21 | 140646 | 372340 | 378059 | 88 | 112 |
| 50 | 32 | 96.69 | 23 | 708025 | 739456 | 108 | 92 |
| 80 | 1 | 295.88 | 8 | 20906 | 22658 | 167 | 33 |
| 80 | 4 | 229.50 | 10 | 80570 | 84087 | 156 | 44 |
| 80 | 16 | 232.18 | 7 | 319227 | 324334 | 157 | 43 |
| 80 | 32 | 277.61 | 2 | 336023 | 395966 | 165 | 35 |
| 100 | 1 | 323821.09 | 2 | 3 | 18 | 200 | 0 |
| 100 | 4 | 163576.84 | 4 | 9 | 22 | 200 | 0 |
| 100 | 16 | 325115.50 | 2 | 3 | 5 | 200 | 0 |
| 100 | 32 | 331011.84 | 2 | 3 | 5 | 200 | 0 |

## Best Throughput per Read Ratio

| read_ratio | best_concurrency | throughput_ops_per_sec | p95_us | p99_us |
|---:|---:|---:|---:|---:|
| 0 | 32 | 23.86 | 2018849 | 2037023 |
| 20 | 1 | 64.30 | 24769 | 27222 |
| 50 | 1 | 105.12 | 22718 | 26059 |
| 80 | 1 | 295.88 | 20906 | 22658 |
| 100 | 32 | 331011.84 | 3 | 5 |

## Notes

- This snapshot is intentionally small and fast for reproducibility.
- Pure-read results are cache-hot and represent an upper-bound path.
- Write-heavy latency rises sharply with higher concurrency; this is expected with current write serialization/backpressure behavior.
