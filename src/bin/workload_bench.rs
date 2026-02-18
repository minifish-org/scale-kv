//! Workload benchmark runner for EmbeddedCompute + storage_server.
//!
//! Example:
//!   cargo run --release --bin storage_server -- --addr 127.0.0.1:50051 --dir ./data/bench-store
//!   cargo run --release --bin workload_bench -- --addr 127.0.0.1:50051 --records 100000 --ops 200000 --concurrency 16 --read-ratio 80

use rand::Rng;
use scale_kv::{EmbeddedCompute, KEY_SIZE, VALUE_SIZE};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::LocalSet;

#[derive(Clone, Debug)]
struct BenchConfig {
    addr: SocketAddr,
    records: usize,
    ops: usize,
    concurrency: usize,
    read_ratio: u32,
}

#[derive(Debug, Default)]
struct RunStats {
    read_ops: usize,
    write_ops: usize,
    latencies_us: Vec<u64>,
}

fn parse_arg(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn parse_usize(args: &[String], key: &str, default: usize) -> usize {
    parse_arg(args, key)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u32(args: &[String], key: &str, default: u32) -> u32 {
    parse_arg(args, key)
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default)
}

fn key_for(id: u64) -> [u8; KEY_SIZE] {
    let mut key = [0u8; KEY_SIZE];
    key[..8].copy_from_slice(&id.to_le_bytes());
    key
}

fn value_for(id: u64) -> Vec<u8> {
    let mut value = vec![0u8; VALUE_SIZE];
    value[..8].copy_from_slice(&id.to_le_bytes());
    value
}

fn percentile_us(mut values: Vec<u64>, p: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let rank = ((values.len() - 1) as f64 * p).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn is_transient_by_text(msg: &str) -> bool {
    msg.contains("quorum not reached")
        || msg.contains("out of order")
        || msg.contains("wal backpressure")
        || msg.contains("WouldBlock")
}

async fn put_with_retry(compute: &EmbeddedCompute, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
    const MAX_ATTEMPTS: usize = 12;
    let mut backoff = Duration::from_millis(1);
    for attempt in 1..=MAX_ATTEMPTS {
        match compute.put(key, value).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let msg = err.to_string();
                let transient_by_text = is_transient_by_text(&msg);
                if attempt == MAX_ATTEMPTS || (!err.is_retryable() && !transient_by_text) {
                    return Err(anyhow::anyhow!(
                        "put failed after {attempt} attempts: {err}"
                    ));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(25));
            }
        }
    }
    Err(anyhow::anyhow!("put failed unexpectedly"))
}

async fn run_with_config(config: BenchConfig, local: &LocalSet) -> anyhow::Result<()> {
    let cfg = config;
    local
        .run_until(async {
            let addrs = vec![cfg.addr.to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, local).await?;

            let load_start = Instant::now();
            let mut i = 0u64;
            while (i as usize) < cfg.records {
                let mut tx = compute.begin_rw().await;
                let mut n = 0usize;
                while n < 256 && (i as usize) < cfg.records {
                    let key = key_for(i);
                    let value = value_for(i);
                    tx.put(&key, &value).await?;
                    i += 1;
                    n += 1;
                }
                let _ = tx.commit().await?;
            }
            let load_s = load_start.elapsed().as_secs_f64();

            let stats = Arc::new(Mutex::new(RunStats::default()));
            let bench_start = Instant::now();

            let mut handles = Vec::with_capacity(cfg.concurrency);
            let base = cfg.ops / cfg.concurrency;
            let rem = cfg.ops % cfg.concurrency;
            for worker_id in 0..cfg.concurrency {
                let compute = compute.clone();
                let stats = Arc::clone(&stats);
                let worker_ops = if worker_id < rem { base + 1 } else { base };
                let records = cfg.records as u64;
                let read_ratio = cfg.read_ratio;
                handles.push(tokio::task::spawn_local(async move {
                    let mut rng = rand::thread_rng();
                    let mut local_reads = 0usize;
                    let mut local_writes = 0usize;
                    let mut local_lat = Vec::with_capacity(worker_ops);

                    for n in 0..worker_ops {
                        let key_id = rng.gen_range(0..records);
                        let key = key_for(key_id);
                        let t0 = Instant::now();
                        if rng.gen_range(0..100) < read_ratio {
                            let _ = compute.get_exists(&key).await?;
                            local_reads += 1;
                        } else {
                            // deterministic-ish overwrite pattern to avoid growing key cardinality.
                            let value = value_for((worker_id as u64) << 32 | n as u64);
                            put_with_retry(&compute, &key, &value).await?;
                            local_writes += 1;
                        }
                        local_lat.push(t0.elapsed().as_micros() as u64);
                    }

                    let mut guard = stats.lock().await;
                    guard.read_ops += local_reads;
                    guard.write_ops += local_writes;
                    guard.latencies_us.extend(local_lat);
                    anyhow::Ok(())
                }));
            }

            for handle in handles {
                handle.await??;
            }

            let bench_s = bench_start.elapsed().as_secs_f64();
            let stats = Arc::try_unwrap(stats)
                .expect("stats still referenced")
                .into_inner();
            let total_ops = stats.read_ops + stats.write_ops;
            let throughput = if bench_s > 0.0 {
                total_ops as f64 / bench_s
            } else {
                0.0
            };
            let p50 = percentile_us(stats.latencies_us.clone(), 0.50);
            let p95 = percentile_us(stats.latencies_us.clone(), 0.95);
            let p99 = percentile_us(stats.latencies_us.clone(), 0.99);

            println!(
                "{{\"addr\":\"{}\",\"records\":{},\"ops\":{},\"concurrency\":{},\"read_ratio\":{},\"load_seconds\":{:.3},\"run_seconds\":{:.3},\"throughput_ops_per_sec\":{:.2},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"read_ops\":{},\"write_ops\":{}}}",
                cfg.addr,
                cfg.records,
                cfg.ops,
                cfg.concurrency,
                cfg.read_ratio,
                load_s,
                bench_s,
                throughput,
                p50,
                p95,
                p99,
                stats.read_ops,
                stats.write_ops
            );
            Ok::<(), anyhow::Error>(())
        })
        .await?;

    // Let stdout flush in constrained runners.
    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let addr: SocketAddr = parse_arg(&args, "--addr")
        .unwrap_or_else(|| "127.0.0.1:50051".to_string())
        .parse()?;
    let config = BenchConfig {
        addr,
        records: parse_usize(&args, "--records", 100_000),
        ops: parse_usize(&args, "--ops", 200_000),
        concurrency: parse_usize(&args, "--concurrency", 16).max(1),
        read_ratio: parse_u32(&args, "--read-ratio", 80).min(100),
    };

    let local = LocalSet::new();
    run_with_config(config, &local).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use scale_kv::StorageServer;
    use tempfile::tempdir;

    #[test]
    fn test_parse_helpers_and_key_value_layout() {
        let args = vec![
            "workload_bench".to_string(),
            "--records".to_string(),
            "123".to_string(),
            "--read-ratio".to_string(),
            "77".to_string(),
        ];
        assert_eq!(parse_usize(&args, "--records", 1), 123);
        assert_eq!(parse_usize(&args, "--missing", 9), 9);
        assert_eq!(parse_u32(&args, "--read-ratio", 0), 77);
        assert_eq!(parse_u32(&args, "--none", 11), 11);

        let k = key_for(5);
        assert_eq!(k.len(), KEY_SIZE);
        assert_eq!(&k[..8], &5u64.to_le_bytes());
        let v = value_for(9);
        assert_eq!(v.len(), VALUE_SIZE);
        assert_eq!(&v[..8], &9u64.to_le_bytes());
    }

    #[test]
    fn test_percentiles_and_transient_message_matcher() {
        assert_eq!(percentile_us(vec![], 0.5), 0);
        assert_eq!(percentile_us(vec![1, 100, 10], 0.5), 10);
        assert_eq!(percentile_us(vec![1, 100, 10], 0.99), 100);

        assert!(is_transient_by_text("wal backpressure"));
        assert!(is_transient_by_text("WouldBlock"));
        assert!(is_transient_by_text("quorum not reached"));
        assert!(!is_transient_by_text("invalid key"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_run_with_config_smoke() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let dir = tempdir().unwrap();
                let server = StorageServer::start_with_dir(
                    "127.0.0.1:0".parse().unwrap(),
                    dir.path().to_path_buf(),
                )
                .await
                .unwrap();
                let cfg = BenchConfig {
                    addr: server.addr(),
                    records: 32,
                    ops: 32,
                    concurrency: 2,
                    read_ratio: 70,
                };
                run_with_config(cfg, &local).await.unwrap();
            })
            .await;
    }
}
