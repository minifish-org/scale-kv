//! Simple benchmark runner for EmbeddedCompute + external storage_server.
//!
//! Example:
//!   cargo run --release --bin storage_server -- --addr 127.0.0.1:50051 --dir ./data/bench-store
//!   cargo run --release --bin simple_bench -- --addr 127.0.0.1:50051 --records 100000 --ops 200000

use rand::SeedableRng;
use scale_kv::{EmbeddedCompute, KEY_SIZE, VALUE_SIZE};
use std::net::SocketAddr;
use std::time::Instant;
use tokio::task::LocalSet;

fn parse_arg(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn key_for(i: u64) -> Vec<u8> {
    let mut k = format!("k{i:016}").into_bytes();
    while k.len() < KEY_SIZE {
        k.push(b'x');
    }
    k.truncate(KEY_SIZE);
    k
}

fn val_for(i: u64) -> Vec<u8> {
    let mut v = format!("v{i:016}").into_bytes();
    while v.len() < VALUE_SIZE {
        v.push(b'y');
    }
    v.truncate(VALUE_SIZE);
    v
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let addr: SocketAddr = parse_arg(&args, "--addr")
        .unwrap_or_else(|| "127.0.0.1:50051".to_string())
        .parse()?;
    let records: usize = parse_arg(&args, "--records")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let ops: usize = parse_arg(&args, "--ops")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);

    let local = LocalSet::new();
    local
        .run_until(async {
            let addrs = vec![addr.to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            // Load records.
            let start = Instant::now();
            for i in 0..records as u64 {
                let k = key_for(i);
                let v = val_for(i);
                compute.put(&k, &v).await.unwrap();
            }
            let dur = start.elapsed();
            eprintln!(
                "load: records={records} time={:.3}s ops/s={:.0}",
                dur.as_secs_f64(),
                (records as f64) / dur.as_secs_f64()
            );

            // Mixed ops: 90% get, 10% put overwrite.
            use rand::RngCore;
            let mut rng = rand::rngs::StdRng::seed_from_u64(0x5ca1e);
            let start = Instant::now();
            let mut get_ok = 0usize;
            for i in 0..ops as u64 {
                let r: u32 = rng.next_u32();
                let id = (r as u64) % (records as u64);
                let k = key_for(id);
                if (r % 10) == 0 {
                    let v = val_for(i);
                    compute.put(&k, &v).await.unwrap();
                } else if compute.get(&k).unwrap().is_some() {
                    get_ok += 1;
                }
            }
            let dur = start.elapsed();
            eprintln!(
                "run: ops={ops} get_ok={get_ok} time={:.3}s ops/s={:.0}",
                dur.as_secs_f64(),
                (ops as f64) / dur.as_secs_f64()
            );
        })
        .await;

    Ok(())
}
