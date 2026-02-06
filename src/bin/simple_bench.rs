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

            // Load records (256 puts per txn).
            let start = Instant::now();
            let mut i = 0u64;
            while (i as usize) < records {
                let mut tx = compute.begin();
                let mut n = 0usize;
                while n < 256 && (i as usize) < records {
                    let k = key_for(i);
                    let v = val_for(i);
                    tx.put(&k, &v).unwrap();
                    n += 1;
                    i += 1;
                }
                tx.commit().await.unwrap();
            }
            let dur = start.elapsed();
            eprintln!(
                "load: records={records} batch=256 time={:.3}s ops/s={:.0}",
                dur.as_secs_f64(),
                (records as f64) / dur.as_secs_f64()
            );

            use rand::RngCore;
            let mut rng = rand::rngs::StdRng::seed_from_u64(0x5ca1e);

            // Run 1: pure get
            let start = Instant::now();
            let mut get_ok = 0usize;
            for _ in 0..ops {
                let id = (rng.next_u32() as u64) % (records as u64);
                let k = key_for(id);
                if compute.get(&k).unwrap().is_some() {
                    get_ok += 1;
                }
            }
            let dur = start.elapsed();
            eprintln!(
                "get: ops={ops} ok={get_ok} time={:.3}s ops/s={:.0}",
                dur.as_secs_f64(),
                (ops as f64) / dur.as_secs_f64()
            );

            // Run 2: pure put overwrite (10 puts per txn)
            let start = Instant::now();
            let mut done = 0usize;
            let mut seq = 0u64;
            while done < ops {
                let mut tx = compute.begin();
                let mut n = 0usize;
                while n < 10 && done < ops {
                    let id = (rng.next_u32() as u64) % (records as u64);
                    let k = key_for(id);
                    let v = val_for(seq);
                    tx.put(&k, &v).unwrap();
                    n += 1;
                    done += 1;
                    seq += 1;
                }
                tx.commit().await.unwrap();
            }
            let dur = start.elapsed();
            eprintln!(
                "put: ops={ops} batch=10 time={:.3}s ops/s={:.0}",
                dur.as_secs_f64(),
                (ops as f64) / dur.as_secs_f64()
            );
        })
        .await;

    Ok(())
}
