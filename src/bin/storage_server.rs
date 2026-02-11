//! Minimal StorageServer runner.
//!
//! Example: run 3 local replicas (each with its own data dir):
//!
//! ```bash
//! cargo run --bin storage_server -- --addr 127.0.0.1:4001 --dir /tmp/scale-kv-replica1
//! cargo run --bin storage_server -- --addr 127.0.0.1:4002 --dir /tmp/scale-kv-replica2
//! cargo run --bin storage_server -- --addr 127.0.0.1:4003 --dir /tmp/scale-kv-replica3
//! ```
//!
//! Notes:
//! - `--dir` is optional. If omitted, it defaults to `./data/storage-<port>`.
//! - This binary is just for simulation/dev; it has no shutdown signal handling.

use std::net::SocketAddr;
use std::path::PathBuf;

use scale_kv::StorageMaintenanceConfig;
use scale_kv::server::StorageServer;

fn parse_arg(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn parse_u64(args: &[String], key: &str) -> Option<u64> {
    parse_arg(args, key)?.parse::<u64>().ok()
}

fn parse_usize(args: &[String], key: &str) -> Option<usize> {
    parse_arg(args, key)?.parse::<usize>().ok()
}

fn parse_bool(args: &[String], key: &str) -> Option<bool> {
    let v = parse_arg(args, key)?;
    match v.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_u64_or_default(args: &[String], key: &str, default: u64) -> u64 {
    parse_u64(args, key).unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let addr: SocketAddr = parse_arg(&args, "--addr")
        .unwrap_or_else(|| "127.0.0.1:0".to_string())
        .parse()?;

    let dir: PathBuf = match parse_arg(&args, "--dir") {
        Some(v) => PathBuf::from(v),
        None => {
            let port = addr.port();
            PathBuf::from(format!("./data/storage-{port}"))
        }
    };

    let mut maintenance = StorageMaintenanceConfig::default();
    if let Some(v) = parse_u64(&args, "--checkpoint-interval-secs") {
        maintenance.checkpoint.interval = std::time::Duration::from_secs(v.max(1));
    }
    if let Some(v) = parse_usize(&args, "--checkpoint-max-dirty-pages") {
        maintenance.checkpoint.max_dirty_pages = v.max(1);
    }
    if let Some(v) = parse_usize(&args, "--checkpoint-max-dirty-bytes") {
        maintenance.checkpoint.max_dirty_bytes = v.max(1);
    }
    if let Some(v) = parse_bool(&args, "--wal-truncate") {
        maintenance.truncate_wal = v;
    }
    if let Some(v) = parse_u64(&args, "--wal-max-bytes") {
        maintenance.max_wal_bytes = v.max(1);
    }
    if let Some(v) = parse_usize(&args, "--wal-max-segments") {
        maintenance.max_wal_segments = v.max(1);
    }
    if let Some(v) = parse_usize(&args, "--mvcc-gc-every-wal-batches") {
        maintenance.mvcc_gc_every_wal_batches = v.max(1);
    }
    if let Some(v) = parse_usize(&args, "--wal-group-max-batches") {
        maintenance.wal_group_commit_max_batches = v.max(1);
    }
    if let Some(v) = parse_u64(&args, "--wal-group-wait-us") {
        maintenance.wal_group_commit_wait_us = v;
    }
    let metrics_interval_secs = parse_u64_or_default(&args, "--metrics-interval-secs", 0);
    let metrics_format = parse_arg(&args, "--metrics-format").unwrap_or_else(|| "json".to_string());

    eprintln!(
        "storage_server effective_config: {}",
        maintenance.render_json()
    );

    let server =
        StorageServer::start_with_dir_and_maintenance(addr, dir.clone(), maintenance).await?;
    eprintln!(
        "storage_server listening on {} (dir={})",
        server.addr(),
        dir.display()
    );
    if metrics_interval_secs > 0 {
        let node = server.data_arc();
        let fmt = metrics_format.to_ascii_lowercase();
        tokio::spawn(async move {
            let interval_secs = metrics_interval_secs.max(1);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
                let snap = node.metrics_snapshot().await;
                if fmt == "prom" || fmt == "prometheus" {
                    eprintln!("[metrics]\n{}", snap.render_prometheus());
                } else {
                    eprintln!("[metrics] {}", snap.render_json());
                }
            }
        });
    }

    // park forever
    futures::future::pending::<()>().await;
    Ok(())
}
