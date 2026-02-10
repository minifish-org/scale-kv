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

    let server =
        StorageServer::start_with_dir_and_maintenance(addr, dir.clone(), maintenance).await?;
    eprintln!(
        "storage_server listening on {} (dir={})",
        server.addr(),
        dir.display()
    );

    // park forever
    futures::future::pending::<()>().await;
    Ok(())
}
