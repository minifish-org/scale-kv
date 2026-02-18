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

fn build_maintenance_from_args(args: &[String]) -> StorageMaintenanceConfig {
    let mut maintenance = StorageMaintenanceConfig::default();
    if let Some(v) = parse_u64(args, "--checkpoint-interval-secs") {
        maintenance.checkpoint.interval = std::time::Duration::from_secs(v.max(1));
    }
    if let Some(v) = parse_usize(args, "--checkpoint-max-dirty-pages") {
        maintenance.checkpoint.max_dirty_pages = v.max(1);
    }
    if let Some(v) = parse_usize(args, "--checkpoint-max-dirty-bytes") {
        maintenance.checkpoint.max_dirty_bytes = v.max(1);
    }
    if let Some(v) = parse_bool(args, "--wal-truncate") {
        maintenance.truncate_wal = v;
    }
    if let Some(v) = parse_u64(args, "--wal-max-bytes") {
        maintenance.max_wal_bytes = v.max(1);
    }
    if let Some(v) = parse_usize(args, "--wal-max-segments") {
        maintenance.max_wal_segments = v.max(1);
    }
    if let Some(v) = parse_usize(args, "--mvcc-gc-every-wal-batches") {
        maintenance.mvcc_gc_every_wal_batches = v.max(1);
    }
    if let Some(v) = parse_usize(args, "--wal-group-max-batches") {
        maintenance.wal_group_commit_max_batches = v.max(1);
    }
    if let Some(v) = parse_u64(args, "--wal-group-wait-us") {
        maintenance.wal_group_commit_wait_us = v;
    }
    maintenance
}

async fn run_with_args(args: &[String], run_forever: bool) -> anyhow::Result<()> {
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

    let maintenance = build_maintenance_from_args(&args);
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

    if run_forever {
        futures::future::pending::<()>().await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    run_with_args(&args, true).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bool_and_numeric_helpers() {
        let args = vec![
            "storage_server".to_string(),
            "--wal-truncate".to_string(),
            "yes".to_string(),
            "--metrics-interval-secs".to_string(),
            "9".to_string(),
        ];
        assert_eq!(parse_bool(&args, "--wal-truncate"), Some(true));
        assert_eq!(parse_u64_or_default(&args, "--metrics-interval-secs", 0), 9);
        assert_eq!(parse_u64_or_default(&args, "--missing", 3), 3);

        let args = vec![
            "storage_server".to_string(),
            "--wal-truncate".to_string(),
            "OFF".to_string(),
        ];
        assert_eq!(parse_bool(&args, "--wal-truncate"), Some(false));
        assert_eq!(parse_bool(&args, "--x"), None);
    }

    #[test]
    fn test_build_maintenance_applies_clamps() {
        let args = vec![
            "storage_server".to_string(),
            "--checkpoint-interval-secs".to_string(),
            "0".to_string(),
            "--checkpoint-max-dirty-pages".to_string(),
            "0".to_string(),
            "--checkpoint-max-dirty-bytes".to_string(),
            "0".to_string(),
            "--wal-truncate".to_string(),
            "false".to_string(),
            "--wal-max-bytes".to_string(),
            "0".to_string(),
            "--wal-max-segments".to_string(),
            "0".to_string(),
            "--mvcc-gc-every-wal-batches".to_string(),
            "0".to_string(),
            "--wal-group-max-batches".to_string(),
            "0".to_string(),
            "--wal-group-wait-us".to_string(),
            "123".to_string(),
        ];
        let cfg = build_maintenance_from_args(&args);
        assert_eq!(cfg.checkpoint.interval.as_secs(), 1);
        assert_eq!(cfg.checkpoint.max_dirty_pages, 1);
        assert_eq!(cfg.checkpoint.max_dirty_bytes, 1);
        assert!(!cfg.truncate_wal);
        assert_eq!(cfg.max_wal_bytes, 1);
        assert_eq!(cfg.max_wal_segments, 1);
        assert_eq!(cfg.mvcc_gc_every_wal_batches, 1);
        assert_eq!(cfg.wal_group_commit_max_batches, 1);
        assert_eq!(cfg.wal_group_commit_wait_us, 123);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_run_with_args_startup_smoke_and_metrics_branch() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("srv");
        let args = vec![
            "storage_server".to_string(),
            "--addr".to_string(),
            "127.0.0.1:0".to_string(),
            "--dir".to_string(),
            dir.display().to_string(),
            "--metrics-interval-secs".to_string(),
            "1".to_string(),
            "--metrics-format".to_string(),
            "prom".to_string(),
        ];
        run_with_args(&args, false).await.unwrap();
    }
}
