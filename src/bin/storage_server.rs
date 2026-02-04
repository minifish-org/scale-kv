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

use scale_kv::server::StorageServer;

fn parse_arg(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .cloned()
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

    let server = StorageServer::start_with_dir(addr, dir.clone()).await?;
    eprintln!("storage_server listening on {} (dir={})", server.addr(), dir.display());

    // park forever
    futures::future::pending::<()>().await;
    Ok(())
}
