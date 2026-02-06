use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

use scale_kv::{EmbeddedCompute, StorageServer};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("scale-kv-embedded-e2e-{}-{}", process::id(), id));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_embedded_compute_put_get_and_txn_commit() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.clone())
                .await
                .unwrap();

            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            // Auto-txn single op.
            let c1 = compute.put(b"k1".to_vec(), b"v1".to_vec()).await.unwrap();
            let v = compute.get_at(b"k1", c1).unwrap();
            assert_eq!(v, Some(b"v1".to_vec()));

            // Buffered txn with 2 ops.
            let read_before = compute.begin_ro();
            let mut txn = compute.begin();
            txn.put(b"k2".to_vec(), b"v2".to_vec());
            txn.delete(b"k1".to_vec());
            let commit_lsn = txn.commit().await.unwrap();

            // Old snapshot: k2 absent and k1 still present.
            let old_k2 = compute.get_at(b"k2", read_before).unwrap();
            assert_eq!(old_k2, None);
            let old_k1 = compute.get_at(b"k1", read_before).unwrap();
            assert_eq!(old_k1, Some(b"v1".to_vec()));

            // New snapshot at commit point.
            let new_k2 = compute.get_at(b"k2", commit_lsn).unwrap();
            assert_eq!(new_k2, Some(b"v2".to_vec()));
            let new_k1 = compute.get_at(b"k1", commit_lsn).unwrap();
            assert_eq!(new_k1, None);
        })
        .await;

    cleanup_dir(&dir);
}
