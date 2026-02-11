use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

use scale_kv::{
    ComputeSequencer, EmbeddedCompute, PAGE_SIZE, StorageMaintenanceConfig, StorageServer,
};
use std::time::Duration;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn unreachable_addr() -> String {
    // Reserve a port then drop the listener; address should be unreachable for immediate connect.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe port");
    let addr = listener.local_addr().expect("probe addr");
    drop(listener);
    addr.to_string()
}

fn temp_dir(tag: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!(
        "scale-kv-quorum-page-redo-e2e-{}-{}-{}",
        tag,
        process::id(),
        id
    ));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_quorum_commit_with_one_ahead_node() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir1 = temp_dir("s1");
    let dir2 = temp_dir("s2");
    let dir3 = temp_dir("s3");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let s1 = StorageServer::start_with_dir(addr, dir1.clone())
                .await
                .unwrap();
            let s2 = StorageServer::start_with_dir(addr, dir2.clone())
                .await
                .unwrap();
            let s3 = StorageServer::start_with_dir(addr, dir3.clone())
                .await
                .unwrap();

            let a1 = s1.addr().to_string();
            let a2 = s2.addr().to_string();
            let a3 = s3.addr().to_string();

            // Pre-advance s3 so it will reject the next batch from the sequencer.
            // Use an EmbeddedCompute connected only to s3 to append a few pages.
            let addrs3 = vec![a3.clone()];
            let c3 = EmbeddedCompute::connect(&addrs3, 1, &local).await.unwrap();
            for i in 0..4u64 {
                let page_id = 9000 + i;
                let mut page = vec![0u8; PAGE_SIZE];
                page[0] = i as u8;
                let _ = c3.write_page(page_id, page).await.unwrap();
            }

            let addrs = vec![a1, a2, a3];
            let seq = ComputeSequencer::connect(&addrs, 2, &local).await.unwrap();
            let read_lsn = seq.begin_ro();

            // Commit a single page write. s3 should reject due to being ahead.
            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 1;
            let writes = vec![(42u64, page)];
            let commit_lsn = seq.commit_txn_batch(writes).await.unwrap();
            assert!(commit_lsn > read_lsn);

            // s1 and s2 should have advanced durable_lsn to >= commit_lsn.
            let c1 = scale_kv::StorageClient::connect(&addrs[0], &local)
                .await
                .unwrap();
            let c2 = scale_kv::StorageClient::connect(&addrs[1], &local)
                .await
                .unwrap();
            let d1 = c1.get_durable_lsn().await.unwrap();
            let d2 = c2.get_durable_lsn().await.unwrap();
            assert!(d1 >= commit_lsn);
            assert!(d2 >= commit_lsn);
        })
        .await;

    cleanup_dir(&dir1);
    cleanup_dir(&dir2);
    cleanup_dir(&dir3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_quorum_commit_succeeds_with_one_backpressured_node() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir1 = temp_dir("bp-s1");
    let dir2 = temp_dir("bp-s2");
    let dir3 = temp_dir("bp-s3");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let s1 = StorageServer::start_with_dir(addr, dir1.clone())
                .await
                .unwrap();
            let s2 = StorageServer::start_with_dir(addr, dir2.clone())
                .await
                .unwrap();

            let mut bad_cfg = StorageMaintenanceConfig::default();
            bad_cfg.max_wal_bytes = 1;
            bad_cfg.truncate_wal = false;
            let s3 = StorageServer::start_with_dir_and_maintenance(addr, dir3.clone(), bad_cfg)
                .await
                .unwrap();

            let addrs = vec![
                s1.addr().to_string(),
                s2.addr().to_string(),
                s3.addr().to_string(),
            ];
            let seq = ComputeSequencer::connect(&addrs, 2, &local).await.unwrap();

            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 9;
            let writes = vec![(77u64, page)];

            let commit_lsn =
                tokio::time::timeout(Duration::from_secs(5), seq.commit_txn_batch(writes))
                    .await
                    .expect("commit timed out under partial failure")
                    .unwrap();

            let c1 = scale_kv::StorageClient::connect(&addrs[0], &local)
                .await
                .unwrap();
            let c2 = scale_kv::StorageClient::connect(&addrs[1], &local)
                .await
                .unwrap();
            let d1 = c1.get_durable_lsn().await.unwrap();
            let d2 = c2.get_durable_lsn().await.unwrap();
            assert!(d1 >= commit_lsn);
            assert!(d2 >= commit_lsn);
        })
        .await;

    cleanup_dir(&dir1);
    cleanup_dir(&dir2);
    cleanup_dir(&dir3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_quorum_commit_fails_when_quorum_requires_backpressured_node() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir1 = temp_dir("bp-fail-s1");
    let dir2 = temp_dir("bp-fail-s2");
    let dir3 = temp_dir("bp-fail-s3");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let s1 = StorageServer::start_with_dir(addr, dir1.clone())
                .await
                .unwrap();
            let s2 = StorageServer::start_with_dir(addr, dir2.clone())
                .await
                .unwrap();

            let mut bad_cfg = StorageMaintenanceConfig::default();
            bad_cfg.max_wal_bytes = 1;
            bad_cfg.truncate_wal = false;
            let s3 = StorageServer::start_with_dir_and_maintenance(addr, dir3.clone(), bad_cfg)
                .await
                .unwrap();

            let addrs = vec![
                s1.addr().to_string(),
                s2.addr().to_string(),
                s3.addr().to_string(),
            ];
            let seq = ComputeSequencer::connect(&addrs, 3, &local).await.unwrap();

            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 11;
            let writes = vec![(88u64, page)];
            let err = seq.commit_txn_batch(writes).await.unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("quorum not reached"));
        })
        .await;

    cleanup_dir(&dir1);
    cleanup_dir(&dir2);
    cleanup_dir(&dir3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_connect_allows_unreachable_nodes_if_quorum_reachable() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir1 = temp_dir("connect-ok-s1");
    let dir2 = temp_dir("connect-ok-s2");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let s1 = StorageServer::start_with_dir(addr, dir1.clone())
                .await
                .unwrap();
            let s2 = StorageServer::start_with_dir(addr, dir2.clone())
                .await
                .unwrap();
            let dead = unreachable_addr();

            let addrs = vec![s1.addr().to_string(), s2.addr().to_string(), dead];
            let seq = ComputeSequencer::connect(&addrs, 2, &local).await.unwrap();
            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 33;
            let commit = seq.commit_txn_batch(vec![(333, page)]).await.unwrap();
            assert!(commit > 0);
        })
        .await;

    cleanup_dir(&dir1);
    cleanup_dir(&dir2);
}

#[tokio::test(flavor = "current_thread")]
async fn test_connect_fails_if_reachable_nodes_below_quorum() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir1 = temp_dir("connect-fail-s1");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let s1 = StorageServer::start_with_dir(addr, dir1.clone())
                .await
                .unwrap();
            let dead1 = unreachable_addr();
            let dead2 = unreachable_addr();

            let addrs = vec![s1.addr().to_string(), dead1, dead2];
            let err = match ComputeSequencer::connect(&addrs, 2, &local).await {
                Ok(_) => panic!("expected connect failure"),
                Err(err) => err,
            };
            assert!(
                err.to_string()
                    .contains("not enough reachable storage nodes")
            );
        })
        .await;

    cleanup_dir(&dir1);
}
