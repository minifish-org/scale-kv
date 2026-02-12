use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use std::{fs, process};

use scale_kv::{EmbeddedCompute, KEY_SIZE, PAGE_SIZE, StorageServer, VALUE_SIZE};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!(
        "scale-kv-embedded-page-redo-e2e-{}-{}",
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
async fn test_page_redo_commit_and_recover_by_scan() {
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
            let compute1 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            // Commit a single page after-image.
            let page_id = 42u64;
            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 7;
            let commit_lsn = compute1.write_page(page_id, page.clone()).await.unwrap();
            assert!(commit_lsn > 0);

            // New compute instance should be able to warm up by scanning pages.
            let compute2 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();
            let warmed = compute2.warmup_scan_all(256).await.unwrap();
            assert!(warmed >= 1);

            if compute2.cached_page(page_id).is_none() {
                // Debug: scan directly from storage
                let c = scale_kv::StorageClient::connect(&addrs[0], &local)
                    .await
                    .unwrap();
                let (pages, _) = c.scan_pages(0, 256).await.unwrap();
                panic!(
                    "page not warmed; scanPages returned ids: {:?}",
                    pages.iter().map(|(id, _, _)| *id).collect::<Vec<_>>()
                );
            }

            let got = compute2.cached_page(page_id).unwrap();
            assert_eq!(got, page);
        })
        .await;

    cleanup_dir(&dir);
}

fn test_key(n: u64) -> Vec<u8> {
    let mut k = vec![0u8; KEY_SIZE];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k
}

fn test_value(b: u8) -> Vec<u8> {
    vec![b; VALUE_SIZE]
}

#[tokio::test(flavor = "current_thread")]
async fn test_scan_range_snapshot_inclusive_and_limit() {
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

            let k1 = test_key(1);
            let k2 = test_key(2);
            let k3 = test_key(3);
            let v1 = test_value(1);
            let v2 = test_value(2);
            let v2_new = test_value(9);
            let v3 = test_value(3);

            compute.put(&k1, &v1).await.unwrap();
            compute.put(&k2, &v2).await.unwrap();
            compute.put(&k3, &v3).await.unwrap();

            // Capture a read snapshot, then overwrite k2 to force undo-chain visibility.
            let mut ro = compute.begin_ro_timeout(std::time::Duration::from_secs(10));
            compute.put(&k2, &v2_new).await.unwrap();

            let snap = ro.scan_range(&k1, &k2, 10).await.unwrap();
            assert_eq!(snap.len(), 2);
            assert_eq!(snap[0], (k1.clone(), v1.clone()));
            assert_eq!(snap[1], (k2.clone(), v2.clone()));

            // End bound is inclusive.
            let inclusive = compute.scan_range(&k2, &k2, 10).await.unwrap();
            assert_eq!(inclusive, vec![(k2.clone(), v2_new.clone())]);

            // Limit applies to visible rows.
            let limited = compute.scan_range(&k1, &k3, 2).await.unwrap();
            assert_eq!(limited.len(), 2);
            assert_eq!(limited[0], (k1, v1));
            assert_eq!(limited[1], (k2, v2_new));
        })
        .await;

    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rw_txn_is_serialized_by_single_writer_gate() {
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

            let mut tx1 = compute.begin_rw().await;
            tx1.write_page(100, vec![1u8; PAGE_SIZE]);

            let compute2 = compute.clone();
            let tx2_done = std::sync::Arc::new(AtomicBool::new(false));
            let tx2_done2 = tx2_done.clone();
            tokio::task::spawn_local(async move {
                let mut tx2 = compute2.begin_rw().await;
                tx2.write_page(101, vec![2u8; PAGE_SIZE]);
                tx2.commit().await.unwrap();
                tx2_done2.store(true, Ordering::Release);
            });

            assert!(
                tokio::time::timeout(Duration::from_millis(100), async {
                    while !tx2_done.load(Ordering::Acquire) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .is_err()
            );

            tx1.commit().await.unwrap();

            // After tx1 finishes, tx2 can acquire the writer gate and commit.
            tokio::time::timeout(Duration::from_secs(2), async {
                while !tx2_done.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        })
        .await;

    cleanup_dir(&dir);
}
