use bytes::Bytes;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

use scale_kv::{EmbeddedCompute, ErrorCategory, KEY_SIZE, PAGE_SIZE, StorageServer, VALUE_SIZE};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> TempDir {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    tempfile::Builder::new()
        .prefix(&format!("scale-kv-embedded-page-redo-e2e-{id}-"))
        .tempdir()
        .expect("failed to create temp dir")
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
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();

            let addrs = vec![server.addr().to_string()];
            let compute1 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            // Commit a single page after-image.
            let page_id = 42u64;
            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 7;
            let commit_lsn = compute1
                .write_page(page_id, page.clone().into())
                .await
                .unwrap();
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
}

fn test_key(n: u64) -> Vec<u8> {
    let mut k = vec![0u8; KEY_SIZE];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k
}

fn test_value(b: u8) -> Bytes {
    Bytes::from(vec![b; VALUE_SIZE])
}

fn test_value_with_tag(fill: u8, tag: [u8; 2]) -> Bytes {
    let mut v = vec![fill; VALUE_SIZE];
    v[0..2].copy_from_slice(&tag);
    Bytes::from(v)
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
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
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
}

#[tokio::test(flavor = "current_thread")]
async fn test_secondary_btree_index_create_and_query_eq() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            compute
                .create_btree_secondary_index("tag", 0, 2)
                .await
                .unwrap();
            let defs = compute.list_secondary_indexes().await;
            assert_eq!(defs.len(), 1);
            assert_eq!(defs[0].name, "tag");

            let k1 = test_key(101);
            let k2 = test_key(102);
            let v1 = test_value_with_tag(1, *b"aa");
            let v2 = test_value_with_tag(2, *b"bb");
            compute.put(&k1, &v1).await.unwrap();
            compute.put(&k2, &v2).await.unwrap();

            let r1 = compute
                .scan_secondary_index_eq("tag", b"aa", 10)
                .await
                .unwrap();
            assert_eq!(r1.len(), 1);
            assert_eq!(r1[0], (k1.clone(), v1.clone()));

            let r2 = compute
                .scan_secondary_index_eq("tag", b"bb", 10)
                .await
                .unwrap();
            assert_eq!(r2.len(), 1);
            assert_eq!(r2[0], (k2.clone(), v2.clone()));

            assert!(compute.drop_secondary_index("tag").await.unwrap());
            assert!(!compute.drop_secondary_index("tag").await.unwrap());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_secondary_btree_index_snapshot_visibility() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            compute
                .create_btree_secondary_index("tag", 0, 2)
                .await
                .unwrap();

            let key = test_key(201);
            let v_old = test_value_with_tag(3, *b"aa");
            let v_new = test_value_with_tag(4, *b"bb");
            compute.put(&key, &v_old).await.unwrap();

            let mut ro = compute.begin_ro_timeout(Duration::from_secs(10));
            compute.put(&key, &v_new).await.unwrap();

            let snap_old = ro.scan_secondary_index_eq("tag", b"aa", 10).await.unwrap();
            assert_eq!(snap_old, vec![(key.clone(), v_old.clone())]);

            let now_old = compute
                .scan_secondary_index_eq("tag", b"aa", 10)
                .await
                .unwrap();
            assert!(now_old.is_empty());

            let now_new = compute
                .scan_secondary_index_eq("tag", b"bb", 10)
                .await
                .unwrap();
            assert_eq!(now_new, vec![(key.clone(), v_new.clone())]);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_secondary_btree_index_backfill_existing_rows() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let k1 = test_key(301);
            let k2 = test_key(302);
            let v1 = test_value_with_tag(5, *b"aa");
            let v2 = test_value_with_tag(6, *b"aa");
            compute.put(&k1, &v1).await.unwrap();
            compute.put(&k2, &v2).await.unwrap();

            compute
                .create_btree_secondary_index("tag", 0, 2)
                .await
                .unwrap();
            let got = compute
                .scan_secondary_index_eq("tag", b"aa", 10)
                .await
                .unwrap();
            assert_eq!(got.len(), 2);
            assert_eq!(got[0], (k1.clone(), v1.clone()));
            assert_eq!(got[1], (k2.clone(), v2.clone()));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_secondary_index_catalog_persist_and_recover() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];

            let compute1 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();
            let key = test_key(401);
            let val = test_value_with_tag(8, *b"eu");
            compute1.put(&key, &val).await.unwrap();
            compute1
                .create_btree_secondary_index("region", 0, 2)
                .await
                .unwrap();
            let defs = compute1.list_secondary_indexes().await;
            assert_eq!(defs.len(), 1);
            assert_eq!(defs[0].name, "region");

            let compute2 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();
            let defs2 = compute2.list_secondary_indexes().await;
            assert_eq!(defs2.len(), 1);
            assert_eq!(defs2[0].name, "region");
            let got = compute2
                .scan_secondary_index_eq("region", b"eu", 10)
                .await
                .unwrap();
            assert_eq!(got, vec![(key.clone(), val.clone())]);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_concurrent_rw_txns_on_different_keys_both_commit() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let k1 = test_key(2001);
            let k2 = test_key(2002);
            let v1 = test_value(11);
            let v2 = test_value(22);
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

            let compute2 = compute.clone();
            let barrier1 = barrier.clone();
            let k1_task = k1.clone();
            let v1_task = v1.clone();
            let t1 = tokio::task::spawn_local(async move {
                let mut tx = compute2.begin_rw().await;
                barrier1.wait().await;
                tx.put(&k1_task, &v1_task).await.unwrap();
                tx.commit().await.unwrap();
            });

            let compute3 = compute.clone();
            let barrier2 = barrier.clone();
            let k2_task = k2.clone();
            let v2_task = v2.clone();
            let t2 = tokio::task::spawn_local(async move {
                let mut tx = compute3.begin_rw().await;
                barrier2.wait().await;
                tx.put(&k2_task, &v2_task).await.unwrap();
                tx.commit().await.unwrap();
            });

            t1.await.unwrap();
            t2.await.unwrap();

            assert_eq!(compute.get(&k1).await.unwrap(), Some(v1));
            assert_eq!(compute.get(&k2).await.unwrap(), Some(v2));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_concurrent_rw_txns_on_same_key_conflict_is_retryable() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let key = test_key(3001);
            let winner = test_value(7);
            let loser = test_value(9);

            let (intent_ready_tx, intent_ready_rx) = tokio::sync::oneshot::channel();
            let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();

            let compute2 = compute.clone();
            let key2 = key.clone();
            let winner2 = winner.clone();
            let writer = tokio::task::spawn_local(async move {
                let mut tx = compute2.begin_rw().await;
                tx.put(&key2, &winner2).await.unwrap();
                let _ = intent_ready_tx.send(());
                let _ = commit_rx.await;
                tx.commit().await.unwrap();
            });

            intent_ready_rx.await.unwrap();

            let mut tx2 = compute.begin_rw().await;
            let err = tx2.put(&key, &loser).await.unwrap_err();
            assert_eq!(err.category(), ErrorCategory::Backpressure);
            assert!(err.is_retryable());
            assert!(
                matches!(err, scale_kv::Error::Io(ref ioe) if ioe.kind() == ErrorKind::WouldBlock)
            );
            tx2.abort().await.unwrap();

            let _ = commit_tx.send(());
            writer.await.unwrap();

            assert_eq!(compute.get(&key).await.unwrap(), Some(winner));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_timeout_reaper_aborts_dropped_rw_txn_and_resolves_intent() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let key = test_key(42);
            let old_value = test_value(7);
            let new_value = test_value(9);
            compute.put(&key, &old_value).await.unwrap();

            {
                let mut tx = compute.begin_rw_timeout(Duration::from_millis(100)).await;
                tx.put(&key, &new_value).await.unwrap();
                // Drop without commit/abort.
            }

            tokio::time::sleep(Duration::from_millis(350)).await;
            let got = compute.get(&key).await.unwrap();
            assert_eq!(got, Some(old_value));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_reader_waits_on_foreign_intent_until_commit() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let key = test_key(99);
            let old_value = test_value(1);
            let pending = test_value(2);
            compute.put(&key, &old_value).await.unwrap();

            let mut tx = compute.begin_rw_timeout(Duration::from_secs(2)).await;
            tx.put(&key, &pending).await.unwrap();

            let compute2 = compute.clone();
            let key2 = key.clone();
            let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
            tokio::task::spawn_local(async move {
                let _ = done_tx.send(compute2.get(&key2).await);
            });

            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut done_rx)
                    .await
                    .is_err()
            );

            tx.commit().await.unwrap();

            let got = tokio::time::timeout(Duration::from_secs(2), &mut done_rx)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            // Reader snapshot was taken while the intent existed, so even after waiting for
            // commit it should read the old version if commit_lsn > read_lsn.
            assert_eq!(got, Some(old_value));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_reader_does_not_wait_when_intent_is_newer_than_snapshot() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let key = test_key(101);
            let old_value = test_value(5);
            let pending_value = test_value(6);
            compute.put(&key, &old_value).await.unwrap();

            let mut ro = compute.begin_ro_timeout(Duration::from_secs(2));
            // Advance global LSN after the RO snapshot so the upcoming intent_lsn (writer start_lsn)
            // is greater than ro.read_lsn, and RO should NOT wait.
            let bump_key = test_key(1);
            let bump_val = test_value(1);
            compute.put(&bump_key, &bump_val).await.unwrap();

            let mut tx = compute.begin_rw_timeout(Duration::from_secs(2)).await;
            tx.put(&key, &pending_value).await.unwrap();

            let got = tokio::time::timeout(Duration::from_millis(100), ro.get(&key))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got, Some(old_value));

            tx.abort().await.unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_reader_times_out_waiting_on_unresolved_foreign_intent() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.path().to_path_buf())
                .await
                .unwrap();
            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let key = test_key(100);
            let pending = test_value(3);

            let mut tx = compute.begin_rw_timeout(Duration::from_millis(120)).await;
            tx.put(&key, &pending).await.unwrap();

            let err = compute.get(&key).await.unwrap_err();
            assert!(err.is_retryable());
            assert_eq!(err.category(), ErrorCategory::Backpressure);

            let scan_err = compute.scan_range(&key, &key, 10).await.unwrap_err();
            assert!(scan_err.is_retryable());
            assert_eq!(scan_err.category(), ErrorCategory::Backpressure);

            tx.abort().await.unwrap();
        })
        .await;
}
