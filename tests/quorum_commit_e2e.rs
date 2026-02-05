use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

use scale_kv::node::{WalBatch, WalRecord, WAL_OP_TXN_COMMIT, WAL_OP_TXN_DEL, WAL_OP_TXN_PUT};
use scale_kv::{ComputeSequencer, StorageClient, StorageServer};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir(tag: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("scale-kv-quorum-e2e-{}-{}-{}", tag, process::id(), id));
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
            let s1 = StorageServer::start_with_dir(addr, dir1.clone()).await.unwrap();
            let s2 = StorageServer::start_with_dir(addr, dir2.clone()).await.unwrap();
            let s3 = StorageServer::start_with_dir(addr, dir3.clone()).await.unwrap();

            let a1 = s1.addr().to_string();
            let a2 = s2.addr().to_string();
            let a3 = s3.addr().to_string();

            // Pre-advance s3 so it will reject the next batch from the sequencer.
            let c3 = StorageClient::connect(&a3, &local).await.unwrap();
            let mut start = c3.get_durable_lsn().await.unwrap();
            // Append 4 commit-only batches to push durable forward.
            for rid in 1..=4u64 {
                let rec = WalRecord {
                    lsn: start,
                    op: WAL_OP_TXN_COMMIT,
                    page_id: 0,
                    slot_id: 0,
                    key: Vec::new(),
                    value: Vec::new(),
                };
                let batch = WalBatch {
                    request_id: 10_000 + rid,
                    start_lsn: start,
                    end_lsn: start + 1,
                    records: vec![rec],
                };
                let durable = c3.append_wal(&batch).await.unwrap();
                start = durable;
            }

            let addrs = vec![a1, a2, a3];
            let seq = ComputeSequencer::connect(&addrs, 2, &local).await.unwrap();
            let read_lsn = seq.begin_ro();

            let key = b"kq".to_vec();
            let value = b"vq".to_vec();
            let records = vec![
                WalRecord {
                    lsn: 0,
                    op: WAL_OP_TXN_PUT,
                    page_id: 0,
                    slot_id: 0,
                    key: key.clone(),
                    value: value.clone(),
                },
                WalRecord {
                    lsn: 0,
                    op: WAL_OP_TXN_COMMIT,
                    page_id: 0,
                    slot_id: 0,
                    key: Vec::new(),
                    value: Vec::new(),
                },
            ];

            let commit_lsn = seq.commit_txn_batch(records).await.unwrap();
            assert!(commit_lsn > read_lsn);

            // s1 and s2 should see it at commit_lsn, s3 may not.
            let c1 = StorageClient::connect(&addrs[0], &local).await.unwrap();
            let c2 = StorageClient::connect(&addrs[1], &local).await.unwrap();
            let (v1, _) = c1.txn_get(&key, commit_lsn).await.unwrap();
            let (v2, _) = c2.txn_get(&key, commit_lsn).await.unwrap();
            assert_eq!(v1, Some(value.clone()));
            assert_eq!(v2, Some(value.clone()));

            // Old snapshot must not see it.
            let (v_old, _) = c1.txn_get(&key, read_lsn).await.unwrap();
            assert_eq!(v_old, None);

            // Now delete with another txn.
            let records2 = vec![
                WalRecord {
                    lsn: 0,
                    op: WAL_OP_TXN_DEL,
                    page_id: 0,
                    slot_id: 0,
                    key: key.clone(),
                    value: Vec::new(),
                },
                WalRecord {
                    lsn: 0,
                    op: WAL_OP_TXN_COMMIT,
                    page_id: 0,
                    slot_id: 0,
                    key: Vec::new(),
                    value: Vec::new(),
                },
            ];
            let commit2 = seq.commit_txn_batch(records2).await.unwrap();
            let (v_after, _) = c2.txn_get(&key, commit2).await.unwrap();
            assert_eq!(v_after, None);
        })
        .await;

    cleanup_dir(&dir1);
    cleanup_dir(&dir2);
    cleanup_dir(&dir3);
}
