use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

use scale_kv::{StorageClient, StorageServer};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("scale-kv-txn-e2e-{}-{}", process::id(), id));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

// txn op values (capnp TxnRecord.op)
const OP_PUT: u8 = 1;
const OP_DEL: u8 = 2;
const OP_COMMIT: u8 = 3;

#[tokio::test(flavor = "current_thread")]
async fn test_txn_snapshot_mvcc_and_idempotent_commit() {
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
            let client = StorageClient::connect(&server.addr().to_string(), &local)
                .await
                .unwrap();

            // Cold-start init: fetch durable LSN.
            let read_lsn = client.get_durable_lsn().await.unwrap();

            // Commit a txn: PUT k=v then COMMIT.
            let key = b"k1".to_vec();
            let val = b"v1".to_vec();
            let request_id = 42u64;
            let records = vec![
                (OP_PUT, key.clone(), val.clone()),
                (OP_COMMIT, Vec::new(), Vec::new()),
            ];
            let (commit_lsn_1, durable_1) =
                client.append_txn_batch(request_id, &records).await.unwrap();
            assert!(durable_1 >= commit_lsn_1);

            // Old snapshot must not see the new write.
            let (seen_old, _durable_old) = client.txn_get(&key, read_lsn).await.unwrap();
            assert_eq!(seen_old, None);

            // New snapshot at commit point must see it.
            let (seen_new, _durable_new) = client.txn_get(&key, commit_lsn_1).await.unwrap();
            assert_eq!(seen_new, Some(val.clone()));

            // Idempotent retry with same requestId should return same commitLsn.
            let (commit_lsn_2, _durable_2) =
                client.append_txn_batch(request_id, &records).await.unwrap();
            assert_eq!(commit_lsn_1, commit_lsn_2);

            // A new txn deleting the key.
            let request_id2 = 43u64;
            let records2 = vec![
                (OP_DEL, key.clone(), Vec::new()),
                (OP_COMMIT, Vec::new(), Vec::new()),
            ];
            let (commit_lsn_del, _durable_del) = client
                .append_txn_batch(request_id2, &records2)
                .await
                .unwrap();

            // Snapshot at commit_lsn_1 still sees v1.
            let (seen_before_del, _) = client.txn_get(&key, commit_lsn_1).await.unwrap();
            assert_eq!(seen_before_del, Some(val));

            // Snapshot at delete commit point sees None.
            let (seen_after_del, _) = client.txn_get(&key, commit_lsn_del).await.unwrap();
            assert_eq!(seen_after_del, None);
        })
        .await;

    cleanup_dir(&dir);
}
