use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use scale_kv::node::{WalBatch, WalRecord};
use scale_kv::{StorageClient, StorageNode, StorageServer};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("scale-kv-storage-test-{}-{}", std::process::id(), id));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn test_storage_crud_persists() {
    let dir = temp_dir();
    {
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, b"one");
        node.put(2, b"two");
        assert_eq!(node.get(1), Some(b"one".to_vec()));
        node.delete(1);
        assert_eq!(node.get(1), None);
    }
    let node = StorageNode::open(&dir).unwrap();
    assert_eq!(node.get(2), Some(b"two".to_vec()));
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_append_wal_applies_to_storage() {
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
            let record = WalRecord {
                lsn: 1,
                op: 1,
                page_id: 1,
                slot_id: 0,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            };
            let batch = WalBatch {
                start_lsn: 1,
                end_lsn: 1,
                records: vec![record],
            };
            client.append_wal(&batch).await.unwrap();

            assert!(client.get(1).await.unwrap().is_none());
        })
        .await;
    cleanup_dir(&dir);
}
