use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use scale_kv::node::{WalBatch, WalRecord};
use scale_kv::{StorageClient, StorageNode, StorageServer, PAGE_SIZE};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

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

fn make_page(fill: u8) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    page[0] = fill;
    page[PAGE_SIZE - 1] = fill;
    page
}

#[tokio::test]
async fn test_storage_crud_persists() {
    let dir = temp_dir();
    let page1 = make_page(1);
    let page2 = make_page(2);
    {
        let node = StorageNode::open(&dir).await.unwrap();
        node.put(1, &page1);
        node.put(2, &page2);
        assert_eq!(node.get(1).await.unwrap()[0], 1);
        node.delete(1);
        assert_eq!(node.get(1).await, None);
        node.checkpoint().await.unwrap();
    }
    let node = StorageNode::open(&dir).await.unwrap();
    assert_eq!(node.get(2).await.unwrap()[0], 2);
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_append_wal_applies_to_storage() {
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
            let record = WalRecord {
                lsn: 1,
                op: 1,
                page_id: 1,
                slot_id: 0,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            };
            let batch = WalBatch {
                request_id: 0,
                start_lsn: 1,
                end_lsn: 2,
                records: vec![record],
            };
            let _durable = client.append_wal(&batch).await.unwrap();

            assert!(client.get(1).await.unwrap().is_none());
        })
        .await;
    cleanup_dir(&dir);
}
