use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

use scale_kv::{StorageClient, StorageServer, PAGE_SIZE};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("scale-kv-net-test-{}-{}", process::id(), id));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rpc_put_get_delete_roundtrip() {
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

            let page = vec![b'x'; PAGE_SIZE];
            client.put(1, &page).await.unwrap();
            let stored = client.get(1).await.unwrap().unwrap();
            assert_eq!(stored.len(), PAGE_SIZE);

            assert!(client.delete(1).await.unwrap());
            assert_eq!(client.get(1).await.unwrap(), None);
        })
        .await;
    cleanup_dir(&dir);
}
