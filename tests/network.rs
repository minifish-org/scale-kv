use scale_kv::{ComputeNode, PageId, StorageServer, PAGE_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

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
async fn test_network_operations() {
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let page_id: PageId = 1;
                let value1 = vec![b'a'; PAGE_SIZE];
                let value2 = vec![b'b'; PAGE_SIZE];

            let compute = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();

            compute.put(page_id, &value1).await.unwrap();
            assert_eq!(
                compute.get(page_id).await.unwrap(),
                Some(value1.clone())
            );

            compute.put(page_id, &value2).await.unwrap();
            assert_eq!(
                compute.get(page_id).await.unwrap(),
                Some(value2.clone())
            );

            compute.delete(page_id).await.unwrap();
            assert_eq!(compute.get(page_id).await.unwrap(), None);
            })
            .await;
    }
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_clients() {
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let page_id: PageId = 42;
                let value1 = vec![b'1'; PAGE_SIZE];
                let value2 = vec![b'2'; PAGE_SIZE];

            let compute1 = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();
            let compute2 = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();

            compute1.put(page_id, &value1).await.unwrap();
            assert_eq!(
                compute1.get(page_id).await.unwrap(),
                Some(value1.clone())
            );

            assert_eq!(
                compute2.get(page_id).await.unwrap(),
                Some(value1.clone())
            );

            compute2.put(page_id, &value2).await.unwrap();
            assert_eq!(
                compute2.get(page_id).await.unwrap(),
                Some(value2.clone())
            );
            })
            .await;
    }
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_streaming() {
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let compute = ComputeNode::with_storage(&server.addr().to_string())
                    .await
                    .unwrap();
                let v1 = vec![b'1'; PAGE_SIZE];
                let v2 = vec![b'2'; PAGE_SIZE];

            compute.put(1, &v1).await.unwrap();
            compute.put(2, &v2).await.unwrap();

            let stream = compute.open_stream().await.unwrap();
            let (items, done) = stream.next(10).await.unwrap();
            let keys: std::collections::HashSet<_> =
                items.into_iter().map(|(k, _)| k).collect();
            assert!(keys.contains(&1));
            assert!(keys.contains(&2));
            assert!(done);
            })
            .await;
    }
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_batch_put() {
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let compute = ComputeNode::with_storage(&server.addr().to_string())
                    .await
                    .unwrap();
                let v1 = vec![b'1'; PAGE_SIZE];
                let v2 = vec![b'2'; PAGE_SIZE];

            let items = vec![(1u64, v1.clone()), (2u64, v2.clone())];
            compute.batch_put(&items).await.unwrap();

            assert_eq!(compute.get(1).await.unwrap(), Some(v1));
            assert_eq!(compute.get(2).await.unwrap(), Some(v2));
            })
            .await;
    }
    cleanup_dir(&dir);
}
