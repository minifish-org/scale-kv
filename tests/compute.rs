use std::sync::atomic::{AtomicUsize, Ordering};

use scale_kv::{ComputeNode, StorageServer, KEY_SIZE, VALUE_SIZE};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("scale-kv-compute-test-{}-{}", std::process::id(), id));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn fixed_key(raw: &str) -> String {
    let mut out = raw.to_string();
    while out.len() < KEY_SIZE {
        out.push('_');
    }
    out.truncate(KEY_SIZE);
    out
}

#[tokio::test(flavor = "current_thread")]
async fn test_compute_basic_ops() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let compute = ComputeNode::new();
            let value1 = vec![b'a'; VALUE_SIZE];
            let value2 = vec![b'b'; VALUE_SIZE];

            let key1 = fixed_key("key1");
            compute.put(&key1, &value1).await.unwrap();
            assert_eq!(compute.get(&key1).await.unwrap(), Some(value1.clone()));
            assert!(compute.exists(&key1));

            compute.put(&key1, &value2).await.unwrap();
            assert_eq!(compute.get(&key1).await.unwrap(), Some(value2.clone()));

            assert!(compute.delete(&key1).await.unwrap());
            assert_eq!(compute.get(&key1).await.unwrap(), None);
            assert!(!compute.delete(&key1).await.unwrap());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_compute_multi_and_range() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let compute = ComputeNode::new();
            let v1 = vec![b'1'; VALUE_SIZE];
            let v2 = vec![b'2'; VALUE_SIZE];
            let v3 = vec![b'3'; VALUE_SIZE];
            let items = vec![
                (fixed_key("a1"), v1.clone()),
                (fixed_key("a2"), v2.clone()),
                (fixed_key("b1"), v3.clone()),
            ];
            compute.put_multi(&items).await.unwrap();

            let keys = vec![fixed_key("a1"), fixed_key("a2"), fixed_key("b1")];
            let values = compute.get_multi(&keys).await.unwrap();
            assert_eq!(values[0], Some(v1));
            assert_eq!(values[1], Some(v2));
            assert_eq!(values[2], Some(v3));

            let range = compute.range(&fixed_key("a1"), &fixed_key("a9")).await.unwrap();
            assert_eq!(range.len(), 2);
            assert_eq!(range[0].0, fixed_key("a1"));
            assert_eq!(range[1].0, fixed_key("a2"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_compute_wal_queue_async() {
    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.clone()).await.unwrap();
            let compute = ComputeNode::with_storage(&server.addr().to_string(), &local)
                .await
                .unwrap();

            let v1 = vec![b'1'; VALUE_SIZE];
            let v2 = vec![b'2'; VALUE_SIZE];
            let w1 = fixed_key("w1");
            let w2 = fixed_key("w2");
            compute.put(&w1, &v1).await.unwrap();
            compute.put(&w2, &v2).await.unwrap();

            // WAL is async; we only assert it doesn't block local operations.
            assert_eq!(compute.get(&w1).await.unwrap(), Some(v1));
            assert_eq!(compute.get(&w2).await.unwrap(), Some(v2));
        })
        .await;
    cleanup_dir(&dir);
}
