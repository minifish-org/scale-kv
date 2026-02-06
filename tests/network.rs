use scale_kv::{ComputeNode, KEY_SIZE, StorageServer, VALUE_SIZE};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

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

fn fixed_key(raw: &str) -> String {
    let mut out = raw.to_string();
    while out.len() < KEY_SIZE {
        out.push('_');
    }
    out.truncate(KEY_SIZE);
    out
}

#[tokio::test(flavor = "current_thread")]
async fn test_network_operations() {
    if !tcp_bind_allowed() {
        return;
    }
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let value1 = vec![b'a'; VALUE_SIZE];
                let value2 = vec![b'b'; VALUE_SIZE];
                let key = fixed_key("network_key");

                let compute = ComputeNode::with_storage(&server.addr().to_string(), &local)
                    .await
                    .unwrap();

                compute.put(&key, &value1).await.unwrap();
                assert_eq!(compute.get(&key).await.unwrap(), Some(value1.clone()));
                assert!(compute.exists(&key));

                compute.put(&key, &value2).await.unwrap();
                assert_eq!(compute.get(&key).await.unwrap(), Some(value2.clone()));

                assert!(compute.delete(&key).await.unwrap());
                assert_eq!(compute.get(&key).await.unwrap(), None);
            })
            .await;
    }
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_clients() {
    if !tcp_bind_allowed() {
        return;
    }
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let value1 = vec![b'1'; VALUE_SIZE];
                let value2 = vec![b'2'; VALUE_SIZE];
                let key = fixed_key("shared_key");

                let compute1 = ComputeNode::with_storage(&server.addr().to_string(), &local)
                    .await
                    .unwrap();
                let compute2 = ComputeNode::with_storage(&server.addr().to_string(), &local)
                    .await
                    .unwrap();

                compute1.put(&key, &value1).await.unwrap();
                assert_eq!(compute1.get(&key).await.unwrap(), Some(value1.clone()));

                assert_eq!(compute2.get(&key).await.unwrap(), None);

                compute2.put(&key, &value2).await.unwrap();
                assert_eq!(compute2.get(&key).await.unwrap(), Some(value2.clone()));
                assert_eq!(compute1.get(&key).await.unwrap(), Some(value1.clone()));
            })
            .await;
    }
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn test_batch_put() {
    if !tcp_bind_allowed() {
        return;
    }
    let dir = temp_dir();
    {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let server = StorageServer::start_with_dir(addr, dir.clone())
                    .await
                    .unwrap();
                let compute = ComputeNode::with_storage(&server.addr().to_string(), &local)
                    .await
                    .unwrap();
                let v1 = vec![b'1'; VALUE_SIZE];
                let v2 = vec![b'2'; VALUE_SIZE];

                let items = vec![(fixed_key("b1"), v1.clone()), (fixed_key("b2"), v2.clone())];
                compute.batch_put(&items).await.unwrap();

                assert_eq!(compute.get(&fixed_key("b1")).await.unwrap(), Some(v1));
                assert_eq!(compute.get(&fixed_key("b2")).await.unwrap(), Some(v2));
            })
            .await;
    }
    cleanup_dir(&dir);
}
