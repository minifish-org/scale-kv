use scale_kv::{ComputeNode, StorageServer};
use std::net::SocketAddr;

#[tokio::test(flavor = "current_thread")]
async fn test_network_operations() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start(addr).await.unwrap();

            let compute = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();

            compute.put("network_key", b"network_value").await.unwrap();
            assert_eq!(
                compute.get("network_key").await.unwrap(),
                Some(b"network_value".to_vec())
            );

            compute.put("network_key", b"updated_value").await.unwrap();
            assert_eq!(
                compute.get("network_key").await.unwrap(),
                Some(b"updated_value".to_vec())
            );

            compute.delete("network_key").await.unwrap();
            assert_eq!(compute.get("network_key").await.unwrap(), None);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_clients() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start(addr).await.unwrap();

            let compute1 = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();
            let compute2 = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();

            compute1.put("shared_key", b"from_client1").await.unwrap();
            assert_eq!(
                compute1.get("shared_key").await.unwrap(),
                Some(b"from_client1".to_vec())
            );

            assert_eq!(
                compute2.get("shared_key").await.unwrap(),
                Some(b"from_client1".to_vec())
            );

            compute2.put("shared_key", b"from_client2").await.unwrap();
            assert_eq!(
                compute2.get("shared_key").await.unwrap(),
                Some(b"from_client2".to_vec())
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_streaming() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start(addr).await.unwrap();
            let compute = ComputeNode::with_storage(&server.addr().to_string())
                .await
                .unwrap();

            compute.put("k1", b"v1").await.unwrap();
            compute.put("k2", b"v2").await.unwrap();

            let stream = compute.open_stream().await.unwrap();
            let (items, done) = stream.next(10).await.unwrap();
            let keys: std::collections::HashSet<_> =
                items.into_iter().map(|(k, _)| k).collect();
            assert!(keys.contains("k1"));
            assert!(keys.contains("k2"));
            assert!(done);
        })
        .await;
}
