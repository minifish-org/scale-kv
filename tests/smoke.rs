use scale_kv::{ComputeNode, PAGE_SIZE};

#[tokio::test(flavor = "current_thread")]
async fn test_smoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let compute = ComputeNode::new();
            let value1 = vec![b'a'; PAGE_SIZE / 4];
            let value2 = vec![b'b'; PAGE_SIZE / 4];

            compute.put("key1", &value1).await.unwrap();
            assert_eq!(
                compute.get("key1").await.unwrap(),
                Some(value1.clone())
            );

            compute.put("key1", &value2).await.unwrap();
            assert_eq!(
                compute.get("key1").await.unwrap(),
                Some(value2.clone())
            );

            compute.delete("key1").await.unwrap();
            assert_eq!(compute.get("key1").await.unwrap(), None);
        })
        .await;
}
