use scale_kv::ComputeNode;

#[tokio::test(flavor = "current_thread")]
async fn test_smoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let compute = ComputeNode::new();

            compute.put("key1", b"value1").await.unwrap();
            assert_eq!(
                compute.get("key1").await.unwrap(),
                Some(b"value1".to_vec())
            );

            compute.put("key1", b"value2").await.unwrap();
            assert_eq!(
                compute.get("key1").await.unwrap(),
                Some(b"value2".to_vec())
            );

            compute.delete("key1").await.unwrap();
            assert_eq!(compute.get("key1").await.unwrap(), None);
        })
        .await;
}
