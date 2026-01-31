use scale_kv::{ComputeNode, PageId, PAGE_SIZE};

#[tokio::test(flavor = "current_thread")]
async fn test_smoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let compute = ComputeNode::new();
            let page_id: PageId = 1;
            let value1 = vec![b'a'; PAGE_SIZE];
            let value2 = vec![b'b'; PAGE_SIZE];

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
