use scale_kv::{ComputeNode, KEY_SIZE, VALUE_SIZE};

fn fixed_key(raw: &str) -> String {
    let mut out = raw.to_string();
    while out.len() < KEY_SIZE {
        out.push('_');
    }
    out.truncate(KEY_SIZE);
    out
}

#[tokio::test(flavor = "current_thread")]
async fn test_smoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let compute = ComputeNode::new();
            let value1 = vec![b'a'; VALUE_SIZE];
            let value2 = vec![b'b'; VALUE_SIZE];

            let key = fixed_key("key1");
            compute.put(&key, &value1).await.unwrap();
            assert_eq!(compute.get(&key).await.unwrap(), Some(value1.clone()));
            assert!(compute.exists(&key));

            compute.put(&key, &value2).await.unwrap();
            assert_eq!(compute.get(&key).await.unwrap(), Some(value2.clone()));

            assert!(compute.delete(&key).await.unwrap());
            assert_eq!(compute.get(&key).await.unwrap(), None);
            assert!(!compute.delete(&key).await.unwrap());
        })
        .await;
}
