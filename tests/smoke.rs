// Quick smoke test for scale-kv
use scale_kv::{ComputeNode, StorageNode};

#[test]
fn test_smoke() {
    let _storage = StorageNode::new();
    let mut compute = ComputeNode::new();

    // Basic operations
    compute.put("key1", b"value1");
    assert_eq!(compute.get("key1"), Some(b"value1".to_vec()));

    compute.put("key1", b"value2");
    assert_eq!(compute.get("key1"), Some(b"value2".to_vec()));

    compute.put("key2", b"value2");
    assert_eq!(compute.get("key2"), Some(b"value2".to_vec()));

    println!("Smoke test passed!");
}
