use crate::Value;
use dashmap::DashMap;

/// Storage node - holds data and handles requests from compute nodes.
pub struct StorageNode {
    data: DashMap<String, Value>,
}

impl StorageNode {
    /// Create a new empty storage node.
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    /// Get the number of keys in storage.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if storage is empty.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Put a key-value pair.
    pub fn put(&self, key: impl AsRef<str>, value: &[u8]) {
        self.data.insert(key.as_ref().to_string(), value.to_vec());
    }

    /// Get a value by key.
    pub fn get(&self, key: impl AsRef<str>) -> Option<Value> {
        self.data.get(key.as_ref()).map(|v| v.clone())
    }

    /// Delete a key.
    pub fn delete(&self, key: impl AsRef<str>) {
        self.data.remove(key.as_ref());
    }

    /// Check if key exists.
    pub fn contains(&self, key: impl AsRef<str>) -> bool {
        self.data.contains_key(key.as_ref())
    }

    pub fn keys(&self) -> Vec<String> {
        self.data.iter().map(|entry| entry.key().clone()).collect()
    }
}

impl Default for StorageNode {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let node = StorageNode::new();
        node.put("foo", b"bar");
        assert_eq!(node.get("foo"), Some(b"bar".to_vec()));
    }

    #[test]
    fn test_get_missing() {
        let node = StorageNode::new();
        assert_eq!(node.get("missing"), None);
    }

    #[test]
    fn test_overwrite() {
        let node = StorageNode::new();
        node.put("foo", b"bar");
        node.put("foo", b"baz");
        assert_eq!(node.get("foo"), Some(b"baz".to_vec()));
    }

    #[test]
    fn test_delete() {
        let node = StorageNode::new();
        node.put("foo", b"bar");
        node.delete("foo");
        assert_eq!(node.get("foo"), None);
    }

    #[test]
    fn test_len() {
        let node = StorageNode::new();
        assert_eq!(node.len(), 0);
        node.put("a", b"1");
        node.put("b", b"2");
        assert_eq!(node.len(), 2);
    }

    #[test]
    fn test_contains() {
        let node = StorageNode::new();
        node.put("foo", b"bar");
        assert!(node.contains("foo"));
        assert!(!node.contains("missing"));
    }
}
