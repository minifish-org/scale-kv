use std::sync::RwLock;

const MAX_KEYS: usize = 8;

pub struct BPlusTree<K: Ord + Clone> {
    root: RwLock<Node<K>>,
}

#[derive(Clone)]
enum Node<K: Ord + Clone> {
    Leaf {
        keys: Vec<K>,
    },
    Internal {
        keys: Vec<K>,
        children: Vec<Box<Node<K>>>,
    },
}

impl<K: Ord + Clone> Default for Node<K> {
    fn default() -> Self {
        Node::Leaf { keys: Vec::new() }
    }
}

impl<K: Ord + Clone> Default for BPlusTree<K> {
    fn default() -> Self {
        Self {
            root: RwLock::new(Node::default()),
        }
    }
}

impl<K: Ord + Clone> BPlusTree<K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, key: &K) -> bool {
        let root = self.root.read().unwrap();
        root.contains(key)
    }

    pub fn insert(&self, key: K) {
        let mut root = self.root.write().unwrap();
        if root.insert(key).is_some() {
            let (promote, right) = root.split();
            let left = Box::new(std::mem::take(&mut *root));
            *root = Node::Internal {
                keys: vec![promote],
                children: vec![left, right],
            };
        }
    }

    pub fn remove(&self, key: &K) {
        let mut root = self.root.write().unwrap();
        root.remove(key);
        if let Node::Internal { keys: _, children } = &mut *root {
            if children.len() == 1 {
                let child = children.pop().unwrap();
                *root = *child;
            }
        }
    }
}

impl<K: Ord + Clone> Node<K> {
    fn contains(&self, key: &K) -> bool {
        match self {
            Node::Leaf { keys } => keys.binary_search(key).is_ok(),
            Node::Internal { keys, children } => {
                let idx = child_index(keys, key);
                children[idx].contains(key)
            }
        }
    }

    fn insert(&mut self, key: K) -> Option<()> {
        match self {
            Node::Leaf { keys } => {
                match keys.binary_search(&key) {
                    Ok(_) => return None,
                    Err(pos) => keys.insert(pos, key),
                }
                if keys.len() > MAX_KEYS {
                    Some(())
                } else {
                    None
                }
            }
            Node::Internal { keys, children } => {
                let idx = child_index(keys, &key);
                if children[idx].insert(key).is_some() {
                    let (promote, right) = children[idx].split();
                    keys.insert(idx, promote);
                    children.insert(idx + 1, right);
                    if keys.len() > MAX_KEYS {
                        return Some(());
                    }
                }
                None
            }
        }
    }

    fn split(&mut self) -> (K, Box<Node<K>>) {
        match self {
            Node::Leaf { keys } => {
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let promote = right_keys[0].clone();
                (promote, Box::new(Node::Leaf { keys: right_keys }))
            }
            Node::Internal { keys, children } => {
                let mid = keys.len() / 2;
                let promote = keys[mid].clone();
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                keys.truncate(mid);
                (
                    promote,
                    Box::new(Node::Internal {
                        keys: right_keys,
                        children: right_children,
                    }),
                )
            }
        }
    }

    fn remove(&mut self, key: &K) {
        match self {
            Node::Leaf { keys } => {
                if let Ok(pos) = keys.binary_search(key) {
                    keys.remove(pos);
                }
            }
            Node::Internal { keys, children } => {
                let idx = child_index(keys, key);
                children[idx].remove(key);
            }
        }
    }
}

fn child_index<K: Ord + Clone>(keys: &[K], key: &K) -> usize {
    match keys.binary_search(key) {
        Ok(pos) => pos + 1,
        Err(pos) => pos,
    }
}

#[cfg(test)]
mod tests {
    use super::BPlusTree;

    #[test]
    fn test_insert_and_contains() {
        let tree = BPlusTree::new();
        for key in 1..=100u64 {
            tree.insert(key);
        }

        for key in 1..=100u64 {
            assert!(tree.contains(&key));
        }
        assert!(!tree.contains(&101));
    }

    #[test]
    fn test_remove_and_contains() {
        let tree = BPlusTree::new();
        for key in 1..=32u64 {
            tree.insert(key);
        }

        for key in (1..=32u64).step_by(2) {
            tree.remove(&key);
        }

        for key in 1..=32u64 {
            let expected = key % 2 == 0;
            assert_eq!(tree.contains(&key), expected);
        }
    }
}
