use super::{AsyncPageProvider, InMemoryPageProvider, LeafValue, LeafValueRef, PageBPlusTree};
use crate::{KEY_SIZE, VALUE_SIZE};
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::task::LocalSet;

fn key_for(i: u32) -> Vec<u8> {
    let mut key = format!("k{:03}", i).into_bytes();
    while key.len() < KEY_SIZE {
        key.push(b'x');
    }
    key.truncate(KEY_SIZE);
    key
}

fn next_rand(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn assert_leaf_matches(got: Option<LeafValueRef>, expected: Option<&LeafValue>) {
    match (got, expected) {
        (None, None) => {}
        (Some(got), Some(expected)) => {
            assert_eq!(got.value.as_ref(), expected.value.as_slice());
            assert_eq!(got.meta.commit_lsn, expected.commit_lsn);
            assert_eq!(got.meta.undo_ptr, expected.undo_ptr);
            assert_eq!(got.meta.flags, expected.flags);
            assert_eq!(got.meta.intent_txn_id, expected.intent_txn_id);
            assert_eq!(got.meta.intent_lsn, expected.intent_lsn);
        }
        (left, right) => panic!("leaf mismatch: got={left:?} expected={right:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_get_remove_len() {
    let tree = PageBPlusTree::new();
    let key = key_for(1);
    let slot = LeafValue {
        value: [1u8; VALUE_SIZE],
        commit_lsn: 11,
        undo_ptr: None,
        flags: 0,
        intent_txn_id: 0,
        intent_lsn: 0,
    };
    let updated = LeafValue {
        value: [2u8; VALUE_SIZE],
        commit_lsn: 22,
        undo_ptr: None,
        flags: 1,
        intent_txn_id: 0,
        intent_lsn: 0,
    };

    assert_leaf_matches(tree.get(&key).await, None);
    tree.insert(key.clone(), slot.clone()).await.unwrap();
    assert_leaf_matches(tree.get(&key).await, Some(&slot));
    assert_eq!(tree.len(), 1);

    tree.insert(key.clone(), updated.clone()).await.unwrap();
    assert_leaf_matches(tree.get(&key).await, Some(&updated));
    assert_eq!(tree.len(), 1);

    tree.remove(&key).await.unwrap();
    assert_leaf_matches(tree.get(&key).await, None);
    assert_eq!(tree.len(), 0);
    tree.validate().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_across_splits() {
    let tree = PageBPlusTree::new();
    for i in 0..120u32 {
        let key = key_for(i);
        let slot = LeafValue {
            value: [i as u8; VALUE_SIZE],
            commit_lsn: i as u64,
            undo_ptr: None,
            flags: 0,
            intent_txn_id: 0,
            intent_lsn: 0,
        };
        tree.insert(key, slot).await.unwrap();
    }

    let start = key_for(10);
    let end = key_for(50);
    let range = tree.range(&start, &end).await;
    assert_eq!(range.len(), 41);
    assert_eq!(range.first().unwrap().0, start);
    assert_eq!(range.last().unwrap().0, end);
}

#[tokio::test(flavor = "current_thread")]
async fn test_with_custom_provider() {
    let provider = InMemoryPageProvider::new();
    let tree = PageBPlusTree::new_with_provider(provider);

    let key = key_for(2);
    let slot = LeafValue {
        value: [3u8; VALUE_SIZE],
        commit_lsn: 100,
        undo_ptr: None,
        flags: 0,
        intent_txn_id: 0,
        intent_lsn: 0,
    };

    tree.insert(key.clone(), slot.clone()).await.unwrap();
    assert_leaf_matches(tree.get(&key).await, Some(&slot));
    assert_eq!(tree.provider().page_count(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_provider_page_allocation() {
    let provider = InMemoryPageProvider::new();

    let p1 = provider.alloc_page_id();
    let p2 = provider.alloc_page_id();
    let p3 = provider.alloc_page_id();

    assert_eq!(p1, 1);
    assert_eq!(p2, 2);
    assert_eq!(p3, 3);

    let page_data = vec![42u8; crate::PAGE_SIZE];
    provider
        .write_page(p1, crate::Page::from(page_data.clone()))
        .await;

    assert_eq!(
        provider.read_page(p1).await,
        Some(crate::Page::from(page_data))
    );
    assert_eq!(provider.read_page(p2).await, None);
}

#[tokio::test(flavor = "current_thread")]
async fn test_shared_page_provider() {
    use super::{PageCache, SharedPageProvider};
    use std::sync::Arc;

    let pages = Arc::new(PageCache::new(4));
    let next_page_id = Arc::new(AtomicU64::new(1));

    let provider = SharedPageProvider::new(pages.clone(), next_page_id.clone());
    let tree = PageBPlusTree::new_with_provider(provider);

    let key = key_for(3);
    let slot = LeafValue {
        value: [7u8; VALUE_SIZE],
        commit_lsn: 42,
        undo_ptr: None,
        flags: 0,
        intent_txn_id: 0,
        intent_lsn: 0,
    };

    tree.insert(key.clone(), slot.clone()).await.unwrap();
    assert_leaf_matches(tree.get(&key).await, Some(&slot));

    assert!(pages.len() >= 1);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concurrent_inserts_split_safety() {
    use super::{PageCache, SharedPageProvider};
    use std::sync::Arc;

    let pages = Arc::new(PageCache::new_with_capacity(8, 256));
    let next_page_id = Arc::new(AtomicU64::new(1));
    let provider = SharedPageProvider::new(Arc::clone(&pages), Arc::clone(&next_page_id));

    // Initialize root once; other tree handles share provider state and latch table.
    let _tree = PageBPlusTree::new_with_provider(provider.clone());

    let workers = 6u32;
    let per_worker = 180u32;
    let local = LocalSet::new();
    let provider_for_tasks = provider.clone();

    tokio::time::timeout(Duration::from_secs(10), async move {
        local
            .run_until(async move {
                let mut handles = Vec::new();
                for worker in 0..workers {
                    let provider = provider_for_tasks.clone();
                    handles.push(tokio::task::spawn_local(async move {
                        let tree = PageBPlusTree::with_provider(provider);
                        for i in 0..per_worker {
                            let logical = worker * per_worker + i;
                            let key = key_for(logical);
                            let slot = LeafValue {
                                value: [logical as u8; VALUE_SIZE],
                                commit_lsn: logical as u64,
                                undo_ptr: None,
                                flags: 0,
                                intent_txn_id: 0,
                                intent_lsn: 0,
                            };
                            tree.insert(key, slot).await.unwrap();
                        }
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            })
            .await;
    })
    .await
    .expect("concurrent insert run timed out (possible deadlock)");

    let tree = PageBPlusTree::with_provider(provider.clone());
    for logical in 0..(workers * per_worker) {
        let key = key_for(logical);
        assert!(
            tree.get(&key).await.is_some(),
            "missing key after concurrent split inserts: logical={}",
            logical
        );
    }

    let start = key_for(0);
    let end = key_for(workers * per_worker - 1);
    let rows = tree.range(&start, &end).await;
    assert!(!rows.is_empty());
    tree.validate().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_concurrent_mixed_workload_delete_rebalance_safety() {
    use super::{PageCache, SharedPageProvider};
    use std::sync::Arc;

    let pages = Arc::new(PageCache::new_with_capacity(16, 1024));
    let next_page_id = Arc::new(AtomicU64::new(1));
    let provider = SharedPageProvider::new(Arc::clone(&pages), Arc::clone(&next_page_id));
    let _tree = PageBPlusTree::new_with_provider(provider.clone());

    let workers = 8u32;
    let ops_per_worker = 500u32;
    let local = LocalSet::new();
    let provider_for_tasks = provider.clone();

    tokio::time::timeout(Duration::from_secs(20), async move {
        local
            .run_until(async move {
                let mut handles = Vec::new();
                for worker in 0..workers {
                    let provider = provider_for_tasks.clone();
                    handles.push(tokio::task::spawn_local(async move {
                        let tree = PageBPlusTree::with_provider(provider);
                        let mut seed = worker as u64 + 1;
                        for _ in 0..ops_per_worker {
                            let r = next_rand(&mut seed);
                            let key_id = (r % 220) as u32;
                            let key = key_for(key_id);
                            let op = ((r >> 16) % 100) as u32;
                            if op < 38 {
                                let slot = LeafValue {
                                    value: [((key_id + worker) & 0xff) as u8; VALUE_SIZE],
                                    commit_lsn: r,
                                    undo_ptr: None,
                                    flags: 0,
                                    intent_txn_id: 0,
                                    intent_lsn: 0,
                                };
                                tree.insert(key, slot).await.unwrap();
                            } else if op < 65 {
                                tree.remove(&key).await.unwrap();
                            } else if op < 85 {
                                let _ = tree.get(&key).await;
                            } else {
                                let k2 = (key_id + ((r >> 24) % 12) as u32).min(219);
                                let (s, e) = if key_id <= k2 {
                                    (key_for(key_id), key_for(k2))
                                } else {
                                    (key_for(k2), key_for(key_id))
                                };
                                let _ = tree.range(&s, &e).await;
                            }
                            if (r & 0x0f) == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            })
            .await;
    })
    .await
    .expect("mixed concurrent run timed out (possible deadlock)");

    let tree = PageBPlusTree::with_provider(provider.clone());
    tree.validate().await.unwrap();

    let rows = tree.range(&key_for(0), &key_for(219)).await;
    assert!(rows.windows(2).all(|w| w[0].0 < w[1].0));
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_meta_and_debug_helpers_are_consistent() {
    let tree = PageBPlusTree::new();
    assert!(tree.is_empty());

    for i in 0..32u32 {
        let key = key_for(i);
        let row = LeafValue {
            value: [i as u8; VALUE_SIZE],
            commit_lsn: i as u64 + 10,
            undo_ptr: None,
            flags: (i % 3) as u16,
            intent_txn_id: 0,
            intent_lsn: 0,
        };
        tree.insert(key, row).await.unwrap();
    }
    assert!(!tree.is_empty());

    let start = key_for(4);
    let end = key_for(20);
    let rows = tree.range(&start, &end).await;
    let metas = tree.range_meta(&start, &end).await;
    assert_eq!(rows.len(), metas.len());
    for ((k1, row), (k2, meta)) in rows.iter().zip(metas.iter()) {
        assert_eq!(k1, k2);
        assert_eq!(row.meta.commit_lsn, meta.commit_lsn);
        assert_eq!(row.meta.flags, meta.flags);
    }

    let debug_keys = tree.debug_leaf_keys(&start, 8).await;
    assert!(!debug_keys.is_empty());
    let debug_entries = tree.debug_leaf_entries(&start, 8).await;
    assert_eq!(debug_keys.len(), debug_entries.len());
    for (k, v) in debug_entries {
        assert_eq!(k.len(), KEY_SIZE);
        assert!(!v.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_page_cache_and_provider_default_paths() {
    use super::{PageCache, SharedPageProvider};
    use std::sync::Arc;

    let cache = PageCache::new(4);
    assert_eq!(cache.len(), 0);
    assert!(!cache.contains(7));
    assert!(cache.get(7).is_none());
    cache.remove(7);

    let page = crate::Page::from(vec![1u8; crate::PAGE_SIZE]);
    cache.insert(7, page.clone());
    assert!(cache.contains(7));
    assert_eq!(cache.get(7), Some(page.clone()));
    assert!(cache.get_arc(7).is_some());

    let read_guard = cache.acquire_read_latch(7).await;
    drop(read_guard);
    let _write_guard = cache.acquire_write_latch(7).await;

    let pages = Arc::new(cache);
    let next = Arc::new(AtomicU64::new(50));
    let provider = SharedPageProvider::new(Arc::clone(&pages), Arc::clone(&next));
    assert_eq!(provider.alloc_page_id(), 50);
    provider.set_root_page_id(33);
    assert_eq!(provider.root_page_id(), 33);

    let _tree = PageBPlusTree::new_with_provider(provider.clone());
    assert!(provider.root_page_id() > 0);
}
