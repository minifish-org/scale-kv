use super::*;
use tempfile::TempDir;

fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn make_page(fill: u8) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    page[0] = fill;
    page[PAGE_SIZE - 1] = fill;
    page
}

async fn wait_until(
    timeout: Duration,
    interval: Duration,
    mut predicate: impl FnMut() -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    predicate()
}

#[tokio::test]
async fn test_open_empty() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
    assert_eq!(store.checkpoint_lsn(), 0);
}

#[tokio::test]
async fn test_put_and_get() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    let page1 = make_page(1);
    store.put(1, &page1, 100).unwrap();

    let retrieved = store.get(1).await.unwrap();
    assert_eq!(retrieved, page1);
}

#[tokio::test]
async fn test_get_missing() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    assert!(store.get(999).await.is_none());
    let cache = store.cache_stats();
    assert_eq!(cache.hits, 0);
    assert_eq!(cache.misses, 1);
}

#[tokio::test]
async fn test_cache_hit_miss_stats() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    let page = make_page(1);
    store.put(7, &page, 100).unwrap();
    assert!(store.get(7).await.is_some());
    assert!(store.get(9999).await.is_none());

    let cache = store.cache_stats();
    assert!(cache.hits >= 1);
    assert!(cache.misses >= 1);
}

#[tokio::test]
async fn test_overwrite() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    let page1 = make_page(1);
    let page2 = make_page(2);

    store.put(1, &page1, 100).unwrap();
    store.put(1, &page2, 101).unwrap();

    let retrieved = store.get(1).await.unwrap();
    assert_eq!(retrieved, page2);
}

#[tokio::test]
async fn test_checkpoint_persists() {
    let dir = temp_dir();

    // Write and checkpoint
    {
        let store = PageStore::open(dir.path()).await.unwrap();
        let page1 = make_page(1);
        let page2 = make_page(2);
        store.put(1, &page1, 100).unwrap();
        store.put(2, &page2, 101).unwrap();
        store.checkpoint().await.unwrap();

        assert_eq!(store.checkpoint_lsn(), 101);
    }

    // Reopen and verify data persistence
    {
        let store = PageStore::open(dir.path()).await.unwrap();
        assert_eq!(store.checkpoint_lsn(), 101);

        let page1 = store.get(1).await.unwrap();
        let page2 = store.get(2).await.unwrap();
        assert_eq!(page1[0], 1);
        assert_eq!(page2[0], 2);
    }
}

#[tokio::test]
async fn test_dirty_tracking() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    let page = make_page(1);
    store.put(1, &page, 100).unwrap();

    let stats = store.buffer_stats();
    assert_eq!(stats.dirty_count, 1);
    assert_eq!(stats.dirty_bytes, PAGE_SIZE);

    store.checkpoint().await.unwrap();

    let stats = store.buffer_stats();
    assert_eq!(stats.dirty_count, 0);
}

#[tokio::test]
async fn test_sparse_pages() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    // Write non-consecutive page_ids
    let page1 = make_page(1);
    let page100 = make_page(100);

    store.put(1, &page1, 100).unwrap();
    store.put(100, &page100, 101).unwrap();
    store.checkpoint().await.unwrap();

    // Reopen and verify
    drop(store);
    let store = PageStore::open(dir.path()).await.unwrap();

    assert!(store.get(1).await.is_some());
    assert!(store.get(50).await.is_none()); // Middle page doesn't exist
    assert!(store.get(100).await.is_some());
}

#[tokio::test]
async fn test_max_page_id() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    let page = make_page(1);
    store.put(5, &page, 100).unwrap();
    store.put(10, &page, 101).unwrap();
    store.put(3, &page, 102).unwrap();

    assert_eq!(store.max_page_id(), 10);
}

#[tokio::test]
async fn test_invalid_page_size() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    let bad_page = vec![0u8; 100]; // Wrong size
    let result = store.put(1, &bad_page, 100);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_checkpoints() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();

    // First round of writes
    let page1 = make_page(1);
    store.put(1, &page1, 100).unwrap();
    store.checkpoint().await.unwrap();
    assert_eq!(store.checkpoint_lsn(), 100);

    // Second round of writes
    let page2 = make_page(2);
    store.put(2, &page2, 200).unwrap();
    store.checkpoint().await.unwrap();
    assert_eq!(store.checkpoint_lsn(), 200);

    // Verify both pages exist
    assert!(store.get(1).await.is_some());
    assert!(store.get(2).await.is_some());
}

#[tokio::test]
async fn test_should_checkpoint_by_dirty_count() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = CheckpointConfig {
        interval: Duration::from_secs(3600),
        max_dirty_pages: 5,
        max_dirty_bytes: 1024 * 1024 * 1024,
    };

    for i in 0..4 {
        let page = make_page(i);
        store.put(i as u64, &page, i as u64 + 1).unwrap();
    }
    assert!(!store.should_checkpoint(&config));

    for i in 4..6 {
        let page = make_page(i);
        store.put(i as u64, &page, i as u64 + 1).unwrap();
    }
    assert!(store.should_checkpoint(&config));
}

#[tokio::test]
async fn test_should_checkpoint_by_dirty_bytes() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = CheckpointConfig {
        interval: Duration::from_secs(3600),
        max_dirty_pages: 1000,
        max_dirty_bytes: PAGE_SIZE * 2,
    };

    let page = make_page(1);
    store.put(1, &page, 1).unwrap();
    assert!(!store.should_checkpoint(&config));

    let page = make_page(2);
    store.put(2, &page, 2).unwrap();
    assert!(!store.should_checkpoint(&config));

    let page = make_page(3);
    store.put(3, &page, 3).unwrap();
    assert!(store.should_checkpoint(&config));
}

#[tokio::test]
async fn test_should_checkpoint_by_interval() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = CheckpointConfig {
        interval: Duration::from_millis(50),
        max_dirty_pages: 1000,
        max_dirty_bytes: 1024 * 1024 * 1024,
    };

    let page = make_page(1);
    store.put(1, &page, 1).unwrap();
    assert!(!store.should_checkpoint(&config));

    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(store.should_checkpoint(&config));
}

#[tokio::test]
async fn test_maybe_checkpoint() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = CheckpointConfig {
        interval: Duration::from_secs(3600),
        max_dirty_pages: 2,
        max_dirty_bytes: 1024 * 1024 * 1024,
    };

    let page = make_page(1);
    store.put(1, &page, 100).unwrap();
    assert!(!store.maybe_checkpoint(&config).await.unwrap());
    assert_eq!(store.buffer_stats().dirty_count, 1);

    let page = make_page(2);
    store.put(2, &page, 101).unwrap();
    let page = make_page(3);
    store.put(3, &page, 102).unwrap();
    assert!(store.maybe_checkpoint(&config).await.unwrap());
    assert_eq!(store.buffer_stats().dirty_count, 0);
    assert_eq!(store.checkpoint_lsn(), 102);
}

#[tokio::test]
async fn test_background_checkpoint() {
    let dir = temp_dir();
    let store = Arc::new(PageStore::open(dir.path()).await.unwrap());
    let config = CheckpointConfig::for_test();

    let handle = store.start_background_checkpoint(config);

    for i in 0..15u64 {
        let page = make_page(i as u8);
        store.put(i, &page, i + 1).unwrap();
    }

    let drained = wait_until(Duration::from_secs(2), Duration::from_millis(20), || {
        store.buffer_stats().dirty_count == 0
    })
    .await;
    assert!(
        drained,
        "dirty pages were not checkpointed in time, dirty_count={}",
        store.buffer_stats().dirty_count
    );
    assert!(store.checkpoint_lsn() >= 15);

    store.shutdown();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_shutdown_flushes_dirty_pages() {
    let dir = temp_dir();
    let store = Arc::new(PageStore::open(dir.path()).await.unwrap());
    let config = CheckpointConfig {
        interval: Duration::from_secs(3600),
        max_dirty_pages: 1000,
        max_dirty_bytes: 1024 * 1024 * 1024,
    };

    let handle = store.start_background_checkpoint(config);

    let page = make_page(1);
    store.put(1, &page, 100).unwrap();
    assert_eq!(store.buffer_stats().dirty_count, 1);

    store.shutdown();
    handle.await.unwrap();

    assert_eq!(store.buffer_stats().dirty_count, 0);
    assert_eq!(store.checkpoint_lsn(), 100);
}

#[tokio::test]
async fn test_evict_cold_pages_removes_oldest() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = BufferPoolConfig {
        max_pages: 5,
        eviction_batch_size: 2,
    };

    for i in 0..10u64 {
        let page = make_page(i as u8);
        store.put(i, &page, i + 1).unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(store.len(), 10);

    let evicted = store.evict_cold_pages(&config).await.unwrap();
    assert_eq!(evicted, 7);
    assert_eq!(store.len(), 3);

    store.checkpoint().await.unwrap();

    let pool_keys: std::collections::HashSet<_> = store.keys().into_iter().collect();
    assert!(pool_keys.contains(&7));
    assert!(pool_keys.contains(&8));
    assert!(pool_keys.contains(&9));
}

#[tokio::test]
async fn test_evict_flushes_dirty_before_remove() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = BufferPoolConfig {
        max_pages: 3,
        eviction_batch_size: 1,
    };

    for i in 0..5u64 {
        let page = make_page((i + 1) as u8);
        store.put(i, &page, i + 1).unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(store.buffer_stats().dirty_count, 5);

    let evicted = store.evict_cold_pages(&config).await.unwrap();
    assert!(evicted > 0);

    drop(store);
    let store2 = PageStore::open(dir.path()).await.unwrap();

    let page0 = store2.get(0).await;
    assert!(page0.is_some(), "Evicted dirty page should be on disk");
    assert_eq!(page0.unwrap()[0], 1);
}

#[tokio::test]
async fn test_maybe_evict() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = BufferPoolConfig {
        max_pages: 5,
        eviction_batch_size: 2,
    };

    for i in 0..4u64 {
        let page = make_page(i as u8);
        store.put(i, &page, i + 1).unwrap();
    }

    let evicted = store.maybe_evict(&config).await.unwrap();
    assert_eq!(evicted, 0);
    assert_eq!(store.len(), 4);

    for i in 4..8u64 {
        let page = make_page(i as u8);
        store.put(i, &page, i + 1).unwrap();
    }
    assert_eq!(store.len(), 8);

    let evicted = store.maybe_evict(&config).await.unwrap();
    assert!(evicted > 0);
    assert!(store.len() <= config.max_pages + config.eviction_batch_size);
}

#[tokio::test]
async fn test_background_checkpoint_with_eviction() {
    let dir = temp_dir();
    let store = Arc::new(PageStore::open(dir.path()).await.unwrap());
    let checkpoint_config = CheckpointConfig::for_test();
    let buffer_config = BufferPoolConfig {
        max_pages: 10,
        eviction_batch_size: 3,
    };

    let handle =
        store.start_background_checkpoint_with_eviction(checkpoint_config, buffer_config.clone());

    for i in 0..25u64 {
        let page = make_page(i as u8);
        store.put(i, &page, i + 1).unwrap();
    }

    let converged = wait_until(Duration::from_secs(2), Duration::from_millis(20), || {
        store.len() <= buffer_config.max_pages + buffer_config.eviction_batch_size
            && store.buffer_stats().dirty_count == 0
    })
    .await;
    assert!(
        converged,
        "background checkpoint+eviction did not converge, len={} dirty_count={}",
        store.len(),
        store.buffer_stats().dirty_count
    );

    store.shutdown();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_evict_no_op_when_under_limit() {
    let dir = temp_dir();
    let store = PageStore::open(dir.path()).await.unwrap();
    let config = BufferPoolConfig {
        max_pages: 100,
        eviction_batch_size: 10,
    };

    for i in 0..5u64 {
        let page = make_page(i as u8);
        store.put(i, &page, i + 1).unwrap();
    }

    let evicted = store.evict_cold_pages(&config).await.unwrap();
    assert_eq!(evicted, 0);
    assert_eq!(store.len(), 5);
}
