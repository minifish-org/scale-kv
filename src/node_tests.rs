use super::*;
use crate::{KEY_SIZE, VALUE_SIZE};
use tempfile::TempDir;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn make_page(fill: u8) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SIZE];
    page[0] = fill;
    page[PAGE_SIZE - 1] = fill;
    page
}

#[tokio::test]
async fn test_put_and_get() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let page = make_page(1);
    node.put(1, &page).await;
    let result = node.get(1).await.unwrap();
    assert_eq!(result[0], 1);
    assert_eq!(result[PAGE_SIZE - 1], 1);
    drop(node);
}

#[tokio::test]
async fn test_get_missing() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    assert_eq!(node.get(999).await, None);
    drop(node);
}

#[tokio::test]
async fn test_overwrite() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let page1 = make_page(1);
    let page2 = make_page(2);
    node.put(1, &page1).await;
    node.put(1, &page2).await;
    let result = node.get(1).await.unwrap();
    assert_eq!(result[0], 2);
    drop(node);
}

#[tokio::test]
async fn test_delete() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let page = make_page(1);
    node.put(1, &page).await;
    node.delete(1).await;
    assert_eq!(node.get(1).await, None);
    drop(node);
}

#[tokio::test]
async fn test_len() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    assert_eq!(node.len().await, 0);
    let page1 = make_page(1);
    let page2 = make_page(2);
    node.put(1, &page1).await;
    node.put(2, &page2).await;
    assert_eq!(node.len().await, 2);
    drop(node);
}

#[tokio::test]
async fn test_contains() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let page = make_page(1);
    node.put(1, &page).await;
    assert!(node.contains(1).await);
    assert!(!node.contains(999).await);
    drop(node);
}

#[tokio::test]
async fn test_reopen_persists() {
    let dir = temp_dir();
    let page1 = make_page(1);
    let page2 = make_page(2);
    {
        let node = StorageNode::open(dir.path()).await.unwrap();
        node.put(1, &page1).await;
        node.put(2, &page2).await;
        node.checkpoint().await.unwrap();
    }
    let node = StorageNode::open(dir.path()).await.unwrap();
    assert_eq!(node.get(1).await.unwrap()[0], 1);
    assert_eq!(node.get(2).await.unwrap()[0], 2);
    drop(node);
}

#[tokio::test]
async fn test_checkpoint_persists_data() {
    let dir = temp_dir();
    let page1 = make_page(1);
    let page2 = make_page(2);
    {
        let node = StorageNode::open(dir.path()).await.unwrap();
        node.put(1, &page1).await;
        node.put(2, &page2).await;
        node.checkpoint().await.unwrap();
    }
    {
        let node = StorageNode::open(dir.path()).await.unwrap();
        assert_eq!(node.get(1).await.unwrap()[0], 1);
        assert_eq!(node.get(2).await.unwrap()[0], 2);
    }
}

#[tokio::test]
async fn test_compact_is_checkpoint() {
    let dir = temp_dir();
    let page = make_page(1);
    let node = StorageNode::open(dir.path()).await.unwrap();
    node.put(1, &page).await;
    node.compact().await.unwrap();
    drop(node);

    let node = StorageNode::open(dir.path()).await.unwrap();
    assert_eq!(node.get(1).await.unwrap()[0], 1);
    drop(node);
}

#[tokio::test]
async fn test_wal_state_roundtrip() {
    let dir = temp_dir();
    write_wal_state(dir.path(), 42).await.unwrap();
    let state = read_wal_state(dir.path()).await.unwrap();
    assert_eq!(state.last_applied_lsn, 42);
}

#[tokio::test]
async fn test_wal_encode_decode_roundtrip() {
    let dir = temp_dir();
    let path = dir.path().join("wal-test.log");
    let batch = WalBatch {
        request_id: 0,
        start_lsn: 1,
        end_lsn: 2,
        records: vec![
            WalRecord {
                lsn: 1,
                op: WAL_OP_PAGE_PUT,
                page_id: 7,
                slot_id: 0,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            WalRecord {
                lsn: 2,
                op: WAL_OP_PAGE_DEL,
                page_id: 7,
                slot_id: 0,
                key: b"k1".to_vec(),
                value: Vec::new(),
            },
        ],
    };
    let mut buf = Vec::new();
    encode_wal_batch(&batch, &mut buf).unwrap();
    let mut file = File::create(&path).await.unwrap();
    file.write_all(&buf).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);

    let mut file = File::open(&path).await.unwrap();
    let decoded = read_wal_batch(&mut file).await.unwrap().unwrap();
    assert_eq!(decoded.start_lsn, 1);
    assert_eq!(decoded.end_lsn, 2);
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].key, b"k1".to_vec());
    assert_eq!(decoded.records[0].value, b"v1".to_vec());
}

#[tokio::test]
async fn test_wal_decode_rejects_oversized_frame() {
    let dir = temp_dir();
    let path = dir.path().join("wal-oversized.log");
    let oversized_len = (16 * 1024 * 1024 + 1) as u32;
    let mut file = File::create(&path).await.unwrap();
    file.write_all(&oversized_len.to_le_bytes()).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);

    let mut file = File::open(&path).await.unwrap();
    let err = read_wal_batch(&mut file).await.unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidData));
    assert!(format!("{err}").contains("too large"));
}

#[tokio::test]
async fn test_wal_decode_rejects_truncated_frame_payload() {
    let dir = temp_dir();
    let path = dir.path().join("wal-truncated.log");
    let mut file = File::create(&path).await.unwrap();
    file.write_all(&100u32.to_le_bytes()).await.unwrap();
    file.write_all(&[0u8; 3]).await.unwrap();
    file.sync_all().await.unwrap();
    drop(file);

    let mut file = File::open(&path).await.unwrap();
    let err = read_wal_batch(&mut file).await.unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn test_append_wal_batch_rejects_out_of_order_start_lsn() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let durable = node.durable_lsn();
    let batch = WalBatch {
        request_id: 0,
        start_lsn: durable + 1,
        end_lsn: durable + 2,
        records: vec![WalRecord {
            lsn: durable + 1,
            op: WAL_OP_PAGE_IMAGE,
            page_id: 1,
            slot_id: 0,
            key: vec![],
            value: vec![0u8; PAGE_SIZE],
        }],
    };
    let err = node.append_wal_batch_sync(batch).await.unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidInput));
}

#[tokio::test]
async fn test_append_wal_batch_rejects_invalid_range_and_length() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let durable = node.durable_lsn();

    let bad_range = WalBatch {
        request_id: 0,
        start_lsn: durable,
        end_lsn: durable.saturating_sub(1),
        records: vec![],
    };
    let err = node.append_wal_batch_sync(bad_range).await.unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidInput));

    let bad_len = WalBatch {
        request_id: 0,
        start_lsn: durable,
        end_lsn: durable + 2,
        records: vec![WalRecord {
            lsn: durable,
            op: WAL_OP_PAGE_IMAGE,
            page_id: 1,
            slot_id: 0,
            key: vec![],
            value: vec![0u8; PAGE_SIZE],
        }],
    };
    let err = node.append_wal_batch_sync(bad_len).await.unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidInput));
}

#[tokio::test]
async fn test_append_txn_batch_rejects_empty_writes() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();
    let durable = node.durable_lsn();
    let err = node
        .append_txn_batch_with_lsn_sync(7, durable, durable, vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidInput));
}

#[tokio::test]
async fn test_replay_page_records_applies_updates() {
    let dir = temp_dir();
    let page_store = Arc::new(PageStore::open(dir.path()).await.unwrap());
    let page = crate::slotted_page::new_page();
    page_store.put_direct(10, &page).unwrap();

    let replay = PageStoreReplay::new(
        Arc::clone(&page_store),
        dir.path().to_path_buf(),
        0,
        Arc::new(tokio::sync::RwLock::new(HashSet::new())),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Arc::new(tokio::sync::Notify::new()),
    );
    let records = vec![
        WalRecord {
            lsn: 1,
            op: WAL_OP_PAGE_PUT,
            page_id: 10,
            slot_id: 0,
            key: crate::slotted_page::fixed_key_bytes(b"k1"),
            value: vec![b'v'; VALUE_SIZE],
        },
        WalRecord {
            lsn: 2,
            op: WAL_OP_PAGE_PUT,
            page_id: 10,
            slot_id: 1,
            key: crate::slotted_page::fixed_key_bytes(b"k2"),
            value: vec![b'w'; VALUE_SIZE],
        },
    ];
    replay_page_records(&replay, 10, records).await.unwrap();
    let page = page_store.get(10).await.unwrap();
    let read_slot = |page: &[u8], slot_id: u16| -> Vec<u8> {
        // Use slotted page layout (same offsets):
        let offset = 6 + 4 * slot_id as usize;
        let pos = u16::from_le_bytes([page[offset], page[offset + 1]]) as usize;
        let len = u16::from_le_bytes([page[offset + 2], page[offset + 3]]) as usize;
        if len == 0 {
            return Vec::new();
        }
        let value_start = pos + KEY_SIZE;
        let value_end = value_start + VALUE_SIZE;
        page[value_start..value_end].to_vec()
    };
    assert_eq!(read_slot(&page, 0), vec![b'v'; VALUE_SIZE]);
    assert_eq!(read_slot(&page, 1), vec![b'w'; VALUE_SIZE]);
}

#[test]
fn test_wal_slot_key_mismatch_is_rejected() {
    let mut page = crate::slotted_page::new_page().to_vec();
    crate::slotted_page::insert_record_at_slot_checked(
        &mut page,
        0,
        &crate::slotted_page::fixed_key_bytes(b"k1"),
        &vec![b'v'; VALUE_SIZE],
    )
    .unwrap();
    let err = crate::slotted_page::insert_record_at_slot_checked(
        &mut page,
        0,
        &crate::slotted_page::fixed_key_bytes(b"k2"),
        &vec![b'w'; VALUE_SIZE],
    )
    .unwrap_err();
    // slotted_page helper may return a generic key-mismatch error; just assert it is rejected.
    assert!(format!("{err}").contains("mismatch") || format!("{err}").contains("key"));
}

#[test]
fn test_mvcc_gc_keeps_last_visible_base_version() {
    let mut store: BTreeMap<Vec<u8>, Vec<MvccVersion>> = BTreeMap::new();
    store.insert(
        b"k".to_vec(),
        vec![
            MvccVersion {
                commit_lsn: 10,
                value: Some(b"v1".to_vec()),
            },
            MvccVersion {
                commit_lsn: 20,
                value: Some(b"v2".to_vec()),
            },
            MvccVersion {
                commit_lsn: 30,
                value: None,
            },
        ],
    );

    let (_keys_touched, removed) = gc_mvcc_versions(&mut store, 20);
    assert_eq!(removed, 1);
    let versions = store.get(b"k".as_ref()).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].commit_lsn, 20);
    assert_eq!(versions[1].commit_lsn, 30);
}

#[tokio::test]
async fn test_mvcc_apply_batch_requires_commit_marker() {
    let mvcc: Arc<tokio::sync::RwLock<BTreeMap<Vec<u8>, Vec<MvccVersion>>>> =
        Arc::new(tokio::sync::RwLock::new(BTreeMap::new()));
    let batch = WalBatch {
        request_id: 0,
        start_lsn: 10,
        end_lsn: 11,
        records: vec![WalRecord {
            lsn: 10,
            op: WAL_OP_TXN_PUT,
            page_id: 0,
            slot_id: 0,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
    };
    apply_txn_batch_to_mvcc(&mvcc, &batch).await;
    assert!(mvcc.read().await.is_empty());
}

#[tokio::test]
async fn test_mvcc_get_at_snapshot_boundaries() {
    let mvcc: Arc<tokio::sync::RwLock<BTreeMap<Vec<u8>, Vec<MvccVersion>>>> =
        Arc::new(tokio::sync::RwLock::new(BTreeMap::new()));
    {
        let mut store = mvcc.write().await;
        store.insert(
            b"k".to_vec(),
            vec![
                MvccVersion {
                    commit_lsn: 10,
                    value: Some(b"v1".to_vec()),
                },
                MvccVersion {
                    commit_lsn: 20,
                    value: None,
                },
            ],
        );
    }

    assert_eq!(mvcc_get_at(&mvcc, b"k", 9).await, None);
    assert_eq!(mvcc_get_at(&mvcc, b"k", 10).await, Some(Some(b"v1".to_vec())));
    assert_eq!(mvcc_get_at(&mvcc, b"k", 25).await, Some(None));
}

#[test]
fn test_mvcc_gc_noop_when_watermark_precedes_all_versions() {
    let mut store: BTreeMap<Vec<u8>, Vec<MvccVersion>> = BTreeMap::new();
    store.insert(
        b"k".to_vec(),
        vec![
            MvccVersion {
                commit_lsn: 30,
                value: Some(b"v1".to_vec()),
            },
            MvccVersion {
                commit_lsn: 40,
                value: Some(b"v2".to_vec()),
            },
        ],
    );

    let (keys_touched, removed) = gc_mvcc_versions(&mut store, 20);
    assert_eq!(keys_touched, 0);
    assert_eq!(removed, 0);
    assert_eq!(store.get(b"k".as_ref()).unwrap().len(), 2);
}

#[tokio::test]
async fn test_maybe_checkpoint_by_pressure_noop_when_under_threshold() {
    let dir = temp_dir();
    let store = Arc::new(PageStore::open(dir.path()).await.unwrap());
    let maintenance = StorageMaintenanceConfig::default();
    let metrics = Arc::new(StorageMetricsInner::default());

    let triggered = maybe_checkpoint_by_pressure(&store, &maintenance, &metrics)
        .await
        .unwrap();
    assert!(!triggered);
    assert_eq!(
        metrics
            .checkpoint_runs
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn test_maybe_checkpoint_by_pressure_triggers_on_dirty_pressure() {
    let dir = temp_dir();
    let store = Arc::new(PageStore::open(dir.path()).await.unwrap());
    let mut maintenance = StorageMaintenanceConfig::default();
    maintenance.checkpoint.max_dirty_pages = 0;
    maintenance.checkpoint.max_dirty_bytes = usize::MAX;
    let metrics = Arc::new(StorageMetricsInner::default());

    let page = make_page(1);
    store.put(1, &page, 1).unwrap();
    assert_eq!(store.buffer_stats().dirty_count, 1);

    let triggered = maybe_checkpoint_by_pressure(&store, &maintenance, &metrics)
        .await
        .unwrap();
    assert!(triggered);
    assert_eq!(store.buffer_stats().dirty_count, 0);
    assert!(
        metrics
            .checkpoint_runs
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
}

#[tokio::test]
async fn test_mvcc_read_handle_admin_abort() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();

    {
        let mut mvcc = node.mvcc.write().await;
        mvcc.insert(
            b"k".to_vec(),
            vec![MvccVersion {
                commit_lsn: node.durable_lsn(),
                value: Some(b"v".to_vec()),
            }],
        );
    }

    let mut handle = node.begin_mvcc_ro();
    let id = handle.id().unwrap();
    assert!(node.list_active_reads().iter().any(|r| r.id == id));
    assert_eq!(
        node.mvcc_get(&mut handle, b"k").await.unwrap(),
        Some(b"v".to_vec())
    );

    assert!(node.abort_active_read(id));
    let err = node.mvcc_get(&mut handle, b"k").await.unwrap_err();
    assert!(format!("{err}").contains("aborted"));

    drop(handle);
}

#[tokio::test]
async fn test_mvcc_read_handle_timeout() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();

    {
        let mut mvcc = node.mvcc.write().await;
        mvcc.insert(
            b"k".to_vec(),
            vec![MvccVersion {
                commit_lsn: node.durable_lsn(),
                value: Some(b"v".to_vec()),
            }],
        );
    }

    let mut handle = node.begin_mvcc_ro_with_timeout(Some(Duration::from_millis(1)));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let err = node.mvcc_get(&mut handle, b"k").await.unwrap_err();
    assert!(matches!(err, crate::Error::TxnTimeout));
    assert!(!handle.is_active());
}

#[tokio::test]
async fn test_wal_truncation_keeps_current_segment() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();

    let page = make_page(1);
    node.put(1, &page).await;
    node.checkpoint().await.unwrap();

    let segments_before = list_wal_segments(dir.path()).await.unwrap();
    assert_eq!(segments_before.len(), 1);

    let deleted = node.truncate_wal().await.unwrap();
    assert_eq!(deleted, 0);

    let segments_after = list_wal_segments(dir.path()).await.unwrap();
    assert_eq!(segments_after.len(), 1);

    drop(node);
}

#[tokio::test]
async fn test_wal_truncation_deletes_old_segments() {
    let dir = temp_dir();

    create_wal_segment(dir.path(), 1).await.unwrap();
    create_wal_segment(dir.path(), 2).await.unwrap();
    create_wal_segment(dir.path(), 3).await.unwrap();

    {
        let batch1 = WalBatch {
            request_id: 0,
            start_lsn: 1,
            end_lsn: 10,
            records: vec![],
        };
        let batch2 = WalBatch {
            request_id: 0,
            start_lsn: 11,
            end_lsn: 20,
            records: vec![],
        };
        let batch3 = WalBatch {
            request_id: 0,
            start_lsn: 21,
            end_lsn: 30,
            records: vec![],
        };

        let mut buf1 = Vec::new();
        encode_wal_batch(&batch1, &mut buf1).unwrap();
        let mut file1 = OpenOptions::new()
            .write(true)
            .open(wal_segment_path(dir.path(), 1))
            .await
            .unwrap();
        file1.write_all(&buf1).await.unwrap();

        let mut buf2 = Vec::new();
        encode_wal_batch(&batch2, &mut buf2).unwrap();
        let mut file2 = OpenOptions::new()
            .write(true)
            .open(wal_segment_path(dir.path(), 2))
            .await
            .unwrap();
        file2.write_all(&buf2).await.unwrap();

        let mut buf3 = Vec::new();
        encode_wal_batch(&batch3, &mut buf3).unwrap();
        let mut file3 = OpenOptions::new()
            .write(true)
            .open(wal_segment_path(dir.path(), 3))
            .await
            .unwrap();
        file3.write_all(&buf3).await.unwrap();
    }

    let segments = list_wal_segments(dir.path()).await.unwrap();
    assert_eq!(segments, vec![1, 2, 3]);

    let deleted = truncate_wal_segments(dir.path(), 15).await.unwrap();
    assert_eq!(deleted, 1);

    let segments = list_wal_segments(dir.path()).await.unwrap();
    assert_eq!(segments, vec![2, 3]);

    let deleted = truncate_wal_segments(dir.path(), 25).await.unwrap();
    assert_eq!(deleted, 1);

    let segments = list_wal_segments(dir.path()).await.unwrap();
    assert_eq!(segments, vec![3]);
}

#[tokio::test]
async fn test_checkpoint_and_truncate() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();

    let page = make_page(1);
    node.put(1, &page).await;

    let deleted = node.checkpoint_and_truncate().await.unwrap();
    assert_eq!(deleted, 0);

    assert!(node.page_store().checkpoint_lsn() == 0 || node.get(1).await.is_some());

    drop(node);
}

#[tokio::test]
async fn test_background_checkpoint_with_truncate() {
    use crate::page_store::CheckpointConfig;
    use std::time::Duration;

    let dir = temp_dir();

    create_wal_segment(dir.path(), 1).await.unwrap();
    create_wal_segment(dir.path(), 2).await.unwrap();
    {
        let batch1 = WalBatch {
            request_id: 0,
            start_lsn: 1,
            end_lsn: 5,
            records: vec![],
        };
        let batch2 = WalBatch {
            request_id: 0,
            start_lsn: 6,
            end_lsn: 10,
            records: vec![],
        };
        let mut buf1 = Vec::new();
        encode_wal_batch(&batch1, &mut buf1).unwrap();
        let mut file1 = OpenOptions::new()
            .write(true)
            .open(wal_segment_path(dir.path(), 1))
            .await
            .unwrap();
        file1.write_all(&buf1).await.unwrap();

        let mut buf2 = Vec::new();
        encode_wal_batch(&batch2, &mut buf2).unwrap();
        let mut file2 = OpenOptions::new()
            .write(true)
            .open(wal_segment_path(dir.path(), 2))
            .await
            .unwrap();
        file2.write_all(&buf2).await.unwrap();
    }

    let page_store = Arc::new(PageStore::open(dir.path()).await.unwrap());

    for i in 0..15u64 {
        let page = make_page(i as u8);
        page_store.put(i, &page, 100 + i).unwrap();
    }

    let config = CheckpointConfig {
        interval: Duration::from_millis(50),
        max_dirty_pages: 5,
        max_dirty_bytes: 1024 * 1024,
    };

    let store_clone = Arc::clone(&page_store);
    let dir_clone = dir.path().to_path_buf();
    let handle = tokio::spawn(async move {
        while !store_clone.is_shutdown() {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if store_clone.is_shutdown() {
                break;
            }
            if store_clone.maybe_checkpoint(&config).await.unwrap_or(false) {
                let checkpoint_lsn = store_clone.checkpoint_lsn();
                let _ = truncate_wal_segments(&dir_clone, checkpoint_lsn).await;
            }
        }
        let _ = store_clone.checkpoint().await;
        let checkpoint_lsn = store_clone.checkpoint_lsn();
        let _ = truncate_wal_segments(&dir_clone, checkpoint_lsn).await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    page_store.shutdown();
    handle.await.unwrap();

    let segments = list_wal_segments(dir.path()).await.unwrap();
    assert!(segments.len() <= 2);
}

#[tokio::test]
async fn test_metrics_snapshot_exposes_core_fields() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();

    let page = make_page(7);
    node.put(7, &page).await;
    let _ = node.get(7).await;
    let _ = node.get(9999).await;
    let _ = node.checkpoint().await;

    let metrics = node.metrics_snapshot().await;
    assert!(metrics.page_cache_hits >= 1);
    assert!(metrics.page_cache_misses >= 1);
    assert!(metrics.checkpoint_runs >= 1);
    assert!(metrics.page_cache_resident_pages >= 1);
    assert!(!metrics.render_json().is_empty());
    assert!(!metrics.render_prometheus().is_empty());

    drop(node);
}

#[tokio::test]
async fn test_invalid_maintenance_config_fails_fast() {
    let dir = temp_dir();
    let cfg = StorageMaintenanceConfig {
        max_wal_segments: 0,
        ..StorageMaintenanceConfig::default()
    };
    let err = match StorageNode::open_with_maintenance(dir.path(), cfg).await {
        Ok(_) => panic!("expected invalid config failure"),
        Err(err) => err,
    };
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidInput));
}

#[tokio::test]
async fn test_checkpoint_sync_failure_is_reported_and_recovers() {
    let dir = temp_dir();
    let node = StorageNode::open(dir.path()).await.unwrap();

    let page = make_page(4);
    node.put(4, &page).await;

    node.page_store().inject_fail_next_checkpoint_sync();
    let err = node.checkpoint().await.unwrap_err();
    assert!(format!("{err}").contains("injected checkpoint sync failure"));

    // Next checkpoint should recover and persist normally.
    node.checkpoint().await.unwrap();
    drop(node);

    let reopened = StorageNode::open(dir.path()).await.unwrap();
    let got = reopened.get(4).await.unwrap();
    assert_eq!(got[0], 4);
    drop(reopened);
}

#[tokio::test]
async fn test_wal_backpressure_under_tight_byte_limit() {
    let dir = temp_dir();
    let maintenance = StorageMaintenanceConfig {
        max_wal_bytes: 1,
        max_wal_segments: usize::MAX,
        truncate_wal: false,
        ..StorageMaintenanceConfig::default()
    };

    let node = StorageNode::open_with_maintenance(dir.path(), maintenance)
        .await
        .unwrap();
    let start = node.durable_lsn();
    let page = make_page(9);
    let err = node
        .append_txn_batch_with_lsn_sync(1, start, start + 1, vec![(99, page.into())])
        .await
        .unwrap_err();
    assert!(matches!(err, crate::Error::Io(ref ioe) if ioe.kind() == ErrorKind::WouldBlock));

    let metrics = node.metrics_snapshot().await;
    assert!(metrics.wal_backpressure_count >= 1);

    drop(node);
}

#[tokio::test]
async fn test_restart_preserves_data_and_durable_lsn_monotonic() {
    let dir = temp_dir();

    let commit_lsn = {
        let node = StorageNode::open(dir.path()).await.unwrap();
        let start = node.durable_lsn();
        let page = make_page(5);
        let commit = node
            .append_txn_batch_with_lsn_sync(10, start, start + 1, vec![(55, page.into())])
            .await
            .unwrap();
        assert_eq!(commit, start + 1);
        drop(node);
        commit
    };

    let node = StorageNode::open(dir.path()).await.unwrap();
    let durable_after_restart = node.durable_lsn();
    assert!(durable_after_restart >= commit_lsn);
    let got = node.get(55).await.unwrap();
    assert_eq!(got[0], 5);

    drop(node);
}
