use super::{
    MvccVersion, PageStoreReplay, ReplayBuffer, StorageMaintenanceConfig, StorageMetricsInner,
    WAL_OP_PAGE_DEL, WAL_OP_PAGE_IMAGE, WAL_OP_PAGE_PUT, WAL_OP_TXN_COMMIT, WAL_OP_TXN_DEL,
    WAL_OP_TXN_PUT, WalRecord, apply_txn_batch_to_mvcc, gc_mvcc_versions, list_wal_segments,
    read_wal_batch, truncate_wal_segments, wal_segment_path, wal_usage_exceeds_limits,
};
use crate::page_store::PageStore;
use crate::{ActiveReads, PAGE_SIZE, Page, PageId, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File;
use tokio::sync::mpsc::Receiver;

pub(super) async fn wal_replay_loop(
    replay: PageStoreReplay,
    mut rx: Receiver<super::WalBatch>,
    mvcc: Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    request_index: Arc<std::sync::Mutex<HashMap<u64, u64>>>,
    active_reads: Arc<ActiveReads>,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
    maintenance: StorageMaintenanceConfig,
    metrics: Arc<StorageMetricsInner>,
) {
    let mut buffers: HashMap<PageId, ReplayBuffer> = HashMap::new();
    let mut gc_batch_counter = 0usize;
    while let Some(batch) = rx.recv().await {
        // Build requestId -> commitLsn mapping for idempotent retry.
        if batch.request_id != 0 {
            request_index
                .lock()
                .unwrap()
                .insert(batch.request_id, batch.end_lsn);
        }

        // Apply txn MVCC records if this batch ends with a commit marker.
        apply_txn_batch_to_mvcc(&mvcc, &batch);

        let mut last_seen = replay.last_applied();
        for record in batch.records.into_iter() {
            if record.lsn <= last_seen {
                continue;
            }
            if record.lsn != last_seen + 1 {
                break;
            }
            last_seen = record.lsn;

            // Skip txn records for page replay.
            if record.op == WAL_OP_TXN_PUT
                || record.op == WAL_OP_TXN_DEL
                || record.op == WAL_OP_TXN_COMMIT
            {
                continue;
            }

            let page_id = record.page_id;
            let buffer = buffers.entry(page_id).or_insert_with(ReplayBuffer::new);
            buffer.push(record);
            if buffer.should_flush() {
                let records = buffer.take();
                let _ = replay_page_records(&replay, page_id, records).await;
            }
        }

        let _ =
            super::maybe_checkpoint_by_pressure(&replay.page_store, &maintenance, &metrics).await;
        if maintenance.truncate_wal {
            let checkpoint_lsn = replay.page_store.checkpoint_lsn();
            let _ = truncate_wal_segments(&replay.dir, checkpoint_lsn).await;
        }

        if let Ok(Some((segments, bytes))) =
            wal_usage_exceeds_limits(&replay.dir, &maintenance, 0).await
        {
            eprintln!(
                "[wal-limit-replay] over threshold after replay: segments={} bytes={} limits=(segments:{} bytes:{})",
                segments, bytes, maintenance.max_wal_segments, maintenance.max_wal_bytes
            );
        }

        gc_batch_counter += 1;
        if gc_batch_counter >= maintenance.mvcc_gc_every_wal_batches {
            gc_batch_counter = 0;
            let durable = durable_lsn.load(std::sync::atomic::Ordering::Acquire);
            let watermark = active_reads.min_read_lsn().unwrap_or(durable).min(durable);
            let started = Instant::now();
            let (keys_touched, versions_removed) = {
                let mut store = mvcc.lock().unwrap();
                gc_mvcc_versions(&mut store, watermark)
            };
            let ms = started.elapsed().as_millis() as u64;
            metrics
                .gc_runs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics.gc_versions_removed_total.fetch_add(
                versions_removed as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            metrics
                .gc_last_duration_ms
                .store(ms, std::sync::atomic::Ordering::Relaxed);
            if versions_removed > 0 {
                eprintln!(
                    "[mvcc-gc] watermark={} keys_touched={} versions_removed={}",
                    watermark, keys_touched, versions_removed
                );
            }
        }
    }

    for (page_id, mut buffer) in buffers.into_iter() {
        let records = buffer.take();
        let _ = replay_page_records(&replay, page_id, records).await;
    }
}

pub(super) async fn replay_page_records(
    replay: &PageStoreReplay,
    page_id: PageId,
    records: Vec<WalRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut max_lsn = 0u64;
    let page = replay
        .read_page(page_id)
        .await
        .unwrap_or_else(|| Page::from(vec![0u8; PAGE_SIZE]));
    let mut page = page.to_vec();
    for record in records {
        if record.lsn > max_lsn {
            max_lsn = record.lsn;
        }
        apply_wal_record(&mut page, &record)?;
    }
    replay.write_page(page_id, &page, max_lsn)?;
    replay.update_last_applied(max_lsn).await;
    Ok(())
}

pub(super) async fn replay_wal_segments_to_store(
    dir: &std::path::Path,
    page_store: Arc<PageStore>,
    last_applied_lsn: u64,
    page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
    mvcc: &Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    request_index: &Arc<std::sync::Mutex<HashMap<u64, u64>>>,
) -> Result<()> {
    let mut segments = list_wal_segments(dir).await?;
    segments.sort_unstable();
    if segments.is_empty() {
        return Ok(());
    }
    let replay = PageStoreReplay::new(
        page_store,
        dir.to_path_buf(),
        last_applied_lsn,
        page_index,
        Arc::new(std::sync::atomic::AtomicU64::new(last_applied_lsn)),
        Arc::new(tokio::sync::Notify::new()),
    );
    for file_id in segments {
        let path = wal_segment_path(dir, file_id);
        let mut file = File::open(&path).await?;
        loop {
            let batch = match read_wal_batch(&mut file).await? {
                Some(batch) => batch,
                None => break,
            };

            if batch.request_id != 0 {
                request_index
                    .lock()
                    .unwrap()
                    .insert(batch.request_id, batch.end_lsn);
            }
            apply_txn_batch_to_mvcc(mvcc, &batch);

            for record in batch.records {
                if record.lsn <= replay.last_applied() {
                    continue;
                }
                // Skip logical txn records. Page-level redo uses WAL_OP_PAGE_IMAGE.
                if record.op == WAL_OP_TXN_PUT
                    || record.op == WAL_OP_TXN_DEL
                    || record.op == WAL_OP_TXN_COMMIT
                {
                    continue;
                }
                replay.apply_record(record).await?;
            }
        }
    }
    Ok(())
}

pub(super) fn apply_wal_record(page: &mut [u8], record: &WalRecord) -> Result<()> {
    match record.op {
        WAL_OP_PAGE_IMAGE => {
            if record.value.len() != PAGE_SIZE {
                return Err(crate::Error::InvalidPageSize(record.value.len(), PAGE_SIZE));
            }
            page.copy_from_slice(&record.value);
            Ok(())
        }
        WAL_OP_PAGE_PUT => crate::slotted_page::insert_record_at_slot_checked(
            page,
            record.slot_id,
            &record.key,
            &record.value,
        ),
        WAL_OP_PAGE_DEL => {
            crate::slotted_page::clear_slot(page, record.slot_id);
            Ok(())
        }
        // txn records are not applied to the page store
        WAL_OP_TXN_PUT | WAL_OP_TXN_DEL | WAL_OP_TXN_COMMIT => Ok(()),
        _ => Ok(()),
    }
}
