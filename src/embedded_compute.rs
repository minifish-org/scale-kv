#![allow(clippy::arc_with_non_send_sync)]

use crate::compute_sequencer::ComputeSequencer;
use crate::embedded_compute_runtime::{
    InProcessPageStore, InProcessSequencer, PendingTxn, Sequencer,
};
use crate::meta_page::{META_PAGE_ID, MetaPage};
use crate::page_bptree::{
    AsyncPageProvider, DEFAULT_PAGE_CACHE_SHARDS, LeafValue, LeafValueMeta, LeafValueRef,
    PageBPlusTree, PageCache,
};
use crate::secondary_index::{
    SecondaryIndexDefinition, SecondaryIndexManager, SecondaryIndexMutation,
};
use crate::secondary_index_meta::{
    SECONDARY_INDEX_META_PAGE_ID, SecondaryIndexCatalog, decode_secondary_index_catalog,
    encode_secondary_index_catalog,
};
use crate::secondary_posting_log::{
    SecondaryPostingLogState, append_record as append_posting_record,
    decode_records as decode_posting_records, new_log_page, read_next_page_id,
    write_next_page_id as write_posting_next_page_id,
};
use crate::txn_page_provider::{CURRENT_TXN_DIRTY, TxnPageProvider};
use crate::{ActiveReads, ReadGuard};
use crate::{BTREE_META_PAGE_ID, BtreeMeta};
use crate::{
    Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, StorageClient, VALUE_SIZE,
    undo_pg::{
        self, SEGMENT_STATE_COMMITTED, SEGMENT_STATE_PURGED, UndoPtr, UndoRecord, UndoSegmentHeader,
    },
};
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::LocalSet;

#[path = "embedded_compute_recovery.rs"]
mod embedded_compute_recovery;
#[path = "embedded_compute_visibility.rs"]
mod embedded_compute_visibility;

const FLAG_TOMBSTONE: u16 = 1;
const FLAG_INTENT: u16 = 2;

/// Embedded compute-side API for an Aurora-style KV (page-level redo).
///
/// Compute owns the B+Tree and the page cache. Writes are page after-images.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Sequencer,
    readers: Arc<Vec<Arc<StorageClient>>>,

    min_read_lsn: Arc<std::sync::atomic::AtomicU64>,
    active_reads: Arc<ActiveReads>,

    page_cache: Arc<PageCache>,
    page_fetcher:
        Arc<dyn Fn(PageId, u64) -> futures::future::LocalBoxFuture<'static, Option<Page>>>,
    provider: Arc<TxnPageProvider>,
    tree: Arc<PageBPlusTree<TxnPageProvider>>,
    secondary_indexes: Arc<SecondaryIndexManager>,
    secondary_posting_log: Arc<Mutex<SecondaryPostingLogState>>,
    txn_registry: Arc<Mutex<HashMap<u64, PendingTxn<KEY_SIZE>>>>,
    default_rw_txn_timeout: Duration,
    txn_reaper_interval: Duration,
}

impl EmbeddedCompute {
    fn read_duration_env_ms(name: &str, default_ms: u64) -> Duration {
        let ms = std::env::var(name)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default_ms);
        Duration::from_millis(ms)
    }

    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        let sequencer = Sequencer::Network(Arc::new(
            ComputeSequencer::connect(addrs, quorum, local).await?,
        ));

        let mut readers: Vec<Arc<StorageClient>> = Vec::with_capacity(addrs.len());
        for addr in addrs {
            readers.push(Arc::new(StorageClient::connect(addr, local).await?));
        }

        // Buffer pool sizing:
        // Align with PostgreSQL default shared_buffers=128MB.
        // Our page size is 16KB, so 128MB ~= 8192 pages.
        // Override via env SCALE_KV_PAGE_CACHE_PAGES.
        let default_pages: usize = 8 * 1024;
        let capacity_pages: usize = std::env::var("SCALE_KV_PAGE_CACHE_PAGES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default_pages);

        let page_cache = Arc::new(PageCache::new_with_capacity(
            DEFAULT_PAGE_CACHE_SHARDS,
            capacity_pages,
        ));

        // Demand page fetcher (cache miss -> storage getPage).
        let reader0 = readers.first().cloned().ok_or_else(|| {
            Error::Io(std::io::Error::other("no readers"))
        })?;
        let page_fetcher: Arc<
            dyn Fn(PageId, u64) -> futures::future::LocalBoxFuture<'static, Option<Page>>,
        > = Arc::new(move |pid, need| {
            let reader0 = Arc::clone(&reader0);
            Box::pin(async move {
                let mut backoff_ms = 1u64;
                for _ in 0..200 {
                    match reader0.get_page(pid).await {
                        Ok(Some((page, _page_lsn, durable))) => {
                            if durable >= need {
                                return Some(page);
                            }
                        }
                        Ok(None) => return None,
                        Err(_) => {}
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(50);
                }
                None
            })
        });

        let next_page_id = Arc::new(AtomicU64::new(1));
        let min_read_lsn = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let min_read_lsn2 = Arc::clone(&min_read_lsn);
        let page_fetcher2 = Arc::clone(&page_fetcher);

        let fetcher_for_provider: Arc<
            dyn Fn(PageId) -> futures::future::LocalBoxFuture<'static, Option<Page>>,
        > = Arc::new(move |pid| {
            let need = min_read_lsn2.load(std::sync::atomic::Ordering::Acquire);
            page_fetcher2(pid, need)
        });

        let provider = Arc::new(TxnPageProvider::new(
            page_cache.clone(),
            next_page_id,
            BTREE_META_PAGE_ID,
            fetcher_for_provider,
        ));
        let tree = Arc::new(PageBPlusTree::with_provider((*provider).clone()));

        let this = Self {
            sequencer,
            readers: Arc::new(readers),
            min_read_lsn,
            active_reads: Arc::new(ActiveReads::new()),
            page_cache,
            page_fetcher,
            provider,
            tree,
            secondary_indexes: Arc::new(SecondaryIndexManager::default()),
            secondary_posting_log: Arc::new(Mutex::new(SecondaryPostingLogState::default())),
            txn_registry: Arc::new(Mutex::new(HashMap::new())),
            default_rw_txn_timeout: Self::read_duration_env_ms("SCALE_KV_RW_TXN_TIMEOUT_MS", 5_000),
            txn_reaper_interval: Self::read_duration_env_ms("SCALE_KV_TXN_REAPER_MS", 100),
        };

        // Initialize or recover meta/root.
        this.recover_or_init().await?;
        this.spawn_txn_reaper(local);
        Ok(this)
    }

    pub async fn connect_inprocess(local: &LocalSet) -> Result<Self> {
        // Buffer pool sizing:
        // Align with PostgreSQL default shared_buffers=128MB.
        // Our page size is 16KB, so 128MB ~= 8192 pages.
        // Override via env SCALE_KV_PAGE_CACHE_PAGES.
        let default_pages: usize = 8 * 1024;
        let capacity_pages: usize = std::env::var("SCALE_KV_PAGE_CACHE_PAGES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default_pages);

        let page_cache = Arc::new(PageCache::new_with_capacity(
            DEFAULT_PAGE_CACHE_SHARDS,
            capacity_pages,
        ));
        let page_store = Arc::new(InProcessPageStore::default());
        let sequencer =
            Sequencer::InProcess(Arc::new(InProcessSequencer::new(Arc::clone(&page_store))));

        let page_fetcher: Arc<
            dyn Fn(PageId, u64) -> futures::future::LocalBoxFuture<'static, Option<Page>>,
        > = Arc::new(move |pid, _need| {
            let page_store = Arc::clone(&page_store);
            Box::pin(async move { page_store.get(pid).await })
        });

        let next_page_id = Arc::new(AtomicU64::new(1));
        let min_read_lsn = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let min_read_lsn2 = Arc::clone(&min_read_lsn);
        let page_fetcher2 = Arc::clone(&page_fetcher);
        let fetcher_for_provider: Arc<
            dyn Fn(PageId) -> futures::future::LocalBoxFuture<'static, Option<Page>>,
        > = Arc::new(move |pid| {
            let need = min_read_lsn2.load(std::sync::atomic::Ordering::Acquire);
            page_fetcher2(pid, need)
        });

        let provider = Arc::new(TxnPageProvider::new(
            page_cache.clone(),
            next_page_id,
            BTREE_META_PAGE_ID,
            fetcher_for_provider,
        ));
        let tree = Arc::new(PageBPlusTree::with_provider((*provider).clone()));

        let this = Self {
            sequencer,
            readers: Arc::new(Vec::new()),
            min_read_lsn,
            active_reads: Arc::new(ActiveReads::new()),
            page_cache,
            page_fetcher,
            provider,
            tree,
            secondary_indexes: Arc::new(SecondaryIndexManager::default()),
            secondary_posting_log: Arc::new(Mutex::new(SecondaryPostingLogState::default())),
            txn_registry: Arc::new(Mutex::new(HashMap::new())),
            default_rw_txn_timeout: Self::read_duration_env_ms("SCALE_KV_RW_TXN_TIMEOUT_MS", 5_000),
            txn_reaper_interval: Self::read_duration_env_ms("SCALE_KV_TXN_REAPER_MS", 100),
        };

        this.recover_or_init().await?;
        this.spawn_txn_reaper(local);
        Ok(this)
    }

    fn spawn_txn_reaper(&self, _local: &LocalSet) {
        let compute = self.clone();
        tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(compute.txn_reaper_interval).await;
                let expired = {
                    let now = Instant::now();
                    let registry = compute.txn_registry.lock().await;
                    registry
                        .iter()
                        .filter_map(|(txn_id, pending)| {
                            if pending.deadline <= now {
                                Some(*txn_id)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                };
                if expired.is_empty() {
                    continue;
                }
                for txn_id in expired {
                    let write_keys = {
                        let now = Instant::now();
                        let registry = compute.txn_registry.lock().await;
                        registry
                            .get(&txn_id)
                            .and_then(|pending| {
                                if pending.deadline <= now {
                                    Some(pending.write_keys.iter().copied().collect::<Vec<_>>())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default()
                    };
                    let _ = compute.resolve_txn_intents(txn_id, &write_keys).await;
                    let _ = compute.remove_pending_txn(txn_id).await;
                }
            }
        });
    }

    pub(crate) fn begin_ro(&self) -> u64 {
        self.sequencer.begin_ro()
    }

    async fn register_pending_txn(&self, txn_id: u64, deadline: Instant) {
        let mut registry = self.txn_registry.lock().await;
        registry.insert(
            txn_id,
            PendingTxn {
                deadline,
                write_keys: BTreeSet::new(),
                notify: Arc::new(tokio::sync::Notify::new()),
            },
        );
    }

    async fn record_txn_write(&self, txn_id: u64, key: [u8; KEY_SIZE]) {
        let mut registry = self.txn_registry.lock().await;
        if let Some(pending) = registry.get_mut(&txn_id) {
            pending.write_keys.insert(key);
        }
    }

    async fn remove_pending_txn(&self, txn_id: u64) -> Option<PendingTxn<KEY_SIZE>> {
        let pending = self.txn_registry.lock().await.remove(&txn_id);
        if let Some(p) = &pending {
            p.notify.notify_waiters();
        }
        pending
    }

    async fn wait_on_pending_txn(&self, txn_id: u64, grace: Duration) -> Result<()> {
        loop {
            let pending = { self.txn_registry.lock().await.get(&txn_id).cloned() };
            let Some(pending) = pending else {
                return Ok(());
            };

            let deadline = pending.deadline + grace;
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "timed out waiting for intent txn_id={} deadline_with_grace_elapsed",
                        txn_id
                    ),
                )));
            }

            let sleep_until = tokio::time::Instant::from_std(deadline);
            match tokio::time::timeout_at(sleep_until, pending.notify.notified()).await {
                Ok(_) => {}
                Err(_) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!(
                            "timed out waiting for intent txn_id={} deadline_with_grace_elapsed",
                            txn_id
                        ),
                    )));
                }
            }
        }
    }

    async fn resolve_txn_intents(&self, txn_id: u64, keys: &[[u8; KEY_SIZE]]) -> Result<()> {
        for key in keys {
            let Some(current) = self.tree.get(key).await else {
                continue;
            };
            if (current.meta.flags & FLAG_INTENT) == 0 || current.meta.intent_txn_id != txn_id {
                continue;
            }
            let mut undo_ptr = current.meta.undo_ptr;
            loop {
                let Some(ptr) = undo_ptr else {
                    if let Some(latest) = self.tree.get(key).await
                        && (latest.meta.flags & FLAG_INTENT) != 0
                        && latest.meta.intent_txn_id == txn_id
                        && latest.meta.undo_ptr.is_none()
                    {
                        self.tree.remove(key).await?;
                    }
                    break;
                };
                let upage = self
                    .page_cache
                    .get(ptr.page_id)
                    .ok_or(Error::InMemoryPageMissing(ptr.page_id))?;
                let rec = undo_pg::read_record_ref(&upage, ptr.slot_id)?;
                if (rec.old_flags & FLAG_INTENT) != 0 {
                    undo_ptr = rec.prev;
                    continue;
                }
                if rec.old_commit_lsn == 0 && rec.prev.is_none() {
                    if let Some(latest) = self.tree.get(key).await
                        && (latest.meta.flags & FLAG_INTENT) != 0
                        && latest.meta.intent_txn_id == txn_id
                    {
                        self.tree.remove(key).await?;
                    }
                } else {
                    let mut restored_value = [0u8; VALUE_SIZE];
                    restored_value.copy_from_slice(&rec.old_value);
                    let restored = LeafValue {
                        value: restored_value,
                        commit_lsn: rec.old_commit_lsn,
                        undo_ptr: rec.prev,
                        flags: rec.old_flags & !FLAG_INTENT,
                        intent_txn_id: 0,
                        intent_lsn: 0,
                    };
                    if let Some(latest) = self.tree.get(key).await
                        && (latest.meta.flags & FLAG_INTENT) != 0
                        && latest.meta.intent_txn_id == txn_id
                    {
                        self.tree.insert(key.to_vec(), restored).await?;
                    }
                }
                break;
            }
        }
        Ok(())
    }

    /// Begin a read snapshot and keep it active (for GC watermarking) until the guard is dropped.
    pub fn begin_ro_guard(&self) -> (u64, ReadGuard) {
        let read_lsn = self.begin_ro();
        let guard = self.active_reads.register(read_lsn);
        (read_lsn, guard)
    }

    /// Safe GC watermark: minimum active read_lsn if any, otherwise durable_lsn.
    fn gc_lsn(&self) -> u64 {
        self.active_reads
            .min_read_lsn()
            .unwrap_or_else(|| self.durable_lsn())
    }

    pub fn durable_lsn(&self) -> u64 {
        self.sequencer.durable_lsn()
    }

    pub(crate) fn warmed_pages(&self) -> usize {
        self.page_cache.len()
    }

    #[doc(hidden)]
    pub fn cached_page(&self, page_id: PageId) -> Option<Page> {
        self.page_cache.get(page_id)
    }

    #[doc(hidden)]
    pub async fn get_page(&self, page_id: PageId, need_lsn: u64) -> Option<Page> {
        if let Some(p) = self.page_cache.get(page_id) {
            return Some(p);
        }
        let p = (self.page_fetcher)(page_id, need_lsn).await?;
        self.page_cache.insert(page_id, p.clone());
        Some(p)
    }

    /// Warm up compute by scanning pages from storage and caching them locally.
    pub async fn warmup_scan_all(&self, limit_per_batch: u32) -> Result<usize> {
        if self.readers.is_empty() {
            return Ok(self.warmed_pages());
        }
        let limit_per_batch = limit_per_batch.max(1);
        let reader = self.readers.first().ok_or_else(|| {
            Error::Io(std::io::Error::other(
                "no storage readers",
            ))
        })?;

        let mut start: PageId = 0;
        loop {
            let (pages, _durable) = reader.scan_pages(start, limit_per_batch).await?;
            if pages.is_empty() {
                break;
            }
            for (page_id, _page_lsn, page) in &pages {
                self.page_cache.insert(*page_id, page.clone());
            }
            if let Some((last_id, _, _)) = pages.last() {
                start = last_id.saturating_add(1);
            } else {
                break;
            }
        }
        Ok(self.warmed_pages())
    }

    fn begin_tx(&self, read_only: bool, timeout: Option<Duration>) -> EmbeddedTxn {
        let read_lsn = self.begin_ro();
        // make sure demand paging won't read behind our snapshot fence
        self.min_read_lsn
            .fetch_max(read_lsn, std::sync::atomic::Ordering::AcqRel);

        let read_guard = Some(self.active_reads.register(read_lsn));

        EmbeddedTxn {
            compute: self.clone(),
            read_lsn,
            read_guard,
            started_at: Instant::now(),
            timeout,
            read_only,
            write_attempted: false,
            dirty: BTreeMap::new(),
            bptree_dirty: Rc::new(RefCell::new(BTreeMap::new())),
            ro_cache: BTreeMap::new(),
            modified: BTreeSet::new(),
            secondary_mutations: Vec::new(),
            undo_txn_id: if read_only {
                None
            } else {
                Some(self.sequencer.allocate_request_id())
            },
            // Use txn snapshot LSN as intent base for first writes in this txn.
            intent_base_lsn: if read_only { 0 } else { read_lsn },
            undo_segment_first_page_id: None,
            undo_segment_last_page_id: None,
            undo_segment_last_record: None,
            undo_segment_record_count: 0,
        }
    }

    /// Begin a read-write transaction.
    pub async fn begin_rw(&self) -> EmbeddedTxn {
        let tx = self.begin_tx(false, None);
        if let Some(txn_id) = tx.undo_txn_id {
            let deadline = tx.registry_deadline(self.default_rw_txn_timeout);
            self.register_pending_txn(txn_id, deadline).await;
        }
        tx
    }

    /// Begin a read-only transaction with timeout.
    pub fn begin_ro_timeout(&self, timeout: Duration) -> EmbeddedTxn {
        self.begin_tx(true, Some(timeout))
    }

    /// Begin a read-write transaction with timeout.
    pub async fn begin_rw_timeout(&self, timeout: Duration) -> EmbeddedTxn {
        let tx = self.begin_tx(false, Some(timeout));
        if let Some(txn_id) = tx.undo_txn_id {
            let deadline = tx.registry_deadline(self.default_rw_txn_timeout);
            self.register_pending_txn(txn_id, deadline).await;
        }
        tx
    }

    /// Convenience: write a single page after-image in its own txn.
    #[doc(hidden)]
    pub async fn write_page(&self, page_id: PageId, page: Page) -> Result<u64> {
        let mut tx = self.begin_rw().await;
        tx.write_page(page_id, page);
        tx.commit().await
    }

    /// Create a secondary B+Tree index over a fixed byte range in VALUE and backfill existing rows.
    pub async fn create_btree_secondary_index(
        &self,
        name: &str,
        value_offset: usize,
        value_len: usize,
    ) -> Result<()> {
        self.secondary_indexes
            .create_btree_index(name, value_offset, value_len)
            .await?;
        if let Err(err) = self.persist_secondary_index_catalog().await {
            let _ = self.secondary_indexes.drop_index(name).await;
            return Err(err);
        }
        if let Err(err) = self.backfill_secondary_index(name).await {
            let _ = self.secondary_indexes.drop_index(name).await;
            let _ = self.persist_secondary_index_catalog().await;
            return Err(err);
        }
        Ok(())
    }

    /// Drop a previously created secondary index. Returns whether an index was removed.
    pub async fn drop_secondary_index(&self, name: &str) -> Result<bool> {
        let dropped = self.secondary_indexes.drop_index(name).await;
        if dropped {
            self.persist_secondary_index_catalog().await?;
        }
        Ok(dropped)
    }

    /// List all configured secondary indexes.
    pub async fn list_secondary_indexes(&self) -> Vec<SecondaryIndexDefinition> {
        self.secondary_indexes.list_indexes().await
    }

    /// Query one secondary index with exact-match semantics at a read-only snapshot.
    pub async fn scan_secondary_index_eq(
        &self,
        index_name: &str,
        secondary_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.begin_tx(true, None);
        tx.scan_secondary_index_eq(index_name, secondary_key, limit)
            .await
    }

    /// Put a fixed-size value (VALUE_SIZE) for a fixed-size key (KEY_SIZE).
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        if value.len() != VALUE_SIZE {
            return Err(Error::InvalidValueSize(value.len(), VALUE_SIZE));
        }

        let mut tx = self.begin_rw().await;
        tx.put(key, value).await?;
        tx.commit().await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin_tx(true, None);
        tx.get(key).await
    }

    /// Return whether `key` is visible at this snapshot without materializing value bytes.
    pub async fn get_exists(&self, key: &[u8]) -> Result<bool> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin_tx(true, None);
        tx.get_exists(key).await
    }

    /// Scan the primary key range `[start, end]` (inclusive) at a read-only snapshot.
    ///
    /// Returns up to `limit` visible key/value pairs according to this scan's `read_lsn`.
    pub async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if start.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(start.len(), KEY_SIZE));
        }
        if end.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(end.len(), KEY_SIZE));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.begin_tx(true, None);
        tx.scan_range(start, end, limit).await
    }

    /// Count visible rows in `[start, end]` (inclusive) up to `limit` without materializing values.
    pub async fn scan_range_exists_count(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<usize> {
        if start.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(start.len(), KEY_SIZE));
        }
        if end.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(end.len(), KEY_SIZE));
        }
        if limit == 0 {
            return Ok(0);
        }
        let mut tx = self.begin_tx(true, None);
        tx.scan_range_exists_count(start, end, limit).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<u64> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin_rw().await;
        tx.delete(key).await?;
        tx.commit().await
    }

    /// Run one GC pass over primary-key rows up to `budget_pages` entries.
    pub async fn gc_once(&self, budget_pages: usize) -> Result<usize> {
        if budget_pages == 0 {
            return Ok(0);
        }
        let gc_lsn = self.gc_lsn();
        let tx = self.begin_rw().await;
        let start = vec![0u8; KEY_SIZE];
        let end = vec![0xFFu8; KEY_SIZE];

        let entries = tx.tree_range(&start, &end).await;

        let mut scanned = 0usize;
        let mut keys_to_remove: Vec<Vec<u8>> = Vec::new();
        let mut rows_to_update: Vec<(Vec<u8>, LeafValue)> = Vec::new();

        for (key, row) in entries {
            if scanned >= budget_pages {
                break;
            }
            scanned += 1;
            if row.meta.commit_lsn == 0 || row.meta.commit_lsn > gc_lsn {
                continue;
            }

            if (row.meta.flags & FLAG_TOMBSTONE) != 0 {
                keys_to_remove.push(key);
                continue;
            }

            if row.meta.undo_ptr.is_some() {
                let mut meta = row.meta;
                meta.undo_ptr = None;
                rows_to_update.push((key, EmbeddedTxn::owned_leaf_from_ref(&row, meta)));
            }
        }

        if !keys_to_remove.is_empty() || !rows_to_update.is_empty() {
            for key in keys_to_remove {
                let _ = tx.tree_remove(&key).await;
            }
            for (key, row) in rows_to_update {
                tx.tree_insert(key, row).await?;
            }
            tx.commit().await?;
        }

        let _ = self.gc_purge_undo_segments(gc_lsn).await;
        Ok(scanned)
    }

    async fn gc_purge_undo_segments(&self, gc_lsn: u64) -> Result<()> {
        let meta_bytes = if let Some(p) = self.page_cache.get(META_PAGE_ID) {
            p
        } else {
            self.get_page(META_PAGE_ID, gc_lsn)
                .await
                .ok_or(Error::InMemoryPageMissing(META_PAGE_ID))?
        };
        let mut meta = MetaPage::decode(&meta_bytes)?;

        let mut dirty_pages: BTreeMap<PageId, Page> = BTreeMap::new();
        loop {
            let head = meta.undo_history_head;
            if head == 0 {
                break;
            }

            let mut head_page = self
                .get_page(head, gc_lsn)
                .await
                .ok_or(Error::InMemoryPageMissing(head))?
                .to_vec();
            let mut head_hdr = undo_pg::read_segment_header(&head_page)?;
            if head_hdr.state != SEGMENT_STATE_COMMITTED || head_hdr.commit_lsn > gc_lsn {
                break;
            }

            let next = head_hdr.history_next;
            if next == 0 {
                meta.undo_history_head = 0;
                meta.undo_history_tail = 0;
            } else {
                meta.undo_history_head = next;
                let mut next_page = self
                    .get_page(next, gc_lsn)
                    .await
                    .ok_or(Error::InMemoryPageMissing(next))?
                    .to_vec();
                let mut next_hdr = undo_pg::read_segment_header(&next_page)?;
                next_hdr.history_prev = 0;
                undo_pg::write_segment_header(&mut next_page, next_hdr)?;
                dirty_pages.insert(next, Page::from(next_page));
            }

            let mut pid = head_hdr.first_page_id;
            while pid != 0 {
                let page = self
                    .get_page(pid, gc_lsn)
                    .await
                    .ok_or(Error::InMemoryPageMissing(pid))?;
                let next_pid = undo_pg::page_next_id(&page)?;
                meta.push_free_undo(pid);
                pid = next_pid;
            }

            head_hdr.state = SEGMENT_STATE_PURGED;
            head_hdr.history_prev = 0;
            head_hdr.history_next = 0;
            undo_pg::write_segment_header(&mut head_page, head_hdr)?;
            dirty_pages.insert(head, Page::from(head_page));
        }

        if dirty_pages.is_empty() {
            return Ok(());
        }

        dirty_pages.insert(META_PAGE_ID, meta.encode());
        let pages: Vec<(PageId, Page)> = dirty_pages.into_iter().collect();
        let reserve_n = pages.len().max(1);
        let (request_id, start_lsn, end_lsn) = self.sequencer.reserve_txn(reserve_n)?;
        let _ = self
            .commit_pages_reserved(request_id, start_lsn, end_lsn, pages)
            .await?;
        Ok(())
    }

    async fn commit_pages_reserved(
        &self,
        request_id: u64,
        start_lsn: u64,
        end_lsn: u64,
        pages: Vec<(PageId, Page)>,
    ) -> Result<u64> {
        // Dedup by page_id: later wins.
        let mut map: BTreeMap<PageId, Page> = BTreeMap::new();
        for (pid, p) in pages {
            map.insert(pid, p);
        }
        let writes: Vec<(PageId, Page)> = map.into_iter().collect();
        // ensure page size
        for (_pid, p) in &writes {
            if p.len() != PAGE_SIZE {
                return Err(Error::InvalidPageSize(p.len(), PAGE_SIZE));
            }
        }

        let commit_lsn = self
            .sequencer
            .commit_pages_reserved(request_id, start_lsn, end_lsn, writes.clone())
            .await?;
        for (pid, p) in writes {
            self.page_cache.insert(pid, p);
        }
        Ok(commit_lsn)
    }

    async fn write_pages_direct(&self, pages: Vec<(PageId, Page)>) -> Result<u64> {
        if pages.is_empty() {
            return Ok(self.durable_lsn());
        }
        let reserve_n = pages.len().max(1);
        let (request_id, start_lsn, end_lsn) = self.sequencer.reserve_txn(reserve_n)?;
        self.commit_pages_reserved(request_id, start_lsn, end_lsn, pages)
            .await
    }
}

/// Buffered transaction that tracks dirty pages (tree + meta + undo pages).
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,
    read_lsn: u64,
    // Keeps this txn's snapshot active for GC watermarking.
    read_guard: Option<ReadGuard>,
    started_at: Instant,
    timeout: Option<Duration>,
    read_only: bool,
    write_attempted: bool,

    dirty: BTreeMap<PageId, Page>,
    // Limitation: writes from different txns still share `page_cache` images.
    // This fixes commit accounting by isolating dirty tracking, not page-level isolation.
    bptree_dirty: Rc<RefCell<BTreeMap<PageId, Page>>>,
    ro_cache: BTreeMap<PageId, Arc<Page>>,
    modified: BTreeSet<[u8; KEY_SIZE]>,
    secondary_mutations: Vec<SecondaryIndexMutation>,

    undo_txn_id: Option<u64>,
    intent_base_lsn: u64,
    undo_segment_first_page_id: Option<PageId>,
    undo_segment_last_page_id: Option<PageId>,
    undo_segment_last_record: Option<UndoPtr>,
    undo_segment_record_count: u32,
}

impl Drop for EmbeddedTxn {
    fn drop(&mut self) {
        // Ensure we never leak active read snapshots if a txn is dropped early.
        let _ = self.read_guard.take();
    }
}

impl EmbeddedTxn {
    fn owned_leaf_from_ref(row: &LeafValueRef, meta: LeafValueMeta) -> LeafValue {
        let mut value = [0u8; VALUE_SIZE];
        value.copy_from_slice(&row.value);
        LeafValue {
            value,
            commit_lsn: meta.commit_lsn,
            undo_ptr: meta.undo_ptr,
            flags: meta.flags,
            intent_txn_id: meta.intent_txn_id,
            intent_lsn: meta.intent_lsn,
        }
    }

    fn value_array_from_ref(row: &LeafValueRef) -> [u8; VALUE_SIZE] {
        let mut value = [0u8; VALUE_SIZE];
        value.copy_from_slice(&row.value);
        value
    }

    async fn with_bptree_dirty<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        CURRENT_TXN_DIRTY
            .scope(self.bptree_dirty.clone(), fut)
            .await
    }

    fn merge_bptree_dirty(&mut self) {
        for (pid, page) in self.bptree_dirty.borrow().iter() {
            self.dirty.insert(*pid, page.clone());
        }
    }

    async fn tree_get(&self, key: &[u8]) -> Option<LeafValueRef> {
        self.with_bptree_dirty(self.compute.tree.get(key)).await
    }

    async fn tree_insert(&self, key: Vec<u8>, value: LeafValue) -> Result<()> {
        self.with_bptree_dirty(self.compute.tree.insert(key, value))
            .await
    }

    async fn tree_remove(&self, key: &[u8]) -> Result<()> {
        self.with_bptree_dirty(self.compute.tree.remove(key)).await
    }

    async fn tree_range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, LeafValueRef)> {
        self.with_bptree_dirty(self.compute.tree.range(start, end))
            .await
    }

    async fn tree_range_meta(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, LeafValueMeta)> {
        self.with_bptree_dirty(self.compute.tree.range_meta(start, end))
            .await
    }

    async fn tree_debug_leaf_entries(&self, key: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.with_bptree_dirty(self.compute.tree.debug_leaf_entries(key, limit))
            .await
    }

    async fn build_secondary_log_pages(
        &self,
        commit_lsn: u64,
        start_state: &SecondaryPostingLogState,
    ) -> Result<(BTreeMap<PageId, Page>, SecondaryPostingLogState)> {
        if self.secondary_mutations.is_empty() {
            return Ok((BTreeMap::new(), start_state.clone()));
        }

        let mut state = start_state.clone();
        let mut pages: BTreeMap<PageId, Page> = BTreeMap::new();
        let mut tail_page_id = state.tail_page_id;
        let mut tail_page = if tail_page_id == 0 {
            let new_id = state.next_page_id;
            state.next_page_id = state.next_page_id.saturating_add(1);
            state.head_page_id = new_id;
            state.tail_page_id = new_id;
            tail_page_id = new_id;
            new_log_page().to_vec()
        } else if let Some(p) = self.dirty.get(&tail_page_id).cloned() {
            p.to_vec()
        } else if let Some(p) = self.ro_cache.get(&tail_page_id).map(|p| p.as_ref().clone()) {
            p.to_vec()
        } else if let Some(p) = self.compute.page_cache.get(tail_page_id) {
            p.to_vec()
        } else if let Some(p) = self.compute.get_page(tail_page_id, self.read_lsn).await {
            p.to_vec()
        } else {
            return Err(Error::InMemoryPageMissing(tail_page_id));
        };

        for m in &self.secondary_mutations {
            if append_posting_record(&mut tail_page, commit_lsn, m)? {
                continue;
            }
            let new_id = state.next_page_id;
            state.next_page_id = state.next_page_id.saturating_add(1);
            write_posting_next_page_id(&mut tail_page, new_id)?;
            pages.insert(tail_page_id, Page::from(tail_page));

            tail_page = new_log_page().to_vec();
            if !append_posting_record(&mut tail_page, commit_lsn, m)? {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "posting mutation record too large",
                )));
            }
            tail_page_id = new_id;
            state.tail_page_id = new_id;
        }

        pages.insert(tail_page_id, Page::from(tail_page));
        let defs = self.compute.secondary_indexes.list_indexes().await;
        let catalog_page = encode_secondary_index_catalog(&SecondaryIndexCatalog {
            defs,
            posting_log: state.clone(),
        })?;
        pages.insert(SECONDARY_INDEX_META_PAGE_ID, catalog_page);
        Ok((pages, state))
    }

    fn registry_deadline(&self, default_timeout: Duration) -> Instant {
        self.started_at + self.timeout.unwrap_or(default_timeout)
    }

    fn ensure_not_timed_out(&self) -> Result<()> {
        if let Some(timeout) = self.timeout
            && self.started_at.elapsed() > timeout
        {
            return Err(Error::TxnTimeout);
        }
        Ok(())
    }

    pub fn write_page(&mut self, page_id: PageId, page: Page) {
        if self.read_only {
            self.write_attempted = true;
            return;
        }
        // Stage into txn-local dirty map.
        self.dirty.insert(page_id, page.clone());
        // Also update shared page_cache so helper subsystems that consult page_cache
        // can observe the latest in-memory page image.
        self.compute.page_cache.insert(page_id, page);
    }

    async fn get_page_for_read(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        if let Some(p) = self.dirty.get(&page_id) {
            return Some(Arc::new(p.clone()));
        }
        if let Some(p) = self.ro_cache.get(&page_id) {
            return Some(Arc::clone(p));
        }
        if let Some(p) = self.compute.page_cache.get_arc(page_id) {
            self.ro_cache.insert(page_id, Arc::clone(&p));
            return Some(p);
        }
        let p = (self.compute.page_fetcher)(page_id, self.read_lsn).await?;
        let p = Arc::new(p);
        self.compute.page_cache.insert_arc(page_id, Arc::clone(&p));
        self.ro_cache.insert(page_id, Arc::clone(&p));
        Some(p)
    }

    async fn append_undo(
        &mut self,
        data_page_id: PageId,
        data_slot_id: u16,
        prev: Option<UndoPtr>,
        old_commit_lsn: u64,
        old_flags: u16,
        old_value: &[u8],
    ) -> Result<UndoPtr> {
        if old_value.len() != VALUE_SIZE {
            return Err(Error::InvalidValueSize(old_value.len(), VALUE_SIZE));
        }
        let txn_id = self.undo_txn_id.ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "read-only transaction",
            ))
        })?;

        let meta_bytes = self
            .compute
            .page_cache
            .get(META_PAGE_ID)
            .or_else(|| self.dirty.get(&META_PAGE_ID).cloned())
            .ok_or(Error::InMemoryPageMissing(META_PAGE_ID))?;
        let mut meta = MetaPage::decode(&meta_bytes)?;
        if meta.next_undo_page_id < 2_000_000 {
            meta.next_undo_page_id = 2_000_000;
        }

        let alloc_page_id = |meta: &mut MetaPage| -> PageId {
            if let Some(free) = meta.pop_free_undo() {
                free
            } else {
                let pid = meta.next_undo_page_id;
                meta.next_undo_page_id += 1;
                pid
            }
        };

        let mut current_id = if let Some(pid) = self.undo_segment_last_page_id {
            pid
        } else {
            let pid = alloc_page_id(&mut meta);
            let segment_page = undo_pg::new_undo_segment_page(txn_id, pid);
            self.undo_segment_first_page_id = Some(pid);
            self.undo_segment_last_page_id = Some(pid);
            self.write_page(pid, segment_page);
            pid
        };

        let mut page = self
            .dirty
            .get(&current_id)
            .cloned()
            .or_else(|| {
                self.compute
                    .page_cache
                    .get_arc(current_id)
                    .map(|p| p.as_ref().clone())
            })
            .or_else(|| self.ro_cache.get(&current_id).map(|p| p.as_ref().clone()))
            .ok_or(Error::InMemoryPageMissing(current_id))?
            .to_vec();

        let mut old_value_arr = [0u8; VALUE_SIZE];
        old_value_arr.copy_from_slice(old_value);
        let rec = UndoRecord {
            data_page_id,
            data_slot_id,
            prev,
            txn_id,
            txn_next: self.undo_segment_last_record,
            old_commit_lsn,
            old_flags,
            old_value: old_value_arr,
        };

        let slot = match undo_pg::append_record(&mut page, &rec) {
            Ok(s) => s,
            Err(_) => {
                let new_pid = alloc_page_id(&mut meta);
                undo_pg::set_page_next_id(&mut page, new_pid)?;
                self.write_page(current_id, Page::from(page));

                let mut new_page = undo_pg::new_undo_page().to_vec();
                let s = undo_pg::append_record(&mut new_page, &rec)?;
                current_id = new_pid;
                page = new_page;
                s
            }
        };

        self.undo_segment_last_page_id = Some(current_id);
        let ptr = UndoPtr {
            page_id: current_id,
            slot_id: slot,
        };
        self.undo_segment_last_record = Some(ptr);
        self.undo_segment_record_count = self.undo_segment_record_count.saturating_add(1);

        self.write_page(current_id, Page::from(page));
        self.write_page(META_PAGE_ID, meta.encode());

        Ok(ptr)
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_not_timed_out()?;
        if self.read_only {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "read-only transaction",
            )));
        }
        let existing = self.tree_get(key).await;
        let old_value = existing.as_ref().map(Self::value_array_from_ref);
        let mut new_value = [0u8; VALUE_SIZE];
        new_value.copy_from_slice(value);
        let key_arr: [u8; KEY_SIZE] = key.try_into().unwrap();
        let planned_mutations = self.compute.secondary_indexes.plan_mutations(
            key_arr,
            old_value.as_ref(),
            Some(&new_value),
        ).await?;

        let mut row = LeafValue {
            value: new_value,
            commit_lsn: 0,
            undo_ptr: None,
            flags: FLAG_INTENT,
            intent_txn_id: self.undo_txn_id.unwrap_or(0),
            intent_lsn: self.intent_base_lsn,
        };
        if let Some(old) = existing {
            if (old.meta.flags & FLAG_INTENT) != 0
                && self.undo_txn_id == Some(old.meta.intent_txn_id)
            {
                row.undo_ptr = old.meta.undo_ptr;
                row.intent_lsn = old.meta.intent_lsn;
            } else {
                if (old.meta.flags & FLAG_INTENT) != 0
                    && self.undo_txn_id != Some(old.meta.intent_txn_id)
                {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!(
                            "encountered foreign intent txn_id={}",
                            old.meta.intent_txn_id
                        ),
                    )));
                }
                row.undo_ptr = Some(
                    self.append_undo(
                        0,
                        0,
                        old.meta.undo_ptr,
                        old.meta.commit_lsn,
                        old.meta.flags,
                        &old.value,
                    )
                    .await?,
                );
            }
        }

        self.tree_insert(key.to_vec(), row).await?;
        self.modified.insert(key_arr);
        self.secondary_mutations.extend(planned_mutations);
        if let Some(txn_id) = self.undo_txn_id {
            self.compute.record_txn_write(txn_id, key_arr).await;
        }

        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_not_timed_out()?;
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        self.read_visible_value_from_row(key).await
    }

    pub async fn get_exists(&mut self, key: &[u8]) -> Result<bool> {
        self.ensure_not_timed_out()?;
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        self.read_visible_exists_from_row(key).await
    }

    /// Scan the primary key range `[start, end]` (inclusive) at this txn snapshot.
    ///
    /// Visibility is evaluated using MVCC metadata (`commit_lsn` and undo chain) at `read_lsn`.
    /// Returns at most `limit` visible rows.
    pub async fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.ensure_not_timed_out()?;
        if start.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(start.len(), KEY_SIZE));
        }
        if end.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(end.len(), KEY_SIZE));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }

        let entries = self.tree_range(start, end).await;

        let mut out = Vec::with_capacity(limit.min(entries.len()));
        for (key, row) in entries {
            if let Some(value) = self.read_visible_value_from_scan_entry(&key, row).await? {
                out.push((key, value));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Count visible rows in `[start, end]` (inclusive), up to `limit`, without copying values.
    pub async fn scan_range_exists_count(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<usize> {
        self.ensure_not_timed_out()?;
        if start.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(start.len(), KEY_SIZE));
        }
        if end.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(end.len(), KEY_SIZE));
        }
        if limit == 0 {
            return Ok(0);
        }

        let entries = self.tree_range_meta(start, end).await;

        let mut visible = 0usize;
        for (key, row) in entries {
            if self.read_visible_exists_from_scan_entry(&key, row).await? {
                visible += 1;
                if visible >= limit {
                    break;
                }
            }
        }
        Ok(visible)
    }

    pub async fn scan_secondary_index_eq(
        &mut self,
        index_name: &str,
        secondary_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.ensure_not_timed_out()?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let pks = self.compute.secondary_indexes.query_equal(
            index_name,
            secondary_key,
            self.read_lsn,
            limit,
        ).await?;
        let mut out = Vec::with_capacity(pks.len());
        for pk in pks {
            let key = pk.to_vec();
            if let Some(value) = self.read_visible_value_from_row(&key).await? {
                out.push((key, value));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub async fn debug_check_mapping(&mut self, key: &[u8]) -> Result<()> {
        let (row, leaf_entries) = {
            let row = self.tree_get(key).await;
            let leaf_entries = self.tree_debug_leaf_entries(key, 16).await;
            (row, leaf_entries)
        };

        let Some(row) = row else {
            eprintln!(
                "[verify] missing key in btree: key_hex={}",
                hex::encode(key)
            );
            return Ok(());
        };

        let leaf_entries_hex: Vec<(String, String)> = leaf_entries
            .into_iter()
            .map(|(k, v)| (hex::encode(k), hex::encode(v)))
            .collect();
        eprintln!(
            "[verify] inline row: key_hex={} commit_lsn={} flags={} undo={:?} leaf_entries_hex={:?}",
            hex::encode(key),
            row.meta.commit_lsn,
            row.meta.flags,
            row.meta.undo_ptr,
            leaf_entries_hex
        );

        Ok(())
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.ensure_not_timed_out()?;
        if self.read_only {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "read-only transaction",
            )));
        }
        let existing = self.tree_get(key).await;

        if let Some(old) = existing {
            let key_arr: [u8; KEY_SIZE] = key.try_into().unwrap();
            let old_value = Self::value_array_from_ref(&old);
            let planned_mutations = self
                .compute
                .secondary_indexes
                .plan_mutations(key_arr, Some(&old_value), None)
                .await?;
            if (old.meta.flags & FLAG_INTENT) != 0
                && self.undo_txn_id != Some(old.meta.intent_txn_id)
            {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "encountered foreign intent txn_id={}",
                        old.meta.intent_txn_id
                    ),
                )));
            }
            let undo_ptr = if (old.meta.flags & FLAG_INTENT) != 0
                && self.undo_txn_id == Some(old.meta.intent_txn_id)
            {
                old.meta.undo_ptr
            } else {
                Some(
                    self.append_undo(
                        0,
                        0,
                        old.meta.undo_ptr,
                        old.meta.commit_lsn,
                        old.meta.flags,
                        &old.value,
                    )
                    .await?,
                )
            };
            let row = LeafValue {
                value: {
                    let mut v = [0u8; VALUE_SIZE];
                    v.copy_from_slice(&old.value);
                    v
                },
                commit_lsn: 0,
                undo_ptr,
                flags: FLAG_TOMBSTONE | FLAG_INTENT,
                intent_txn_id: self.undo_txn_id.unwrap_or(0),
                intent_lsn: if (old.meta.flags & FLAG_INTENT) != 0
                    && self.undo_txn_id == Some(old.meta.intent_txn_id)
                {
                    old.meta.intent_lsn
                } else {
                    self.intent_base_lsn
                },
            };
            self.tree_insert(key.to_vec(), row).await?;
            self.modified.insert(key_arr);
            self.secondary_mutations.extend(planned_mutations);
            if let Some(txn_id) = self.undo_txn_id {
                self.compute.record_txn_write(txn_id, key_arr).await;
            }
        }

        Ok(())
    }

    pub async fn abort(mut self) -> Result<()> {
        if self.read_only {
            let _ = self.read_guard.take();
            return Ok(());
        }
        let txn_id = self.undo_txn_id.unwrap_or(0);
        let keys = self.modified.iter().copied().collect::<Vec<_>>();
        let out = self.compute.resolve_txn_intents(txn_id, &keys).await;
        let _ = self.compute.remove_pending_txn(txn_id).await;
        let _ = self.read_guard.take();
        out.map(|_| ())
    }

    pub async fn commit(mut self) -> Result<u64> {
        self.ensure_not_timed_out()?;
        if self.read_only {
            let wrote = self.write_attempted
                || !self.dirty.is_empty()
                || !self.bptree_dirty.borrow().is_empty()
                || !self.modified.is_empty();
            let _ = self.read_guard.take();
            if wrote {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "read-only transaction",
                )));
            }
            return Ok(self.read_lsn);
        }

        let meta = if let Some(p) = self.dirty.get(&META_PAGE_ID) {
            MetaPage::decode(p)?
        } else if let Some(p) = self.compute.page_cache.get(META_PAGE_ID) {
            MetaPage::decode(&p)?
        } else {
            MetaPage {
                root_page_id: 10,
                next_bptree_page_id: self.compute.provider.next_page_id(),
                next_data_page_id: 1_000_000,
                next_undo_page_id: 2_000_000,
                undo_free: Vec::new(),
                undo_history_head: 0,
                undo_history_tail: 0,
            }
        };

        if let Some(_first_pid) = self.undo_segment_first_page_id {
            let old_tail = meta.undo_history_tail;
            if old_tail != 0 && !self.dirty.contains_key(&old_tail) {
                let tail_page = self
                    .compute
                    .page_cache
                    .get(old_tail)
                    .or_else(|| self.ro_cache.get(&old_tail).map(|p| p.as_ref().clone()))
                    .ok_or(Error::InMemoryPageMissing(old_tail))?;
                self.write_page(old_tail, tail_page);
            }
        }

        let base_meta = meta.clone();
        let request_id = self
            .undo_txn_id
            .unwrap_or_else(|| self.compute.sequencer.allocate_request_id());
        // Pre-stamp modified rows with a dummy commit_lsn so B+Tree writes dirty all pages
        // that will be touched again when stamping the real commit_lsn.
        if !self.modified.is_empty() {
            for key in &self.modified {
                if let Some(row) = self.tree_get(key).await {
                    if (row.meta.flags & FLAG_INTENT) == 0 || row.meta.intent_txn_id != request_id {
                        let _ = self.read_guard.take();
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "intent ownership changed before commit",
                        )));
                    }
                    let mut meta = row.meta;
                    meta.commit_lsn = 1;
                    self.tree_insert(key.to_vec(), Self::owned_leaf_from_ref(&row, meta))
                        .await?;
                } else {
                    let _ = self.read_guard.take();
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "intent missing before commit",
                    )));
                }
            }
        }
        self.merge_bptree_dirty();

        // Build a stable final page-id set first, then reserve once.
        let mut map: BTreeMap<PageId, Page> = self.dirty.clone();
        let mut count_meta = base_meta.clone();
        {
            count_meta.root_page_id = self.compute.tree.root_page_id();
            count_meta.next_bptree_page_id = self.compute.provider.next_page_id();
        }
        if let Some(first_pid) = self.undo_segment_first_page_id {
            if let std::collections::btree_map::Entry::Vacant(e) = map.entry(first_pid) {
                let head_page = self
                    .compute
                    .page_cache
                    .get(first_pid)
                    .or_else(|| self.ro_cache.get(&first_pid).map(|p| p.as_ref().clone()))
                    .ok_or(Error::InMemoryPageMissing(first_pid))?;
                e.insert(head_page);
            }
            let old_tail = base_meta.undo_history_tail;
            if old_tail != 0 && !map.contains_key(&old_tail) {
                let tail_page = self
                    .compute
                    .page_cache
                    .get(old_tail)
                    .or_else(|| self.ro_cache.get(&old_tail).map(|p| p.as_ref().clone()))
                    .ok_or(Error::InMemoryPageMissing(old_tail))?;
                map.insert(old_tail, tail_page);
            }
        }
        map.insert(META_PAGE_ID, count_meta.encode());

        let mut posting_state_guard_opt = None;
        let mut posting_state_before_opt = None;
        let reserve_n = if self.secondary_mutations.is_empty() {
            map.len().max(1)
        } else {
            let guard = self.compute.secondary_posting_log.lock().await;
            let posting_state_before = guard.clone();
            let (secondary_pages_probe, _secondary_state_probe) = self
                .build_secondary_log_pages(0, &posting_state_before)
                .await?;
            posting_state_before_opt = Some(posting_state_before);
            posting_state_guard_opt = Some(guard);
            map.len().saturating_add(secondary_pages_probe.len()).max(1)
        };

        let (start_lsn, end_lsn) = self
            .compute
            .sequencer
            .reserve_txn_with_request_id(reserve_n, request_id)?;
        let commit_lsn = end_lsn;

        // Stamp the real commit_lsn and merge the dirty pages generated by those writes.
        if !self.modified.is_empty() {
            for key in &self.modified {
                if let Some(row) = self.tree_get(key).await {
                    if (row.meta.flags & FLAG_INTENT) == 0 || row.meta.intent_txn_id != request_id {
                        let _ = self.read_guard.take();
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "intent ownership changed before commit",
                        )));
                    }
                    let mut meta = row.meta;
                    meta.commit_lsn = commit_lsn;
                    meta.flags &= !FLAG_INTENT;
                    meta.intent_txn_id = 0;
                    meta.intent_lsn = 0;
                    self.tree_insert(key.to_vec(), Self::owned_leaf_from_ref(&row, meta))
                        .await?;
                }
            }
            for (pid, page) in self.bptree_dirty.borrow().iter() {
                map.insert(*pid, page.clone());
            }
        }

        let mut meta = base_meta.clone();
        {
            meta.root_page_id = self.compute.tree.root_page_id();
            meta.next_bptree_page_id = self.compute.provider.next_page_id();
        }

        if let Some(first_pid) = self.undo_segment_first_page_id {
            let last_pid = self.undo_segment_last_page_id.unwrap_or(first_pid);
            let mut head_page = map
                .get(&first_pid)
                .cloned()
                .or_else(|| self.compute.page_cache.get(first_pid))
                .ok_or(Error::InMemoryPageMissing(first_pid))?
                .to_vec();

            let old_tail = base_meta.undo_history_tail;
            let hdr = UndoSegmentHeader {
                txn_id: request_id,
                begin_lsn: start_lsn,
                commit_lsn,
                state: SEGMENT_STATE_COMMITTED,
                first_page_id: first_pid,
                last_page_id: last_pid,
                record_count: self.undo_segment_record_count,
                history_prev: old_tail,
                history_next: 0,
            };
            undo_pg::write_segment_header(&mut head_page, hdr)?;
            map.insert(first_pid, Page::from(head_page));

            if old_tail != 0 {
                let mut tail_page = map
                    .get(&old_tail)
                    .cloned()
                    .or_else(|| self.compute.page_cache.get(old_tail))
                    .or_else(|| self.ro_cache.get(&old_tail).map(|p| p.as_ref().clone()))
                    .ok_or(Error::InMemoryPageMissing(old_tail))?
                    .to_vec();
                let mut tail_hdr = undo_pg::read_segment_header(&tail_page)?;
                tail_hdr.history_next = first_pid;
                undo_pg::write_segment_header(&mut tail_page, tail_hdr)?;
                map.insert(old_tail, Page::from(tail_page));
            } else {
                meta.undo_history_head = first_pid;
            }
            meta.undo_history_tail = first_pid;
        }
        map.insert(META_PAGE_ID, meta.encode());

        let mut secondary_state_after_opt = None;
        if !self.secondary_mutations.is_empty() {
            let posting_state_before = posting_state_before_opt
                .as_ref()
                .expect("secondary posting state before missing");
            let (secondary_pages, secondary_state_after) = self
                .build_secondary_log_pages(commit_lsn, posting_state_before)
                .await?;
            for (pid, page) in secondary_pages {
                map.insert(pid, page);
            }
            secondary_state_after_opt = Some(secondary_state_after);
        }

        if map.len() != reserve_n {
            let _ = self.read_guard.take();
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "commit page set changed: reserved={} final={}",
                    reserve_n,
                    map.len()
                ),
            )));
        }

        let pages: Vec<(PageId, Page)> = map.into_iter().collect();
        let out = self
            .compute
            .commit_pages_reserved(request_id, start_lsn, end_lsn, pages)
            .await;

        if out.is_ok() {
            self.compute
                .secondary_indexes
                .apply_commit(commit_lsn, &self.secondary_mutations)
                .await;
            if let Some(mut posting_state_guard) = posting_state_guard_opt {
                if let Some(secondary_state_after) = secondary_state_after_opt {
                    *posting_state_guard = secondary_state_after;
                }
                drop(posting_state_guard);
            }
            let _ = self.compute.remove_pending_txn(request_id).await;
        } else if let Some(mut posting_state_guard) = posting_state_guard_opt
            && let Some(posting_state_before) = posting_state_before_opt
        {
            *posting_state_guard = posting_state_before;
        }

        // Mark snapshot inactive before returning.
        let _ = self.read_guard.take();

        out
    }
}

#[cfg(test)]
#[path = "embedded_compute_tests.rs"]
mod tests;
