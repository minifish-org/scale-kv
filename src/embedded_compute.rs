use crate::compute_sequencer::ComputeSequencer;
use crate::meta_page::{META_PAGE_ID, MetaPage};
use crate::page_bptree::{
    AsyncPageProvider, DEFAULT_PAGE_CACHE_SHARDS, LeafValue, PageBPlusTree, PageCache,
};
use crate::txn_page_provider::TxnPageProvider;
use crate::{ActiveReads, ReadGuard};
use crate::{BTREE_META_PAGE_ID, BtreeMeta};
use crate::{
    Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, StorageClient, VALUE_SIZE,
    undo_pg::{self, UndoPtr, UndoRecord},
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::LocalSet;

/// Embedded compute-side API for an Aurora-style KV (page-level redo).
///
/// Compute owns the B+Tree and the page cache. Writes are page after-images.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Arc<ComputeSequencer>,
    readers: Arc<Vec<Arc<StorageClient>>>,

    min_read_lsn: Arc<std::sync::atomic::AtomicU64>,
    active_reads: Arc<ActiveReads>,

    page_cache: Arc<PageCache>,
    page_fetcher:
        Arc<dyn Fn(PageId, u64) -> futures::future::LocalBoxFuture<'static, Option<Page>>>,
    provider: Arc<TxnPageProvider>,
    tree: Arc<Mutex<PageBPlusTree<TxnPageProvider>>>,
}

impl EmbeddedCompute {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        let sequencer = Arc::new(ComputeSequencer::connect(addrs, quorum, local).await?);

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
        let reader0 = readers.get(0).cloned().ok_or_else(|| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::Other, "no readers"))
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
        let tree = Arc::new(Mutex::new(PageBPlusTree::with_provider(
            (*provider).clone(),
        )));

        let this = Self {
            sequencer,
            readers: Arc::new(readers),
            min_read_lsn,
            active_reads: Arc::new(ActiveReads::new()),
            page_cache,
            page_fetcher,
            provider,
            tree,
        };

        // Initialize or recover meta/root.
        this.recover_or_init().await?;
        Ok(this)
    }

    pub(crate) fn begin_ro(&self) -> u64 {
        self.sequencer.begin_ro()
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
        let limit_per_batch = limit_per_batch.max(1);
        let reader = self.readers.get(0).ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
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

    async fn recover_or_init(&self) -> Result<()> {
        // Warmup first (best-effort). If storage is empty, we'll init below.
        let _ = self.warmup_scan_all(256).await;

        if let Some(global_meta_bytes) = self.page_cache.get(META_PAGE_ID) {
            let mut meta = MetaPage::decode(&global_meta_bytes)?;
            self.provider.set_next_page_id(meta.next_bptree_page_id);

            // Backward-compat guard: older meta pages may have next_undo_page_id=0.
            if meta.next_undo_page_id < 2_000_000 {
                meta.next_undo_page_id = 2_000_000;
                // best-effort persist
                let _ = self.write_page(META_PAGE_ID, meta.encode()).await;
            }

            // Prefer btree metapage for root.
            if let Some(btree_meta_bytes) = self.page_cache.get(BTREE_META_PAGE_ID) {
                let bm = BtreeMeta::decode(&btree_meta_bytes)?;
                self.provider.set_root_page_id(bm.root_page_id);
            } else {
                // Fallback: root in global meta.
                self.provider.set_root_page_id(meta.root_page_id);
            }
            return Ok(());
        }

        // Cold start: create meta + root page as one txn.
        // PageId plan:
        // - 0: meta
        // - 10.. : bptree pages
        // - 2_000_000.. : undo pages

        const BPTREE_ROOT_ID: PageId = 10;
        const UNDO_BASE: PageId = 2_000_000;

        let mut tx = self.begin_rw();

        // Configure provider root + next_page_id (next after reserved ids).
        let mut tree = self.tree.lock().await;
        tree.provider_mut().set_root_page_id(BPTREE_ROOT_ID);
        self.provider.set_next_page_id(BPTREE_ROOT_ID + 1);

        // Root leaf page.
        let root_page = crate::page_bptree::new_page(2, 0);
        tx.write_page(BPTREE_ROOT_ID, root_page);

        // B-Tree metapage.
        tx.write_page(
            BTREE_META_PAGE_ID,
            BtreeMeta {
                root_page_id: BPTREE_ROOT_ID,
            }
            .encode(),
        );

        // Meta page.
        let meta = MetaPage {
            root_page_id: BPTREE_ROOT_ID,
            next_bptree_page_id: BPTREE_ROOT_ID + 1,
            next_data_page_id: 1_000_000,
            next_undo_page_id: UNDO_BASE,
            undo_free: Vec::new(),
        };
        tx.write_page(META_PAGE_ID, meta.encode());

        drop(tree);
        tx.commit().await?;
        Ok(())
    }

    fn begin_tx(&self, read_only: bool, timeout: Option<Duration>) -> EmbeddedTxn {
        // Clear dirty pages collected by provider from any previous operations.
        let _ = self.provider.take_dirty();
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
            ro_cache: BTreeMap::new(),
            modified: Vec::new(),
            undo_page_id: None,
        }
    }

    /// Begin a read-write transaction.
    pub fn begin_rw(&self) -> EmbeddedTxn {
        self.begin_tx(false, None)
    }

    /// Begin a read-only transaction with timeout.
    pub fn begin_ro_timeout(&self, timeout: Duration) -> EmbeddedTxn {
        self.begin_tx(true, Some(timeout))
    }

    /// Begin a read-write transaction with timeout.
    pub fn begin_rw_timeout(&self, timeout: Duration) -> EmbeddedTxn {
        self.begin_tx(false, Some(timeout))
    }

    /// Convenience: write a single page after-image in its own txn.
    #[doc(hidden)]
    pub async fn write_page(&self, page_id: PageId, page: Page) -> Result<u64> {
        let mut tx = self.begin_rw();
        tx.write_page(page_id, page);
        tx.commit().await
    }

    /// Put a fixed-size value (VALUE_SIZE) for a fixed-size key (KEY_SIZE).
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        if value.len() != VALUE_SIZE {
            return Err(Error::InvalidValueSize(value.len(), VALUE_SIZE));
        }

        let mut tx = self.begin_rw();
        tx.put(key, value).await?;
        tx.commit().await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin_rw();
        tx.get(key).await
    }

    /// Scan the primary key range `[start, end]` (inclusive) at a read-only snapshot.
    ///
    /// Returns up to `limit` visible key/value pairs according to this scan's `read_lsn`.
    pub async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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

    pub async fn delete(&self, key: &[u8]) -> Result<u64> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin_rw();
        tx.delete(key).await?;
        tx.commit().await
    }

    /// Run one GC pass over primary-key rows up to `budget_pages` entries.
    pub async fn gc_once(&self, budget_pages: usize) -> Result<usize> {
        if budget_pages == 0 {
            return Ok(0);
        }
        let gc_lsn = self.gc_lsn();
        let tx = self.begin_rw();
        let start = vec![0u8; KEY_SIZE];
        let end = vec![0xFFu8; KEY_SIZE];

        let entries = {
            let tree = tx.compute.tree.lock().await;
            tree.range(&start, &end).await
        };

        let mut scanned = 0usize;
        let mut keys_to_remove: Vec<Vec<u8>> = Vec::new();
        let mut rows_to_update: Vec<(Vec<u8>, LeafValue)> = Vec::new();

        for (key, mut row) in entries {
            if scanned >= budget_pages {
                break;
            }
            scanned += 1;
            if row.commit_lsn == 0 || row.commit_lsn > gc_lsn {
                continue;
            }

            if (row.flags & 1) != 0 {
                keys_to_remove.push(key);
                continue;
            }

            if row.undo_ptr.is_some() {
                row.undo_ptr = None;
                rows_to_update.push((key, row));
            }
        }

        if !keys_to_remove.is_empty() || !rows_to_update.is_empty() {
            let mut tree = tx.compute.tree.lock().await;
            for key in keys_to_remove {
                let _ = tree.remove(&key).await;
            }
            for (key, row) in rows_to_update {
                tree.insert(key, row).await?;
            }
            drop(tree);
            tx.commit().await?;
        }

        let _ = self.gc_sweep_undo(gc_lsn).await;
        Ok(scanned)
    }

    async fn gc_sweep_undo(&self, gc_lsn: u64) -> Result<()> {
        use std::collections::HashSet;

        let meta_bytes = if let Some(p) = self.page_cache.get(META_PAGE_ID) {
            p
        } else {
            self.get_page(META_PAGE_ID, gc_lsn)
                .await
                .ok_or(Error::InMemoryPageMissing(META_PAGE_ID))?
        };
        let mut meta = MetaPage::decode(&meta_bytes)?;

        const UNDO_BASE: PageId = 2_000_000;

        // 1) Collect all live undo pages reachable from inline row head pointers.
        let mut live_undo: HashSet<PageId> = HashSet::new();
        let start = vec![0u8; KEY_SIZE];
        let end = vec![0xFFu8; KEY_SIZE];
        let entries = {
            let tree = self.tree.lock().await;
            tree.range(&start, &end).await
        };

        for (_key, row) in entries {
            let mut ptr = row.undo_ptr;
            while let Some(p) = ptr {
                let _ = live_undo.insert(p.page_id);
                let upage = self.get_page(p.page_id, gc_lsn).await;
                let Some(upage) = upage else {
                    break;
                };
                let rec = undo_pg::read_record(&upage, p.slot_id)?;
                ptr = rec.prev;
            }
        }

        // 2) Free unreachable undo pages.
        // Note: we only add to freelist; actual reuse happens in append_undo.
        for pid in UNDO_BASE..meta.next_undo_page_id {
            if !live_undo.contains(&pid) {
                meta.push_free_undo(pid);
            }
        }

        // Persist meta update.
        let _ = self.write_page(META_PAGE_ID, meta.encode()).await?;
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
            .commit_reserved_txn_batch(request_id, start_lsn, end_lsn, writes.clone())
            .await?;
        for (pid, p) in writes {
            self.page_cache.insert(pid, p);
        }
        Ok(commit_lsn)
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
    ro_cache: BTreeMap<PageId, Page>,
    modified: Vec<[u8; KEY_SIZE]>,

    // undo page writer (single page buffered)
    undo_page_id: Option<PageId>,
}

impl Drop for EmbeddedTxn {
    fn drop(&mut self) {
        // Ensure we never leak active read snapshots if a txn is dropped early.
        let _ = self.read_guard.take();
    }
}

impl EmbeddedTxn {
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
        // Also update shared page_cache so helper subsystems (e.g. FSM hint tree)
        // that consult only page_cache can observe txn-local changes.
        // NOTE: EmbeddedCompute currently assumes a single writer; if we later add
        // concurrent txns, this must become txn-scoped.
        self.compute.page_cache.insert(page_id, page);
    }

    async fn get_page_for_read(&mut self, page_id: PageId) -> Option<Page> {
        if let Some(p) = self.dirty.get(&page_id).cloned() {
            return Some(p);
        }
        if let Some(p) = self.ro_cache.get(&page_id).cloned() {
            return Some(p);
        }
        if let Some(p) = self.compute.page_cache.get(page_id) {
            self.ro_cache.insert(page_id, p.clone());
            return Some(p);
        }
        let p = (self.compute.page_fetcher)(page_id, self.read_lsn).await?;
        self.compute.page_cache.insert(page_id, p.clone());
        self.ro_cache.insert(page_id, p.clone());
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

        // Load meta so we can allocate undo pages.
        let meta_bytes = self
            .compute
            .page_cache
            .get(META_PAGE_ID)
            .or_else(|| self.dirty.get(&META_PAGE_ID).cloned())
            .ok_or(Error::InMemoryPageMissing(META_PAGE_ID))?;
        let mut meta = MetaPage::decode(&meta_bytes)?;

        let mut current_id = self.undo_page_id;
        let mut page = match current_id {
            Some(pid) => self
                .get_page_for_read(pid)
                .await
                .unwrap_or_else(undo_pg::new_undo_page),
            None => undo_pg::new_undo_page(),
        };
        if meta.next_undo_page_id < 2_000_000 {
            meta.next_undo_page_id = 2_000_000;
        }
        if current_id.is_none() {
            if let Some(free) = meta.pop_free_undo() {
                current_id = Some(free);
            } else {
                current_id = Some(meta.next_undo_page_id);
                meta.next_undo_page_id += 1;
            }
        }

        let mut old_value_arr = [0u8; VALUE_SIZE];
        old_value_arr.copy_from_slice(old_value);
        let rec = UndoRecord {
            data_page_id,
            data_slot_id,
            prev,
            old_commit_lsn,
            old_flags,
            old_value: old_value_arr,
        };

        let slot = match undo_pg::append_record(&mut page, &rec) {
            Ok(s) => s,
            Err(_) => {
                // Current undo page full: persist it, allocate a new one.
                let pid = current_id.unwrap();
                self.write_page(pid, page);

                let new_pid = meta.pop_free_undo().unwrap_or_else(|| {
                    let pid = meta.next_undo_page_id;
                    meta.next_undo_page_id += 1;
                    pid
                });
                let mut new_page = undo_pg::new_undo_page();
                let s = undo_pg::append_record(&mut new_page, &rec)?;
                current_id = Some(new_pid);
                page = new_page;
                s
            }
        };

        // Stage undo page + meta.
        let pid = current_id.unwrap();
        self.undo_page_id = Some(pid);
        self.write_page(pid, page);
        self.write_page(META_PAGE_ID, meta.encode());

        Ok(UndoPtr {
            page_id: pid,
            slot_id: slot,
        })
    }

    async fn read_visible_value_from_row(&mut self, row: &LeafValue) -> Result<Option<Vec<u8>>> {
        if row.commit_lsn <= self.read_lsn {
            if (row.flags & 1) != 0 {
                return Ok(None);
            }
            return Ok(Some(row.value.to_vec()));
        }

        // Not visible at this snapshot: walk undo chain to find latest visible version.
        let mut undo = row.undo_ptr;
        while let Some(ptr) = undo {
            let upage = self
                .get_page_for_read(ptr.page_id)
                .await
                .ok_or(Error::InMemoryPageMissing(ptr.page_id))?;
            let rec = undo_pg::read_record(&upage, ptr.slot_id)?;
            if rec.old_commit_lsn <= self.read_lsn {
                if (rec.old_flags & 1) != 0 {
                    return Ok(None);
                }
                return Ok(Some(rec.old_value.to_vec()));
            }
            undo = rec.prev;
        }

        Ok(None)
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_not_timed_out()?;
        if self.read_only {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "read-only transaction",
            )));
        }
        let existing = {
            let tree = self.compute.tree.lock().await;
            tree.get(key).await
        };
        let mut new_value = [0u8; VALUE_SIZE];
        new_value.copy_from_slice(value);

        let mut row = LeafValue {
            value: new_value,
            commit_lsn: 0,
            undo_ptr: None,
            flags: 0,
        };
        if let Some(old) = existing {
            row.undo_ptr = Some(
                self.append_undo(0, 0, old.undo_ptr, old.commit_lsn, old.flags, &old.value)
                    .await?,
            );
        }

        {
            let mut tree = self.compute.tree.lock().await;
            tree.insert(key.to_vec(), row).await?;
        }
        self.modified.push(key.try_into().unwrap());

        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_not_timed_out()?;
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let row = {
            let tree = self.compute.tree.lock().await;
            tree.get(key).await
        };
        let Some(row) = row else {
            return Ok(None);
        };

        self.read_visible_value_from_row(&row).await
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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

        let entries = {
            let tree = self.compute.tree.lock().await;
            tree.range(start, end).await
        };

        let mut out = Vec::with_capacity(limit.min(entries.len()));
        for (key, row) in entries {
            if let Some(value) = self.read_visible_value_from_row(&row).await? {
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
            let tree = self.compute.tree.lock().await;
            let row = tree.get(key).await;
            let leaf_entries = tree.debug_leaf_entries(key, 16).await;
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
            row.commit_lsn,
            row.flags,
            row.undo_ptr,
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
        let existing = {
            let tree = self.compute.tree.lock().await;
            tree.get(key).await
        };

        if let Some(old) = existing {
            let undo_ptr = self
                .append_undo(0, 0, old.undo_ptr, old.commit_lsn, old.flags, &old.value)
                .await?;
            let row = LeafValue {
                value: old.value,
                commit_lsn: 0,
                undo_ptr: Some(undo_ptr),
                flags: old.flags | 1,
            };
            let mut tree = self.compute.tree.lock().await;
            tree.insert(key.to_vec(), row).await?;
            drop(tree);
            self.modified.push(key.try_into().unwrap());
        }

        Ok(())
    }

    pub async fn commit(mut self) -> Result<u64> {
        self.ensure_not_timed_out()?;
        if self.read_only {
            let wrote = self.write_attempted || !self.dirty.is_empty() || !self.modified.is_empty();
            let _ = self.read_guard.take();
            if wrote {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "read-only transaction",
                )));
            }
            return Ok(self.read_lsn);
        }
        // Also persist meta page updates for next_page_id/root.
        let tree = self.compute.tree.lock().await;
        let old_meta_bytes = if let Some(p) = self.dirty.get(&META_PAGE_ID) {
            p.clone()
        } else if let Some(p) = self.compute.page_cache.get(META_PAGE_ID) {
            p
        } else {
            // During cold start, meta page may not exist yet; assume next_data_page_id is unchanged.
            MetaPage {
                root_page_id: tree.root_page_id(),
                next_bptree_page_id: self.compute.provider.next_page_id(),
                next_data_page_id: 1_000_000,
                next_undo_page_id: 2_000_000,
                undo_free: Vec::new(),
            }
            .encode()
        };
        let old_meta = MetaPage::decode(&old_meta_bytes)?;
        let meta = MetaPage {
            root_page_id: tree.root_page_id(),
            next_bptree_page_id: self.compute.provider.next_page_id(),
            next_data_page_id: old_meta.next_data_page_id,
            next_undo_page_id: old_meta.next_undo_page_id,
            undo_free: old_meta.undo_free.clone(),
        };
        drop(tree);
        self.write_page(META_PAGE_ID, meta.encode());

        // Pull dirty bptree pages from provider and stage into txn.
        for (pid, page) in self.compute.provider.take_dirty() {
            self.write_page(pid, page);
        }

        let pages_vec: Vec<(PageId, Page)> = std::mem::take(&mut self.dirty).into_iter().collect();
        let reserve_n = pages_vec.len().max(1);
        let (request_id, start_lsn, end_lsn) = self.compute.sequencer.reserve_txn(reserve_n)?;
        let commit_lsn = end_lsn;

        // Stamp commit_lsn into modified inline rows.
        if !self.modified.is_empty() {
            let mut tree = self.compute.tree.lock().await;
            for key in &self.modified {
                if let Some(mut row) = tree.get(key).await {
                    row.commit_lsn = commit_lsn;
                    tree.insert(key.to_vec(), row).await?;
                }
            }
            drop(tree);
        }

        let mut map: BTreeMap<PageId, Page> = BTreeMap::new();
        for (pid, p) in pages_vec {
            map.insert(pid, p);
        }
        for (pid, page) in self.compute.provider.take_dirty() {
            map.insert(pid, page);
        }

        let pages: Vec<(PageId, Page)> = map.into_iter().collect();
        // Commit with reserved range.
        let out = self
            .compute
            .commit_pages_reserved(request_id, start_lsn, end_lsn, pages)
            .await;

        // Mark snapshot inactive before returning.
        let _ = self.read_guard.take();

        out
    }
}
