use crate::compute_sequencer::ComputeSequencer;
use crate::meta_page::{META_PAGE_ID, MetaPage};
use crate::page_bptree::{
    DEFAULT_PAGE_CACHE_SHARDS, PageBPlusTree, PageCache, PageProvider, SlotRef as PageSlotRef,
};
use crate::txn_page_provider::TxnPageProvider;
use crate::{BTREE_META_PAGE_ID, BtreeMeta};
use crate::{
    Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, StorageClient, VALUE_SIZE, slotted_page,
    undo_pg::{self, UndoPtr, UndoRecord},
};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tokio::task::LocalSet;

/// Embedded compute-side API for an Aurora-style KV (page-level redo).
///
/// Compute owns the B+Tree and the page cache. Writes are page after-images.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Arc<ComputeSequencer>,
    readers: Arc<Vec<Arc<StorageClient>>>,

    min_read_lsn: Arc<std::sync::atomic::AtomicU64>,

    page_cache: Arc<PageCache>,
    page_fetcher: Arc<dyn Fn(PageId, u64) -> Option<Page>>,
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
        let handle = tokio::runtime::Handle::current();
        let page_fetcher: Arc<dyn Fn(PageId, u64) -> Option<Page>> = Arc::new(move |pid, need| {
            tokio::task::block_in_place(|| {
                handle.block_on(async {
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
            })
        });

        let next_page_id = Arc::new(AtomicU64::new(1));
        let min_read_lsn = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let min_read_lsn2 = Arc::clone(&min_read_lsn);
        let page_fetcher2 = Arc::clone(&page_fetcher);

        let fetcher_for_provider: Arc<dyn Fn(PageId) -> Option<Page>> = Arc::new(move |pid| {
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
            page_cache,
            page_fetcher,
            provider,
            tree,
        };

        // Initialize or recover meta/root.
        this.recover_or_init().await?;
        Ok(this)
    }

    pub fn begin_ro(&self) -> u64 {
        self.sequencer.begin_ro()
    }

    pub fn durable_lsn(&self) -> u64 {
        self.sequencer.durable_lsn()
    }

    pub fn warmed_pages(&self) -> usize {
        self.page_cache.len()
    }

    pub fn cached_page(&self, page_id: PageId) -> Option<Page> {
        self.page_cache.get(page_id)
    }

    pub fn get_page_blocking(&self, page_id: PageId, need_lsn: u64) -> Option<Page> {
        if let Some(p) = self.page_cache.get(page_id) {
            return Some(p);
        }
        let p = (self.page_fetcher)(page_id, need_lsn)?;
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
        Ok(self.page_cache.len())
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

        // Cold start: create meta + FSM + root page as one txn.
        // PageId plan (simple):
        // - 0: meta
        // - 1: fsm meta
        // - 2.. : fsm level pages
        // - 10.. : bptree pages
        // - 1_000_000.. : data pages

        const BPTREE_ROOT_ID: PageId = 10;
        const DATA_BASE: PageId = 1_000_000;
        const UNDO_BASE: PageId = 2_000_000;
        const UNDO_BASE_DEFAULT: PageId = UNDO_BASE;
        const FSM_LEVEL0_BASE: PageId = 2;
        const INIT_DATA_LEAVES: u64 = 1024; // tracks first 1024 data pages initially

        // Stash these conventions into the page cache for later use (simple approach: store them in meta in next iteration).

        let mut tx = self.begin();

        // Configure provider root + next_page_id (next after reserved ids).
        let mut tree = self.tree.lock().unwrap();
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

        // FSM pages (empty).
        let (_fsm_meta, fsm_writes) =
            crate::fsm_pg::init_fsm_pages(DATA_BASE, INIT_DATA_LEAVES, FSM_LEVEL0_BASE);
        for (pid, page) in fsm_writes {
            tx.write_page(pid, page);
        }

        // Meta page.
        let meta = MetaPage {
            root_page_id: BPTREE_ROOT_ID,
            next_bptree_page_id: BPTREE_ROOT_ID + 1,
            next_data_page_id: DATA_BASE,
            next_undo_page_id: UNDO_BASE,
        };
        tx.write_page(META_PAGE_ID, meta.encode());

        drop(tree);
        tx.commit().await?;
        Ok(())
    }

    pub fn begin(&self) -> EmbeddedTxn {
        // Clear dirty pages collected by provider from any previous operations.
        let _ = self.provider.take_dirty();
        let read_lsn = self.begin_ro();
        // make sure demand paging won't read behind our snapshot fence
        self.min_read_lsn
            .fetch_max(read_lsn, std::sync::atomic::Ordering::AcqRel);

        EmbeddedTxn {
            compute: self.clone(),
            read_lsn,
            dirty: BTreeMap::new(),
            ro_cache: BTreeMap::new(),
            modified: Vec::new(),
            undo_page_id: None,
        }
    }

    /// Convenience: write a single page after-image in its own txn.
    pub async fn write_page(&self, page_id: PageId, page: Page) -> Result<u64> {
        let mut tx = self.begin();
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

        let mut tx = self.begin();
        tx.put(key, value)?;
        tx.commit().await
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin();
        tx.get(key)
    }

    pub async fn delete(&self, key: &[u8]) -> Result<u64> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin();
        tx.delete(key)?;
        tx.commit().await
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

/// Buffered transaction that tracks all dirty pages (tree pages + data pages + meta page).
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,
    read_lsn: u64,
    dirty: BTreeMap<PageId, Page>,
    ro_cache: BTreeMap<PageId, Page>,
    modified: Vec<(PageId, u16, [u8; KEY_SIZE])>,

    // undo page writer (single page buffered)
    undo_page_id: Option<PageId>,
}

impl EmbeddedTxn {
    pub fn write_page(&mut self, page_id: PageId, page: Page) {
        self.dirty.insert(page_id, page);
    }

    fn get_page_for_read(&mut self, page_id: PageId) -> Option<Page> {
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
        let p = (self.compute.page_fetcher)(page_id, self.read_lsn)?;
        self.compute.page_cache.insert(page_id, p.clone());
        self.ro_cache.insert(page_id, p.clone());
        Some(p)
    }

    fn append_undo(
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
        let mut page = current_id
            .and_then(|pid| self.get_page_for_read(pid))
            .unwrap_or_else(undo_pg::new_undo_page);
        if meta.next_undo_page_id < 2_000_000 {
            meta.next_undo_page_id = 2_000_000;
        }
        if current_id.is_none() {
            current_id = Some(meta.next_undo_page_id);
            meta.next_undo_page_id += 1;
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

                let new_pid = meta.next_undo_page_id;
                meta.next_undo_page_id += 1;
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

    fn alloc_or_update_data_page(
        &mut self,
        existing: Option<PageSlotRef>,
        key: &[u8],
        value: &[u8],
    ) -> Result<PageSlotRef> {
        // Update existing record in-place.
        if let Some(slot) = existing {
            let mut page = self
                .get_page_for_read(slot.page_id)
                .ok_or(Error::InMemoryPageMissing(slot.page_id))?;
            // Write undo before overwriting.
            let old_commit = slotted_page::read_commit_lsn(&page, slot.slot_id, key).unwrap_or(0);
            let old_flags = slotted_page::read_flags(&page, slot.slot_id, key).unwrap_or(0);
            let old_undo = slotted_page::read_undo_ptr(&page, slot.slot_id, key);
            let old_value = slotted_page::read_value(&page, slot.slot_id, key)
                .unwrap_or_else(|| vec![0u8; VALUE_SIZE]);

            let undo_ptr = self.append_undo(
                slot.page_id,
                slot.slot_id,
                old_undo,
                old_commit,
                old_flags,
                &old_value,
            )?;

            slotted_page::overwrite_value(&mut page, slot.slot_id, key, value)?;
            slotted_page::write_flags(&mut page, slot.slot_id, 0);
            slotted_page::write_undo_ptr(&mut page, slot.slot_id, Some(undo_ptr));
            self.modified
                .push((slot.page_id, slot.slot_id, key.try_into().unwrap()));

            self.write_page(slot.page_id, page);
            return Ok(slot);
        }

        // Load meta + FSM meta.
        let meta_bytes = self
            .compute
            .page_cache
            .get(META_PAGE_ID)
            .ok_or(Error::InMemoryPageMissing(META_PAGE_ID))?;
        let mut meta = MetaPage::decode(&meta_bytes)?;

        let fsm_meta_bytes = self
            .compute
            .page_cache
            .get(crate::fsm_pg::FSM_META_PAGE_ID)
            .ok_or(Error::InMemoryPageMissing(crate::fsm_pg::FSM_META_PAGE_ID))?;
        let fsm_meta = crate::fsm_pg::FsmMeta::decode(&fsm_meta_bytes)?;

        // Required bytes: payload + possible new slot entry.
        // NOTE: slotted_page payload now includes MVCC header.
        let required = (KEY_SIZE + VALUE_SIZE + 8 + 8 + 2 + 2) + 4;
        let need_class = crate::fsm_pg::need_class(required);

        let cache = Arc::clone(&self.compute.page_cache);
        let get_page = move |pid: PageId| cache.get(pid);

        // Try find a candidate leaf index and insert.
        for _ in 0..8 {
            let cand = crate::fsm_pg::find_candidate(&fsm_meta, &get_page, need_class)?;
            let Some(leaf_idx) = cand else {
                break;
            };
            let page_id = fsm_meta.data_base + leaf_idx;
            if let Some(mut page) = self.compute.page_cache.get(page_id) {
                match slotted_page::insert_record(&mut page, key, value) {
                    Ok(slot_id) => {
                        self.write_page(page_id, page.clone());
                        // New record: remember to stamp commit_lsn.
                        self.modified
                            .push((page_id, slot_id, key.try_into().unwrap()));

                        let free = slotted_page::page_free_space(&page);
                        let class = crate::fsm_pg::class_from_free_bytes(free);
                        for (pid, p) in
                            crate::fsm_pg::update_leaf(&fsm_meta, &get_page, leaf_idx, class)?
                        {
                            self.write_page(pid, p);
                        }
                        return Ok(PageSlotRef { page_id, slot_id });
                    }
                    Err(_) => {
                        // FSM hint was stale; fix it downwards based on current page header.
                        let free = slotted_page::page_free_space(&page);
                        let class = crate::fsm_pg::class_from_free_bytes(free);
                        for (pid, p) in
                            crate::fsm_pg::update_leaf(&fsm_meta, &get_page, leaf_idx, class)?
                        {
                            self.write_page(pid, p);
                        }
                        continue;
                    }
                }
            } else {
                // Page not present yet; treat as empty (class 0).
                for (pid, p) in crate::fsm_pg::update_leaf(&fsm_meta, &get_page, leaf_idx, 0)? {
                    self.write_page(pid, p);
                }
            }
        }

        // Allocate a new data page.
        let page_id = meta.next_data_page_id;
        if page_id < fsm_meta.data_base {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "next_data_page_id below data_base",
            )));
        }
        let leaf_idx = page_id - fsm_meta.data_base;
        let mut fsm_meta = fsm_meta;
        if leaf_idx >= fsm_meta.leaf_count {
            // Grow FSM to cover this leaf.
            // Grow exponentially to reduce frequency of FSM expansion.
            let desired = leaf_idx + 1;
            let target = fsm_meta.leaf_count.saturating_mul(2).max(desired).max(1024);
            let (new_meta, grow_writes) = crate::fsm_pg::ensure_capacity(&fsm_meta, target)?;
            for (pid, p) in grow_writes {
                self.write_page(pid, p);
            }
            fsm_meta = new_meta;
        }

        let mut page = slotted_page::new_page();
        let slot_id = slotted_page::insert_record(&mut page, key, value)?;
        self.modified
            .push((page_id, slot_id, key.try_into().unwrap()));
        self.write_page(page_id, page.clone());

        let free = slotted_page::page_free_space(&page);
        let class = crate::fsm_pg::class_from_free_bytes(free);
        for (pid, p) in crate::fsm_pg::update_leaf(&fsm_meta, &get_page, leaf_idx, class)? {
            self.write_page(pid, p);
        }

        // Advance next_data_page_id and persist meta.
        meta.next_data_page_id = page_id + 1;
        self.write_page(META_PAGE_ID, meta.encode());

        Ok(PageSlotRef { page_id, slot_id })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Read existing mapping first.
        let existing = {
            let tree = self.compute.tree.lock().unwrap();
            tree.get(key)
        };

        // Write/allocate data page.
        let slot_ref = self.alloc_or_update_data_page(existing, key, value)?;

        // Update B+Tree mapping.
        {
            let mut tree = self.compute.tree.lock().unwrap();
            tree.insert(key.to_vec(), slot_ref)?;
        }

        // Debug self-check: btree mapping must point to a slot whose key matches.
        // (Catches corruption/misaligned page writes early.)
        if let Some(page) = self.get_page_for_read(slot_ref.page_id) {
            if let Some(slot_key) = slotted_page::read_key(&page, slot_ref.slot_id) {
                if slot_key.as_slice() != key {
                    eprintln!(
                        "[selfcheck] slot key mismatch: page_id={} slot_id={} key_hex={} slot_key_hex={}",
                        slot_ref.page_id,
                        slot_ref.slot_id,
                        hex::encode(key),
                        hex::encode(&slot_key)
                    );
                }
            } else {
                eprintln!(
                    "[selfcheck] slot missing after put: key={:?} page_id={} slot_id={}",
                    key, slot_ref.page_id, slot_ref.slot_id
                );
            }
        }

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let tree = self.compute.tree.lock().unwrap();
        let slot = tree.get(key);
        drop(tree);
        let Some(slot) = slot else {
            return Ok(None);
        };

        let page = self
            .get_page_for_read(slot.page_id)
            .ok_or(Error::InMemoryPageMissing(slot.page_id))?;

        let commit_lsn = slotted_page::read_commit_lsn(&page, slot.slot_id, key).unwrap_or(0);
        let flags = slotted_page::read_flags(&page, slot.slot_id, key).unwrap_or(0);
        if commit_lsn <= self.read_lsn {
            if (flags & 1) != 0 {
                return Ok(None);
            }
            return Ok(slotted_page::read_value(&page, slot.slot_id, key));
        }

        // Not visible: walk undo chain.
        let mut undo = slotted_page::read_undo_ptr(&page, slot.slot_id, key);
        while let Some(ptr) = undo {
            let upage = self
                .get_page_for_read(ptr.page_id)
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

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        // Find slotref (if any).
        let existing = {
            let tree = self.compute.tree.lock().unwrap();
            tree.get(key)
        };

        if let Some(slot) = existing {
            let mut page = self
                .get_page_for_read(slot.page_id)
                .ok_or(Error::InMemoryPageMissing(slot.page_id))?;

            // Write undo before tombstoning.
            let old_commit = slotted_page::read_commit_lsn(&page, slot.slot_id, key).unwrap_or(0);
            let old_flags = slotted_page::read_flags(&page, slot.slot_id, key).unwrap_or(0);
            let old_undo = slotted_page::read_undo_ptr(&page, slot.slot_id, key);
            let old_value = slotted_page::read_value(&page, slot.slot_id, key)
                .unwrap_or_else(|| vec![0u8; VALUE_SIZE]);

            let undo_ptr = self.append_undo(
                slot.page_id,
                slot.slot_id,
                old_undo,
                old_commit,
                old_flags,
                &old_value,
            )?;

            // Keep slot allocated; mark tombstone for snapshot reads.
            slotted_page::mark_tombstone(&mut page, slot.slot_id);
            slotted_page::write_undo_ptr(&mut page, slot.slot_id, Some(undo_ptr));
            self.modified
                .push((slot.page_id, slot.slot_id, key.try_into().unwrap()));

            self.write_page(slot.page_id, page.clone());

            // FSM: no free-space increase until GC/vacuum.
        }

        {
            let mut tree = self.compute.tree.lock().unwrap();
            tree.remove(key)?;
        }

        Ok(())
    }

    pub async fn commit(mut self) -> Result<u64> {
        // Also persist meta page updates for next_page_id/root.
        let tree = self.compute.tree.lock().unwrap();
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
            }
            .encode()
        };
        let old_meta = MetaPage::decode(&old_meta_bytes)?;
        let meta = MetaPage {
            root_page_id: tree.root_page_id(),
            next_bptree_page_id: self.compute.provider.next_page_id(),
            next_data_page_id: old_meta.next_data_page_id,
            next_undo_page_id: old_meta.next_undo_page_id,
        };
        drop(tree);
        self.write_page(META_PAGE_ID, meta.encode());

        // Pull dirty bptree pages from provider and stage into txn.
        for (pid, page) in self.compute.provider.take_dirty() {
            self.write_page(pid, page);
        }

        // Reserve LSN range first so we can stamp commit_lsn into modified records.
        let pages_vec: Vec<(PageId, Page)> = self.dirty.into_iter().collect();

        // Dedup count is computed inside commit_pages; here we pessimistically reserve on raw count.
        // This is OK (may waste a few LSNs) and keeps logic simple.
        let (request_id, start_lsn, end_lsn) =
            self.compute.sequencer.reserve_txn(pages_vec.len())?;
        let commit_lsn = end_lsn;

        // Stamp commit_lsn into modified records.
        let mut map: BTreeMap<PageId, Page> = BTreeMap::new();
        for (pid, p) in pages_vec {
            map.insert(pid, p);
        }
        for (pid, slot_id, _k) in &self.modified {
            if let Some(page) = map.get_mut(pid) {
                slotted_page::write_commit_lsn(page, *slot_id, commit_lsn);
            }
        }

        let pages: Vec<(PageId, Page)> = map.into_iter().collect();
        // Commit with reserved range.
        self.compute
            .commit_pages_reserved(request_id, start_lsn, end_lsn, pages)
            .await
    }
}
