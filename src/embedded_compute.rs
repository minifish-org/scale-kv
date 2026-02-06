use crate::compute_sequencer::ComputeSequencer;
use crate::meta_page::{META_PAGE_ID, MetaPage};
use crate::page_bptree::{
    DEFAULT_PAGE_CACHE_SHARDS, PageBPlusTree, PageCache, PageProvider, SlotRef as PageSlotRef,
};
use crate::txn_page_provider::TxnPageProvider;
use crate::{
    Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, StorageClient, VALUE_SIZE, slotted_page,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tokio::task::LocalSet;

/// Embedded compute-side API for an Aurora-style KV (page-level redo).
///
/// Compute owns the B+Tree and the page cache. Writes are page after-images.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Arc<ComputeSequencer>,
    readers: Arc<Vec<StorageClient>>,

    page_cache: Arc<PageCache>,
    provider: Arc<TxnPageProvider>,
    tree: Arc<Mutex<PageBPlusTree<TxnPageProvider>>>,

    fsm: Arc<Mutex<FreeSpaceMap>>,
}

impl EmbeddedCompute {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        let sequencer = Arc::new(ComputeSequencer::connect(addrs, quorum, local).await?);

        let mut readers = Vec::with_capacity(addrs.len());
        for addr in addrs {
            readers.push(StorageClient::connect(addr, local).await?);
        }

        let page_cache = Arc::new(PageCache::new(DEFAULT_PAGE_CACHE_SHARDS));
        let next_page_id = Arc::new(AtomicU64::new(1));
        let provider = Arc::new(TxnPageProvider::new(page_cache.clone(), next_page_id));
        let tree = Arc::new(Mutex::new(PageBPlusTree::with_provider(
            (*provider).clone(),
        )));

        let fsm = Arc::new(Mutex::new(FreeSpaceMap::new()));

        let this = Self {
            sequencer,
            readers: Arc::new(readers),
            page_cache,
            provider,
            tree,
            fsm,
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

        if let Some(meta_bytes) = self.page_cache.get(META_PAGE_ID) {
            let meta = MetaPage::decode(&meta_bytes)?;
            // configure provider root + next_page_id
            self.provider.set_root_page_id(meta.root_page_id);
            self.provider.set_next_page_id(meta.next_page_id);
            return Ok(());
        }

        // Cold start: create meta page + root page as one txn.
        let mut tx = self.begin();
        let root_id: PageId = 1;
        let next_id: PageId = 2;

        // Create empty root leaf page using a temporary in-txn provider write.
        let mut tree = self.tree.lock().unwrap();
        tree.provider_mut().set_root_page_id(root_id);
        self.provider.set_next_page_id(next_id);

        // Root leaf page: ask the B+Tree helper to init via new_with_provider semantics.
        // We mimic it here to avoid reallocating ids.
        let root_page = crate::page_bptree::new_page(2, 0); // PAGE_TYPE_LEAF=2, level=0
        tx.write_page(root_id, root_page);

        let meta = MetaPage {
            root_page_id: root_id,
            next_page_id: next_id,
        };
        tx.write_page(META_PAGE_ID, meta.encode());

        drop(tree);
        tx.commit().await?;
        Ok(())
    }

    pub fn begin(&self) -> EmbeddedTxn {
        // Clear dirty pages collected by provider from any previous operations.
        let _ = self.provider.take_dirty();
        EmbeddedTxn {
            compute: self.clone(),
            dirty: BTreeMap::new(),
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
        let tree = self.tree.lock().unwrap();
        let slot = tree.get(key);
        drop(tree);
        let Some(slot) = slot else {
            return Ok(None);
        };
        let page = self
            .page_cache
            .get(slot.page_id)
            .ok_or(Error::InMemoryPageMissing(slot.page_id))?;
        let v = slotted_page::read_value(&page, slot.slot_id, key).ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "record missing",
            ))
        })?;
        Ok(Some(v))
    }

    pub async fn delete(&self, key: &[u8]) -> Result<u64> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let mut tx = self.begin();
        tx.delete(key)?;
        tx.commit().await
    }

    async fn commit_pages(&self, pages: Vec<(PageId, Page)>) -> Result<u64> {
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

        let commit_lsn = self.sequencer.commit_txn_batch(writes.clone()).await?;
        for (pid, p) in writes {
            self.page_cache.insert(pid, p);
        }
        Ok(commit_lsn)
    }
}

const BUCKET_SIZE: usize = 1024;

fn bucket_count() -> usize {
    PAGE_SIZE / BUCKET_SIZE + 1
}

fn bucket_index(free: usize) -> usize {
    let count = bucket_count();
    if count == 0 {
        0
    } else {
        (free / BUCKET_SIZE).min(count - 1)
    }
}

/// Very simple in-memory free-space map for data pages.
///
/// It only tracks pages we have already created/updated in this process.
struct FreeSpaceMap {
    buckets: Vec<VecDeque<PageId>>,
    free_space: HashMap<PageId, usize>,
}

impl FreeSpaceMap {
    fn new() -> Self {
        let mut buckets = Vec::with_capacity(bucket_count());
        for _ in 0..bucket_count() {
            buckets.push(VecDeque::new());
        }
        Self {
            buckets,
            free_space: HashMap::new(),
        }
    }

    fn track_page(&mut self, page_id: PageId, free: usize) {
        self.free_space.insert(page_id, free);
        let idx = bucket_index(free);
        self.buckets[idx].push_back(page_id);
    }

    fn update_page(&mut self, page_id: PageId, free: usize) {
        self.track_page(page_id, free)
    }

    fn pick_page(&mut self, required: usize) -> Option<PageId> {
        let start = bucket_index(required);
        for idx in start..self.buckets.len() {
            while let Some(page_id) = self.buckets[idx].pop_front() {
                if let Some(free) = self.free_space.get(&page_id) {
                    if *free >= required {
                        return Some(page_id);
                    }
                }
            }
        }
        None
    }
}

/// Buffered transaction that tracks all dirty pages (tree pages + data pages + meta page).
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,
    dirty: BTreeMap<PageId, Page>,
}

impl EmbeddedTxn {
    pub fn write_page(&mut self, page_id: PageId, page: Page) {
        self.dirty.insert(page_id, page);
    }

    fn alloc_or_update_data_page(
        &mut self,
        existing: Option<PageSlotRef>,
        key: &[u8],
        value: &[u8],
    ) -> Result<PageSlotRef> {
        if let Some(slot) = existing {
            // In-place overwrite value.
            let mut page = self
                .compute
                .page_cache
                .get(slot.page_id)
                .ok_or(Error::InMemoryPageMissing(slot.page_id))?;
            slotted_page::overwrite_value(&mut page, slot.slot_id, key, value)?;
            self.write_page(slot.page_id, page);
            return Ok(slot);
        }

        // Try reuse an existing page with enough free space.
        // Required bytes: payload + (maybe) slot entry.
        let required = KEY_SIZE + VALUE_SIZE + 4;

        // Check candidate pages from FSM.
        let picked = { self.compute.fsm.lock().unwrap().pick_page(required) };
        if let Some(page_id) = picked {
            if let Some(mut page) = self.compute.page_cache.get(page_id) {
                if let Ok(slot_id) = slotted_page::insert_record(&mut page, key, value) {
                    self.write_page(page_id, page.clone());
                    // Update FSM with new free space.
                    let free = slotted_page::page_free_space(&page);
                    self.compute.fsm.lock().unwrap().update_page(page_id, free);
                    return Ok(PageSlotRef { page_id, slot_id });
                }
            }
        }

        // Otherwise allocate a new data page.
        let data_page_id = self.compute.provider.alloc_page_id();
        let mut page = slotted_page::new_page();
        let slot_id = slotted_page::insert_record(&mut page, key, value)?;
        let free = slotted_page::page_free_space(&page);
        self.compute
            .fsm
            .lock()
            .unwrap()
            .track_page(data_page_id, free);
        self.write_page(data_page_id, page);
        Ok(PageSlotRef {
            page_id: data_page_id,
            slot_id,
        })
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

        // Pull dirty bptree pages from provider and stage into txn.
        for (pid, page) in self.compute.provider.take_dirty() {
            self.write_page(pid, page);
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        // Find slotref (if any).
        let existing = {
            let tree = self.compute.tree.lock().unwrap();
            tree.get(key)
        };

        if let Some(slot) = existing {
            let mut page = self
                .compute
                .page_cache
                .get(slot.page_id)
                .ok_or(Error::InMemoryPageMissing(slot.page_id))?;
            slotted_page::clear_slot(&mut page, slot.slot_id);
            self.write_page(slot.page_id, page);
        }

        {
            let mut tree = self.compute.tree.lock().unwrap();
            tree.remove(key)?;
        }

        for (pid, page) in self.compute.provider.take_dirty() {
            self.write_page(pid, page);
        }
        Ok(())
    }

    pub async fn commit(mut self) -> Result<u64> {
        // Also persist meta page updates for next_page_id/root.
        let tree = self.compute.tree.lock().unwrap();
        let meta = MetaPage {
            root_page_id: tree.root_page_id(),
            next_page_id: self.compute.provider.next_page_id(),
        };
        drop(tree);
        self.write_page(META_PAGE_ID, meta.encode());

        let pages: Vec<(PageId, Page)> = self.dirty.into_iter().collect();
        self.compute.commit_pages(pages).await
    }
}
