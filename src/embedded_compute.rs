use crate::compute_sequencer::ComputeSequencer;
use crate::meta_page::{META_PAGE_ID, MetaPage};
use crate::page_bptree::{
    DEFAULT_PAGE_CACHE_SHARDS, PageBPlusTree, PageCache, PageProvider, SlotRef as PageSlotRef,
};
use crate::txn_page_provider::TxnPageProvider;
use crate::{Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, StorageClient, VALUE_SIZE};
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
    readers: Arc<Vec<StorageClient>>,

    page_cache: Arc<PageCache>,
    provider: Arc<TxnPageProvider>,
    tree: Arc<Mutex<PageBPlusTree<TxnPageProvider>>>,
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

        let this = Self {
            sequencer,
            readers: Arc::new(readers),
            page_cache,
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
        let Some(slot) = slot else { return Ok(None) };
        let page = self
            .page_cache
            .get(slot.page_id)
            .ok_or(Error::InMemoryPageMissing(slot.page_id))?;
        Ok(Some(read_value_from_data_page(&page)?))
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

/// Buffered transaction that tracks all dirty pages (tree pages + data pages + meta page).
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,
    dirty: BTreeMap<PageId, Page>,
}

impl EmbeddedTxn {
    pub fn write_page(&mut self, page_id: PageId, page: Page) {
        self.dirty.insert(page_id, page);
    }

    /// Simplest data page layout: [key(16) | value(1024) | zero padding].
    fn alloc_or_reuse_data_page(
        &mut self,
        existing: Option<PageSlotRef>,
        key: &[u8],
        value: &[u8],
    ) -> Result<PageSlotRef> {
        let data_page_id = if let Some(slot) = existing {
            slot.page_id
        } else {
            self.compute.provider.alloc_page_id()
        };
        let mut page = vec![0u8; PAGE_SIZE];
        page[0..KEY_SIZE].copy_from_slice(key);
        page[KEY_SIZE..KEY_SIZE + VALUE_SIZE].copy_from_slice(value);
        self.write_page(data_page_id, page);
        Ok(PageSlotRef {
            page_id: data_page_id,
            slot_id: 0,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Read existing mapping first.
        let existing = {
            let tree = self.compute.tree.lock().unwrap();
            tree.get(key)
        };

        // Write/allocate data page.
        let slot_ref = self.alloc_or_reuse_data_page(existing, key, value)?;

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

fn read_value_from_data_page(page: &[u8]) -> Result<Vec<u8>> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    Ok(page[KEY_SIZE..KEY_SIZE + VALUE_SIZE].to_vec())
}
