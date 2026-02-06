use crate::compute_sequencer::ComputeSequencer;
use crate::page_bptree::{DEFAULT_PAGE_CACHE_SHARDS, PageCache};
use crate::{Error, PAGE_SIZE, Page, PageId, Result, StorageClient};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::LocalSet;

/// Embedded compute-side API for an Aurora-style KV (page-level redo).
///
/// - No compute server / network protocol required.
/// - All writes are committed as page after-images via [`ComputeSequencer`].
/// - Storage persists and replays pages into PageStore.
/// - Compute can warm up by scanning pages and caching them locally.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Arc<ComputeSequencer>,
    readers: Arc<Vec<StorageClient>>,

    /// Warmed pages. (Eventually this should back the compute-side B+Tree + data pages.)
    page_cache: Arc<PageCache>,
}

impl EmbeddedCompute {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        let sequencer = Arc::new(ComputeSequencer::connect(addrs, quorum, local).await?);

        let mut readers = Vec::with_capacity(addrs.len());
        for addr in addrs {
            readers.push(StorageClient::connect(addr, local).await?);
        }

        Ok(Self {
            sequencer,
            readers: Arc::new(readers),
            page_cache: Arc::new(PageCache::new(DEFAULT_PAGE_CACHE_SHARDS)),
        })
    }

    /// Returns a safe snapshot point for reads (quorum-durable LSN).
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

    /// Begin a read-write transaction context (buffer page writes until commit).
    pub fn begin(&self) -> EmbeddedTxn {
        EmbeddedTxn {
            compute: self.clone(),
            dirty: BTreeMap::new(),
        }
    }

    /// Convenience: auto-wrap a single page write and commit.
    pub async fn write_page(&self, page_id: PageId, page: Page) -> Result<u64> {
        let mut txn = self.begin();
        txn.write_page(page_id, page);
        txn.commit().await
    }

    /// Warm up compute by scanning pages from storage and caching them locally.
    ///
    /// Returns the number of pages cached.
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

    async fn commit_writes(&self, writes: Vec<(PageId, Page)>) -> Result<u64> {
        // Store-side expects each record to represent one page write at one LSN.
        for (_id, p) in &writes {
            if p.len() != PAGE_SIZE {
                return Err(Error::InvalidPageSize(p.len(), PAGE_SIZE));
            }
        }
        let commit_lsn = self.sequencer.commit_txn_batch(writes.clone()).await?;

        // Apply locally to cache as well.
        for (page_id, page) in writes {
            self.page_cache.insert(page_id, page);
        }

        Ok(commit_lsn)
    }
}

/// Buffered page-writes transaction.
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,

    // page_id -> latest after-image in this txn
    dirty: BTreeMap<PageId, Page>,
}

impl EmbeddedTxn {
    pub fn write_page(&mut self, page_id: PageId, page: Page) {
        self.dirty.insert(page_id, page);
    }

    pub async fn commit(self) -> Result<u64> {
        let writes: Vec<(PageId, Page)> = self.dirty.into_iter().collect();
        self.compute.commit_writes(writes).await
    }
}
