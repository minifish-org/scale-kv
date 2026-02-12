use crate::page_bptree::{AsyncPageProvider, PageCache, PageLatchTable};
use crate::{Page, PageId};
use futures::future::LocalBoxFuture;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A shared page provider that additionally tracks pages written since the last `take_dirty()`.
///
/// This is used on compute side to collect page after-images for page-level redo.
#[derive(Clone)]
pub struct TxnPageProvider {
    pages: Arc<PageCache>,
    next_page_id: Arc<AtomicU64>,
    root: Arc<AtomicU64>,
    btree_meta_page_id: PageId,
    fetcher: Arc<dyn Fn(PageId) -> LocalBoxFuture<'static, Option<Page>>>,

    dirty: Arc<Mutex<BTreeMap<PageId, Page>>>,
}

impl TxnPageProvider {
    pub fn new(
        pages: Arc<PageCache>,
        next_page_id: Arc<AtomicU64>,
        btree_meta_page_id: PageId,
        fetcher: Arc<dyn Fn(PageId) -> LocalBoxFuture<'static, Option<Page>>>,
    ) -> Self {
        Self {
            pages,
            next_page_id,
            root: Arc::new(AtomicU64::new(0)),
            btree_meta_page_id,
            fetcher,
            dirty: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn set_next_page_id(&self, next: PageId) {
        self.next_page_id.store(next, Ordering::Release);
    }

    pub fn next_page_id(&self) -> PageId {
        self.next_page_id.load(Ordering::Acquire)
    }

    pub fn take_dirty(&self) -> Vec<(PageId, Page)> {
        let mut d = self.dirty.lock().unwrap();
        let out: Vec<(PageId, Page)> = d.iter().map(|(k, v)| (*k, v.clone())).collect();
        d.clear();
        out
    }

    fn record_dirty(&self, page_id: PageId, page: &Page) {
        self.dirty.lock().unwrap().insert(page_id, page.clone());
    }
}

impl AsyncPageProvider for TxnPageProvider {
    async fn read_page(&self, page_id: PageId) -> Option<Page> {
        // Prefer in-txn dirty version.
        if let Some(p) = self.dirty.lock().unwrap().get(&page_id).cloned() {
            return Some(p);
        }
        if let Some(p) = self.pages.get(page_id) {
            return Some(p);
        }
        // Demand paging on miss.
        let p = (self.fetcher)(page_id).await?;
        self.pages.insert(page_id, p.clone());
        Some(p)
    }

    async fn write_page(&self, page_id: PageId, page: Page) {
        self.pages.insert(page_id, page.clone());
        self.record_dirty(page_id, &page);
    }

    fn alloc_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    fn root_page_id(&self) -> PageId {
        self.root.load(Ordering::Relaxed)
    }

    fn set_root_page_id(&self, page_id: PageId) {
        self.root.store(page_id, Ordering::Relaxed);

        // Persist B-Tree meta page as a normal page after-image.
        let meta = crate::btree_meta::BtreeMeta {
            root_page_id: page_id,
        };
        let page = meta.encode();
        self.pages.insert(self.btree_meta_page_id, page.clone());
        self.record_dirty(self.btree_meta_page_id, &page);
    }

    fn page_latch_table(&self) -> Arc<PageLatchTable> {
        self.pages.latch_table()
    }
}
