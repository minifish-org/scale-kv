use crate::page_bptree::{AsyncPageProvider, PageCache, PageLatchTable};
use crate::{Page, PageId};
use futures::future::LocalBoxFuture;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

tokio::task_local! {
    pub(crate) static CURRENT_TXN_DIRTY: Rc<RefCell<BTreeMap<PageId, Page>>>;
}

/// Shared page provider for B+Tree pages.
#[derive(Clone)]
pub struct TxnPageProvider {
    pages: Arc<PageCache>,
    next_page_id: Arc<AtomicU64>,
    root: Arc<AtomicU64>,
    btree_meta_page_id: PageId,
    fetcher: Arc<dyn Fn(PageId) -> LocalBoxFuture<'static, Option<Page>>>,
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
        }
    }

    pub fn set_next_page_id(&self, next: PageId) {
        self.next_page_id.store(next, Ordering::Release);
    }

    pub fn next_page_id(&self) -> PageId {
        self.next_page_id.load(Ordering::Acquire)
    }
}

impl AsyncPageProvider for TxnPageProvider {
    async fn read_page(&self, page_id: PageId) -> Option<Page> {
        if let Some(p) = self.pages.get_arc(page_id) {
            return Some(p.as_ref().clone());
        }
        // Demand paging on miss.
        let p = (self.fetcher)(page_id).await?;
        let p = Arc::new(p);
        self.pages.insert_arc(page_id, Arc::clone(&p));
        Some(p.as_ref().clone())
    }

    async fn read_page_arc(&self, page_id: PageId) -> Option<Arc<Page>> {
        if let Some(p) = self.pages.get_arc(page_id) {
            return Some(p);
        }
        let p = Arc::new((self.fetcher)(page_id).await?);
        self.pages.insert_arc(page_id, Arc::clone(&p));
        Some(p)
    }

    async fn write_page(&self, page_id: PageId, page: Page) {
        self.pages.insert(page_id, page.clone());
        let _ = CURRENT_TXN_DIRTY.try_with(|dirty| {
            dirty.borrow_mut().insert(page_id, page);
        });
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
        let _ = CURRENT_TXN_DIRTY.try_with(|dirty| {
            dirty.borrow_mut().insert(self.btree_meta_page_id, page);
        });
    }

    fn page_latch_table(&self) -> Arc<PageLatchTable> {
        self.pages.latch_table()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_bptree::DEFAULT_PAGE_CACHE_SHARDS;
    use crate::{BTREE_META_PAGE_ID, PAGE_SIZE};
    use std::sync::atomic::AtomicUsize;

    fn make_provider(
        fetch_count: Arc<AtomicUsize>,
        fetch_value: u8,
    ) -> (TxnPageProvider, Arc<PageCache>) {
        let pages = Arc::new(PageCache::new(DEFAULT_PAGE_CACHE_SHARDS));
        let next_page_id = Arc::new(AtomicU64::new(100));
        let fetcher = Arc::new(move |_page_id: PageId| {
            let fetch_count = Arc::clone(&fetch_count);
            Box::pin(async move {
                fetch_count.fetch_add(1, Ordering::SeqCst);
                Some(vec![fetch_value; PAGE_SIZE].into())
            }) as LocalBoxFuture<'static, Option<Page>>
        });
        let provider = TxnPageProvider::new(
            Arc::clone(&pages),
            next_page_id,
            BTREE_META_PAGE_ID,
            fetcher,
        );
        (provider, pages)
    }

    #[tokio::test]
    async fn test_demand_paging_hits_fetcher_once_then_cache() {
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let (provider, _pages) = make_provider(Arc::clone(&fetch_count), 7);

        let p1 = provider.read_page(42).await.unwrap();
        let p2 = provider.read_page(42).await.unwrap();
        assert_eq!(p1[0], 7);
        assert_eq!(p2[0], 7);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_write_page_and_set_root_track_dirty_in_task_local() {
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let (provider, pages) = make_provider(fetch_count, 1);
        let dirty = Rc::new(RefCell::new(BTreeMap::<PageId, Page>::new()));

        CURRENT_TXN_DIRTY
            .scope(Rc::clone(&dirty), async {
                provider.write_page(5, vec![9; PAGE_SIZE].into()).await;
                provider.set_root_page_id(77);
            })
            .await;

        let dirty = dirty.borrow();
        assert!(dirty.contains_key(&5));
        assert!(dirty.contains_key(&BTREE_META_PAGE_ID));
        assert_eq!(provider.root_page_id(), 77);
        assert!(pages.contains(BTREE_META_PAGE_ID));
    }

    #[test]
    fn test_alloc_and_next_page_id_controls() {
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let (provider, _pages) = make_provider(fetch_count, 1);

        assert_eq!(provider.next_page_id(), 100);
        assert_eq!(provider.alloc_page_id(), 100);
        assert_eq!(provider.alloc_page_id(), 101);
        provider.set_next_page_id(1000);
        assert_eq!(provider.next_page_id(), 1000);
    }
}
