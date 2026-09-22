use crate::{Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, VALUE_SIZE, undo_pg::UndoPtr};
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use hex;

const PAGE_TYPE_INTERNAL: u8 = 1;
const PAGE_TYPE_LEAF: u8 = 2;
const ROOT_LATCH_PAGE_ID: PageId = 0;

const HIGH_KEY_SIZE: usize = KEY_SIZE;
const HEADER_SIZE: usize = 48; // padded header
// layout:
// 0: page_type u8
// 1: level u8
// 2..4: key_count u16
// 4..12: prev_leaf u64 (0 means none)
// 12..20: next_leaf u64 (0 means none)
// 20..28: left_child u64
// 28: high_key_present u8
// 29..(29+HIGH_KEY_SIZE): high_key bytes

const OFFSET_ENTRY_SIZE: usize = 2;
pub const LEAF_VALUE_SIZE: usize = VALUE_SIZE + 8 + 8 + 2 + 2 + 8 + 8;
const CHILD_ID_SIZE: usize = 8;
const DEFAULT_LATCH_SHARDS: usize = 256;
const DEFAULT_LATCH_STRIPES: usize = 32 * 1024;
const LATCH_STRIPE_PERMITS: u32 = 1024;
const MAX_TRAVERSAL_RETRIES: usize = 8;
const MAX_RIGHT_LINK_HOPS: usize = 256;
const MAX_SCAN_LEAF_HOPS: usize = 8192;

pub type PageReadLatchGuard = OwnedSemaphorePermit;
pub type PageWriteLatchGuard = OwnedSemaphorePermit;

#[derive(Debug)]
pub struct PageLatchTable {
    stripes: Vec<Arc<Semaphore>>,
}

impl PageLatchTable {
    pub fn new(shards: usize) -> Self {
        let count = shards.max(1).max(DEFAULT_LATCH_STRIPES);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(Arc::new(Semaphore::new(LATCH_STRIPE_PERMITS as usize)));
        }
        Self { stripes: out }
    }

    fn shard(&self, page_id: PageId) -> usize {
        (page_id as usize) % self.stripes.len()
    }

    fn latch_for(&self, page_id: PageId) -> Arc<Semaphore> {
        let idx = self.shard(page_id);
        Arc::clone(&self.stripes[idx])
    }

    pub async fn acquire_read(&self, page_id: PageId) -> PageReadLatchGuard {
        self.latch_for(page_id)
            .acquire_owned()
            .await
            .expect("page latch semaphore closed")
    }

    pub async fn acquire_write(&self, page_id: PageId) -> PageWriteLatchGuard {
        self.latch_for(page_id)
            .acquire_many_owned(LATCH_STRIPE_PERMITS)
            .await
            .expect("page latch semaphore closed")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeafValue {
    pub value: [u8; VALUE_SIZE],
    pub commit_lsn: u64,
    pub undo_ptr: Option<UndoPtr>,
    pub flags: u16,
    pub intent_txn_id: u64,
    pub intent_lsn: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeafValueRef {
    pub meta: LeafValueMeta,
    pub value: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeafValueMeta {
    pub commit_lsn: u64,
    pub undo_ptr: Option<UndoPtr>,
    pub flags: u16,
    pub intent_txn_id: u64,
    pub intent_lsn: u64,
}

/// Trait for page storage backend.
/// B+Tree uses this trait to read/write pages, allowing different backends:
/// - InMemoryPageProvider: HashMap-based, for testing
/// - SharedPageProvider: Wraps Arc<RwLock<HashMap>> for shared cache
#[allow(async_fn_in_trait)]
pub trait AsyncPageProvider {
    /// Read a page by ID. Returns None if page doesn't exist.
    async fn read_page(&self, page_id: PageId) -> Option<Page>;
    async fn read_page_arc(&self, page_id: PageId) -> Option<Arc<Page>> {
        self.read_page(page_id).await.map(Arc::new)
    }

    /// Write a page. Creates if not exists, updates if exists.
    async fn write_page(&self, page_id: PageId, page: Page);

    /// Allocate a new page ID.
    fn alloc_page_id(&self) -> PageId;

    /// Get the root page ID (for B+Tree metadata).
    fn root_page_id(&self) -> PageId;

    /// Set the root page ID.
    fn set_root_page_id(&self, page_id: PageId);

    fn page_latch_table(&self) -> Arc<PageLatchTable> {
        static FALLBACK: OnceLock<Arc<PageLatchTable>> = OnceLock::new();
        Arc::clone(FALLBACK.get_or_init(|| Arc::new(PageLatchTable::new(DEFAULT_LATCH_SHARDS))))
    }

    async fn acquire_read_latch(&self, page_id: PageId) -> PageReadLatchGuard {
        self.page_latch_table().acquire_read(page_id).await
    }

    async fn acquire_write_latch(&self, page_id: PageId) -> PageWriteLatchGuard {
        self.page_latch_table().acquire_write(page_id).await
    }
}

/// In-memory page provider using HashMap with RefCell for interior mutability.
/// Suitable for testing and single-node usage.
pub struct InMemoryPageProvider {
    pages: RefCell<HashMap<PageId, Page>>,
    next_page_id: AtomicU64,
    root: AtomicU64,
    latch_table: Arc<PageLatchTable>,
}

impl InMemoryPageProvider {
    pub fn new() -> Self {
        Self {
            pages: RefCell::new(HashMap::new()),
            next_page_id: AtomicU64::new(1),
            root: AtomicU64::new(0),
            latch_table: Arc::new(PageLatchTable::new(DEFAULT_LATCH_SHARDS)),
        }
    }

    pub fn page_count(&self) -> usize {
        self.pages.borrow().len()
    }
}

impl Default for InMemoryPageProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncPageProvider for InMemoryPageProvider {
    async fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.pages.borrow().get(&page_id).cloned()
    }

    async fn read_page_arc(&self, page_id: PageId) -> Option<Arc<Page>> {
        self.pages.borrow().get(&page_id).cloned().map(Arc::new)
    }

    async fn write_page(&self, page_id: PageId, page: Page) {
        self.pages.borrow_mut().insert(page_id, page);
    }

    fn alloc_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    fn root_page_id(&self) -> PageId {
        self.root.load(Ordering::Relaxed)
    }

    fn set_root_page_id(&self, page_id: PageId) {
        self.root.store(page_id, Ordering::Relaxed);
    }

    fn page_latch_table(&self) -> Arc<PageLatchTable> {
        Arc::clone(&self.latch_table)
    }
}

pub const DEFAULT_PAGE_CACHE_SHARDS: usize = 64;

pub struct PageCache {
    shards: Vec<parking_lot::Mutex<lru::LruCache<PageId, Arc<Page>>>>,
    latches: Arc<PageLatchTable>,
}

impl PageCache {
    /// Create an LRU page cache with a fixed total capacity (in pages).
    pub fn new(shards: usize) -> Self {
        Self::new_with_capacity(shards, 8192)
    }

    pub fn new_with_capacity(shards: usize, capacity_pages: usize) -> Self {
        use std::num::NonZeroUsize;

        let count = shards.max(1);
        let cap = capacity_pages.max(1);
        let per = cap.div_ceil(count);
        let per = NonZeroUsize::new(per.max(1)).unwrap();

        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(parking_lot::Mutex::new(lru::LruCache::new(per)));
        }
        Self {
            shards: out,
            latches: Arc::new(PageLatchTable::new(count)),
        }
    }

    fn shard(&self, page_id: PageId) -> usize {
        (page_id as usize) % self.shards.len()
    }

    pub fn get(&self, page_id: PageId) -> Option<Page> {
        self.get_arc(page_id).map(|p| p.as_ref().clone())
    }

    pub fn get_arc(&self, page_id: PageId) -> Option<Arc<Page>> {
        let idx = self.shard(page_id);
        self.shards[idx].lock().get(&page_id).cloned()
    }

    pub fn insert(&self, page_id: PageId, page: Page) {
        self.insert_arc(page_id, Arc::new(page));
    }

    pub fn insert_arc(&self, page_id: PageId, page: Arc<Page>) {
        let idx = self.shard(page_id);
        self.shards[idx].lock().put(page_id, page);
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        let idx = self.shard(page_id);
        self.shards[idx].lock().contains(&page_id)
    }

    pub fn remove(&self, page_id: PageId) {
        let idx = self.shard(page_id);
        let _ = self.shards[idx].lock().pop(&page_id);
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn latch_table(&self) -> Arc<PageLatchTable> {
        Arc::clone(&self.latches)
    }

    pub async fn acquire_read_latch(&self, page_id: PageId) -> PageReadLatchGuard {
        self.latches.acquire_read(page_id).await
    }

    pub async fn acquire_write_latch(&self, page_id: PageId) -> PageWriteLatchGuard {
        self.latches.acquire_write(page_id).await
    }
}

/// Shared page provider that wraps Arc<RwLock<HashMap>>.
/// Used by ComputeNode to share page_cache between B+Tree and data pages.
#[derive(Clone)]
pub struct SharedPageProvider {
    pages: Arc<PageCache>,
    next_page_id: Arc<AtomicU64>,
    root: Arc<AtomicU64>,
}

impl SharedPageProvider {
    pub fn new(pages: Arc<PageCache>, next_page_id: Arc<AtomicU64>) -> Self {
        Self {
            pages,
            next_page_id,
            root: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }
}

impl AsyncPageProvider for SharedPageProvider {
    async fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.pages.get(page_id)
    }

    async fn read_page_arc(&self, page_id: PageId) -> Option<Arc<Page>> {
        self.pages.get_arc(page_id)
    }

    async fn write_page(&self, page_id: PageId, page: Page) {
        self.pages.insert(page_id, page);
    }

    fn alloc_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    fn root_page_id(&self) -> PageId {
        self.root.load(Ordering::Relaxed)
    }

    fn set_root_page_id(&self, page_id: PageId) {
        self.root.store(page_id, Ordering::Relaxed);
    }

    fn page_latch_table(&self) -> Arc<PageLatchTable> {
        self.pages.latch_table()
    }
}

/// Page-based B+Tree that uses AsyncPageProvider for storage.
pub struct PageBPlusTree<P: AsyncPageProvider> {
    provider: P,
    len: AtomicU64,
}

impl PageBPlusTree<InMemoryPageProvider> {
    /// Create a new B+Tree with in-memory storage.
    pub fn new() -> Self {
        let provider = InMemoryPageProvider::new();
        let root_id = provider.alloc_page_id();
        provider
            .pages
            .borrow_mut()
            .insert(root_id, new_page(PAGE_TYPE_LEAF, 0));
        provider.set_root_page_id(root_id);
        Self {
            provider,
            len: AtomicU64::new(0),
        }
    }
}

impl Default for PageBPlusTree<InMemoryPageProvider> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: AsyncPageProvider> PageBPlusTree<P> {
    /// Create a new B+Tree with a custom page provider.
    /// The provider should already have an empty root page allocated.
    pub fn with_provider(provider: P) -> Self {
        Self {
            provider,
            len: AtomicU64::new(0),
        }
    }

    /// Create a new B+Tree, initializing the root page in the provider.
    pub async fn new_with_provider(provider: P) -> Self {
        let root_id = provider.alloc_page_id();
        provider
            .write_page(root_id, new_page(PAGE_TYPE_LEAF, 0))
            .await;
        provider.set_root_page_id(root_id);
        Self {
            provider,
            len: AtomicU64::new(0),
        }
    }

    /// Get a reference to the page provider.
    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// Get a mutable reference to the page provider.
    pub fn provider_mut(&mut self) -> &mut P {
        &mut self.provider
    }

    pub fn root_page_id(&self) -> PageId {
        self.provider.root_page_id()
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    pub async fn get(&self, key: &[u8]) -> Option<LeafValueRef> {
        for _ in 0..MAX_TRAVERSAL_RETRIES {
            let mut leaf_id = self.find_leaf_for_read(key).await?;
            for _ in 0..MAX_RIGHT_LINK_HOPS {
                let _leaf_guard = self.provider.acquire_read_latch(leaf_id).await;
                let page = self.provider.read_page_arc(leaf_id).await?;
                let header = page_header(&page);
                if header.page_type != PAGE_TYPE_LEAF {
                    break;
                }
                if let Some(hk) = header.high_key
                    && key > hk.as_slice()
                    && let Some(next) = header.next_leaf
                {
                    if next == leaf_id {
                        break;
                    }
                    leaf_id = next;
                    continue;
                }
                return find_in_leaf(&page, key);
            }
        }
        None
    }

    /// Locking protocol:
    /// - Writers use top-down split/merge-before-descend with write-latch coupling.
    /// - The parent is always write-latched while fixing an unsafe child.
    /// - We never upgrade from child to parent, avoiding upgrade deadlocks.
    ///
    /// Known limits:
    /// - Range/key gap locks are not implemented, so phantom protection is not provided.
    /// - Readers are lock-coupled for physical safety, but are not a serializable snapshot.
    pub async fn insert(&self, key: Vec<u8>, leaf_value: LeafValue) -> Result<()> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let encoded = encode_leaf_value(&leaf_value);
        let root_guard = self.provider.acquire_write_latch(ROOT_LATCH_PAGE_ID).await;
        self.ensure_root_not_full().await?;
        let mut current_id = self.provider.root_page_id();
        let mut current_guard = self.provider.acquire_write_latch(current_id).await;
        drop(root_guard);

        loop {
            let mut current_page = self
                .provider
                .read_page(current_id)
                .await
                .ok_or(Error::InvalidPageSize(current_id as usize, PAGE_SIZE))?
                .to_vec();
            let current_header = page_header(&current_page);

            if current_header.page_type == PAGE_TYPE_LEAF {
                let (found, pos) = find_key_pos(&current_page, &key).unwrap_or((false, 0));
                if found {
                    let offset = entry_offset_at(&current_page, pos)
                        .ok_or(Error::InvalidPageSize(pos, PAGE_SIZE))?;
                    let value_offset = offset + KEY_SIZE;
                    current_page[value_offset..value_offset + LEAF_VALUE_SIZE]
                        .copy_from_slice(&encoded);
                    self.provider
                        .write_page(current_id, Bytes::from(current_page))
                        .await;
                    drop(current_guard);
                    return Ok(());
                }

                let rebuilt =
                    rebuild_page_with_insert(&current_page, current_header, pos, &key, &encoded)?;
                self.provider.write_page(current_id, rebuilt).await;
                self.len.fetch_add(1, Ordering::AcqRel);
                drop(current_guard);
                return Ok(());
            }

            let child_pos = child_insert_pos_for_key(&current_page, current_header, &key);
            let child_id = child_id_at_pos(&current_page, current_header, child_pos);
            let child_guard = self.provider.acquire_write_latch(child_id).await;
            let child_page = self
                .provider
                .read_page(child_id)
                .await
                .ok_or(Error::InvalidPageSize(child_id as usize, PAGE_SIZE))?;
            let child_header = page_header(&child_page);

            if page_is_full_for_insert(child_header) {
                if !page_has_room_for_one_more(current_header) {
                    return Err(Error::InvalidPageSize(current_id as usize, PAGE_SIZE));
                }

                let (separator, right_id) = self
                    .split_child_and_update_parent(
                        current_id,
                        Bytes::from(current_page),
                        current_header,
                        child_id,
                        child_page,
                        child_header,
                        child_pos,
                    )
                    .await?;

                if key >= separator {
                    let right_guard = self.provider.acquire_write_latch(right_id).await;
                    drop(child_guard);
                    drop(current_guard);
                    current_id = right_id;
                    current_guard = right_guard;
                } else {
                    drop(current_guard);
                    current_id = child_id;
                    current_guard = child_guard;
                }
                continue;
            }

            drop(current_guard);
            current_id = child_id;
            current_guard = child_guard;
        }
    }

    pub async fn remove(&self, key: &[u8]) -> Result<()> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }

        // Top-down delete:
        // Keep parent write-latched while making the next child safe (borrow/merge),
        // then descend. This avoids lock upgrades and underflow cascades after descent.
        let root_guard = self.provider.acquire_write_latch(ROOT_LATCH_PAGE_ID).await;
        let mut current_id = self.provider.root_page_id();
        let mut current_guard = self.provider.acquire_write_latch(current_id).await;
        let mut parent_hint: Option<(PageId, usize)> = None;
        drop(root_guard);

        loop {
            let current_page = match self.provider.read_page(current_id).await {
                Some(p) => p,
                None => return Ok(()),
            };
            let current_header = page_header(&current_page);
            if current_header.page_type == PAGE_TYPE_LEAF {
                let (found, pos) = find_key_pos(&current_page, key).unwrap_or((false, 0));
                if found {
                    let rebuilt = rebuild_page_with_remove(&current_page, current_header, pos)?;
                    let new_first = if pos == 0 {
                        entry_key_at(&rebuilt, 0).map(|k| k.to_vec())
                    } else {
                        None
                    };
                    self.provider.write_page(current_id, rebuilt).await;
                    self.len
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                            Some(v.saturating_sub(1))
                        })
                        .ok();
                    drop(current_guard);
                    if let (Some((parent_id, hint_pos)), Some(new_first_key)) =
                        (parent_hint, new_first.as_ref())
                    {
                        self.repair_parent_separator_after_leaf_first_key_change(
                            parent_id,
                            hint_pos,
                            current_id,
                            new_first_key,
                        )
                        .await?;
                    }
                    self.repair_leaf_boundary_after_delete(current_id).await?;
                    self.refresh_leaf_high_keys().await?;
                    self.maybe_collapse_root_after_delete().await?;
                    return Ok(());
                }
                drop(current_guard);
                self.maybe_collapse_root_after_delete().await?;
                return Ok(());
            }

            let child_pos = child_insert_pos_for_key(&current_page, current_header, key);
            let descend_id = child_id_at_pos(&current_page, current_header, child_pos);
            let descend_guard = self.provider.acquire_write_latch(descend_id).await;
            let mut child_page = self
                .provider
                .read_page(descend_id)
                .await
                .ok_or(Error::InvalidPageSize(descend_id as usize, PAGE_SIZE))?
                .to_vec();
            let mut child_header = page_header(&child_page);
            let min_keys = min_keys_non_root(child_header.page_type);

            if child_header.key_count as usize <= min_keys {
                let mut parent_entries = collect_entries(&current_page);
                let child_type = child_header.page_type;

                let left_option = if child_pos > 0 {
                    let left_id = child_id_at_pos(&current_page, current_header, child_pos - 1);
                    let left_guard = self.provider.acquire_write_latch(left_id).await;
                    let left_page = self
                        .provider
                        .read_page(left_id)
                        .await
                        .ok_or(Error::InvalidPageSize(left_id as usize, PAGE_SIZE))?;
                    Some((left_id, left_guard, left_page))
                } else {
                    None
                };

                if let Some((left_id, left_guard, left_page)) = left_option {
                    let left_header = page_header(&left_page);
                    if left_header.key_count as usize > min_keys {
                        let mut left_entries = collect_entries(&left_page);
                        let mut child_entries = collect_entries(&child_page);
                        if child_type == PAGE_TYPE_LEAF {
                            let borrowed = left_entries
                                .pop()
                                .ok_or(Error::InvalidPageSize(left_id as usize, PAGE_SIZE))?;
                            child_entries.insert(0, borrowed);
                            let new_sep = child_entries[0].0.clone();
                            parent_entries[child_pos - 1].0 = new_sep.clone();

                            let mut left_header_new = leaf_page_header(&left_page);
                            left_header_new.high_key = key_to_high_key(&new_sep);
                            let mut child_header_new = leaf_page_header(&child_page);
                            child_header_new.prev_leaf = Some(left_id);
                            encode_entries(&mut child_page, &child_entries, child_header_new)?;
                            let mut left_rebuilt = left_page.to_vec();
                            encode_entries(&mut left_rebuilt, &left_entries, left_header_new)?;
                            self.provider
                                .write_page(left_id, Bytes::from(left_rebuilt))
                                .await;
                        } else {
                            let sep_idx = child_pos - 1;
                            let parent_sep = parent_entries[sep_idx].0.clone();
                            let (up_key, up_right_child) = left_entries
                                .pop()
                                .ok_or(Error::InvalidPageSize(left_id as usize, PAGE_SIZE))?;
                            let old_left = child_header.left_child;
                            child_header.left_child = decode_child_id(&up_right_child);
                            child_entries.insert(0, (parent_sep, encode_child_id(old_left)));
                            parent_entries[sep_idx].0 = up_key.clone();

                            let mut left_header_new = page_header(&left_page);
                            left_header_new.high_key = key_to_high_key(&up_key);
                            let mut left_rebuilt = left_page.to_vec();
                            encode_entries(&mut left_rebuilt, &left_entries, left_header_new)?;
                            encode_entries(&mut child_page, &child_entries, child_header)?;
                            self.provider
                                .write_page(left_id, Bytes::from(left_rebuilt))
                                .await;
                        }

                        let mut parent_rebuilt = current_page.to_vec();
                        encode_entries(&mut parent_rebuilt, &parent_entries, current_header)?;
                        self.provider
                            .write_page(current_id, Bytes::from(parent_rebuilt))
                            .await;
                        let parent_id = current_id;
                        drop(left_guard);
                        drop(current_guard);
                        parent_hint = Some((parent_id, child_pos));
                        current_id = descend_id;
                        current_guard = descend_guard;
                        continue;
                    }
                    drop(left_guard);
                }

                let right_option = if child_pos < current_header.key_count as usize {
                    let right_id = child_id_at_pos(&current_page, current_header, child_pos + 1);
                    let right_guard = self.provider.acquire_write_latch(right_id).await;
                    let right_page = self
                        .provider
                        .read_page(right_id)
                        .await
                        .ok_or(Error::InvalidPageSize(right_id as usize, PAGE_SIZE))?;
                    Some((right_id, right_guard, right_page))
                } else {
                    None
                };

                if let Some((right_id, right_guard, right_page)) = right_option {
                    let right_header = page_header(&right_page);
                    if right_header.key_count as usize > min_keys {
                        let mut right_entries = collect_entries(&right_page);
                        let mut child_entries = collect_entries(&child_page);
                        if child_type == PAGE_TYPE_LEAF {
                            let borrowed = right_entries.remove(0);
                            child_entries.push(borrowed);
                            let new_sep = right_entries[0].0.clone();
                            parent_entries[child_pos].0 = new_sep.clone();

                            let mut child_header_new = leaf_page_header(&child_page);
                            child_header_new.high_key = key_to_high_key(&new_sep);
                            let mut right_header_new = leaf_page_header(&right_page);
                            right_header_new.prev_leaf = Some(descend_id);
                            encode_entries(&mut child_page, &child_entries, child_header_new)?;
                            let mut right_rebuilt = right_page.to_vec();
                            encode_entries(&mut right_rebuilt, &right_entries, right_header_new)?;
                            self.provider
                                .write_page(right_id, Bytes::from(right_rebuilt))
                                .await;
                        } else {
                            let sep_idx = child_pos;
                            let parent_sep = parent_entries[sep_idx].0.clone();
                            let promoted = right_entries.remove(0);
                            let mut right_header_new = right_header;
                            let old_left_of_right = right_header_new.left_child;
                            right_header_new.left_child = decode_child_id(&promoted.1);
                            child_entries.push((parent_sep, encode_child_id(old_left_of_right)));
                            parent_entries[sep_idx].0 = promoted.0.clone();

                            let mut child_header_new = child_header;
                            child_header_new.high_key = key_to_high_key(&promoted.0);
                            let mut right_rebuilt = right_page.to_vec();
                            encode_entries(&mut right_rebuilt, &right_entries, right_header_new)?;
                            encode_entries(&mut child_page, &child_entries, child_header_new)?;
                            self.provider
                                .write_page(right_id, Bytes::from(right_rebuilt))
                                .await;
                        }

                        let mut parent_rebuilt = current_page.to_vec();
                        encode_entries(&mut parent_rebuilt, &parent_entries, current_header)?;
                        self.provider
                            .write_page(current_id, Bytes::from(parent_rebuilt))
                            .await;
                        let parent_id = current_id;
                        drop(right_guard);
                        drop(current_guard);
                        parent_hint = Some((parent_id, child_pos));
                        current_id = descend_id;
                        current_guard = descend_guard;
                        continue;
                    }

                    if child_type == PAGE_TYPE_LEAF {
                        let mut child_entries = collect_entries(&child_page);
                        let right_entries = collect_entries(&right_page);
                        child_entries.extend(right_entries);
                        let mut child_header_new = leaf_page_header(&child_page);
                        child_header_new.next_leaf = right_header.next_leaf;
                        child_header_new.high_key = right_header.high_key;
                        encode_entries(&mut child_page, &child_entries, child_header_new)?;
                    } else {
                        let mut child_entries = collect_entries(&child_page);
                        let right_entries = collect_entries(&right_page);
                        let sep = parent_entries[child_pos].0.clone();
                        child_entries.push((sep, encode_child_id(right_header.left_child)));
                        child_entries.extend(right_entries);
                        let mut child_header_new = child_header;
                        child_header_new.high_key = right_header.high_key;
                        encode_entries(&mut child_page, &child_entries, child_header_new)?;
                    }

                    parent_entries.remove(child_pos);
                    let mut parent_rebuilt = current_page.to_vec();
                    encode_entries(&mut parent_rebuilt, &parent_entries, current_header)?;
                    self.provider
                        .write_page(current_id, Bytes::from(parent_rebuilt))
                        .await;
                    self.provider
                        .write_page(descend_id, Bytes::from(child_page.to_vec()))
                        .await;
                    let parent_id = current_id;
                    drop(right_guard);
                    drop(current_guard);
                    parent_hint = Some((parent_id, child_pos));
                    current_id = descend_id;
                    current_guard = descend_guard;
                    continue;
                }

                if child_pos > 0 {
                    let left_id = child_id_at_pos(&current_page, current_header, child_pos - 1);
                    let left_guard = self.provider.acquire_write_latch(left_id).await;
                    let mut left_page = self
                        .provider
                        .read_page(left_id)
                        .await
                        .ok_or(Error::InvalidPageSize(left_id as usize, PAGE_SIZE))?
                        .to_vec();
                    let mut left_entries = collect_entries(&left_page);
                    if child_type == PAGE_TYPE_LEAF {
                        let child_entries = collect_entries(&child_page);
                        left_entries.extend(child_entries);
                        let mut left_header = leaf_page_header(&left_page);
                        left_header.next_leaf = child_header.next_leaf;
                        left_header.high_key = child_header.high_key;
                        encode_entries(&mut left_page, &left_entries, left_header)?;
                    } else {
                        let child_entries = collect_entries(&child_page);
                        let sep = parent_entries[child_pos - 1].0.clone();
                        left_entries.push((sep, encode_child_id(child_header.left_child)));
                        left_entries.extend(child_entries);
                        let mut left_header = page_header(&left_page);
                        left_header.high_key = child_header.high_key;
                        encode_entries(&mut left_page, &left_entries, left_header)?;
                    }

                    parent_entries.remove(child_pos - 1);
                    let mut parent_rebuilt = current_page.to_vec();
                    encode_entries(&mut parent_rebuilt, &parent_entries, current_header)?;
                    self.provider
                        .write_page(current_id, Bytes::from(parent_rebuilt))
                        .await;
                    self.provider
                        .write_page(left_id, Bytes::from(left_page))
                        .await;
                    let parent_id = current_id;
                    drop(descend_guard);
                    drop(current_guard);
                    parent_hint = Some((parent_id, child_pos - 1));
                    current_id = left_id;
                    current_guard = left_guard;
                    continue;
                }
            }

            if child_header.page_type == PAGE_TYPE_LEAF {
                let (found, pos) = find_key_pos(&child_page, key).unwrap_or((false, 0));
                if found {
                    let rebuilt = rebuild_page_with_remove(&child_page, child_header, pos)?;
                    self.provider.write_page(descend_id, rebuilt.clone()).await;
                    if child_pos > 0
                        && pos == 0
                        && let Some(new_first) = entry_key_at(&rebuilt, 0)
                    {
                        let mut parent_entries = collect_entries(&current_page);
                        parent_entries[child_pos - 1].0 = new_first.to_vec();
                        let mut parent_rebuilt = current_page.to_vec();
                        encode_entries(&mut parent_rebuilt, &parent_entries, current_header)?;
                        self.provider
                            .write_page(current_id, Bytes::from(parent_rebuilt))
                            .await;

                        let left_id = child_id_at_pos(&current_page, current_header, child_pos - 1);
                        let _left_guard = self.provider.acquire_write_latch(left_id).await;
                        if let Some(mut left_page) =
                            self.provider.read_page(left_id).await.map(|p| p.to_vec())
                        {
                            let mut left_header = leaf_page_header(&left_page);
                            left_header.high_key = key_to_high_key(new_first);
                            write_header(&mut left_page, left_header);
                            self.provider
                                .write_page(left_id, Bytes::from(left_page))
                                .await;
                        }
                    }
                    self.len
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                            Some(v.saturating_sub(1))
                        })
                        .ok();
                }
                drop(descend_guard);
                drop(current_guard);
                if found {
                    self.repair_leaf_boundary_after_delete(descend_id).await?;
                    self.refresh_leaf_high_keys().await?;
                }
                self.maybe_collapse_root_after_delete().await?;
                return Ok(());
            }

            let parent_id = current_id;
            drop(current_guard);
            parent_hint = Some((parent_id, child_pos));
            current_id = descend_id;
            current_guard = descend_guard;
        }
    }

    pub async fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, LeafValueRef)> {
        let mut out = Vec::new();
        self.range_visit(start, end, |k, leaf_value| {
            out.push((k.to_vec(), leaf_value));
            true
        })
        .await;
        out
    }

    pub async fn range_meta(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, LeafValueMeta)> {
        let mut out = Vec::new();
        self.range_visit_meta(start, end, |k, leaf_value| {
            out.push((k.to_vec(), leaf_value));
            true
        })
        .await;
        out
    }

    pub async fn debug_leaf_keys(&self, key: &[u8], limit: usize) -> Vec<Vec<u8>> {
        let Some(leaf_id) = self.find_leaf_for_read(key).await else {
            return Vec::new();
        };
        let page = match self.provider.read_page(leaf_id).await {
            Some(p) => p,
            None => return Vec::new(),
        };
        let header = page_header(&page);
        if header.page_type != PAGE_TYPE_LEAF {
            return Vec::new();
        }
        let n = (header.key_count as usize).min(limit);
        let mut out = Vec::with_capacity(n);
        for idx in 0..n {
            if let Some(k) = entry_key_at(&page, idx) {
                out.push(k.to_vec());
            }
        }
        out
    }

    pub async fn debug_leaf_entries(&self, key: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Some(leaf_id) = self.find_leaf_for_read(key).await else {
            return Vec::new();
        };
        let page = match self.provider.read_page(leaf_id).await {
            Some(p) => p,
            None => return Vec::new(),
        };
        let header = page_header(&page);
        if header.page_type != PAGE_TYPE_LEAF {
            return Vec::new();
        }
        let n = (header.key_count as usize).min(limit);
        let mut out = Vec::with_capacity(n);
        for idx in 0..n {
            let Some(k) = entry_key_at(&page, idx) else {
                break;
            };
            let Some(v) = entry_value_at(&page, idx) else {
                break;
            };
            out.push((k.to_vec(), v.to_vec()));
        }
        out
    }

    pub async fn range_visit(
        &self,
        start: &[u8],
        end: &[u8],
        mut f: impl FnMut(&[u8], LeafValueRef) -> bool,
    ) {
        for _ in 0..MAX_TRAVERSAL_RETRIES {
            let Some(mut leaf_id) = self.find_leaf_for_read(start).await else {
                return;
            };
            let mut first = true;
            let mut visited = HashSet::new();
            for _ in 0..MAX_SCAN_LEAF_HOPS {
                if !visited.insert(leaf_id) {
                    break;
                }
                let _leaf_guard = self.provider.acquire_read_latch(leaf_id).await;
                let page = match self.provider.read_page_arc(leaf_id).await {
                    Some(page) => page,
                    None => break,
                };
                let header = leaf_page_header(&page);
                if header.page_type != PAGE_TYPE_LEAF {
                    break;
                }
                if let Some(hk) = header.high_key
                    && start > hk.as_slice()
                    && let Some(next) = header.next_leaf
                {
                    if next == leaf_id {
                        break;
                    }
                    leaf_id = next;
                    continue;
                }
                let mut idx = if first {
                    lower_bound_in_leaf(&page, start)
                } else {
                    0
                };
                first = false;
                let key_count = header.key_count as usize;
                while idx < key_count {
                    let Some(key) = entry_key_at(&page, idx) else {
                        break;
                    };
                    if key > end {
                        return;
                    }
                    let Some((value_offset, _)) = entry_value_bounds(&page, idx) else {
                        break;
                    };
                    let Some(row) = decode_leaf_value_ref(&page, value_offset) else {
                        break;
                    };
                    if !f(key, row) {
                        return;
                    }
                    idx += 1;
                }

                let Some(next) = header.next_leaf else {
                    return;
                };
                if next == leaf_id {
                    break;
                }
                leaf_id = next;
            }
        }
    }

    pub async fn range_visit_meta(
        &self,
        start: &[u8],
        end: &[u8],
        mut f: impl FnMut(&[u8], LeafValueMeta) -> bool,
    ) {
        for _ in 0..MAX_TRAVERSAL_RETRIES {
            let Some(mut leaf_id) = self.find_leaf_for_read(start).await else {
                return;
            };
            let mut first = true;
            let mut visited = HashSet::new();
            for _ in 0..MAX_SCAN_LEAF_HOPS {
                if !visited.insert(leaf_id) {
                    break;
                }
                let _leaf_guard = self.provider.acquire_read_latch(leaf_id).await;
                let page = match self.provider.read_page_arc(leaf_id).await {
                    Some(page) => page,
                    None => break,
                };
                let header = leaf_page_header(&page);
                if header.page_type != PAGE_TYPE_LEAF {
                    break;
                }
                if let Some(hk) = header.high_key
                    && start > hk.as_slice()
                    && let Some(next) = header.next_leaf
                {
                    if next == leaf_id {
                        break;
                    }
                    leaf_id = next;
                    continue;
                }
                let mut idx = if first {
                    lower_bound_in_leaf(&page, start)
                } else {
                    0
                };
                first = false;
                let key_count = header.key_count as usize;
                while idx < key_count {
                    let Some(key) = entry_key_at(&page, idx) else {
                        break;
                    };
                    if key > end {
                        return;
                    }
                    let Some(value) = entry_value_at(&page, idx) else {
                        break;
                    };
                    if !f(key, decode_leaf_value_meta(value)) {
                        return;
                    }
                    idx += 1;
                }

                let Some(next) = header.next_leaf else {
                    return;
                };
                if next == leaf_id {
                    break;
                }
                leaf_id = next;
            }
        }
    }

    async fn find_leaf_for_read(&self, key: &[u8]) -> Option<PageId> {
        for _ in 0..MAX_TRAVERSAL_RETRIES {
            let mut current = self.provider.root_page_id();
            let mut current_guard = self.provider.acquire_read_latch(current).await;
            for _ in 0..MAX_SCAN_LEAF_HOPS {
                let page = match self.provider.read_page_arc(current).await {
                    Some(p) => p,
                    None => break,
                };
                let header = page_header(&page);
                if header.page_type == PAGE_TYPE_LEAF {
                    drop(current_guard);
                    return Some(current);
                }
                // Read-coupling: parent read latch is held while acquiring child read latch,
                // so child pointers cannot be concurrently torn by structural writes.
                let child =
                    child_id_at_pos(&page, header, child_insert_pos_for_key(&page, header, key));
                let child_guard = self.provider.acquire_read_latch(child).await;
                drop(current_guard);
                current = child;
                current_guard = child_guard;
            }
            drop(current_guard);
        }
        None
    }

    async fn maybe_collapse_root_after_delete(&self) -> Result<()> {
        let _root_guard = self.provider.acquire_write_latch(ROOT_LATCH_PAGE_ID).await;
        loop {
            let root_id = self.provider.root_page_id();
            let _root_page_guard = self.provider.acquire_write_latch(root_id).await;
            let root = match self.provider.read_page(root_id).await {
                Some(p) => p,
                None => return Ok(()),
            };
            let header = page_header(&root);
            if header.page_type == PAGE_TYPE_INTERNAL
                && header.key_count == 0
                && header.left_child != 0
            {
                self.provider.set_root_page_id(header.left_child);
                continue;
            }
            return Ok(());
        }
    }

    async fn repair_parent_separator_after_leaf_first_key_change(
        &self,
        parent_id: PageId,
        hint_pos: usize,
        child_id: PageId,
        new_first_key: &[u8],
    ) -> Result<()> {
        let _parent_guard = self.provider.acquire_write_latch(parent_id).await;
        let mut parent_page = match self.provider.read_page(parent_id).await {
            Some(p) => p.to_vec(),
            None => return Ok(()),
        };
        let parent_header = page_header(&parent_page);
        if parent_header.page_type != PAGE_TYPE_INTERNAL || parent_header.key_count == 0 {
            return Ok(());
        }

        let mut pos = None;
        if hint_pos <= parent_header.key_count as usize
            && child_id_at_pos(&parent_page, parent_header, hint_pos) == child_id
        {
            pos = Some(hint_pos);
        } else {
            for idx in 0..=parent_header.key_count as usize {
                if child_id_at_pos(&parent_page, parent_header, idx) == child_id {
                    pos = Some(idx);
                    break;
                }
            }
        }
        let Some(child_pos) = pos else {
            return Ok(());
        };
        if child_pos == 0 {
            return Ok(());
        }

        let mut parent_entries = collect_entries(&parent_page);
        parent_entries[child_pos - 1].0 = new_first_key.to_vec();
        let left_id = child_id_at_pos(&parent_page, parent_header, child_pos - 1);
        encode_entries(&mut parent_page, &parent_entries, parent_header)?;
        self.provider
            .write_page(parent_id, Bytes::from(parent_page))
            .await;
        let _left_guard = self.provider.acquire_write_latch(left_id).await;
        if let Some(mut left_page) = self.provider.read_page(left_id).await.map(|p| p.to_vec()) {
            let mut left_header = leaf_page_header(&left_page);
            left_header.high_key = key_to_high_key(new_first_key);
            write_header(&mut left_page, left_header);
            self.provider
                .write_page(left_id, Bytes::from(left_page))
                .await;
        }
        Ok(())
    }

    async fn repair_leaf_boundary_after_delete(&self, leaf_id: PageId) -> Result<()> {
        let _leaf_guard = self.provider.acquire_write_latch(leaf_id).await;
        let mut leaf_page = match self.provider.read_page(leaf_id).await {
            Some(p) => p.to_vec(),
            None => return Ok(()),
        };
        let leaf_header = leaf_page_header(&leaf_page);
        if leaf_header.page_type != PAGE_TYPE_LEAF {
            return Ok(());
        }
        let leaf_first = entry_key_at(&leaf_page, 0).map(|k| k.to_vec());
        let next_first = if let Some(next_id) = leaf_header.next_leaf {
            self.provider
                .read_page(next_id)
                .await
                .and_then(|next_page| entry_key_at(&next_page, 0).map(|k| k.to_vec()))
        } else {
            None
        };
        let mut leaf_header_new = leaf_header;
        leaf_header_new.high_key = next_first.as_deref().and_then(key_to_high_key);
        write_header(&mut leaf_page, leaf_header_new);
        self.provider
            .write_page(leaf_id, Bytes::from(leaf_page))
            .await;
        drop(_leaf_guard);

        if let (Some(prev_id), Some(first_key)) = (leaf_header.prev_leaf, leaf_first.as_deref()) {
            let _prev_guard = self.provider.acquire_write_latch(prev_id).await;
            if let Some(mut prev_page) = self.provider.read_page(prev_id).await.map(|p| p.to_vec())
            {
                let mut prev_header = leaf_page_header(&prev_page);
                if prev_header.page_type == PAGE_TYPE_LEAF && prev_header.next_leaf == Some(leaf_id)
                {
                    prev_header.high_key = key_to_high_key(first_key);
                    write_header(&mut prev_page, prev_header);
                    self.provider
                        .write_page(prev_id, Bytes::from(prev_page))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn refresh_leaf_high_keys(&self) -> Result<()> {
        let mut current = self.provider.root_page_id();
        for _ in 0..MAX_SCAN_LEAF_HOPS {
            let _guard = self.provider.acquire_read_latch(current).await;
            let page = match self.provider.read_page(current).await {
                Some(p) => p,
                None => return Ok(()),
            };
            let header = page_header(&page);
            if header.page_type == PAGE_TYPE_LEAF {
                break;
            }
            current = header.left_child;
        }

        for _ in 0..MAX_SCAN_LEAF_HOPS {
            let _leaf_guard = self.provider.acquire_write_latch(current).await;
            let mut leaf_page = match self.provider.read_page(current).await {
                Some(p) => p.to_vec(),
                None => return Ok(()),
            };
            let leaf_header = leaf_page_header(&leaf_page);
            if leaf_header.page_type != PAGE_TYPE_LEAF {
                return Ok(());
            }

            let next = leaf_header.next_leaf;
            let new_hk = if let Some(next_id) = next {
                self.provider
                    .read_page(next_id)
                    .await
                    .and_then(|next_page| entry_key_at(&next_page, 0).map(|k| k.to_vec()))
                    .as_deref()
                    .and_then(key_to_high_key)
            } else {
                None
            };

            if leaf_header.high_key != new_hk {
                let mut updated = leaf_header;
                updated.high_key = new_hk;
                write_header(&mut leaf_page, updated);
                self.provider
                    .write_page(current, Bytes::from(leaf_page))
                    .await;
            }

            let Some(next_id) = next else {
                return Ok(());
            };
            if next_id == current {
                return Ok(());
            }
            current = next_id;
        }
        Ok(())
    }

    async fn ensure_root_not_full(&self) -> Result<()> {
        let root_id = self.provider.root_page_id();
        let _root_guard = self.provider.acquire_write_latch(root_id).await;
        let root = self
            .provider
            .read_page(root_id)
            .await
            .ok_or(Error::InvalidPageSize(root_id as usize, PAGE_SIZE))?;
        let root_header = page_header(&root);
        if !page_is_full_for_insert(root_header) {
            return Ok(());
        }

        let entries = collect_entries(&root);
        let right_id = self.provider.alloc_page_id();
        let (separator, left_page, right_page) = if root_header.page_type == PAGE_TYPE_LEAF {
            let (separator, (left, right)) = split_leaf(&root, &entries)?;
            (separator, left, right)
        } else {
            let (separator, (left, right)) = split_internal(&root, &entries)?;
            (separator, left, right)
        };
        let mut left_page = left_page.to_vec();
        let mut right_page = right_page.to_vec();

        if root_header.page_type == PAGE_TYPE_LEAF {
            let old_header = leaf_page_header(&root);
            let mut left_header = leaf_page_header(&left_page);
            left_header.next_leaf = Some(right_id);
            left_header.prev_leaf = old_header.prev_leaf;
            write_header(&mut left_page, left_header);

            let mut right_header = leaf_page_header(&right_page);
            right_header.prev_leaf = Some(root_id);
            right_header.next_leaf = old_header.next_leaf;
            write_header(&mut right_page, right_header);
        }

        self.provider
            .write_page(root_id, Bytes::from(left_page))
            .await;
        self.provider
            .write_page(right_id, Bytes::from(right_page))
            .await;

        let mut new_root =
            new_page(PAGE_TYPE_INTERNAL, root_header.level.saturating_add(1)).to_vec();
        let mut new_root_header = page_header(&new_root);
        new_root_header.left_child = root_id;
        let entries = vec![(separator, encode_child_id(right_id))];
        encode_entries(&mut new_root, &entries, new_root_header)?;
        let new_root_id = self.provider.alloc_page_id();
        self.provider
            .write_page(new_root_id, Bytes::from(new_root))
            .await;
        self.provider.set_root_page_id(new_root_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn split_child_and_update_parent(
        &self,
        parent_id: PageId,
        parent_page: Page,
        parent_header: PageHeader,
        child_id: PageId,
        child_page: Page,
        child_header: PageHeader,
        child_pos: usize,
    ) -> Result<(Vec<u8>, PageId)> {
        let entries = collect_entries(&child_page);
        let right_id = self.provider.alloc_page_id();
        let (separator, left_page, right_page) = if child_header.page_type == PAGE_TYPE_LEAF {
            let (separator, (left, right)) = split_leaf(&child_page, &entries)?;
            (separator, left, right)
        } else {
            let (separator, (left, right)) = split_internal(&child_page, &entries)?;
            (separator, left, right)
        };
        let mut left_page = left_page.to_vec();
        let mut right_page = right_page.to_vec();

        if child_header.page_type == PAGE_TYPE_LEAF {
            let old_header = leaf_page_header(&child_page);
            let mut left_header = leaf_page_header(&left_page);
            left_header.next_leaf = Some(right_id);
            left_header.prev_leaf = old_header.prev_leaf;
            write_header(&mut left_page, left_header);

            let mut right_header = leaf_page_header(&right_page);
            right_header.prev_leaf = Some(child_id);
            right_header.next_leaf = old_header.next_leaf;
            write_header(&mut right_page, right_header);
        }

        self.provider
            .write_page(child_id, Bytes::from(left_page))
            .await;
        self.provider
            .write_page(right_id, Bytes::from(right_page))
            .await;

        let mut parent_entries = collect_entries(&parent_page);
        parent_entries.insert(child_pos, (separator.clone(), encode_child_id(right_id)));
        let mut parent_page_buf = parent_page.to_vec();
        encode_entries(&mut parent_page_buf, &parent_entries, parent_header)?;
        self.provider
            .write_page(parent_id, Bytes::from(parent_page_buf))
            .await;

        Ok((separator, right_id))
    }

    #[cfg(test)]
    pub async fn validate(&self) -> std::result::Result<(), String> {
        type ValidateFrame = (PageId, Option<Vec<u8>>, Option<Vec<u8>>);
        let root_id = self.provider.root_page_id();
        let mut stack: Vec<ValidateFrame> = vec![(root_id, None, None)];
        let mut seen = HashSet::new();
        let mut leaves = HashSet::new();

        while let Some((page_id, lower, upper)) = stack.pop() {
            if !seen.insert(page_id) {
                return Err(format!("cycle detected at page_id={page_id}"));
            }
            let _guard = self.provider.acquire_read_latch(page_id).await;
            let page = self
                .provider
                .read_page(page_id)
                .await
                .ok_or_else(|| format!("missing page_id={page_id}"))?;
            let header = page_header(&page);
            let entries = collect_entries(&page);

            for i in 1..entries.len() {
                if entries[i - 1].0 >= entries[i].0 {
                    return Err(format!("non-ascending keys in page_id={page_id}"));
                }
            }
            if let Some(lo) = &lower
                && let Some(first) = entries.first()
                && first.0.as_slice() < lo.as_slice()
            {
                return Err(format!("page_id={page_id} violates lower bound"));
            }
            if let Some(hi) = &upper
                && let Some(last) = entries.last()
                && last.0.as_slice() >= hi.as_slice()
            {
                return Err(format!("page_id={page_id} violates upper bound"));
            }

            if header.page_type == PAGE_TYPE_LEAF {
                leaves.insert(page_id);
                if let Some(hk) = header.high_key
                    && let Some(last) = entries.last()
                    && last.0.as_slice() >= hk.as_slice()
                {
                    return Err(format!("leaf page_id={page_id} key >= high_key"));
                }
                continue;
            }

            if header.page_type != PAGE_TYPE_INTERNAL {
                return Err(format!(
                    "unknown page type {} at page_id={page_id}",
                    header.page_type
                ));
            }

            if header.left_child == 0 && !entries.is_empty() {
                return Err(format!("internal page_id={page_id} has zero left child"));
            }

            let mut child_ids = Vec::with_capacity(entries.len() + 1);
            child_ids.push(header.left_child);
            child_ids.extend(entries.iter().map(|(_, v)| decode_child_id(v)));
            for i in (0..child_ids.len()).rev() {
                let child = child_ids[i];
                let child_lower = if i == 0 {
                    lower.clone()
                } else {
                    Some(entries[i - 1].0.clone())
                };
                let child_upper = if i == entries.len() {
                    upper.clone()
                } else {
                    Some(entries[i].0.clone())
                };
                stack.push((child, child_lower, child_upper));
            }
        }

        let mut leftmost = root_id;
        loop {
            let _guard = self.provider.acquire_read_latch(leftmost).await;
            let page = self
                .provider
                .read_page(leftmost)
                .await
                .ok_or_else(|| format!("missing page_id={leftmost}"))?;
            let header = page_header(&page);
            if header.page_type == PAGE_TYPE_LEAF {
                break;
            }
            leftmost = header.left_child;
        }

        let mut chain_seen = HashSet::new();
        let mut current = Some(leftmost);
        let mut prev_last: Option<Vec<u8>> = None;
        let mut hops = 0usize;
        while let Some(pid) = current {
            hops += 1;
            if hops > leaves.len().saturating_add(1) {
                return Err("leaf chain exceeded expected length".to_string());
            }
            if !chain_seen.insert(pid) {
                return Err(format!("leaf chain cycle at page_id={pid}"));
            }

            let _guard = self.provider.acquire_read_latch(pid).await;
            let page = self
                .provider
                .read_page(pid)
                .await
                .ok_or_else(|| format!("missing page_id={pid}"))?;
            let header = leaf_page_header(&page);
            let entries = collect_entries(&page);
            if let Some(prev) = &prev_last
                && let Some(first) = entries.first()
                && first.0.as_slice() < prev.as_slice()
            {
                return Err(format!("leaf chain out of order at page_id={pid}"));
            }
            if let Some(last) = entries.last() {
                prev_last = Some(last.0.clone());
            }

            match (header.high_key, header.next_leaf) {
                (Some(hk), Some(next)) => {
                    let _next_guard = self.provider.acquire_read_latch(next).await;
                    let next_page = self
                        .provider
                        .read_page(next)
                        .await
                        .ok_or_else(|| format!("missing next leaf page_id={next}"))?;
                    let next_entries = collect_entries(&next_page);
                    if let Some(next_first) = next_entries.first()
                        && hk.as_slice() != next_first.0.as_slice()
                    {
                        return Err(format!(
                            "high_key mismatch at page_id={pid}: hk={} next_first={}",
                            hex::encode(hk),
                            hex::encode(&next_first.0)
                        ));
                    }
                    current = Some(next);
                }
                (None, Some(_)) => return Err(format!("missing high_key at page_id={pid}")),
                (_, None) => current = None,
            }
        }

        if chain_seen != leaves {
            return Err("leaf chain does not match discovered leaves".to_string());
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
struct PageHeader {
    page_type: u8,
    level: u8,
    key_count: u16,
    prev_leaf: Option<PageId>,
    next_leaf: Option<PageId>,
    left_child: PageId,
    high_key: Option<[u8; HIGH_KEY_SIZE]>,
}

fn page_header(page: &[u8]) -> PageHeader {
    let page_type = page[0];
    let level = page[1];
    let key_count = u16::from_le_bytes([page[2], page[3]]);
    let prev_leaf = u64::from_le_bytes(page[4..12].try_into().unwrap());
    let next_leaf = u64::from_le_bytes(page[12..20].try_into().unwrap());
    let left_child = u64::from_le_bytes(page[20..28].try_into().unwrap());
    let high_present = page[28] != 0;
    let high_key = if high_present {
        let mut hk = [0u8; HIGH_KEY_SIZE];
        hk.copy_from_slice(&page[29..29 + HIGH_KEY_SIZE]);
        Some(hk)
    } else {
        None
    };
    PageHeader {
        page_type,
        level,
        key_count,
        prev_leaf: if prev_leaf == 0 {
            None
        } else {
            Some(prev_leaf)
        },
        next_leaf: if next_leaf == 0 {
            None
        } else {
            Some(next_leaf)
        },
        left_child,
        high_key,
    }
}

fn leaf_page_header(page: &[u8]) -> PageHeader {
    page_header(page)
}

fn write_header(page: &mut [u8], header: PageHeader) {
    page[0] = header.page_type;
    page[1] = header.level;
    page[2..4].copy_from_slice(&header.key_count.to_le_bytes());

    let prev_leaf = header.prev_leaf.unwrap_or(0);
    page[4..12].copy_from_slice(&prev_leaf.to_le_bytes());

    let next_leaf = header.next_leaf.unwrap_or(0);
    page[12..20].copy_from_slice(&next_leaf.to_le_bytes());

    page[20..28].copy_from_slice(&header.left_child.to_le_bytes());

    match header.high_key {
        Some(hk) => {
            page[28] = 1;
            page[29..29 + HIGH_KEY_SIZE].copy_from_slice(&hk);
        }
        None => {
            page[28] = 0;
            page[29..29 + HIGH_KEY_SIZE].fill(0);
        }
    }

    // padding
    if HEADER_SIZE > 29 + HIGH_KEY_SIZE {
        page[29 + HIGH_KEY_SIZE..HEADER_SIZE].fill(0);
    }
}

pub fn new_page(page_type: u8, level: u8) -> Page {
    let mut page = vec![0u8; PAGE_SIZE];
    let header = PageHeader {
        page_type,
        level,
        key_count: 0,
        prev_leaf: None,
        next_leaf: None,
        left_child: 0,
        high_key: None,
    };
    write_header(&mut page, header);
    Bytes::from(page)
}

fn entry_value_size(page_type: u8) -> usize {
    match page_type {
        PAGE_TYPE_LEAF => LEAF_VALUE_SIZE,
        PAGE_TYPE_INTERNAL => CHILD_ID_SIZE,
        _ => 0,
    }
}

fn entry_size(page_type: u8) -> usize {
    KEY_SIZE + entry_value_size(page_type)
}

fn data_start(key_count: u16) -> usize {
    HEADER_SIZE + key_count as usize * OFFSET_ENTRY_SIZE
}

fn read_u16(page: &[u8], offset: usize) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&page[offset..offset + 2]);
    u16::from_le_bytes(buf)
}

fn write_u16(page: &mut [u8], offset: usize, value: u16) {
    let bytes = value.to_le_bytes();
    page[offset..offset + 2].copy_from_slice(&bytes);
}

fn entry_key_at(page: &[u8], index: usize) -> Option<&[u8]> {
    let header = page_header(page);
    if index >= header.key_count as usize {
        return None;
    }
    let start = data_start(header.key_count);
    let offset_pos = HEADER_SIZE + index * OFFSET_ENTRY_SIZE;
    let rel = read_u16(page, offset_pos) as usize;
    let entry_offset = start + rel;
    let key_end = entry_offset + KEY_SIZE;
    if key_end > PAGE_SIZE {
        return None;
    }
    Some(&page[entry_offset..key_end])
}

fn entry_value_at(page: &[u8], index: usize) -> Option<&[u8]> {
    let (key_end, value_end) = entry_value_bounds(page, index)?;
    Some(&page[key_end..value_end])
}

fn entry_value_bounds(page: &[u8], index: usize) -> Option<(usize, usize)> {
    let header = page_header(page);
    if index >= header.key_count as usize {
        return None;
    }
    let value_size = entry_value_size(header.page_type);
    let start = data_start(header.key_count);
    let offset_pos = HEADER_SIZE + index * OFFSET_ENTRY_SIZE;
    let rel = read_u16(page, offset_pos) as usize;
    let entry_offset = start + rel;
    let key_end = entry_offset + KEY_SIZE;
    let value_end = key_end + value_size;
    if value_end > PAGE_SIZE {
        return None;
    }
    Some((key_end, value_end))
}

fn collect_entries(page: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let header = page_header(page);
    let mut entries = Vec::with_capacity(header.key_count as usize);
    for index in 0..header.key_count as usize {
        let key = match entry_key_at(page, index) {
            Some(key) => key.to_vec(),
            None => break,
        };
        let value = match entry_value_at(page, index) {
            Some(value) => value.to_vec(),
            None => break,
        };
        entries.push((key, value));
    }
    entries
}

fn entry_offset_at(page: &[u8], index: usize) -> Option<usize> {
    let header = page_header(page);
    if index >= header.key_count as usize {
        return None;
    }
    let start = data_start(header.key_count);
    let offset_pos = HEADER_SIZE + index * OFFSET_ENTRY_SIZE;
    let rel = read_u16(page, offset_pos) as usize;
    let entry_offset = start + rel;
    if entry_offset + KEY_SIZE > PAGE_SIZE {
        return None;
    }
    Some(entry_offset)
}

fn find_key_pos(page: &[u8], key: &[u8]) -> Option<(bool, usize)> {
    let header = page_header(page);
    let mut lo = 0usize;
    let mut hi = header.key_count as usize;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let mid_key = entry_key_at(page, mid)?;
        match mid_key.cmp(key) {
            std::cmp::Ordering::Equal => return Some((true, mid)),
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    Some((false, lo))
}

fn page_has_room_for_one_more(header: PageHeader) -> bool {
    let total = header.key_count as usize + 1;
    let size = HEADER_SIZE + total * OFFSET_ENTRY_SIZE + total * entry_size(header.page_type);
    size <= PAGE_SIZE
}

fn page_is_full_for_insert(header: PageHeader) -> bool {
    !page_has_room_for_one_more(header)
}

fn max_keys_for_page_type(page_type: u8) -> usize {
    let unit = OFFSET_ENTRY_SIZE + entry_size(page_type);
    if unit == 0 || PAGE_SIZE <= HEADER_SIZE {
        return 0;
    }
    (PAGE_SIZE - HEADER_SIZE) / unit
}

fn min_keys_non_root(page_type: u8) -> usize {
    let max = max_keys_for_page_type(page_type);
    max / 2
}

fn key_to_high_key(key: &[u8]) -> Option<[u8; HIGH_KEY_SIZE]> {
    if key.len() != HIGH_KEY_SIZE {
        return None;
    }
    let mut out = [0u8; HIGH_KEY_SIZE];
    out.copy_from_slice(key);
    Some(out)
}

fn child_insert_pos_for_key(page: &[u8], header: PageHeader, key: &[u8]) -> usize {
    let mut lo = 0usize;
    let mut hi = header.key_count as usize;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let Some(mid_key) = entry_key_at(page, mid) else {
            return lo;
        };
        if key < mid_key {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

fn child_id_at_pos(page: &[u8], header: PageHeader, pos: usize) -> PageId {
    if pos == 0 {
        return header.left_child;
    }
    let idx = pos.saturating_sub(1);
    entry_value_at(page, idx)
        .map(decode_child_id)
        .unwrap_or(header.left_child)
}

fn rebuild_page_with_insert(
    page: &[u8],
    header: PageHeader,
    insert_pos: usize,
    key: &[u8],
    value: &[u8],
) -> Result<Page> {
    let value_size = entry_value_size(header.page_type);
    let total = header.key_count as usize + 1;
    let mut out = new_page(header.page_type, header.level).to_vec();
    let mut new_header = header;
    new_header.key_count = total as u16;
    write_header(&mut out, new_header);

    let start = data_start(new_header.key_count);
    let mut cursor = start;
    for pos in 0..total {
        let (k, v) = if pos == insert_pos {
            (key, value)
        } else {
            let idx = if pos < insert_pos { pos } else { pos - 1 };
            let k = entry_key_at(page, idx).ok_or(Error::InvalidPageSize(pos, PAGE_SIZE))?;
            let v = entry_value_at(page, idx).ok_or(Error::InvalidPageSize(pos, PAGE_SIZE))?;
            (k, v)
        };

        let offset_pos = HEADER_SIZE + pos * OFFSET_ENTRY_SIZE;
        let rel = (cursor - start) as u16;
        write_u16(&mut out, offset_pos, rel);
        out[cursor..cursor + KEY_SIZE].copy_from_slice(k);
        cursor += KEY_SIZE;
        out[cursor..cursor + value_size].copy_from_slice(v);
        cursor += value_size;
    }
    Ok(Bytes::from(out))
}

fn rebuild_page_with_remove(page: &[u8], header: PageHeader, remove_pos: usize) -> Result<Page> {
    let value_size = entry_value_size(header.page_type);
    let total = header.key_count as usize;
    if remove_pos >= total {
        return Ok(Bytes::copy_from_slice(page));
    }
    let new_total = total.saturating_sub(1);
    let mut out = new_page(header.page_type, header.level).to_vec();
    let mut new_header = header;
    new_header.key_count = new_total as u16;
    write_header(&mut out, new_header);
    let start = data_start(new_header.key_count);
    let mut cursor = start;
    let mut out_pos = 0usize;
    for idx in 0..total {
        if idx == remove_pos {
            continue;
        }
        let k = entry_key_at(page, idx).ok_or(Error::InvalidPageSize(idx, PAGE_SIZE))?;
        let v = entry_value_at(page, idx).ok_or(Error::InvalidPageSize(idx, PAGE_SIZE))?;
        let offset_pos = HEADER_SIZE + out_pos * OFFSET_ENTRY_SIZE;
        let rel = (cursor - start) as u16;
        write_u16(&mut out, offset_pos, rel);
        out[cursor..cursor + KEY_SIZE].copy_from_slice(k);
        cursor += KEY_SIZE;
        out[cursor..cursor + value_size].copy_from_slice(v);
        cursor += value_size;
        out_pos += 1;
    }
    Ok(Bytes::from(out))
}

fn find_in_leaf(page: &Arc<Page>, key: &[u8]) -> Option<LeafValueRef> {
    let header = page_header(page);
    let mut lo = 0usize;
    let mut hi = header.key_count as usize;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let mid_key = entry_key_at(page, mid)?;
        match mid_key.cmp(key) {
            std::cmp::Ordering::Equal => {
                let (value_offset, _) = entry_value_bounds(page, mid)?;
                if matches!(std::env::var("SCALE_KV_BTREE_DEBUG").as_deref(), Ok("1")) {
                    let row = decode_leaf_value_ref(page, value_offset)?;
                    eprintln!(
                        "[btree-get] equal mid={} key_hex={} commit_lsn={} flags={} value_hex={}",
                        mid,
                        hex::encode(key),
                        row.meta.commit_lsn,
                        row.meta.flags,
                        hex::encode(row.value)
                    );
                }
                return decode_leaf_value_ref(page, value_offset);
            }
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    None
}

fn lower_bound_in_leaf(page: &[u8], key: &[u8]) -> usize {
    let header = page_header(page);
    let mut lo = 0usize;
    let mut hi = header.key_count as usize;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let mid_key = match entry_key_at(page, mid) {
            Some(key) => key,
            None => return lo,
        };
        if mid_key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn encode_entries(
    page: &mut [u8],
    entries: &[(Vec<u8>, Vec<u8>)],
    mut header: PageHeader,
) -> Result<()> {
    let value_size = entry_value_size(header.page_type);
    let start = data_start(entries.len() as u16);
    let mut cursor = start;
    for (index, (k, v)) in entries.iter().enumerate() {
        if k.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(k.len(), KEY_SIZE));
        }
        if v.len() != value_size {
            return Err(Error::InvalidValueSize(v.len(), value_size));
        }
        if cursor + KEY_SIZE + value_size > PAGE_SIZE {
            return Err(Error::InvalidPageSize(cursor, PAGE_SIZE));
        }
        let offset_pos = HEADER_SIZE + index * OFFSET_ENTRY_SIZE;
        let rel = (cursor - start) as u16;
        write_u16(page, offset_pos, rel);
        page[cursor..cursor + KEY_SIZE].copy_from_slice(k);
        cursor += KEY_SIZE;
        page[cursor..cursor + value_size].copy_from_slice(v);
        cursor += value_size;
    }
    header.key_count = entries.len() as u16;
    write_header(page, header);
    Ok(())
}

fn split_leaf(page: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(Vec<u8>, (Page, Page))> {
    let mid = entries.len() / 2;
    let left_entries = &entries[..mid];
    let right_entries = &entries[mid..];
    let mut left = new_page(PAGE_TYPE_LEAF, 0).to_vec();
    let mut right = new_page(PAGE_TYPE_LEAF, 0).to_vec();
    let mut left_header = leaf_page_header(page);
    let mut right_header = leaf_page_header(page);
    let sep = right_entries[0].0.clone();

    // Preserve existing high_key as right high_key; set left high_key to sep.
    right_header.high_key = left_header.high_key;

    let mut hk = [0u8; HIGH_KEY_SIZE];
    hk.copy_from_slice(&sep);
    left_header.high_key = Some(hk);

    encode_entries(&mut left, left_entries, left_header)?;
    encode_entries(&mut right, right_entries, right_header)?;
    Ok((sep, (Bytes::from(left), Bytes::from(right))))
}

fn split_internal(page: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(Vec<u8>, (Page, Page))> {
    let mid = entries.len() / 2;
    let separator = entries[mid].0.clone();
    let left_entries = &entries[..mid];
    let right_entries = &entries[mid + 1..];
    let mut left = new_page(PAGE_TYPE_INTERNAL, page_header(page).level).to_vec();
    let mut right = new_page(PAGE_TYPE_INTERNAL, page_header(page).level).to_vec();
    let old_hk = page_header(page).high_key;
    let mut left_header = page_header(page);
    let mut right_header = page_header(page);

    // High key for left internal becomes separator; right inherits old high_key.
    let mut hk = [0u8; HIGH_KEY_SIZE];
    hk.copy_from_slice(&separator);
    left_header.high_key = Some(hk);

    right_header.high_key = old_hk;
    right_header.left_child = decode_child_id(&entries[mid].1);

    encode_entries(&mut left, left_entries, left_header)?;
    encode_entries(&mut right, right_entries, right_header)?;
    Ok((separator, (Bytes::from(left), Bytes::from(right))))
}

fn encode_leaf_value(leaf_value: &LeafValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(LEAF_VALUE_SIZE);
    buf.extend_from_slice(&leaf_value.value);
    buf.extend_from_slice(&leaf_value.commit_lsn.to_le_bytes());
    let (undo_pid, undo_sid) = leaf_value
        .undo_ptr
        .map(|u| (u.page_id, u.slot_id))
        .unwrap_or((0, 0));
    buf.extend_from_slice(&undo_pid.to_le_bytes());
    buf.extend_from_slice(&undo_sid.to_le_bytes());
    buf.extend_from_slice(&leaf_value.flags.to_le_bytes());
    buf.extend_from_slice(&leaf_value.intent_txn_id.to_le_bytes());
    buf.extend_from_slice(&leaf_value.intent_lsn.to_le_bytes());
    buf
}

fn decode_leaf_value_ref(page: &Arc<Page>, value_offset: usize) -> Option<LeafValueRef> {
    let value_end = value_offset.checked_add(VALUE_SIZE)?;
    let meta_end = value_offset.checked_add(LEAF_VALUE_SIZE)?;
    if meta_end > page.len() || value_end > page.len() {
        return None;
    }
    let raw = &page[value_offset..meta_end];
    Some(LeafValueRef {
        meta: decode_leaf_value_meta(raw),
        value: page.slice(value_offset..value_end),
    })
}

fn decode_leaf_value_meta(buf: &[u8]) -> LeafValueMeta {
    let mut off = VALUE_SIZE;
    let commit_lsn = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
    off += 8;
    let undo_page_id = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
    off += 8;
    let undo_slot_id = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap());
    off += 2;
    let flags = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap());
    off += 2;
    let intent_txn_id = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
    off += 8;
    let intent_lsn = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
    let undo_ptr = if undo_page_id == 0 {
        None
    } else {
        Some(UndoPtr {
            page_id: undo_page_id,
            slot_id: undo_slot_id,
        })
    };
    LeafValueMeta {
        commit_lsn,
        undo_ptr,
        flags,
        intent_txn_id,
        intent_lsn,
    }
}

fn encode_child_id(child: PageId) -> Vec<u8> {
    child.to_le_bytes().to_vec()
}

fn decode_child_id(buf: &[u8]) -> PageId {
    u64::from_le_bytes(buf[0..8].try_into().unwrap())
}

#[cfg(test)]
#[path = "page_bptree_tests.rs"]
mod page_bptree_tests;
