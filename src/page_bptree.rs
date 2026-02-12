use crate::{Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result, VALUE_SIZE, undo_pg::UndoPtr};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

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

pub type PageReadLatchGuard = OwnedRwLockReadGuard<()>;
pub type PageWriteLatchGuard = OwnedRwLockWriteGuard<()>;

#[derive(Debug)]
pub struct PageLatchTable {
    shards: Vec<std::sync::Mutex<HashMap<PageId, Arc<RwLock<()>>>>>,
}

impl PageLatchTable {
    pub fn new(shards: usize) -> Self {
        let count = shards.max(1);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(std::sync::Mutex::new(HashMap::new()));
        }
        Self { shards: out }
    }

    fn shard(&self, page_id: PageId) -> usize {
        (page_id as usize) % self.shards.len()
    }

    fn latch_for(&self, page_id: PageId) -> Arc<RwLock<()>> {
        let idx = self.shard(page_id);
        let mut shard = self.shards[idx].lock().unwrap();
        Arc::clone(
            shard
                .entry(page_id)
                .or_insert_with(|| Arc::new(RwLock::new(()))),
        )
    }

    pub async fn acquire_read(&self, page_id: PageId) -> PageReadLatchGuard {
        self.latch_for(page_id).read_owned().await
    }

    pub async fn acquire_write(&self, page_id: PageId) -> PageWriteLatchGuard {
        self.latch_for(page_id).write_owned().await
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

/// Trait for page storage backend.
/// B+Tree uses this trait to read/write pages, allowing different backends:
/// - InMemoryPageProvider: HashMap-based, for testing
/// - SharedPageProvider: Wraps Arc<RwLock<HashMap>> for shared cache
#[allow(async_fn_in_trait)]
pub trait AsyncPageProvider {
    /// Read a page by ID. Returns None if page doesn't exist.
    async fn read_page(&self, page_id: PageId) -> Option<Page>;

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
    shards: Vec<std::sync::Mutex<lru::LruCache<PageId, Page>>>,
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
        let per = (cap + count - 1) / count;
        let per = NonZeroUsize::new(per.max(1)).unwrap();

        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(std::sync::Mutex::new(lru::LruCache::new(per)));
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
        let idx = self.shard(page_id);
        self.shards[idx].lock().unwrap().get(&page_id).cloned()
    }

    pub fn insert(&self, page_id: PageId, page: Page) {
        let idx = self.shard(page_id);
        self.shards[idx].lock().unwrap().put(page_id, page);
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        let idx = self.shard(page_id);
        self.shards[idx].lock().unwrap().contains(&page_id)
    }

    pub fn remove(&self, page_id: PageId) {
        let idx = self.shard(page_id);
        let _ = self.shards[idx].lock().unwrap().pop(&page_id);
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().len())
            .sum()
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
    len: usize,
}

impl PageBPlusTree<InMemoryPageProvider> {
    /// Create a new B+Tree with in-memory storage.
    pub fn new() -> Self {
        let provider = InMemoryPageProvider::new();
        let root_id = provider.alloc_page_id();
        futures::executor::block_on(async {
            provider
                .write_page(root_id, new_page(PAGE_TYPE_LEAF, 0))
                .await;
        });
        provider.set_root_page_id(root_id);
        Self { provider, len: 0 }
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
        Self { provider, len: 0 }
    }

    /// Create a new B+Tree, initializing the root page in the provider.
    pub fn new_with_provider(provider: P) -> Self {
        let root_id = provider.alloc_page_id();
        futures::executor::block_on(async {
            provider
                .write_page(root_id, new_page(PAGE_TYPE_LEAF, 0))
                .await;
        });
        provider.set_root_page_id(root_id);
        Self { provider, len: 0 }
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
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub async fn get(&self, key: &[u8]) -> Option<LeafValue> {
        for _ in 0..8 {
            let mut leaf_id = self.find_leaf_for_read(key).await?;
            loop {
                let _leaf_guard = self.provider.acquire_read_latch(leaf_id).await;
                let page = self.provider.read_page(leaf_id).await?;
                let header = page_header(&page);
                if header.page_type != PAGE_TYPE_LEAF {
                    break;
                }
                if let Some(hk) = header.high_key {
                    if key > hk.as_slice() {
                        if let Some(next) = header.next_leaf {
                            leaf_id = next;
                            continue;
                        }
                        break;
                    }
                }
                return find_in_leaf(&page, key);
            }
        }
        None
    }

    /// Locking protocol:
    /// - Writers use top-down split-before-descend with write-latch coupling.
    /// - The parent is always write-latched while splitting a full child.
    /// - We never upgrade a child lock to parent, avoiding upgrade deadlocks.
    /// Not handled yet: merge/rebalance on delete, range/key gap locks, and full phantom protection.
    pub async fn insert(&mut self, key: Vec<u8>, leaf_value: LeafValue) -> Result<()> {
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
                .ok_or_else(|| Error::InvalidPageSize(current_id as usize, PAGE_SIZE))?;
            let current_header = page_header(&current_page);

            if current_header.page_type == PAGE_TYPE_LEAF {
                let (found, pos) = find_key_pos(&current_page, &key).unwrap_or((false, 0));
                if found {
                    let offset = entry_offset_at(&current_page, pos)
                        .ok_or_else(|| Error::InvalidPageSize(pos, PAGE_SIZE))?;
                    let value_offset = offset + KEY_SIZE;
                    current_page[value_offset..value_offset + LEAF_VALUE_SIZE]
                        .copy_from_slice(&encoded);
                    self.provider.write_page(current_id, current_page).await;
                    drop(current_guard);
                    return Ok(());
                }

                let rebuilt =
                    rebuild_page_with_insert(&current_page, current_header, pos, &key, &encoded)?;
                self.provider.write_page(current_id, rebuilt).await;
                self.len += 1;
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
                .ok_or_else(|| Error::InvalidPageSize(child_id as usize, PAGE_SIZE))?;
            let child_header = page_header(&child_page);

            if page_is_full_for_insert(child_header) {
                if !page_has_room_for_one_more(current_header) {
                    return Err(Error::InvalidPageSize(current_id as usize, PAGE_SIZE));
                }

                let (separator, right_id) = self
                    .split_child_and_update_parent(
                        current_id,
                        current_page,
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

    pub async fn remove(&mut self, key: &[u8]) -> Result<()> {
        // Merge/rebalance is intentionally deferred. Deletes only remove the key from the leaf.
        let leaf_id = match self.find_leaf_for_read(key).await {
            Some(id) => id,
            None => return Ok(()),
        };
        let _leaf_guard = self.provider.acquire_write_latch(leaf_id).await;
        let leaf = match self.provider.read_page(leaf_id).await {
            Some(p) => p,
            None => return Ok(()),
        };
        let (found, pos) = find_key_pos(&leaf, key).unwrap_or((false, 0));
        if !found {
            return Ok(());
        }

        let header = leaf_page_header(&leaf);
        let rebuilt = rebuild_page_with_remove(&leaf, header, pos)?;
        self.provider.write_page(leaf_id, rebuilt).await;
        self.len = self.len.saturating_sub(1);
        Ok(())
    }

    pub async fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, LeafValue)> {
        let mut out = Vec::new();
        self.range_visit(start, end, |k, leaf_value| {
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
        mut f: impl FnMut(&[u8], LeafValue) -> bool,
    ) {
        let Some(mut leaf_id) = self.find_leaf_for_read(start).await else {
            return;
        };

        // Same high-key correction as point lookup.
        loop {
            let _leaf_guard = self.provider.acquire_read_latch(leaf_id).await;
            let page = match self.provider.read_page(leaf_id).await {
                Some(page) => page,
                None => break,
            };
            let header = leaf_page_header(&page);
            if let Some(hk) = header.high_key {
                if start > hk.as_slice() {
                    if let Some(next) = header.next_leaf {
                        leaf_id = next;
                        continue;
                    }
                }
            }
            break;
        }

        let mut current = Some(leaf_id);
        while let Some(page_id) = current {
            let _page_guard = self.provider.acquire_read_latch(page_id).await;
            let page = match self.provider.read_page(page_id).await {
                Some(page) => page,
                None => break,
            };
            let header = leaf_page_header(&page);
            let mut idx = if page_id == leaf_id {
                lower_bound_in_leaf(&page, start)
            } else {
                0
            };
            let key_count = header.key_count as usize;
            while idx < key_count {
                let key = match entry_key_at(&page, idx) {
                    Some(key) => key,
                    None => break,
                };
                if key > end {
                    return;
                }
                let value = match entry_value_at(&page, idx) {
                    Some(value) => value,
                    None => break,
                };
                if !f(key, decode_leaf_value(value)) {
                    return;
                }
                idx += 1;
            }
            current = header.next_leaf;
        }
    }

    async fn find_leaf_for_read(&self, key: &[u8]) -> Option<PageId> {
        let mut current = self.provider.root_page_id();
        let mut current_guard = self.provider.acquire_read_latch(current).await;
        loop {
            let page = self.provider.read_page(current).await?;
            let header = page_header(&page);
            if header.page_type == PAGE_TYPE_LEAF {
                drop(current_guard);
                return Some(current);
            }
            let child =
                child_id_at_pos(&page, header, child_insert_pos_for_key(&page, header, key));
            let child_guard = self.provider.acquire_read_latch(child).await;
            drop(current_guard);
            current = child;
            current_guard = child_guard;
        }
    }

    async fn ensure_root_not_full(&self) -> Result<()> {
        let root_id = self.provider.root_page_id();
        let _root_guard = self.provider.acquire_write_latch(root_id).await;
        let root = self
            .provider
            .read_page(root_id)
            .await
            .ok_or_else(|| Error::InvalidPageSize(root_id as usize, PAGE_SIZE))?;
        let root_header = page_header(&root);
        if !page_is_full_for_insert(root_header) {
            return Ok(());
        }

        let entries = collect_entries(&root);
        let right_id = self.provider.alloc_page_id();
        let (separator, mut left_page, mut right_page) = if root_header.page_type == PAGE_TYPE_LEAF
        {
            let (separator, (left, right)) = split_leaf(&root, &entries)?;
            (separator, left, right)
        } else {
            let (separator, (left, right)) = split_internal(&root, &entries)?;
            (separator, left, right)
        };

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

        self.provider.write_page(root_id, left_page).await;
        self.provider.write_page(right_id, right_page).await;

        let mut new_root = new_page(PAGE_TYPE_INTERNAL, root_header.level.saturating_add(1));
        let mut new_root_header = page_header(&new_root);
        new_root_header.left_child = root_id;
        let entries = vec![(separator, encode_child_id(right_id))];
        encode_entries(&mut new_root, &entries, new_root_header)?;
        let new_root_id = self.provider.alloc_page_id();
        self.provider.write_page(new_root_id, new_root).await;
        self.provider.set_root_page_id(new_root_id);
        Ok(())
    }

    async fn split_child_and_update_parent(
        &self,
        parent_id: PageId,
        mut parent_page: Page,
        parent_header: PageHeader,
        child_id: PageId,
        child_page: Page,
        child_header: PageHeader,
        child_pos: usize,
    ) -> Result<(Vec<u8>, PageId)> {
        let entries = collect_entries(&child_page);
        let right_id = self.provider.alloc_page_id();
        let (separator, mut left_page, mut right_page) = if child_header.page_type == PAGE_TYPE_LEAF
        {
            let (separator, (left, right)) = split_leaf(&child_page, &entries)?;
            (separator, left, right)
        } else {
            let (separator, (left, right)) = split_internal(&child_page, &entries)?;
            (separator, left, right)
        };

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

        self.provider.write_page(child_id, left_page).await;
        self.provider.write_page(right_id, right_page).await;

        let mut parent_entries = collect_entries(&parent_page);
        parent_entries.insert(child_pos, (separator.clone(), encode_child_id(right_id)));
        encode_entries(&mut parent_page, &parent_entries, parent_header)?;
        self.provider.write_page(parent_id, parent_page).await;

        Ok((separator, right_id))
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

fn page_header(page: &Page) -> PageHeader {
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

fn leaf_page_header(page: &Page) -> PageHeader {
    page_header(page)
}

fn write_header(page: &mut Page, header: PageHeader) {
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
    page
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

#[allow(dead_code)]
fn decode_entries(page: &Page) -> Vec<(Vec<u8>, Vec<u8>)> {
    let header = page_header(page);
    let mut entries = Vec::with_capacity(header.key_count as usize);
    let value_size = entry_value_size(header.page_type);
    let start = data_start(header.key_count);
    for index in 0..header.key_count as usize {
        let offset_pos = HEADER_SIZE + index * OFFSET_ENTRY_SIZE;
        let rel = read_u16(page, offset_pos) as usize;
        let entry_offset = start + rel;
        let key_end = entry_offset + KEY_SIZE;
        let value_end = key_end + value_size;
        if value_end > PAGE_SIZE {
            break;
        }
        let key = page[entry_offset..key_end].to_vec();
        let val = page[key_end..value_end].to_vec();
        entries.push((key, val));
    }
    entries
}

#[allow(dead_code)]
fn for_each_entry<'a>(page: &'a Page, mut f: impl FnMut(&'a [u8], &'a [u8]) -> bool) {
    let header = page_header(page);
    let value_size = entry_value_size(header.page_type);
    let start = data_start(header.key_count);
    for index in 0..header.key_count as usize {
        let offset_pos = HEADER_SIZE + index * OFFSET_ENTRY_SIZE;
        let rel = read_u16(page, offset_pos) as usize;
        let entry_offset = start + rel;
        let key_end = entry_offset + KEY_SIZE;
        let value_end = key_end + value_size;
        if value_end > PAGE_SIZE {
            break;
        }
        let key = &page[entry_offset..key_end];
        let val = &page[key_end..value_end];
        if !f(key, val) {
            break;
        }
    }
}

fn entry_key_at<'a>(page: &'a Page, index: usize) -> Option<&'a [u8]> {
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

fn entry_value_at<'a>(page: &'a Page, index: usize) -> Option<&'a [u8]> {
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
    Some(&page[key_end..value_end])
}

fn collect_entries(page: &Page) -> Vec<(Vec<u8>, Vec<u8>)> {
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

fn entry_offset_at(page: &Page, index: usize) -> Option<usize> {
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

fn find_key_pos(page: &Page, key: &[u8]) -> Option<(bool, usize)> {
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

fn child_insert_pos_for_key(page: &Page, header: PageHeader, key: &[u8]) -> usize {
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

fn child_id_at_pos(page: &Page, header: PageHeader, pos: usize) -> PageId {
    if pos == 0 {
        return header.left_child;
    }
    let idx = pos.saturating_sub(1);
    entry_value_at(page, idx)
        .map(decode_child_id)
        .unwrap_or(header.left_child)
}

fn rebuild_page_with_insert(
    page: &Page,
    header: PageHeader,
    insert_pos: usize,
    key: &[u8],
    value: &[u8],
) -> Result<Page> {
    let value_size = entry_value_size(header.page_type);
    let total = header.key_count as usize + 1;
    let mut out = new_page(header.page_type, header.level);
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
            let k =
                entry_key_at(page, idx).ok_or_else(|| Error::InvalidPageSize(pos, PAGE_SIZE))?;
            let v =
                entry_value_at(page, idx).ok_or_else(|| Error::InvalidPageSize(pos, PAGE_SIZE))?;
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
    Ok(out)
}

fn rebuild_page_with_remove(page: &Page, header: PageHeader, remove_pos: usize) -> Result<Page> {
    let value_size = entry_value_size(header.page_type);
    let total = header.key_count as usize;
    if remove_pos >= total {
        return Ok(page.clone());
    }
    let new_total = total.saturating_sub(1);
    let mut out = new_page(header.page_type, header.level);
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
        let k = entry_key_at(page, idx).ok_or_else(|| Error::InvalidPageSize(idx, PAGE_SIZE))?;
        let v = entry_value_at(page, idx).ok_or_else(|| Error::InvalidPageSize(idx, PAGE_SIZE))?;
        let offset_pos = HEADER_SIZE + out_pos * OFFSET_ENTRY_SIZE;
        let rel = (cursor - start) as u16;
        write_u16(&mut out, offset_pos, rel);
        out[cursor..cursor + KEY_SIZE].copy_from_slice(k);
        cursor += KEY_SIZE;
        out[cursor..cursor + value_size].copy_from_slice(v);
        cursor += value_size;
        out_pos += 1;
    }
    Ok(out)
}

fn find_in_leaf(page: &Page, key: &[u8]) -> Option<LeafValue> {
    let header = page_header(page);
    let mut lo = 0usize;
    let mut hi = header.key_count as usize;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let mid_key = entry_key_at(page, mid)?;
        match mid_key.cmp(key) {
            std::cmp::Ordering::Equal => {
                let value = entry_value_at(page, mid)?;
                if matches!(std::env::var("SCALE_KV_BTREE_DEBUG").as_deref(), Ok("1")) {
                    let row = decode_leaf_value(value);
                    eprintln!(
                        "[btree-get] equal mid={} key_hex={} commit_lsn={} flags={} value_hex={}",
                        mid,
                        hex::encode(key),
                        row.commit_lsn,
                        row.flags,
                        hex::encode(value)
                    );
                }
                return Some(decode_leaf_value(value));
            }
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    None
}

fn lower_bound_in_leaf(page: &Page, key: &[u8]) -> usize {
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
    page: &mut Page,
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

#[allow(dead_code)]
fn fits_in_page(entries: &[(Vec<u8>, Vec<u8>)]) -> bool {
    if entries.is_empty() {
        return HEADER_SIZE <= PAGE_SIZE;
    }
    let value_size = entries[0].1.len();
    let size =
        HEADER_SIZE + entries.len() * OFFSET_ENTRY_SIZE + entries.len() * (KEY_SIZE + value_size);
    size <= PAGE_SIZE
}

fn split_leaf(page: &Page, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(Vec<u8>, (Page, Page))> {
    let mid = entries.len() / 2;
    let left_entries = &entries[..mid];
    let right_entries = &entries[mid..];
    let mut left = new_page(PAGE_TYPE_LEAF, 0);
    let mut right = new_page(PAGE_TYPE_LEAF, 0);
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
    Ok((sep, (left, right)))
}

fn split_internal(page: &Page, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(Vec<u8>, (Page, Page))> {
    let mid = entries.len() / 2;
    let separator = entries[mid].0.clone();
    let left_entries = &entries[..mid];
    let right_entries = &entries[mid + 1..];
    let mut left = new_page(PAGE_TYPE_INTERNAL, page_header(page).level);
    let mut right = new_page(PAGE_TYPE_INTERNAL, page_header(page).level);
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
    Ok((separator, (left, right)))
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

fn decode_leaf_value(buf: &[u8]) -> LeafValue {
    let mut value = [0u8; VALUE_SIZE];
    value.copy_from_slice(&buf[0..VALUE_SIZE]);
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
    LeafValue {
        value,
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
mod tests {
    use super::{AsyncPageProvider, InMemoryPageProvider, LeafValue, PageBPlusTree};
    use crate::{KEY_SIZE, VALUE_SIZE};
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::task::LocalSet;

    fn key_for(i: u32) -> Vec<u8> {
        let mut key = format!("k{:03}", i).into_bytes();
        while key.len() < KEY_SIZE {
            key.push(b'x');
        }
        key.truncate(KEY_SIZE);
        key
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_insert_get_remove_len() {
        let mut tree = PageBPlusTree::new();
        let key = key_for(1);
        let slot = LeafValue {
            value: [1u8; VALUE_SIZE],
            commit_lsn: 11,
            undo_ptr: None,
            flags: 0,
            intent_txn_id: 0,
            intent_lsn: 0,
        };
        let updated = LeafValue {
            value: [2u8; VALUE_SIZE],
            commit_lsn: 22,
            undo_ptr: None,
            flags: 1,
            intent_txn_id: 0,
            intent_lsn: 0,
        };

        assert_eq!(tree.get(&key).await, None);
        tree.insert(key.clone(), slot.clone()).await.unwrap();
        assert_eq!(tree.get(&key).await, Some(slot));
        assert_eq!(tree.len(), 1);

        tree.insert(key.clone(), updated.clone()).await.unwrap();
        assert_eq!(tree.get(&key).await, Some(updated));
        assert_eq!(tree.len(), 1);

        tree.remove(&key).await.unwrap();
        assert_eq!(tree.get(&key).await, None);
        assert_eq!(tree.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_range_across_splits() {
        let mut tree = PageBPlusTree::new();
        for i in 0..120u32 {
            let key = key_for(i);
            let slot = LeafValue {
                value: [i as u8; VALUE_SIZE],
                commit_lsn: i as u64,
                undo_ptr: None,
                flags: 0,
                intent_txn_id: 0,
                intent_lsn: 0,
            };
            tree.insert(key, slot).await.unwrap();
        }

        let start = key_for(10);
        let end = key_for(50);
        let range = tree.range(&start, &end).await;
        assert_eq!(range.len(), 41);
        assert_eq!(range.first().unwrap().0, start);
        assert_eq!(range.last().unwrap().0, end);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_with_custom_provider() {
        let provider = InMemoryPageProvider::new();
        let tree = PageBPlusTree::new_with_provider(provider);
        let mut tree = tree;

        let key = key_for(2);
        let slot = LeafValue {
            value: [3u8; VALUE_SIZE],
            commit_lsn: 100,
            undo_ptr: None,
            flags: 0,
            intent_txn_id: 0,
            intent_lsn: 0,
        };

        tree.insert(key.clone(), slot.clone()).await.unwrap();
        assert_eq!(tree.get(&key).await, Some(slot));
        assert_eq!(tree.provider().page_count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_provider_page_allocation() {
        let provider = InMemoryPageProvider::new();

        let p1 = provider.alloc_page_id();
        let p2 = provider.alloc_page_id();
        let p3 = provider.alloc_page_id();

        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
        assert_eq!(p3, 3);

        let page_data = vec![42u8; crate::PAGE_SIZE];
        provider.write_page(p1, page_data.clone()).await;

        assert_eq!(provider.read_page(p1).await, Some(page_data));
        assert_eq!(provider.read_page(p2).await, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_shared_page_provider() {
        use super::{PageCache, SharedPageProvider};
        use std::sync::Arc;

        let pages = Arc::new(PageCache::new(4));
        let next_page_id = Arc::new(AtomicU64::new(1));

        let provider = SharedPageProvider::new(pages.clone(), next_page_id.clone());
        let tree = PageBPlusTree::new_with_provider(provider);
        let mut tree = tree;

        let key = key_for(3);
        let slot = LeafValue {
            value: [7u8; VALUE_SIZE],
            commit_lsn: 42,
            undo_ptr: None,
            flags: 0,
            intent_txn_id: 0,
            intent_lsn: 0,
        };

        tree.insert(key.clone(), slot.clone()).await.unwrap();
        assert_eq!(tree.get(&key).await, Some(slot));

        assert!(pages.len() >= 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_concurrent_inserts_split_safety() {
        use super::{PageCache, SharedPageProvider};
        use std::sync::Arc;

        let pages = Arc::new(PageCache::new_with_capacity(8, 256));
        let next_page_id = Arc::new(AtomicU64::new(1));
        let provider = SharedPageProvider::new(Arc::clone(&pages), Arc::clone(&next_page_id));

        // Initialize root once; other tree handles share provider state and latch table.
        let _tree = PageBPlusTree::new_with_provider(provider.clone());

        let workers = 6u32;
        let per_worker = 180u32;
        let local = LocalSet::new();
        let provider_for_tasks = provider.clone();

        tokio::time::timeout(Duration::from_secs(10), async move {
            local
                .run_until(async move {
                    let mut handles = Vec::new();
                    for worker in 0..workers {
                        let provider = provider_for_tasks.clone();
                        handles.push(tokio::task::spawn_local(async move {
                            let mut tree = PageBPlusTree::with_provider(provider);
                            for i in 0..per_worker {
                                let logical = worker * per_worker + i;
                                let key = key_for(logical);
                                let slot = LeafValue {
                                    value: [logical as u8; VALUE_SIZE],
                                    commit_lsn: logical as u64,
                                    undo_ptr: None,
                                    flags: 0,
                                    intent_txn_id: 0,
                                    intent_lsn: 0,
                                };
                                tree.insert(key, slot).await.unwrap();
                            }
                        }));
                    }
                    for handle in handles {
                        handle.await.unwrap();
                    }
                })
                .await;
        })
        .await
        .expect("concurrent insert run timed out (possible deadlock)");

        let tree = PageBPlusTree::with_provider(provider.clone());
        for logical in 0..(workers * per_worker) {
            let key = key_for(logical);
            assert!(
                tree.get(&key).await.is_some(),
                "missing key after concurrent split inserts: logical={}",
                logical
            );
        }

        let start = key_for(0);
        let end = key_for(workers * per_worker - 1);
        let rows = tree.range(&start, &end).await;
        assert!(!rows.is_empty());
    }
}
