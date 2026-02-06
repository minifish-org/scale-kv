use crate::{Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hex;

const PAGE_TYPE_INTERNAL: u8 = 1;
const PAGE_TYPE_LEAF: u8 = 2;

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
const SLOT_REF_SIZE: usize = 10;
const CHILD_ID_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

/// Trait for page storage backend.
/// B+Tree uses this trait to read/write pages, allowing different backends:
/// - InMemoryPageProvider: HashMap-based, for testing
/// - SharedPageProvider: Wraps Arc<RwLock<HashMap>> for shared cache
pub trait PageProvider {
    /// Read a page by ID. Returns None if page doesn't exist.
    fn read_page(&self, page_id: PageId) -> Option<Page>;

    /// Write a page. Creates if not exists, updates if exists.
    fn write_page(&self, page_id: PageId, page: Page);

    /// Allocate a new page ID.
    fn alloc_page_id(&self) -> PageId;

    /// Get the root page ID (for B+Tree metadata).
    fn root_page_id(&self) -> PageId;

    /// Set the root page ID.
    fn set_root_page_id(&self, page_id: PageId);
}

/// In-memory page provider using HashMap with RefCell for interior mutability.
/// Suitable for testing and single-node usage.
pub struct InMemoryPageProvider {
    pages: RefCell<HashMap<PageId, Page>>,
    next_page_id: AtomicU64,
    root: AtomicU64,
}

impl InMemoryPageProvider {
    pub fn new() -> Self {
        Self {
            pages: RefCell::new(HashMap::new()),
            next_page_id: AtomicU64::new(1),
            root: AtomicU64::new(0),
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

impl PageProvider for InMemoryPageProvider {
    fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.pages.borrow().get(&page_id).cloned()
    }

    fn write_page(&self, page_id: PageId, page: Page) {
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
}

pub const DEFAULT_PAGE_CACHE_SHARDS: usize = 64;

pub struct PageCache {
    shards: Vec<std::sync::Mutex<lru::LruCache<PageId, Page>>>,
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
        Self { shards: out }
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
}

/// Shared page provider that wraps Arc<RwLock<HashMap>>.
/// Used by ComputeNode to share page_cache between B+Tree and data pages.
pub struct SharedPageProvider {
    pages: Arc<PageCache>,
    next_page_id: Arc<AtomicU64>,
    root: AtomicU64,
}

impl SharedPageProvider {
    pub fn new(pages: Arc<PageCache>, next_page_id: Arc<AtomicU64>) -> Self {
        Self {
            pages,
            next_page_id,
            root: AtomicU64::new(0),
        }
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }
}

impl PageProvider for SharedPageProvider {
    fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.pages.get(page_id)
    }

    fn write_page(&self, page_id: PageId, page: Page) {
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
}

/// Page-based B+Tree that uses PageProvider for storage.
pub struct PageBPlusTree<P: PageProvider> {
    provider: P,
    len: usize,
}

impl PageBPlusTree<InMemoryPageProvider> {
    /// Create a new B+Tree with in-memory storage.
    pub fn new() -> Self {
        let provider = InMemoryPageProvider::new();
        let root_id = provider.alloc_page_id();
        provider.write_page(root_id, new_page(PAGE_TYPE_LEAF, 0));
        provider.set_root_page_id(root_id);
        Self { provider, len: 0 }
    }
}

impl Default for PageBPlusTree<InMemoryPageProvider> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: PageProvider> PageBPlusTree<P> {
    /// Create a new B+Tree with a custom page provider.
    /// The provider should already have an empty root page allocated.
    pub fn with_provider(provider: P) -> Self {
        Self { provider, len: 0 }
    }

    /// Create a new B+Tree, initializing the root page in the provider.
    pub fn new_with_provider(provider: P) -> Self {
        let root_id = provider.alloc_page_id();
        provider.write_page(root_id, new_page(PAGE_TYPE_LEAF, 0));
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

    pub fn get(&self, key: &[u8]) -> Option<SlotRef> {
        let (mut leaf_id, _) = self.find_leaf(key);

        // High-key correction: if key is above this leaf's range, follow next_leaf.
        loop {
            let page = self.provider.read_page(leaf_id)?;
            if page_header(&page).page_type != PAGE_TYPE_LEAF {
                break;
            }
            if let Some(hk) = page_header(&page).high_key {
                if key > hk.as_slice() {
                    if let Some(next) = page_header(&page).next_leaf {
                        leaf_id = next;
                        continue;
                    }
                }
            }
            return find_in_leaf(&page, key);
        }

        None
    }

    pub fn insert(&mut self, key: Vec<u8>, slot_ref: SlotRef) -> Result<()> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
        }
        let (leaf_id, stack) = self.find_leaf(&key);
        let mut leaf = self.provider.read_page(leaf_id).unwrap();
        let header = leaf_page_header(&leaf);
        let (found, pos) = match find_key_pos(&leaf, &key) {
            Some(result) => result,
            None => (false, 0),
        };
        let encoded = encode_slot_ref(slot_ref);

        if found {
            let offset = entry_offset_at(&leaf, pos)
                .ok_or_else(|| Error::InvalidPageSize(pos, PAGE_SIZE))?;
            let value_offset = offset + KEY_SIZE;
            leaf[value_offset..value_offset + SLOT_REF_SIZE].copy_from_slice(&encoded);
            self.provider.write_page(leaf_id, leaf);
            return Ok(());
        }

        let total = header.key_count as usize + 1;
        let size = HEADER_SIZE + total * OFFSET_ENTRY_SIZE + total * entry_size(PAGE_TYPE_LEAF);
        if size <= PAGE_SIZE {
            let rebuilt = rebuild_page_with_insert(&leaf, header, pos, &key, &encoded)?;
            self.provider.write_page(leaf_id, rebuilt);
            self.len += 1;
            return Ok(());
        }

        let mut entries = collect_entries(&leaf);
        entries.insert(pos, (key, encoded));
        let (separator, pages) = split_leaf(&leaf, &entries)?;
        let right_id = self.provider.alloc_page_id();
        let mut left_page = pages.0;
        let mut right_page = pages.1;

        // Wire sibling links.
        let old_header = leaf_page_header(&leaf);

        {
            let mut left_header = leaf_page_header(&left_page);
            left_header.next_leaf = Some(right_id);
            left_header.prev_leaf = old_header.prev_leaf;
            write_header(&mut left_page, left_header);
        }
        {
            let mut right_header = leaf_page_header(&right_page);
            right_header.prev_leaf = Some(leaf_id);
            right_header.next_leaf = old_header.next_leaf;
            write_header(&mut right_page, right_header);
        }

        // Fix successor's prev pointer if there was an old next.
        if let Some(next_id) = old_header.next_leaf {
            if let Some(mut next_page) = self.provider.read_page(next_id) {
                let mut nh = leaf_page_header(&next_page);
                nh.prev_leaf = Some(right_id);
                write_header(&mut next_page, nh);
                self.provider.write_page(next_id, next_page);
            }
        }

        self.provider.write_page(leaf_id, left_page);
        self.provider.write_page(right_id, right_page);
        self.len += 1;
        self.insert_into_parent(leaf_id, right_id, separator, stack)
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let (leaf_id, mut stack) = self.find_leaf(key);
        let leaf = self.provider.read_page(leaf_id).unwrap();
        let (found, pos) = match find_key_pos(&leaf, key) {
            Some(result) => result,
            None => (false, 0),
        };
        if !found {
            return Ok(());
        }

        let header = leaf_page_header(&leaf);
        let mut rebuilt = rebuild_page_with_remove(&leaf, header, pos)?;
        self.len = self.len.saturating_sub(1);

        // If this leaf becomes empty and is not the root, delete it from the parent
        // (no page id reuse) and unlink it from the leaf chain.
        let mut new_header = leaf_page_header(&rebuilt);
        if new_header.key_count == 0 && leaf_id != self.provider.root_page_id() {
            // 1) unlink from leaf chain
            if let Some(prev_id) = new_header.prev_leaf {
                if let Some(mut prev_page) = self.provider.read_page(prev_id) {
                    let mut ph = leaf_page_header(&prev_page);
                    ph.next_leaf = new_header.next_leaf;
                    write_header(&mut prev_page, ph);
                    self.provider.write_page(prev_id, prev_page);
                }
            }
            if let Some(next_id) = new_header.next_leaf {
                if let Some(mut next_page) = self.provider.read_page(next_id) {
                    let mut nh = leaf_page_header(&next_page);
                    nh.prev_leaf = new_header.prev_leaf;
                    write_header(&mut next_page, nh);
                    self.provider.write_page(next_id, next_page);
                }
            }
            new_header.prev_leaf = None;
            write_header(&mut rebuilt, new_header);

            // 2) delete reference from parent (no rebalancing; parent may become sparse)
            if let Some(parent_id) = stack.pop() {
                if let Some(mut parent) = self.provider.read_page(parent_id) {
                    let ph = page_header(&parent);
                    if ph.page_type == PAGE_TYPE_INTERNAL {
                        let mut child_index = None;
                        if ph.left_child == leaf_id {
                            child_index = Some(0usize);
                        } else {
                            for idx in 0..ph.key_count as usize {
                                let v = entry_value_at(&parent, idx).unwrap();
                                if decode_child_id(v) == leaf_id {
                                    child_index = Some(idx + 1);
                                    break;
                                }
                            }
                        }

                        if let Some(j) = child_index {
                            let mut entries = collect_entries(&parent);
                            let mut new_ph = ph;
                            if j == 0 {
                                // remove left_child: promote child1 to left_child, drop entry0
                                if !entries.is_empty() {
                                    new_ph.left_child = decode_child_id(&entries[0].1);
                                    entries.remove(0);
                                }
                            } else {
                                // remove entry(j-1), which points to child j
                                if j - 1 < entries.len() {
                                    entries.remove(j - 1);
                                }
                            }

                            // If this was the root and now has 0 keys, collapse root to its only child.
                            if parent_id == self.provider.root_page_id() && entries.is_empty() {
                                let new_root = new_ph.left_child;
                                self.provider.set_root_page_id(new_root);
                            } else {
                                encode_entries(&mut parent, &entries, new_ph)?;
                                self.provider.write_page(parent_id, parent);
                            }
                        }
                    }
                }
            }
        }

        self.provider.write_page(leaf_id, rebuilt);
        Ok(())
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, SlotRef)> {
        let mut out = Vec::new();
        self.range_visit(start, end, |k, slot_ref| {
            out.push((k.to_vec(), slot_ref));
            true
        });
        out
    }

    pub fn debug_leaf_keys(&self, key: &[u8], limit: usize) -> Vec<Vec<u8>> {
        let (leaf_id, _) = self.find_leaf(key);
        let page = match self.provider.read_page(leaf_id) {
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

    pub fn debug_leaf_entries(&self, key: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (leaf_id, _) = self.find_leaf(key);
        let page = match self.provider.read_page(leaf_id) {
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

    pub fn range_visit(&self, start: &[u8], end: &[u8], mut f: impl FnMut(&[u8], SlotRef) -> bool) {
        let (mut leaf_id, _) = self.find_leaf(start);

        // Same high-key correction as point lookup.
        loop {
            let page = match self.provider.read_page(leaf_id) {
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
            let page = match self.provider.read_page(page_id) {
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
                if !f(key, decode_slot_ref(value)) {
                    return;
                }
                idx += 1;
            }
            current = header.next_leaf;
        }
    }

    fn find_leaf(&self, key: &[u8]) -> (PageId, Vec<PageId>) {
        let mut current = self.provider.root_page_id();
        let mut stack = Vec::new();
        loop {
            let page = self.provider.read_page(current).unwrap();
            let header = page_header(&page);
            if header.page_type == PAGE_TYPE_LEAF {
                return (current, stack);
            }
            stack.push(current);
            let mut lo = 0usize;
            let mut hi = header.key_count as usize;
            while lo < hi {
                let mid = (lo + hi) / 2;
                let mid_key = entry_key_at(&page, mid).unwrap();
                if key < mid_key {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            let child = if lo == 0 {
                header.left_child
            } else {
                let value = entry_value_at(&page, lo - 1).unwrap();
                decode_child_id(value)
            };
            current = child;
        }
    }

    fn insert_into_parent(
        &mut self,
        left_id: PageId,
        right_id: PageId,
        separator: Vec<u8>,
        mut stack: Vec<PageId>,
    ) -> Result<()> {
        if stack.is_empty() {
            let mut root = new_page(PAGE_TYPE_INTERNAL, 1);
            let mut header = page_header(&root);
            header.left_child = left_id;
            let entries = vec![(separator, encode_child_id(right_id))];
            encode_entries(&mut root, &entries, header)?;
            let root_id = self.provider.alloc_page_id();
            self.provider.write_page(root_id, root);
            self.provider.set_root_page_id(root_id);
            return Ok(());
        }

        let parent_id = stack.pop().unwrap();
        let mut parent = self.provider.read_page(parent_id).unwrap();
        let header = page_header(&parent);
        let mut pos = if header.left_child == left_id {
            0
        } else {
            usize::MAX
        };
        if pos == usize::MAX {
            let key_count = header.key_count as usize;
            for idx in 0..key_count {
                let value = entry_value_at(&parent, idx).unwrap();
                if decode_child_id(value) == left_id {
                    pos = idx + 1;
                    break;
                }
            }
            if pos == usize::MAX {
                pos = key_count;
            }
        }

        let total = header.key_count as usize + 1;
        let size = HEADER_SIZE + total * OFFSET_ENTRY_SIZE + total * entry_size(PAGE_TYPE_INTERNAL);
        if size <= PAGE_SIZE {
            let encoded = encode_child_id(right_id);
            let rebuilt = rebuild_page_with_insert(&parent, header, pos, &separator, &encoded)?;
            self.provider.write_page(parent_id, rebuilt);
            return Ok(());
        }

        let mut entries = collect_entries(&parent);
        entries.insert(pos, (separator, encode_child_id(right_id)));
        let (sep, right_page) = split_internal(&parent, &entries)?;
        let new_right_id = self.provider.alloc_page_id();
        self.provider.write_page(parent_id, right_page.0);
        self.provider.write_page(new_right_id, right_page.1);
        self.insert_into_parent(parent_id, new_right_id, sep, stack)
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
        PAGE_TYPE_LEAF => SLOT_REF_SIZE,
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

fn find_in_leaf(page: &Page, key: &[u8]) -> Option<SlotRef> {
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
                    let sr = decode_slot_ref(value);
                    eprintln!(
                        "[btree-get] equal mid={} key_hex={} slot_ref=({}, {}) value_hex={}",
                        mid,
                        hex::encode(key),
                        sr.page_id,
                        sr.slot_id,
                        hex::encode(value)
                    );
                }
                return Some(decode_slot_ref(value));
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

fn encode_slot_ref(slot_ref: SlotRef) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    buf.extend_from_slice(&slot_ref.page_id.to_le_bytes());
    buf.extend_from_slice(&slot_ref.slot_id.to_le_bytes());
    buf
}

fn decode_slot_ref(buf: &[u8]) -> SlotRef {
    let page_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let slot_id = u16::from_le_bytes(buf[8..10].try_into().unwrap());
    SlotRef { page_id, slot_id }
}

fn encode_child_id(child: PageId) -> Vec<u8> {
    child.to_le_bytes().to_vec()
}

fn decode_child_id(buf: &[u8]) -> PageId {
    u64::from_le_bytes(buf[0..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::{InMemoryPageProvider, PageBPlusTree, PageProvider, SlotRef};
    use crate::KEY_SIZE;
    use std::collections::HashMap;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicU64;

    fn key_for(i: u32) -> Vec<u8> {
        let mut key = format!("k{:03}", i).into_bytes();
        while key.len() < KEY_SIZE {
            key.push(b'x');
        }
        key.truncate(KEY_SIZE);
        key
    }

    #[test]
    fn test_insert_get_remove_len() {
        let mut tree = PageBPlusTree::new();
        let key = key_for(1);
        let slot = SlotRef {
            page_id: 1,
            slot_id: 2,
        };
        let updated = SlotRef {
            page_id: 7,
            slot_id: 9,
        };

        assert_eq!(tree.get(&key), None);
        tree.insert(key.clone(), slot).unwrap();
        assert_eq!(tree.get(&key), Some(slot));
        assert_eq!(tree.len(), 1);

        tree.insert(key.clone(), updated).unwrap();
        assert_eq!(tree.get(&key), Some(updated));
        assert_eq!(tree.len(), 1);

        tree.remove(&key).unwrap();
        assert_eq!(tree.get(&key), None);
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_range_across_splits() {
        let mut tree = PageBPlusTree::new();
        for i in 0..120u32 {
            let key = key_for(i);
            let slot = SlotRef {
                page_id: i as u64 + 1,
                slot_id: (i % 512) as u16,
            };
            tree.insert(key, slot).unwrap();
        }

        let start = key_for(10);
        let end = key_for(50);
        let range = tree.range(&start, &end);
        assert_eq!(range.len(), 41);
        assert_eq!(range.first().unwrap().0, start);
        assert_eq!(range.last().unwrap().0, end);
    }

    #[test]
    fn test_with_custom_provider() {
        let provider = InMemoryPageProvider::new();
        let mut tree = PageBPlusTree::new_with_provider(provider);

        let key = key_for(2);
        let slot = SlotRef {
            page_id: 100,
            slot_id: 5,
        };

        tree.insert(key.clone(), slot).unwrap();
        assert_eq!(tree.get(&key), Some(slot));
        assert_eq!(tree.provider().page_count(), 1);
    }

    #[test]
    fn test_provider_page_allocation() {
        let provider = InMemoryPageProvider::new();

        let p1 = provider.alloc_page_id();
        let p2 = provider.alloc_page_id();
        let p3 = provider.alloc_page_id();

        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
        assert_eq!(p3, 3);

        let page_data = vec![42u8; crate::PAGE_SIZE];
        provider.write_page(p1, page_data.clone());

        assert_eq!(provider.read_page(p1), Some(page_data));
        assert_eq!(provider.read_page(p2), None);
    }

    #[test]
    fn test_shared_page_provider() {
        use super::{PageCache, SharedPageProvider};
        use std::sync::Arc;

        let pages = Arc::new(PageCache::new(4));
        let next_page_id = Arc::new(AtomicU64::new(1));

        let provider = SharedPageProvider::new(pages.clone(), next_page_id.clone());
        let mut tree = PageBPlusTree::new_with_provider(provider);

        let key = key_for(3);
        let slot = SlotRef {
            page_id: 42,
            slot_id: 7,
        };

        tree.insert(key.clone(), slot).unwrap();
        assert_eq!(tree.get(&key), Some(slot));

        assert!(pages.len() >= 1);
    }
}
