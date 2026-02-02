use crate::{Error, Page, PageId, Result, PAGE_SIZE};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

const PAGE_TYPE_INTERNAL: u8 = 1;
const PAGE_TYPE_LEAF: u8 = 2;

const HEADER_SIZE: usize = 1 + 1 + 2 + 8 + 8; // type, level, key_count, next_leaf, left_child

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

/// Shared page provider that wraps Arc<RwLock<HashMap>>.
/// Used by ComputeNode to share page_cache between B+Tree and data pages.
pub struct SharedPageProvider {
    pages: Arc<RwLock<HashMap<PageId, Page>>>,
    next_page_id: Arc<AtomicU64>,
    root: AtomicU64,
}

impl SharedPageProvider {
    pub fn new(pages: Arc<RwLock<HashMap<PageId, Page>>>, next_page_id: Arc<AtomicU64>) -> Self {
        Self {
            pages,
            next_page_id,
            root: AtomicU64::new(0),
        }
    }

    pub fn page_count(&self) -> usize {
        self.pages.read().unwrap().len()
    }
}

impl PageProvider for SharedPageProvider {
    fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.pages.read().unwrap().get(&page_id).cloned()
    }

    fn write_page(&self, page_id: PageId, page: Page) {
        self.pages.write().unwrap().insert(page_id, page);
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
        let (leaf_id, _) = self.find_leaf(key);
        let page = self.provider.read_page(leaf_id)?;
        let entries = decode_entries(&page);
        for (k, v) in entries {
            if k == key {
                return Some(decode_slot_ref(&v));
            }
        }
        None
    }

    pub fn insert(&mut self, key: Vec<u8>, slot_ref: SlotRef) -> Result<()> {
        let (leaf_id, stack) = self.find_leaf(&key);
        let mut leaf = self.provider.read_page(leaf_id).unwrap();
        let mut entries = decode_entries(&leaf);
        match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key.as_slice())) {
            Ok(pos) => entries[pos].1 = encode_slot_ref(slot_ref),
            Err(pos) => {
                entries.insert(pos, (key.clone(), encode_slot_ref(slot_ref)));
                self.len += 1;
            }
        }

        if fits_in_page(&entries) {
            let header = leaf_page_header(&leaf);
            encode_entries(&mut leaf, &entries, header)?;
            self.provider.write_page(leaf_id, leaf);
            return Ok(());
        }

        let (separator, right_page) = split_leaf(&leaf, &entries)?;
        let right_id = self.provider.alloc_page_id();
        let mut left_page = right_page.0;
        let right_page_buf = right_page.1;
        let mut left_header = leaf_page_header(&left_page);
        left_header.next_leaf = Some(right_id);
        write_header(&mut left_page, left_header);
        self.provider.write_page(leaf_id, left_page);
        self.provider.write_page(right_id, right_page_buf);
        self.insert_into_parent(leaf_id, right_id, separator, stack)
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let (leaf_id, _) = self.find_leaf(key);
        let mut leaf = self.provider.read_page(leaf_id).unwrap();
        let mut entries = decode_entries(&leaf);
        if let Ok(pos) = entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            entries.remove(pos);
            self.len = self.len.saturating_sub(1);
            let header = leaf_page_header(&leaf);
            encode_entries(&mut leaf, &entries, header)?;
            self.provider.write_page(leaf_id, leaf);
        }
        Ok(())
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, SlotRef)> {
        let (leaf_id, _) = self.find_leaf(start);
        let mut out = Vec::new();
        let mut current = Some(leaf_id);
        while let Some(page_id) = current {
            let page = match self.provider.read_page(page_id) {
                Some(page) => page,
                None => break,
            };
            let header = leaf_page_header(&page);
            let entries = decode_entries(&page);
            for (k, v) in entries {
                if k.as_slice() < start {
                    continue;
                }
                if k.as_slice() > end {
                    return out;
                }
                out.push((k, decode_slot_ref(&v)));
            }
            current = header.next_leaf;
        }
        out
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
            let entries = decode_entries(&page);
            let mut child = header.left_child;
            for (k, v) in entries {
                if key < k.as_slice() {
                    break;
                }
                child = decode_child_id(&v);
            }
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
        let mut entries = decode_entries(&parent);
        let pos = entries
            .iter()
            .position(|(_, v)| decode_child_id(v) == left_id)
            .map(|p| p + 1)
            .unwrap_or_else(|| {
                if page_header(&parent).left_child == left_id {
                    0
                } else {
                    entries.len()
                }
            });
        entries.insert(pos, (separator.clone(), encode_child_id(right_id)));

        if fits_in_page(&entries) {
            let header = page_header(&parent);
            encode_entries(&mut parent, &entries, header)?;
            self.provider.write_page(parent_id, parent);
            return Ok(());
        }

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
    next_leaf: Option<PageId>,
    left_child: PageId,
}

fn page_header(page: &Page) -> PageHeader {
    let page_type = page[0];
    let level = page[1];
    let key_count = u16::from_le_bytes([page[2], page[3]]);
    let next_leaf = u64::from_le_bytes(page[4..12].try_into().unwrap());
    let left_child = u64::from_le_bytes(page[12..20].try_into().unwrap());
    PageHeader {
        page_type,
        level,
        key_count,
        next_leaf: if next_leaf == 0 {
            None
        } else {
            Some(next_leaf)
        },
        left_child,
    }
}

fn leaf_page_header(page: &Page) -> PageHeader {
    page_header(page)
}

fn write_header(page: &mut Page, header: PageHeader) {
    page[0] = header.page_type;
    page[1] = header.level;
    page[2..4].copy_from_slice(&header.key_count.to_le_bytes());
    let next_leaf = header.next_leaf.unwrap_or(0);
    page[4..12].copy_from_slice(&next_leaf.to_le_bytes());
    page[12..20].copy_from_slice(&header.left_child.to_le_bytes());
}

fn new_page(page_type: u8, level: u8) -> Page {
    let mut page = vec![0u8; PAGE_SIZE];
    let header = PageHeader {
        page_type,
        level,
        key_count: 0,
        next_leaf: None,
        left_child: 0,
    };
    write_header(&mut page, header);
    page
}

fn decode_entries(page: &Page) -> Vec<(Vec<u8>, Vec<u8>)> {
    let header = page_header(page);
    let mut entries = Vec::with_capacity(header.key_count as usize);
    let mut cursor = HEADER_SIZE;
    for _ in 0..header.key_count {
        let key_len = u16::from_le_bytes([page[cursor], page[cursor + 1]]) as usize;
        let val_len = u16::from_le_bytes([page[cursor + 2], page[cursor + 3]]) as usize;
        cursor += 4;
        let key = page[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let val = page[cursor..cursor + val_len].to_vec();
        cursor += val_len;
        entries.push((key, val));
    }
    entries
}

fn encode_entries(
    page: &mut Page,
    entries: &[(Vec<u8>, Vec<u8>)],
    mut header: PageHeader,
) -> Result<()> {
    let mut cursor = HEADER_SIZE;
    for (k, v) in entries {
        let key_len = k.len() as u16;
        let val_len = v.len() as u16;
        if cursor + 4 + k.len() + v.len() > PAGE_SIZE {
            return Err(Error::InvalidPageSize(cursor, PAGE_SIZE));
        }
        page[cursor..cursor + 2].copy_from_slice(&key_len.to_le_bytes());
        page[cursor + 2..cursor + 4].copy_from_slice(&val_len.to_le_bytes());
        cursor += 4;
        page[cursor..cursor + k.len()].copy_from_slice(k);
        cursor += k.len();
        page[cursor..cursor + v.len()].copy_from_slice(v);
        cursor += v.len();
    }
    header.key_count = entries.len() as u16;
    write_header(page, header);
    Ok(())
}

fn fits_in_page(entries: &[(Vec<u8>, Vec<u8>)]) -> bool {
    let mut size = HEADER_SIZE;
    for (k, v) in entries {
        size += 4 + k.len() + v.len();
    }
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
    right_header.next_leaf = left_header.next_leaf;
    left_header.next_leaf = None;
    encode_entries(&mut left, left_entries, left_header)?;
    encode_entries(&mut right, right_entries, right_header)?;
    Ok((right_entries[0].0.clone(), (left, right)))
}

fn split_internal(page: &Page, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(Vec<u8>, (Page, Page))> {
    let mid = entries.len() / 2;
    let separator = entries[mid].0.clone();
    let left_entries = &entries[..mid];
    let right_entries = &entries[mid + 1..];
    let mut left = new_page(PAGE_TYPE_INTERNAL, page_header(page).level);
    let mut right = new_page(PAGE_TYPE_INTERNAL, page_header(page).level);
    let left_header = page_header(page);
    let mut right_header = page_header(page);
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
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::RwLock;

    fn key_for(i: u32) -> Vec<u8> {
        let mut key = format!("k{:03}", i).into_bytes();
        key.extend_from_slice(&vec![b'x'; 100]);
        key
    }

    #[test]
    fn test_insert_get_remove_len() {
        let mut tree = PageBPlusTree::new();
        let key = b"alpha".to_vec();
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

        let key = b"test_key".to_vec();
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
        use super::SharedPageProvider;
        use std::sync::Arc;

        let pages = Arc::new(RwLock::new(HashMap::new()));
        let next_page_id = Arc::new(AtomicU64::new(1));

        let provider = SharedPageProvider::new(pages.clone(), next_page_id.clone());
        let mut tree = PageBPlusTree::new_with_provider(provider);

        let key = b"shared_key".to_vec();
        let slot = SlotRef {
            page_id: 42,
            slot_id: 7,
        };

        tree.insert(key.clone(), slot).unwrap();
        assert_eq!(tree.get(&key), Some(slot));

        assert!(pages.read().unwrap().len() >= 1);
    }
}
