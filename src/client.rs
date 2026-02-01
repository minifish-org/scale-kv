use crate::bptree::BPlusTree;
use crate::storage_capnp::{storage, stream as storage_stream};
use crate::{Error, Page, PageId, Result, Value, PAGE_SIZE};
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use futures::{future::try_join_all, FutureExt};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::net::TcpStream;
use tokio::task::LocalSet;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

const PAGE_HEADER_SIZE: usize = 6;
const SLOT_ENTRY_SIZE: usize = 4;
const BUCKET_SIZE: usize = 1024;


pub struct StorageClientPool {
    clients: Vec<StorageClient>,
    next: AtomicUsize,
}

impl StorageClientPool {
    pub async fn connect(addr: &str, size: usize, local: &LocalSet) -> Result<Self> {
        let size = size.max(1);
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            clients.push(StorageClient::connect(addr, local).await?);
        }
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    fn pick(&self) -> &StorageClient {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        &self.clients[index % self.clients.len()]
    }

    pub async fn get(&self, page_id: PageId) -> Result<Option<Page>> {
        self.pick().get(page_id).await
    }

    pub async fn put(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        self.pick().put(page_id, page).await
    }

    pub async fn delete(&self, page_id: PageId) -> Result<bool> {
        self.pick().delete(page_id).await
    }

    pub async fn open_stream(&self) -> Result<StreamClient> {
        self.pick().open_stream().await
    }

    pub async fn batch_put(&self, items: &[(PageId, Page)]) -> Result<()> {
        self.pick().batch_put(items).await
    }

}

pub struct BatchSender {
    client: Arc<StorageClient>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: usize,
    pending: Arc<Mutex<VecDeque<(PageId, Page)>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl BatchSender {
    /// Create a new batch sender with the given window size.
    pub fn new(client: Arc<StorageClient>, max_in_flight: usize) -> Self {
        Self {
            client,
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: max_in_flight.max(1),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Send a put request with sliding window batching.
    /// If the window is full, the request will be queued until a slot is available.
    pub async fn put(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        let slot_available = self.in_flight.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| {
                if current < self.max_in_flight {
                    Some(current + 1)
                } else {
                    None
                }
            },
        ).is_ok();

        if !slot_available {
            // Window is full, queue the request
            let mut pending = self.pending.lock().unwrap();
            pending.push_back((page_id, page.to_vec()));

            // Wait for a slot to become available
            loop {
                // Check again after acquiring the lock
                let in_flight = self.in_flight.load(Ordering::Acquire);
                if in_flight < self.max_in_flight {
                    // Slot available, take from pending instead
                    if let Some((queued_id, queued_page)) = pending.pop_front() {
                        drop(pending);
                        return self.send_and_complete(queued_id, &queued_page).await;
                    }
                    // No pending, proceed with original request
                    drop(pending);
                    self.in_flight.fetch_add(1, Ordering::Release);
                    return self.send_and_complete(page_id, page).await;
                }

                // Wait for notification
                let notify = self.notify.clone();
                drop(pending);
                notify.notified().await;
                pending = self.pending.lock().unwrap();
            }
        }

        // Window has space, send directly
        self.send_and_complete(page_id, page).await
    }

    async fn send_and_complete(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        let result = self.client.put(page_id, page).await;

        // Release the slot
        let remaining = self.in_flight.fetch_sub(1, Ordering::Release);

        // If there are pending requests and we were the one who freed a slot,
        // notify waiters (but only notify once)
        if remaining == self.max_in_flight {
            self.notify.notify_one();
        }

        result
    }

    /// Get the current number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Get the number of pending requests waiting for a slot.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

/// Batched storage pool using sliding window.
pub struct BatchedStorageClientPool {
    sender: Arc<BatchSender>,
}

impl BatchedStorageClientPool {
    /// Connect to a storage server with batching.
    pub async fn connect(addr: &str, window_size: usize, local: &LocalSet) -> Result<Self> {
        let client = Arc::new(StorageClient::connect(addr, local).await?);
        let sender = Arc::new(BatchSender::new(client, window_size));
        Ok(Self { sender })
    }

    pub async fn get(&self, page_id: PageId) -> Result<Option<Page>> {
        self.sender.client.get(page_id).await
    }

    pub async fn put(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        self.sender.put(page_id, page).await
    }

    pub async fn delete(&self, page_id: PageId) -> Result<bool> {
        self.sender.client.delete(page_id).await
    }

    pub async fn batch_put(&self, items: &[(PageId, Page)]) -> Result<()> {
        // For batch_put, we send directly without batching
        // This is useful for large bulk operations
        self.sender.client.batch_put(items).await
    }

    /// Get the number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.sender.in_flight()
    }

    /// Get the number of pending requests.
    pub fn pending(&self) -> usize {
        self.sender.pending_count()
    }
}

/// Compute node that uses batched storage for better RPC performance.
pub struct BatchedComputeNode {
    tree: BPlusTree<String>,
    index: RwLock<HashMap<String, SlotRef>>,
    page_cache: RwLock<HashMap<PageId, Page>>,
    fsm: Mutex<FreeSpaceMap>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    operations: AtomicUsize,
    storage: Option<Arc<BatchedStorageClientPool>>,
}

impl BatchedComputeNode {
    pub fn new() -> Self {
        Self {
            tree: BPlusTree::new(),
            index: RwLock::new(HashMap::new()),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: None,
        }
    }

    pub async fn with_storage(addr: &str, window_size: usize, local: &LocalSet) -> Result<Self> {
        let storage = Some(Arc::new(BatchedStorageClientPool::connect(addr, window_size, local).await?));
        Ok(Self {
            tree: BPlusTree::new(),
            index: RwLock::new(HashMap::new()),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage,
        })
    }

    pub async fn get(&self, key: impl AsRef<str>) -> Result<Option<Value>> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key = key.as_ref().to_string();
        if !self.tree.contains(&key) {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }

        let slot_ref = {
            let index = self.index.read().unwrap();
            match index.get(&key) {
                Some(slot_ref) => *slot_ref,
                None => {
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        };

        if let Some(page) = self.page_cache.read().unwrap().get(&slot_ref.page_id) {
            if let Some(value) = read_value(page, slot_ref.slot_id, key.as_bytes()) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(value));
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        let page = match &self.storage {
            Some(storage) => storage.get(slot_ref.page_id).await?,
            None => return Ok(None),
        };
        if let Some(page) = page {
            let value = read_value(&page, slot_ref.slot_id, key.as_bytes());
            self.page_cache.write().unwrap().insert(slot_ref.page_id, page);
            return Ok(value);
        }

        Ok(None)
    }

    pub async fn put(&self, key: impl AsRef<str>, value: &[u8]) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key = key.as_ref().to_string();
        let payload_len = payload_len(key.as_bytes(), value)?;
        let required = payload_len + SLOT_ENTRY_SIZE;

        let mut old_slot = None;
        {
            let mut index = self.index.write().unwrap();
            if let Some(slot_ref) = index.remove(&key) {
                old_slot = Some(slot_ref);
            }
        }
        if let Some(slot_ref) = old_slot {
            let mut page = if let Some(page) = self.page_cache.read().unwrap().get(&slot_ref.page_id) {
                Some(page.clone())
            } else {
                self.fetch_page(slot_ref.page_id).await?
            };
            if let Some(mut page) = page.take() {
                clear_slot(&mut page, slot_ref.slot_id);
                let free = page_free_space(&page);
                self.fsm.lock().unwrap().update_page(slot_ref.page_id, free);
                self.page_cache.write().unwrap().insert(slot_ref.page_id, page.clone());
        if let Some(storage) = &self.storage {
            storage.put(slot_ref.page_id, &page).await?;
        }
            }
            self.tree.remove(&key);
        }

        let (mut page_id, is_new) = self.fsm.lock().unwrap().allocate(required);
        let mut page = if let Some(page) = self.page_cache.read().unwrap().get(&page_id) {
            page.clone()
        } else if is_new {
            new_page()
        } else {
            self.fetch_page(page_id).await?.unwrap_or_else(new_page)
        };

        let mut slot_id = insert_record(&mut page, key.as_bytes(), value)?;
        if slot_id.is_none() {
            let (fresh_id, _) = self.fsm.lock().unwrap().allocate(PAGE_SIZE);
            page_id = fresh_id;
            page = new_page();
            slot_id = insert_record(&mut page, key.as_bytes(), value)?;
        }

        let slot_id = slot_id.ok_or_else(|| {
            Error::InvalidValueSize(value.len(), max_value_size_for_key(key.as_bytes()))
        })?;

        let free = page_free_space(&page);
        self.fsm.lock().unwrap().update_page(page_id, free);
        self.page_cache.write().unwrap().insert(page_id, page.clone());
        self.index
            .write()
            .unwrap()
            .insert(key.clone(), SlotRef { page_id, slot_id });
        self.tree.insert(key);

        if let Some(storage) = &self.storage {
            storage.put(page_id, &page).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, key: impl AsRef<str>) -> Result<bool> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key = key.as_ref().to_string();
        let slot_ref = {
            let mut index = self.index.write().unwrap();
            index.remove(&key)
        };

        if let Some(slot_ref) = slot_ref {
            let mut page = if let Some(page) = self.page_cache.read().unwrap().get(&slot_ref.page_id)
            {
                page.clone()
            } else {
                self.fetch_page(slot_ref.page_id)
                    .await?
                    .unwrap_or_else(new_page)
            };
            clear_slot(&mut page, slot_ref.slot_id);
            let free = page_free_space(&page);
            self.fsm.lock().unwrap().update_page(slot_ref.page_id, free);
            self.page_cache.write().unwrap().insert(slot_ref.page_id, page.clone());
            if let Some(storage) = &self.storage {
                storage.put(slot_ref.page_id, &page).await?;
            }
            self.tree.remove(&key);
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        self.put_multi(items).await
    }

    pub fn cache_hits(&self) -> usize {
        self.cache_hits.load(Ordering::Relaxed)
    }

    pub fn cache_misses(&self) -> usize {
        self.cache_misses.load(Ordering::Relaxed)
    }

    pub fn operations(&self) -> usize {
        self.operations.load(Ordering::Relaxed)
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn cache_size(&self) -> usize {
        self.page_cache.read().unwrap().len()
    }

    pub fn reset_metrics(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.operations.store(0, Ordering::Relaxed);
    }

    pub async fn get_multi(&self, keys: &[String]) -> Result<Vec<Option<Value>>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.get(key).await?);
        }
        Ok(out)
    }

    pub async fn put_multi(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        for (key, value) in items {
            self.put(key, value).await?;
        }
        Ok(())
    }

    pub async fn range(&self, start: &str, end: &str) -> Result<Vec<(String, Value)>> {
        let keys = self.tree.keys_in_range(&start.to_string(), &end.to_string());
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(keys.len());
        let mut missing_pages: HashSet<PageId> = HashSet::new();
        let mut key_slots: Vec<(String, SlotRef)> = Vec::with_capacity(keys.len());

        {
            let index = self.index.read().unwrap();
            let page_cache = self.page_cache.read().unwrap();
            for key in keys.into_iter() {
                if let Some(slot_ref) = index.get(&key).copied() {
                    if let Some(page) = page_cache.get(&slot_ref.page_id) {
                        if let Some(value) = read_value(page, slot_ref.slot_id, key.as_bytes()) {
                            out.push((key, value));
                            continue;
                        }
                    }
                    missing_pages.insert(slot_ref.page_id);
                    key_slots.push((key, slot_ref));
                }
            }
        }

        if !missing_pages.is_empty() {
            let fetches = missing_pages
                .iter()
                .map(|page_id| self.fetch_page(*page_id))
                .collect::<Vec<_>>();
            let pages = try_join_all(fetches).await?;
            {
                let mut cache = self.page_cache.write().unwrap();
                for (page_id, page) in missing_pages.iter().copied().zip(pages.into_iter()) {
                    if let Some(page) = page {
                        cache.insert(page_id, page);
                    }
                }
            }
        }

        let cache = self.page_cache.read().unwrap();
        for (key, slot_ref) in key_slots.into_iter() {
            if let Some(page) = cache.get(&slot_ref.page_id) {
                if let Some(value) = read_value(page, slot_ref.slot_id, key.as_bytes()) {
                    out.push((key, value));
                }
            }
        }

        Ok(out)
    }

    pub fn stats(&self) -> ComputeStats {
        let cache_hits = self.cache_hits();
        let cache_misses = self.cache_misses();
        let operations = self.operations();
        let cache_hit_rate = self.cache_hit_rate();
        ComputeStats {
            keys: self.tree.len(),
            pages: self.page_cache.read().unwrap().len(),
            cache_hits,
            cache_misses,
            operations,
            cache_hit_rate,
        }
    }

    pub fn in_flight(&self) -> usize {
        self.storage.as_ref().map(|s| s.in_flight()).unwrap_or(0)
    }

    pub fn pending(&self) -> usize {
        self.storage.as_ref().map(|s| s.pending()).unwrap_or(0)
    }

    async fn fetch_page(&self, page_id: PageId) -> Result<Option<Page>> {
        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Ok(None),
        };
        storage.get(page_id).await
    }
}

impl Default for BatchedComputeNode {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ComputeStats {
    pub keys: usize,
    pub pages: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub operations: usize,
    pub cache_hit_rate: f64,
}

#[derive(Clone, Copy)]
struct SlotRef {
    page_id: PageId,
    slot_id: u16,
}

struct FreeSpaceMap {
    buckets: Vec<VecDeque<PageId>>,
    free_space: HashMap<PageId, usize>,
    next_page: PageId,
}

impl FreeSpaceMap {
    fn new() -> Self {
        Self {
            buckets: vec![VecDeque::new(); bucket_count()],
            free_space: HashMap::new(),
            next_page: 1,
        }
    }

    fn allocate(&mut self, required: usize) -> (PageId, bool) {
        let start = bucket_index(required);
        for idx in start..self.buckets.len() {
            while let Some(page_id) = self.buckets[idx].pop_front() {
                if let Some(free) = self.free_space.get(&page_id) {
                    if *free >= required {
                        return (page_id, false);
                    }
                }
            }
        }

        let page_id = self.next_page;
        self.next_page += 1;
        let free = PAGE_SIZE - PAGE_HEADER_SIZE;
        self.free_space.insert(page_id, free);
        self.buckets[bucket_index(free)].push_back(page_id);
        (page_id, true)
    }

    fn update_page(&mut self, page_id: PageId, free: usize) {
        self.free_space.insert(page_id, free);
        let idx = bucket_index(free);
        self.buckets[idx].push_back(page_id);
    }
}

pub struct ComputeNode {
    tree: BPlusTree<String>,
    index: RwLock<HashMap<String, SlotRef>>,
    page_cache: RwLock<HashMap<PageId, Page>>,
    fsm: Mutex<FreeSpaceMap>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    operations: AtomicUsize,
    storage: Option<StorageClientPool>,
    batch_sender: Option<Arc<BatchSender>>,
}

impl ComputeNode {
    pub fn new() -> Self {
        Self {
            tree: BPlusTree::new(),
            index: RwLock::new(HashMap::new()),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: None,
            batch_sender: None,
        }
    }

    pub async fn with_storage(addr: &str, local: &LocalSet) -> Result<Self> {
        let storage = StorageClientPool::connect(addr, 1, local).await?;
        Ok(Self {
            tree: BPlusTree::new(),
            index: RwLock::new(HashMap::new()),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
            batch_sender: None,
        })
    }

    pub async fn with_storage_workers(addr: &str, workers: usize, local: &LocalSet) -> Result<Self> {
        let storage = StorageClientPool::connect(addr, workers, local).await?;
        Ok(Self {
            tree: BPlusTree::new(),
            index: RwLock::new(HashMap::new()),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
            batch_sender: None,
        })
    }

    pub async fn with_storage_pool(addr: &str, size: usize, local: &LocalSet) -> Result<Self> {
        Self::with_storage_workers(addr, size, local).await
    }

    pub async fn with_storage_batched(addr: &str, window_size: usize, local: &LocalSet) -> Result<Self> {
        let client = Arc::new(StorageClient::connect(addr, local).await?);
        let batch_sender = Arc::new(BatchSender::new(client, window_size));
        Ok(Self {
            tree: BPlusTree::new(),
            index: RwLock::new(HashMap::new()),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: None,
            batch_sender: Some(batch_sender),
        })
    }

    pub async fn get(&self, key: impl AsRef<str>) -> Result<Option<Value>> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key = key.as_ref().to_string();
        if !self.tree.contains(&key) {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }

        let slot_ref = {
            let index = self.index.read().unwrap();
            match index.get(&key) {
                Some(slot_ref) => *slot_ref,
                None => {
                    self.cache_misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        };

        if let Some(page) = self.page_cache.read().unwrap().get(&slot_ref.page_id) {
            if let Some(value) = read_value(page, slot_ref.slot_id, key.as_bytes()) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(value));
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        let page = if let Some(batch_sender) = &self.batch_sender {
            batch_sender.client.get(slot_ref.page_id).await?
        } else if let Some(storage) = &self.storage {
            storage.get(slot_ref.page_id).await?
        } else {
            return Ok(None);
        };
        if let Some(page) = page {
            let value = read_value(&page, slot_ref.slot_id, key.as_bytes());
            self.page_cache
                .write()
                .unwrap()
                .insert(slot_ref.page_id, page);
            return Ok(value);
        }

        Ok(None)
    }

    pub async fn put(&self, key: impl AsRef<str>, value: &[u8]) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key = key.as_ref().to_string();
        let payload_len = payload_len(key.as_bytes(), value)?;
        let required = payload_len + SLOT_ENTRY_SIZE;

        let mut old_slot = None;
        {
            let mut index = self.index.write().unwrap();
            if let Some(slot_ref) = index.remove(&key) {
                old_slot = Some(slot_ref);
            }
        }
        if let Some(slot_ref) = old_slot {
            let mut page = if let Some(page) = self.page_cache.read().unwrap().get(&slot_ref.page_id) {
                Some(page.clone())
            } else {
                self.fetch_page(slot_ref.page_id).await?
            };
            if let Some(mut page) = page.take() {
                clear_slot(&mut page, slot_ref.slot_id);
                let free = page_free_space(&page);
                self.fsm.lock().unwrap().update_page(slot_ref.page_id, free);
                self.page_cache
                    .write()
                    .unwrap()
                    .insert(slot_ref.page_id, page.clone());
                if let Some(batch_sender) = &self.batch_sender {
                    batch_sender.put(slot_ref.page_id, &page).await?;
                } else if let Some(storage) = &self.storage {
                    storage.put(slot_ref.page_id, &page).await?;
                }
            }
            self.tree.remove(&key);
        }

        let (mut page_id, is_new) = self.fsm.lock().unwrap().allocate(required);
        let mut page = if let Some(page) = self.page_cache.read().unwrap().get(&page_id) {
            page.clone()
        } else if is_new {
            new_page()
        } else {
            self.fetch_page(page_id).await?.unwrap_or_else(new_page)
        };

        let mut slot_id = insert_record(&mut page, key.as_bytes(), value)?;
        if slot_id.is_none() {
            let (fresh_id, _) = self.fsm.lock().unwrap().allocate(PAGE_SIZE);
            page_id = fresh_id;
            page = new_page();
            slot_id = insert_record(&mut page, key.as_bytes(), value)?;
        }

        let slot_id = slot_id.ok_or_else(|| {
            Error::InvalidValueSize(value.len(), max_value_size_for_key(key.as_bytes()))
        })?;

        let free = page_free_space(&page);
        self.fsm.lock().unwrap().update_page(page_id, free);
        self.page_cache.write().unwrap().insert(page_id, page.clone());
        self.index
            .write()
            .unwrap()
            .insert(key.clone(), SlotRef { page_id, slot_id });
        self.tree.insert(key);

        if let Some(batch_sender) = &self.batch_sender {
            batch_sender.put(page_id, &page).await?;
        } else if let Some(storage) = &self.storage {
            storage.put(page_id, &page).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, key: impl AsRef<str>) -> Result<bool> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key = key.as_ref().to_string();
        let slot_ref = {
            let mut index = self.index.write().unwrap();
            index.remove(&key)
        };

        if let Some(slot_ref) = slot_ref {
            let mut page = if let Some(page) = self.page_cache.read().unwrap().get(&slot_ref.page_id)
            {
                page.clone()
            } else {
                self.fetch_page(slot_ref.page_id)
                    .await?
                    .unwrap_or_else(new_page)
            };
            clear_slot(&mut page, slot_ref.slot_id);
            let free = page_free_space(&page);
            self.fsm.lock().unwrap().update_page(slot_ref.page_id, free);
            self.page_cache
                .write()
                .unwrap()
                .insert(slot_ref.page_id, page.clone());
            if let Some(batch_sender) = &self.batch_sender {
                batch_sender.put(slot_ref.page_id, &page).await?;
            } else if let Some(storage) = &self.storage {
                storage.put(slot_ref.page_id, &page).await?;
            }
            self.tree.remove(&key);
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        self.put_multi(items).await
    }

    pub fn cache_hits(&self) -> usize {
        self.cache_hits.load(Ordering::Relaxed)
    }

    pub fn cache_misses(&self) -> usize {
        self.cache_misses.load(Ordering::Relaxed)
    }

    pub fn operations(&self) -> usize {
        self.operations.load(Ordering::Relaxed)
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn cache_size(&self) -> usize {
        self.page_cache.read().unwrap().len()
    }

    pub fn reset_metrics(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.operations.store(0, Ordering::Relaxed);
    }


    pub async fn open_stream(&self) -> Result<StreamClient> {
        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Err(crate::Error::Capnp("no storage configured".to_string())),
        };
        storage.open_stream().await
    }

    pub fn exists(&self, key: impl AsRef<str>) -> bool {
        let key = key.as_ref().to_string();
        self.tree.contains(&key)
    }

    pub async fn get_multi(&self, keys: &[String]) -> Result<Vec<Option<Value>>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.get(key).await?);
        }
        Ok(out)
    }

    pub async fn put_multi(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        for (key, value) in items {
            self.put(key, value).await?;
        }
        Ok(())
    }

    pub async fn range(&self, start: &str, end: &str) -> Result<Vec<(String, Value)>> {
        let keys = self.tree.keys_in_range(&start.to_string(), &end.to_string());
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(keys.len());
        let mut missing_pages: HashSet<PageId> = HashSet::new();
        let mut key_slots: Vec<(String, SlotRef)> = Vec::with_capacity(keys.len());

        {
            let index = self.index.read().unwrap();
            let page_cache = self.page_cache.read().unwrap();
            for key in keys.into_iter() {
                if let Some(slot_ref) = index.get(&key).copied() {
                    if let Some(page) = page_cache.get(&slot_ref.page_id) {
                        if let Some(value) = read_value(page, slot_ref.slot_id, key.as_bytes()) {
                            out.push((key, value));
                            continue;
                        }
                    }
                    missing_pages.insert(slot_ref.page_id);
                    key_slots.push((key, slot_ref));
                }
            }
        }

        if !missing_pages.is_empty() {
            let fetches = missing_pages
                .iter()
                .map(|page_id| self.fetch_page(*page_id))
                .collect::<Vec<_>>();
            let pages = try_join_all(fetches).await?;
            {
                let mut cache = self.page_cache.write().unwrap();
                for (page_id, page) in missing_pages.iter().copied().zip(pages.into_iter()) {
                    if let Some(page) = page {
                        cache.insert(page_id, page);
                    }
                }
            }
        }

        let cache = self.page_cache.read().unwrap();
        for (key, slot_ref) in key_slots.into_iter() {
            if let Some(page) = cache.get(&slot_ref.page_id) {
                if let Some(value) = read_value(page, slot_ref.slot_id, key.as_bytes()) {
                    out.push((key, value));
                }
            }
        }

        Ok(out)
    }

    pub fn stats(&self) -> ComputeStats {
        let cache_hits = self.cache_hits();
        let cache_misses = self.cache_misses();
        let operations = self.operations();
        let cache_hit_rate = self.cache_hit_rate();
        ComputeStats {
            keys: self.tree.len(),
            pages: self.page_cache.read().unwrap().len(),
            cache_hits,
            cache_misses,
            operations,
            cache_hit_rate,
        }
    }

    async fn fetch_page(&self, page_id: PageId) -> Result<Option<Page>> {
        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Ok(None),
        };
        storage.get(page_id).await
    }

}

impl Default for ComputeNode {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StorageClient {
    client: storage::Client,
    _task: tokio::task::JoinHandle<()>,
}

impl StorageClient {
    pub async fn connect(addr: &str, local: &LocalSet) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let reader = reader.compat();
        let writer = writer.compat_write();

        let network = VatNetwork::new(reader, writer, Side::Client, Default::default());
        let mut rpc_system = RpcSystem::new(Box::new(network), None);
        let client: storage::Client = rpc_system.bootstrap(Side::Server);
        let task = local.spawn_local(rpc_system.map(|_| ()));

        Ok(Self {
            client,
            _task: task,
        })
    }

    pub async fn get(&self, page_id: PageId) -> Result<Option<Page>> {
        let mut request = self.client.get_request();
        request.get().set_key(page_id);
        let response = request.send().promise.await?;
        let response = response.get()?;
        if response.get_found() {
            Ok(Some(response.get_value()?.to_vec()))
        } else {
            Ok(None)
        }
    }

    pub async fn put(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        let mut request = self.client.put_request();
        let mut params = request.get();
        params.set_key(page_id);
        params.set_value(page);
        request.send().promise.await?;
        Ok(())
    }

    pub async fn delete(&self, page_id: PageId) -> Result<bool> {
        let mut request = self.client.delete_request();
        request.get().set_key(page_id);
        let response = request.send().promise.await?;
        Ok(response.get()?.get_found())
    }

    pub async fn open_stream(&self) -> Result<StreamClient> {
        let request = self.client.stream_request();
        let response = request.send().promise.await?;
        let stream = response.get()?.get_stream()?;
        Ok(StreamClient { stream })
    }

    pub async fn batch_put(&self, items: &[(PageId, Page)]) -> Result<()> {
        let mut request = self.client.batch_put_request();
        let params = request.get();
        let mut list = params.init_items(items.len() as u32);
        for (index, (page_id, page)) in items.iter().enumerate() {
            let mut slot = list.reborrow().get(index as u32);
            slot.set_key(*page_id);
            slot.set_value(page);
        }
        request.send().promise.await?;
        Ok(())
    }
}


pub struct StreamClient {
    stream: storage_stream::Client,
}

impl StreamClient {
    pub async fn next(&self, max: u32) -> Result<(Vec<(PageId, Page)>, bool)> {
        let mut request = self.stream.next_request();
        request.get().set_max(max);
        let response = request.send().promise.await?;
        let response = response.get()?;
        let items = response.get_items()?;
        let mut out = Vec::with_capacity(items.len() as usize);
        for item in items.iter() {
            let page_id = item.get_key();
            let value = item.get_value()?.to_vec();
            out.push((page_id, value));
        }
        Ok((out, response.get_done()))
    }
}

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

fn max_value_size_for_key(key: &[u8]) -> usize {
    PAGE_SIZE
        .saturating_sub(PAGE_HEADER_SIZE)
        .saturating_sub(SLOT_ENTRY_SIZE)
        .saturating_sub(4)
        .saturating_sub(key.len())
}

fn payload_len(key: &[u8], value: &[u8]) -> Result<usize> {
    if key.len() > u16::MAX as usize {
        return Err(Error::InvalidValueSize(key.len(), u16::MAX as usize));
    }
    if value.len() > max_value_size_for_key(key) {
        return Err(Error::InvalidValueSize(value.len(), max_value_size_for_key(key)));
    }
    Ok(4 + key.len() + value.len())
}

fn new_page() -> Page {
    let mut page = vec![0u8; PAGE_SIZE];
    write_header(&mut page, 0, PAGE_HEADER_SIZE as u16, PAGE_SIZE as u16);
    page
}

fn read_header(page: &[u8]) -> (u16, u16, u16) {
    let slots = read_u16(page, 0);
    let free_start = read_u16(page, 2);
    let free_end = read_u16(page, 4);
    (slots, free_start, free_end)
}

fn write_header(page: &mut [u8], slots: u16, free_start: u16, free_end: u16) {
    write_u16(page, 0, slots);
    write_u16(page, 2, free_start);
    write_u16(page, 4, free_end);
}

fn read_slot(page: &[u8], slot_id: u16) -> (u16, u16) {
    let offset = slot_offset(slot_id);
    let pos = read_u16(page, offset);
    let len = read_u16(page, offset + 2);
    (pos, len)
}

fn write_slot(page: &mut [u8], slot_id: u16, offset: u16, len: u16) {
    let pos = slot_offset(slot_id);
    write_u16(page, pos, offset);
    write_u16(page, pos + 2, len);
}

fn slot_offset(slot_id: u16) -> usize {
    PAGE_HEADER_SIZE + SLOT_ENTRY_SIZE * slot_id as usize
}

fn page_free_space(page: &[u8]) -> usize {
    let (_, free_start, free_end) = read_header(page);
    free_end.saturating_sub(free_start) as usize
}

fn find_free_slot(page: &[u8], slots: u16) -> Option<u16> {
    for slot_id in 0..slots {
        let (_, len) = read_slot(page, slot_id);
        if len == 0 {
            return Some(slot_id);
        }
    }
    None
}

fn insert_record(page: &mut [u8], key: &[u8], value: &[u8]) -> Result<Option<u16>> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    let (mut slots, mut free_start, mut free_end) = read_header(page);
    let payload_len = payload_len(key, value)?;

    let free_slot = find_free_slot(page, slots);
    let mut needed = payload_len;
    if free_slot.is_none() {
        needed += SLOT_ENTRY_SIZE;
    }
    let free_bytes = free_end.saturating_sub(free_start) as usize;
    if free_bytes < needed {
        if slots > 0 {
            defragment_page(page);
            let (s, fs, fe) = read_header(page);
            slots = s;
            free_start = fs;
            free_end = fe;
        } else {
            return Ok(None);
        }
    }

    let free_bytes = free_end.saturating_sub(free_start) as usize;
    if free_bytes < needed {
        return Ok(None);
    }

    let slot_id = free_slot.unwrap_or(slots);
    if free_slot.is_none() {
        free_start = free_start.saturating_add(SLOT_ENTRY_SIZE as u16);
        slots = slots.saturating_add(1);
    }

    let payload_offset = (free_end as usize).saturating_sub(payload_len) as u16;
    write_u16(page, payload_offset as usize, key.len() as u16);
    write_u16(page, payload_offset as usize + 2, value.len() as u16);
    let mut cursor = payload_offset as usize + 4;
    page[cursor..cursor + key.len()].copy_from_slice(key);
    cursor += key.len();
    page[cursor..cursor + value.len()].copy_from_slice(value);

    write_slot(page, slot_id, payload_offset, payload_len as u16);
    free_end = payload_offset;
    write_header(page, slots, free_start, free_end);
    Ok(Some(slot_id))
}

fn clear_slot(page: &mut [u8], slot_id: u16) {
    if page.len() != PAGE_SIZE {
        return;
    }
    write_slot(page, slot_id, 0, 0);
}

fn defragment_page(page: &mut [u8]) {
    let (slots, _, _) = read_header(page);
    let mut entries: Vec<(u16, u16, u16)> = Vec::new();
    for slot_id in 0..slots {
        let (offset, len) = read_slot(page, slot_id);
        if len != 0 {
            entries.push((slot_id, offset, len));
        }
    }
    entries.sort_by_key(|(_, offset, _)| *offset);

    let mut new_free_end = PAGE_SIZE as u16;
    for (slot_id, offset, len) in entries.into_iter().rev() {
        let new_offset = new_free_end.saturating_sub(len);
        if offset != new_offset {
            let src_start = offset as usize;
            let src_end = src_start + len as usize;
            let dst_start = new_offset as usize;
            let _dst_end = dst_start + len as usize;
            page.copy_within(src_start..src_end, dst_start);
        }
        write_slot(page, slot_id, new_offset, len);
        new_free_end = new_offset;
    }

    let new_free_start = PAGE_HEADER_SIZE as u16 + slots * SLOT_ENTRY_SIZE as u16;
    write_header(page, slots, new_free_start, new_free_end);
}

fn read_value(page: &[u8], slot_id: u16, key: &[u8]) -> Option<Value> {
    if page.len() != PAGE_SIZE {
        return None;
    }
    let (offset, len) = read_slot(page, slot_id);
    if len == 0 {
        return None;
    }
    let end = offset as usize + len as usize;
    if end > PAGE_SIZE || offset as usize + 4 > PAGE_SIZE {
        return None;
    }
    let key_len = read_u16(page, offset as usize) as usize;
    let val_len = read_u16(page, offset as usize + 2) as usize;
    let payload_start = offset as usize + 4;
    let key_end = payload_start + key_len;
    let value_end = key_end + val_len;
    if value_end > PAGE_SIZE {
        return None;
    }
    if page[payload_start..key_end] != *key {
        return None;
    }
    Some(page[key_end..value_end].to_vec())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defragment_page_reclaims_space() {
        let mut page = new_page();
        let key1 = b"k1";
        let key2 = b"k2";
        let value = vec![b'x'; 4000];

        let slot1 = insert_record(&mut page, key1, &value).unwrap().unwrap();
        let _slot2 = insert_record(&mut page, key2, &value).unwrap().unwrap();
        clear_slot(&mut page, slot1);

        let (_slots_before, free_start_before, free_end_before) = read_header(&page);
        let free_bytes_before = free_end_before.saturating_sub(free_start_before) as usize;

        let large_value = vec![b'y'; 9000];
        let needed = payload_len(b"k3", &large_value).unwrap();
        assert!(free_bytes_before < needed);

        let slot3 = insert_record(&mut page, b"k3", &large_value)
            .unwrap()
            .unwrap();
        assert!(read_value(&page, slot3, b"k3").is_some());
    }
}
