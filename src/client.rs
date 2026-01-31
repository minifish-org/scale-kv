use crate::bptree::BPlusTree;
use crate::storage_capnp::{storage, stream as storage_stream};
use crate::{Error, Page, PageId, Result, PAGE_SIZE};
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use futures::FutureExt;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};


pub struct StorageClientPool {
    clients: Vec<StorageClient>,
    next: AtomicUsize,
}

impl StorageClientPool {
    pub async fn connect(addr: &str, size: usize) -> Result<Self> {
        let size = size.max(1);
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            clients.push(StorageClient::connect(addr).await?);
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

pub struct ComputeNode {
    tree: BPlusTree,
    page_cache: RwLock<HashMap<PageId, Page>>,
    fsm: Mutex<FreeSpaceMap>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    operations: AtomicUsize,
    storage: Option<StorageClientPool>,
}

struct FreeSpaceMap {
    buckets: Vec<VecDeque<PageId>>,
    next_page: PageId,
}

impl FreeSpaceMap {
    fn new() -> Self {
        Self {
            buckets: vec![VecDeque::new()],
            next_page: 1,
        }
    }

    fn allocate(&mut self) -> PageId {
        if let Some(bucket) = self.buckets.get_mut(0) {
            if let Some(id) = bucket.pop_front() {
                return id;
            }
        }
        let id = self.next_page;
        self.next_page += 1;
        id
    }

    fn free(&mut self, page_id: PageId) {
        if let Some(bucket) = self.buckets.get_mut(0) {
            bucket.push_back(page_id);
        }
    }
}

impl ComputeNode {
    pub fn new() -> Self {
        Self {
            tree: BPlusTree::new(),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: None,
        }
    }

    pub async fn with_storage(addr: &str) -> Result<Self> {
        let storage = StorageClientPool::connect(addr, 1).await?;
        Ok(Self {
            tree: BPlusTree::new(),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
        })
    }

    pub async fn with_storage_workers(addr: &str, workers: usize) -> Result<Self> {
        let storage = StorageClientPool::connect(addr, workers).await?;
        Ok(Self {
            tree: BPlusTree::new(),
            page_cache: RwLock::new(HashMap::new()),
            fsm: Mutex::new(FreeSpaceMap::new()),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
        })
    }

    pub async fn with_storage_pool(addr: &str, size: usize) -> Result<Self> {
        Self::with_storage_workers(addr, size).await
    }

    pub async fn get(&self, page_id: PageId) -> Result<Option<Page>> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let in_tree = self.tree.contains(page_id);
        if in_tree {
            if let Some(page) = self.page_cache.read().unwrap().get(&page_id) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(page.clone()));
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Ok(None),
        };

        let page = storage.get(page_id).await?;
        if let Some(page) = page {
            self.tree.insert(page_id);
            self.page_cache.write().unwrap().insert(page_id, page.clone());
            return Ok(Some(page));
        }
        Ok(None)
    }

    pub async fn put(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let page = ensure_page(page)?;
        self.tree.insert(page_id);
        self.page_cache.write().unwrap().insert(page_id, page.clone());

        if let Some(storage) = &self.storage {
            storage.put(page_id, &page).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, page_id: PageId) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        self.tree.remove(page_id);
        self.page_cache.write().unwrap().remove(&page_id);

        if let Some(storage) = &self.storage {
            storage.delete(page_id).await?;
        }

        Ok(())
    }

    pub async fn batch_put(&self, items: &[(PageId, Page)]) -> Result<()> {
        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Err(crate::Error::Capnp("no storage configured".to_string())),
        };

        let mut pages = Vec::with_capacity(items.len());
        for (page_id, page) in items {
            pages.push((*page_id, ensure_page(page)?));
        }

        storage.batch_put(&pages).await?;

        let mut cache = self.page_cache.write().unwrap();
        for (page_id, page) in pages {
            self.tree.insert(page_id);
            cache.insert(page_id, page);
        }

        Ok(())
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

    pub fn allocate_page_id(&self) -> PageId {
        self.fsm.lock().unwrap().allocate()
    }

    pub fn free_page_id(&self, page_id: PageId) {
        self.fsm.lock().unwrap().free(page_id);
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
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let reader = reader.compat();
        let writer = writer.compat_write();

        let network = VatNetwork::new(reader, writer, Side::Client, Default::default());
        let mut rpc_system = RpcSystem::new(Box::new(network), None);
        let client: storage::Client = rpc_system.bootstrap(Side::Server);
        let task = tokio::task::spawn_local(rpc_system.map(|_| ()));

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

fn ensure_page(page: &[u8]) -> Result<Page> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    Ok(page.to_vec())
}
