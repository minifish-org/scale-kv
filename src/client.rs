use crate::storage_capnp::{storage, stream as storage_stream};
use crate::{Result, Value};
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use dashmap::DashMap;
use futures::FutureExt;
use std::sync::atomic::{AtomicUsize, Ordering};
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

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.pick().get(key).await
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.pick().put(key, value).await
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.pick().delete(key).await
    }

    pub async fn open_stream(&self) -> Result<StreamClient> {
        self.pick().open_stream().await
    }

    pub async fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        self.pick().batch_put(items).await
    }

}

pub struct ComputeNode {
    cache: DashMap<String, Value>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    operations: AtomicUsize,
    storage: Option<StorageClientPool>,
}

impl ComputeNode {
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: None,
        }
    }

    pub async fn with_storage(addr: &str) -> Result<Self> {
        let storage = StorageClientPool::connect(addr, 1).await?;
        Ok(Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
        })
    }

    pub async fn with_storage_workers(addr: &str, workers: usize) -> Result<Self> {
        let storage = StorageClientPool::connect(addr, workers).await?;
        Ok(Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
        })
    }

    pub async fn with_storage_pool(addr: &str, size: usize) -> Result<Self> {
        Self::with_storage_workers(addr, size).await
    }

    pub async fn get(&self, key: impl AsRef<str>) -> Result<Option<Value>> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_ref = key.as_ref();

        if let Some(value) = self.cache.get(key_ref) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(value.clone()));
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Ok(None),
        };

        let value = storage.get(key_ref).await?;
        if let Some(value) = value {
            let key_string = key_ref.to_string();
            self.cache.insert(key_string, value.clone());
            return Ok(Some(value));
        }
        Ok(None)
    }

    pub async fn put(&self, key: impl AsRef<str>, value: &[u8]) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_ref = key.as_ref();
        let key_string = key_ref.to_string();
        let value_vec = value.to_vec();

        self.cache.insert(key_string, value_vec);

        if let Some(storage) = &self.storage {
            storage.put(key_ref, value).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_ref = key.as_ref();

        self.cache.remove(key_ref);

        if let Some(storage) = &self.storage {
            storage.delete(key_ref).await?;
        }

        Ok(())
    }

    pub async fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Err(crate::Error::Capnp("no storage configured".to_string())),
        };

        storage.batch_put(items).await?;

        for (key, value) in items {
            self.cache.insert(key.clone(), value.clone());
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
        self.cache.len()
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

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut request = self.client.get_request();
        request.get().set_key(key.into());
        let response = request.send().promise.await?;
        let response = response.get()?;
        if response.get_found() {
            Ok(Some(response.get_value()?.to_vec()))
        } else {
            Ok(None)
        }
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut request = self.client.put_request();
        let mut params = request.get();
        params.set_key(key.into());
        params.set_value(value);
        request.send().promise.await?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut request = self.client.delete_request();
        request.get().set_key(key.into());
        let response = request.send().promise.await?;
        Ok(response.get()?.get_found())
    }

    pub async fn open_stream(&self) -> Result<StreamClient> {
        let request = self.client.stream_request();
        let response = request.send().promise.await?;
        let stream = response.get()?.get_stream()?;
        Ok(StreamClient { stream })
    }

    pub async fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<()> {
        let mut request = self.client.batch_put_request();
        let params = request.get();
        let mut list = params.init_items(items.len() as u32);
        for (index, (key, value)) in items.iter().enumerate() {
            let mut slot = list.reborrow().get(index as u32);
            slot.set_key(key.as_str().into());
            slot.set_value(value);
        }
        request.send().promise.await?;
        Ok(())
    }
}


pub struct StreamClient {
    stream: storage_stream::Client,
}

impl StreamClient {
    pub async fn next(&self, max: u32) -> Result<(Vec<(String, Vec<u8>)>, bool)> {
        let mut request = self.stream.next_request();
        request.get().set_max(max);
        let response = request.send().promise.await?;
        let response = response.get()?;
        let items = response.get_items()?;
        let mut out = Vec::with_capacity(items.len() as usize);
        for item in items.iter() {
            let key = item.get_key()?.to_str().map_err(|err| {
                crate::Error::Capnp(err.to_string())
            })?;
            let value = item.get_value()?.to_vec();
            out.push((key.to_string(), value));
        }
        Ok((out, response.get_done()))
    }
}
