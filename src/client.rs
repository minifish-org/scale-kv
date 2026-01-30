use crate::storage_capnp::storage;
use crate::{Result, Value};
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use dashmap::DashMap;
use futures::FutureExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub struct ComputeNode {
    cache: DashMap<String, Value>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    operations: AtomicUsize,
    storage: Option<StorageClient>,
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
        let storage = StorageClient::connect(addr).await?;
        Ok(Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage: Some(storage),
        })
    }

    pub async fn get(&self, key: impl AsRef<str>) -> Result<Option<Value>> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_str = key.as_ref().to_string();

        if let Some(value) = self.cache.get(key_str.as_str()) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(value.clone()));
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Ok(None),
        };

        let value = storage.get(&key_str).await?;
        if let Some(value) = value.clone() {
            self.cache.insert(key_str, value.clone());
        }
        Ok(value)
    }

    pub async fn put(&self, key: impl AsRef<str>, value: &[u8]) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_str = key.as_ref().to_string();
        let value_vec = value.to_vec();

        self.cache.insert(key_str.clone(), value_vec);

        if let Some(storage) = &self.storage {
            storage.put(&key_str, value).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_str = key.as_ref().to_string();

        self.cache.remove(&key_str);

        if let Some(storage) = &self.storage {
            storage.delete(&key_str).await?;
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

        Ok(Self { client, _task: task })
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
}
