use crate::Value;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::{Op, Request, Response, Result};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Base delay between retries.
    pub base_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Whether to retry on connection errors.
    pub retry_connection_errors: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            retry_connection_errors: true,
        }
    }
}

/// Compute node - client with local cache that talks to storage node.
pub struct ComputeNode {
    cache: DashMap<String, Value>,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    operations: AtomicUsize,
    storage_addr: Option<String>,
    retry_config: RetryConfig,
}

impl ComputeNode {
    /// Create a compute node without storage connection.
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage_addr: None,
            retry_config: RetryConfig::default(),
        }
    }

    /// Create a compute node connected to a storage node.
    pub fn with_storage(addr: &str) -> Self {
        Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage_addr: Some(addr.to_string()),
            retry_config: RetryConfig::default(),
        }
    }

    /// Create a compute node with custom retry configuration.
    pub fn with_config(addr: &str, retry_config: RetryConfig) -> Self {
        Self {
            cache: DashMap::new(),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            storage_addr: Some(addr.to_string()),
            retry_config,
        }
    }

    /// Get a value from cache or storage.
    pub fn get(&self, key: impl AsRef<str>) -> Option<Value> {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_str = key.as_ref().to_string();

        // Check cache first
        if let Some(v) = self.cache.get(key_str.as_str()) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Some(v.clone());
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // If connected to storage, fetch from there
        if let Some(ref addr) = self.storage_addr {
            if let Ok(mut client) = StorageClient::connect(addr) {
                if let Ok(Some(value)) = client.get(&key_str) {
                    self.cache.insert(key_str.clone(), value.clone());
                    return Some(value);
                }
            }
        }

        None
    }

    /// Put a value to storage and cache.
    pub fn put(&mut self, key: impl AsRef<str>, value: &[u8]) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_str = key.as_ref().to_string();
        let value_bytes = value.to_vec();

        // Write to cache
        self.cache.insert(key_str.clone(), value_bytes.clone());

        // If connected to storage, write there too
        if let Some(ref addr) = self.storage_addr {
            if let Ok(mut client) = StorageClient::connect(addr) {
                let _ = client.put(&key_str, value);
            }
        }
    }

    /// Delete a value from storage and cache.
    pub fn delete(&mut self, key: impl AsRef<str>) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        let key_str = key.as_ref().to_string();

        // Remove from cache
        self.cache.remove(&key_str);

        // If connected to storage, delete there too
        if let Some(ref addr) = self.storage_addr {
            if let Ok(mut client) = StorageClient::connect(addr) {
                let _ = client.delete(&key_str);
            }
        }
    }

    /// Get cache hit count.
    pub fn cache_hits(&self) -> usize {
        self.cache_hits.load(Ordering::Relaxed)
    }

    /// Get cache miss count.
    pub fn cache_misses(&self) -> usize {
        self.cache_misses.load(Ordering::Relaxed)
    }

    /// Get total operation count.
    pub fn operations(&self) -> usize {
        self.operations.load(Ordering::Relaxed)
    }

    /// Get cache hit rate as a percentage (0.0 to 1.0).
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

    /// Get current cache size.
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Reset all metrics.
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

/// Client for communicating with storage node.
pub struct StorageClient {
    stream: TcpStream,
    retry_config: RetryConfig,
}

impl StorageClient {
    /// Connect to a storage node with default retry config.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Self::connect_with_config(addr, RetryConfig::default())
    }

    /// Connect to a storage node with custom retry config.
    pub fn connect_with_config<A: ToSocketAddrs>(
        addr: A,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| crate::Error::Connection(format!("failed to connect: {}", e)))?;
        Ok(Self {
            stream,
            retry_config,
        })
    }

    /// Calculate delay for exponential backoff.
    fn calculate_delay(attempt: u32, base_delay: Duration, max_delay: Duration) -> Duration {
        let multiplier = 2_u32.saturating_pow(attempt.min(31));
        let delay = base_delay * multiplier;
        if delay > max_delay {
            max_delay
        } else {
            delay
        }
    }

    /// Send a request with retry logic.
    fn send_request_with_retry(&mut self, request: &Request) -> Result<Response> {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            match self.send_request_inner(request) {
                Ok(response) => return Ok(response),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.retry_config.max_retries {
                        let delay = Self::calculate_delay(
                            attempt,
                            self.retry_config.base_delay,
                            self.retry_config.max_delay,
                        );
                        std::thread::sleep(delay);
                    }
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| crate::Error::Connection("unknown error after retries".to_string())))
    }

    /// Send a request with retry logic.
    fn send_request(&mut self, request: &Request) -> Result<Response> {
        self.send_request_with_retry(request)
    }

    /// Inner send request without retry.
    fn send_request_inner(&mut self, request: &Request) -> Result<Response> {
        // Serialize request
        let data = bincode::encode_to_vec(request, bincode::config::standard())
            .map_err(|e| crate::Error::Connection(format!("encode failed: {}", e)))?;

        // Send length prefix
        let len = data.len() as u32;
        self.stream.write_all(&len.to_le_bytes())?;
        self.stream.write_all(&data)?;
        self.stream.flush()?;

        // Read response
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; resp_len];
        self.stream.read_exact(&mut resp_buf)?;

        let response: Response = bincode::decode_from_slice(&resp_buf, bincode::config::standard())
            .map_err(|e| crate::Error::Connection(format!("decode failed: {}", e)))?
            .0;

        Ok(response)
    }

    /// Get a value.
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let request = Request {
            op: Op::Get,
            key: key.to_string(),
            value: None,
        };

        let response = self.send_request(&request)?;

        if response.found {
            Ok(response.value.map(|v| v.to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Put a key-value pair.
    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let request = Request {
            op: Op::Put,
            key: key.to_string(),
            value: Some(value.to_vec()),
        };

        self.send_request(&request)?;
        Ok(())
    }

    /// Delete a key.
    pub fn delete(&mut self, key: &str) -> Result<()> {
        let request = Request {
            op: Op::Delete,
            key: key.to_string(),
            value: None,
        };

        self.send_request(&request)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_node() {
        let node = ComputeNode::new();
        assert_eq!(node.cache_size(), 0);
        assert_eq!(node.cache_hits(), 0);
        assert_eq!(node.cache_misses(), 0);
    }

    #[test]
    fn test_local_cache() {
        let mut node = ComputeNode::new();
        node.put("foo", b"bar");
        assert_eq!(node.get("foo"), Some(b"bar".to_vec()));
        assert_eq!(node.cache_size(), 1);
    }

    #[test]
    fn test_local_delete() {
        let mut node = ComputeNode::new();
        node.put("foo", b"bar");
        node.delete("foo");
        assert_eq!(node.get("foo"), None);
        assert_eq!(node.cache_size(), 0);
    }
}
