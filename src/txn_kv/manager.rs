use super::{Key, Result, TxnError, TxnStorage, Version};
use crate::{KEY_SIZE, VALUE_SIZE, Value};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::codec::{WAL_HEADER_LEN, WAL_MAGIC, encode_payload};
use super::storage::QuorumStorage;

#[derive(Debug)]
struct TxnManagerInner {
    store: RwLock<BTreeMap<Key, Vec<Version>>>,
    commit_ts: AtomicU64,
    commit_lock: tokio::sync::Mutex<()>,
    storage: Arc<dyn TxnStorage>,
}

#[derive(Clone, Debug)]
pub struct TxnManager {
    inner: Arc<TxnManagerInner>,
}

#[derive(Debug)]
pub struct Txn {
    inner: Arc<TxnManagerInner>,
    read_ts: u64,
    started_at: Instant,
    timeout: Option<Duration>,
    write_set: HashMap<Key, Option<Value>>,
    read_only: bool,
}

impl TxnManager {
    pub async fn open_quorum(replica_paths: Vec<PathBuf>, quorum: usize) -> Result<Self> {
        let storage = QuorumStorage::open(replica_paths, quorum)?;
        Self::open_with_storage(storage).await
    }

    pub async fn open_with_storage(storage: impl TxnStorage + 'static) -> Result<Self> {
        let snapshot = storage.load_snapshot().await?;
        let (base_store, base_ts) = snapshot.unwrap_or_default();
        let (store, max_ts) = storage.replay_wal(base_store, base_ts).await?;
        Ok(Self {
            inner: Arc::new(TxnManagerInner {
                store: RwLock::new(store),
                commit_ts: AtomicU64::new(max_ts),
                commit_lock: tokio::sync::Mutex::new(()),
                storage: Arc::new(storage),
            }),
        })
    }

    pub async fn checkpoint(&self) -> Result<()> {
        let _guard = self.inner.commit_lock.lock().await;
        let commit_ts = self.inner.commit_ts.load(Ordering::SeqCst);
        let snapshot = {
            let store = self.inner.store.read().unwrap();
            store.clone()
        };
        self.inner
            .storage
            .write_snapshot(commit_ts, &snapshot)
            .await?;
        self.inner.storage.truncate_wal().await?;
        Ok(())
    }

    pub fn begin_ro_timeout(&self, timeout: Duration) -> Txn {
        self.begin_tx(true, Some(timeout))
    }

    pub fn begin_rw_timeout(&self, timeout: Duration) -> Txn {
        self.begin_tx(false, Some(timeout))
    }

    fn begin_tx(&self, read_only: bool, timeout: Option<Duration>) -> Txn {
        Txn {
            inner: Arc::clone(&self.inner),
            read_ts: self.inner.commit_ts.load(Ordering::SeqCst),
            started_at: Instant::now(),
            timeout,
            write_set: HashMap::new(),
            read_only,
        }
    }
}

impl Txn {
    fn ensure_not_timed_out(&self) -> Result<()> {
        if let Some(timeout) = self.timeout
            && self.started_at.elapsed() > timeout
        {
            return Err(TxnError::TxnTimeout);
        }
        Ok(())
    }

    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_not_timed_out()?;
        let key = parse_key(key)?;
        if let Some(entry) = self.write_set.get(&key) {
            return Ok(entry.clone().map(Bytes::from));
        }
        let store = self.inner.store.read().unwrap();
        Ok(read_at_ts(&store, &key, self.read_ts))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_not_timed_out()?;
        let key = parse_key(key)?;
        if value.len() > VALUE_SIZE {
            return Err(TxnError::InvalidValueSize(value.len(), VALUE_SIZE));
        }
        self.write_set.insert(key, Some(value.to_vec()));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.ensure_not_timed_out()?;
        let key = parse_key(key)?;
        self.write_set.insert(key, None);
        Ok(())
    }

    pub fn scan(
        &self,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.ensure_not_timed_out()?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start = parse_key(start_inclusive)?;
        let end = parse_key(end_exclusive)?;
        if start == end {
            return Ok(Vec::new());
        }

        let mut merged: BTreeMap<Key, Value> = BTreeMap::new();
        {
            let store = self.inner.store.read().unwrap();
            for (key, versions) in store.range(start..end) {
                if let Some(value) = versions
                    .iter()
                    .rfind(|version| version.commit_ts <= self.read_ts)
                    .and_then(|version| version.value.clone())
                {
                    merged.insert(*key, value);
                }
            }
        }

        for (key, value) in &self.write_set {
            if *key >= start && *key < end {
                match value {
                    Some(value) => {
                        merged.insert(*key, value.clone());
                    }
                    None => {
                        merged.remove(key);
                    }
                }
            }
        }

        Ok(merged
            .into_iter()
            .take(limit)
            .map(|(key, value)| (key.to_vec(), Bytes::from(value)))
            .collect())
    }

    pub async fn commit(self) -> Result<u64> {
        self.ensure_not_timed_out()?;
        if self.read_only || self.write_set.is_empty() {
            return Ok(self.read_ts);
        }
        let _guard = self.inner.commit_lock.lock().await;
        {
            let store = self.inner.store.read().unwrap();
            for key in self.write_set.keys() {
                if let Some(versions) = store.get(key)
                    && let Some(last) = versions.last()
                    && last.commit_ts > self.read_ts
                {
                    return Err(TxnError::WriteWriteConflict);
                }
            }
        }

        let commit_ts = self.inner.commit_ts.fetch_add(1, Ordering::SeqCst) + 1;
        let payload = encode_payload(commit_ts, &self.write_set)?;
        let crc = crc32fast::hash(&payload);
        let mut record = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
        record.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        record.extend_from_slice(&crc.to_le_bytes());
        record.extend_from_slice(&payload);

        self.inner.storage.append_wal_record(&record).await?;

        {
            let mut store = self.inner.store.write().unwrap();
            for (key, value) in self.write_set {
                store
                    .entry(key)
                    .or_default()
                    .push(Version { commit_ts, value });
            }
        }

        Ok(commit_ts)
    }
}

fn parse_key(key: &[u8]) -> Result<Key> {
    if key.len() != KEY_SIZE {
        return Err(TxnError::InvalidKeySize(key.len(), KEY_SIZE));
    }
    let mut out = [0u8; KEY_SIZE];
    out.copy_from_slice(key);
    Ok(out)
}

fn read_at_ts(store: &BTreeMap<Key, Vec<Version>>, key: &Key, read_ts: u64) -> Option<Bytes> {
    let versions = store.get(key)?;
    versions
        .iter()
        .rfind(|v| v.commit_ts <= read_ts)
        .and_then(|v| v.value.clone().map(Bytes::from))
}
