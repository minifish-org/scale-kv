use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::{KEY_SIZE, VALUE_SIZE, Value};

const WAL_MAGIC: u32 = 0x534B5657; // "SKVW"
const WAL_HEADER_LEN: usize = 12; // magic + len + crc32
const SNAPSHOT_MAGIC: u32 = 0x53534E50; // "SSNP"
const SNAPSHOT_VERSION: u32 = 1;

pub type Key = [u8; KEY_SIZE];

#[derive(Debug, thiserror::Error)]
pub enum TxnError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid key size: {0} (expected {1})")]
    InvalidKeySize(usize, usize),
    #[error("invalid value size: {0} (max {1})")]
    InvalidValueSize(usize, usize),
    #[error("write-write conflict")]
    WriteWriteConflict,
    #[error("corrupt wal: {0}")]
    CorruptWal(String),
}

pub type Result<T> = std::result::Result<T, TxnError>;

#[derive(Debug, Clone)]
struct Version {
    commit_ts: u64,
    value: Option<Value>,
}

#[derive(Debug)]
struct TxnManagerInner {
    store: RwLock<BTreeMap<Key, Vec<Version>>>,
    commit_ts: AtomicU64,
    commit_lock: Mutex<()>,
    wal: Mutex<File>,
    wal_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct TxnManager {
    inner: Arc<TxnManagerInner>,
}

#[derive(Debug)]
pub struct Txn {
    inner: Arc<TxnManagerInner>,
    read_ts: u64,
    write_set: HashMap<Key, Option<Value>>,
    read_only: bool,
}

impl TxnManager {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let snapshot = load_snapshot(&snapshot_path(&path))?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let (base_store, base_ts) = snapshot.unwrap_or_default();
        let (store, max_ts, valid_len) = replay_wal_with_base(&mut file, base_store, base_ts)?;
        let file_len = file.metadata()?.len();
        if valid_len < file_len {
            file.set_len(valid_len)?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            inner: Arc::new(TxnManagerInner {
                store: RwLock::new(store),
                commit_ts: AtomicU64::new(max_ts),
                commit_lock: Mutex::new(()),
                wal: Mutex::new(file),
                wal_path: path,
            }),
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _guard = self.inner.commit_lock.lock().unwrap();
        let commit_ts = self.inner.commit_ts.load(Ordering::SeqCst);
        let store = self.inner.store.read().unwrap();
        write_snapshot(&snapshot_path(&self.inner.wal_path), commit_ts, &store)?;

        let mut wal = self.inner.wal.lock().unwrap();
        wal.set_len(0)?;
        wal.seek(SeekFrom::Start(0))?;
        wal.sync_all()?;
        Ok(())
    }

    pub fn begin_ro(&self) -> Txn {
        Txn {
            inner: Arc::clone(&self.inner),
            read_ts: self.inner.commit_ts.load(Ordering::SeqCst),
            write_set: HashMap::new(),
            read_only: true,
        }
    }

    pub fn begin_rw(&self) -> Txn {
        Txn {
            inner: Arc::clone(&self.inner),
            read_ts: self.inner.commit_ts.load(Ordering::SeqCst),
            write_set: HashMap::new(),
            read_only: false,
        }
    }
}

impl Txn {
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        let key = parse_key(key)?;
        if let Some(entry) = self.write_set.get(&key) {
            return Ok(entry.clone());
        }
        let store = self.inner.store.read().unwrap();
        Ok(read_at_ts(&store, &key, self.read_ts))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = parse_key(key)?;
        if value.len() > VALUE_SIZE {
            return Err(TxnError::InvalidValueSize(value.len(), VALUE_SIZE));
        }
        self.write_set.insert(key, Some(value.to_vec()));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let key = parse_key(key)?;
        self.write_set.insert(key, None);
        Ok(())
    }

    pub fn scan(
        &self,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
            .map(|(key, value)| (key.to_vec(), value))
            .collect())
    }

    pub fn commit(self) -> Result<u64> {
        if self.read_only || self.write_set.is_empty() {
            return Ok(self.read_ts);
        }
        let _guard = self.inner.commit_lock.lock().unwrap();
        {
            let store = self.inner.store.read().unwrap();
            for key in self.write_set.keys() {
                if let Some(versions) = store.get(key) {
                    if let Some(last) = versions.last() {
                        if last.commit_ts > self.read_ts {
                            return Err(TxnError::WriteWriteConflict);
                        }
                    }
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

        {
            let mut wal = self.inner.wal.lock().unwrap();
            wal.write_all(&record)?;
            wal.sync_all()?;
        }

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

fn read_at_ts(
    store: &BTreeMap<Key, Vec<Version>>,
    key: &Key,
    read_ts: u64,
) -> Option<Value> {
    let versions = store.get(key)?;
    versions
        .iter()
        .rfind(|v| v.commit_ts <= read_ts)
        .and_then(|v| v.value.clone())
}

fn encode_payload(commit_ts: u64, writes: &HashMap<Key, Option<Value>>) -> Result<Vec<u8>> {
    if writes.len() > u32::MAX as usize {
        return Err(TxnError::CorruptWal("too many ops".to_string()));
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(&commit_ts.to_le_bytes());
    buf.extend_from_slice(&(writes.len() as u32).to_le_bytes());
    for (key, value) in writes {
        buf.extend_from_slice(key);
        match value {
            Some(v) => {
                if v.len() > VALUE_SIZE {
                    return Err(TxnError::InvalidValueSize(v.len(), VALUE_SIZE));
                }
                buf.push(1);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => {
                buf.push(0);
            }
        }
    }
    Ok(buf)
}

fn replay_wal_with_base(
    file: &mut File,
    mut store: BTreeMap<Key, Vec<Version>>,
    base_ts: u64,
) -> Result<(BTreeMap<Key, Vec<Version>>, u64, u64)> {
    let mut data = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut data)?;

    let mut max_ts = base_ts;
    let mut offset = 0usize;
    let mut last_good = 0usize;

    while offset + WAL_HEADER_LEN <= data.len() {
        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if magic != WAL_MAGIC {
            break;
        }
        let len = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[offset + 8..offset + 12].try_into().unwrap());
        let payload_start = offset + WAL_HEADER_LEN;
        let payload_end = payload_start + len;
        if payload_end > data.len() {
            break;
        }
        let payload = &data[payload_start..payload_end];
        if crc32fast::hash(payload) != crc {
            break;
        }
        let decoded = decode_payload(payload);
        let (commit_ts, ops) = match decoded {
            Ok(value) => value,
            Err(_) => break,
        };
        if commit_ts > base_ts {
            for (key, value) in ops {
                store
                    .entry(key)
                    .or_default()
                    .push(Version { commit_ts, value });
            }
            if commit_ts > max_ts {
                max_ts = commit_ts;
            }
        }
        offset = payload_end;
        last_good = offset;
    }

    Ok((store, max_ts, last_good as u64))
}

fn snapshot_path(wal_path: &Path) -> PathBuf {
    let mut os = wal_path.as_os_str().to_os_string();
    os.push(".snapshot");
    PathBuf::from(os)
}

fn load_snapshot(path: &Path) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    if data.len() < 4 + 4 + 8 + 4 {
        return Err(TxnError::CorruptWal("snapshot too short".to_string()));
    }
    let mut cursor = 0usize;
    let magic = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    if magic != SNAPSHOT_MAGIC {
        return Err(TxnError::CorruptWal("invalid snapshot magic".to_string()));
    }
    let version = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    if version != SNAPSHOT_VERSION {
        return Err(TxnError::CorruptWal(
            "unsupported snapshot version".to_string(),
        ));
    }
    let commit_ts = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let num_entries = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;

    let mut store: BTreeMap<Key, Vec<Version>> = BTreeMap::new();
    for _ in 0..num_entries {
        if cursor + KEY_SIZE + 4 > data.len() {
            return Err(TxnError::CorruptWal("snapshot truncated".to_string()));
        }
        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(&data[cursor..cursor + KEY_SIZE]);
        cursor += KEY_SIZE;
        let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if len > VALUE_SIZE {
            return Err(TxnError::InvalidValueSize(len, VALUE_SIZE));
        }
        if cursor + len > data.len() {
            return Err(TxnError::CorruptWal("snapshot truncated".to_string()));
        }
        let value = data[cursor..cursor + len].to_vec();
        cursor += len;
        store
            .entry(key)
            .or_default()
            .push(Version {
                commit_ts,
                value: Some(value),
            });
    }

    if cursor != data.len() {
        return Err(TxnError::CorruptWal(
            "snapshot length mismatch".to_string(),
        ));
    }

    Ok(Some((store, commit_ts)))
}

fn write_snapshot(
    path: &Path,
    commit_ts: u64,
    store: &BTreeMap<Key, Vec<Version>>,
) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buf.extend_from_slice(&commit_ts.to_le_bytes());

    let mut entries = Vec::new();
    for (key, versions) in store {
        if let Some(version) = versions.last() {
            if let Some(value) = &version.value {
                entries.push((*key, value.as_slice()));
            }
        }
    }

    if entries.len() > u32::MAX as usize {
        return Err(TxnError::CorruptWal("too many snapshot entries".to_string()));
    }
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Snapshot stores only the latest committed value per key. This is safe because
    // after recovery all new transactions begin after the snapshot commit_ts.
    for (key, value) in entries {
        if value.len() > VALUE_SIZE {
            return Err(TxnError::InvalidValueSize(value.len(), VALUE_SIZE));
        }
        buf.extend_from_slice(&key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
    }

    let tmp_path = path.with_extension("snapshot.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn decode_payload(payload: &[u8]) -> Result<(u64, Vec<(Key, Option<Value>)>)> {
    if payload.len() < 12 {
        return Err(TxnError::CorruptWal("payload too short".to_string()));
    }
    let mut cursor = 0usize;
    let commit_ts = u64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let num_ops = u32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;

    let mut ops = Vec::with_capacity(num_ops);
    for _ in 0..num_ops {
        if cursor + KEY_SIZE + 1 > payload.len() {
            return Err(TxnError::CorruptWal("payload truncated".to_string()));
        }
        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(&payload[cursor..cursor + KEY_SIZE]);
        cursor += KEY_SIZE;
        let op = payload[cursor];
        cursor += 1;
        match op {
            0 => ops.push((key, None)),
            1 => {
                if cursor + 4 > payload.len() {
                    return Err(TxnError::CorruptWal("payload truncated".to_string()));
                }
                let len = u32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap())
                    as usize;
                cursor += 4;
                if len > VALUE_SIZE {
                    return Err(TxnError::InvalidValueSize(len, VALUE_SIZE));
                }
                if cursor + len > payload.len() {
                    return Err(TxnError::CorruptWal("payload truncated".to_string()));
                }
                let value = payload[cursor..cursor + len].to_vec();
                cursor += len;
                ops.push((key, Some(value)));
            }
            _ => return Err(TxnError::CorruptWal("unknown op".to_string())),
        }
    }

    if cursor != payload.len() {
        return Err(TxnError::CorruptWal("payload length mismatch".to_string()));
    }

    Ok((commit_ts, ops))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn key(byte: u8) -> [u8; KEY_SIZE] {
        let mut k = [0u8; KEY_SIZE];
        k[0] = byte;
        k
    }

    #[test]
    fn test_wal_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let manager = TxnManager::open(&path).unwrap();
        let mut tx = manager.begin_rw();
        tx.put(&key(1), b"v1").unwrap();
        tx.delete(&key(2)).unwrap();
        let ts = tx.commit().unwrap();
        assert!(ts > 0);
        drop(manager);

        let manager = TxnManager::open(&path).unwrap();
        let tx = manager.begin_ro();
        assert_eq!(tx.get(&key(1)).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(tx.get(&key(2)).unwrap(), None);
    }
}
