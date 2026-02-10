use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

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
    #[error("quorum not met: required {required}, succeeded {succeeded}")]
    QuorumNotMet { required: usize, succeeded: usize },
    #[error("invalid quorum {quorum} for {replicas} replicas")]
    InvalidQuorum { quorum: usize, replicas: usize },
    #[error("transaction timed out")]
    TxnTimeout,
}

pub type Result<T> = std::result::Result<T, TxnError>;

pub trait TxnStorage: Send + Sync + std::fmt::Debug {
    fn load_snapshot(&self) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>>;
    fn write_snapshot(&self, commit_ts: u64, store: &BTreeMap<Key, Vec<Version>>) -> Result<()>;
    fn replay_wal(
        &self,
        base_store: BTreeMap<Key, Vec<Version>>,
        base_ts: u64,
    ) -> Result<(BTreeMap<Key, Vec<Version>>, u64)>;
    fn append_wal_record(&self, record: &[u8]) -> Result<()>;
    fn truncate_wal(&self) -> Result<()>;
}

#[derive(Debug)]
struct LocalFileStorage {
    wal: Mutex<File>,
    wal_path: PathBuf,
}

impl LocalFileStorage {
    fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            wal: Mutex::new(file),
            wal_path: path,
        })
    }
}

impl TxnStorage for LocalFileStorage {
    fn load_snapshot(&self) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
        load_snapshot_file(&snapshot_path(&self.wal_path))
    }

    fn write_snapshot(&self, commit_ts: u64, store: &BTreeMap<Key, Vec<Version>>) -> Result<()> {
        write_snapshot_file(&snapshot_path(&self.wal_path), commit_ts, store)
    }

    fn replay_wal(
        &self,
        base_store: BTreeMap<Key, Vec<Version>>,
        base_ts: u64,
    ) -> Result<(BTreeMap<Key, Vec<Version>>, u64)> {
        let mut wal = self.wal.lock().unwrap();
        let (store, max_ts, valid_len) = replay_wal_with_base(&mut wal, base_store, base_ts)?;
        let file_len = wal.metadata()?.len();
        if valid_len < file_len {
            wal.set_len(valid_len)?;
        }
        wal.seek(SeekFrom::End(0))?;
        Ok((store, max_ts))
    }

    fn append_wal_record(&self, record: &[u8]) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        wal.write_all(record)?;
        wal.sync_all()?;
        Ok(())
    }

    fn truncate_wal(&self) -> Result<()> {
        let mut wal = self.wal.lock().unwrap();
        wal.set_len(0)?;
        wal.seek(SeekFrom::Start(0))?;
        wal.sync_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct QuorumReplica {
    wal_path: PathBuf,
    storage: Mutex<Option<LocalFileStorage>>,
}

#[derive(Clone, Debug)]
enum QuorumOp {
    Append {
        record: Arc<Vec<u8>>,
    },
    Snapshot {
        commit_ts: u64,
        store: Arc<BTreeMap<Key, Vec<Version>>>,
    },
    Truncate,
}

impl QuorumOp {
    fn apply(&self, storage: &LocalFileStorage) -> Result<()> {
        match self {
            QuorumOp::Append { record } => storage.append_wal_record(record.as_slice()),
            QuorumOp::Snapshot { commit_ts, store } => storage.write_snapshot(*commit_ts, store),
            QuorumOp::Truncate => storage.truncate_wal(),
        }
    }
}

#[derive(Debug)]
struct RepairTask {
    replica_index: usize,
    op: QuorumOp,
    attempt: u32,
}

#[derive(Debug)]
pub struct QuorumStorage {
    replicas: Arc<Vec<QuorumReplica>>,
    quorum: usize,
    repair_tx: mpsc::Sender<RepairTask>,
}

impl QuorumStorage {
    pub fn open(paths: Vec<PathBuf>, quorum: usize) -> Result<Self> {
        if quorum < 1 || quorum > paths.len() {
            return Err(TxnError::InvalidQuorum {
                quorum,
                replicas: paths.len(),
            });
        }
        let replicas = paths
            .into_iter()
            .map(|path| QuorumReplica {
                wal_path: path.join("wal.log"),
                storage: Mutex::new(None),
            })
            .collect::<Vec<_>>();
        let replicas = Arc::new(replicas);
        let (repair_tx, repair_rx) = mpsc::channel();
        let worker_replicas = Arc::clone(&replicas);
        std::thread::spawn(move || repair_worker(worker_replicas, repair_rx));
        Ok(Self {
            replicas,
            quorum,
            repair_tx,
        })
    }

    fn with_replica<F, T>(&self, index: usize, f: F) -> Result<T>
    where
        F: FnOnce(&LocalFileStorage) -> Result<T>,
    {
        with_replica(&self.replicas, index, f)
    }

    fn quorum_write(&self, op: QuorumOp) -> Result<()> {
        let (tx, rx) = mpsc::channel();
        let replicas = Arc::clone(&self.replicas);
        for index in 0..replicas.len() {
            let tx = tx.clone();
            let replicas = Arc::clone(&replicas);
            let op = op.clone();
            let repair_tx = self.repair_tx.clone();
            std::thread::spawn(move || {
                let result = with_replica(&replicas, index, |storage| op.apply(storage));
                if result.is_err() {
                    let _ = repair_tx.send(RepairTask {
                        replica_index: index,
                        op: op.clone(),
                        attempt: 1,
                    });
                }
                let _ = tx.send(result);
            });
        }
        drop(tx);
        let mut successes = 0usize;
        let mut last_err: Option<TxnError> = None;
        for result in rx {
            match result {
                Ok(()) => {
                    successes += 1;
                    if successes >= self.quorum {
                        return Ok(());
                    }
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or(TxnError::QuorumNotMet {
            required: self.quorum,
            succeeded: successes,
        }))
    }
}

fn with_replica<F, T>(replicas: &Arc<Vec<QuorumReplica>>, index: usize, f: F) -> Result<T>
where
    F: FnOnce(&LocalFileStorage) -> Result<T>,
{
    let replica = &replicas[index];
    let mut guard = replica.storage.lock().unwrap();
    if guard.is_none() {
        if let Some(parent) = replica.wal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        *guard = Some(LocalFileStorage::open(&replica.wal_path)?);
    }
    f(guard.as_ref().unwrap())
}

fn repair_worker(replicas: Arc<Vec<QuorumReplica>>, repair_rx: mpsc::Receiver<RepairTask>) {
    const REPAIR_MAX_ATTEMPTS: u32 = 5;
    const REPAIR_BASE_DELAY_MS: u64 = 50;
    while let Ok(mut task) = repair_rx.recv() {
        loop {
            if with_replica(&replicas, task.replica_index, |storage| {
                task.op.apply(storage)
            })
            .is_ok()
            {
                break;
            }
            if task.attempt >= REPAIR_MAX_ATTEMPTS {
                break;
            }
            let backoff = REPAIR_BASE_DELAY_MS.saturating_mul(1u64 << (task.attempt - 1));
            std::thread::sleep(Duration::from_millis(backoff));
            task.attempt += 1;
        }
    }
}

impl TxnStorage for QuorumStorage {
    fn load_snapshot(&self) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
        let mut best: Option<(BTreeMap<Key, Vec<Version>>, u64)> = None;
        let mut saw_ok = false;
        let mut last_err: Option<TxnError> = None;
        for index in 0..self.replicas.len() {
            match self.with_replica(index, |storage| storage.load_snapshot()) {
                Ok(snapshot) => {
                    saw_ok = true;
                    if let Some((store, ts)) = snapshot {
                        let replace = match &best {
                            Some((_, best_ts)) => ts > *best_ts,
                            None => true,
                        };
                        if replace {
                            best = Some((store, ts));
                        }
                    }
                }
                Err(err) => last_err = Some(err),
            }
        }
        if best.is_some() {
            return Ok(best);
        }
        if saw_ok {
            Ok(None)
        } else {
            Err(last_err.unwrap_or(TxnError::QuorumNotMet {
                required: self.quorum,
                succeeded: 0,
            }))
        }
    }

    fn write_snapshot(&self, commit_ts: u64, store: &BTreeMap<Key, Vec<Version>>) -> Result<()> {
        let op = QuorumOp::Snapshot {
            commit_ts,
            store: Arc::new(store.clone()),
        };
        self.quorum_write(op)
    }

    fn replay_wal(
        &self,
        _base_store: BTreeMap<Key, Vec<Version>>,
        _base_ts: u64,
    ) -> Result<(BTreeMap<Key, Vec<Version>>, u64)> {
        // Best-effort recovery: for each replica, replay WAL after its own snapshot,
        // then pick the replica that yields the highest max_ts.
        let mut best: Option<(BTreeMap<Key, Vec<Version>>, u64)> = None;
        let mut last_err: Option<TxnError> = None;
        for index in 0..self.replicas.len() {
            let result = self.with_replica(index, |storage| {
                let snapshot = storage.load_snapshot()?;
                let (base_store, base_ts) = snapshot.unwrap_or_default();
                storage.replay_wal(base_store, base_ts)
            });
            match result {
                Ok((store, max_ts)) => {
                    let replace = match &best {
                        Some((_, best_ts)) => max_ts > *best_ts,
                        None => true,
                    };
                    if replace {
                        best = Some((store, max_ts));
                    }
                }
                Err(err) => last_err = Some(err),
            }
        }
        best.ok_or_else(|| {
            last_err.unwrap_or(TxnError::QuorumNotMet {
                required: self.quorum,
                succeeded: 0,
            })
        })
    }

    fn append_wal_record(&self, record: &[u8]) -> Result<()> {
        let op = QuorumOp::Append {
            record: Arc::new(record.to_vec()),
        };
        self.quorum_write(op)
    }

    fn truncate_wal(&self) -> Result<()> {
        self.quorum_write(QuorumOp::Truncate)
    }
}

#[derive(Debug, Clone)]
pub struct Version {
    commit_ts: u64,
    value: Option<Value>,
}

#[derive(Debug)]
struct TxnManagerInner {
    store: RwLock<BTreeMap<Key, Vec<Version>>>,
    commit_ts: AtomicU64,
    commit_lock: Mutex<()>,
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let storage = LocalFileStorage::open(path)?;
        Self::open_with_storage(storage)
    }

    pub fn open_quorum(replica_paths: Vec<PathBuf>, quorum: usize) -> Result<Self> {
        let storage = QuorumStorage::open(replica_paths, quorum)?;
        Self::open_with_storage(storage)
    }

    pub fn open_with_storage(storage: impl TxnStorage + Send + Sync + 'static) -> Result<Self> {
        let snapshot = storage.load_snapshot()?;
        let (base_store, base_ts) = snapshot.unwrap_or_default();
        let (store, max_ts) = storage.replay_wal(base_store, base_ts)?;
        Ok(Self {
            inner: Arc::new(TxnManagerInner {
                store: RwLock::new(store),
                commit_ts: AtomicU64::new(max_ts),
                commit_lock: Mutex::new(()),
                storage: Arc::new(storage),
            }),
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _guard = self.inner.commit_lock.lock().unwrap();
        let commit_ts = self.inner.commit_ts.load(Ordering::SeqCst);
        let store = self.inner.store.read().unwrap();
        self.inner.storage.write_snapshot(commit_ts, &store)?;
        self.inner.storage.truncate_wal()?;
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
        if let Some(timeout) = self.timeout {
            if self.started_at.elapsed() > timeout {
                return Err(TxnError::TxnTimeout);
            }
        }
        Ok(())
    }

    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.ensure_not_timed_out()?;
        let key = parse_key(key)?;
        if let Some(entry) = self.write_set.get(&key) {
            return Ok(entry.clone());
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
            .map(|(key, value)| (key.to_vec(), value))
            .collect())
    }

    pub fn commit(self) -> Result<u64> {
        self.ensure_not_timed_out()?;
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

        self.inner.storage.append_wal_record(&record)?;

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

fn read_at_ts(store: &BTreeMap<Key, Vec<Version>>, key: &Key, read_ts: u64) -> Option<Value> {
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

fn load_snapshot_file(path: &Path) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
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
        store.entry(key).or_default().push(Version {
            commit_ts,
            value: Some(value),
        });
    }

    if cursor != data.len() {
        return Err(TxnError::CorruptWal("snapshot length mismatch".to_string()));
    }

    Ok(Some((store, commit_ts)))
}

fn write_snapshot_file(
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
        return Err(TxnError::CorruptWal(
            "too many snapshot entries".to_string(),
        ));
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
                let len =
                    u32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap()) as usize;
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
    use std::thread;
    use std::time::Duration;

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
        let mut tx = manager.begin_rw_timeout(Duration::from_secs(30));
        tx.put(&key(1), b"v1").unwrap();
        tx.delete(&key(2)).unwrap();
        let ts = tx.commit().unwrap();
        assert!(ts > 0);
        drop(manager);

        let manager = TxnManager::open(&path).unwrap();
        let tx = manager.begin_ro_timeout(Duration::from_secs(30));
        assert_eq!(tx.get(&key(1)).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(tx.get(&key(2)).unwrap(), None);
    }

    #[test]
    fn test_txn_timeout() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let manager = TxnManager::open(&path).unwrap();
        let mut tx = manager.begin_rw_timeout(Duration::from_millis(1));
        thread::sleep(Duration::from_millis(5));
        let err = tx.put(&key(1), b"v1").unwrap_err();
        assert!(matches!(err, TxnError::TxnTimeout));
    }
}
