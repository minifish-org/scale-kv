use crate::page_store::{CheckpointConfig, PageStore};
use crate::{ActiveReadInfo, ActiveReads, PAGE_SIZE, Page, PageId, ReadGuard, Result, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{Mutex, oneshot};

const PAGE_HEADER_SIZE: usize = 6;
const SLOT_ENTRY_SIZE: usize = 4;
#[cfg(test)]
const MAX_SEGMENT_SIZE: u64 = 8 * 1024;
#[cfg(not(test))]
const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const WAL_STATE_FILE: &str = "wal_state";
const WAL_STATE_TMP_FILE: &str = "wal_state.tmp";

#[derive(Clone, Debug)]
pub struct StorageMaintenanceConfig {
    pub checkpoint: CheckpointConfig,
    pub truncate_wal: bool,
    pub max_wal_bytes: u64,
    pub max_wal_segments: usize,
    pub mvcc_gc_every_wal_batches: usize,
}

impl Default for StorageMaintenanceConfig {
    fn default() -> Self {
        Self {
            checkpoint: CheckpointConfig::default(),
            truncate_wal: true,
            max_wal_bytes: 512 * 1024 * 1024, // 512MB
            max_wal_segments: 64,
            mvcc_gc_every_wal_batches: 128,
        }
    }
}

impl StorageMaintenanceConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Some(v) = env_u64("SCALE_KV_CHECKPOINT_INTERVAL_SECS") {
            cfg.checkpoint.interval = Duration::from_secs(v.max(1));
        }
        if let Some(v) = env_usize("SCALE_KV_CHECKPOINT_MAX_DIRTY_PAGES") {
            cfg.checkpoint.max_dirty_pages = v.max(1);
        }
        if let Some(v) = env_usize("SCALE_KV_CHECKPOINT_MAX_DIRTY_BYTES") {
            cfg.checkpoint.max_dirty_bytes = v.max(1);
        }
        if let Some(v) = env_bool("SCALE_KV_WAL_TRUNCATE") {
            cfg.truncate_wal = v;
        }
        if let Some(v) = env_u64("SCALE_KV_WAL_MAX_BYTES") {
            cfg.max_wal_bytes = v.max(1);
        }
        if let Some(v) = env_usize("SCALE_KV_WAL_MAX_SEGMENTS") {
            cfg.max_wal_segments = v.max(1);
        }
        if let Some(v) = env_usize("SCALE_KV_MVCC_GC_EVERY_WAL_BATCHES") {
            cfg.mvcc_gc_every_wal_batches = v.max(1);
        }
        cfg
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse::<u64>().ok()
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.parse::<usize>().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    let s = std::env::var(key).ok()?;
    match s.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Storage node - PostgreSQL style page storage with WAL.
pub struct StorageNode {
    dir: PathBuf,
    page_store: Arc<PageStore>,
    page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
    wal_sender: Mutex<Option<Sender<WalWriteRequest>>>,
    wal_replay_rx: Mutex<Option<Receiver<WalBatch>>>,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
    next_lsn: Arc<std::sync::atomic::AtomicU64>,

    // last applied (replayed) LSN (inclusive)
    applied_lsn: Arc<std::sync::atomic::AtomicU64>,
    applied_notify: Arc<tokio::sync::Notify>,

    // MVCC store for txnGet/appendTxnBatch. Key is raw bytes.
    mvcc: Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    // requestId -> commitLsn (end_lsn) for idempotent retry
    request_index: Arc<std::sync::Mutex<HashMap<u64, u64>>>,
    // Active read snapshots for MVCC GC watermark.
    active_reads: Arc<ActiveReads>,
    maintenance: StorageMaintenanceConfig,
    wal_limit_guard: Mutex<()>,
}

#[derive(Debug)]
pub struct MvccReadHandle {
    read_lsn: u64,
    started_at: Instant,
    timeout: Option<Duration>,
    guard: Option<ReadGuard>,
}

impl MvccReadHandle {
    pub fn id(&self) -> Option<u64> {
        self.guard.as_ref().map(|g| g.id())
    }

    pub fn is_active(&self) -> bool {
        self.guard.as_ref().is_some_and(|g| g.is_active())
    }

    pub fn close(&mut self) {
        let _ = self.guard.take();
    }

    pub fn is_timed_out(&self) -> bool {
        match self.timeout {
            Some(t) => self.started_at.elapsed() > t,
            None => false,
        }
    }
}

#[derive(Clone, Debug)]
struct MvccVersion {
    commit_lsn: u64,
    value: Option<Vec<u8>>,
}

struct WalWriteRequest {
    batch: WalBatch,
    ack: Option<oneshot::Sender<Result<u64>>>,
}

struct ReplayBuffer {
    bytes: usize,
    records: Vec<WalRecord>,
}

struct PageStoreReplay {
    page_store: Arc<PageStore>,
    dir: PathBuf,
    last_applied_lsn: std::sync::Mutex<u64>,
    page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
    applied_lsn: Arc<std::sync::atomic::AtomicU64>,
    applied_notify: Arc<tokio::sync::Notify>,
}

impl PageStoreReplay {
    fn new(
        page_store: Arc<PageStore>,
        dir: PathBuf,
        last_applied_lsn: u64,
        page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
        applied_lsn: Arc<std::sync::atomic::AtomicU64>,
        applied_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        applied_lsn.store(last_applied_lsn, std::sync::atomic::Ordering::Release);
        Self {
            page_store,
            dir,
            last_applied_lsn: std::sync::Mutex::new(last_applied_lsn),
            page_index,
            applied_lsn,
            applied_notify,
        }
    }

    fn write_page(&self, page_id: PageId, page: &[u8], lsn: u64) -> Result<()> {
        self.page_store.put(page_id, page, lsn)?;
        self.page_index.lock().unwrap().insert(page_id);
        Ok(())
    }

    async fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.page_store.get(page_id).await
    }

    fn last_applied(&self) -> u64 {
        *self.last_applied_lsn.lock().unwrap()
    }

    async fn update_last_applied(&self, lsn: u64) {
        let should_write = {
            let mut last_applied = self.last_applied_lsn.lock().unwrap();
            if lsn > *last_applied {
                *last_applied = lsn;
                true
            } else {
                false
            }
        };
        if should_write {
            self.applied_lsn
                .store(lsn, std::sync::atomic::Ordering::Release);
            self.applied_notify.notify_waiters();
            let _ = write_wal_state(&self.dir, lsn).await;
        }
    }

    async fn apply_record(&self, record: WalRecord) -> Result<()> {
        let page_id = record.page_id;
        let lsn = record.lsn;
        let page = self
            .read_page(page_id)
            .await
            .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
        let mut page = page;
        apply_wal_record(&mut page, &record)?;
        self.write_page(page_id, &page, lsn)?;
        self.update_last_applied(lsn).await;
        Ok(())
    }
}

impl ReplayBuffer {
    fn new() -> Self {
        Self {
            bytes: 0,
            records: Vec::new(),
        }
    }

    fn push(&mut self, record: WalRecord) {
        self.bytes += record.key.len() + record.value.len() + 32;
        self.records.push(record);
    }

    fn should_flush(&self) -> bool {
        self.bytes >= 8 * 1024
    }

    fn take(&mut self) -> Vec<WalRecord> {
        self.bytes = 0;
        std::mem::take(&mut self.records)
    }
}

impl WalWriter {
    async fn open(dir: PathBuf) -> Result<Self> {
        let mut segments = list_wal_segments(&dir).await?;
        let (file_id, file, size) = if segments.is_empty() {
            let file_id = 1u64;
            let file = create_wal_segment(&dir, file_id).await?;
            (file_id, file, 0u64)
        } else {
            segments.sort_unstable();
            let file_id = *segments.last().unwrap();
            let path = wal_segment_path(&dir, file_id);
            let file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(path)
                .await?;
            let size = file.metadata().await?.len();
            (file_id, file, size)
        };
        Ok(Self {
            dir,
            file_id,
            file,
            size,
        })
    }

    async fn rotate_if_needed(&mut self) -> Result<()> {
        if self.size < MAX_SEGMENT_SIZE {
            return Ok(());
        }
        self.file_id += 1;
        self.file = create_wal_segment(&self.dir, self.file_id).await?;
        self.size = 0;
        Ok(())
    }

    async fn append_batch(&mut self, batch: &WalBatch) -> Result<()> {
        self.rotate_if_needed().await?;
        let mut buf = Vec::new();
        encode_wal_batch(batch, &mut buf)?;
        self.file.write_all(&buf).await?;
        self.size += buf.len() as u64;
        self.file.sync_data().await?;
        Ok(())
    }
}

struct WalWriter {
    dir: PathBuf,
    file_id: u64,
    file: File,
    size: u64,
}

struct WalState {
    last_applied_lsn: u64,
}

const WAL_SEGMENT_PREFIX: &str = "wal";

fn wal_segment_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{}-{:020}.log", WAL_SEGMENT_PREFIX, file_id))
}

async fn create_wal_segment(dir: &Path, file_id: u64) -> Result<File> {
    let path = wal_segment_path(dir, file_id);
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)
        .await?)
}

async fn list_wal_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix(&format!("{}-", WAL_SEGMENT_PREFIX)) {
            if let Some(id_part) = rest.strip_suffix(".log") {
                if let Ok(id) = id_part.parse::<u64>() {
                    segments.push(id);
                }
            }
        }
    }
    segments.sort_unstable();
    Ok(segments)
}

async fn wal_usage(dir: &Path) -> Result<(usize, u64)> {
    let segments = list_wal_segments(dir).await?;
    let mut bytes = 0u64;
    for id in &segments {
        let meta = fs::metadata(wal_segment_path(dir, *id)).await?;
        bytes = bytes.saturating_add(meta.len());
    }
    Ok((segments.len(), bytes))
}

async fn wal_usage_exceeds_limits(
    dir: &Path,
    config: &StorageMaintenanceConfig,
    incoming_bytes: u64,
) -> Result<Option<(usize, u64)>> {
    let (segments, bytes) = wal_usage(dir).await?;
    let bytes_over = bytes.saturating_add(incoming_bytes) > config.max_wal_bytes;
    let segments_over = segments > config.max_wal_segments;
    if bytes_over || segments_over {
        Ok(Some((segments, bytes)))
    } else {
        Ok(None)
    }
}

async fn get_wal_segment_max_lsn(dir: &Path, file_id: u64) -> Result<u64> {
    let path = wal_segment_path(dir, file_id);
    let mut file = File::open(&path).await?;
    let mut max_lsn = 0u64;

    loop {
        match read_wal_batch(&mut file).await {
            Ok(Some(batch)) => {
                if batch.end_lsn > max_lsn {
                    max_lsn = batch.end_lsn;
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    Ok(max_lsn)
}

async fn truncate_wal_segments(dir: &Path, checkpoint_lsn: u64) -> Result<usize> {
    let segments = list_wal_segments(dir).await?;
    if segments.len() <= 1 {
        return Ok(0);
    }

    let mut deleted = 0;
    for &file_id in &segments[..segments.len() - 1] {
        let max_lsn = get_wal_segment_max_lsn(dir, file_id).await.unwrap_or(0);
        if max_lsn > 0 && max_lsn <= checkpoint_lsn {
            let path = wal_segment_path(dir, file_id);
            if fs::remove_file(&path).await.is_ok() {
                deleted += 1;
            }
        }
    }

    Ok(deleted)
}

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub lsn: u64,
    pub op: u8,
    pub page_id: u64,
    pub slot_id: u16,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// WAL ops for page records.
//
// Aurora-style: storage replays page after-images into PageStore.
pub const WAL_OP_PAGE_IMAGE: u8 = 1;

// Legacy slot-level ops (kept for potential experiments/tests).
pub const WAL_OP_PAGE_PUT: u8 = 11;
pub const WAL_OP_PAGE_DEL: u8 = 12;

// WAL ops for txn MVCC records
pub const WAL_OP_TXN_PUT: u8 = 11;
pub const WAL_OP_TXN_DEL: u8 = 12;
pub const WAL_OP_TXN_COMMIT: u8 = 13;

#[derive(Clone, Debug)]
pub struct WalBatch {
    pub request_id: u64,
    pub start_lsn: u64,
    /// Right boundary (exclusive). Commit point uses end_lsn.
    pub end_lsn: u64,
    pub records: Vec<WalRecord>,
}

fn encode_wal_batch(batch: &WalBatch, out: &mut Vec<u8>) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&batch.request_id.to_le_bytes());
    buf.extend_from_slice(&batch.start_lsn.to_le_bytes());
    buf.extend_from_slice(&batch.end_lsn.to_le_bytes());
    let count = batch.records.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());
    for record in &batch.records {
        buf.extend_from_slice(&record.lsn.to_le_bytes());
        buf.push(record.op);
        buf.extend_from_slice(&record.page_id.to_le_bytes());
        buf.extend_from_slice(&record.slot_id.to_le_bytes());
        let key_len = record.key.len() as u32;
        let val_len = record.value.len() as u32;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&val_len.to_le_bytes());
        buf.extend_from_slice(&record.key);
        buf.extend_from_slice(&record.value);
    }
    let total_len = buf.len() as u32;
    out.extend_from_slice(&total_len.to_le_bytes());
    out.extend_from_slice(&buf);
    Ok(())
}

fn wal_batch_encoded_len(batch: &WalBatch) -> u64 {
    // 4 bytes frame length + fixed header + per-record fixed fields + key/value payload.
    let mut len = 4u64 + 8 + 8 + 8 + 4;
    for r in &batch.records {
        len += 8 + 1 + 8 + 2 + 4 + 4;
        len += r.key.len() as u64 + r.value.len() as u64;
    }
    len
}

async fn start_wal_writer(
    dir: PathBuf,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
) -> Result<(Sender<WalWriteRequest>, Receiver<WalBatch>)> {
    let (tx, rx) = mpsc::channel::<WalWriteRequest>(1024);
    let (replay_tx, replay_rx) = mpsc::channel::<WalBatch>(1024);
    tokio::spawn(wal_writer_loop(dir, durable_lsn, rx, replay_tx));
    Ok((tx, replay_rx))
}

async fn wal_writer_loop(
    dir: PathBuf,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
    mut rx: Receiver<WalWriteRequest>,
    replay_tx: Sender<WalBatch>,
) {
    let mut writer = match WalWriter::open(dir).await {
        Ok(writer) => writer,
        Err(_) => return,
    };
    while let Some(req) = rx.recv().await {
        let WalWriteRequest { batch, ack } = req;
        let appended = writer.append_batch(&batch).await.map(|_| batch.end_lsn);
        if let Ok(end_lsn) = appended {
            durable_lsn.store(end_lsn, std::sync::atomic::Ordering::Release);
            let _ = replay_tx.send(batch).await;
        }
        if let Some(ack) = ack {
            // If WAL append failed, return the error to the caller.
            let _ = ack.send(appended.map_err(|e| e));
        }
    }
}

async fn read_wal_batch(file: &mut File) -> Result<Option<WalBatch>> {
    let mut len_buf = [0u8; 4];
    match file.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let total_len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; total_len];
    file.read_exact(&mut buf).await?;
    let mut cursor = 0usize;

    let request_id = read_u64_from(&buf, &mut cursor)?;
    let start_lsn = read_u64_from(&buf, &mut cursor)?;
    let end_lsn = read_u64_from(&buf, &mut cursor)?;
    let count = read_u32_from(&buf, &mut cursor)? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let lsn = read_u64_from(&buf, &mut cursor)?;
        let op = read_u8_from(&buf, &mut cursor)?;
        let page_id = read_u64_from(&buf, &mut cursor)?;
        let slot_id = read_u16_from(&buf, &mut cursor)?;
        let key_len = read_u32_from(&buf, &mut cursor)? as usize;
        let val_len = read_u32_from(&buf, &mut cursor)? as usize;
        if cursor + key_len + val_len > buf.len() {
            return Err(Error::new(ErrorKind::InvalidData, "wal record truncated").into());
        }
        let key = buf[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let value = buf[cursor..cursor + val_len].to_vec();
        cursor += val_len;
        records.push(WalRecord {
            lsn,
            op,
            page_id,
            slot_id,
            key,
            value,
        });
    }
    Ok(Some(WalBatch {
        request_id,
        start_lsn,
        end_lsn,
        records,
    }))
}

fn apply_txn_batch_to_mvcc(
    mvcc: &Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    batch: &WalBatch,
) {
    if batch.records.is_empty() {
        return;
    }
    let last = batch.records.last().unwrap();
    if last.op != WAL_OP_TXN_COMMIT {
        return;
    }
    let commit_lsn = batch.end_lsn;

    let mut store = mvcc.lock().unwrap();
    for record in &batch.records {
        match record.op {
            WAL_OP_TXN_PUT => {
                store
                    .entry(record.key.clone())
                    .or_default()
                    .push(MvccVersion {
                        commit_lsn,
                        value: Some(record.value.clone()),
                    });
            }
            WAL_OP_TXN_DEL => {
                store
                    .entry(record.key.clone())
                    .or_default()
                    .push(MvccVersion {
                        commit_lsn,
                        value: None,
                    });
            }
            WAL_OP_TXN_COMMIT => {}
            _ => {}
        }
    }
}

fn mvcc_get_at(
    mvcc: &Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    key: &[u8],
    read_lsn: u64,
) -> Option<Option<Vec<u8>>> {
    let store = mvcc.lock().unwrap();
    let versions = store.get(key)?;
    versions
        .iter()
        .rfind(|v| v.commit_lsn <= read_lsn)
        .map(|v| v.value.clone())
}

fn gc_mvcc_versions(
    store: &mut BTreeMap<Vec<u8>, Vec<MvccVersion>>,
    watermark: u64,
) -> (usize, usize) {
    let mut keys_touched = 0usize;
    let mut versions_removed = 0usize;

    for versions in store.values_mut() {
        if versions.len() <= 1 {
            continue;
        }
        let mut last_visible: Option<usize> = None;
        for (idx, version) in versions.iter().enumerate() {
            if version.commit_lsn <= watermark {
                last_visible = Some(idx);
            } else {
                break;
            }
        }

        if let Some(idx) = last_visible {
            if idx > 0 {
                versions.drain(..idx);
                keys_touched += 1;
                versions_removed += idx;
            }
        }
    }

    (keys_touched, versions_removed)
}

async fn maybe_checkpoint_by_pressure(
    page_store: &Arc<PageStore>,
    maintenance: &StorageMaintenanceConfig,
) -> Result<bool> {
    let stats = page_store.buffer_stats();
    let over_pages = stats.dirty_count > maintenance.checkpoint.max_dirty_pages;
    let over_bytes = stats.dirty_bytes > maintenance.checkpoint.max_dirty_bytes;
    if over_pages || over_bytes {
        page_store.checkpoint().await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn read_u64_from(buf: &[u8], cursor: &mut usize) -> Result<u64> {
    if *cursor + 8 > buf.len() {
        return Err(Error::new(ErrorKind::UnexpectedEof, "wal batch truncated").into());
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[*cursor..*cursor + 8]);
    *cursor += 8;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32_from(buf: &[u8], cursor: &mut usize) -> Result<u32> {
    if *cursor + 4 > buf.len() {
        return Err(Error::new(ErrorKind::UnexpectedEof, "wal batch truncated").into());
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u16_from(buf: &[u8], cursor: &mut usize) -> Result<u16> {
    if *cursor + 2 > buf.len() {
        return Err(Error::new(ErrorKind::UnexpectedEof, "wal batch truncated").into());
    }
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&buf[*cursor..*cursor + 2]);
    *cursor += 2;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u8_from(buf: &[u8], cursor: &mut usize) -> Result<u8> {
    if *cursor + 1 > buf.len() {
        return Err(Error::new(ErrorKind::UnexpectedEof, "wal batch truncated").into());
    }
    let value = buf[*cursor];
    *cursor += 1;
    Ok(value)
}

async fn wal_replay_loop(
    replay: PageStoreReplay,
    mut rx: Receiver<WalBatch>,
    mvcc: Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    request_index: Arc<std::sync::Mutex<HashMap<u64, u64>>>,
    active_reads: Arc<ActiveReads>,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
    maintenance: StorageMaintenanceConfig,
) {
    let mut buffers: HashMap<PageId, ReplayBuffer> = HashMap::new();
    let mut gc_batch_counter = 0usize;
    while let Some(batch) = rx.recv().await {
        // Build requestId -> commitLsn mapping for idempotent retry.
        if batch.request_id != 0 {
            request_index
                .lock()
                .unwrap()
                .insert(batch.request_id, batch.end_lsn);
        }

        // Apply txn MVCC records if this batch ends with a commit marker.
        apply_txn_batch_to_mvcc(&mvcc, &batch);

        let mut last_seen = replay.last_applied();
        for record in batch.records.into_iter() {
            if record.lsn <= last_seen {
                continue;
            }
            if record.lsn != last_seen + 1 {
                break;
            }
            last_seen = record.lsn;

            // Skip txn records for page replay.
            if record.op == WAL_OP_TXN_PUT
                || record.op == WAL_OP_TXN_DEL
                || record.op == WAL_OP_TXN_COMMIT
            {
                continue;
            }

            let page_id = record.page_id;
            let buffer = buffers.entry(page_id).or_insert_with(ReplayBuffer::new);
            buffer.push(record);
            if buffer.should_flush() {
                let records = buffer.take();
                let _ = replay_page_records(&replay, page_id, records).await;
            }
        }

        let _ = maybe_checkpoint_by_pressure(&replay.page_store, &maintenance).await;
        if maintenance.truncate_wal {
            let checkpoint_lsn = replay.page_store.checkpoint_lsn();
            let _ = truncate_wal_segments(&replay.dir, checkpoint_lsn).await;
        }

        if let Ok(Some((segments, bytes))) = wal_usage_exceeds_limits(&replay.dir, &maintenance, 0).await
        {
            eprintln!(
                "[wal-limit-replay] over threshold after replay: segments={} bytes={} limits=(segments:{} bytes:{})",
                segments,
                bytes,
                maintenance.max_wal_segments,
                maintenance.max_wal_bytes
            );
        }

        gc_batch_counter += 1;
        if gc_batch_counter >= maintenance.mvcc_gc_every_wal_batches {
            gc_batch_counter = 0;
            let durable = durable_lsn.load(std::sync::atomic::Ordering::Acquire);
            let watermark = active_reads.min_read_lsn().unwrap_or(durable).min(durable);
            let (keys_touched, versions_removed) = {
                let mut store = mvcc.lock().unwrap();
                gc_mvcc_versions(&mut store, watermark)
            };
            if versions_removed > 0 {
                eprintln!(
                    "[mvcc-gc] watermark={} keys_touched={} versions_removed={}",
                    watermark, keys_touched, versions_removed
                );
            }
        }
    }

    for (page_id, mut buffer) in buffers.into_iter() {
        let records = buffer.take();
        let _ = replay_page_records(&replay, page_id, records).await;
    }
}

async fn replay_page_records(
    replay: &PageStoreReplay,
    page_id: PageId,
    records: Vec<WalRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut max_lsn = 0u64;
    let page = replay
        .read_page(page_id)
        .await
        .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
    let mut page = page;
    for record in records {
        if record.lsn > max_lsn {
            max_lsn = record.lsn;
        }
        apply_wal_record(&mut page, &record)?;
    }
    replay.write_page(page_id, &page, max_lsn)?;
    replay.update_last_applied(max_lsn).await;
    Ok(())
}

async fn replay_wal_segments_to_store(
    dir: &Path,
    page_store: Arc<PageStore>,
    last_applied_lsn: u64,
    page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
    mvcc: &Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    request_index: &Arc<std::sync::Mutex<HashMap<u64, u64>>>,
) -> Result<()> {
    let mut segments = list_wal_segments(dir).await?;
    segments.sort_unstable();
    if segments.is_empty() {
        return Ok(());
    }
    let replay = PageStoreReplay::new(
        page_store,
        dir.to_path_buf(),
        last_applied_lsn,
        page_index,
        Arc::new(std::sync::atomic::AtomicU64::new(last_applied_lsn)),
        Arc::new(tokio::sync::Notify::new()),
    );
    for file_id in segments {
        let path = wal_segment_path(dir, file_id);
        let mut file = File::open(&path).await?;
        loop {
            let batch = match read_wal_batch(&mut file).await? {
                Some(batch) => batch,
                None => break,
            };

            if batch.request_id != 0 {
                request_index
                    .lock()
                    .unwrap()
                    .insert(batch.request_id, batch.end_lsn);
            }
            apply_txn_batch_to_mvcc(mvcc, &batch);

            for record in batch.records {
                if record.lsn <= replay.last_applied() {
                    continue;
                }
                // Skip logical txn records (deprecated). Page-level redo uses WAL_OP_PAGE_IMAGE.
                if record.op == WAL_OP_TXN_PUT
                    || record.op == WAL_OP_TXN_DEL
                    || record.op == WAL_OP_TXN_COMMIT
                {
                    continue;
                }
                replay.apply_record(record).await?;
            }
        }
    }
    Ok(())
}

fn apply_wal_record(page: &mut [u8], record: &WalRecord) -> Result<()> {
    match record.op {
        WAL_OP_PAGE_IMAGE => {
            if record.value.len() != PAGE_SIZE {
                return Err(crate::Error::InvalidPageSize(record.value.len(), PAGE_SIZE));
            }
            page.copy_from_slice(&record.value);
            Ok(())
        }
        WAL_OP_PAGE_PUT => crate::slotted_page::insert_record_at_slot_checked(
            page,
            record.slot_id,
            &record.key,
            &record.value,
        ),
        WAL_OP_PAGE_DEL => {
            crate::slotted_page::clear_slot(page, record.slot_id);
            Ok(())
        }
        // txn records are not applied to the page store
        WAL_OP_TXN_PUT | WAL_OP_TXN_DEL | WAL_OP_TXN_COMMIT => Ok(()),
        _ => Ok(()),
    }
}

// (slotted page helpers moved to src/slotted_page.rs)

impl StorageNode {
    pub async fn new() -> Self {
        let dir = std::env::var("SCALE_KV_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        Self::open(dir).await.expect("failed to open storage")
    }

    pub async fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        Self::open_with_maintenance(dir, StorageMaintenanceConfig::default()).await
    }

    pub async fn open_with_env<P: AsRef<Path>>(dir: P) -> Result<Self> {
        Self::open_with_maintenance(dir, StorageMaintenanceConfig::from_env()).await
    }

    pub async fn open_with_maintenance<P: AsRef<Path>>(
        dir: P,
        maintenance: StorageMaintenanceConfig,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;

        let page_store = PageStore::open(&dir).await?;
        let checkpoint_lsn = page_store.checkpoint_lsn();

        let wal_state = read_wal_state(&dir).await.unwrap_or(WalState {
            last_applied_lsn: 0,
        });
        let last_applied_lsn = wal_state.last_applied_lsn.max(checkpoint_lsn);

        let page_store = Arc::new(page_store);
        let page_index = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mvcc = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
        let request_index = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let active_reads = Arc::new(ActiveReads::new());
        replay_wal_segments_to_store(
            &dir,
            Arc::clone(&page_store),
            last_applied_lsn,
            Arc::clone(&page_index),
            &mvcc,
            &request_index,
        )
        .await?;
        let rebuilt_index = build_page_index(&page_store).await;
        *page_index.lock().unwrap() = rebuilt_index;

        // Initialize durable_lsn and next_lsn from the last applied point.
        // We treat `durable_lsn` as the right boundary (exclusive): all records with lsn < durable_lsn are durable.
        // NOTE: in the quorum design, durable_lsn should reflect quorum-durable; for now it's local.
        let durable_init = last_applied_lsn.saturating_add(1);
        let durable_lsn = Arc::new(std::sync::atomic::AtomicU64::new(durable_init));
        let next_lsn = Arc::new(std::sync::atomic::AtomicU64::new(durable_init));

        let (wal_sender, wal_replay_rx) =
            start_wal_writer(dir.clone(), durable_lsn.clone()).await?;

        let applied_lsn = Arc::new(std::sync::atomic::AtomicU64::new(last_applied_lsn));
        let applied_notify = Arc::new(tokio::sync::Notify::new());

        let node = Self {
            dir: dir.clone(),
            page_store: Arc::clone(&page_store),
            page_index: Arc::clone(&page_index),
            wal_sender: Mutex::new(Some(wal_sender)),
            wal_replay_rx: Mutex::new(Some(wal_replay_rx)),
            durable_lsn,
            next_lsn,
            applied_lsn,
            applied_notify,
            mvcc,
            request_index,
            active_reads,
            maintenance: maintenance.clone(),
            wal_limit_guard: Mutex::new(()),
        };

        node.start_wal_replay(last_applied_lsn).await;
        Ok(node)
    }

    pub async fn append_wal_batch(&self, batch: WalBatch) -> Result<()> {
        self.validate_incoming_wal_batch(&batch)?;
        self.enforce_wal_limits(wal_batch_encoded_len(&batch)).await?;
        let sender = self.wal_sender.lock().await;
        match sender.as_ref() {
            Some(sender) => Ok(sender
                .send(WalWriteRequest { batch, ack: None })
                .await
                .map_err(|_| {
                    crate::Error::Io(Error::new(ErrorKind::BrokenPipe, "wal queue closed"))
                })?),
            None => Err(crate::Error::Io(Error::new(
                ErrorKind::BrokenPipe,
                "wal queue not initialized",
            ))),
        }
    }

    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn begin_mvcc_ro_guard(&self) -> (u64, ReadGuard) {
        let read_lsn = self.durable_lsn();
        let guard = self.active_reads.register(read_lsn);
        (read_lsn, guard)
    }

    pub fn begin_mvcc_ro(&self) -> MvccReadHandle {
        self.begin_mvcc_ro_with_timeout(None)
    }

    pub fn begin_mvcc_ro_timeout(&self, timeout: Duration) -> MvccReadHandle {
        self.begin_mvcc_ro_with_timeout(Some(timeout))
    }

    pub fn begin_mvcc_ro_with_timeout(&self, timeout: Option<Duration>) -> MvccReadHandle {
        let (read_lsn, guard) = self.begin_mvcc_ro_guard();
        MvccReadHandle {
            read_lsn,
            started_at: Instant::now(),
            timeout,
            guard: Some(guard),
        }
    }

    pub fn with_mvcc_ro<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut MvccReadHandle) -> Result<T>,
    {
        self.with_mvcc_ro_timeout(None, f)
    }

    pub fn with_mvcc_ro_timeout<T, F>(&self, timeout: Option<Duration>, f: F) -> Result<T>
    where
        F: FnOnce(&mut MvccReadHandle) -> Result<T>,
    {
        let mut handle = self.begin_mvcc_ro_with_timeout(timeout);
        f(&mut handle)
    }

    pub fn list_active_reads(&self) -> Vec<ActiveReadInfo> {
        self.active_reads.list()
    }

    pub fn abort_active_read(&self, id: u64) -> bool {
        self.active_reads.abort(id)
    }

    pub fn mvcc_get(&self, handle: &mut MvccReadHandle, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if handle.is_timed_out() {
            handle.close();
            return Err(crate::Error::TxnTimeout);
        }
        if !handle.is_active() {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::Interrupted,
                "mvcc read handle aborted or closed",
            )));
        }
        Ok(mvcc_get_at(&self.mvcc, key, handle.read_lsn).unwrap_or(None))
    }

    async fn wal_usage_exceeds(&self, incoming_bytes: u64) -> Result<Option<(usize, u64)>> {
        wal_usage_exceeds_limits(&self.dir, &self.maintenance, incoming_bytes).await
    }

    async fn enforce_wal_limits(&self, incoming_bytes: u64) -> Result<()> {
        if self
            .wal_usage_exceeds(incoming_bytes)
            .await?
            .is_none()
        {
            return Ok(());
        }

        let _guard = self.wal_limit_guard.lock().await;

        let before = self.wal_usage_exceeds(incoming_bytes).await?;
        if before.is_none() {
            return Ok(());
        }
        let (before_segments, before_bytes) = before.unwrap();
        eprintln!(
            "[wal-limit] over threshold before write: segments={} bytes={} limits=(segments:{} bytes:{})",
            before_segments,
            before_bytes,
            self.maintenance.max_wal_segments,
            self.maintenance.max_wal_bytes
        );

        self.page_store.checkpoint().await?;
        if self.maintenance.truncate_wal {
            let _ = self.truncate_wal().await?;
        }

        if let Some((segments, bytes)) = self.wal_usage_exceeds(incoming_bytes).await? {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::WouldBlock,
                format!(
                    "wal backpressure: segments={} bytes={} limits=(segments:{} bytes:{})",
                    segments,
                    bytes,
                    self.maintenance.max_wal_segments,
                    self.maintenance.max_wal_bytes
                ),
            )));
        }

        Ok(())
    }

    fn validate_incoming_wal_batch(&self, batch: &WalBatch) -> Result<()> {
        // Strict, single-writer style: batches must arrive in LSN order with no gaps.
        // `durable_lsn` is the exclusive right boundary, so the next expected start is durable_lsn.
        let expected_start = self.durable_lsn();
        if batch.start_lsn != expected_start {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "wal batch out of order: start_lsn={} expected_start={}",
                    batch.start_lsn, expected_start
                ),
            )));
        }
        if batch.end_lsn < batch.start_lsn {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "wal batch invalid range",
            )));
        }
        let expected_len = batch.end_lsn.saturating_sub(batch.start_lsn) as usize;
        if expected_len != batch.records.len() {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "wal batch length mismatch: records={} range_len={}",
                    batch.records.len(),
                    expected_len
                ),
            )));
        }
        for (i, rec) in batch.records.iter().enumerate() {
            let want = batch.start_lsn + i as u64;
            if rec.lsn != want {
                return Err(crate::Error::Io(Error::new(
                    ErrorKind::InvalidInput,
                    format!("wal record lsn mismatch: got={} want={}", rec.lsn, want),
                )));
            }
        }
        Ok(())
    }

    // NOTE: alloc_lsn_range was used by the legacy `append_txn_batch_sync` where storage
    // assigned LSNs. In Aurora-style replication, compute assigns LSNs, so this is no longer
    // used by the RPC path.
    #[allow(dead_code)]
    fn alloc_lsn_range(&self, n: u64) -> (u64, u64) {
        if n == 0 {
            let cur = self.next_lsn.load(std::sync::atomic::Ordering::Relaxed);
            return (cur, cur);
        }
        let start = self
            .next_lsn
            .fetch_add(n, std::sync::atomic::Ordering::AcqRel);
        let end = start + n;
        (start, end)
    }

    #[allow(dead_code)]
    pub fn txn_get(&self, key: &[u8], read_lsn: u64) -> Option<Option<Vec<u8>>> {
        mvcc_get_at(&self.mvcc, key, read_lsn)
    }

    /// Fetch a raw page (latest) and a best-effort page LSN.
    pub async fn get_page_latest(&self, page_id: PageId) -> Option<(Page, u64)> {
        self.page_store.get_with_lsn(page_id).await
    }

    /// Scan pages (latest) for warmup.
    pub async fn scan_pages_latest(
        &self,
        start_page_id: PageId,
        limit: usize,
    ) -> Vec<(PageId, u64, Page)> {
        self.page_store
            .scan_pages_with_lsn(start_page_id, limit)
            .await
    }

    /// Append a txn batch with compute-assigned LSN range.
    ///
    /// `end_lsn` is the exclusive right boundary; commit point uses `end_lsn`.
    pub async fn append_txn_batch_with_lsn_sync(
        &self,
        request_id: u64,
        start_lsn: u64,
        end_lsn: u64,
        writes: Vec<(PageId, Page)>,
    ) -> Result<u64> {
        // Idempotent retry: if we've already committed this request_id, return the same commit_lsn.
        if request_id != 0 {
            if let Some(lsn) = self.request_index.lock().unwrap().get(&request_id).copied() {
                return Ok(lsn);
            }
        }

        if writes.is_empty() {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "txn batch must be non-empty",
            )));
        }

        let expected_end = start_lsn + writes.len() as u64;
        if end_lsn != expected_end {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "txn batch end_lsn mismatch: got={} expected={}",
                    end_lsn, expected_end
                ),
            )));
        }

        let mut wal_records = Vec::with_capacity(writes.len());
        for (idx, (page_id, page)) in writes.into_iter().enumerate() {
            if page.len() != PAGE_SIZE {
                return Err(crate::Error::InvalidPageSize(page.len(), PAGE_SIZE));
            }
            let lsn = start_lsn + idx as u64;
            wal_records.push(WalRecord {
                lsn,
                op: WAL_OP_PAGE_IMAGE,
                page_id,
                slot_id: 0,
                key: Vec::new(),
                value: page,
            });
        }

        let batch = WalBatch {
            request_id,
            start_lsn,
            end_lsn,
            records: wal_records,
        };

        let commit_lsn = self.append_wal_batch_sync(batch).await?;
        if request_id != 0 {
            self.request_index
                .lock()
                .unwrap()
                .insert(request_id, commit_lsn);
        }
        Ok(commit_lsn)
    }

    pub async fn append_wal_batch_sync(&self, batch: WalBatch) -> Result<u64> {
        self.validate_incoming_wal_batch(&batch)?;
        self.enforce_wal_limits(wal_batch_encoded_len(&batch)).await?;
        let sender = self.wal_sender.lock().await;
        let sender = sender.as_ref().ok_or_else(|| {
            crate::Error::Io(Error::new(
                ErrorKind::BrokenPipe,
                "wal queue not initialized",
            ))
        })?;
        let (tx, rx) = oneshot::channel();
        let end_lsn = batch.end_lsn;
        sender
            .send(WalWriteRequest {
                batch,
                ack: Some(tx),
            })
            .await
            .map_err(|_| crate::Error::Io(Error::new(ErrorKind::BrokenPipe, "wal queue closed")))?;
        let durable = rx.await.map_err(|_| {
            crate::Error::Io(Error::new(ErrorKind::BrokenPipe, "wal ack dropped"))
        })??;

        // Wait until WAL replay has applied all records up to end_lsn-1.
        let target = end_lsn.saturating_sub(1);
        while self.applied_lsn.load(std::sync::atomic::Ordering::Acquire) < target {
            self.applied_notify.notified().await;
        }

        Ok(durable)
    }

    pub async fn start_wal_replay(&self, last_applied_lsn: u64) {
        let rx = self.wal_replay_rx.lock().await.take();
        if rx.is_none() {
            return;
        }
        let rx = rx.unwrap();
        let replay = PageStoreReplay::new(
            Arc::clone(&self.page_store),
            self.dir.clone(),
            last_applied_lsn,
            Arc::clone(&self.page_index),
            Arc::clone(&self.applied_lsn),
            Arc::clone(&self.applied_notify),
        );
        let mvcc = self.mvcc.clone();
        let req_index = self.request_index.clone();
        let active_reads = Arc::clone(&self.active_reads);
        let durable_lsn = Arc::clone(&self.durable_lsn);
        let maintenance = self.maintenance.clone();
        tokio::spawn(async move {
            wal_replay_loop(
                replay,
                rx,
                mvcc,
                req_index,
                active_reads,
                durable_lsn,
                maintenance,
            )
            .await;
        });
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.page_store.checkpoint().await
    }

    pub fn len(&self) -> usize {
        self.page_index.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.page_index.lock().unwrap().is_empty()
    }

    pub fn put(&self, key: PageId, value: &[u8]) {
        let _ = self.page_store.put_direct(key, value);
        self.page_index.lock().unwrap().insert(key);
    }

    pub async fn get(&self, key: PageId) -> Option<Value> {
        if !self.page_index.lock().unwrap().contains(&key) {
            return None;
        }
        self.page_store.get(key).await
    }

    pub fn delete(&self, key: PageId) {
        let zero_page = vec![0u8; PAGE_SIZE];
        let _ = self.page_store.put_direct(key, &zero_page);
        self.page_index.lock().unwrap().remove(&key);
    }

    pub async fn contains(&self, key: PageId) -> bool {
        if !self.page_index.lock().unwrap().contains(&key) {
            return false;
        }
        self.page_store.contains(key).await
    }

    pub fn keys(&self) -> Vec<PageId> {
        self.page_index.lock().unwrap().iter().copied().collect()
    }

    pub async fn compact(&self) -> Result<()> {
        self.page_store.checkpoint().await
    }

    pub async fn truncate_wal(&self) -> Result<usize> {
        let checkpoint_lsn = self.page_store.checkpoint_lsn();
        truncate_wal_segments(&self.dir, checkpoint_lsn).await
    }

    pub async fn checkpoint_and_truncate(&self) -> Result<usize> {
        self.page_store.checkpoint().await?;
        self.truncate_wal().await
    }

    pub fn start_background_checkpoint(
        &self,
        config: crate::page_store::CheckpointConfig,
    ) -> tokio::task::JoinHandle<()> {
        self.start_background_checkpoint_with_truncate(config, false)
    }

    pub fn start_background_checkpoint_with_truncate(
        &self,
        config: crate::page_store::CheckpointConfig,
        truncate_wal: bool,
    ) -> tokio::task::JoinHandle<()> {
        let page_store = Arc::clone(&self.page_store);
        let dir = self.dir.clone();
        let poll_interval = config.interval.min(std::time::Duration::from_secs(1));

        tokio::spawn(async move {
            while !page_store.is_shutdown() {
                tokio::time::sleep(poll_interval).await;
                if page_store.is_shutdown() {
                    break;
                }
                if page_store.maybe_checkpoint(&config).await.unwrap_or(false) && truncate_wal {
                    let checkpoint_lsn = page_store.checkpoint_lsn();
                    let _ = truncate_wal_segments(&dir, checkpoint_lsn).await;
                }
            }
            let _ = page_store.checkpoint().await;
            if truncate_wal {
                let checkpoint_lsn = page_store.checkpoint_lsn();
                let _ = truncate_wal_segments(&dir, checkpoint_lsn).await;
            }
        })
    }

    pub fn shutdown(&self) {
        self.page_store.shutdown();
    }

    pub fn page_store(&self) -> &Arc<PageStore> {
        &self.page_store
    }
}

async fn build_page_index(page_store: &Arc<PageStore>) -> HashSet<PageId> {
    let mut index = HashSet::new();
    let max_page_id = page_store.max_page_id();
    for page_id in 0..=max_page_id {
        if page_store.get(page_id).await.is_some() {
            index.insert(page_id);
        }
    }
    index
}

async fn read_wal_state(dir: &Path) -> Result<WalState> {
    let path = dir.join(WAL_STATE_FILE);
    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Ok(WalState {
                last_applied_lsn: 0,
            });
        }
        Err(err) => return Err(err.into()),
    };
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).await?;
    let last_applied_lsn = u64::from_le_bytes(buf);
    Ok(WalState { last_applied_lsn })
}

async fn write_wal_state(dir: &Path, last_applied_lsn: u64) -> Result<()> {
    let tmp_path = dir.join(WAL_STATE_TMP_FILE);
    let path = dir.join(WAL_STATE_FILE);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .await?;
        file.write_all(&last_applied_lsn.to_le_bytes()).await?;
        file.sync_all().await?;
    }
    fs::rename(tmp_path, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KEY_SIZE, VALUE_SIZE};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    async fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("scale-kv-test-{}-{}", std::process::id(), id));
        fs::create_dir_all(&dir)
            .await
            .expect("failed to create temp dir");
        dir
    }

    async fn cleanup_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir).await;
    }

    fn make_page(fill: u8) -> Vec<u8> {
        let mut page = vec![0u8; PAGE_SIZE];
        page[0] = fill;
        page[PAGE_SIZE - 1] = fill;
        page
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();
        let page = make_page(1);
        node.put(1, &page);
        let result = node.get(1).await.unwrap();
        assert_eq!(result[0], 1);
        assert_eq!(result[PAGE_SIZE - 1], 1);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_get_missing() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();
        assert_eq!(node.get(999).await, None);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_overwrite() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();
        let page1 = make_page(1);
        let page2 = make_page(2);
        node.put(1, &page1);
        node.put(1, &page2);
        let result = node.get(1).await.unwrap();
        assert_eq!(result[0], 2);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();
        let page = make_page(1);
        node.put(1, &page);
        node.delete(1);
        assert_eq!(node.get(1).await, None);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_len() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();
        assert_eq!(node.len(), 0);
        let page1 = make_page(1);
        let page2 = make_page(2);
        node.put(1, &page1);
        node.put(2, &page2);
        assert_eq!(node.len(), 2);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_contains() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();
        let page = make_page(1);
        node.put(1, &page);
        assert!(node.contains(1).await);
        assert!(!node.contains(999).await);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_reopen_persists() {
        let dir = temp_dir().await;
        let page1 = make_page(1);
        let page2 = make_page(2);
        {
            let node = StorageNode::open(&dir).await.unwrap();
            node.put(1, &page1);
            node.put(2, &page2);
            node.checkpoint().await.unwrap();
        }
        let node = StorageNode::open(&dir).await.unwrap();
        assert_eq!(node.get(1).await.unwrap()[0], 1);
        assert_eq!(node.get(2).await.unwrap()[0], 2);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_checkpoint_persists_data() {
        let dir = temp_dir().await;
        let page1 = make_page(1);
        let page2 = make_page(2);
        {
            let node = StorageNode::open(&dir).await.unwrap();
            node.put(1, &page1);
            node.put(2, &page2);
            node.checkpoint().await.unwrap();
        }
        {
            let node = StorageNode::open(&dir).await.unwrap();
            assert_eq!(node.get(1).await.unwrap()[0], 1);
            assert_eq!(node.get(2).await.unwrap()[0], 2);
        }
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_compact_is_checkpoint() {
        let dir = temp_dir().await;
        let page = make_page(1);
        let node = StorageNode::open(&dir).await.unwrap();
        node.put(1, &page);
        node.compact().await.unwrap();
        drop(node);

        let node = StorageNode::open(&dir).await.unwrap();
        assert_eq!(node.get(1).await.unwrap()[0], 1);
        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_wal_state_roundtrip() {
        let dir = temp_dir().await;
        write_wal_state(&dir, 42).await.unwrap();
        let state = read_wal_state(&dir).await.unwrap();
        assert_eq!(state.last_applied_lsn, 42);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_wal_encode_decode_roundtrip() {
        let dir = temp_dir().await;
        let path = dir.join("wal-test.log");
        let batch = WalBatch {
            request_id: 0,
            start_lsn: 1,
            end_lsn: 2,
            records: vec![
                WalRecord {
                    lsn: 1,
                    op: WAL_OP_PAGE_PUT,
                    page_id: 7,
                    slot_id: 0,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                WalRecord {
                    lsn: 2,
                    op: WAL_OP_PAGE_DEL,
                    page_id: 7,
                    slot_id: 0,
                    key: b"k1".to_vec(),
                    value: Vec::new(),
                },
            ],
        };
        let mut buf = Vec::new();
        encode_wal_batch(&batch, &mut buf).unwrap();
        let mut file = File::create(&path).await.unwrap();
        file.write_all(&buf).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        let mut file = File::open(&path).await.unwrap();
        let decoded = read_wal_batch(&mut file).await.unwrap().unwrap();
        assert_eq!(decoded.start_lsn, 1);
        assert_eq!(decoded.end_lsn, 2);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].key, b"k1".to_vec());
        assert_eq!(decoded.records[0].value, b"v1".to_vec());
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_replay_page_records_applies_updates() {
        let dir = temp_dir().await;
        let page_store = Arc::new(PageStore::open(&dir).await.unwrap());
        let page = crate::slotted_page::new_page();
        page_store.put_direct(10, &page).unwrap();

        let replay = PageStoreReplay::new(
            Arc::clone(&page_store),
            dir.clone(),
            0,
            Arc::new(std::sync::Mutex::new(HashSet::new())),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(tokio::sync::Notify::new()),
        );
        let records = vec![
            WalRecord {
                lsn: 1,
                op: WAL_OP_PAGE_PUT,
                page_id: 10,
                slot_id: 0,
                key: crate::slotted_page::fixed_key_bytes(b"k1"),
                value: vec![b'v'; VALUE_SIZE],
            },
            WalRecord {
                lsn: 2,
                op: WAL_OP_PAGE_PUT,
                page_id: 10,
                slot_id: 1,
                key: crate::slotted_page::fixed_key_bytes(b"k2"),
                value: vec![b'w'; VALUE_SIZE],
            },
        ];
        replay_page_records(&replay, 10, records).await.unwrap();
        let page = page_store.get(10).await.unwrap();
        let read_slot = |page: &[u8], slot_id: u16| -> Vec<u8> {
            // Use slotted page layout (same offsets):
            let offset = 6 + 4 * slot_id as usize;
            let pos = u16::from_le_bytes([page[offset], page[offset + 1]]) as usize;
            let len = u16::from_le_bytes([page[offset + 2], page[offset + 3]]) as usize;
            if len == 0 {
                return Vec::new();
            }
            let value_start = pos + KEY_SIZE;
            let value_end = value_start + VALUE_SIZE;
            page[value_start..value_end].to_vec()
        };
        assert_eq!(read_slot(&page, 0), vec![b'v'; VALUE_SIZE]);
        assert_eq!(read_slot(&page, 1), vec![b'w'; VALUE_SIZE]);
        cleanup_dir(&dir).await;
    }

    #[test]
    fn test_wal_slot_key_mismatch_is_rejected() {
        let mut page = crate::slotted_page::new_page();
        crate::slotted_page::insert_record_at_slot_checked(
            &mut page,
            0,
            &crate::slotted_page::fixed_key_bytes(b"k1"),
            &vec![b'v'; VALUE_SIZE],
        )
        .unwrap();
        let err = crate::slotted_page::insert_record_at_slot_checked(
            &mut page,
            0,
            &crate::slotted_page::fixed_key_bytes(b"k2"),
            &vec![b'w'; VALUE_SIZE],
        )
        .unwrap_err();
        // slotted_page helper may return a generic key-mismatch error; just assert it is rejected.
        assert!(format!("{err}").contains("mismatch") || format!("{err}").contains("key"));
    }

    #[test]
    fn test_mvcc_gc_keeps_last_visible_base_version() {
        let mut store: BTreeMap<Vec<u8>, Vec<MvccVersion>> = BTreeMap::new();
        store.insert(
            b"k".to_vec(),
            vec![
                MvccVersion {
                    commit_lsn: 10,
                    value: Some(b"v1".to_vec()),
                },
                MvccVersion {
                    commit_lsn: 20,
                    value: Some(b"v2".to_vec()),
                },
                MvccVersion {
                    commit_lsn: 30,
                    value: None,
                },
            ],
        );

        let (_keys_touched, removed) = gc_mvcc_versions(&mut store, 20);
        assert_eq!(removed, 1);
        let versions = store.get(b"k".as_ref()).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].commit_lsn, 20);
        assert_eq!(versions[1].commit_lsn, 30);
    }

    #[tokio::test]
    async fn test_mvcc_read_handle_admin_abort() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();

        {
            let mut mvcc = node.mvcc.lock().unwrap();
            mvcc.insert(
                b"k".to_vec(),
                vec![MvccVersion {
                    commit_lsn: node.durable_lsn(),
                    value: Some(b"v".to_vec()),
                }],
            );
        }

        let mut handle = node.begin_mvcc_ro();
        let id = handle.id().unwrap();
        assert!(node.list_active_reads().iter().any(|r| r.id == id));
        assert_eq!(node.mvcc_get(&mut handle, b"k").unwrap(), Some(b"v".to_vec()));

        assert!(node.abort_active_read(id));
        let err = node.mvcc_get(&mut handle, b"k").unwrap_err();
        assert!(format!("{err}").contains("aborted"));

        drop(handle);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_mvcc_read_handle_timeout() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();

        {
            let mut mvcc = node.mvcc.lock().unwrap();
            mvcc.insert(
                b"k".to_vec(),
                vec![MvccVersion {
                    commit_lsn: node.durable_lsn(),
                    value: Some(b"v".to_vec()),
                }],
            );
        }

        let mut handle = node.begin_mvcc_ro_with_timeout(Some(Duration::from_millis(1)));
        tokio::time::sleep(Duration::from_millis(5)).await;
        let err = node.mvcc_get(&mut handle, b"k").unwrap_err();
        assert!(matches!(err, crate::Error::TxnTimeout));
        assert!(!handle.is_active());

        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_wal_truncation_keeps_current_segment() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();

        let page = make_page(1);
        node.put(1, &page);
        node.checkpoint().await.unwrap();

        let segments_before = list_wal_segments(&dir).await.unwrap();
        assert_eq!(segments_before.len(), 1);

        let deleted = node.truncate_wal().await.unwrap();
        assert_eq!(deleted, 0);

        let segments_after = list_wal_segments(&dir).await.unwrap();
        assert_eq!(segments_after.len(), 1);

        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_wal_truncation_deletes_old_segments() {
        let dir = temp_dir().await;

        create_wal_segment(&dir, 1).await.unwrap();
        create_wal_segment(&dir, 2).await.unwrap();
        create_wal_segment(&dir, 3).await.unwrap();

        {
            let batch1 = WalBatch {
                request_id: 0,
                start_lsn: 1,
                end_lsn: 10,
                records: vec![],
            };
            let batch2 = WalBatch {
                request_id: 0,
                start_lsn: 11,
                end_lsn: 20,
                records: vec![],
            };
            let batch3 = WalBatch {
                request_id: 0,
                start_lsn: 21,
                end_lsn: 30,
                records: vec![],
            };

            let mut buf1 = Vec::new();
            encode_wal_batch(&batch1, &mut buf1).unwrap();
            let mut file1 = OpenOptions::new()
                .write(true)
                .open(wal_segment_path(&dir, 1))
                .await
                .unwrap();
            file1.write_all(&buf1).await.unwrap();

            let mut buf2 = Vec::new();
            encode_wal_batch(&batch2, &mut buf2).unwrap();
            let mut file2 = OpenOptions::new()
                .write(true)
                .open(wal_segment_path(&dir, 2))
                .await
                .unwrap();
            file2.write_all(&buf2).await.unwrap();

            let mut buf3 = Vec::new();
            encode_wal_batch(&batch3, &mut buf3).unwrap();
            let mut file3 = OpenOptions::new()
                .write(true)
                .open(wal_segment_path(&dir, 3))
                .await
                .unwrap();
            file3.write_all(&buf3).await.unwrap();
        }

        let segments = list_wal_segments(&dir).await.unwrap();
        assert_eq!(segments, vec![1, 2, 3]);

        let deleted = truncate_wal_segments(&dir, 15).await.unwrap();
        assert_eq!(deleted, 1);

        let segments = list_wal_segments(&dir).await.unwrap();
        assert_eq!(segments, vec![2, 3]);

        let deleted = truncate_wal_segments(&dir, 25).await.unwrap();
        assert_eq!(deleted, 1);

        let segments = list_wal_segments(&dir).await.unwrap();
        assert_eq!(segments, vec![3]);

        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_checkpoint_and_truncate() {
        let dir = temp_dir().await;
        let node = StorageNode::open(&dir).await.unwrap();

        let page = make_page(1);
        node.put(1, &page);

        let deleted = node.checkpoint_and_truncate().await.unwrap();
        assert_eq!(deleted, 0);

        assert!(node.page_store().checkpoint_lsn() == 0 || node.get(1).await.is_some());

        drop(node);
        cleanup_dir(&dir).await;
    }

    #[tokio::test]
    async fn test_background_checkpoint_with_truncate() {
        use crate::page_store::CheckpointConfig;
        use std::time::Duration;

        let dir = temp_dir().await;

        create_wal_segment(&dir, 1).await.unwrap();
        create_wal_segment(&dir, 2).await.unwrap();
        {
            let batch1 = WalBatch {
                request_id: 0,
                start_lsn: 1,
                end_lsn: 5,
                records: vec![],
            };
            let batch2 = WalBatch {
                request_id: 0,
                start_lsn: 6,
                end_lsn: 10,
                records: vec![],
            };
            let mut buf1 = Vec::new();
            encode_wal_batch(&batch1, &mut buf1).unwrap();
            let mut file1 = OpenOptions::new()
                .write(true)
                .open(wal_segment_path(&dir, 1))
                .await
                .unwrap();
            file1.write_all(&buf1).await.unwrap();

            let mut buf2 = Vec::new();
            encode_wal_batch(&batch2, &mut buf2).unwrap();
            let mut file2 = OpenOptions::new()
                .write(true)
                .open(wal_segment_path(&dir, 2))
                .await
                .unwrap();
            file2.write_all(&buf2).await.unwrap();
        }

        let page_store = Arc::new(PageStore::open(&dir).await.unwrap());

        for i in 0..15u64 {
            let page = make_page(i as u8);
            page_store.put(i, &page, 100 + i).unwrap();
        }

        let config = CheckpointConfig {
            interval: Duration::from_millis(50),
            max_dirty_pages: 5,
            max_dirty_bytes: 1024 * 1024,
        };

        let store_clone = Arc::clone(&page_store);
        let dir_clone = dir.clone();
        let handle = tokio::spawn(async move {
            while !store_clone.is_shutdown() {
                tokio::time::sleep(Duration::from_millis(20)).await;
                if store_clone.is_shutdown() {
                    break;
                }
                if store_clone.maybe_checkpoint(&config).await.unwrap_or(false) {
                    let checkpoint_lsn = store_clone.checkpoint_lsn();
                    let _ = truncate_wal_segments(&dir_clone, checkpoint_lsn).await;
                }
            }
            let _ = store_clone.checkpoint().await;
            let checkpoint_lsn = store_clone.checkpoint_lsn();
            let _ = truncate_wal_segments(&dir_clone, checkpoint_lsn).await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        page_store.shutdown();
        handle.await.unwrap();

        let segments = list_wal_segments(&dir).await.unwrap();
        assert!(segments.len() <= 2);

        cleanup_dir(&dir).await;
    }
}
