use crate::page_store::{CheckpointConfig, PageStore};
use crate::{ActiveReadInfo, ActiveReads, PAGE_SIZE, Page, PageId, ReadGuard, Result, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, oneshot};

#[path = "node_bootstrap.rs"]
mod node_bootstrap;
use node_bootstrap::{build_page_index, read_wal_state, write_wal_state};
#[path = "node_wal_files.rs"]
mod node_wal_files;
use node_wal_files::{
    create_wal_segment, list_wal_segments, truncate_wal_segments, wal_segment_path, wal_usage,
    wal_usage_exceeds_limits,
};
#[path = "node_wal_codec.rs"]
mod node_wal_codec;
pub use node_wal_codec::{
    WAL_OP_PAGE_DEL, WAL_OP_PAGE_IMAGE, WAL_OP_PAGE_PUT, WAL_OP_TXN_COMMIT, WAL_OP_TXN_DEL,
    WAL_OP_TXN_PUT, WalBatch, WalRecord,
};
use node_wal_codec::{encode_wal_batch, read_wal_batch, wal_batch_encoded_len};
#[path = "node_wal_replay.rs"]
mod node_wal_replay;
#[cfg(test)]
use node_wal_replay::replay_page_records;
use node_wal_replay::{apply_wal_record, replay_wal_segments_to_store, wal_replay_loop};
#[path = "node_wal_writer.rs"]
mod node_wal_writer;
use node_wal_writer::start_wal_writer;
#[path = "node_mvcc.rs"]
mod node_mvcc;
use node_mvcc::{apply_txn_batch_to_mvcc, gc_mvcc_versions, mvcc_get_at};
#[path = "node_maintenance.rs"]
mod node_maintenance;
use node_maintenance::{
    checkpoint_with_metrics, maybe_checkpoint_by_pressure, maybe_checkpoint_with_config,
};

#[cfg(test)]
const MAX_SEGMENT_SIZE: u64 = 8 * 1024;
#[cfg(not(test))]
const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StorageMaintenanceConfig {
    pub checkpoint: CheckpointConfig,
    pub truncate_wal: bool,
    pub max_wal_bytes: u64,
    pub max_wal_segments: usize,
    pub mvcc_gc_every_wal_batches: usize,
    pub wal_group_commit_max_batches: usize,
    pub wal_group_commit_wait_us: u64,
}

#[derive(Debug, Default)]
struct StorageMetricsInner {
    wal_backpressure_count: std::sync::atomic::AtomicU64,
    checkpoint_runs: std::sync::atomic::AtomicU64,
    checkpoint_total_duration_ms: std::sync::atomic::AtomicU64,
    checkpoint_last_duration_ms: std::sync::atomic::AtomicU64,
    gc_runs: std::sync::atomic::AtomicU64,
    gc_versions_removed_total: std::sync::atomic::AtomicU64,
    gc_last_duration_ms: std::sync::atomic::AtomicU64,
}

#[derive(Clone, Debug)]
pub struct StorageMetricsSnapshot {
    pub wal_segments: usize,
    pub wal_bytes: u64,
    pub wal_backpressure_count: u64,
    pub checkpoint_runs: u64,
    pub checkpoint_total_duration_ms: u64,
    pub checkpoint_last_duration_ms: u64,
    pub gc_runs: u64,
    pub gc_versions_removed_total: u64,
    pub gc_last_duration_ms: u64,
    pub mvcc_keys: usize,
    pub mvcc_versions: usize,
    pub mvcc_avg_versions_per_key: f64,
    pub active_reads: usize,
    pub page_cache_resident_pages: usize,
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
}

impl StorageMetricsSnapshot {
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("scale_kv_wal_segments {}\n", self.wal_segments));
        out.push_str(&format!("scale_kv_wal_bytes {}\n", self.wal_bytes));
        out.push_str(&format!(
            "scale_kv_wal_backpressure_count {}\n",
            self.wal_backpressure_count
        ));
        out.push_str(&format!(
            "scale_kv_checkpoint_runs_total {}\n",
            self.checkpoint_runs
        ));
        out.push_str(&format!(
            "scale_kv_checkpoint_duration_ms_total {}\n",
            self.checkpoint_total_duration_ms
        ));
        out.push_str(&format!(
            "scale_kv_checkpoint_duration_ms_last {}\n",
            self.checkpoint_last_duration_ms
        ));
        out.push_str(&format!("scale_kv_gc_runs_total {}\n", self.gc_runs));
        out.push_str(&format!(
            "scale_kv_gc_versions_removed_total {}\n",
            self.gc_versions_removed_total
        ));
        out.push_str(&format!(
            "scale_kv_gc_duration_ms_last {}\n",
            self.gc_last_duration_ms
        ));
        out.push_str(&format!("scale_kv_mvcc_keys {}\n", self.mvcc_keys));
        out.push_str(&format!("scale_kv_mvcc_versions {}\n", self.mvcc_versions));
        out.push_str(&format!(
            "scale_kv_mvcc_versions_per_key {}\n",
            self.mvcc_avg_versions_per_key
        ));
        out.push_str(&format!("scale_kv_active_reads {}\n", self.active_reads));
        out.push_str(&format!(
            "scale_kv_page_cache_resident_pages {}\n",
            self.page_cache_resident_pages
        ));
        out.push_str(&format!(
            "scale_kv_page_cache_hits {}\n",
            self.page_cache_hits
        ));
        out.push_str(&format!(
            "scale_kv_page_cache_misses {}\n",
            self.page_cache_misses
        ));
        out
    }

    pub fn render_json(&self) -> String {
        format!(
            "{{\"wal\":{{\"segments\":{},\"bytes\":{},\"backpressure_count\":{}}},\"checkpoint\":{{\"runs\":{},\"duration_ms_total\":{},\"duration_ms_last\":{}}},\"gc\":{{\"runs\":{},\"versions_removed_total\":{},\"duration_ms_last\":{}}},\"mvcc\":{{\"keys\":{},\"versions\":{},\"avg_versions_per_key\":{:.3},\"active_reads\":{}}},\"cache\":{{\"resident_pages\":{},\"hits\":{},\"misses\":{}}}}}",
            self.wal_segments,
            self.wal_bytes,
            self.wal_backpressure_count,
            self.checkpoint_runs,
            self.checkpoint_total_duration_ms,
            self.checkpoint_last_duration_ms,
            self.gc_runs,
            self.gc_versions_removed_total,
            self.gc_last_duration_ms,
            self.mvcc_keys,
            self.mvcc_versions,
            self.mvcc_avg_versions_per_key,
            self.active_reads,
            self.page_cache_resident_pages,
            self.page_cache_hits,
            self.page_cache_misses
        )
    }
}

impl Default for StorageMaintenanceConfig {
    fn default() -> Self {
        Self {
            checkpoint: CheckpointConfig::default(),
            truncate_wal: true,
            max_wal_bytes: 512 * 1024 * 1024, // 512MB
            max_wal_segments: 64,
            mvcc_gc_every_wal_batches: 128,
            wal_group_commit_max_batches: 64,
            wal_group_commit_wait_us: 200,
        }
    }
}

impl StorageMaintenanceConfig {
    pub fn validate(&self) -> Result<()> {
        if self.checkpoint.interval.is_zero() {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "checkpoint.interval must be > 0",
            )));
        }
        if self.checkpoint.max_dirty_pages == 0 {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "checkpoint.max_dirty_pages must be > 0",
            )));
        }
        if self.checkpoint.max_dirty_bytes == 0 {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "checkpoint.max_dirty_bytes must be > 0",
            )));
        }
        if self.max_wal_bytes == 0 {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "max_wal_bytes must be > 0",
            )));
        }
        if self.max_wal_segments == 0 {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "max_wal_segments must be > 0",
            )));
        }
        if self.mvcc_gc_every_wal_batches == 0 {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "mvcc_gc_every_wal_batches must be > 0",
            )));
        }
        if self.wal_group_commit_max_batches == 0 {
            return Err(crate::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "wal_group_commit_max_batches must be > 0",
            )));
        }
        Ok(())
    }

    pub fn render_json(&self) -> String {
        format!(
            "{{\"checkpoint\":{{\"interval_secs\":{},\"max_dirty_pages\":{},\"max_dirty_bytes\":{}}},\"truncate_wal\":{},\"max_wal_bytes\":{},\"max_wal_segments\":{},\"mvcc_gc_every_wal_batches\":{},\"wal_group_commit_max_batches\":{},\"wal_group_commit_wait_us\":{}}}",
            self.checkpoint.interval.as_secs_f64(),
            self.checkpoint.max_dirty_pages,
            self.checkpoint.max_dirty_bytes,
            self.truncate_wal,
            self.max_wal_bytes,
            self.max_wal_segments,
            self.mvcc_gc_every_wal_batches,
            self.wal_group_commit_max_batches,
            self.wal_group_commit_wait_us
        )
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
    metrics: Arc<StorageMetricsInner>,
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
            .unwrap_or_else(|| Page::from(vec![0u8; PAGE_SIZE]));
        let mut page = page.to_vec();
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

#[derive(Clone, Copy, Debug)]
struct WalWriterConfig {
    max_group_commit_batches: usize,
    group_commit_wait_us: u64,
}

struct WalState {
    last_applied_lsn: u64,
}

// (slotted page helpers moved to src/slotted_page.rs)

impl StorageNode {
    pub async fn new() -> Self {
        Self::open("data").await.expect("failed to open storage")
    }

    pub async fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        Self::open_with_maintenance(dir, StorageMaintenanceConfig::default()).await
    }

    pub async fn open_with_maintenance<P: AsRef<Path>>(
        dir: P,
        maintenance: StorageMaintenanceConfig,
    ) -> Result<Self> {
        maintenance.validate()?;
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
        let metrics = Arc::new(StorageMetricsInner::default());
        replay_wal_segments_to_store(
            &dir,
            Arc::clone(&page_store),
            last_applied_lsn,
            Arc::clone(&page_index),
            &mvcc,
            &request_index,
        )
        .await?;
        let wal_state_after_replay = read_wal_state(&dir)
            .await
            .unwrap_or(WalState { last_applied_lsn });
        let recovered_last_applied = wal_state_after_replay
            .last_applied_lsn
            .max(last_applied_lsn);
        let rebuilt_index = build_page_index(&page_store).await;
        *page_index.lock().unwrap() = rebuilt_index;

        // Initialize durable_lsn from the last applied point.
        // We treat `durable_lsn` as the right boundary (exclusive): all records with lsn < durable_lsn are durable.
        // NOTE: in the quorum design, durable_lsn should reflect quorum-durable; for now it's local.
        let durable_init = recovered_last_applied.saturating_add(1);
        let durable_lsn = Arc::new(std::sync::atomic::AtomicU64::new(durable_init));

        let wal_cfg = WalWriterConfig {
            max_group_commit_batches: maintenance.wal_group_commit_max_batches,
            group_commit_wait_us: maintenance.wal_group_commit_wait_us,
        };
        let (wal_sender, wal_replay_rx) =
            start_wal_writer(dir.clone(), durable_lsn.clone(), wal_cfg).await?;

        let applied_lsn = Arc::new(std::sync::atomic::AtomicU64::new(recovered_last_applied));
        let applied_notify = Arc::new(tokio::sync::Notify::new());

        let node = Self {
            dir: dir.clone(),
            page_store: Arc::clone(&page_store),
            page_index: Arc::clone(&page_index),
            wal_sender: Mutex::new(Some(wal_sender)),
            wal_replay_rx: Mutex::new(Some(wal_replay_rx)),
            durable_lsn,
            applied_lsn,
            applied_notify,
            mvcc,
            request_index,
            active_reads,
            maintenance: maintenance.clone(),
            wal_limit_guard: Mutex::new(()),
            metrics: Arc::clone(&metrics),
        };

        node.start_wal_replay(recovered_last_applied).await;
        Ok(node)
    }

    pub async fn append_wal_batch(&self, batch: WalBatch) -> Result<()> {
        self.validate_incoming_wal_batch(&batch)?;
        self.enforce_wal_limits(wal_batch_encoded_len(&batch))
            .await?;
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

    pub async fn metrics_snapshot(&self) -> StorageMetricsSnapshot {
        let (wal_segments, wal_bytes) = wal_usage(&self.dir).await.unwrap_or((0, 0));
        let (mvcc_keys, mvcc_versions) = {
            let store = self.mvcc.lock().unwrap();
            let keys = store.len();
            let versions = store.values().map(std::vec::Vec::len).sum::<usize>();
            (keys, versions)
        };
        let avg_versions = if mvcc_keys == 0 {
            0.0
        } else {
            mvcc_versions as f64 / mvcc_keys as f64
        };
        let active_reads = self.active_reads.list().len();
        let cache_stats = self.page_store.cache_stats();

        StorageMetricsSnapshot {
            wal_segments,
            wal_bytes,
            wal_backpressure_count: self
                .metrics
                .wal_backpressure_count
                .load(std::sync::atomic::Ordering::Relaxed),
            checkpoint_runs: self
                .metrics
                .checkpoint_runs
                .load(std::sync::atomic::Ordering::Relaxed),
            checkpoint_total_duration_ms: self
                .metrics
                .checkpoint_total_duration_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            checkpoint_last_duration_ms: self
                .metrics
                .checkpoint_last_duration_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            gc_runs: self
                .metrics
                .gc_runs
                .load(std::sync::atomic::Ordering::Relaxed),
            gc_versions_removed_total: self
                .metrics
                .gc_versions_removed_total
                .load(std::sync::atomic::Ordering::Relaxed),
            gc_last_duration_ms: self
                .metrics
                .gc_last_duration_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            mvcc_keys,
            mvcc_versions,
            mvcc_avg_versions_per_key: avg_versions,
            active_reads,
            page_cache_resident_pages: self.page_store.len(),
            page_cache_hits: cache_stats.hits,
            page_cache_misses: cache_stats.misses,
        }
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
        if self.wal_usage_exceeds(incoming_bytes).await?.is_none() {
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

        checkpoint_with_metrics(&self.page_store, &self.metrics).await?;
        if self.maintenance.truncate_wal {
            let _ = self.truncate_wal().await?;
        }

        if let Some((segments, bytes)) = self.wal_usage_exceeds(incoming_bytes).await? {
            self.metrics
                .wal_backpressure_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                value: page.to_vec(),
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
        self.enforce_wal_limits(wal_batch_encoded_len(&batch))
            .await?;
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
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            wal_replay_loop(
                replay,
                rx,
                mvcc,
                req_index,
                active_reads,
                durable_lsn,
                maintenance,
                metrics,
            )
            .await;
        });
    }

    pub async fn checkpoint(&self) -> Result<()> {
        checkpoint_with_metrics(&self.page_store, &self.metrics).await
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
        self.page_store.get(key).await.map(|p| p.to_vec())
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
        checkpoint_with_metrics(&self.page_store, &self.metrics).await
    }

    pub async fn truncate_wal(&self) -> Result<usize> {
        let checkpoint_lsn = self.page_store.checkpoint_lsn();
        truncate_wal_segments(&self.dir, checkpoint_lsn).await
    }

    pub async fn checkpoint_and_truncate(&self) -> Result<usize> {
        checkpoint_with_metrics(&self.page_store, &self.metrics).await?;
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
        let metrics = Arc::clone(&self.metrics);
        let poll_interval = config.interval.min(std::time::Duration::from_secs(1));

        tokio::spawn(async move {
            while !page_store.is_shutdown() {
                tokio::time::sleep(poll_interval).await;
                if page_store.is_shutdown() {
                    break;
                }
                if maybe_checkpoint_with_config(&page_store, &config, &metrics)
                    .await
                    .unwrap_or(false)
                    && truncate_wal
                {
                    let checkpoint_lsn = page_store.checkpoint_lsn();
                    let _ = truncate_wal_segments(&dir, checkpoint_lsn).await;
                }
            }
            let _ = checkpoint_with_metrics(&page_store, &metrics).await;
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

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
