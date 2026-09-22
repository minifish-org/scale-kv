use crate::{PAGE_SIZE, Page, PageId, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const PAGE_FILE_NAME: &str = "pages.dat";
const CHECKPOINT_STATE_FILE: &str = "checkpoint_state";
const CHECKPOINT_STATE_TMP: &str = "checkpoint_state.tmp";

/// Configuration for periodic checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Checkpoint interval (default: 60 seconds)
    pub interval: Duration,
    /// Max dirty pages before triggering checkpoint (default: 1000)
    pub max_dirty_pages: usize,
    /// Max dirty bytes before triggering checkpoint (default: 64MB)
    pub max_dirty_bytes: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            max_dirty_pages: 1000,
            max_dirty_bytes: 64 * 1024 * 1024, // 64MB
        }
    }
}

impl CheckpointConfig {
    /// Create a config for testing with shorter intervals.
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            interval: Duration::from_millis(100),
            max_dirty_pages: 10,
            max_dirty_bytes: 64 * 1024, // 64KB
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    pub max_pages: usize,
    pub eviction_batch_size: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_pages: 10000,
            eviction_batch_size: 100,
        }
    }
}

impl BufferPoolConfig {
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            max_pages: 20,
            eviction_batch_size: 5,
        }
    }
}

struct BufferPage {
    data: Box<[u8; PAGE_SIZE]>,
    dirty: bool,
    lsn: u64,
    last_accessed: Instant,
}

impl BufferPage {
    fn new(data: [u8; PAGE_SIZE], dirty: bool, lsn: u64) -> Self {
        Self {
            data: Box::new(data),
            dirty,
            lsn,
            last_accessed: Instant::now(),
        }
    }

    fn from_slice(slice: &[u8], dirty: bool, lsn: u64) -> Result<Self> {
        if slice.len() != PAGE_SIZE {
            return Err(crate::Error::InvalidPageSize(slice.len(), PAGE_SIZE));
        }
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(slice);
        Ok(Self::new(data, dirty, lsn))
    }

    fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}

struct PageFile {
    file: File,
    size: u64,
}

impl PageFile {
    async fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .await?;
        let size = file.metadata().await?.len();
        Ok(Self { file, size })
    }

    async fn read_page(&mut self, page_id: PageId) -> Result<Option<Page>> {
        use std::io::SeekFrom;
        let offset = page_id * PAGE_SIZE as u64;

        if offset + PAGE_SIZE as u64 > self.size {
            return Ok(None);
        }

        self.file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; PAGE_SIZE];
        match self.file.read_exact(&mut buf).await {
            Ok(_) => {
                if buf.iter().all(|&b| b == 0) {
                    return Ok(None);
                }
                Ok(Some(Page::from(buf)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        use std::io::SeekFrom;
        if data.len() != PAGE_SIZE {
            return Err(crate::Error::InvalidPageSize(data.len(), PAGE_SIZE));
        }

        let offset = page_id * PAGE_SIZE as u64;
        let required_size = offset + PAGE_SIZE as u64;

        if required_size > self.size {
            self.file.set_len(required_size).await?;
            self.size = required_size;
        }

        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(data).await?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    pub total_pages: usize,
    pub dirty_count: usize,
    pub dirty_bytes: usize,
    pub max_lsn: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
}

pub struct PageStore {
    dir: PathBuf,
    buffer_pool: RwLock<HashMap<PageId, BufferPage>>,
    page_file: Mutex<PageFile>,
    checkpoint_lsn: AtomicU64,
    max_page_id: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    #[cfg(test)]
    fail_next_checkpoint_sync: AtomicBool,
    last_checkpoint: Mutex<Instant>,
    shutdown: AtomicBool,
}

impl PageStore {
    pub async fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;

        let checkpoint_lsn = read_checkpoint_state(&dir).await.unwrap_or(0);
        let page_file_path = dir.join(PAGE_FILE_NAME);
        let page_file = PageFile::open(&page_file_path).await?;

        let max_page_id = if page_file.size > 0 {
            (page_file.size / PAGE_SIZE as u64).saturating_sub(1)
        } else {
            0
        };

        Ok(Self {
            dir,
            buffer_pool: RwLock::new(HashMap::new()),
            page_file: Mutex::new(page_file),
            checkpoint_lsn: AtomicU64::new(checkpoint_lsn),
            max_page_id: AtomicU64::new(max_page_id),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            #[cfg(test)]
            fail_next_checkpoint_sync: AtomicBool::new(false),
            last_checkpoint: Mutex::new(Instant::now()),
            shutdown: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    pub fn inject_fail_next_checkpoint_sync(&self) {
        self.fail_next_checkpoint_sync
            .store(true, Ordering::Release);
    }

    pub async fn get(&self, page_id: PageId) -> Option<Page> {
        self.get_with_lsn(page_id).await.map(|(p, _lsn)| p)
    }

    /// Get the latest page bytes and a best-effort page LSN.
    ///
    /// Note: pages persisted in `pages.dat` currently do not encode LSN, so pages
    /// loaded from disk will return `page_lsn=0`.
    pub async fn get_with_lsn(&self, page_id: PageId) -> Option<(Page, u64)> {
        {
            let mut pool = self.buffer_pool.write().unwrap();
            if let Some(bp) = pool.get_mut(&page_id) {
                bp.touch();
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Some((Page::copy_from_slice(&bp.data[..]), bp.lsn));
            }
        }

        let mut pf = self.page_file.lock().await;
        match pf.read_page(page_id).await {
            Ok(Some(data)) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                Some((data, 0))
            }
            Ok(None) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Scan pages starting from `start_page_id` and return up to `limit` pages.
    ///
    /// This is "sparse"-friendly: it will skip missing page ids and continue searching
    /// until it collects `limit` pages or reaches the current max page id.
    pub async fn scan_pages_with_lsn(
        &self,
        start_page_id: PageId,
        limit: usize,
    ) -> Vec<(PageId, u64, Page)> {
        let mut out = Vec::new();
        if limit == 0 {
            return out;
        }
        let max_id = self.max_page_id.load(Ordering::Acquire);
        for page_id in start_page_id..=max_id {
            if let Some((page, lsn)) = self.get_with_lsn(page_id).await {
                out.push((page_id, lsn, page));
                if out.len() >= limit {
                    break;
                }
            }
        }
        out
    }

    pub fn put(&self, page_id: PageId, data: &[u8], lsn: u64) -> Result<()> {
        let bp = BufferPage::from_slice(data, true, lsn)?;

        let mut pool = self.buffer_pool.write().unwrap();
        pool.insert(page_id, bp);

        let _ = self.max_page_id.fetch_max(page_id, Ordering::Relaxed);

        Ok(())
    }

    pub fn put_direct(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        self.put(page_id, data, 0)
    }

    pub fn delete(&self, page_id: PageId) {
        let mut pool = self.buffer_pool.write().unwrap();
        pool.remove(&page_id);
    }

    pub async fn contains(&self, page_id: PageId) -> bool {
        self.get(page_id).await.is_some()
    }

    pub fn buffer_stats(&self) -> BufferStats {
        let pool = self.buffer_pool.read().unwrap();
        let mut stats = BufferStats {
            total_pages: pool.len(),
            ..BufferStats::default()
        };

        for bp in pool.values() {
            if bp.dirty {
                stats.dirty_count += 1;
                stats.dirty_bytes += PAGE_SIZE;
            }
            if bp.lsn > stats.max_lsn {
                stats.max_lsn = bp.lsn;
            }
        }

        stats
    }

    pub fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn.load(Ordering::Acquire)
    }

    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            hits: self.cache_hits.load(Ordering::Relaxed),
            misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }

    pub fn max_page_id(&self) -> PageId {
        self.max_page_id.load(Ordering::Relaxed)
    }

    pub async fn checkpoint(&self) -> Result<()> {
        let dirty_pages: Vec<(PageId, Vec<u8>, u64)> = {
            let pool = self.buffer_pool.read().unwrap();
            let mut pages: Vec<_> = pool
                .iter()
                .filter(|(_, bp)| bp.dirty)
                .map(|(id, bp)| (*id, bp.data.to_vec(), bp.lsn))
                .collect();
            pages.sort_by_key(|(id, _, _)| *id);
            pages
        };

        if dirty_pages.is_empty() {
            return Ok(());
        }

        let mut max_lsn = 0u64;
        {
            let mut pf = self.page_file.lock().await;
            for (page_id, data, lsn) in &dirty_pages {
                pf.write_page(*page_id, data).await?;
                if *lsn > max_lsn {
                    max_lsn = *lsn;
                }
            }
            #[cfg(test)]
            if self.fail_next_checkpoint_sync.swap(false, Ordering::AcqRel) {
                return Err(std::io::Error::other("injected checkpoint sync failure").into());
            }
            pf.sync().await?;
        }

        {
            let mut pool = self.buffer_pool.write().unwrap();
            for (page_id, _, _) in &dirty_pages {
                if let Some(bp) = pool.get_mut(page_id) {
                    bp.dirty = false;
                }
            }
        }

        if max_lsn > self.checkpoint_lsn.load(Ordering::Acquire) {
            self.checkpoint_lsn.store(max_lsn, Ordering::Release);
            write_checkpoint_state(&self.dir, max_lsn).await?;
        }

        *self.last_checkpoint.lock().await = Instant::now();

        Ok(())
    }

    pub async fn should_checkpoint(&self, config: &CheckpointConfig) -> bool {
        let stats = self.buffer_stats();
        let elapsed = self.last_checkpoint.lock().await.elapsed();

        stats.dirty_count > config.max_dirty_pages
            || stats.dirty_bytes > config.max_dirty_bytes
            || elapsed > config.interval
    }

    pub async fn maybe_checkpoint(&self, config: &CheckpointConfig) -> Result<bool> {
        if self.should_checkpoint(config).await {
            self.checkpoint().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn evict_cold_pages(&self, config: &BufferPoolConfig) -> Result<usize> {
        let current_count = self.buffer_pool.read().unwrap().len();
        if current_count <= config.max_pages {
            return Ok(0);
        }

        let to_evict = current_count - config.max_pages + config.eviction_batch_size;

        let candidates: Vec<(PageId, Instant, bool)> = {
            let pool = self.buffer_pool.read().unwrap();
            pool.iter()
                .map(|(id, bp)| (*id, bp.last_accessed, bp.dirty))
                .collect()
        };

        let mut sorted: Vec<_> = candidates.into_iter().collect();
        sorted.sort_by_key(|(_, accessed, _)| *accessed);

        let mut evicted = 0;
        let mut dirty_to_flush: Vec<(PageId, Vec<u8>)> = Vec::new();

        {
            let pool = self.buffer_pool.read().unwrap();
            for (page_id, _, is_dirty) in sorted.iter().take(to_evict) {
                if *is_dirty && let Some(bp) = pool.get(page_id) {
                    dirty_to_flush.push((*page_id, bp.data.to_vec()));
                }
            }
        }

        if !dirty_to_flush.is_empty() {
            let mut pf = self.page_file.lock().await;
            for (page_id, data) in &dirty_to_flush {
                pf.write_page(*page_id, data).await?;
            }
            pf.sync().await?;
        }

        {
            let mut pool = self.buffer_pool.write().unwrap();
            for (page_id, _, _) in sorted.iter().take(to_evict) {
                if pool.remove(page_id).is_some() {
                    evicted += 1;
                }
            }
        }

        Ok(evicted)
    }

    pub async fn maybe_evict(&self, config: &BufferPoolConfig) -> Result<usize> {
        let current_count = self.buffer_pool.read().unwrap().len();
        if current_count > config.max_pages {
            self.evict_cold_pages(config).await
        } else {
            Ok(0)
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub fn start_background_checkpoint(
        self: &Arc<Self>,
        config: CheckpointConfig,
    ) -> JoinHandle<()> {
        let store = Arc::clone(self);
        let poll_interval = config.interval.min(Duration::from_secs(1));

        tokio::spawn(async move {
            while !store.is_shutdown() {
                tokio::time::sleep(poll_interval).await;
                if store.is_shutdown() {
                    break;
                }
                let _ = store.maybe_checkpoint(&config).await;
            }
            let _ = store.checkpoint().await;
        })
    }

    pub fn start_background_checkpoint_with_eviction(
        self: &Arc<Self>,
        checkpoint_config: CheckpointConfig,
        buffer_config: BufferPoolConfig,
    ) -> JoinHandle<()> {
        let store = Arc::clone(self);
        let poll_interval = checkpoint_config.interval.min(Duration::from_secs(1));

        tokio::spawn(async move {
            while !store.is_shutdown() {
                tokio::time::sleep(poll_interval).await;
                if store.is_shutdown() {
                    break;
                }
                let _ = store.maybe_checkpoint(&checkpoint_config).await;
                let _ = store.maybe_evict(&buffer_config).await;
            }
            let _ = store.checkpoint().await;
        })
    }

    pub fn len(&self) -> usize {
        self.buffer_pool.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer_pool.read().unwrap().is_empty()
    }

    pub fn keys(&self) -> Vec<PageId> {
        self.buffer_pool.read().unwrap().keys().cloned().collect()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

async fn read_checkpoint_state(dir: &Path) -> Result<u64> {
    use tokio::io::AsyncReadExt;
    let path = dir.join(CHECKPOINT_STATE_FILE);
    let mut file = match File::open(&path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };

    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

async fn write_checkpoint_state(dir: &Path, lsn: u64) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let tmp_path = dir.join(CHECKPOINT_STATE_TMP);
    let path = dir.join(CHECKPOINT_STATE_FILE);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
        file.write_all(&lsn.to_le_bytes()).await?;
        file.sync_all().await?;
    }

    fs::rename(tmp_path, path).await?;
    Ok(())
}

#[cfg(test)]
#[path = "page_store_tests.rs"]
mod tests;
