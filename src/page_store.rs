use crate::{Page, PageId, Result, PAGE_SIZE};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let size = file.metadata()?.len();
        Ok(Self { file, size })
    }

    fn read_page(&mut self, page_id: PageId) -> Result<Option<Page>> {
        let offset = page_id * PAGE_SIZE as u64;

        if offset + PAGE_SIZE as u64 > self.size {
            return Ok(None);
        }

        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        match self.file.read_exact(&mut buf) {
            Ok(()) => {
                if buf.iter().all(|&b| b == 0) {
                    return Ok(None);
                }
                Ok(Some(buf))
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(crate::Error::InvalidPageSize(data.len(), PAGE_SIZE));
        }

        let offset = page_id * PAGE_SIZE as u64;
        let required_size = offset + PAGE_SIZE as u64;

        if required_size > self.size {
            self.file.set_len(required_size)?;
            self.size = required_size;
        }

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
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

pub struct PageStore {
    dir: PathBuf,
    buffer_pool: RwLock<HashMap<PageId, BufferPage>>,
    page_file: Mutex<PageFile>,
    checkpoint_lsn: AtomicU64,
    max_page_id: AtomicU64,
    last_checkpoint: Mutex<Instant>,
    shutdown: AtomicBool,
}

impl PageStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let checkpoint_lsn = read_checkpoint_state(&dir).unwrap_or(0);
        let page_file_path = dir.join(PAGE_FILE_NAME);
        let page_file = PageFile::open(&page_file_path)?;

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
            last_checkpoint: Mutex::new(Instant::now()),
            shutdown: AtomicBool::new(false),
        })
    }

    pub fn get(&self, page_id: PageId) -> Option<Page> {
        {
            let mut pool = self.buffer_pool.write().unwrap();
            if let Some(bp) = pool.get_mut(&page_id) {
                bp.touch();
                return Some(bp.data.to_vec());
            }
        }

        let mut pf = self.page_file.lock().unwrap();
        match pf.read_page(page_id) {
            Ok(Some(data)) => Some(data),
            Ok(None) => None,
            Err(_) => None,
        }
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

    pub fn contains(&self, page_id: PageId) -> bool {
        self.get(page_id).is_some()
    }

    pub fn buffer_stats(&self) -> BufferStats {
        let pool = self.buffer_pool.read().unwrap();
        let mut stats = BufferStats::default();
        stats.total_pages = pool.len();

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

    pub fn max_page_id(&self) -> PageId {
        self.max_page_id.load(Ordering::Relaxed)
    }

    pub fn checkpoint(&self) -> Result<()> {
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
            let mut pf = self.page_file.lock().unwrap();
            for (page_id, data, lsn) in &dirty_pages {
                pf.write_page(*page_id, data)?;
                if *lsn > max_lsn {
                    max_lsn = *lsn;
                }
            }
            pf.sync()?;
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
            write_checkpoint_state(&self.dir, max_lsn)?;
        }

        *self.last_checkpoint.lock().unwrap() = Instant::now();

        Ok(())
    }

    pub fn should_checkpoint(&self, config: &CheckpointConfig) -> bool {
        let stats = self.buffer_stats();
        let elapsed = self.last_checkpoint.lock().unwrap().elapsed();

        stats.dirty_count > config.max_dirty_pages
            || stats.dirty_bytes > config.max_dirty_bytes
            || elapsed > config.interval
    }

    pub fn maybe_checkpoint(&self, config: &CheckpointConfig) -> Result<bool> {
        if self.should_checkpoint(config) {
            self.checkpoint()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn evict_cold_pages(&self, config: &BufferPoolConfig) -> Result<usize> {
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
                if *is_dirty {
                    if let Some(bp) = pool.get(page_id) {
                        dirty_to_flush.push((*page_id, bp.data.to_vec()));
                    }
                }
            }
        }

        if !dirty_to_flush.is_empty() {
            let mut pf = self.page_file.lock().unwrap();
            for (page_id, data) in &dirty_to_flush {
                pf.write_page(*page_id, data)?;
            }
            pf.sync()?;
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

    pub fn maybe_evict(&self, config: &BufferPoolConfig) -> Result<usize> {
        let current_count = self.buffer_pool.read().unwrap().len();
        if current_count > config.max_pages {
            self.evict_cold_pages(config)
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

        thread::spawn(move || {
            while !store.is_shutdown() {
                thread::sleep(poll_interval);
                if store.is_shutdown() {
                    break;
                }
                let _ = store.maybe_checkpoint(&config);
            }
            let _ = store.checkpoint();
        })
    }

    pub fn start_background_checkpoint_with_eviction(
        self: &Arc<Self>,
        checkpoint_config: CheckpointConfig,
        buffer_config: BufferPoolConfig,
    ) -> JoinHandle<()> {
        let store = Arc::clone(self);
        let poll_interval = checkpoint_config.interval.min(Duration::from_secs(1));

        thread::spawn(move || {
            while !store.is_shutdown() {
                thread::sleep(poll_interval);
                if store.is_shutdown() {
                    break;
                }
                let _ = store.maybe_checkpoint(&checkpoint_config);
                let _ = store.maybe_evict(&buffer_config);
            }
            let _ = store.checkpoint();
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

fn read_checkpoint_state(dir: &Path) -> Result<u64> {
    let path = dir.join(CHECKPOINT_STATE_FILE);
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };

    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn write_checkpoint_state(dir: &Path, lsn: u64) -> Result<()> {
    let tmp_path = dir.join(CHECKPOINT_STATE_TMP);
    let path = dir.join(CHECKPOINT_STATE_FILE);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&lsn.to_le_bytes())?;
        file.sync_all()?;
    }

    fs::rename(tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!(
            "scale-kv-pagestore-test-{}-{}",
            std::process::id(),
            id
        ));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn cleanup_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    fn make_page(fill: u8) -> Vec<u8> {
        let mut page = vec![0u8; PAGE_SIZE];
        page[0] = fill;
        page[PAGE_SIZE - 1] = fill;
        page
    }

    #[test]
    fn test_open_empty() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.checkpoint_lsn(), 0);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_put_and_get() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        let page1 = make_page(1);
        store.put(1, &page1, 100).unwrap();

        let retrieved = store.get(1).unwrap();
        assert_eq!(retrieved, page1);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_get_missing() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        assert!(store.get(999).is_none());
        cleanup_dir(&dir);
    }

    #[test]
    fn test_overwrite() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        let page1 = make_page(1);
        let page2 = make_page(2);

        store.put(1, &page1, 100).unwrap();
        store.put(1, &page2, 101).unwrap();

        let retrieved = store.get(1).unwrap();
        assert_eq!(retrieved, page2);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_checkpoint_persists() {
        let dir = temp_dir();

        // 写入并 checkpoint
        {
            let store = PageStore::open(&dir).unwrap();
            let page1 = make_page(1);
            let page2 = make_page(2);
            store.put(1, &page1, 100).unwrap();
            store.put(2, &page2, 101).unwrap();
            store.checkpoint().unwrap();

            assert_eq!(store.checkpoint_lsn(), 101);
        }

        // 重新打开，验证数据持久化
        {
            let store = PageStore::open(&dir).unwrap();
            assert_eq!(store.checkpoint_lsn(), 101);

            let page1 = store.get(1).unwrap();
            let page2 = store.get(2).unwrap();
            assert_eq!(page1[0], 1);
            assert_eq!(page2[0], 2);
        }

        cleanup_dir(&dir);
    }

    #[test]
    fn test_dirty_tracking() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        let page = make_page(1);
        store.put(1, &page, 100).unwrap();

        let stats = store.buffer_stats();
        assert_eq!(stats.dirty_count, 1);
        assert_eq!(stats.dirty_bytes, PAGE_SIZE);

        store.checkpoint().unwrap();

        let stats = store.buffer_stats();
        assert_eq!(stats.dirty_count, 0);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_sparse_pages() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        // 写入非连续的 page_id
        let page1 = make_page(1);
        let page100 = make_page(100);

        store.put(1, &page1, 100).unwrap();
        store.put(100, &page100, 101).unwrap();
        store.checkpoint().unwrap();

        // 重新打开验证
        drop(store);
        let store = PageStore::open(&dir).unwrap();

        assert!(store.get(1).is_some());
        assert!(store.get(50).is_none()); // 中间的 page 不存在
        assert!(store.get(100).is_some());
        cleanup_dir(&dir);
    }

    #[test]
    fn test_max_page_id() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        let page = make_page(1);
        store.put(5, &page, 100).unwrap();
        store.put(10, &page, 101).unwrap();
        store.put(3, &page, 102).unwrap();

        assert_eq!(store.max_page_id(), 10);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_invalid_page_size() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        let bad_page = vec![0u8; 100]; // 错误的大小
        let result = store.put(1, &bad_page, 100);
        assert!(result.is_err());
        cleanup_dir(&dir);
    }

    #[test]
    fn test_multiple_checkpoints() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();

        // 第一轮写入
        let page1 = make_page(1);
        store.put(1, &page1, 100).unwrap();
        store.checkpoint().unwrap();
        assert_eq!(store.checkpoint_lsn(), 100);

        // 第二轮写入
        let page2 = make_page(2);
        store.put(2, &page2, 200).unwrap();
        store.checkpoint().unwrap();
        assert_eq!(store.checkpoint_lsn(), 200);

        // 验证两个 page 都存在
        assert!(store.get(1).is_some());
        assert!(store.get(2).is_some());
        cleanup_dir(&dir);
    }

    #[test]
    fn test_should_checkpoint_by_dirty_count() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = CheckpointConfig {
            interval: Duration::from_secs(3600),
            max_dirty_pages: 5,
            max_dirty_bytes: 1024 * 1024 * 1024,
        };

        for i in 0..4 {
            let page = make_page(i);
            store.put(i as u64, &page, i as u64 + 1).unwrap();
        }
        assert!(!store.should_checkpoint(&config));

        for i in 4..6 {
            let page = make_page(i);
            store.put(i as u64, &page, i as u64 + 1).unwrap();
        }
        assert!(store.should_checkpoint(&config));

        cleanup_dir(&dir);
    }

    #[test]
    fn test_should_checkpoint_by_dirty_bytes() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = CheckpointConfig {
            interval: Duration::from_secs(3600),
            max_dirty_pages: 1000,
            max_dirty_bytes: PAGE_SIZE * 2,
        };

        let page = make_page(1);
        store.put(1, &page, 1).unwrap();
        assert!(!store.should_checkpoint(&config));

        let page = make_page(2);
        store.put(2, &page, 2).unwrap();
        assert!(!store.should_checkpoint(&config));

        let page = make_page(3);
        store.put(3, &page, 3).unwrap();
        assert!(store.should_checkpoint(&config));

        cleanup_dir(&dir);
    }

    #[test]
    fn test_should_checkpoint_by_interval() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = CheckpointConfig {
            interval: Duration::from_millis(50),
            max_dirty_pages: 1000,
            max_dirty_bytes: 1024 * 1024 * 1024,
        };

        let page = make_page(1);
        store.put(1, &page, 1).unwrap();
        assert!(!store.should_checkpoint(&config));

        thread::sleep(Duration::from_millis(60));
        assert!(store.should_checkpoint(&config));

        cleanup_dir(&dir);
    }

    #[test]
    fn test_maybe_checkpoint() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = CheckpointConfig {
            interval: Duration::from_secs(3600),
            max_dirty_pages: 2,
            max_dirty_bytes: 1024 * 1024 * 1024,
        };

        let page = make_page(1);
        store.put(1, &page, 100).unwrap();
        assert_eq!(store.maybe_checkpoint(&config).unwrap(), false);
        assert_eq!(store.buffer_stats().dirty_count, 1);

        let page = make_page(2);
        store.put(2, &page, 101).unwrap();
        let page = make_page(3);
        store.put(3, &page, 102).unwrap();
        assert_eq!(store.maybe_checkpoint(&config).unwrap(), true);
        assert_eq!(store.buffer_stats().dirty_count, 0);
        assert_eq!(store.checkpoint_lsn(), 102);

        cleanup_dir(&dir);
    }

    #[test]
    fn test_background_checkpoint() {
        let dir = temp_dir();
        let store = Arc::new(PageStore::open(&dir).unwrap());
        let config = CheckpointConfig::for_test();

        let handle = store.start_background_checkpoint(config);

        for i in 0..15u64 {
            let page = make_page(i as u8);
            store.put(i, &page, i + 1).unwrap();
        }

        thread::sleep(Duration::from_millis(200));

        assert_eq!(store.buffer_stats().dirty_count, 0);
        assert!(store.checkpoint_lsn() >= 15);

        store.shutdown();
        handle.join().unwrap();

        cleanup_dir(&dir);
    }

    #[test]
    fn test_shutdown_flushes_dirty_pages() {
        let dir = temp_dir();
        let store = Arc::new(PageStore::open(&dir).unwrap());
        let config = CheckpointConfig {
            interval: Duration::from_secs(3600),
            max_dirty_pages: 1000,
            max_dirty_bytes: 1024 * 1024 * 1024,
        };

        let handle = store.start_background_checkpoint(config);

        let page = make_page(1);
        store.put(1, &page, 100).unwrap();
        assert_eq!(store.buffer_stats().dirty_count, 1);

        store.shutdown();
        handle.join().unwrap();

        assert_eq!(store.buffer_stats().dirty_count, 0);
        assert_eq!(store.checkpoint_lsn(), 100);

        cleanup_dir(&dir);
    }

    #[test]
    fn test_evict_cold_pages_removes_oldest() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = BufferPoolConfig {
            max_pages: 5,
            eviction_batch_size: 2,
        };

        for i in 0..10u64 {
            let page = make_page(i as u8);
            store.put(i, &page, i + 1).unwrap();
            thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(store.len(), 10);

        let evicted = store.evict_cold_pages(&config).unwrap();
        assert_eq!(evicted, 7);
        assert_eq!(store.len(), 3);

        store.checkpoint().unwrap();

        let pool_keys: std::collections::HashSet<_> = store.keys().into_iter().collect();
        assert!(pool_keys.contains(&7));
        assert!(pool_keys.contains(&8));
        assert!(pool_keys.contains(&9));

        cleanup_dir(&dir);
    }

    #[test]
    fn test_evict_flushes_dirty_before_remove() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = BufferPoolConfig {
            max_pages: 3,
            eviction_batch_size: 1,
        };

        for i in 0..5u64 {
            let page = make_page((i + 1) as u8);
            store.put(i, &page, i + 1).unwrap();
            thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(store.buffer_stats().dirty_count, 5);

        let evicted = store.evict_cold_pages(&config).unwrap();
        assert!(evicted > 0);

        drop(store);
        let store2 = PageStore::open(&dir).unwrap();

        let page0 = store2.get(0);
        assert!(page0.is_some(), "Evicted dirty page should be on disk");
        assert_eq!(page0.unwrap()[0], 1);

        cleanup_dir(&dir);
    }

    #[test]
    fn test_maybe_evict() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = BufferPoolConfig {
            max_pages: 5,
            eviction_batch_size: 2,
        };

        for i in 0..4u64 {
            let page = make_page(i as u8);
            store.put(i, &page, i + 1).unwrap();
        }

        let evicted = store.maybe_evict(&config).unwrap();
        assert_eq!(evicted, 0);
        assert_eq!(store.len(), 4);

        for i in 4..8u64 {
            let page = make_page(i as u8);
            store.put(i, &page, i + 1).unwrap();
        }
        assert_eq!(store.len(), 8);

        let evicted = store.maybe_evict(&config).unwrap();
        assert!(evicted > 0);
        assert!(store.len() <= config.max_pages + config.eviction_batch_size);

        cleanup_dir(&dir);
    }

    #[test]
    fn test_background_checkpoint_with_eviction() {
        let dir = temp_dir();
        let store = Arc::new(PageStore::open(&dir).unwrap());
        let checkpoint_config = CheckpointConfig::for_test();
        let buffer_config = BufferPoolConfig {
            max_pages: 10,
            eviction_batch_size: 3,
        };

        let handle = store
            .start_background_checkpoint_with_eviction(checkpoint_config, buffer_config.clone());

        for i in 0..25u64 {
            let page = make_page(i as u8);
            store.put(i, &page, i + 1).unwrap();
        }

        thread::sleep(Duration::from_millis(200));

        assert!(
            store.len() <= buffer_config.max_pages + buffer_config.eviction_batch_size,
            "Buffer pool should be evicted to ~max_pages, got {}",
            store.len()
        );

        assert_eq!(store.buffer_stats().dirty_count, 0);

        store.shutdown();
        handle.join().unwrap();

        cleanup_dir(&dir);
    }

    #[test]
    fn test_evict_no_op_when_under_limit() {
        let dir = temp_dir();
        let store = PageStore::open(&dir).unwrap();
        let config = BufferPoolConfig {
            max_pages: 100,
            eviction_batch_size: 10,
        };

        for i in 0..5u64 {
            let page = make_page(i as u8);
            store.put(i, &page, i + 1).unwrap();
        }

        let evicted = store.evict_cold_pages(&config).unwrap();
        assert_eq!(evicted, 0);
        assert_eq!(store.len(), 5);

        cleanup_dir(&dir);
    }
}
