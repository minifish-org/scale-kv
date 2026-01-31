use crate::{Result, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::hash::Hasher;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const TOMBSTONE: u32 = u32::MAX;
#[cfg(test)]
const MAX_SEGMENT_SIZE: u64 = 8 * 1024;
#[cfg(not(test))]
const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
#[cfg(test)]
const COMPACTION_SEGMENT_LIMIT: usize = 3;
#[cfg(not(test))]
const COMPACTION_SEGMENT_LIMIT: usize = 32;
const COMPACTION_STALE_RATIO: u64 = 50;
const COMPACTION_SEGMENT_LIMIT_ENV: &str = "SCALE_KV_COMPACTION_SEGMENTS";
const COMPACTION_STALE_RATIO_ENV: &str = "SCALE_KV_COMPACTION_RATIO";
const MANIFEST_FILE: &str = "manifest";
const MANIFEST_TMP_FILE: &str = "manifest.tmp";

/// Storage node - Bitcask style append-only log + in-memory index.
pub struct StorageNode {
    dir: PathBuf,
    index: Mutex<HashMap<String, IndexEntry>>,
    writer: Mutex<LogWriter>,
    readers: Mutex<HashMap<u64, File>>,
    stats: Mutex<Stats>,
    compaction_segment_limit: usize,
    compaction_stale_ratio: u64,
}

#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    file_id: u64,
    offset: u64,
}

struct LogWriter {
    file_id: u64,
    file: File,
    size: u64,
}

struct Stats {
    stale_entries: u64,
}

struct Manifest {
    active: u64,
    segments: Vec<u64>,
}

impl StorageNode {
    /// Create a new storage node in default data directory.
    pub fn new() -> Self {
        let dir = std::env::var("SCALE_KV_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        Self::open(dir).expect("failed to open storage")
    }

    /// Open storage node at a specific directory.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let manifest = read_manifest(&dir)?;
        let mut segments = match manifest.as_ref() {
            Some(manifest) if !manifest.segments.is_empty() => manifest.segments.clone(),
            _ => list_segments(&dir)?,
        };
        if !segments.iter().all(|id| segment_path(&dir, *id).exists()) {
            segments = list_segments(&dir)?;
        }
        segments.sort_unstable();

        let mut index = HashMap::new();
        let mut entries_total = 0u64;
        let mut last_id = 0u64;
        for file_id in segments.iter().copied() {
            let path = segment_path(&dir, file_id);
            let mut file = File::open(&path)?;
            let mut offset = 0u64;
            loop {
                let (key, value, len) = match read_entry(&mut file, offset)? {
                    Some(entry) => entry,
                    None => break,
                };
                entries_total += 1;
                if value == TOMBSTONE {
                    index.remove(&key);
                } else {
                    index.insert(key, IndexEntry { file_id, offset });
                }
                offset += len as u64;
            }
            last_id = last_id.max(file_id);
        }

        let (file_id, file, size) = if last_id == 0 {
            let file_id = 1u64;
            let file = create_segment(&dir, file_id)?;
            (file_id, file, 0u64)
        } else {
            let file_id = manifest
                .as_ref()
                .map(|manifest| manifest.active)
                .unwrap_or(last_id);
            let path = segment_path(&dir, file_id);
            let file = OpenOptions::new().append(true).read(true).open(path)?;
            let size = file.metadata()?.len();
            (file_id, file, size)
        };

        let stale_entries = entries_total.saturating_sub(index.len() as u64);
        let compaction_segment_limit = env_usize(COMPACTION_SEGMENT_LIMIT_ENV)
            .unwrap_or(COMPACTION_SEGMENT_LIMIT)
            .max(1);
        let compaction_stale_ratio = env_u64(COMPACTION_STALE_RATIO_ENV)
            .unwrap_or(COMPACTION_STALE_RATIO)
            .min(100);

        if segments.is_empty() {
            segments.push(file_id);
        }
        write_manifest(&dir, file_id, &segments)?;

        Ok(Self {
            dir,
            index: Mutex::new(index),
            writer: Mutex::new(LogWriter {
                file_id,
                file,
                size,
            }),
            readers: Mutex::new(HashMap::new()),
            stats: Mutex::new(Stats { stale_entries }),
            compaction_segment_limit,
            compaction_stale_ratio,
        })
    }

    /// Get the number of keys in storage.
    pub fn len(&self) -> usize {
        self.index.lock().unwrap().len()
    }

    /// Check if storage is empty.
    pub fn is_empty(&self) -> bool {
        self.index.lock().unwrap().is_empty()
    }

    /// Put a key-value pair.
    pub fn put(&self, key: impl AsRef<str>, value: &[u8]) {
        let key = key.as_ref().to_string();
        if let Ok(entry) = self.append_entry(&key, value) {
            let mut index = self.index.lock().unwrap();
            let existed = index.contains_key(&key);
            index.insert(key, entry);
            drop(index);
            if existed {
                self.stats.lock().unwrap().stale_entries += 1;
            }
        }
        self.maybe_compact();
    }

    /// Get a value by key.
    pub fn get(&self, key: impl AsRef<str>) -> Option<Value> {
        let key_ref = key.as_ref();
        let entry = *self.index.lock().unwrap().get(key_ref)?;
        self.read_value(entry)
    }

    /// Delete a key.
    pub fn delete(&self, key: impl AsRef<str>) {
        let key = key.as_ref().to_string();
        if self.append_tombstone(&key).is_ok() {
            let mut index = self.index.lock().unwrap();
            let existed = index.remove(&key).is_some();
            drop(index);
            let mut stats = self.stats.lock().unwrap();
            stats.stale_entries += if existed { 2 } else { 1 };
        }
        self.maybe_compact();
    }

    /// Check if key exists.
    pub fn contains(&self, key: impl AsRef<str>) -> bool {
        self.index.lock().unwrap().contains_key(key.as_ref())
    }

    pub fn keys(&self) -> Vec<String> {
        self.index.lock().unwrap().keys().cloned().collect()
    }

    /// Compact existing segments into new segments.
    pub fn compact(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let mut index = self.index.lock().unwrap();
        let mut segments = list_segments(&self.dir)?;
        if segments.len() <= 1 {
            return Ok(());
        }
        segments.sort_unstable();

        let mut new_segments = Vec::new();
        let mut new_file_id = segments.last().copied().unwrap_or(0) + 1;
        let mut new_file = create_segment(&self.dir, new_file_id)?;
        let mut size = 0u64;
        new_segments.push(new_file_id);

        let mut new_index = HashMap::with_capacity(index.len());
        for (key, entry) in index.iter() {
            let value = match read_value_at(&self.dir, *entry)? {
                Some(value) => value,
                None => continue,
            };
            let entry_len = 8 + key.as_bytes().len() as u64 + value.len() as u64;
            if size > 0 && size + entry_len > MAX_SEGMENT_SIZE {
                new_file.sync_all()?;
                new_file_id += 1;
                new_file = create_segment(&self.dir, new_file_id)?;
                size = 0;
                new_segments.push(new_file_id);
            }
            let offset = size;
            write_u32(&mut new_file, key.as_bytes().len() as u32)?;
            write_u32(&mut new_file, value.len() as u32)?;
            new_file.write_all(key.as_bytes())?;
            new_file.write_all(&value)?;
            size += entry_len;
            new_index.insert(
                key.clone(),
                IndexEntry {
                    file_id: new_file_id,
                    offset,
                },
            );
        }

        new_file.sync_all()?;
        write_manifest(&self.dir, new_file_id, &new_segments)?;

        writer.file_id = new_file_id;
        writer.file = new_file;
        writer.size = size;
        *index = new_index;

        let mut stats = self.stats.lock().unwrap();
        stats.stale_entries = 0;

        let mut readers = self.readers.lock().unwrap();
        readers.clear();
        for file_id in segments {
            let path = segment_path(&self.dir, file_id);
            let _ = fs::remove_file(path);
        }

        Ok(())
    }

    fn maybe_compact(&self) {
        let segments = match list_segments(&self.dir) {
            Ok(segments) => segments,
            Err(_) => return,
        };
        if segments.len() <= self.compaction_segment_limit {
            return;
        }
        let live = self.index.lock().unwrap().len() as u64;
        let stale = self.stats.lock().unwrap().stale_entries;
        let total = live + stale;
        if total == 0 {
            return;
        }
        let ratio = stale * 100 / total;
        if ratio >= self.compaction_stale_ratio {
            let _ = self.compact();
        }
    }

    fn append_entry(&self, key: &str, value: &[u8]) -> Result<IndexEntry> {
        let mut writer = self.writer.lock().unwrap();
        if writer.size >= MAX_SEGMENT_SIZE {
            writer.file_id += 1;
            writer.file = create_segment(&self.dir, writer.file_id)?;
            writer.size = 0;
            let _ = refresh_manifest(&self.dir, writer.file_id);
        }

        let offset = writer.size;
        let key_bytes = key.as_bytes();
        let entry_len = 8 + key_bytes.len() as u64 + value.len() as u64;
        write_u32(&mut writer.file, key_bytes.len() as u32)?;
        write_u32(&mut writer.file, value.len() as u32)?;
        writer.file.write_all(key_bytes)?;
        writer.file.write_all(value)?;
        writer.size += entry_len;

        Ok(IndexEntry {
            file_id: writer.file_id,
            offset,
        })
    }

    fn append_tombstone(&self, key: &str) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        if writer.size >= MAX_SEGMENT_SIZE {
            writer.file_id += 1;
            writer.file = create_segment(&self.dir, writer.file_id)?;
            writer.size = 0;
            let _ = refresh_manifest(&self.dir, writer.file_id);
        }

        let key_bytes = key.as_bytes();
        let entry_len = 8 + key_bytes.len() as u64;
        write_u32(&mut writer.file, key_bytes.len() as u32)?;
        write_u32(&mut writer.file, TOMBSTONE)?;
        writer.file.write_all(key_bytes)?;
        writer.size += entry_len;
        Ok(())
    }

    fn read_value(&self, entry: IndexEntry) -> Option<Value> {
        let mut readers = self.readers.lock().unwrap();
        let file = readers.entry(entry.file_id).or_insert_with(|| {
            let path = segment_path(&self.dir, entry.file_id);
            File::open(path).expect("failed to open segment")
        });

        if file.seek(SeekFrom::Start(entry.offset)).is_err() {
            return None;
        }

        let key_len = read_u32(file).ok()? as usize;
        let val_len = read_u32(file).ok()?;
        if val_len == TOMBSTONE {
            return None;
        }

        let mut skip = vec![0u8; key_len];
        if file.read_exact(&mut skip).is_err() {
            return None;
        }
        let mut value = vec![0u8; val_len as usize];
        if file.read_exact(&mut value).is_err() {
            return None;
        }
        Some(value)
    }
}

impl Default for StorageNode {
    fn default() -> Self {
        Self::new()
    }
}

fn segment_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("segment-{file_id:020}.log"))
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_FILE)
}

fn manifest_tmp_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_TMP_FILE)
}

fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".log"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            segments.push(id);
        }
    }
    Ok(segments)
}

fn create_segment(dir: &Path, file_id: u64) -> Result<File> {
    let path = segment_path(dir, file_id);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    Ok(file)
}

fn read_manifest(dir: &Path) -> Result<Option<Manifest>> {
    let path = manifest_path(dir);
    let data = match fs::read_to_string(&path) {
        Ok(data) => data,
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(err.into());
        }
    };

    let mut version = None;
    let mut active = None;
    let mut segments = None;
    let mut checksum = None;
    for line in data.lines() {
        if let Some(value) = line.strip_prefix("version=") {
            version = value.parse::<u32>().ok();
        } else if let Some(value) = line.strip_prefix("active=") {
            active = value.parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("segments=") {
            let ids = if value.trim().is_empty() {
                Vec::new()
            } else {
                value
                    .split(',')
                    .filter_map(|part| part.trim().parse::<u64>().ok())
                    .collect()
            };
            segments = Some(ids);
        } else if let Some(value) = line.strip_prefix("checksum=") {
            checksum = value.parse::<u64>().ok();
        }
    }

    let version = version.unwrap_or(1);
    if version != 1 {
        return Ok(None);
    }
    let active = match active {
        Some(active) => active,
        None => return Ok(None),
    };
    let mut segments = segments.unwrap_or_default();
    if !segments.contains(&active) {
        segments.push(active);
    }

    if let Some(expected) = checksum {
        let payload = format!(
            "version={}\nactive={}\nsegments={}\n",
            version,
            active,
            segments
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        if manifest_checksum(payload.as_bytes()) != expected {
            return Ok(None);
        }
    }

    Ok(Some(Manifest { active, segments }))
}

fn write_manifest(dir: &Path, active: u64, segments: &[u64]) -> Result<()> {
    let mut segments_sorted = segments.to_vec();
    segments_sorted.sort_unstable();
    let payload = format!(
        "version=1\nactive={}\nsegments={}\n",
        active,
        segments_sorted
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let checksum = manifest_checksum(payload.as_bytes());
    let data = format!("{}checksum={}\n", payload, checksum);
    let tmp_path = manifest_tmp_path(dir);
    let path = manifest_path(dir);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    file.write_all(data.as_bytes())?;
    file.sync_all()?;
    fs::rename(tmp_path, path)?;
    sync_dir(dir);
    Ok(())
}

fn refresh_manifest(dir: &Path, active: u64) -> Result<()> {
    let mut segments = list_segments(dir)?;
    if segments.is_empty() {
        segments.push(active);
    }
    write_manifest(dir, active, &segments)
}

fn manifest_checksum(data: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(data);
    hasher.finish()
}

fn sync_dir(dir: &Path) {
    if let Ok(file) = File::open(dir) {
        let _ = file.sync_all();
    }
}

fn write_u32(file: &mut File, value: u32) -> Result<()> {
    file.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u32(file: &mut File) -> Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.parse::<usize>().ok()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse::<u64>().ok()
}

fn read_value_at(dir: &Path, entry: IndexEntry) -> Result<Option<Value>> {
    let path = segment_path(dir, entry.file_id);
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(entry.offset))?;
    let key_len = read_u32(&mut file)? as usize;
    let val_len = read_u32(&mut file)?;
    if val_len == TOMBSTONE {
        return Ok(None);
    }
    let mut skip = vec![0u8; key_len];
    file.read_exact(&mut skip)?;
    let mut value = vec![0u8; val_len as usize];
    file.read_exact(&mut value)?;
    Ok(Some(value))
}

fn read_entry(file: &mut File, offset: u64) -> Result<Option<(String, u32, u32)>> {
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return Ok(None);
    }
    let key_len = match read_u32(file) {
        Ok(len) => len as usize,
        Err(_) => return Ok(None),
    };
    let val_len = match read_u32(file) {
        Ok(len) => len,
        Err(_) => return Ok(None),
    };

    let mut key_buf = vec![0u8; key_len];
    if file.read_exact(&mut key_buf).is_err() {
        return Ok(None);
    }

    if val_len != TOMBSTONE {
        let mut skip = vec![0u8; val_len as usize];
        if file.read_exact(&mut skip).is_err() {
            return Ok(None);
        }
    }

    let key = String::from_utf8_lossy(&key_buf).to_string();
    let entry_len = 8 + key_len as u32 + if val_len == TOMBSTONE { 0 } else { val_len };
    Ok(Some((key, val_len, entry_len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("scale-kv-test-{}-{}", std::process::id(), id));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn cleanup_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_put_and_get() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put("foo", b"bar");
        assert_eq!(node.get("foo"), Some(b"bar".to_vec()));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_get_missing() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.get("missing"), None);
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_overwrite() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put("foo", b"bar");
        node.put("foo", b"baz");
        assert_eq!(node.get("foo"), Some(b"baz".to_vec()));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_delete() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put("foo", b"bar");
        node.delete("foo");
        assert_eq!(node.get("foo"), None);
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_len() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.len(), 0);
        node.put("a", b"1");
        node.put("b", b"2");
        assert_eq!(node.len(), 2);
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_contains() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put("foo", b"bar");
        assert!(node.contains("foo"));
        assert!(!node.contains("missing"));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_reopen_persists() {
        let dir = temp_dir();
        {
            let node = StorageNode::open(&dir).unwrap();
            node.put("foo", b"bar");
            node.put("baz", b"qux");
        }
        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.get("foo"), Some(b"bar".to_vec()));
        assert_eq!(node.get("baz"), Some(b"qux".to_vec()));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_compact_rewrites_segments() {
        let dir = temp_dir();
        let value = vec![b'x'; MAX_SEGMENT_SIZE as usize];
        let node = StorageNode::open(&dir).unwrap();
        node.put("k1", &value);
        node.put("k2", &value);

        let segments_before = list_segments(&dir).unwrap();
        assert!(segments_before.len() >= 2);
        let max_before = segments_before.iter().copied().max().unwrap_or(0);

        node.delete("k1");
        node.compact().unwrap();

        let segments_after = list_segments(&dir).unwrap();
        assert!(segments_after.iter().all(|id| *id > max_before));
        assert_eq!(node.get("k1"), None);
        assert_eq!(node.get("k2"), Some(value));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_auto_compact_trigger() {
        let dir = temp_dir();
        let value = vec![b'x'; (MAX_SEGMENT_SIZE / 2) as usize];
        let node = StorageNode::open(&dir).unwrap();

        node.put("k1", &value);
        node.put("k2", &value);
        node.put("k3", &value);
        node.put("k4", &value);

        node.delete("k1");
        node.delete("k2");
        node.delete("k3");
        node.put("k5", &value);

        let segments = list_segments(&dir).unwrap();
        assert!(segments.len() <= COMPACTION_SEGMENT_LIMIT);
        assert_eq!(node.get("k4"), Some(value.clone()));
        assert_eq!(node.get("k5"), Some(value));

        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_manifest_written() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put("k1", b"v1");

        let manifest_contents = fs::read_to_string(manifest_path(&dir)).unwrap();
        assert!(manifest_contents.lines().any(|line| line == "version=1"));
        let manifest = read_manifest(&dir).unwrap().expect("missing manifest");
        assert!(!manifest.segments.is_empty());
        assert!(manifest.segments.contains(&manifest.active));

        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_manifest_checksum_mismatch_fallback() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put("k1", b"v1");
        drop(node);

        let manifest_path = manifest_path(&dir);
        fs::write(
            &manifest_path,
            "version=1\nactive=999\nsegments=999\nchecksum=1\n",
        )
        .unwrap();

        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.get("k1"), Some(b"v1".to_vec()));

        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_compact_manifest_failure_keeps_segments() {
        let dir = temp_dir();
        let value = vec![b'x'; MAX_SEGMENT_SIZE as usize];
        let node = StorageNode::open(&dir).unwrap();
        node.put("k1", &value);
        node.put("k2", &value);
        node.put("k3", &value);

        let mut segments_before = list_segments(&dir).unwrap();
        segments_before.sort_unstable();
        assert!(segments_before.len() >= 2);

        fs::create_dir_all(manifest_tmp_path(&dir)).unwrap();
        assert!(node.compact().is_err());

        let mut segments_after = list_segments(&dir).unwrap();
        segments_after.sort_unstable();
        for id in &segments_before {
            assert!(segments_after.contains(id));
        }
        assert_eq!(node.get("k1"), Some(value.clone()));
        assert_eq!(node.get("k2"), Some(value.clone()));
        assert_eq!(node.get("k3"), Some(value));

        drop(node);
        cleanup_dir(&dir);
    }
}
