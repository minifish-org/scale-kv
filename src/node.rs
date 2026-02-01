use crate::{Page, PageId, Result, Value, PAGE_SIZE};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::hash::Hasher;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

const TOMBSTONE: u32 = u32::MAX;
const PAGE_HEADER_SIZE: usize = 6;
const SLOT_ENTRY_SIZE: usize = 4;
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
const WAL_STATE_FILE: &str = "wal_state";
const WAL_STATE_TMP_FILE: &str = "wal_state.tmp";

/// Storage node - Bitcask style append-only log + in-memory index.
pub struct StorageNode {
    dir: PathBuf,
    index: Mutex<HashMap<PageId, IndexEntry>>,
    writer: Mutex<LogWriter>,
    readers: Mutex<HashMap<u64, File>>,
    stats: Mutex<Stats>,
    compaction_segment_limit: usize,
    compaction_stale_ratio: u64,
    wal_sender: Mutex<Option<Sender<WalBatch>>>,
    wal_replay_rx: Mutex<Option<Receiver<WalBatch>>>,
}

struct ReplayBuffer {
    bytes: usize,
    records: Vec<WalRecord>,
}

struct StorageNodeReplay {
    node: Arc<StorageNode>,
    last_applied_lsn: Mutex<u64>,
}

impl StorageNodeReplay {
    fn new(node: Arc<StorageNode>, last_applied_lsn: u64) -> Self {
        Self {
            node,
            last_applied_lsn: Mutex::new(last_applied_lsn),
        }
    }

    fn write_page(&self, page_id: PageId, page: &[u8]) -> Result<()> {
        self.node.put(page_id, page);
        Ok(())
    }

    fn read_page(&self, page_id: PageId) -> Option<Page> {
        self.node.get(page_id)
    }

    fn should_apply(&self, lsn: u64) -> bool {
        let last_applied = self.last_applied_lsn.lock().unwrap();
        lsn > *last_applied
    }

    fn last_applied(&self) -> u64 {
        *self.last_applied_lsn.lock().unwrap()
    }

    fn update_last_applied(&self, lsn: u64) {
        let mut last_applied = self.last_applied_lsn.lock().unwrap();
        if lsn > *last_applied {
            *last_applied = lsn;
            let _ = write_wal_state(&self.node.dir, *last_applied);
        }
    }

    fn apply_record(&self, record: WalRecord) -> Result<()> {
        let page_id = record.page_id;
        let page = self
            .read_page(page_id)
            .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
        let mut page = page;
        apply_wal_record(&mut page, &record)?;
        self.write_page(page_id, &page)?;
        self.update_last_applied(record.lsn);
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
struct IndexEntry {
    file_id: u64,
    offset: u64,
}

struct LogWriter {
    file_id: u64,
    file: File,
    size: u64,
}

impl Clone for LogWriter {
    fn clone(&self) -> Self {
        Self {
            file_id: self.file_id,
            file: self.file.try_clone().expect("failed to clone log writer"),
            size: self.size,
        }
    }
}

impl LogWriter {
    fn rotate_if_needed(&mut self, dir: &Path) -> Result<()> {
        if self.size < MAX_SEGMENT_SIZE {
            return Ok(());
        }
        self.file_id += 1;
        self.file = create_segment(dir, self.file_id)?;
        self.size = 0;
        Ok(())
    }
}

fn append_entry_to_log(
    dir: &Path,
    writer: &mut LogWriter,
    key: PageId,
    value: &[u8],
) -> Result<IndexEntry> {
    writer.rotate_if_needed(dir)?;
    let offset = writer.size;
    let entry_len = 8 + 8 + value.len() as u64;
    write_u32(&mut writer.file, 8)?;
    write_u32(&mut writer.file, value.len() as u32)?;
    writer.file.write_all(&key.to_le_bytes())?;
    writer.file.write_all(value)?;
    writer.size += entry_len;
    let _ = refresh_manifest(dir, writer.file_id);
    Ok(IndexEntry {
        file_id: writer.file_id,
        offset,
    })
}

impl WalWriter {
    fn open(dir: PathBuf) -> Result<Self> {
        let mut segments = list_wal_segments(&dir)?;
        let (file_id, file, size) = if segments.is_empty() {
            let file_id = 1u64;
            let file = create_wal_segment(&dir, file_id)?;
            (file_id, file, 0u64)
        } else {
            segments.sort_unstable();
            let file_id = *segments.last().unwrap();
            let path = wal_segment_path(&dir, file_id);
            let file = OpenOptions::new().append(true).read(true).open(path)?;
            let size = file.metadata()?.len();
            (file_id, file, size)
        };
        Ok(Self {
            dir,
            file_id,
            file,
            size,
        })
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        if self.size < MAX_SEGMENT_SIZE {
            return Ok(());
        }
        self.file_id += 1;
        self.file = create_wal_segment(&self.dir, self.file_id)?;
        self.size = 0;
        Ok(())
    }

    fn append_batch(&mut self, batch: &WalBatch) -> Result<()> {
        self.rotate_if_needed()?;
        let mut buf = Vec::new();
        encode_wal_batch(batch, &mut buf)?;
        self.file.write_all(&buf)?;
        self.size += buf.len() as u64;
        self.file.sync_data()?;
        Ok(())
    }
}

struct WalWriter {
    dir: PathBuf,
    file_id: u64,
    file: File,
    size: u64,
}

struct Stats {
    stale_entries: u64,
}

impl Clone for Stats {
    fn clone(&self) -> Self {
        Self {
            stale_entries: self.stale_entries,
        }
    }
}

struct Manifest {
    active: u64,
    segments: Vec<u64>,
}

struct WalState {
    last_applied_lsn: u64,
}

const WAL_SEGMENT_PREFIX: &str = "wal";

fn wal_segment_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{}-{:020}.log", WAL_SEGMENT_PREFIX, file_id))
}

fn create_wal_segment(dir: &Path, file_id: u64) -> Result<File> {
    let path = wal_segment_path(dir, file_id);
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)?)
}

fn list_wal_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
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

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub lsn: u64,
    pub op: u8,
    pub page_id: u64,
    pub slot_id: u16,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct WalBatch {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub records: Vec<WalRecord>,
}

fn encode_wal_batch(batch: &WalBatch, out: &mut Vec<u8>) -> Result<()> {
    let mut buf = Vec::new();
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

fn start_wal_writer(dir: PathBuf) -> Result<(Sender<WalBatch>, Receiver<WalBatch>)> {
    let (tx, rx) = mpsc::channel::<WalBatch>();
    let (replay_tx, replay_rx) = mpsc::channel::<WalBatch>();
    thread::spawn(move || wal_writer_loop(dir, rx, replay_tx));
    Ok((tx, replay_rx))
}

fn wal_writer_loop(dir: PathBuf, rx: Receiver<WalBatch>, replay_tx: Sender<WalBatch>) {
    let mut writer = match WalWriter::open(dir) {
        Ok(writer) => writer,
        Err(_) => return,
    };
    for batch in rx {
        if writer.append_batch(&batch).is_ok() {
            let _ = replay_tx.send(batch);
        }
    }
}

fn read_wal_batch(file: &mut File) -> Result<Option<WalBatch>> {
    let mut len_buf = [0u8; 4];
    match file.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let total_len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; total_len];
    file.read_exact(&mut buf)?;
    let mut cursor = 0usize;

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
        start_lsn,
        end_lsn,
        records,
    }))
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

fn wal_replay_loop(node: StorageNodeReplay, rx: Receiver<WalBatch>) {
    let mut buffers: HashMap<PageId, ReplayBuffer> = HashMap::new();
    for batch in rx {
        let mut last_seen = node.last_applied();
        for record in batch.records.into_iter() {
            if record.lsn <= last_seen {
                continue;
            }
            if record.lsn != last_seen + 1 {
                // out-of-order or gap: stop applying this batch
                break;
            }
            last_seen = record.lsn;
            let page_id = record.page_id;
            let buffer = buffers.entry(page_id).or_insert_with(ReplayBuffer::new);
            buffer.push(record);
            if buffer.should_flush() {
                let records = buffer.take();
                let _ = replay_page_records(&node, page_id, records);
            }
        }
    }

    for (page_id, mut buffer) in buffers.into_iter() {
        let records = buffer.take();
        let _ = replay_page_records(&node, page_id, records);
    }
}

fn replay_page_records(
    node: &StorageNodeReplay,
    page_id: PageId,
    records: Vec<WalRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut max_lsn = 0u64;
    let page = node
        .read_page(page_id)
        .unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
    let mut page = page;
    for record in records {
        if record.lsn > max_lsn {
            max_lsn = record.lsn;
        }
        apply_wal_record(&mut page, &record)?;
    }
    node.write_page(page_id, &page)?;
    node.update_last_applied(max_lsn);
    Ok(())
}

fn apply_wal_record(page: &mut [u8], record: &WalRecord) -> Result<()> {
    match record.op {
        1 => insert_record_at_slot_checked(page, record.slot_id, &record.key, &record.value),
        2 => {
            clear_slot(page, record.slot_id);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn read_header(page: &[u8]) -> (u16, u16, u16) {
    let slots = read_u16(page, 0);
    let free_start = read_u16(page, 2);
    let free_end = read_u16(page, 4);
    (slots, free_start, free_end)
}

fn write_header(page: &mut [u8], slots: u16, free_start: u16, free_end: u16) {
    write_u16(page, 0, slots);
    write_u16(page, 2, free_start);
    write_u16(page, 4, free_end);
}

fn read_slot(page: &[u8], slot_id: u16) -> (u16, u16) {
    let offset = slot_offset(slot_id);
    let pos = read_u16(page, offset);
    let len = read_u16(page, offset + 2);
    (pos, len)
}

fn write_slot(page: &mut [u8], slot_id: u16, offset: u16, len: u16) {
    let pos = slot_offset(slot_id);
    write_u16(page, pos, offset);
    write_u16(page, pos + 2, len)
}

fn slot_offset(slot_id: u16) -> usize {
    PAGE_HEADER_SIZE + SLOT_ENTRY_SIZE * slot_id as usize
}

fn find_free_slot(page: &[u8], slots: u16) -> Option<u16> {
    for slot_id in 0..slots {
        let (_, len) = read_slot(page, slot_id);
        if len == 0 {
            return Some(slot_id);
        }
    }
    None
}

fn insert_record_at_slot(page: &mut [u8], slot_id: u16, key: &[u8], value: &[u8]) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(crate::Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    let (mut slots, mut free_start, mut free_end) = read_header(page);
    let payload_len = 4 + key.len() + value.len();

    let free_slot = if slot_id < slots {
        Some(slot_id)
    } else {
        find_free_slot(page, slots)
    };

    let mut needed = payload_len;
    if free_slot.is_none() {
        needed += SLOT_ENTRY_SIZE;
    }
    let free_bytes = free_end.saturating_sub(free_start) as usize;
    if free_bytes < needed {
        return Err(crate::Error::InvalidValueSize(needed, free_bytes));
    }

    let slot_id = free_slot.unwrap_or(slots);
    if free_slot.is_none() {
        free_start = free_start.saturating_add(SLOT_ENTRY_SIZE as u16);
        slots = slots.saturating_add(1);
    }

    let payload_offset = (free_end as usize).saturating_sub(payload_len) as u16;
    write_u16(page, payload_offset as usize, key.len() as u16);
    write_u16(page, payload_offset as usize + 2, value.len() as u16);
    let mut cursor = payload_offset as usize + 4;
    page[cursor..cursor + key.len()].copy_from_slice(key);
    cursor += key.len();
    page[cursor..cursor + value.len()].copy_from_slice(value);

    write_slot(page, slot_id, payload_offset, payload_len as u16);
    free_end = payload_offset;
    write_header(page, slots, free_start, free_end);
    Ok(())
}

fn insert_record_at_slot_checked(
    page: &mut [u8],
    slot_id: u16,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(crate::Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    let (slots, _, _) = read_header(page);
    if slot_id < slots {
        let (offset, len) = read_slot(page, slot_id);
        if len != 0 {
            let offset = offset as usize;
            if offset + 4 <= PAGE_SIZE {
                let key_len = read_u16(page, offset) as usize;
                let value_len = read_u16(page, offset + 2) as usize;
                let key_start = offset + 4;
                let key_end = key_start + key_len;
                let value_end = key_end + value_len;
                if value_end <= PAGE_SIZE {
                    let old_key = &page[key_start..key_end];
                    if old_key != key {
                        return Err(crate::Error::Capnp(
                            "wal replay slot key mismatch".to_string(),
                        ));
                    }
                }
            }
        }
    }
    insert_record_at_slot(page, slot_id, key, value)
}

fn clear_slot(page: &mut [u8], slot_id: u16) {
    if page.len() != PAGE_SIZE {
        return;
    }
    write_slot(page, slot_id, 0, 0)
}

fn read_u16(page: &[u8], offset: usize) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&page[offset..offset + 2]);
    u16::from_le_bytes(buf)
}

fn write_u16(page: &mut [u8], offset: usize, value: u16) {
    let bytes = value.to_le_bytes();
    page[offset..offset + 2].copy_from_slice(&bytes)
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

        let wal_state = read_wal_state(&dir).unwrap_or(WalState {
            last_applied_lsn: 0,
        });

        let manifest = read_manifest(&dir)?;
        let mut segments = match manifest.as_ref() {
            Some(manifest) if !manifest.segments.is_empty() => manifest.segments.clone(),
            _ => list_segments(&dir)?,
        };
        if !segments.iter().all(|id| segment_path(&dir, *id).exists()) {
            segments = list_segments(&dir)?;
        }
        segments.sort_unstable();

        let mut index: HashMap<PageId, IndexEntry> = HashMap::new();
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

        let (wal_sender, wal_replay_rx) = start_wal_writer(dir.clone())?;
        let node = Self {
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
            wal_sender: Mutex::new(Some(wal_sender)),
            wal_replay_rx: Mutex::new(Some(wal_replay_rx)),
        };
        node.replay_wal_segments(wal_state.last_applied_lsn)?;
        node.start_wal_replay(wal_state.last_applied_lsn);
        Ok(node)
    }

    pub fn append_wal_batch(&self, batch: WalBatch) -> Result<()> {
        let sender = self.wal_sender.lock().unwrap();
        match sender.as_ref() {
            Some(sender) => Ok(sender.send(batch).map_err(|_| {
                crate::Error::Io(Error::new(ErrorKind::BrokenPipe, "wal queue closed"))
            })?),
            None => Err(crate::Error::Io(Error::new(
                ErrorKind::BrokenPipe,
                "wal queue not initialized",
            ))),
        }
    }

    fn clone_inner(&self) -> StorageNode {
        StorageNode {
            dir: self.dir.clone(),
            index: Mutex::new(self.index.lock().unwrap().clone()),
            writer: Mutex::new(self.writer.lock().unwrap().clone()),
            readers: Mutex::new(HashMap::new()),
            stats: Mutex::new(self.stats.lock().unwrap().clone()),
            compaction_segment_limit: self.compaction_segment_limit,
            compaction_stale_ratio: self.compaction_stale_ratio,
            wal_sender: Mutex::new(None),
            wal_replay_rx: Mutex::new(None),
        }
    }

    pub fn start_wal_replay(&self, last_applied_lsn: u64) {
        let rx = self.wal_replay_rx.lock().unwrap().take();
        if rx.is_none() {
            return;
        }
        let rx = rx.unwrap();
        let replay = StorageNodeReplay::new(Arc::new(self.clone_inner()), last_applied_lsn);
        thread::spawn(move || {
            wal_replay_loop(replay, rx);
        });
    }

    fn replay_wal_segments(&self, last_applied_lsn: u64) -> Result<()> {
        let mut segments = list_wal_segments(&self.dir)?;
        segments.sort_unstable();
        if segments.is_empty() {
            return Ok(());
        }
        let replay = StorageNodeReplay::new(Arc::new(self.clone_inner()), last_applied_lsn);
        for file_id in segments {
            let path = wal_segment_path(&self.dir, file_id);
            let mut file = File::open(&path)?;
            loop {
                let batch = match read_wal_batch(&mut file)? {
                    Some(batch) => batch,
                    None => break,
                };
                for record in batch.records {
                    if record.lsn <= replay.last_applied() {
                        continue;
                    }
                    replay.apply_record(record)?;
                }
            }
        }
        Ok(())
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
    pub fn put(&self, key: PageId, value: &[u8]) {
        if let Ok(entry) = self.append_entry(key, value) {
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
    pub fn get(&self, key: PageId) -> Option<Value> {
        let entry = *self.index.lock().unwrap().get(&key)?;
        self.read_value(entry)
    }

    /// Delete a key.
    pub fn delete(&self, key: PageId) {
        if self.append_tombstone(key).is_ok() {
            let mut index = self.index.lock().unwrap();
            let existed = index.remove(&key).is_some();
            drop(index);
            let mut stats = self.stats.lock().unwrap();
            stats.stale_entries += if existed { 2 } else { 1 };
        }
        self.maybe_compact();
    }

    /// Check if key exists.
    pub fn contains(&self, key: PageId) -> bool {
        self.index.lock().unwrap().contains_key(&key)
    }

    pub fn keys(&self) -> Vec<PageId> {
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
            let entry_len = 8 + 8 + value.len() as u64;
            if size > 0 && size + entry_len > MAX_SEGMENT_SIZE {
                new_file.sync_all()?;
                new_file_id += 1;
                new_file = create_segment(&self.dir, new_file_id)?;
                size = 0;
                new_segments.push(new_file_id);
            }
            let offset = size;
            write_u32(&mut new_file, 8)?;
            write_u32(&mut new_file, value.len() as u32)?;
            new_file.write_all(&key.to_le_bytes())?;
            new_file.write_all(&value)?;
            size += entry_len;
            new_index.insert(
                *key,
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

    fn append_entry(&self, key: PageId, value: &[u8]) -> Result<IndexEntry> {
        let mut writer = self.writer.lock().unwrap();
        let entry = append_entry_to_log(&self.dir, &mut writer, key, value)?;
        Ok(entry)
    }

    fn append_tombstone(&self, key: PageId) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        if writer.size >= MAX_SEGMENT_SIZE {
            writer.file_id += 1;
            writer.file = create_segment(&self.dir, writer.file_id)?;
            writer.size = 0;
            let _ = refresh_manifest(&self.dir, writer.file_id);
        }

        let entry_len = 8 + 8;
        write_u32(&mut writer.file, 8)?;
        write_u32(&mut writer.file, TOMBSTONE)?;
        writer.file.write_all(&key.to_le_bytes())?;
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

fn read_wal_state(dir: &Path) -> Result<WalState> {
    let path = dir.join(WAL_STATE_FILE);
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Ok(WalState {
                last_applied_lsn: 0,
            })
        }
        Err(err) => return Err(err.into()),
    };
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    let last_applied_lsn = u64::from_le_bytes(buf);
    Ok(WalState { last_applied_lsn })
}

fn write_wal_state(dir: &Path, last_applied_lsn: u64) -> Result<()> {
    let tmp_path = dir.join(WAL_STATE_TMP_FILE);
    let path = dir.join(WAL_STATE_FILE);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(&last_applied_lsn.to_le_bytes())?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, path)?;
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

fn read_entry(file: &mut File, offset: u64) -> Result<Option<(PageId, u32, u32)>> {
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

    if key_len != 8 {
        return Err(std::io::Error::new(ErrorKind::InvalidData, "invalid key length").into());
    }

    if val_len != TOMBSTONE {
        let mut skip = vec![0u8; val_len as usize];
        if file.read_exact(&mut skip).is_err() {
            return Ok(None);
        }
    }

    let mut key_bytes = [0u8; 8];
    key_bytes.copy_from_slice(&key_buf);
    let key = PageId::from_le_bytes(key_bytes);
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
        node.put(1, b"bar");
        assert_eq!(node.get(1), Some(b"bar".to_vec()));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_get_missing() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.get(999), None);
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_overwrite() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, b"bar");
        node.put(1, b"baz");
        assert_eq!(node.get(1), Some(b"baz".to_vec()));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_delete() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, b"bar");
        node.delete(1);
        assert_eq!(node.get(1), None);
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_len() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.len(), 0);
        node.put(1, b"1");
        node.put(2, b"2");
        assert_eq!(node.len(), 2);
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_contains() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, b"bar");
        assert!(node.contains(1));
        assert!(!node.contains(999));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_reopen_persists() {
        let dir = temp_dir();
        {
            let node = StorageNode::open(&dir).unwrap();
            node.put(1, b"bar");
            node.put(2, b"qux");
        }
        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.get(1), Some(b"bar".to_vec()));
        assert_eq!(node.get(2), Some(b"qux".to_vec()));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_compact_rewrites_segments() {
        let dir = temp_dir();
        let value = vec![b'x'; MAX_SEGMENT_SIZE as usize];
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, &value);
        node.put(2, &value);

        let segments_before = list_segments(&dir).unwrap();
        assert!(segments_before.len() >= 2);
        let max_before = segments_before.iter().copied().max().unwrap_or(0);

        node.delete(1);
        node.compact().unwrap();

        let segments_after = list_segments(&dir).unwrap();
        assert!(segments_after.iter().all(|id| *id > max_before));
        assert_eq!(node.get(1), None);
        assert_eq!(node.get(2), Some(value));
        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_auto_compact_trigger() {
        let dir = temp_dir();
        let value = vec![b'x'; (MAX_SEGMENT_SIZE / 2) as usize];
        let node = StorageNode::open(&dir).unwrap();

        node.put(1, &value);
        node.put(2, &value);
        node.put(3, &value);
        node.put(4, &value);

        node.delete(1);
        node.delete(2);
        node.delete(3);
        node.put(5, &value);

        let segments = list_segments(&dir).unwrap();
        assert!(segments.len() <= COMPACTION_SEGMENT_LIMIT);
        assert_eq!(node.get(4), Some(value.clone()));
        assert_eq!(node.get(5), Some(value));

        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_manifest_written() {
        let dir = temp_dir();
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, b"v1");

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
        node.put(1, b"v1");
        drop(node);

        let manifest_path = manifest_path(&dir);
        fs::write(
            &manifest_path,
            "version=1\nactive=999\nsegments=999\nchecksum=1\n",
        )
        .unwrap();

        let node = StorageNode::open(&dir).unwrap();
        assert_eq!(node.get(1), Some(b"v1".to_vec()));

        drop(node);
        cleanup_dir(&dir);
    }

    #[test]
    fn test_compact_manifest_failure_keeps_segments() {
        let dir = temp_dir();
        let value = vec![b'x'; MAX_SEGMENT_SIZE as usize];
        let node = StorageNode::open(&dir).unwrap();
        node.put(1, &value);
        node.put(2, &value);
        node.put(3, &value);

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
        assert_eq!(node.get(1), Some(value.clone()));
        assert_eq!(node.get(2), Some(value.clone()));
        assert_eq!(node.get(3), Some(value));

        drop(node);
        cleanup_dir(&dir);
    }
}
