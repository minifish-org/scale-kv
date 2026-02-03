use crate::page_store::PageStore;
use crate::{Page, PageId, Result, Value, PAGE_SIZE};
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;

const PAGE_HEADER_SIZE: usize = 6;
const SLOT_ENTRY_SIZE: usize = 4;
#[cfg(test)]
const MAX_SEGMENT_SIZE: u64 = 8 * 1024;
#[cfg(not(test))]
const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const WAL_STATE_FILE: &str = "wal_state";
const WAL_STATE_TMP_FILE: &str = "wal_state.tmp";

/// Storage node - PostgreSQL style page storage with WAL.
pub struct StorageNode {
    dir: PathBuf,
    page_store: Arc<PageStore>,
    page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
    wal_sender: Mutex<Option<Sender<WalBatch>>>,
    wal_replay_rx: Mutex<Option<Receiver<WalBatch>>>,
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
}

impl PageStoreReplay {
    fn new(
        page_store: Arc<PageStore>,
        dir: PathBuf,
        last_applied_lsn: u64,
        page_index: Arc<std::sync::Mutex<HashSet<PageId>>>,
    ) -> Self {
        Self {
            page_store,
            dir,
            last_applied_lsn: std::sync::Mutex::new(last_applied_lsn),
            page_index,
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
            let _ = write_wal_state(&self.dir, lsn).await;
        }
    }

    async fn apply_record(&self, record: WalRecord) -> Result<()> {
        let page_id = record.page_id;
        let lsn = record.lsn;
        let page = self
            .read_page(page_id).await
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
            let file = OpenOptions::new().append(true).read(true).open(path).await?;
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

async fn start_wal_writer(dir: PathBuf) -> Result<(Sender<WalBatch>, Receiver<WalBatch>)> {
    let (tx, rx) = mpsc::channel::<WalBatch>(1024);
    let (replay_tx, replay_rx) = mpsc::channel::<WalBatch>(1024);
    tokio::spawn(wal_writer_loop(dir, rx, replay_tx));
    Ok((tx, replay_rx))
}

async fn wal_writer_loop(dir: PathBuf, mut rx: Receiver<WalBatch>, replay_tx: Sender<WalBatch>) {
    let mut writer = match WalWriter::open(dir).await {
        Ok(writer) => writer,
        Err(_) => return,
    };
    while let Some(batch) = rx.recv().await {
        if writer.append_batch(&batch).await.is_ok() {
            let _ = replay_tx.send(batch).await;
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

async fn wal_replay_loop(replay: PageStoreReplay, mut rx: Receiver<WalBatch>) {
    let mut buffers: HashMap<PageId, ReplayBuffer> = HashMap::new();
    while let Some(batch) = rx.recv().await {
        let mut last_seen = replay.last_applied();
        for record in batch.records.into_iter() {
            if record.lsn <= last_seen {
                continue;
            }
            if record.lsn != last_seen + 1 {
                break;
            }
            last_seen = record.lsn;
            let page_id = record.page_id;
            let buffer = buffers.entry(page_id).or_insert_with(ReplayBuffer::new);
            buffer.push(record);
            if buffer.should_flush() {
                let records = buffer.take();
                let _ = replay_page_records(&replay, page_id, records).await;
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
        .read_page(page_id).await
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
) -> Result<()> {
    let mut segments = list_wal_segments(dir).await?;
    segments.sort_unstable();
    if segments.is_empty() {
        return Ok(());
    }
    let replay = PageStoreReplay::new(page_store, dir.to_path_buf(), last_applied_lsn, page_index);
    for file_id in segments {
        let path = wal_segment_path(dir, file_id);
        let mut file = File::open(&path).await?;
        loop {
            let batch = match read_wal_batch(&mut file).await? {
                Some(batch) => batch,
                None => break,
            };
            for record in batch.records {
                if record.lsn <= replay.last_applied() {
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
    pub async fn new() -> Self {
        let dir = std::env::var("SCALE_KV_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        Self::open(dir).await.expect("failed to open storage")
    }

    pub async fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
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
        replay_wal_segments_to_store(
            &dir,
            Arc::clone(&page_store),
            last_applied_lsn,
            Arc::clone(&page_index),
        )
        .await?;
        let rebuilt_index = build_page_index(&page_store).await;
        *page_index.lock().unwrap() = rebuilt_index;

        let (wal_sender, wal_replay_rx) = start_wal_writer(dir.clone()).await?;
        let node = Self {
            dir: dir.clone(),
            page_store: Arc::clone(&page_store),
            page_index: Arc::clone(&page_index),
            wal_sender: Mutex::new(Some(wal_sender)),
            wal_replay_rx: Mutex::new(Some(wal_replay_rx)),
        };

        node.start_wal_replay(last_applied_lsn).await;
        Ok(node)
    }

    pub async fn append_wal_batch(&self, batch: WalBatch) -> Result<()> {
        let sender = self.wal_sender.lock().await;
        match sender.as_ref() {
            Some(sender) => Ok(sender.send(batch).await.map_err(|_| {
                crate::Error::Io(Error::new(ErrorKind::BrokenPipe, "wal queue closed"))
            })?),
            None => Err(crate::Error::Io(Error::new(
                ErrorKind::BrokenPipe,
                "wal queue not initialized",
            ))),
        }
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
        );
        tokio::spawn(async move {
            wal_replay_loop(replay, rx).await;
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
            })
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    async fn temp_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("scale-kv-test-{}-{}", std::process::id(), id));
        fs::create_dir_all(&dir).await.expect("failed to create temp dir");
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
            start_lsn: 1,
            end_lsn: 2,
            records: vec![
                WalRecord {
                    lsn: 1,
                    op: 1,
                    page_id: 7,
                    slot_id: 0,
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                WalRecord {
                    lsn: 2,
                    op: 2,
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
        let mut page = vec![0u8; PAGE_SIZE];
        write_header(&mut page, 0, PAGE_HEADER_SIZE as u16, PAGE_SIZE as u16);
        page_store.put_direct(10, &page).unwrap();

        let replay = PageStoreReplay::new(
            Arc::clone(&page_store),
            dir.clone(),
            0,
            Arc::new(std::sync::Mutex::new(HashSet::new())),
        );
        let records = vec![
            WalRecord {
                lsn: 1,
                op: 1,
                page_id: 10,
                slot_id: 0,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            WalRecord {
                lsn: 2,
                op: 1,
                page_id: 10,
                slot_id: 1,
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            },
        ];
        replay_page_records(&replay, 10, records).await.unwrap();
        let page = page_store.get(10).await.unwrap();
        let read_slot = |page: &[u8], slot_id: u16| -> Vec<u8> {
            let offset = slot_offset(slot_id);
            let pos = read_u16(page, offset) as usize;
            let len = read_u16(page, offset + 2) as usize;
            if len == 0 {
                return Vec::new();
            }
            let key_len = read_u16(page, pos) as usize;
            let value_len = read_u16(page, pos + 2) as usize;
            let value_start = pos + 4 + key_len;
            let value_end = value_start + value_len;
            page[value_start..value_end].to_vec()
        };
        assert_eq!(read_slot(&page, 0), b"v1");
        assert_eq!(read_slot(&page, 1), b"v2");
        cleanup_dir(&dir).await;
    }

    #[test]
    fn test_wal_slot_key_mismatch_is_rejected() {
        let mut page = vec![0u8; PAGE_SIZE];
        write_header(&mut page, 0, PAGE_HEADER_SIZE as u16, PAGE_SIZE as u16);
        insert_record_at_slot_checked(&mut page, 0, b"k1", b"v1").unwrap();
        let err = insert_record_at_slot_checked(&mut page, 0, b"k2", b"v2").unwrap_err();
        assert!(format!("{err}").contains("wal replay slot key mismatch"));
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
                start_lsn: 1,
                end_lsn: 10,
                records: vec![],
            };
            let batch2 = WalBatch {
                start_lsn: 11,
                end_lsn: 20,
                records: vec![],
            };
            let batch3 = WalBatch {
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
                start_lsn: 1,
                end_lsn: 5,
                records: vec![],
            };
            let batch2 = WalBatch {
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
