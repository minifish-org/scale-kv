use crate::Result;
use std::io::{Error, ErrorKind};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const MAX_WAL_FRAME_LEN: usize = 16 * 1024 * 1024;

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

// Slot-level ops used by replay/tests.
pub const WAL_OP_PAGE_PUT: u8 = 11;
pub const WAL_OP_PAGE_DEL: u8 = 12;

// WAL ops for txn MVCC records
pub const WAL_OP_TXN_PUT: u8 = 21;
pub const WAL_OP_TXN_DEL: u8 = 22;
pub const WAL_OP_TXN_COMMIT: u8 = 23;

#[derive(Clone, Debug)]
pub struct WalBatch {
    pub request_id: u64,
    pub start_lsn: u64,
    /// Right boundary (exclusive). Commit point uses end_lsn.
    pub end_lsn: u64,
    pub records: Vec<WalRecord>,
}

pub(super) fn encode_wal_batch(batch: &WalBatch, out: &mut Vec<u8>) -> Result<()> {
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
    let total_len = u32::try_from(buf.len()).map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "wal frame too large to encode into u32 length",
        )
    })?;
    out.extend_from_slice(&total_len.to_le_bytes());
    out.extend_from_slice(&buf);
    Ok(())
}

pub(super) fn wal_batch_encoded_len(batch: &WalBatch) -> u64 {
    // 4 bytes frame length + fixed header + per-record fixed fields + key/value payload.
    let mut len = 4u64 + 8 + 8 + 8 + 4;
    for r in &batch.records {
        len += 8 + 1 + 8 + 2 + 4 + 4;
        len += r.key.len() as u64 + r.value.len() as u64;
    }
    len
}

pub(super) async fn read_wal_batch(file: &mut File) -> Result<Option<WalBatch>> {
    let mut len_buf = [0u8; 4];
    match file.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let total_len = u32::from_le_bytes(len_buf) as usize;
    if total_len == 0 {
        return Err(Error::new(ErrorKind::InvalidData, "wal frame is empty").into());
    }
    if total_len > MAX_WAL_FRAME_LEN {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "wal frame too large: {} > {} bytes",
                total_len, MAX_WAL_FRAME_LEN
            ),
        )
        .into());
    }
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
