use super::{Key, Result, TxnError, Version};
use crate::{VALUE_SIZE, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

pub(super) const WAL_MAGIC: u32 = 0x534B5657; // "SKVW"
pub(super) const WAL_HEADER_LEN: usize = 12; // magic + len + crc32
const SNAPSHOT_MAGIC: u32 = 0x53534E50; // "SSNP"
const SNAPSHOT_VERSION: u32 = 1;
type WalOp = (Key, Option<Value>);
type WalOps = Vec<WalOp>;

pub(super) fn encode_payload(
    commit_ts: u64,
    writes: &HashMap<Key, Option<Value>>,
) -> Result<Vec<u8>> {
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

pub(super) async fn replay_wal_with_base(
    file: &mut tokio::fs::File,
    mut store: BTreeMap<Key, Vec<Version>>,
    base_ts: u64,
) -> Result<(BTreeMap<Key, Vec<Version>>, u64, u64)> {
    let mut data = Vec::new();
    file.seek(SeekFrom::Start(0)).await?;
    file.read_to_end(&mut data).await?;

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

pub(super) fn snapshot_path(wal_path: &Path) -> PathBuf {
    let mut os = wal_path.as_os_str().to_os_string();
    os.push(".snapshot");
    PathBuf::from(os)
}

pub(super) async fn load_snapshot_file(
    path: &Path,
) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(None);
    }
    let mut file = tokio::fs::File::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;
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
        if cursor + crate::KEY_SIZE + 4 > data.len() {
            return Err(TxnError::CorruptWal("snapshot truncated".to_string()));
        }
        let mut key = [0u8; crate::KEY_SIZE];
        key.copy_from_slice(&data[cursor..cursor + crate::KEY_SIZE]);
        cursor += crate::KEY_SIZE;
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

pub(super) async fn write_snapshot_file(
    path: &Path,
    commit_ts: u64,
    store: &BTreeMap<Key, Vec<Version>>,
) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buf.extend_from_slice(&commit_ts.to_le_bytes());

    let mut entries: Vec<(Key, Vec<u8>)> = Vec::new();
    for (key, versions) in store {
        if let Some(version) = versions.last()
            && let Some(value) = &version.value
        {
            entries.push((*key, value.clone()));
        }
    }

    if entries.len() > u32::MAX as usize {
        return Err(TxnError::CorruptWal(
            "too many snapshot entries".to_string(),
        ));
    }
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for (key, value) in entries {
        if value.len() > VALUE_SIZE {
            return Err(TxnError::InvalidValueSize(value.len(), VALUE_SIZE));
        }
        buf.extend_from_slice(&key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&value);
    }

    let tmp_path = path.with_extension("snapshot.tmp");
    {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .await?;
        file.write_all(&buf).await?;
        file.sync_all().await?;
    }
    tokio::fs::rename(&tmp_path, path).await?;
    if let Some(parent) = path.parent()
        && let Ok(dir) = tokio::fs::File::open(parent).await
    {
        let _ = dir.sync_all().await;
    }
    Ok(())
}

fn decode_payload(payload: &[u8]) -> Result<(u64, WalOps)> {
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
        if cursor + crate::KEY_SIZE + 1 > payload.len() {
            return Err(TxnError::CorruptWal("payload truncated".to_string()));
        }
        let mut key = [0u8; crate::KEY_SIZE];
        key.copy_from_slice(&payload[cursor..cursor + crate::KEY_SIZE]);
        cursor += crate::KEY_SIZE;
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
