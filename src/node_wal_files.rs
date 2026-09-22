use super::{StorageMaintenanceConfig, read_wal_batch};
use crate::Result;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};

const WAL_SEGMENT_PREFIX: &str = "wal";

pub(super) fn wal_segment_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{}-{:020}.log", WAL_SEGMENT_PREFIX, file_id))
}

pub(super) async fn create_wal_segment(dir: &Path, file_id: u64) -> Result<File> {
    let path = wal_segment_path(dir, file_id);
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)
        .await?)
}

pub(super) async fn list_wal_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix(&format!("{}-", WAL_SEGMENT_PREFIX))
            && let Some(id_part) = rest.strip_suffix(".log")
            && let Ok(id) = id_part.parse::<u64>()
        {
            segments.push(id);
        }
    }
    segments.sort_unstable();
    Ok(segments)
}

pub(super) async fn wal_usage(dir: &Path) -> Result<(usize, u64)> {
    let segments = list_wal_segments(dir).await?;
    let mut bytes = 0u64;
    for id in &segments {
        let meta = fs::metadata(wal_segment_path(dir, *id)).await?;
        bytes = bytes.saturating_add(meta.len());
    }
    Ok((segments.len(), bytes))
}

pub(super) async fn wal_usage_exceeds_limits(
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

pub(super) async fn truncate_wal_segments(dir: &Path, checkpoint_lsn: u64) -> Result<usize> {
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
