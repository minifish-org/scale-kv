use super::WalState;
use crate::page_store::PageStore;
use crate::{PageId, Result};
use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const WAL_STATE_FILE: &str = "wal_state";
const WAL_STATE_TMP_FILE: &str = "wal_state.tmp";

pub(super) async fn build_page_index(page_store: &Arc<PageStore>) -> HashSet<PageId> {
    let mut index = HashSet::new();
    let max_page_id = page_store.max_page_id();
    for page_id in 0..=max_page_id {
        if page_store.get(page_id).await.is_some() {
            index.insert(page_id);
        }
    }
    index
}

pub(super) async fn read_wal_state(dir: &Path) -> Result<WalState> {
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

pub(super) async fn write_wal_state(dir: &Path, last_applied_lsn: u64) -> Result<()> {
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
