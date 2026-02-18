use super::{
    MAX_SEGMENT_SIZE, WalBatch, WalWriteRequest, WalWriterConfig, create_wal_segment,
    encode_wal_batch, list_wal_segments, wal_segment_path,
};
use crate::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{self, Receiver, Sender};

struct WalWriter {
    dir: PathBuf,
    file_id: u64,
    file: File,
    size: u64,
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
            let file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(path)
                .await?;
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
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.file.sync_data().await?;
        Ok(())
    }
}

pub(super) async fn start_wal_writer(
    dir: PathBuf,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
    cfg: WalWriterConfig,
) -> Result<(Sender<WalWriteRequest>, Receiver<WalBatch>)> {
    let (tx, rx) = mpsc::channel::<WalWriteRequest>(1024);
    let (replay_tx, replay_rx) = mpsc::channel::<WalBatch>(1024);
    tokio::spawn(wal_writer_loop(dir, durable_lsn, rx, replay_tx, cfg));
    Ok((tx, replay_rx))
}

async fn wal_writer_loop(
    dir: PathBuf,
    durable_lsn: Arc<std::sync::atomic::AtomicU64>,
    mut rx: Receiver<WalWriteRequest>,
    replay_tx: Sender<WalBatch>,
    cfg: WalWriterConfig,
) {
    let mut writer = match WalWriter::open(dir).await {
        Ok(writer) => writer,
        Err(_) => return,
    };
    while let Some(req) = rx.recv().await {
        let mut pending = vec![req];
        if cfg.group_commit_wait_us == 0 {
            while pending.len() < cfg.max_group_commit_batches {
                match rx.try_recv() {
                    Ok(next) => pending.push(next),
                    Err(_) => break,
                }
            }
        } else {
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_micros(cfg.group_commit_wait_us);
            while pending.len() < cfg.max_group_commit_batches {
                let now = std::time::Instant::now();
                if now >= deadline {
                    break;
                }
                let remaining = deadline.saturating_duration_since(now);
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(next)) => pending.push(next),
                    _ => break,
                }
            }
        }

        let mut append_err: Option<String> = None;
        let mut appended_count = 0usize;
        let mut max_end_lsn = 0u64;
        for req in &pending {
            if let Err(err) = writer.append_batch(&req.batch).await {
                append_err = Some(err.to_string());
                break;
            }
            appended_count += 1;
            max_end_lsn = max_end_lsn.max(req.batch.end_lsn);
        }

        if append_err.is_none()
            && let Err(err) = writer.flush().await
        {
            append_err = Some(err.to_string());
        }

        if let Some(err_msg) = append_err {
            for req in pending {
                if let Some(ack) = req.ack {
                    let _ = ack.send(Err(crate::Error::Io(std::io::Error::other(
                        err_msg.clone(),
                    ))));
                }
            }
            continue;
        }

        durable_lsn.store(max_end_lsn, std::sync::atomic::Ordering::Release);
        for (idx, req) in pending.into_iter().enumerate() {
            if idx < appended_count {
                let _ = replay_tx.send(req.batch.clone()).await;
            }
            if let Some(ack) = req.ack {
                let result = if idx < appended_count {
                    Ok(req.batch.end_lsn)
                } else {
                    Err(crate::Error::Io(std::io::Error::other(
                        "wal append aborted before batch was written",
                    )))
                };
                let _ = ack.send(result);
            }
        }
    }
}
