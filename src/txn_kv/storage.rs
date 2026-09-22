use super::{Key, Result, TxnError, Version};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::mpsc;

use super::codec::{load_snapshot_file, replay_wal_with_base, snapshot_path, write_snapshot_file};

#[async_trait::async_trait]
pub trait TxnStorage: Send + Sync + std::fmt::Debug {
    async fn load_snapshot(&self) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>>;
    async fn write_snapshot(
        &self,
        commit_ts: u64,
        store: &BTreeMap<Key, Vec<Version>>,
    ) -> Result<()>;
    async fn replay_wal(
        &self,
        base_store: BTreeMap<Key, Vec<Version>>,
        base_ts: u64,
    ) -> Result<(BTreeMap<Key, Vec<Version>>, u64)>;
    async fn append_wal_record(&self, record: &[u8]) -> Result<()>;
    async fn truncate_wal(&self) -> Result<()>;
}

#[derive(Debug)]
struct LocalFileStorage {
    wal: tokio::sync::Mutex<tokio::fs::File>,
    wal_path: PathBuf,
}

impl LocalFileStorage {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        Ok(Self {
            wal: tokio::sync::Mutex::new(file),
            wal_path: path,
        })
    }
}

#[async_trait::async_trait]
impl TxnStorage for LocalFileStorage {
    async fn load_snapshot(&self) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
        load_snapshot_file(&snapshot_path(&self.wal_path)).await
    }

    async fn write_snapshot(
        &self,
        commit_ts: u64,
        store: &BTreeMap<Key, Vec<Version>>,
    ) -> Result<()> {
        write_snapshot_file(&snapshot_path(&self.wal_path), commit_ts, store).await
    }

    async fn replay_wal(
        &self,
        base_store: BTreeMap<Key, Vec<Version>>,
        base_ts: u64,
    ) -> Result<(BTreeMap<Key, Vec<Version>>, u64)> {
        let mut wal = self.wal.lock().await;
        let (store, max_ts, valid_len) =
            replay_wal_with_base(&mut wal, base_store, base_ts).await?;
        let file_len = wal.metadata().await?.len();
        if valid_len < file_len {
            wal.set_len(valid_len).await?;
        }
        wal.seek(SeekFrom::End(0)).await?;
        Ok((store, max_ts))
    }

    async fn append_wal_record(&self, record: &[u8]) -> Result<()> {
        let mut wal = self.wal.lock().await;
        wal.write_all(record).await?;
        wal.sync_all().await?;
        Ok(())
    }

    async fn truncate_wal(&self) -> Result<()> {
        let mut wal = self.wal.lock().await;
        wal.set_len(0).await?;
        wal.seek(SeekFrom::Start(0)).await?;
        wal.sync_all().await?;
        Ok(())
    }
}

#[derive(Debug)]
struct QuorumReplica {
    wal_path: PathBuf,
    storage: tokio::sync::Mutex<Option<Arc<LocalFileStorage>>>,
}

#[derive(Clone, Debug)]
enum QuorumOp {
    Append {
        record: Arc<Vec<u8>>,
    },
    Snapshot {
        commit_ts: u64,
        store: Arc<BTreeMap<Key, Vec<Version>>>,
    },
    Truncate,
}

impl QuorumOp {
    async fn apply(&self, storage: &LocalFileStorage) -> Result<()> {
        match self {
            QuorumOp::Append { record } => storage.append_wal_record(record.as_slice()).await,
            QuorumOp::Snapshot { commit_ts, store } => {
                storage.write_snapshot(*commit_ts, store).await
            }
            QuorumOp::Truncate => storage.truncate_wal().await,
        }
    }
}

#[derive(Debug)]
struct RepairTask {
    replica_index: usize,
    op: QuorumOp,
    attempt: u32,
}

#[derive(Debug)]
pub struct QuorumStorage {
    replicas: Arc<Vec<QuorumReplica>>,
    quorum: usize,
    repair_tx: mpsc::UnboundedSender<RepairTask>,
}

impl QuorumStorage {
    pub fn open(paths: Vec<PathBuf>, quorum: usize) -> Result<Self> {
        if quorum < 1 || quorum > paths.len() {
            return Err(TxnError::InvalidQuorum {
                quorum,
                replicas: paths.len(),
            });
        }
        let replicas = paths
            .into_iter()
            .map(|path| QuorumReplica {
                wal_path: if path.file_name().and_then(|name| name.to_str()) == Some("wal.log") {
                    path
                } else {
                    path.join("wal.log")
                },
                storage: tokio::sync::Mutex::new(None),
            })
            .collect::<Vec<_>>();
        let replicas = Arc::new(replicas);
        let (repair_tx, repair_rx) = mpsc::unbounded_channel();
        let worker_replicas = Arc::clone(&replicas);
        tokio::spawn(async move { repair_worker(worker_replicas, repair_rx).await });
        Ok(Self {
            replicas,
            quorum,
            repair_tx,
        })
    }

    async fn quorum_write(&self, op: QuorumOp) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for index in 0..self.replicas.len() {
            let tx = tx.clone();
            let replicas = Arc::clone(&self.replicas);
            let op = op.clone();
            let repair_tx = self.repair_tx.clone();
            tokio::spawn(async move {
                let result = with_replica(&replicas, index).await;
                let result = match result {
                    Ok(storage) => op.apply(&storage).await,
                    Err(err) => Err(err),
                };
                if result.is_err() {
                    let _ = repair_tx.send(RepairTask {
                        replica_index: index,
                        op: op.clone(),
                        attempt: 1,
                    });
                }
                let _ = tx.send(result);
            });
        }
        drop(tx);
        let mut successes = 0usize;
        let mut last_err: Option<TxnError> = None;
        while let Some(result) = rx.recv().await {
            match result {
                Ok(()) => {
                    successes += 1;
                    if successes >= self.quorum {
                        return Ok(());
                    }
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or(TxnError::QuorumNotMet {
            required: self.quorum,
            succeeded: successes,
        }))
    }
}

async fn with_replica(
    replicas: &Arc<Vec<QuorumReplica>>,
    index: usize,
) -> Result<Arc<LocalFileStorage>> {
    let replica = &replicas[index];
    let mut guard = replica.storage.lock().await;
    if guard.is_none() {
        if let Some(parent) = replica.wal_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        *guard = Some(Arc::new(LocalFileStorage::open(&replica.wal_path).await?));
    }
    Ok(guard.as_ref().expect("replica initialized").clone())
}

async fn repair_worker(
    replicas: Arc<Vec<QuorumReplica>>,
    mut repair_rx: mpsc::UnboundedReceiver<RepairTask>,
) {
    const REPAIR_MAX_ATTEMPTS: u32 = 5;
    const REPAIR_BASE_DELAY_MS: u64 = 50;
    while let Some(mut task) = repair_rx.recv().await {
        loop {
            let ok = match with_replica(&replicas, task.replica_index).await {
                Ok(storage) => task.op.apply(&storage).await.is_ok(),
                Err(_) => false,
            };
            if ok {
                break;
            }
            if task.attempt >= REPAIR_MAX_ATTEMPTS {
                break;
            }
            let backoff = REPAIR_BASE_DELAY_MS.saturating_mul(1u64 << (task.attempt - 1));
            tokio::time::sleep(Duration::from_millis(backoff)).await;
            task.attempt += 1;
        }
    }
}

#[async_trait::async_trait]
impl TxnStorage for QuorumStorage {
    async fn load_snapshot(&self) -> Result<Option<(BTreeMap<Key, Vec<Version>>, u64)>> {
        let mut best: Option<(BTreeMap<Key, Vec<Version>>, u64)> = None;
        let mut saw_ok = false;
        let mut last_err: Option<TxnError> = None;
        for index in 0..self.replicas.len() {
            match with_replica(&self.replicas, index).await {
                Ok(storage) => match storage.load_snapshot().await {
                    Ok(snapshot) => {
                        saw_ok = true;
                        if let Some((store, ts)) = snapshot {
                            let replace = match &best {
                                Some((_, best_ts)) => ts > *best_ts,
                                None => true,
                            };
                            if replace {
                                best = Some((store, ts));
                            }
                        }
                    }
                    Err(err) => last_err = Some(err),
                },
                Err(err) => last_err = Some(err),
            }
        }
        if best.is_some() {
            return Ok(best);
        }
        if saw_ok {
            Ok(None)
        } else {
            Err(last_err.unwrap_or(TxnError::QuorumNotMet {
                required: self.quorum,
                succeeded: 0,
            }))
        }
    }

    async fn write_snapshot(
        &self,
        commit_ts: u64,
        store: &BTreeMap<Key, Vec<Version>>,
    ) -> Result<()> {
        let op = QuorumOp::Snapshot {
            commit_ts,
            store: Arc::new(store.clone()),
        };
        self.quorum_write(op).await
    }

    async fn replay_wal(
        &self,
        _base_store: BTreeMap<Key, Vec<Version>>,
        _base_ts: u64,
    ) -> Result<(BTreeMap<Key, Vec<Version>>, u64)> {
        let mut best: Option<(BTreeMap<Key, Vec<Version>>, u64)> = None;
        let mut last_err: Option<TxnError> = None;
        for index in 0..self.replicas.len() {
            let result = match with_replica(&self.replicas, index).await {
                Ok(storage) => {
                    let snapshot = storage.load_snapshot().await?;
                    let (base_store, base_ts) = snapshot.unwrap_or_default();
                    storage.replay_wal(base_store, base_ts).await
                }
                Err(err) => Err(err),
            };
            match result {
                Ok((store, max_ts)) => {
                    let replace = match &best {
                        Some((_, best_ts)) => max_ts > *best_ts,
                        None => true,
                    };
                    if replace {
                        best = Some((store, max_ts));
                    }
                }
                Err(err) => last_err = Some(err),
            }
        }
        best.ok_or_else(|| {
            last_err.unwrap_or(TxnError::QuorumNotMet {
                required: self.quorum,
                succeeded: 0,
            })
        })
    }

    async fn append_wal_record(&self, record: &[u8]) -> Result<()> {
        let op = QuorumOp::Append {
            record: Arc::new(record.to_vec()),
        };
        self.quorum_write(op).await
    }

    async fn truncate_wal(&self) -> Result<()> {
        self.quorum_write(QuorumOp::Truncate).await
    }
}
