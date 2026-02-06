use crate::node::{WAL_OP_TXN_COMMIT, WalBatch, WalRecord};
use crate::{Error, Result, StorageClient, StorageQuorumClient};
use futures::future::join_all;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::LocalSet;

/// Compute-side sequencer.
///
/// Assigns a global LSN (single-writer) and replicates the same WAL batch to storage nodes,
/// returning success only after quorum acks.
pub struct ComputeSequencer {
    clients: Vec<StorageClient>,
    quorum: usize,
    next_lsn: AtomicU64, // exclusive right boundary
    request_id: AtomicU64,
    durable_lsn: AtomicU64, // quorum-durable right boundary cache
}

impl ComputeSequencer {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        if addrs.is_empty() {
            return Err(Error::InvalidKeySize(0, 1));
        }
        let quorum = quorum.max(1).min(addrs.len());

        let qc = StorageQuorumClient::connect(addrs, quorum, local).await?;
        let durable = qc.quorum_durable_lsn().await?;

        let mut clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            clients.push(StorageClient::connect(addr, local).await?);
        }

        Ok(Self {
            clients,
            quorum,
            next_lsn: AtomicU64::new(durable),
            request_id: AtomicU64::new(1),
            durable_lsn: AtomicU64::new(durable),
        })
    }

    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    pub fn begin_ro(&self) -> u64 {
        self.durable_lsn()
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Replicate a txn batch represented as WAL records.
    ///
    /// Requirements:
    /// - records must be non-empty
    /// - last record must be COMMIT marker (WAL_OP_TXN_COMMIT)
    ///
    /// Returns commitLsn (= end_lsn) on success.
    pub async fn commit_txn_batch(&self, mut records: Vec<WalRecord>) -> Result<u64> {
        if records.is_empty() {
            return Err(Error::InvalidKeySize(0, 1));
        }
        if records.last().map(|r| r.op) != Some(WAL_OP_TXN_COMMIT) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "txn batch must end with COMMIT marker",
            )));
        }

        let n = records.len() as u64;
        let start_lsn = self.next_lsn.load(Ordering::Acquire);
        let end_lsn = start_lsn + n;

        // Fill record LSNs sequentially.
        for (i, rec) in records.iter_mut().enumerate() {
            rec.lsn = start_lsn + i as u64;
        }

        let request_id = self.next_request_id();
        let batch = WalBatch {
            request_id,
            start_lsn,
            end_lsn,
            records,
        };

        // Fan-out to all storage nodes.
        let futs = self.clients.iter().map(|c| c.append_wal(&batch));
        let results = join_all(futs).await;

        let mut acks: Vec<u64> = Vec::new();
        let mut errs: Vec<String> = Vec::new();
        for r in results {
            match r {
                Ok(durable) => {
                    if durable >= end_lsn {
                        acks.push(durable);
                    } else {
                        errs.push(format!(
                            "ack durable_lsn too small: got={durable} need>={end_lsn}"
                        ));
                    }
                }
                Err(e) => {
                    errs.push(format!("rpc err: {e}"));
                }
            }
        }

        if acks.len() < self.quorum {
            let mut msg = format!(
                "quorum not reached: acks={} quorum={}",
                acks.len(),
                self.quorum
            );
            if !errs.is_empty() {
                msg.push_str("; errors=[");
                for (i, e) in errs.iter().take(3).enumerate() {
                    if i > 0 {
                        msg.push_str(" | ");
                    }
                    msg.push_str(e);
                }
                if errs.len() > 3 {
                    msg.push_str(" | ...");
                }
                msg.push(']');
            }
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                msg,
            )));
        }

        // Quorum-durable point is the quorum-th largest durable among acks.
        acks.sort_unstable_by(|a, b| b.cmp(a));
        let quorum_durable = acks[self.quorum - 1];
        self.durable_lsn.fetch_max(quorum_durable, Ordering::AcqRel);
        self.next_lsn.store(end_lsn, Ordering::Release);

        Ok(end_lsn)
    }

    /// Helper: build a minimal commit-only batch (useful for tests).
    pub fn make_commit_marker() -> WalRecord {
        WalRecord {
            lsn: 0,
            op: WAL_OP_TXN_COMMIT,
            page_id: 0,
            slot_id: 0,
            key: Vec::new(),
            value: Vec::new(),
        }
    }
}
