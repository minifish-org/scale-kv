use crate::{Error, Result, StorageClient, StorageQuorumClient};
use futures::future::join_all;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::LocalSet;

/// Compute-side sequencer.
///
/// Assigns a global LSN (single-writer) and replicates the same txn batch to storage nodes,
/// returning success only after quorum acks.
pub struct ComputeSequencer {
    clients: Vec<StorageClient>,
    quorum: usize,
    next_lsn: AtomicU64,    // exclusive right boundary
    request_id: AtomicU64,  // per-sequencer request id generator
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

    /// Replicate a txn batch represented as page after-images.
    ///
    /// `writes` format: (page_id, page_bytes)
    ///
    /// Returns commitLsn (= end_lsn) on success.
    pub async fn commit_txn_batch(&self, writes: Vec<(u64, Vec<u8>)>) -> Result<u64> {
        const MAX_WRITES_PER_TXN: usize = 256;

        if writes.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "txn batch must be non-empty",
            )));
        }
        if writes.len() > MAX_WRITES_PER_TXN {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "txn batch too large: writes={} max={}",
                    writes.len(),
                    MAX_WRITES_PER_TXN
                ),
            )));
        }

        let n = writes.len() as u64;
        let start_lsn = self.next_lsn.load(Ordering::Acquire);
        let end_lsn = start_lsn + n;

        let request_id = self.next_request_id();

        // Fan-out to all storage nodes.
        let futs = self
            .clients
            .iter()
            .map(|c| c.append_txn_batch(request_id, start_lsn, end_lsn, &writes));
        let results = join_all(futs).await;

        let mut acks: Vec<u64> = Vec::new();
        let mut errs: Vec<String> = Vec::new();
        for r in results {
            match r {
                Ok((_commit, durable)) => {
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
}
