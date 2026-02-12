use crate::{Error, Result, StorageClient};
use futures::future::join_all;
use rand;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use tokio::task::LocalSet;

/// Compute-side sequencer.
///
/// Assigns a global LSN (single-writer) and replicates the same txn batch to storage nodes,
/// returning success only after quorum acks.
pub struct ComputeSequencer {
    clients: Vec<StorageClient>,
    quorum: usize,
    next_lsn: AtomicU64,     // exclusive right boundary
    request_id: AtomicU64,   // per-sequencer request id generator
    durable_lsn: AtomicU64,  // quorum-durable right boundary cache
    dispatch_lsn: AtomicU64, // next start_lsn allowed to dispatch
    dispatch_notify: Arc<Notify>,
}

impl ComputeSequencer {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        if addrs.is_empty() {
            return Err(Error::InvalidKeySize(0, 1));
        }
        let quorum = quorum.max(1).min(addrs.len());
        let mut clients = Vec::with_capacity(addrs.len());
        let mut errs = Vec::new();
        for addr in addrs {
            match StorageClient::connect(addr, local).await {
                Ok(c) => clients.push(c),
                Err(e) => errs.push(format!("{addr}: {e}")),
            }
        }
        if clients.len() < quorum {
            let msg = format!(
                "not enough reachable storage nodes: reachable={} required={} (errors: {})",
                clients.len(),
                quorum,
                errs.join(" | ")
            );
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                msg,
            )));
        }
        let mut lsns = Vec::with_capacity(clients.len());
        for c in &clients {
            lsns.push(c.get_durable_lsn().await?);
        }
        lsns.sort_unstable_by(|a, b| b.cmp(a));
        let durable = *lsns.get(quorum - 1).unwrap_or(&0);

        let seed = {
            // Avoid request_id collisions across compute restarts.
            // (Storage keeps an in-memory request_index for idempotency.)
            let r = rand::random::<u64>();
            if r == 0 { 1 } else { r }
        };

        Ok(Self {
            clients,
            quorum,
            next_lsn: AtomicU64::new(durable),
            request_id: AtomicU64::new(seed),
            durable_lsn: AtomicU64::new(durable),
            dispatch_lsn: AtomicU64::new(durable),
            dispatch_notify: Arc::new(Notify::new()),
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

    pub fn allocate_request_id(&self) -> u64 {
        self.next_request_id()
    }

    pub fn reserve_txn_with_request_id(
        &self,
        n_writes: usize,
        request_id: u64,
    ) -> Result<(u64, u64)> {
        const MAX_WRITES_PER_TXN: usize = 256;
        if n_writes == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "txn batch must be non-empty",
            )));
        }
        if n_writes > MAX_WRITES_PER_TXN {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "txn batch too large: writes={} max={}",
                    n_writes, MAX_WRITES_PER_TXN
                ),
            )));
        }
        if request_id == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "request_id must be non-zero",
            )));
        }

        let n = n_writes as u64;
        let start_lsn = self.next_lsn.fetch_add(n, Ordering::AcqRel);
        let end_lsn = start_lsn + n;
        Ok((start_lsn, end_lsn))
    }

    pub fn reserve_txn(&self, n_writes: usize) -> Result<(u64, u64, u64)> {
        let request_id = self.next_request_id();
        let (start_lsn, end_lsn) = self.reserve_txn_with_request_id(n_writes, request_id)?;
        Ok((request_id, start_lsn, end_lsn))
    }

    /// Replicate a txn batch represented as page after-images.
    ///
    /// Convenience wrapper: reserves an LSN range then commits.
    /// Returns commitLsn (= end_lsn) on success.
    pub async fn commit_txn_batch(&self, writes: Vec<(u64, Vec<u8>)>) -> Result<u64> {
        let (request_id, start_lsn, end_lsn) = self.reserve_txn(writes.len())?;
        self.commit_reserved_txn_batch(request_id, start_lsn, end_lsn, writes)
            .await
    }

    /// Replicate a reserved txn batch represented as page after-images.
    ///
    /// Returns commitLsn (= end_lsn) on success.
    pub async fn commit_reserved_txn_batch(
        &self,
        request_id: u64,
        start_lsn: u64,
        end_lsn: u64,
        writes: Vec<(u64, Vec<u8>)>,
    ) -> Result<u64> {
        // Enforce global dispatch order by reserved LSN range.
        loop {
            let cur = self.dispatch_lsn.load(Ordering::Acquire);
            if cur == start_lsn {
                break;
            }
            if start_lsn < cur {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "stale reserved range: start_lsn={} dispatch_lsn={}",
                        start_lsn, cur
                    ),
                )));
            }
            self.dispatch_notify.notified().await;
        }

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
                Ok((commit, durable)) => {
                    // Storage returns commit_lsn (= end_lsn) only after WAL is flushed and replay applied.
                    // Treat commit_lsn as the authoritative quorum-ack.
                    if commit >= end_lsn {
                        acks.push(commit);
                    } else if durable >= end_lsn {
                        acks.push(durable);
                    } else {
                        errs.push(format!(
                            "ack too small: commit={commit} durable={durable} need>={end_lsn}"
                        ));
                    }
                }
                Err(e) => {
                    errs.push(format!("rpc err: {e}"));
                }
            }
        }

        if acks.len() < self.quorum {
            self.dispatch_lsn.store(end_lsn, Ordering::Release);
            self.dispatch_notify.notify_waiters();
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
        self.dispatch_lsn.store(end_lsn, Ordering::Release);
        self.dispatch_notify.notify_waiters();

        Ok(end_lsn)
    }
}
