use crate::compute_sequencer::ComputeSequencer;
use crate::node::{WAL_OP_TXN_COMMIT, WAL_OP_TXN_DEL, WAL_OP_TXN_PUT, WalRecord};
use crate::{Error, Result, StorageClient};
use std::sync::Arc;
use tokio::task::LocalSet;

/// Embedded compute-side API for an Aurora-style KV.
///
/// - No compute server / network protocol required.
/// - All writes are committed as txn WAL batches via [`ComputeSequencer`].
/// - Reads are served via `txn_get(key, read_lsn)` against a storage node that is
///   durable at `read_lsn`.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Arc<ComputeSequencer>,
    readers: Arc<Vec<StorageClient>>,
}

impl EmbeddedCompute {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        let sequencer = Arc::new(ComputeSequencer::connect(addrs, quorum, local).await?);

        let mut readers = Vec::with_capacity(addrs.len());
        for addr in addrs {
            readers.push(StorageClient::connect(addr, local).await?);
        }

        Ok(Self {
            sequencer,
            readers: Arc::new(readers),
        })
    }

    /// Returns a safe snapshot point for reads (quorum-durable LSN).
    pub fn begin_ro(&self) -> u64 {
        self.sequencer.begin_ro()
    }

    pub fn durable_lsn(&self) -> u64 {
        self.sequencer.durable_lsn()
    }

    /// Begin a read-write transaction context (buffer ops until commit).
    pub fn begin(&self) -> EmbeddedTxn {
        EmbeddedTxn {
            compute: self.clone(),
            records: Vec::new(),
        }
    }

    /// Convenience: auto-wrap a single PUT in its own txn and commit.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let mut txn = self.begin();
        txn.put(key, value);
        txn.commit().await
    }

    /// Convenience: auto-wrap a single DELETE in its own txn and commit.
    pub async fn delete(&self, key: Vec<u8>) -> Result<u64> {
        let mut txn = self.begin();
        txn.delete(key);
        txn.commit().await
    }

    /// Read at a specific snapshot (read_lsn).
    pub async fn get_at(&self, key: &[u8], read_lsn: u64) -> Result<Option<Vec<u8>>> {
        // Try to find a reader that is durable at read_lsn.
        for c in self.readers.iter() {
            let durable = c.get_durable_lsn().await?;
            if durable >= read_lsn {
                let (v, _durable2) = c.txn_get(&key.to_vec(), read_lsn).await?;
                return Ok(v);
            }
        }

        // None of the nodes claims durable>=read_lsn. In a real system we'd wait or retry.
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            format!(
                "no storage node is durable at read_lsn={}; try again later",
                read_lsn
            ),
        )))
    }

    /// Convenience: read at the latest quorum-durable snapshot.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let read_lsn = self.begin_ro();
        self.get_at(key, read_lsn).await
    }

    async fn commit_records(&self, mut records: Vec<WalRecord>) -> Result<u64> {
        // Ensure COMMIT marker.
        if records
            .last()
            .map(|r| r.op)
            .is_none_or(|op| op != WAL_OP_TXN_COMMIT)
        {
            records.push(WalRecord {
                lsn: 0,
                op: WAL_OP_TXN_COMMIT,
                page_id: 0,
                slot_id: 0,
                key: Vec::new(),
                value: Vec::new(),
            });
        }
        self.sequencer.commit_txn_batch(records).await
    }
}

/// An embedded, buffered transaction.
///
/// If you never call [`EmbeddedCompute::begin`], you can still use `put/delete` which
/// auto-wrap each operation in its own txn.
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,
    records: Vec<WalRecord>,
}

impl EmbeddedTxn {
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.records.push(WalRecord {
            lsn: 0,
            op: WAL_OP_TXN_PUT,
            page_id: 0,
            slot_id: 0,
            key,
            value,
        });
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.records.push(WalRecord {
            lsn: 0,
            op: WAL_OP_TXN_DEL,
            page_id: 0,
            slot_id: 0,
            key,
            value: Vec::new(),
        });
    }

    pub async fn commit(self) -> Result<u64> {
        self.compute.commit_records(self.records).await
    }
}
