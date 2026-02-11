use crate::{Error, Result, StorageClient};
use tokio::task::LocalSet;

/// Quorum-aware wrapper for talking to multiple storage nodes.
///
/// Currently supports quorum-durable LSN computation by querying each node.
pub struct StorageQuorumClient {
    clients: Vec<StorageClient>,
    quorum: usize,
}

impl StorageQuorumClient {
    pub async fn connect(addrs: &[String], quorum: usize, local: &LocalSet) -> Result<Self> {
        if addrs.is_empty() {
            return Err(Error::InvalidValueSize(0, 1));
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
        Ok(Self { clients, quorum })
    }

    /// Returns the maximum LSN that is durable on at least `quorum` nodes.
    pub async fn quorum_durable_lsn(&self) -> Result<u64> {
        let mut lsns = Vec::with_capacity(self.clients.len());
        for c in &self.clients {
            lsns.push(c.get_durable_lsn().await?);
        }
        // We want max X such that at least quorum nodes have durable >= X.
        // Sort desc; pick the quorum-th element.
        lsns.sort_unstable_by(|a, b| b.cmp(a));
        Ok(*lsns.get(self.quorum - 1).unwrap_or(&0))
    }
}
