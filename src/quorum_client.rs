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
                Ok(c) => match c.get_durable_lsn().await {
                    Ok(_) => clients.push(c),
                    Err(e) => errs.push(format!("{addr}: {e}")),
                },
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
            return Err(Error::Io(std::io::Error::other(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EmbeddedCompute, KEY_SIZE, StorageServer, VALUE_SIZE};
    use tempfile::tempdir;

    #[tokio::test(flavor = "current_thread")]
    async fn test_connect_rejects_empty_and_unreachable_quorum() {
        let local = LocalSet::new();
        local
            .run_until(async {
                assert!(StorageQuorumClient::connect(&[], 1, &local).await.is_err());

                let dir = tempdir().unwrap();
                let server = StorageServer::start_with_dir(
                    "127.0.0.1:0".parse().unwrap(),
                    dir.path().to_path_buf(),
                )
                .await
                .unwrap();
                let addrs = vec![server.addr().to_string(), "127.0.0.1:1".to_string()];
                let err = match StorageQuorumClient::connect(&addrs, 2, &local).await {
                    Ok(_) => panic!("connect should fail when reachable nodes are below quorum"),
                    Err(e) => e.to_string(),
                };
                assert!(err.contains("not enough reachable storage nodes"));
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_quorum_durable_lsn_smoke() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let dir1 = tempdir().unwrap();
                let dir2 = tempdir().unwrap();
                let s1 = StorageServer::start_with_dir(
                    "127.0.0.1:0".parse().unwrap(),
                    dir1.path().to_path_buf(),
                )
                .await
                .unwrap();
                let s2 = StorageServer::start_with_dir(
                    "127.0.0.1:0".parse().unwrap(),
                    dir2.path().to_path_buf(),
                )
                .await
                .unwrap();
                let addrs = vec![s1.addr().to_string(), s2.addr().to_string()];

                let compute = EmbeddedCompute::connect(&addrs, 2, &local).await.unwrap();
                let key = [9u8; KEY_SIZE];
                let value = vec![7u8; VALUE_SIZE];
                compute.put(&key, &value).await.unwrap();

                let quorum = StorageQuorumClient::connect(&addrs, 2, &local)
                    .await
                    .unwrap();
                let lsn = quorum.quorum_durable_lsn().await.unwrap();
                assert!(lsn > 0);
            })
            .await;
    }
}
