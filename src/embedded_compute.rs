use crate::compute_sequencer::ComputeSequencer;
use crate::page_bptree::{DEFAULT_PAGE_CACHE_SHARDS, PageCache};
use crate::{Error, PageId, Result, StorageClient};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::task::LocalSet;

/// Embedded compute-side API for an Aurora-style KV.
///
/// - No compute server / network protocol required.
/// - All writes are committed as txn WAL batches via [`ComputeSequencer`].
/// - Reads are served from compute's in-memory MVCC state.
///
/// Note: page fetch (`getPage/scanPages`) is provided by storage for compute warmup,
/// but this embedded compute currently does not rebuild state from pages yet.
#[derive(Clone)]
pub struct EmbeddedCompute {
    sequencer: Arc<ComputeSequencer>,
    readers: Arc<Vec<StorageClient>>,

    // Warmed pages. (Eventually this should back the compute-side page cache / B+Tree.)
    page_cache: Arc<PageCache>,

    // key -> versions sorted by commit_lsn (append-only)
    mvcc: Arc<Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
}

#[derive(Clone, Debug)]
struct MvccVersion {
    commit_lsn: u64,
    value: Option<Vec<u8>>,
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
            page_cache: Arc::new(PageCache::new(DEFAULT_PAGE_CACHE_SHARDS)),
            mvcc: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Returns a safe snapshot point for reads (quorum-durable LSN).
    pub fn begin_ro(&self) -> u64 {
        self.sequencer.begin_ro()
    }

    pub fn durable_lsn(&self) -> u64 {
        self.sequencer.durable_lsn()
    }

    pub fn warmed_pages(&self) -> usize {
        self.page_cache.len()
    }

    /// Warm up compute by scanning pages from storage and caching them locally.
    ///
    /// This uses `scanPages` and fetches the latest pages only.
    ///
    /// Returns the number of pages cached.
    pub async fn warmup_scan_all(&self, limit_per_batch: u32) -> Result<usize> {
        let limit_per_batch = limit_per_batch.max(1);

        // Pick the first reader for warmup.
        let reader = self.readers.get(0).ok_or_else(|| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::Other, "no storage"))
        })?;

        let mut start: PageId = 0;
        loop {
            let (pages, _durable) = reader.scan_pages(start, limit_per_batch).await?;
            if pages.is_empty() {
                break;
            }

            for (page_id, _page_lsn, page) in &pages {
                self.page_cache.insert(*page_id, page.clone());
            }

            // Continue from the largest page id we saw + 1.
            if let Some((last_id, _, _)) = pages.last() {
                start = last_id.saturating_add(1);
            } else {
                break;
            }
        }

        Ok(self.page_cache.len())
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

    /// Read at a specific snapshot (read_lsn) from compute MVCC.
    pub fn get_at(&self, key: &[u8], read_lsn: u64) -> Result<Option<Vec<u8>>> {
        let store = self.mvcc.lock().unwrap();
        let versions = store.get(key);
        if versions.is_none() {
            return Ok(None);
        }
        let versions = versions.unwrap();
        let found = versions
            .iter()
            .rfind(|v| v.commit_lsn <= read_lsn)
            .map(|v| v.value.clone())
            .unwrap_or(None);
        Ok(found)
    }

    /// Convenience: read at the latest quorum-durable snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let read_lsn = self.begin_ro();
        self.get_at(key, read_lsn)
    }

    async fn commit_records(&self, mut records: Vec<(u8, Vec<u8>, Vec<u8>)>) -> Result<u64> {
        // Ensure COMMIT marker.
        if records.last().map(|r| r.0).is_none_or(|op| op != 3) {
            records.push((3, Vec::new(), Vec::new()));
        }

        let commit_lsn = self.sequencer.commit_txn_batch(records.clone()).await?;

        // Apply to compute MVCC at commit point.
        let mut store = self.mvcc.lock().unwrap();
        for (op, key, value) in records {
            match op {
                1 => {
                    store.entry(key).or_default().push(MvccVersion {
                        commit_lsn,
                        value: Some(value),
                    });
                }
                2 => {
                    store.entry(key).or_default().push(MvccVersion {
                        commit_lsn,
                        value: None,
                    });
                }
                3 => {}
                _ => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid txn op: {op}"),
                    )));
                }
            }
        }

        Ok(commit_lsn)
    }
}

/// An embedded, buffered transaction.
///
/// If you never call [`EmbeddedCompute::begin`], you can still use `put/delete` which
/// auto-wrap each operation in its own txn.
pub struct EmbeddedTxn {
    compute: EmbeddedCompute,
    records: Vec<(u8, Vec<u8>, Vec<u8>)>,
}

impl EmbeddedTxn {
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.records.push((1, key, value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.records.push((2, key, Vec::new()));
    }

    pub async fn commit(self) -> Result<u64> {
        self.compute.commit_records(self.records).await
    }
}
