use crate::compute_sequencer::ComputeSequencer;
use crate::{Error, Page, PageId, Result};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct InProcessPageStore {
    pages: std::sync::Mutex<HashMap<PageId, Page>>,
}

impl InProcessPageStore {
    pub(crate) fn get(&self, page_id: PageId) -> Option<Page> {
        self.pages
            .lock()
            .expect("inprocess page store lock poisoned")
            .get(&page_id)
            .cloned()
    }

    pub(crate) fn put_pages(&self, pages: Vec<(PageId, Page)>) {
        let mut guard = self
            .pages
            .lock()
            .expect("inprocess page store lock poisoned");
        for (page_id, page) in pages {
            guard.insert(page_id, page);
        }
    }
}

pub(crate) struct InProcessSequencer {
    page_store: Arc<InProcessPageStore>,
    next_lsn: AtomicU64,
    request_id: AtomicU64,
    durable_lsn: AtomicU64,
}

impl InProcessSequencer {
    pub(crate) fn new(page_store: Arc<InProcessPageStore>) -> Self {
        let seed = {
            let r = rand::random::<u64>();
            if r == 0 { 1 } else { r }
        };
        Self {
            page_store,
            next_lsn: AtomicU64::new(0),
            request_id: AtomicU64::new(seed),
            durable_lsn: AtomicU64::new(0),
        }
    }

    pub(crate) fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn begin_ro(&self) -> u64 {
        self.durable_lsn()
    }

    pub(crate) fn allocate_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn reserve_txn_with_request_id(
        &self,
        n_writes: usize,
        request_id: u64,
    ) -> Result<(u64, u64)> {
        if n_writes == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "txn batch must be non-empty",
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

    pub(crate) fn reserve_txn(&self, n_writes: usize) -> Result<(u64, u64, u64)> {
        let request_id = self.allocate_request_id();
        let (start_lsn, end_lsn) = self.reserve_txn_with_request_id(n_writes, request_id)?;
        Ok((request_id, start_lsn, end_lsn))
    }

    pub(crate) async fn commit_pages_reserved(
        &self,
        _request_id: u64,
        _start_lsn: u64,
        end_lsn: u64,
        pages: Vec<(PageId, Page)>,
    ) -> Result<u64> {
        self.page_store.put_pages(pages);
        self.durable_lsn.fetch_max(end_lsn, Ordering::AcqRel);
        Ok(end_lsn)
    }
}

#[derive(Clone)]
pub(crate) enum Sequencer {
    Network(Arc<ComputeSequencer>),
    InProcess(Arc<InProcessSequencer>),
}

impl Sequencer {
    pub(crate) fn begin_ro(&self) -> u64 {
        match self {
            Self::Network(s) => s.begin_ro(),
            Self::InProcess(s) => s.begin_ro(),
        }
    }

    pub(crate) fn durable_lsn(&self) -> u64 {
        match self {
            Self::Network(s) => s.durable_lsn(),
            Self::InProcess(s) => s.durable_lsn(),
        }
    }

    pub(crate) fn allocate_request_id(&self) -> u64 {
        match self {
            Self::Network(s) => s.allocate_request_id(),
            Self::InProcess(s) => s.allocate_request_id(),
        }
    }

    pub(crate) fn reserve_txn_with_request_id(
        &self,
        n_writes: usize,
        request_id: u64,
    ) -> Result<(u64, u64)> {
        match self {
            Self::Network(s) => s.reserve_txn_with_request_id(n_writes, request_id),
            Self::InProcess(s) => s.reserve_txn_with_request_id(n_writes, request_id),
        }
    }

    pub(crate) fn reserve_txn(&self, n_writes: usize) -> Result<(u64, u64, u64)> {
        match self {
            Self::Network(s) => s.reserve_txn(n_writes),
            Self::InProcess(s) => s.reserve_txn(n_writes),
        }
    }

    pub(crate) async fn commit_pages_reserved(
        &self,
        request_id: u64,
        start_lsn: u64,
        end_lsn: u64,
        pages: Vec<(PageId, Page)>,
    ) -> Result<u64> {
        match self {
            Self::Network(s) => {
                s.commit_reserved_txn_batch(request_id, start_lsn, end_lsn, pages)
                    .await
            }
            Self::InProcess(s) => {
                s.commit_pages_reserved(request_id, start_lsn, end_lsn, pages)
                    .await
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingTxn<const KEY_SIZE: usize> {
    pub(crate) deadline: Instant,
    pub(crate) write_keys: BTreeSet<[u8; KEY_SIZE]>,
    pub(crate) notify: Arc<Notify>,
}
