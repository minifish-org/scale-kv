use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Tracks active read snapshots (read_lsn) so GC can compute a safe watermark.
///
/// Invariants:
/// - Each active txn registers exactly one read_lsn.
/// - Dropping the returned guard unregisters it.
#[derive(Debug, Default)]
pub struct ActiveReads {
    next_id: AtomicU64,
    // id -> read_lsn
    map: Mutex<BTreeMap<u64, u64>>,
}

impl ActiveReads {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            map: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn register(self: &Arc<Self>, read_lsn: u64) -> ReadGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.lock().unwrap().insert(id, read_lsn);
        ReadGuard {
            inner: Arc::clone(self),
            id,
        }
    }

    pub fn unregister(&self, id: u64) {
        self.map.lock().unwrap().remove(&id);
    }

    pub fn min_read_lsn(&self) -> Option<u64> {
        self.map.lock().unwrap().values().copied().min()
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

#[derive(Debug)]
pub struct ReadGuard {
    inner: Arc<ActiveReads>,
    id: u64,
}

impl ReadGuard {
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.inner.unregister(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_reads_min() {
        let ar = Arc::new(ActiveReads::new());
        let g1 = ar.register(5);
        let g2 = ar.register(3);
        let g3 = ar.register(7);
        assert_eq!(ar.min_read_lsn(), Some(3));
        drop(g2);
        assert_eq!(ar.min_read_lsn(), Some(5));
        drop(g1);
        assert_eq!(ar.min_read_lsn(), Some(7));
        drop(g3);
        assert_eq!(ar.min_read_lsn(), None);
        assert_eq!(ar.len(), 0);
    }
}
