use super::{MvccVersion, WAL_OP_TXN_COMMIT, WAL_OP_TXN_DEL, WAL_OP_TXN_PUT, WalBatch};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn apply_txn_batch_to_mvcc(
    mvcc: &Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    batch: &WalBatch,
) {
    if batch.records.is_empty() {
        return;
    }
    let last = batch.records.last().unwrap();
    if last.op != WAL_OP_TXN_COMMIT {
        return;
    }
    let commit_lsn = batch.end_lsn;

    let mut store = mvcc.lock().unwrap();
    for record in &batch.records {
        match record.op {
            WAL_OP_TXN_PUT => {
                store
                    .entry(record.key.clone())
                    .or_default()
                    .push(MvccVersion {
                        commit_lsn,
                        value: Some(record.value.clone()),
                    });
            }
            WAL_OP_TXN_DEL => {
                store
                    .entry(record.key.clone())
                    .or_default()
                    .push(MvccVersion {
                        commit_lsn,
                        value: None,
                    });
            }
            WAL_OP_TXN_COMMIT => {}
            _ => {}
        }
    }
}

pub(super) fn mvcc_get_at(
    mvcc: &Arc<std::sync::Mutex<BTreeMap<Vec<u8>, Vec<MvccVersion>>>>,
    key: &[u8],
    read_lsn: u64,
) -> Option<Option<Vec<u8>>> {
    let store = mvcc.lock().unwrap();
    let versions = store.get(key)?;
    versions
        .iter()
        .rfind(|v| v.commit_lsn <= read_lsn)
        .map(|v| v.value.clone())
}

pub(super) fn gc_mvcc_versions(
    store: &mut BTreeMap<Vec<u8>, Vec<MvccVersion>>,
    watermark: u64,
) -> (usize, usize) {
    let mut keys_touched = 0usize;
    let mut versions_removed = 0usize;

    for versions in store.values_mut() {
        if versions.len() <= 1 {
            continue;
        }
        let mut last_visible: Option<usize> = None;
        for (idx, version) in versions.iter().enumerate() {
            if version.commit_lsn <= watermark {
                last_visible = Some(idx);
            } else {
                break;
            }
        }

        if let Some(idx) = last_visible
            && idx > 0 {
                versions.drain(..idx);
                keys_touched += 1;
                versions_removed += idx;
            }
    }

    (keys_touched, versions_removed)
}
