use tempfile::tempdir;

use scale_kv::txn_kv::{TxnError, TxnManager};
use scale_kv::{KEY_SIZE, VALUE_SIZE};

fn key(byte: u8) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[0] = byte;
    k
}

fn value(byte: u8) -> Vec<u8> {
    let len = VALUE_SIZE.min(8);
    vec![byte; len]
}

#[test]
fn test_snapshot_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open(&path).unwrap();

    let mut tx1 = manager.begin_rw();
    tx1.put(&key(1), &value(1)).unwrap();

    let tx_ro = manager.begin_ro();
    assert_eq!(tx_ro.get(&key(1)).unwrap(), None);

    tx1.commit().unwrap();

    assert_eq!(tx_ro.get(&key(1)).unwrap(), None);
    let tx_ro2 = manager.begin_ro();
    assert_eq!(tx_ro2.get(&key(1)).unwrap(), Some(value(1)));
}

#[test]
fn test_write_write_conflict() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open(&path).unwrap();

    let mut tx1 = manager.begin_rw();
    let mut tx2 = manager.begin_rw();

    tx1.put(&key(2), &value(2)).unwrap();
    tx2.put(&key(2), &value(3)).unwrap();

    tx1.commit().unwrap();
    let err = tx2.commit().unwrap_err();
    assert!(matches!(err, TxnError::WriteWriteConflict));
}

#[test]
fn test_delete_visibility() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open(&path).unwrap();

    let mut tx1 = manager.begin_rw();
    tx1.put(&key(3), &value(3)).unwrap();
    tx1.commit().unwrap();

    let tx_ro = manager.begin_ro();
    assert_eq!(tx_ro.get(&key(3)).unwrap(), Some(value(3)));

    let mut tx2 = manager.begin_rw();
    tx2.delete(&key(3)).unwrap();
    tx2.commit().unwrap();

    assert_eq!(tx_ro.get(&key(3)).unwrap(), Some(value(3)));
    let tx_ro2 = manager.begin_ro();
    assert_eq!(tx_ro2.get(&key(3)).unwrap(), None);
}

#[test]
fn test_multi_key_atomicity() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open(&path).unwrap();

    let mut tx = manager.begin_rw();
    tx.put(&key(4), &value(4)).unwrap();
    tx.put(&key(5), &value(5)).unwrap();

    let tx_ro = manager.begin_ro();
    assert_eq!(tx_ro.get(&key(4)).unwrap(), None);
    assert_eq!(tx_ro.get(&key(5)).unwrap(), None);

    tx.commit().unwrap();

    let tx_ro2 = manager.begin_ro();
    assert_eq!(tx_ro2.get(&key(4)).unwrap(), Some(value(4)));
    assert_eq!(tx_ro2.get(&key(5)).unwrap(), Some(value(5)));
}

#[test]
fn test_recovery_from_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    {
        let manager = TxnManager::open(&path).unwrap();
        let mut tx = manager.begin_rw();
        tx.put(&key(6), &value(6)).unwrap();
        tx.put(&key(7), &value(7)).unwrap();
        tx.commit().unwrap();
    }

    let manager = TxnManager::open(&path).unwrap();
    let tx = manager.begin_ro();
    assert_eq!(tx.get(&key(6)).unwrap(), Some(value(6)));
    assert_eq!(tx.get(&key(7)).unwrap(), Some(value(7)));
}
