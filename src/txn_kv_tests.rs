use super::*;
use bytes::Bytes;
use crate::KEY_SIZE;
use std::time::Duration;
use tempfile::tempdir;

fn key(byte: u8) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[0] = byte;
    k
}

#[tokio::test]
async fn test_wal_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
    let mut tx = manager.begin_rw_timeout(Duration::from_secs(30));
    tx.put(&key(1), b"v1").unwrap();
    tx.delete(&key(2)).unwrap();
    let ts = tx.commit().await.unwrap();
    assert!(ts > 0);
    drop(manager);

    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
    let tx = manager.begin_ro_timeout(Duration::from_secs(30));
    assert_eq!(tx.get(&key(1)).unwrap(), Some(Bytes::from_static(b"v1")));
    assert_eq!(tx.get(&key(2)).unwrap(), None);
}

#[tokio::test]
async fn test_txn_timeout() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
    let mut tx = manager.begin_rw_timeout(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let err = tx.put(&key(1), b"v1").unwrap_err();
    assert!(matches!(err, TxnError::TxnTimeout));
}

#[tokio::test]
async fn test_txn_error_category_and_retryable() {
    let err = TxnError::WriteWriteConflict;
    assert_eq!(err.category(), TxnErrorCategory::Conflict);
    assert!(err.is_retryable());

    let err = TxnError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "dup",
    ));
    assert_eq!(err.category(), TxnErrorCategory::InvalidInput);
    assert!(!err.is_retryable());

    let err = TxnError::InvalidKeySize(1, KEY_SIZE);
    assert_eq!(err.category(), TxnErrorCategory::InvalidInput);
    assert!(!err.is_retryable());
}
