use bytes::Bytes;
use tempfile::tempdir;

#[cfg(unix)]
use std::path::PathBuf;

use scale_kv::txn_kv::{TxnError, TxnManager};
use scale_kv::{KEY_SIZE, VALUE_SIZE};

fn key(byte: u8) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[0] = byte;
    k
}

fn value(byte: u8) -> Bytes {
    let len = VALUE_SIZE.min(8);
    Bytes::from(vec![byte; len])
}

#[tokio::test]
async fn test_snapshot_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx1 = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx1.put(&key(1), &value(1)).unwrap();

    let tx_ro = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx_ro.get(&key(1)).unwrap(), None);

    tx1.commit().await.unwrap();

    assert_eq!(tx_ro.get(&key(1)).unwrap(), None);
    let tx_ro2 = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx_ro2.get(&key(1)).unwrap(), Some(value(1)));
}

#[tokio::test]
async fn test_write_write_conflict() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx1 = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    let mut tx2 = manager.begin_rw_timeout(std::time::Duration::from_secs(30));

    tx1.put(&key(2), &value(2)).unwrap();
    tx2.put(&key(2), &value(3)).unwrap();

    tx1.commit().await.unwrap();
    let err = tx2.commit().await.unwrap_err();
    assert!(matches!(err, TxnError::WriteWriteConflict));
}

#[tokio::test]
async fn test_delete_visibility() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx1 = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx1.put(&key(3), &value(3)).unwrap();
    tx1.commit().await.unwrap();

    let tx_ro = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx_ro.get(&key(3)).unwrap(), Some(value(3)));

    let mut tx2 = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx2.delete(&key(3)).unwrap();
    tx2.commit().await.unwrap();

    assert_eq!(tx_ro.get(&key(3)).unwrap(), Some(value(3)));
    let tx_ro2 = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx_ro2.get(&key(3)).unwrap(), None);
}

#[tokio::test]
async fn test_multi_key_atomicity() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(4), &value(4)).unwrap();
    tx.put(&key(5), &value(5)).unwrap();

    let tx_ro = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx_ro.get(&key(4)).unwrap(), None);
    assert_eq!(tx_ro.get(&key(5)).unwrap(), None);

    tx.commit().await.unwrap();

    let tx_ro2 = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx_ro2.get(&key(4)).unwrap(), Some(value(4)));
    assert_eq!(tx_ro2.get(&key(5)).unwrap(), Some(value(5)));
}

#[tokio::test]
async fn test_recovery_from_wal() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    {
        let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
        let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
        tx.put(&key(6), &value(6)).unwrap();
        tx.put(&key(7), &value(7)).unwrap();
        tx.commit().await.unwrap();
    }

    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
    let tx = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx.get(&key(6)).unwrap(), Some(value(6)));
    assert_eq!(tx.get(&key(7)).unwrap(), Some(value(7)));
}

#[tokio::test]
async fn test_checkpoint_and_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    {
        let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
        let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
        tx.put(&key(10), &value(10)).unwrap();
        tx.put(&key(11), &value(11)).unwrap();
        tx.commit().await.unwrap();
        manager.checkpoint().await.unwrap();
    }

    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();
    let tx = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx.get(&key(10)).unwrap(), Some(value(10)));
    assert_eq!(tx.get(&key(11)).unwrap(), Some(value(11)));
}

#[tokio::test]
async fn test_wal_truncation_reduces_size() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(20), &value(20)).unwrap();
    tx.put(&key(21), &value(21)).unwrap();
    tx.commit().await.unwrap();

    let before = std::fs::metadata(&path).unwrap().len();
    assert!(before > 0);

    manager.checkpoint().await.unwrap();

    let after = std::fs::metadata(&path).unwrap().len();
    assert!(after < before);
}

#[tokio::test]
async fn test_scan_basic_ordering() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(3), &value(3)).unwrap();
    tx.put(&key(1), &value(1)).unwrap();
    tx.put(&key(2), &value(2)).unwrap();
    tx.commit().await.unwrap();

    let tx_ro = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    let rows = tx_ro.scan(&key(1), &key(4), 10).unwrap();
    let keys: Vec<u8> = rows.iter().map(|(k, _)| k[0]).collect();
    assert_eq!(keys, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_scan_respects_snapshot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(1), &value(1)).unwrap();
    tx.commit().await.unwrap();

    let tx_ro = manager.begin_ro_timeout(std::time::Duration::from_secs(30));

    let mut tx2 = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx2.put(&key(1), &value(2)).unwrap();
    tx2.put(&key(2), &value(2)).unwrap();
    tx2.commit().await.unwrap();

    let rows = tx_ro.scan(&key(1), &key(3), 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0[0], 1);
    assert_eq!(rows[0].1, value(1));
}

#[tokio::test]
async fn test_scan_overlays_writes_and_deletes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wal.log");
    let manager = TxnManager::open_quorum(vec![path.clone()], 1).await.unwrap();

    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(1), &value(1)).unwrap();
    tx.put(&key(2), &value(2)).unwrap();
    tx.put(&key(3), &value(3)).unwrap();
    tx.commit().await.unwrap();

    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.delete(&key(1)).unwrap();
    tx.put(&key(2), &value(9)).unwrap();

    let rows = tx.scan(&key(1), &key(4), 2).unwrap();
    let keys: Vec<u8> = rows.iter().map(|(k, _)| k[0]).collect();
    let values: Vec<u8> = rows.iter().map(|(_, v)| v[0]).collect();

    assert_eq!(keys, vec![2, 3]);
    assert_eq!(values, vec![9, 3]);
}

#[tokio::test]
async fn test_quorum_commit_survives_one_replica_down() {
    let dir = tempdir().unwrap();
    let mut replica_dirs = Vec::new();
    for i in 0..3 {
        let path = dir.path().join(format!("replica_{i}"));
        std::fs::create_dir_all(&path).unwrap();
        replica_dirs.push(path);
    }

    let manager = TxnManager::open_quorum(replica_dirs.clone(), 2).await.unwrap();
    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(42), &value(42)).unwrap();
    tx.commit().await.unwrap();
    drop(manager);

    std::fs::remove_dir_all(&replica_dirs[0]).unwrap();

    let manager = TxnManager::open_quorum(replica_dirs, 2).await.unwrap();
    let tx = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx.get(&key(42)).unwrap(), Some(value(42)));
}

#[cfg(unix)]
#[tokio::test]
async fn test_quorum_commit_fails_if_not_enough_replicas() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let mut replica_dirs: Vec<PathBuf> = Vec::new();
    for i in 0..3 {
        let path = dir.path().join(format!("replica_{i}"));
        std::fs::create_dir_all(&path).unwrap();
        replica_dirs.push(path);
    }

    let bad_dir = &replica_dirs[0];
    let mut perms = std::fs::metadata(bad_dir).unwrap().permissions();
    perms.set_mode(0o500);
    std::fs::set_permissions(bad_dir, perms).unwrap();

    let manager = TxnManager::open_quorum(replica_dirs.clone(), 3).await.unwrap();
    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(9), &value(9)).unwrap();
    assert!(tx.commit().await.is_err());

    let mut perms = std::fs::metadata(bad_dir).unwrap().permissions();
    perms.set_mode(0o700);
    std::fs::set_permissions(bad_dir, perms).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn test_quorum_commit_succeeds_with_one_replica_error() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let mut replica_dirs: Vec<PathBuf> = Vec::new();
    for i in 0..3 {
        let path = dir.path().join(format!("replica_{i}"));
        std::fs::create_dir_all(&path).unwrap();
        replica_dirs.push(path);
    }

    let bad_dir = &replica_dirs[0];
    let mut perms = std::fs::metadata(bad_dir).unwrap().permissions();
    perms.set_mode(0o500);
    std::fs::set_permissions(bad_dir, perms).unwrap();

    let manager = TxnManager::open_quorum(replica_dirs.clone(), 2).await.unwrap();
    let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
    tx.put(&key(7), &value(7)).unwrap();
    tx.commit().await.unwrap();
    drop(manager);

    let manager = TxnManager::open_quorum(replica_dirs.clone(), 2).await.unwrap();
    let tx = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx.get(&key(7)).unwrap(), Some(value(7)));

    let mut perms = std::fs::metadata(bad_dir).unwrap().permissions();
    perms.set_mode(0o700);
    std::fs::set_permissions(bad_dir, perms).unwrap();
}

#[tokio::test]
async fn test_quorum_staggered_restart_preserves_latest_value() {
    let dir = tempdir().unwrap();
    let mut replica_dirs = Vec::new();
    for i in 0..3 {
        let path = dir.path().join(format!("replica_{i}"));
        std::fs::create_dir_all(&path).unwrap();
        replica_dirs.push(path);
    }

    // Initial write with all replicas.
    {
        let manager = TxnManager::open_quorum(replica_dirs.clone(), 2).await.unwrap();
        let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
        tx.put(&key(60), &value(1)).unwrap();
        tx.commit().await.unwrap();
    }

    // Simulate a staged restart where one replica is offline:
    // write with only two replicas reachable.
    let partial = vec![replica_dirs[0].clone(), replica_dirs[1].clone()];
    {
        let manager = TxnManager::open_quorum(partial, 2).await.unwrap();
        let mut tx = manager.begin_rw_timeout(std::time::Duration::from_secs(30));
        tx.put(&key(60), &value(2)).unwrap();
        tx.commit().await.unwrap();
    }

    // Rejoin full set and ensure latest value is still selected.
    let manager = TxnManager::open_quorum(replica_dirs, 2).await.unwrap();
    let tx = manager.begin_ro_timeout(std::time::Duration::from_secs(30));
    assert_eq!(tx.get(&key(60)).unwrap(), Some(value(2)));
}
