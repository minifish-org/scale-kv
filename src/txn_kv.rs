use crate::KEY_SIZE;
use crate::Value;

pub type Key = [u8; KEY_SIZE];
pub use error::{Result, TxnError, TxnErrorCategory};

#[path = "txn_kv/codec.rs"]
mod codec;
#[path = "txn_kv/error.rs"]
mod error;
#[path = "txn_kv/manager.rs"]
mod manager;
#[path = "txn_kv/storage.rs"]
mod storage;
pub use manager::{Txn, TxnManager};
pub use storage::TxnStorage;

#[derive(Debug, Clone)]
pub struct Version {
    commit_ts: u64,
    value: Option<Value>,
}

#[cfg(test)]
#[path = "txn_kv_tests.rs"]
mod tests;
