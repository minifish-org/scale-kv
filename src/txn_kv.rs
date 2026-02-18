use crate::Value;
use crate::KEY_SIZE;

pub type Key = [u8; KEY_SIZE];
pub use error::{Result, TxnError, TxnErrorCategory};

#[path = "txn_kv/error.rs"]
mod error;
#[path = "txn_kv/codec.rs"]
mod codec;
#[path = "txn_kv/storage.rs"]
mod storage;
#[path = "txn_kv/manager.rs"]
mod manager;
pub use storage::TxnStorage;
pub use manager::{Txn, TxnManager};

#[derive(Debug, Clone)]
pub struct Version {
    commit_ts: u64,
    value: Option<Value>,
}

#[cfg(test)]
#[path = "txn_kv_tests.rs"]
mod tests;
