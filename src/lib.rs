// Copyright (c) 2024 scale-kv developers
// SPDX-License-Identifier: Apache-2.0

//! scale-kv: A simple distributed KV store with compute-cache separation

pub mod common;
pub use common::*;

pub mod page_bptree;
pub use page_bptree::{
    InMemoryPageProvider,
    PageBPlusTree,
    PageCache,
    PageProvider,
    SharedPageProvider,
    SlotRef as PageSlotRef,
    DEFAULT_PAGE_CACHE_SHARDS,
};

pub mod page_store;
pub use page_store::{BufferPoolConfig, BufferStats, CheckpointConfig, PageStore};

pub mod node;
pub use node::StorageNode;

pub mod client;
pub use client::{BatchSender, BatchedComputeNode, BatchedStorageClientPool, ComputeNode, StorageClient};

pub mod server;
pub use server::StorageServer;

pub mod storage_capnp;

pub mod quorum_client;
pub use quorum_client::StorageQuorumClient;

pub mod txn_kv;
pub use txn_kv::{Txn, TxnError, TxnManager};
