// Copyright (c) 2024 scale-kv developers
// SPDX-License-Identifier: Apache-2.0

//! scale-kv: A simple distributed KV store with compute-cache separation

pub mod common;
pub use common::*;

mod page_bptree;

pub mod node;
pub use node::StorageNode;

pub mod client;
pub use client::{BatchSender, BatchedComputeNode, BatchedStorageClientPool, ComputeNode, StorageClient};

pub use page_bptree::{PageBPlusTree, SlotRef as PageSlotRef};

pub mod server;
pub use server::StorageServer;

pub mod storage_capnp;
