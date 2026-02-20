// Copyright (c) 2024 scale-kv developers
// SPDX-License-Identifier: Apache-2.0

//! scale-kv: A simple distributed KV store with compute-cache separation

pub mod common;
pub use common::*;

pub mod page_bptree;
pub use page_bptree::{
    AsyncPageProvider, DEFAULT_PAGE_CACHE_SHARDS, InMemoryPageProvider, LeafValue, LeafValueMeta,
    LeafValueRef, PageBPlusTree, PageCache, SharedPageProvider,
};

pub mod active_reads;
pub use active_reads::{ActiveReadInfo, ActiveReads, ReadGuard};

#[cfg(not(target_arch = "wasm32"))]
pub mod page_store;
#[cfg(not(target_arch = "wasm32"))]
pub use page_store::{BufferPoolConfig, BufferStats, CheckpointConfig, PageStore};

pub mod meta_page;
pub use meta_page::{META_PAGE_ID, MetaPage};

pub mod btree_meta;
pub use btree_meta::{BTREE_META_PAGE_ID, BtreeMeta};

pub mod slotted_page;

pub mod undo_pg;
pub use undo_pg::{UNDO_RECORD_SIZE, UndoPtr};

pub mod txn_page_provider;
pub use txn_page_provider::TxnPageProvider;

#[cfg(not(target_arch = "wasm32"))]
pub mod node;
#[cfg(not(target_arch = "wasm32"))]
pub use node::{MvccReadHandle, StorageMaintenanceConfig, StorageMetricsSnapshot, StorageNode};

pub mod client;
pub use client::StorageClient;

pub mod http_protocol;

#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::StorageServer;

pub mod quorum_client;
pub use quorum_client::StorageQuorumClient;

pub mod secondary_index;
pub use secondary_index::{SecondaryIndexDefinition, SecondaryIndexKind};
pub mod secondary_index_meta;
pub use secondary_index_meta::SECONDARY_INDEX_META_PAGE_ID;
pub mod secondary_posting_log;

pub mod compute_sequencer;
pub use compute_sequencer::ComputeSequencer;

pub mod embedded_compute;
pub(crate) mod embedded_compute_runtime;
pub use embedded_compute::{EmbeddedCompute, EmbeddedTxn};

#[cfg(target_arch = "wasm32")]
pub mod wasm_compute;
#[cfg(target_arch = "wasm32")]
pub use wasm_compute::WasmComputeClient;

#[cfg(not(target_arch = "wasm32"))]
pub mod txn_kv;
#[cfg(not(target_arch = "wasm32"))]
pub use txn_kv::{Txn, TxnError, TxnErrorCategory, TxnManager};
