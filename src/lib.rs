// Copyright (c) 2024 scale-kv developers
// SPDX-License-Identifier: Apache-2.0

//! scale-kv: A simple distributed KV store with compute-cache separation

pub mod common;
pub use common::*;

pub mod page_bptree;
pub use page_bptree::{
    AsyncPageProvider, DEFAULT_PAGE_CACHE_SHARDS, InMemoryPageProvider, PageBPlusTree, PageCache,
    SharedPageProvider, SlotRef as PageSlotRef,
};

pub mod active_reads;
pub use active_reads::{ActiveReadInfo, ActiveReads, ReadGuard};

pub mod page_store;
pub use page_store::{BufferPoolConfig, BufferStats, CheckpointConfig, PageStore};

pub mod meta_page;
pub use meta_page::{META_PAGE_ID, MetaPage};

pub mod btree_meta;
pub use btree_meta::{BTREE_META_PAGE_ID, BtreeMeta};

pub mod slotted_page;

pub mod undo_pg;
pub use undo_pg::{UNDO_RECORD_SIZE, UndoPtr};

pub mod fsm_pg;

pub mod txn_page_provider;
pub use txn_page_provider::TxnPageProvider;

pub mod node;
pub use node::{MvccReadHandle, StorageMaintenanceConfig, StorageNode};

pub mod client;
pub use client::StorageClient;

pub mod server;
pub use server::StorageServer;

pub mod storage_capnp;

pub mod quorum_client;
pub use quorum_client::StorageQuorumClient;

pub mod compute_sequencer;
pub use compute_sequencer::ComputeSequencer;

pub mod embedded_compute;
pub use embedded_compute::{EmbeddedCompute, EmbeddedTxn};

pub mod txn_kv;
pub use txn_kv::{Txn, TxnError, TxnManager};
