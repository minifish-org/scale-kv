// Copyright (c) 2024 scale-kv developers
// SPDX-License-Identifier: Apache-2.0

//! Common types shared between storage and compute nodes.

/// A value in the KV store.
pub type Value = Vec<u8>;

/// Operation type for requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum Op {
    Get,
    Put,
    Delete,
}

/// A request from compute node to storage node.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct Request {
    pub op: Op,
    pub key: String,
    pub value: Option<Value>,
}

/// A response from storage node to compute node.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct Response {
    pub found: bool,
    pub value: Option<Value>,
}

/// Result type for operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for scale-kv.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key not found")]
    NotFound,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("lock error: {0}")]
    Lock(String),
}
