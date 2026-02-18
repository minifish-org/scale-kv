# Distributed KV Store - Architecture Design Document

**Version**: 2026-02-03 (Refactored)
**Status**: Current Implementation
**Last Updated**: Based on commit `b4c5d85` (async I/O refactor)

## 🎯 Implementation Status Summary

### ✅ **COMPLETED & TESTED**
- **Core Architecture**: Compute-store separation with async I/O
- **Storage Layer**: Segment-Page (PostgreSQL-style) with WAL
- **Indexing**: B+Tree with slotted pages and leaf linked lists
- **Networking**: Cap'n Proto RPC with sliding window batching
- **Crash Recovery**: WAL replay with checkpointing
- **Testing**: Unit, integration, and benchmark tests

### ✅ **RECENTLY COMPLETED OPTIMIZATIONS**
- **Sliding Window Batching**: ✅ Enhanced with adaptive window sizing and congestion control
- **Runtime Compatibility**: ✅ Fixed `spawn_local` usage in multi-thread runtime

### 🔴 **NOT IMPLEMENTED / FUTURE WORK**
- **Distributed Transactions**: Multi-key atomic operations
- **Automatic Load Balancing**: Dynamic data redistribution
- **Advanced Monitoring**: Performance metrics and alerts

### 📊 **Test Coverage**: All 54 tests passing (44 unit + 10 integration)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture Design](#2-architecture-design)
   - 2.1 [Compute-Store Separation](#21-compute-store-separation)
   - 2.2 [Current Implementation Status](#22-current-implementation-status)
3. [Compute Node Design](#3-compute-node-design)
   - 3.1 [B+Tree Index](#31-bptree-index)
   - 3.2 [Slotted Page Format](#32-slotted-page-format)
   - 3.3 [Memory Management](#33-memory-management)
4. [Storage Node Design](#4-storage-node-design)
   - 4.1 [Segment-Page Storage](#41-segment-page-storage)
   - 4.2 [Write-Ahead Log (WAL)](#42-write-ahead-log-wal)
   - 4.3 [Crash Recovery](#43-crash-recovery)
5. [Network Communication](#5-network-communication)
   - 5.1 [RPC Interface](#51-rpc-interface)
   - 5.2 [Sliding Window Batching](#52-sliding-window-batching)
   - 5.3 [Performance Considerations](#53-performance-considerations)
6. [Testing Strategy](#6-testing-strategy)
   - 6.1 [Unit Tests](#61-unit-tests)
   - 6.2 [Integration Tests](#62-integration-tests)
   - 6.3 [Benchmarks](#63-benchmarks)
7. [Code Review Findings](#7-code-review-findings)
   - 7.1 [Fixed Issues](#71-fixed-issues)
   - 7.2 [Open Issues](#72-open-issues)
8. [Development History](#8-development-history)
   - 8.1 [Architecture Evolution](#81-architecture-evolution)
   - 8.2 [Key Design Decisions](#82-key-design-decisions)

---

## 1. Overview

Scale-KV is a distributed key-value store with compute-store separation architecture, written in Rust. The system separates compute (indexing, caching) from storage (persistence, durability) to enable horizontal scaling.

### Demo Scope & Target Model (Current Phase)
- **目标架构**：Aurora-style KV（计算/存储两个进程），计算层只通过异步 WAL 同步到存储层。
- **当前阶段**：单计算 + 单存储 demo，不考虑多写/多存储协调。
- **一致性/事务**：不考虑数据丢失与事务；计算层写入产生 WAL，**攒满 256KB 才发送**。
- **缓存策略**：不做计算层淘汰，**所有数据常驻内存 cache**。
- **读写路径**：计算层内存完成读写，读走 B+Tree 索引并回表读 page。
- **Schema（固定长度）**：Key/Value 均为**预先约定的定长**（当前 `KEY_SIZE=16`, `VALUE_SIZE=1024`），不支持变长类型；长度不匹配直接拒绝。

### Key Characteristics
- **Compute-Store Separation**: Compute nodes handle indexing and caching, storage nodes handle persistence
- **Page-Oriented Storage**: Fixed 16KB pages with PostgreSQL-style segment-page storage
- **Async I/O**: Built on Tokio for high-concurrency operations
- **WAL-Based Durability**: Write-ahead logging for crash recovery
- **B+Tree Indexing**: In-memory B+tree with page-oriented storage

---

## 2. Architecture Design

### 2.1 Compute-Store Separation

```
┌──────────────────────────────────────────────────────────────┐
│  Compute Node                                                │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  B+Tree Index (in-memory)                              │  │
│  │  Page Cache (HashMap<PageId, Page>)                    │  │
│  │  Slotted Page Format                                   │  │
│  └────────────────────────────────────────────────────────┘  │
│                         │ WAL batches (KV redo)               │
│                         ▼                                     │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│  Storage Node                                                │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  PageStore (Segment-Page)                              │  │
│  │  WAL Persistence & Replay                              │  │
│  │  Async Checkpointing                                   │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 Current Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| **Compute Node** | ✅ Implemented | B+tree, page cache, slotted pages |
| **Storage Node** | ✅ Implemented | Segment-Page storage with WAL |
| **Network RPC** | ✅ Implemented | Cap'n Proto with async I/O |
| **Crash Recovery** | ✅ Implemented | WAL replay with checkpointing |
| **Async I/O** | ✅ Implemented | Tokio-based file operations |

---

## 3. Compute Node Design

### 3.1 B+Tree Index

**Location**: `src/page_bptree.rs`

```rust
// Current implementation features:
// - Page-based B+Tree stored in PageCache via PageProvider
// - Root/leaf/internal nodes are pages in memory (not persisted to storage)
// - Concurrent reads with RwLock around the tree
// - Serial writes for consistency
```

**Key Structures**:
```rust
pub struct PageBPlusTree<P: PageProvider> {
    provider: P, // SharedPageProvider -> PageCache
    len: usize,
}

pub struct SharedPageProvider {
    pages: Arc<PageCache>,
    // root page id is tracked here
}
```

### 3.2 Slotted Page Format

**Location**: `src/page_bptree.rs`

Each page (16KB) can hold multiple key-value pairs using slotted page format:

```
┌─────────────────────────────────────────────────────────────┐
│ Page Header (6 bytes)                                        │
│ ├─ free_start: u16                                           │
│ ├─ free_end: u16                                             │
│ └─ slot_count: u16                                           │
├─────────────────────────────────────────────────────────────┤
│ Slot Directory (4 bytes per slot)                           │
│ ├─ offset: u16                                               │
│ └─ length: u16                                               │
├─────────────────────────────────────────────────────────────┤
│ Free Space                                                  │
├─────────────────────────────────────────────────────────────┤
│ Data Records (variable length)                              │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 Memory Management

**Page Cache**: `HashMap<PageId, Page>` with RwLock synchronization
**Free Space Management**: Bucketed free lists for page allocation
**Defragmentation**: Automatic when in-page fragmentation exceeds threshold

### 3.4 Read & Write Flow (Current)

**Read (detailed, ASCII)**:
```
READ (detailed: data page + cache + locks)

[Client get(key)]
        |
        v
+----------------------+
| ComputeNode.get()    |
+----------------------+
        |
        v
+----------------------+
| validate_key_len     |
+----------------------+
        |
        v
+------------------------------+
| tree.read() (RwLock)         |
+------------------------------+
        |
        v
+------------------------------+
| PageBPlusTree.get(key)       |
+------------------------------+
        |
        v
+------------------------------+
| provider.root_page_id()      |
+------------------------------+
        |
        v
+-----------------------------------------+
| provider.read_page(root_id)             |
| -> PageCache.get(root_id)               |
|    -> shard = page_id % shards          |
|    -> shard.read() (RwLock)             |
+-----------------------------------------+
        |
        v
+-----------------------------------------+
| B+Tree page: binary search on keys      |
| -> pick child_id / leaf slot            |
+-----------------------------------------+
        |
        v
+-----------------------------------------+
| provider.read_page(child_id) ... repeat |
+-----------------------------------------+
        |
        v
+------------------------+        +----------------------+
| (page_id, slot_id)     |------->| PageCache.get(page)  |
+------------------------+        | -> shard.read() lock |
                                  +----------------------+
                                           |
                                           v
                              +---------------------------+
                              | read_value(page, slot)    |
                              | - slot directory lookup   |
                              | - fixed-length decode     |
                              |   (KEY_SIZE/VALUE_SIZE)   |
                              +---------------------------+
                                           |
                                           v
                                      [Return value]
```

**Write (detailed, ASCII)**:
```
WRITE (detailed: data page + cache + locks + WAL)

[Client put(key, value)]
        |
        v
+----------------------+
| ComputeNode.put()    |
+----------------------+
        |
        v
+----------------------+
| validate_key_len     |
| payload_len check    |
+----------------------+
        |
        v
+------------------------------+
| tree.read() (RwLock)         |
| lookup old slot (if exists)  |
+------------------------------+
        |
        v
+------------------------------+
| PageCache.get(old_page)      |
| -> shard.read() lock         |
+------------------------------+
        |
        v
+------------------------------+
| clear_slot + update FSM      |
+------------------------------+
        |
        v
+------------------------------+
| FSM.allocate(required)       |
+------------------------------+
        |
        v
+------------------------------+
| PageCache.get(page_id)       |
| -> shard.read() lock         |
+------------------------------+
        |
        v
+------------------------------+
| insert_record(page)          |
| - fixed-length record write  |
| - slot directory update      |
+------------------------------+
        |
        v
+------------------------------+
| PageCache.insert(page_id)    |
| -> shard.write() lock        |
+------------------------------+
        |
        v
+------------------------------+
| tree.write() (RwLock)        |
| update B+Tree pages in cache |
+------------------------------+
        |
        v
     [Return OK]
        |
        | (async WAL)
        v
+------------------------------+
| WalSender.enqueue(record)    |
| pending queue (Mutex)        |
+------------------------------+
        |
        v
+------------------------------+
| background batching to 256KB |
+------------------------------+
        |
        v
+------------------------------+
| append_wal(batch) RPC        |
+------------------------------+
        |
        v
  [ack -> notify waiters]
```

---

## 4. Storage Node Design

### 4.1 Segment-Page Storage

**Location**: `src/page_store.rs`, `src/node.rs`

**Current Implementation**: PostgreSQL-style segment-page storage (replaces earlier Bitcask design)

**Key Components**:
1. **PageStore**: Manages buffer pool and page file
2. **Buffer Pool**: Caches dirty pages in memory
3. **Page File**: Fixed-offset file (`page_id × 16KB`)
4. **Checkpointing**: Periodic flush of dirty pages to disk

**Data Structures**:
```rust
pub struct PageStore {
    dir: PathBuf,
    buffer_pool: RwLock<HashMap<PageId, BufferPage>>,
    page_file: Mutex<PageFile>,
    checkpoint_lsn: AtomicU64,
    // ... other fields
}

struct BufferPage {
    data: Box<[u8; PAGE_SIZE]>,
    dirty: bool,
    lsn: u64,
    last_accessed: Instant,
}
```

### 4.2 Write-Ahead Log (WAL)

**Location**: `src/node.rs`

**WAL Record Format**:
```
lsn (8) | op (1) | page_id (8) | slot_id (2) | key_len (4) | val_len (4) | key | value
```

**WAL Flow**:
1. Compute node appends to in-memory WAL buffer
2. Background thread batches and sends to storage
3. Storage persists WAL to disk segments
4. Background replay applies WAL to page store
5. Checkpoint flushes dirty pages and truncates WAL

### 4.3 Crash Recovery

**Recovery Process**:
1. Read `checkpoint_state` for last persisted LSN
2. Replay WAL records with `lsn > checkpoint_lsn`
3. Apply records to page store
4. Start normal operation

**File Layout**:
```
data/
├── pages.dat           # Page file (fixed offset)
├── checkpoint_state    # Last checkpoint LSN
├── wal-*.log           # WAL segments
└── wal_state           # WAL replay state
```

---

## 5. Network Communication

### 5.1 RPC Interface

**Protocol**: Cap'n Proto
**Location**: `src/storage_capnp.rs`

**Key RPC Methods**:
- `get(page_id) -> Page`
- `appendWal(batch) -> Ack`
- `checkpoint() -> Result`

### 5.2 Sliding Window Batching

**Implemented**: Yes (in `src/client.rs`)

**Design**: Instead of waiting for full batches, use pipelining with sliding window:
- Window size: 16-64 concurrent requests
- Send when window has space
- Average latency ≈ 1 RTT

**Pseudo-code**:
```rust
async fn send_with_window(&self, req: Request) {
    let current = self.in_flight.fetch_add(1, Ordering::AcqRel);
    if current < self.max_in_flight {
        self.send_direct(req).await;
    } else {
        self.pending.lock().unwrap().push(req);
    }
}
```

### 5.3 Performance Considerations

**Network Overhead Findings**:
1. Single RPC write cost is high (network >> local access)
2. Large batches significantly reduce per-operation cost
3. Reads should target local cache hits
4. Writes should use batch interfaces (16KB+)

**Optimizations**:
- Async I/O throughout (Tokio)
- Batch aggregation in WAL replay
- Buffer pooling in page store
- Concurrent operations where possible

---

## 6. Testing Strategy

### 6.1 Unit Tests ✅ **COMPLETED**

**Location**: Inline in source files (`src/*.rs`)

**Coverage**:
- ✅ B+Tree operations (insert, delete, search, range scans)
- ✅ Page store operations (read, write, checkpoint, eviction)
- ✅ WAL record handling and replay
- ✅ Network client/server interactions

**Status**: All unit tests passing (44 tests)

### 6.2 Integration Tests ✅ **COMPLETED**

**Location**: `tests/` directory

| Test File | Purpose | Status |
|-----------|---------|--------|
| `compute.rs` | Compute node functionality | ✅ **Passing** |
| `network_rpc.rs` | RPC communication tests | ✅ **Passing** |
| `network.rs` | Network layer tests | ✅ **Passing** |
| `storage_wal.rs` | Storage with WAL tests | ✅ **Passing** |
| `smoke.rs` | Quick verification tests | ✅ **Passing** |

**Status**: All integration tests passing (10 tests)

**Running Tests**:
```bash
# All tests
cargo test  # ✅ 54 tests passing

# Specific test categories
cargo test --test compute
cargo test --test storage_wal
cargo test --test smoke
```

### 6.3 Benchmarks ✅ **COMPLETED**

The legacy Criterion YCSB benchmark (`benches/ycsb.rs`) was removed because it diverged from the
current API surface and no longer reflected the actively maintained performance path.

**Current benchmark entrypoints**:
- `src/bin/workload_bench.rs` (workload-oriented benchmark runner)
- `src/bin/compare_bench.rs` (backend comparison runner)
- `src/bin/simple_bench.rs` (single-run sanity benchmark)

**Running Benchmarks**:
```bash
cargo run --release --bin workload_bench -- --help
cargo run --release --bin compare_bench -- --help
cargo run --release --bin simple_bench -- --help
```

---

## 7. Code Review Findings

### 7.1 Fixed Issues

| Issue | Status | Fix Commit | Notes |
|-------|--------|------------|-------|
| **Synchronous file I/O** | ✅ Fixed | `b4c5d85` | Converted to async Tokio I/O |
| **MAX_KEYS too small** | ✅ Fixed | `ddfa889` | Increased from 8 to 256 |
| **B+Tree missing leaf linked list** | ✅ Fixed | `04980c5` | Added sibling pointers |
| **Async I/O within lock** | ✅ Fixed | `287933b` | Release lock before async ops |

### 7.2 Recently Fixed Issues

| Issue | Location | Severity | Status | Notes |
|-------|----------|----------|--------|-------|
| **spawn_local compatibility** | `server.rs:196` | 🟡 Medium | ✅ **Fixed** | Replaced `spawn_local` with `spawn` for multi-thread runtime compatibility |
| **Sliding Window Optimization** | `src/client.rs` | 🟡 Medium | ✅ **Enhanced** | Added adaptive window sizing with congestion control |
| **Synchronous Cap'n RPC use** | `client.rs:287` | 🟡 Medium | 🟡 **Partially Fixed** | Has sliding window but still synchronous in some paths |

### 7.3 Remaining Open Issues

| Issue | Location | Severity | Status | Notes |
|-------|----------|----------|--------|-------|
| **Batch operations not concurrent** | `client.rs:387` | 🟡 Medium | 🔴 **Not Fixed** | Could use `join_all` for better parallelism |
| **ComputeNode constructors require LocalSet** | `client.rs` | 🟢 Low | 🔴 **Not Fixed** | Backward compatibility concern, not critical |

---

## 8. Development History

### 8.1 Architecture Evolution

**Phase 1: Initial Implementation**
- Basic compute-store separation
- Simple in-memory storage
- Synchronous I/O

**Phase 2: WAL and Batching**
- Write-ahead logging for durability
- Batch operations for network efficiency
- Sliding window batching

**Phase 3: Storage Refactoring**
- **Replaced Bitcask with Segment-Page storage**
- PostgreSQL-style fixed-offset pages
- Buffer pool with checkpointing

**Phase 4: Async Optimization**
- Converted all I/O to async (Tokio)
- Improved concurrency
- Better resource utilization

### 8.2 Key Design Decisions

#### ✅ **Implemented Decisions**

1. **Segment-Page over Bitcask** ✅ **COMPLETED**
   - **Status**: Fully implemented and tested
   - **Reason**: Bitcask caused space amplification for page updates
   - **Benefit**: 1:1 storage ratio, no compaction needed
   - **Implementation**: `src/page_store.rs`, `src/node.rs`

2. **Slotted Page Format** ✅ **COMPLETED**
   - **Status**: Fully implemented with defragmentation
   - **Reason**: Efficient storage of multiple KVs per page
   - **Benefit**: Better space utilization, fewer page allocations
   - **Implementation**: `src/page_bptree.rs`

3. **Async I/O Throughout** ✅ **COMPLETED**
   - **Status**: Fully converted from synchronous I/O
   - **Reason**: High concurrency requirements
   - **Benefit**: Better resource utilization, higher throughput
   - **Implementation**: Tokio-based I/O in all modules

4. **WAL with Early ACK** ✅ **COMPLETED**
   - **Status**: Implemented with crash recovery
   - **Reason**: Low-latency write acknowledgment
   - **Benefit**: Fast client response times
   - **Implementation**: `src/node.rs` WAL system

#### ✅ **Recently Completed Optimizations**

5. **Sliding Window Batching** ✅ **ENHANCED & OPTIMIZED**
   - **Status**: Enhanced with adaptive window sizing and congestion control
   - **Features**: 
     - Dynamic window adjustment based on latency and success rate
     - Exponential moving average for latency tracking
     - Congestion control with backoff on failures
     - Minimum window size protection
   - **Implementation**: `src/client.rs` - `BatchSender` with `WindowStats`

#### 🔴 **Not Yet Implemented / Future Work**

6. **Distributed Transactions** 🔴 **NOT IMPLEMENTED**
   - **Status**: Future enhancement
   - **Scope**: Multi-key atomic operations across nodes
   - **Complexity**: High - requires consensus protocol

7. **Automatic Load Balancing** 🔴 **NOT IMPLEMENTED**
   - **Status**: Future enhancement
   - **Scope**: Dynamic redistribution of data across storage nodes
   - **Complexity**: Medium - requires monitoring and migration logic

---

## Appendix A: Module Reference

**Actual Module Structure**:
```rust
// Correct imports (different from old documentation)
use crate::node::StorageNode;      // NOT storage::StorageNode
use crate::client::ComputeNode;    // NOT compute::ComputeNode
use crate::page_store::PageStore;
use crate::server::StorageServer;
```

**Source Files**:
```
src/
├── bptree.rs          # ✅ B+Tree implementation (complete)
├── client.rs          # ✅ Compute node and network client (complete)
├── common.rs          # ✅ Common types and constants (complete)
├── lib.rs             # ✅ Module exports (complete)
├── main.rs            # ✅ Entry point (minimal, complete)
├── node.rs            # ✅ Storage node with WAL (complete)
├── page_bptree.rs     # ✅ Slotted page B+Tree (complete)
├── page_store.rs      # ✅ Segment-page storage (complete)
├── server.rs          # ✅ RPC server (complete, minor issues)
└── storage_capnp.rs   # ✅ Cap'n Proto definitions (complete)
```

**Test Files**:
```
tests/
├── compute.rs         # Compute node tests
├── network_rpc.rs     # RPC tests
├── network.rs         # Network layer tests
├── smoke.rs           # Smoke tests
└── storage_wal.rs     # Storage with WAL tests
```

---

## Appendix B: Configuration

**Page Size**: 16KB (`PAGE_SIZE = 16384`)
**WAL Segment Size**: 64MB (8KB for tests)
**Buffer Pool Size**: Configurable, default 10,000 pages
**Checkpoint Threshold**: 1000 dirty pages or 64MB or 60 seconds
**Sliding Window Size**: 16-64 concurrent requests

---

*Document last updated: 2026-02-03*
*Based on code state at commit: b4c5d85 (refactor: convert storage layer to async tokio I/O)*
*Replaces previous documentation with corrected structure and current implementation details*
