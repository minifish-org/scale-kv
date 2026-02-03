# Distributed KV Store - TDD Design Document

## 1. TDD Process Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        TDD Cycle                                │
│                                                                 │
│   1. RED    → Write a failing test                             │
│   2. GREEN  → Write minimal code to make the test pass         │
│   3. REFACTOR → Refactor code                                  │
│   4. REPEAT → Next test                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 2. Test Pyramid

```
                    YCSB Benchmark
                   /              \
            Integration Tests      Smoke Tests
           /        \            /     \
    Component Interaction  Network Communication  Quick Verification  Core Path
```

## 3. Test Layering

| Level | File | Purpose | Running Frequency |
|------|------|------|----------|
| **Unit Tests** | `src/**/*.rs` | Core logic | Every commit |
| **Integration Tests** | `tests/**/*.rs` | Component interaction | Every commit |
| **YCSB Benchmark** | `benches/**/*.rs` | Performance verification | Periodic |
| **Smoke Tests** | `tests/smoke.rs` | Quick verification | Every PR |

---

## 4. Network Overhead and Usage Guidelines

Based on the current architecture and benchmarks, the following conclusions are clear:

1. **Network overhead is much higher than local access**, making single RPC write costs extremely high.
2. **Large-granularity batch operations significantly reduce the cost of slow media**, which applies to both network and disk.
3. **Reads should aim for local cache hits, and writes should use large-batch interfaces** (e.g., 16K batch). This is the correct way to use the system.

---

## 5. Storage and Index Selection (Current Decision)

### 5.1 Compute Node (Single Instance)
- Selection: **Self-developed B+tree + Whole-tree RwLock (Concurrent reads, serial writes)**
- Page Management: **HashMap + RwLock**
- Free Space Map: **Vec<VecDeque<PageId>> Buckets** (Accessed by a single writer thread)
- Status: **Completed** (B+tree entry, page cache, FSM, Slotted Page + defrag)

#### 5.1.1 Compute Layer Page Model (Decision)
- KV to Page mapping: **One Page can hold multiple KVs** (Slotted Page)
- B+tree: **key -> (page_id, slot_id)**
- Value: **Written to page payload (key/value record)**
- Note: In-page fragmentation triggers defrag when write fails and space is sufficient.

### 5.2 Storage Node (Batch Write Focus)
- Selection: **Bitcask-style** (Multi-file append-only)
- Memory Index: **HashMap**, stores `key -> (file_id, offset, len, checksum)`
- Note: Background compaction is executed in segments to avoid blocking foreground writes.
- Status: **Completed** (Append-only segment files, memory index, segment rotation, compaction, manifest + fsync, auto-trigger policy)

### 5.3 Remote WAL Batching (Design Draft)
- Goal: **Replace whole-page writes with WAL batches** to reduce RPC overhead.
- Batch size: **Fixed 256KB** (no timeout flush).
- Sending strategy: **Foreground is non-blocking**, WAL only enters local queue; background asynchronously batches sends and waits for ACK.
- Trigger condition: Background thread only sends when **buffer reaches 256KB** (no timeout flush).
- RPC: **New appendWal(batch)** (Dedicated WAL append interface).
- Storage: **WAL sequential disk persistence + segment (A+B: sequential append + segment rotation)**.
- ACK Semantics: **Storage node ACKs immediately after receiving the WAL batch and successfully enqueuing it** (does not wait for disk/replay).
- Replay: **Background asynchronous replay WAL → page → Bitcask** (Aggregated by page_id, 8KB threshold).
- Read: **Read from Compute Node only** (Storage Node is only for persistence and compaction).
- Crash Recovery: **Scan WAL segments at startup, continue replay from wal_state.last_applied_lsn**.

#### 5.3.1 WAL Minimum Record Format (KV redo)
- Record Granularity: KV level
- Fields: `lsn | op | page_id | slot_id | key_len | val_len | key | value`
- Note: `txn_id/checksum` are optional.

#### 5.3.2 Write Flow (Non-blocking foreground)
```mermaid
flowchart TD
    A[Compute put/delete] --> B[append to in-memory WAL buffer]
    B --> C{buffer size < 256KB?}
    C -->|yes| D[return immediately]
    C -->|no| E[enqueue WAL batch]
    E --> F[appendWal RPC (256KB)]
    F --> G[Storage enqueue]
    G --> H[ack]
```

#### 5.3.3 Storage Semantics (Update: Persistent WAL + Early ACK)
- `appendWal` success = **Enqueued (ACK on receipt)**, does not represent durability.
- WAL **Sequential append + segment** (A+B scheme).
- WAL **Background replay after disk persistence**, does not block write throughput.
- Compaction keeps the latest value, cleans up old versions.

#### 5.3.4 KV + page/slot redo (Current Choice: KV redo)
- Network Transmission: **User KV + page_id + slot_id + lsn**
- Storage: **Replay to page** by lsn, then write to Bitcask in page units.
- Effect: Network sends only increments, but Storage still maintains pages consistent with Compute Node.

Suggested minimum record format:
```
record {
  lsn: u64
  op: PUT | DEL
  page_id: u64
  slot_id: u16
  key_len: u32
  val_len: u32
  key: bytes
  value: bytes
}
```

Key Constraints:
- **page_id/slot_id generated by Compute single-writer** (must be globally unique/ordered).
- **LSN generated by Compute single-writer** (strictly increasing).
- Storage **replays in lsn order**, requires idempotent processing (apply only if `lsn > last_applied`).

#### 5.3.5 WAL Persistence and Replay (A+B Scheme)
- **WAL Persistence**: Sequential append + segment (e.g., 64MB) on Storage side.
- **ACK Semantics**: Storage node ACKs immediately after receiving WAL batch and successfully enqueuing.
- **Replay Thread**: Background reads WAL, replays updates to memory page in lsn order, writes to Bitcask after aggregating by `page_id`.
- **Aggregation Threshold**: Triggered by **8KB** per page (no time-based trigger).
- **LSN**: Strictly increasing, apply only if `lsn > last_applied`.
- **wal_state**: Persist `last_applied_lsn` after replay completion.

### 5.6 Sliding Window Batching Scheme (2026-02-01)
- **Goal**: Reduce average latency, avoid the "waiting for batch" issue of Group Commit.
- **Core Insight**: Instead of "waiting until full to send", use "pipelining".

**Principle Comparison**:

| Scheme | Sending Timing | Average Latency |
|------|---------|---------|
| Traditional Group Commit | Send only when 256KB full | May wait for tens of ms |
| Sliding Window | Send whenever window has space | ≈ 1 RTT |

**How Sliding Window Works**:
```
Control parameter: Window size N (e.g., 16 or 32)

Timeline example (window=4):
T1: Send Req1 Req2 Req3 Req4 (Window full)
T2: Req1 completes → Send Req5
T3: Req2 completes → Send Req6
T4: Req3 completes → Send Req7
...
```

**Pseudo-code**:
```rust
struct BatchSender {
    in_flight: Arc<AtomicUsize>,
    max_in_flight: usize,
    pending: Mutex<Vec<Request>>,
    sender: Sender<Request>,
}

impl BatchSender {
    async fn send(&self, req: Request) {
        let current = self.in_flight.fetch_add(1, Ordering::AcqRel);

        if current < self.max_in_flight {
            // Window has space, send directly
            self.send_direct(req).await;
        } else {
            // Window is full, add to pending queue
            let mut pending = self.pending.lock().unwrap();
            pending.push(req);
        }
    }

    async fn on_complete(&self) {
        self.in_flight.fetch_sub(1, Ordering::Release);

        // Check if there are pending requests
        let req = {
            let mut pending = self.pending.lock().unwrap();
            pending.pop()
        };
        if let Some(req) = req {
            self.send_direct(req).await;
        }
    }
}
```

**Cap'n RPC Integration**:
- Cap'n RPC supports Promises/Pipelining.
- Requires application-layer window control.
- Batch size can be dynamically adjusted.

**Tuning Parameters**:
- Window size: Between 16-64, adjusted based on RTT.
- Best Practice: Window × Single Request Size ≈ Data volume that can be sent in 1-2 RTTs.

**Expected Results**:
```
Assumption: RTT = 0.5ms, Single RPC overhead = 0.1ms

Sliding Window (window=16):
  - Average latency ≈ 1 RTT = 0.5ms
  - Throughput = Window / RTT = 16 / 0.5ms = 32K QPS
```

### 5.7 Segment-Page Storage Refactoring (PostgreSQL Style)

#### 5.7.1 Problem Analysis: Bitcask Unsuitable for Page Storage

Current Storage side uses Bitcask-style storage for pages:

```
Current Architecture:
┌──────────────────────────────────────────────────────────────┐
│  Compute                                                      │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, Page>  (In-place update)              │  │
│  │  B+tree index                                           │  │
│  └────────────────────────────────────────────────────────┘  │
│                         │ WAL batch (KV redo)                 │
│                         ▼                                     │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  Storage (Bitcask)                                            │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, IndexEntry>  (Memory Index)             │  │
│  │  segment-*.log (append-only)                            │  │
│  │  Each page update → Append full 16KB                    │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**Issues**:

| Problem | Cause | Impact |
|------|------|------|
| **Space Amplification** | Appending full page on every update | 1 page updated 10 times = 160KB disk usage |
| **Compaction Pressure** | Stale entries accumulate | Background compaction blocks/jitters |
| **Architectural Asymmetry** | Compute uses in-place updates, Storage appends | High complexity, hard to understand |
| **Recovery Scans** | Scans all segments to rebuild index at startup | Slow startup for large data volumes |

**Bitcask Suitable Scenarios**:
- Small values (< 1KB)
- Write-heavy, read-light
- Version history needed

**Page Storage Characteristics**:
- Fixed size (16KB)
- Frequent updates
- Multi-versioning not required
- Consistent with Compute Node model

#### 5.7.2 Goal: Segment-Page Model (PostgreSQL Style)

```
Refactored Architecture:
┌──────────────────────────────────────────────────────────────┐
│  Compute                                                      │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, Page>  (In-place update)              │  │
│  │  B+tree index                                           │  │
│  └────────────────────────────────────────────────────────┘  │
│                         │ WAL batch (KV redo)                 │
│                         ▼                                     │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  Storage (Segment-Page)                                       │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  HashMap<PageId, Page>  (buffer pool, In-place update)  │  │
│  │  page_file (Fixed offset: page_id × PAGE_SIZE)          │  │
│  │  WAL segments (For crash recovery, reuse existing impl)  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**Core Changes**:
1. **Remove Bitcask**: No longer use append-only segments + memory index.
2. **Fixed Offset Page File**: `page_file[page_id × 16KB]`, in-place overwrite.
3. **Buffer Pool**: Cache dirty pages in memory, batch flush to disk.
4. **WAL Reuse**: Existing WAL mechanism unchanged, used for Crash Recovery.

#### 5.7.3 Data Structure Design

```rust
/// Segment-Page Storage Node
pub struct PageStore {
    dir: PathBuf,
    
    /// Buffer pool: pages in memory
    /// - Check buffer first on read, load from disk on miss
    /// - Update buffer on write, mark as dirty
    buffer_pool: RwLock<HashMap<PageId, BufferPage>>,
    
    /// Page file: Fixed offset, page_id × PAGE_SIZE
    /// - Single file, supports sparse files (unwritten pages don't take disk space)
    /// - Or multi-file segments (e.g., 1GB per file)
    page_file: Mutex<PageFile>,
    
    /// WAL writer: reuse existing implementation
    wal_sender: Mutex<Option<Sender<WalBatch>>>,
    
    /// Checkpoint state
    checkpoint_lsn: AtomicU64,
    
    /// Max allocated page_id (for new page allocation)
    max_page_id: AtomicU64,
}

/// Individual page in Buffer pool
struct BufferPage {
    /// Page data
    data: Box<[u8; PAGE_SIZE]>,
    
    /// Whether it is a dirty page (needs flushing)
    dirty: bool,
    
    /// Page LSN (for WAL recovery judgment)
    lsn: u64,
}

/// Page file abstraction
struct PageFile {
    /// Single file mode: one large file
    file: File,
    
    /// Current file size (for determining expansion needs)
    size: u64,
}
```

#### 5.7.4 Page File Layout

**Scheme A: Single File (Simple, recommended for first implementation)**

```
page_file:
┌─────────────────────────────────────────────────────────────┐
│ Page 0      │ Page 1      │ Page 2      │ ... │ Page N     │
│ [0, 16KB)   │ [16KB, 32KB)│ [32KB, 48KB)│     │            │
└─────────────────────────────────────────────────────────────┘
              │
              └── offset = page_id × PAGE_SIZE

Features:
- Simple and direct
- Relies on file system sparse file support (Linux ext4/xfs, macOS APFS)
- Holes in non-contiguous page_ids don't take disk space
```

**Scheme B: Segmented Files (Optional, for large data volumes)**

```
data/
├── pages-0000000000.dat    # page_id 0 ~ 65535
├── pages-0000000001.dat    # page_id 65536 ~ 131071
└── pages-0000000002.dat    # ...

Each file: 65536 × 16KB = 1GB
In-file offset: (page_id % 65536) × PAGE_SIZE
```

**Current Choice**: Scheme A (Single File), simple and sufficient.

#### 5.7.5 Read/Write Flow

**Read Flow**:

```
get(page_id) -> Option<Page>
    │
    ▼
┌─────────────────────────┐
│ 1. Check buffer_pool    │
│    RwLock::read()       │
└───────────┬─────────────┘
            │
    ┌───────┴───────┐
    │ hit?          │
    ▼               ▼
  Return data    ┌─────────────────────────┐
                │ 2. Read from page_file   │
                │    seek(page_id × 16KB) │
                │    read_exact(16KB)     │
                └───────────┬─────────────┘
                            │
                            ▼
                ┌─────────────────────────┐
                │ 3. Insert into buffer_pool│
                │    dirty = false        │
                └───────────┬─────────────┘
                            │
                            ▼
                         Return data
```

**Write Flow (Triggered by WAL replay)**:

```
put(page_id, data, lsn)
    │
    ▼
┌─────────────────────────┐
│ 1. Update buffer_pool    │
│    RwLock::write()      │
│    dirty = true         │
│    lsn = lsn            │
└───────────┬─────────────┘
            │
            ▼
        Return Ok(())

Note:
- Writes only update the buffer and don't flush immediately
- Flushing is triggered by checkpoint
- WAL is already persistent, buffer loss is recoverable
```

**Checkpoint Flow**:

```
checkpoint()
    │
    ▼
┌─────────────────────────────────────────┐
│ 1. Collect all dirty pages               │
│    let dirty_pages = buffer_pool        │
│        .iter()                          │
│        .filter(|p| p.dirty)             │
│        .collect();                      │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 2. Sort by page_id (Sequential write opt)│
│    dirty_pages.sort_by_key(|p| p.id);   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 3. Batch write to page_file              │
│    for page in dirty_pages:             │
│        seek(page_id × PAGE_SIZE)        │
│        write_all(page.data)             │
│        page.dirty = false               │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 4. fsync + update checkpoint_lsn         │
│    page_file.sync_all()                 │
│    checkpoint_lsn = max(dirty lsn)      │
│    write_checkpoint_state()             │
└───────────────────┬─────────────────────┘
                    │
                    ▼
                Return Ok(())
```

#### 5.7.6 Crash Recovery

**Recovery Flow**:

```
open(dir) -> Result<PageStore>
    │
    ▼
┌─────────────────────────────────────────┐
│ 1. Read checkpoint_state                 │
│    checkpoint_lsn = read_checkpoint()   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 2. Open page_file                        │
│    max_page_id = file_size / PAGE_SIZE  │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 3. Replay WAL (Reuse existing logic)     │
│    for record in wal where              │
│        record.lsn > checkpoint_lsn:     │
│        apply_record(record)             │
└───────────────────┬─────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ 4. Start background WAL replay thread    │
│    start_wal_replay()                   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
                Return Ok(store)
```

**Key Points**:
- WAL before `checkpoint_lsn` can be safely deleted.
- Recovery only requires replaying WAL after `checkpoint_lsn`.
- No need to scan page_file to rebuild index (unlike Bitcask).

#### 5.7.7 Checkpoint Strategy

**Trigger Conditions** (any one):

| Condition | Threshold | Description |
|------|------|------|
| **Number of dirty pages** | > 1000 | Prevent excessive buffer size |
| **Total dirty bytes** | > 64MB | Control memory usage |
| **Time interval** | > 60s | Periodic flush |
| **WAL size** | > 256MB | Allow cleaning old WAL |

**Implementation**:

```rust
impl PageStore {
    fn maybe_checkpoint(&self) {
        let stats = self.buffer_stats();
        
        let should_checkpoint = 
            stats.dirty_count > 1000 ||
            stats.dirty_bytes > 64 * 1024 * 1024 ||
            stats.since_last_checkpoint > Duration::from_secs(60);
        
        if should_checkpoint {
            let _ = self.checkpoint();
        }
    }
}
```

#### 5.7.8 Relationship with Existing Code

**Retained**:
- `WalBatch`, `WalRecord` structures
- `WalWriter`, `wal_writer_loop` logic
- WAL segment format and read/write
- `wal_state` persistence

**Removed**:
- Bitcask segment files (`segment-*.log`)
- `IndexEntry { file_id, offset }` memory index
- `compact()` and related logic
- `stale_entries` statistics

**Modified**:
- `StorageNode` → `PageStore` (or retain name, replace implementation)
- `put(page_id, data)` → update buffer, mark dirty
- `get(page_id)` → buffer pool lookup + disk fallback
- Simplified Crash Recovery logic

#### 5.7.9 Comparative Analysis

| Aspect | Bitcask (Current) | Segment-Page (Refactored) |
|------|---------------|----------------|
| **Update Cost** | Append 16KB | In-place overwrite 16KB |
| **Space Amplification** | High (Multi-version accumulation) | None (1:1) |
| **Compaction** | Required, periodic | Not required |
| **Startup Recovery** | Scans all segments | Replays only WAL increments |
| **Memory Index** | `HashMap<PageId, IndexEntry>` | Not needed (Fixed offset calculation) |
| **Code Complexity** | High (compaction, manifest) | Low |
| **Symmetry with Compute** | No | Yes |

#### 5.7.10 Implementation Steps

1. **Phase 1: PageStore Basic Structure**
   - New `PageStore` structure
   - Implement `open()`, `get()`, `put()`
   - Single-file page_file read/write

2. **Phase 2: Buffer Pool**
   - Implement `BufferPage` and dirty tracking
   - Implement `checkpoint()`
   - `checkpoint_state` persistence

3. **Phase 3: WAL Integration**
   - Reuse existing `WalWriter`
   - Modify `wal_replay_loop` to call `PageStore`
   - Crash Recovery testing

4. **Phase 4: Migration**
   - Replace `StorageNode` implementation
   - Update `StorageServer` RPC handling
   - Clean up Bitcask-related code

5. **Phase 5: Verification**
   - Pass existing tests
   - Add checkpoint tests
   - Crash Recovery tests
   - Performance comparison

#### 5.7.11 File Layout (Refactored)

```
data/
├── pages.dat           # Page file (fixed offset)
├── checkpoint_state    # checkpoint LSN
├── wal-*.log           # WAL segments (reused)
└── wal_state           # WAL replay state (reused)
```

### 5.8 Paged B+tree (Index-as-Page) Scheme

- Goal: Index nodes themselves are fixed 16KB pages, root page_id is persisted.
- Storage only provides `page_id -> page bytes` (can reuse Segment-Page storage).
- Compute Node maintains buffer pool (page cache + dirty flush).

**Suggested Page Header:**
```
PageHeader {
  page_id: u64
  page_type: u8    // INTERNAL=1, LEAF=2
  level: u8        // 0=leaf
  key_count: u16
  free_start: u16
  free_end: u16
  lsn: u64         // redo order
}
```

**Leaf Page:**
- Records `(key, value)` or `(key, slot_ref)`

**Internal Page:**
- `keys[]` + `child_page_id[]` (count = key_count + 1)

**WAL (Required):**
- Write redo on page modification
- WAL durable at commit, then flush dirty pages

**Minimum Implementation Path:**
1) Page format + serialization
2) leaf-only B+tree (no internal)
3) internal + split
4) buffer pool + dirty flush
5) WAL redo + recovery

#### Constraints
- **Storage is single-writer model**, serial writes
- Memory index is HashMap (lock-free), updated by single writer thread

---

### 5.9 Code Review Results (2026-02-01)

#### 5.9.1 Critical Issues

| Severity | Issue | Location | Impact |
|---------|------|------|------|
| 🔴 | Synchronous Cap'n RPC use | client.rs:287 | High RPC overhead, large latency |
| 🔴 | B+Tree missing leaf linked list | bptree.rs:10-18 | Inefficient range scan |
| 🔴 | Synchronous file I/O blocks async | node.rs:156 | Blocks tokio thread pool |
| 🔴 | `spawn_local` compatibility | server.rs:165 | Potential abnormal behavior in multi-thread runtime |

#### 5.9.2 Detailed Issue Description

**Synchronous Cap'n RPC use**:
```rust
// Current: Blocking wait for each RPC
if let Some(storage) = &self.storage {
    storage.put(page_id, &page).await?;  // ← Wait one by one
}

// Sliding window scheme (5.3.5): Send if window has space, otherwise queue
```

**B+Tree missing leaf linked list**:
```rust
// Current: only keys
Node::Leaf {
    keys: Vec<K>,
},

// Suggestion: Add sibling pointers
Node::Leaf {
    keys: Vec<K>,
    prev: Option<Box<Node<K>>>,
    next: Option<Box<Node<K>>>,
},
```

**Synchronous file I/O**:
```rust
// Current: using std::fs::File (blocking)
writer.file.write_all(value)?;

// Suggestion: switch to tokio async API
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
```

**spawn_local compatibility**:
```rust
// Current: mixing spawn_local + features = ["full"]
tokio::task::spawn_local(async move { ... });
// features = ["full"] includes rt-multi-thread

// Scheme A: use spawn uniformly
tokio::task::spawn(async move { ... });

// Scheme B: use current_thread runtime
// Cargo.toml: features = ["rt-current-thread", "net"]
```

#### 5.9.3 Medium Issues

| Severity | Issue | Location | Repair Suggestion |
|---------|------|------|----------|
| 🟡 | Async I/O within lock | client.rs:230-251 | Release lock before I/O |
| 🟡 | MAX_KEYS = 8 too small | bptree.rs:3 | Dynamic adjustment based on page size |
| 🟡 | Batch operations not concurrent | client.rs:387 | Use `join_all` for concurrent execution |

#### 5.9.4 Minor Issues

| Severity | Issue | Location | Repair Suggestion |
|---------|------|------|----------|
| 🟢 | Unused dashmap | Cargo.toml:11 | Remove dependency |
| 🟢 | Excessive tokio features | Cargo.toml:14 | Use `rt-multi-thread + net` |

#### 5.9.5 Fix Priorities

| Priority | Issue | Estimated Change |
|--------|------|----------|
| **P0** | Synchronous file I/O → Async | Significant changes in node.rs |
| **P0** | Cap'n RPC Sliding Window | Medium changes in client.rs |
| **P1** | spawn_local compatibility | Small changes in server.rs |
| **P2** | B+Tree leaf linked list | Medium changes in bptree.rs |
| **P3** | Clean up dependencies | Cargo.toml |

---

## 4. Storage Node - Unit Tests

### 4.1 Test File Structure

```
src/storage/
├── mod.rs
├── node.rs              # Storage Node core logic (DashMap)
├── lock_table.rs        # Lock table implementation
└── tests/
    ├── mod.rs
    ├── basic.rs         # Basic read/write tests
    ├── concurrent.rs    # Concurrent tests
    └── lock.rs          # Lock tests
```

### 4.2 Basic Read/Write Tests

```rust
// src/storage/tests/basic.rs

use crate::storage::StorageNode;

#[test]
fn test_put_and_get() {
    // RED: write test first, expect failure
    let node = StorageNode::new();
    
    // GREEN: test passes after implementation
    node.put("foo", b"bar");
    
    let value = node.get("foo");
    assert_eq!(value, Some(b"bar".to_vec()));
}

#[test]
fn test_get_missing_key() {
    let node = StorageNode::new();
    let value = node.get("missing");
    assert_eq!(value, None);
}

#[test]
fn test_overwrite() {
    let node = StorageNode::new();
    node.put("foo", b"bar");
    node.put("foo", b"baz");
    
    let value = node.get("foo");
    assert_eq!(value, Some(b"baz".to_vec()));
}

#[test]
fn test_delete() {
    let node = StorageNode::new();
    node.put("foo", b"bar");
    node.delete("foo");
    
    let value = node.get("foo");
    assert_eq!(value, None);
}

#[test]
fn test_empty_key() {
    let node = StorageNode::new();
    node.put("", b"value");
    
    let value = node.get("");
    assert_eq!(value, Some(b"value".to_vec()));
}

#[test]
fn test_large_value() {
    let node = StorageNode::new();
    let value = vec![0u8; 100_000];
    
    node.put("large", &value);
    let retrieved = node.get("large").unwrap();
    
    assert_eq!(retrieved.len(), 100_000);
}

#[test]
fn test_many_keys() {
    let node = StorageNode::new();
    
    // Insert 10,000 keys
    for i in 0..10_000 {
        node.put(&format!("key_{}", i), &format!("value_{}", i));
    }
    
    assert_eq!(node.len(), 10_000);
    
    // Verify random reads
    for i in (0..10_000).step_by(1000) {
        let value = node.get(&format!("key_{}", i)).unwrap();
        assert_eq!(value, format!("value_{}").as_bytes());
    }
}
```

### 4.3 Concurrent Tests

```rust
// src/storage/tests/concurrent.rs

use std::sync::Arc;
use std::thread;

#[test]
fn test_concurrent_read() {
    let node = Arc::new(StorageNode::new());
    node.put("key", b"value");
    
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = node.clone();
            thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = node.get("key");
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
}

#[test]
fn test_concurrent_write_different_keys() {
    let node = Arc::new(StorageNode::new());
    
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let node = node.clone();
            thread::spawn(move || {
                for j in 0..1000 {
                    node.put(&format!("key_{}", i * 1000 + j), b"value");
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    assert_eq!(node.len(), 10_000);
}

#[test]
fn test_concurrent_put_same_key() {
    let node = Arc::new(StorageNode::new());
    node.put("key", b"initial");
    
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = node.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    node.put("key", &format!("value_{}", i).into_bytes());
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    // Final value should be result of one of the writes
    let value = node.get("key").unwrap();
    assert!(value.starts_with(b"value_"));
}

#[test]
fn test_concurrent_mixed_operations() {
    let node = Arc::new(StorageNode::new());
    
    // Pre-insert some data
    for i in 0..1000 {
        node.put(&format!("key_{}", i), b"initial");
    }
    
    let mut handles = Vec::new();
    
    // Read threads
    for _ in 0..5 {
        let node = node.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..500 {
                let key = thread_rng().gen_range(0..1000);
                let _ = node.get(&format!("key_{}", key));
            }
        }));
    }
    
    // Write threads
    for i in 0..5 {
        let node = node.clone();
        handles.push(thread::spawn(move || {
            for j in 0..500 {
                node.put(&format!("key_{}", i * 100 + j), b"updated");
            }
        }));
    }
    
    handles.into_iter().for_each(|h| h.join().unwrap());
}
```

### 4.4 Lock Tests

```rust
// src/storage/tests/lock.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Barrier;

#[test]
fn test_lock_blocks_other_writer() {
    let node = StorageNode::new();
    node.put("key", b"value");
    
    let guard = node.lock("key").unwrap();
    
    // Another lock should fail or block
    let result = node.try_lock("key");
    assert!(result.is_err());
    
    drop(guard);
    
    // Should be able to acquire lock after release
    let guard2 = node.lock("key");
    assert!(guard2.is_ok());
}

#[test]
fn test_different_keys_no_contention() {
    let node = StorageNode::new();
    
    let guard1 = node.lock("key1").unwrap();
    let guard2 = node.lock("key2").unwrap();  // Should not block
    
    drop(guard1);
    drop(guard2);
}

#[test]
fn test_lock_with_operations() {
    let node = StorageNode::new();
    node.put("key", b"old_value");
    
    // Execute operations after locking
    {
        let _guard = node.lock("key").unwrap();
        node.put("key", b"new_value");
    }
    
    // Verify after unlocking
    let value = node.get("key").unwrap();
    assert_eq!(value, b"new_value");
}

#[test]
fn test_concurrent_lock_contention() {
    let node = Arc::new(StorageNode::new());
    let barrier = Arc::new(Barrier::new(10));
    let success_count = Arc::new(AtomicUsize::new(0));
    
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = node.clone();
            let barrier = barrier.clone();
            let success = success_count.clone();
            
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    if let Ok(guard) = node.try_lock("contested_key") {
                        node.put("contested_key", b"updated");
                        success.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    std::hint::spin_loop();
                }
            })
        })
        .collect();
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    // At least one successful lock acquisition
    assert!(success_count.load(Ordering::SeqCst) > 0);
}
```

---

## 5. Compute Node - Unit Tests

### 5.1 Test File Structure

```
src/compute/
├── mod.rs
├── node.rs              # Compute Node core logic
├── client.rs            # Network client
└── tests/
    ├── mod.rs
    ├── basic.rs         # Basic read/write tests
    └── cache.rs         # Cache tests
```

### 5.2 Basic Read/Write Tests

```rust
// src/compute/tests/basic.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup_test_infrastructure() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

#[test]
fn test_compute_put_get() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    compute.put("foo", b"bar").unwrap();
    let value = compute.get("foo").unwrap();
    
    assert_eq!(value, Some(b"bar".to_vec()));
}

#[test]
fn test_compute_update() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    compute.put("key", b"v1").unwrap();
    compute.put("key", b"v2").unwrap();
    
    let value = compute.get("key").unwrap();
    assert_eq!(value, Some(b"v2".to_vec()));
}

#[test]
fn test_compute_missing_key() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    let value = compute.get("missing");
    assert_eq!(value, None);
}

#[test]
fn test_compute_multiple_keys() {
    let (_storage, addr) = setup_test_infrastructure();
    let compute = ComputeNode::new(&addr);
    
    for i in 0..100 {
        compute.put(&format!("key_{}", i), &format!("value_{}", i)).unwrap();
    }
    
    for i in 0..100 {
        let value = compute.get(&format!("key_{}", i)).unwrap();
        assert_eq!(value, Some(format!("value_{}").as_bytes().to_vec()));
    }
}
```

### 5.3 Cache Tests

```rust
// src/compute/tests/cache.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

#[test]
fn test_cache_hit_miss() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // First read - cache miss
    let _ = compute.get("key");
    assert_eq!(compute.cache_misses(), 1);
    assert_eq!(compute.cache_hits(), 0);
    
    // Second read - cache hit
    let _ = compute.get("key");
    assert_eq!(compute.cache_hits(), 1);
}

#[test]
fn test_cache_invalidation_on_write() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // Write data
    compute.put("key", b"v1").unwrap();
    
    // Read into cache
    let _ = compute.get("key");
    assert_eq!(compute.cache_hits(), 1);
    
    // Write again - should invalidate cache
    compute.put("key", b"v2").unwrap();
    
    // Next read should fetch from Storage Node again
    let _ = compute.get("key");
    assert_eq!(compute.cache_misses(), 2);
}

#[test]
fn test_cache_statistics() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // Initial state
    assert_eq!(compute.cache_hits(), 0);
    assert_eq!(compute.cache_misses(), 0);
    assert_eq!(compute.cache_size(), 0);
    
    // Multiple reads and writes
    for i in 0..10 {
        compute.put(&format!("key_{}", i), &format!("value_{}", i)).unwrap();
    }
    
    for i in 0..10 {
        let _ = compute.get(&format!("key_{}", i));
    }
    
    // Read 5 keys twice (5 hits)
    for i in 0..5 {
        let _ = compute.get(&format!("key_{}", i));
    }
    
    assert_eq!(compute.cache_hits(), 5);
    assert_eq!(compute.cache_misses(), 10);  // 1 miss for each of the 10 keys
    assert_eq!(compute.cache_size(), 10);
}
```

---

## 6. Integration Tests

### 6.1 Full Workflow Test

```rust
// tests/integration.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

#[test]
fn test_full_workflow() {
    let (storage, addr) = setup();
    
    // Create two compute nodes
    let mut compute1 = ComputeNode::new(&addr);
    let mut compute2 = ComputeNode::new(&addr);
    
    // Compute Node 1 writes
    compute1.put("key1", b"value1").unwrap();
    compute1.put("key2", b"value2").unwrap();
    
    // Compute Node 2 reads
    let v1 = compute2.get("key1").unwrap().unwrap();
    let v2 = compute2.get("key2").unwrap().unwrap();
    
    assert_eq!(v1, b"value1");
    assert_eq!(v2, b"value2");
    
    // Verify storage node data
    assert_eq!(storage.get("key1"), Some(b"value1".to_vec()));
    assert_eq!(storage.get("key2"), Some(b"value2".to_vec()));
}

#[test]
fn test_concurrent_computes() {
    let (storage, addr) = setup();
    
    let mut handles = vec![];
    
    for i in 0..4 {
        let addr = addr.clone();
        handles.push(thread::spawn(move || {
            let mut compute = ComputeNode::new(&addr);
            for j in 0..100 {
                compute.put(&format!("key_{}", i * 100 + j), b"value").unwrap();
            }
        }));
    }
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    // Verify storage node data integrity
    assert_eq!(storage.len(), 400);
}

#[test]
fn test_cross_node_consistency() {
    let (storage, addr) = setup();
    
    let mut compute1 = ComputeNode::new(&addr);
    let mut compute2 = ComputeNode::new(&addr);
    
    // Compute Node 1 writes
    compute1.put("shared", b"from_compute1").unwrap();
    
    // Compute Node 2 reads
    let value = compute2.get("shared").unwrap().unwrap();
    assert_eq!(value, b"from_compute1");
    
    // Compute Node 2 updates
    compute2.put("shared", b"from_compute2").unwrap();
    
    // Compute Node 1 reads the latest value
    let value = compute1.get("shared").unwrap().unwrap();
    assert_eq!(value, b"from_compute2");
}
```

---

## 7. YCSB Benchmark

### 7.1 YCSB Workload Definition

```rust
// benches/ycsb.rs

use kv_store::{ComputeNode, StorageNode};
use rand::Rng;

#[derive(Clone, Copy)]
enum Workload {
    A,  // 50% read, 50% write
    B,  // 95% read, 5% write
    C,  // 100% read
}

struct YcsbClient {
    node: ComputeNode,
    keys: Vec<String>,
    rng: rand::ThreadRng,
    workload: Workload,
}

impl YcsbClient {
    fn new(node: ComputeNode, record_count: usize, workload: Workload) -> Self {
        let keys: Vec<String> = (0..record_count)
            .map(|i| format!("user{:06}", i))
            .collect();
        
        Self {
            node,
            keys,
            rng: rand::thread_rng(),
            workload,
        }
    }
    
    fn run_one_op(&mut self) {
        match self.workload {
            Workload::A => {
                if self.rng.gen::<f64>() < 0.5 {
                    self.do_read();
                } else {
                    self.do_write();
                }
            }
            Workload::B => {
                if self.rng.gen::<f64>() < 0.95 {
                    self.do_read();
                } else {
                    self.do_write();
                }
            }
            Workload::C => {
                self.do_read();
            }
        }
    }
    
    fn do_read(&mut self) {
        let key = self.pick_random_key();
        let _ = self.node.get(&key);
    }
    
    fn do_write(&mut self) {
        let key = self.pick_random_key();
        let value = format!("value_{}", self.rng.gen::<u64>());
        let _ = self.node.put(&key, value.as_bytes());
    }
    
    fn pick_random_key(&mut self) -> &str {
        let idx = self.rng.gen_range(0..self.keys.len());
        &self.keys[idx]
    }
}

fn run_ycsb_workload(ops: usize, threads: usize, workload: Workload) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    std::thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    let start = std::time::Instant::now();
    let mut handles = vec![];
    let ops_per_thread = ops / threads;
    
    for _ in 0..threads {
        let addr = format!("127.0.0.1:{}", addr.port());
        let node = ComputeNode::new(&addr);
        let mut client = YcsbClient::new(node, 100_000, workload);
        handles.push(std::thread::spawn(move || {
            for _ in 0..ops_per_thread {
                client.run_one_op();
            }
        }));
    }
    
    handles.into_iter().for_each(|h| h.join().unwrap());
    
    let duration = start.elapsed();
    let throughput = ops as f64 / duration.as_secs();
    
    println!("=== YCSB {:?} ===", workload);
    println!("Operations: {}", ops);
    println!("Threads: {}", threads);
    println!("Duration: {:?}", duration);
    println!("Throughput: {:.2} ops/sec", throughput);
}
```

### 7.2 YCSB Benchmark Definition

```rust
// benches/ycsb.rs (using criterion)

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("ycsb_a_10000", |b| {
        b.iter(|| {
            run_ycsb_workload(10000, 4, Workload::A);
        });
    });
    
    c.bench_function("ycsb_b_10000", |b| {
        b.iter(|| {
            run_ycsb_workload(10000, 4, Workload::B);
        });
    });
    
    c.bench_function("ycsb_c_10000", |b| {
        b.iter(|| {
            run_ycsb_workload(10000, 4, Workload::C);
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
```

---

## 8. Smoke Tests

```rust
// tests/smoke.rs

use std::net::TcpListener;
use std::thread;
use std::sync::Arc;

use crate::storage::StorageNode;
use crate::compute::ComputeNode;

fn setup() -> (Arc<StorageNode>, String) {
    let storage = Arc::new(StorageNode::new());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    
    let storage_clone = storage.clone();
    thread::spawn(move || {
        accept_connection(listener, storage_clone);
    });
    
    (storage, format!("127.0.0.1:{}", addr.port()))
}

/// Fast smoke test, must pass for every PR
#[test]
fn test_smoke() {
    let (_storage, addr) = setup();
    let mut compute = ComputeNode::new(&addr);
    
    // Basic operations
    compute.put("key1", b"value1").unwrap();
    assert_eq!(compute.get("key1").unwrap(), Some(b"value1".to_vec()));
    
    compute.put("key1", b"value2").unwrap();
    assert_eq!(compute.get("key1").unwrap(), Some(b"value2".to_vec()));
    
    compute.put("key2", b"value2").unwrap();
    assert_eq!(compute.get("key2").unwrap(), Some(b"value2".to_vec()));
    
    println!("Smoke test passed!");
}
```

---

## 9. Running Tests

```bash
# Run all tests
cargo test

# Run unit tests
cargo test --lib

# Run integration tests
cargo test --test integration

# Run YCSB benchmark
cargo bench ycsb

# Run smoke tests
cargo test --test smoke

# Run with logs
RUST_LOG=debug cargo test

# Concurrent tests
cargo test concurrent --release

# Run specific tests
cargo test test_put_and_get
cargo test test_compute_put_get
```

---

## 10. Test Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Run coverage
cargo tarpaulin --out Html

# View coverage report
open tarpaulin-report.html
```

---

## 11. TDD Steps

### Step 1: Write Storage Node Unit Test

```rust
// tests/storage_basic_test.rs (RED - expected to fail)
#[test]
fn test_storage_put_get() {
    let node = StorageNode::new();
    node.put("foo", b"bar");
    assert_eq!(node.get("foo"), Some(b"bar".to_vec()));
}
```

### Step 2: Implement Storage Node

```rust
// src/storage/node.rs (GREEN - pass test)
use dashmap::DashMap;

pub struct StorageNode {
    data: DashMap<String, Vec<u8>>,
}

impl StorageNode {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
    
    pub fn put(&self, key: &str, value: &[u8]) {
        self.data.insert(key.to_string(), value.to_vec());
    }
    
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.data.get(key).map(|v| v.clone())
    }
    
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
```

### Step 3: Refactor and Add More Tests

```rust
// Add more tests...
// Refactor code...
```

---

## 12. Summary

| Test Type | Location | Purpose | Priority |
|----------|------|------|--------|
| **Unit Tests** | `src/**/*.rs` | Core logic | ⭐⭐⭐ |
| **Integration Tests** | `tests/**/*.rs` | Component interaction | ⭐⭐⭐ |
| **YCSB** | `benches/ycsb.rs` | Performance benchmark | ⭐⭐ |
| **Smoke Tests** | `tests/smoke.rs` | Quick verification | ⭐⭐⭐ |

**TDD Process**: Write test first → Implement code → Refactor → Repeat
