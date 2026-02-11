# Scale-KV External API v1 (Draft)

Status: Draft  
Owner: scale-kv maintainers  
Scope: single-compute Aurora-style KV

This document defines the **stable external API surface** and separates it from
internal-only interfaces.

## 1. Stability Levels

- `Stable`: supported external contract in v1.
- `Internal`: implementation detail; may change without notice.
- `Deprecated`: exists temporarily, not recommended for new usage.

## 2. Compute API (Rust)

Type: `EmbeddedCompute` (`/Users/yusp/work/scale-kv/src/embedded_compute.rs`)

### Stable

- `connect(addrs, quorum, local) -> Result<EmbeddedCompute>`
- `begin_ro_timeout(Duration)`
- `begin_rw_timeout(Duration)`
- `begin_ro_guard() -> (u64, ReadGuard)`  
  Use for long read snapshots / GC watermark pinning.
- `begin_rw() -> EmbeddedTxn`
- `put(key, value) -> Result<u64>`
- `get(key) -> Result<Option<Vec<u8>>>`
- `delete(key) -> Result<u64>`
- `warmup_scan_all(limit_per_batch) -> Result<usize>`
- `gc_once(budget_pages) -> Result<usize>`
- `durable_lsn() -> u64`

### Internal

- `warmed_pages() -> usize`
- `gc_lsn()` (internal watermark computation)
- `write_page(...)`
- `get_page(...)`
- `cached_page(...)`
- `EmbeddedTxn::debug_check_mapping(...)`

## 3. Transaction KV API (Rust)

Type: `TxnManager` (`/Users/yusp/work/scale-kv/src/txn_kv.rs`)

### Stable

- `open_quorum(replica_paths, quorum)`
- `open_with_storage(storage)`
- `checkpoint()`
- `begin_ro_timeout(Duration)`
- `begin_rw_timeout(Duration)`

Notes:

- Single-replica mode is `open_quorum(vec![path], 1)`.

Design rule:

- External transaction begin must be explicit timeout.
- Internal auto-commit paths may use non-timeout internal helpers.

### Internal

- `begin_tx(...)` helper

## 4. Storage RPC API (Cap'n Proto)

Schema: `/Users/yusp/work/scale-kv/schema/storage.capnp`

### Stable

- `getDurableLsn`
- `appendTxnBatch`
- `getPage`
- `scanPages`

### Not in v1

- Any logical KV RPC (for example `txnGet`) is out of scope.

## 5. Storage Node Admin API (Rust local)

Type: `StorageNode` (`/Users/yusp/work/scale-kv/src/node.rs`)

### Stable (operational)

- `open(...)`
- `open_with_maintenance(...)`
- `checkpoint()`
- `truncate_wal()`
- `checkpoint_and_truncate()`
- `begin_mvcc_ro_guard()`
- `list_active_reads()`
- `abort_active_read(id)`

### Internal / Local-only

- `mvcc_get(...)`
- `txn_get(...)`
- direct page put/get helpers (`put/get/delete/keys/contains`) for tests/dev

## 6. Error Contract (v1)

External callers should treat these as stable categories:

- Invalid input (key/value/size)
- Conflict (`WriteWriteConflict`)
- Timeout (`TxnTimeout`)
- IO/backpressure (`WouldBlock` on WAL pressure)

Error text is not a stable contract; error kind/category is.

## 7. API Change Process

Any `Stable` API change requires:

1. Update this doc.
2. Add migration note (if behavior changes).
3. Add/adjust tests that lock contract behavior.
