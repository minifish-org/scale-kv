# IOT Refactor Plan (Heap/FSM -> Primary-Tree Inline Rows)

## Goal
Move `scale-kv` from heap+FSM indirection to a pure index-organized table (IOT): the primary `PageBPlusTree` leaf stores row data inline (`VALUE + MVCC header`) with undo-chain based snapshot visibility.

## Phase 1 (this change)
1. Add inline primary-row value encoding in `page_bptree`:
- Replace `SlotRef` payload with fixed-size `LeafValue` (`value`, `commit_lsn`, `undo_ptr`, `flags`).
- Update B+Tree `get/insert/range/range_visit` and tests for the new value type.
2. Switch `EmbeddedCompute` data path to IOT rows:
- `put/get/delete/scan_range` operate directly on primary-tree inline rows.
- Keep snapshot semantics: visible row selection by `commit_lsn <= read_lsn`, otherwise walk undo chain.
- Keep commit protocol: reserve LSN range first, stamp modified rows with `commit_lsn`, then commit dirty pages.
3. Remove heap/FSM allocation path from transactional read/write logic:
- No data-page allocation/insert/update via `slotted_page` in these paths.
4. Adapt GC to inline rows:
- GC scans primary rows, removes old tombstones, and truncates undo chains on globally visible rows.
- Undo sweep computes reachability from inline row `undo_ptr` heads.

## Phase 2 (next)
1. Redesign undo for transaction-based logs (instead of per-row append-only chains only):
- Add txn undo log metadata (`txn_id`, begin/end LSN, record count, state).
- Group undo records by transaction to support efficient purge and rollback traversal.
2. Rollback redesign:
- Roll back via txn undo log iteration in reverse write order.
- Make row-head updates idempotent for restart safety.
3. Purge redesign:
- Purge whole committed/aborted txn undo segments once below GC watermark.
- Keep lightweight free-space accounting for undo pages/segments.
4. Follow-up validation:
- Add crash/recovery and long-snapshot tests for mixed update/delete workloads.
