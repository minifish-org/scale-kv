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

## Phase 2 (txn undo segments + purge-by-txn)
### Scope (this change)
1. Transaction undo segment model:
- Every RW txn gets a `txn_id` (`request_id`) before first undo append.
- Undo records are allocated from that txn's segment pages, not from a shared global append head.
- First page of the segment stores a small segment header:
  - `txn_id`
  - `begin_lsn`
  - `commit_lsn` (`end_lsn`)
  - `state` (`in_progress|committed|aborted|purged`)
  - `first_page_id`, `last_page_id`
  - `record_count`
  - history-list links (`history_prev`, `history_next`) over committed segments.
2. Undo record layout:
- Keep row-version chain pointer (`prev`, old behavior) for MVCC visibility walks.
- Add `txn_id` and `txn_next` (link to previous record in same txn), so rollback can later walk txn-local history without row scans.
3. Commit protocol:
- Reserve LSN range (`start_lsn`, `end_lsn`) using preassigned `request_id`.
- Stamp modified row heads with `commit_lsn=end_lsn`.
- Atomically (single WAL txn batch):
  - mark segment header `committed` with `begin_lsn/commit_lsn/record_count/page bounds`;
  - append segment to global committed history list (`meta.undo_history_head/tail` + tail link update when needed);
  - persist regular dirty pages.
4. Purge redesign:
- Remove reachability-based `gc_sweep_undo` (no full row walk for undo reclamation).
- Purge from history-list head only:
  - while head segment is `committed` and `commit_lsn <= gc_lsn`, reclaim whole segment pages to `undo_free`;
  - unlink from history list and advance head/tail in meta.
- This is O(number of purged segments + pages in those segments), independent of total primary-row count.

### Deferred to later phase
1. Rollback execution path (use `txn_next` chain in reverse write order).
2. Crash-recovery hardening for in-progress/aborted segment transitions.
3. Extended long-snapshot + restart tests for mixed update/delete/rollback workloads.
