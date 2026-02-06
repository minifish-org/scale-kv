@0x9f5b7c9a2b4d4f1c;

# A single page after-image write.
struct PageWrite {
  pageId @0 :UInt64;
  page @1 :Data; # raw page bytes (PAGE_SIZE)
}

# A txn batch with compute-assigned LSN range.
#
# Aurora-style: compute sends page after-images; storage persists and replays them.
struct TxnBatch {
  requestId @0 :UInt64; # for idempotent retry; persisted in WAL
  startLsn @1 :UInt64;
  endLsn @2 :UInt64;    # exclusive right boundary; commit point
  writes @3 :List(PageWrite);
}

struct PageItem {
  pageId @0 :UInt64;
  pageLsn @1 :UInt64;
  page @2 :Data; # raw page bytes (PAGE_SIZE)
}

interface Storage {
  # Progress / fencing
  getDurableLsn @0 () -> (durableLsn :UInt64);

  # WAL replication (Aurora-style). Compute assigns LSN; storage persists and acks durability.
  appendTxnBatch @1 (batch :TxnBatch) -> (commitLsn :UInt64, durableLsn :UInt64);

  # Page fetch (latest only, best-effort pageLsn).
  getPage @2 (pageId :UInt64) -> (found :Bool, page :Data, pageLsn :UInt64, durableLsn :UInt64);

  # Bulk page scan for warmup.
  scanPages @3 (startPageId :UInt64, limit :UInt32) -> (pages :List(PageItem), durableLsn :UInt64);
}
