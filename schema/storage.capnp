@0x9f5b7c9a2b4d4f1c;

struct KeyValue {
  key @0 :UInt64;
  value @1 :Data;
}

struct WalRecord {
  lsn @0 :UInt64;
  op @1 :UInt8;       # 1 = PUT, 2 = DEL
  pageId @2 :UInt64;
  slotId @3 :UInt16;
  key @4 :Data;
  value @5 :Data;
}

struct WalBatch {
  requestId @0 :UInt64; # for idempotent retry; persisted in WAL
  startLsn @1 :UInt64;
  endLsn @2 :UInt64;
  records @3 :List(WalRecord);
}

struct TxnRecord {
  op @0 :UInt8;    # 1=PUT, 2=DEL, 3=COMMIT
  key @1 :Data;
  value @2 :Data;
}

interface Storage {
  # legacy page APIs (unchanged semantics)
  get @0 (key :UInt64) -> (found :Bool, value :Data, durableLsn :UInt64);
  put @1 (key :UInt64, value :Data) -> (durableLsn :UInt64);
  delete @2 (key :UInt64) -> (found :Bool, durableLsn :UInt64);
  batchPut @3 (items :List(KeyValue)) -> (durableLsn :UInt64);

  # WAL append (requestId persisted in batch header)
  appendWal @4 (batch :WalBatch) -> (durableLsn :UInt64);

  # MVCC / txn APIs
  getDurableLsn @5 () -> (durableLsn :UInt64);
  txnGet @6 (key :Data, readLsn :UInt64) -> (found :Bool, value :Data, durableLsn :UInt64);
  appendTxnBatch @7 (requestId :UInt64, records :List(TxnRecord)) -> (commitLsn :UInt64, durableLsn :UInt64);
}
