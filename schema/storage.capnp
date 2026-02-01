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
  startLsn @0 :UInt64;
  endLsn @1 :UInt64;
  records @2 :List(WalRecord);
}

interface Stream {
  next @0 (max :UInt32) -> (items :List(KeyValue), done :Bool);
}

interface Storage {
  get @0 (key :UInt64) -> (found :Bool, value :Data);
  put @1 (key :UInt64, value :Data) -> ();
  delete @2 (key :UInt64) -> (found :Bool);
  stream @3 () -> (stream :Stream);
  batchPut @4 (items :List(KeyValue)) -> ();
  appendWal @5 (batch :WalBatch) -> ();
}
