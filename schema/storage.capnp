@0x9f5b7c9a2b4d4f1c;

struct KeyValue {
  key @0 :UInt64;
  value @1 :Data;
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
}
