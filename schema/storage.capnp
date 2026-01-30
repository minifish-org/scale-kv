@0x9f5b7c9a2b4d4f1c;

struct KeyValue {
  key @0 :Text;
  value @1 :Data;
}

interface Stream {
  next @0 (max :UInt32) -> (items :List(KeyValue), done :Bool);
}

interface Storage {
  get @0 (key :Text) -> (found :Bool, value :Data);
  put @1 (key :Text, value :Data) -> ();
  delete @2 (key :Text) -> (found :Bool);
  stream @3 () -> (stream :Stream);
  batchPut @4 (items :List(KeyValue)) -> ();
}
