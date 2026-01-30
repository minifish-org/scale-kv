@0x9f5b7c9a2b4d4f1c;

interface Storage {
  get @0 (key :Text) -> (found :Bool, value :Data);
  put @1 (key :Text, value :Data) -> ();
  delete @2 (key :Text) -> (found :Bool);
}
