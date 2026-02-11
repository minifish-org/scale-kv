#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

addr="${ADDR:-127.0.0.1:50051}"
data_dir="${DATA_DIR:-$repo_root/data/perf-baseline}"
records="${RECORDS:-2000}"
ops="${OPS:-4000}"
out_dir="${OUT_DIR:-$repo_root/artifacts}"
mkdir -p "$out_dir"
ts="$(date +%Y%m%d-%H%M%S)"
out_file="$out_dir/perf-baseline-$ts.jsonl"

cleanup() {
  if [[ -n "${server_pid:-}" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "[perf] starting storage_server at $addr"
rm -rf "$data_dir"
mkdir -p "$data_dir"

cargo run --release --bin storage_server -- \
  --addr "$addr" \
  --dir "$data_dir" \
  --metrics-interval-secs 0 \
  >"$out_dir/storage-server-$ts.log" 2>&1 &
server_pid=$!

sleep 2

echo "[perf] writing results to $out_file"
echo "# perf baseline $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$out_file"

for ratio in 100 80 50 20 0; do
  for conc in 1 4 16 32; do
    echo "[perf] run read_ratio=$ratio concurrency=$conc"
    cargo run --release --bin workload_bench -- \
      --addr "$addr" \
      --records "$records" \
      --ops "$ops" \
      --concurrency "$conc" \
      --read-ratio "$ratio" \
      >>"$out_file"
  done
done

echo "[perf] done: $out_file"
