#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <perf-baseline-jsonl> [report-md-path]" >&2
  exit 1
fi

in_file="$1"
out_file="${2:-${in_file%.jsonl}.md}"

cargo run --release --bin perf_report -- --in "$in_file" --out "$out_file"
echo "[perf] report: $out_file"
