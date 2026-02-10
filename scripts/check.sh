#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v capnp >/dev/null 2>&1; then
  echo "[check] missing dependency: capnp"
  echo "Install Cap'n Proto compiler: https://capnproto.org/install.html"
  exit 1
fi

echo "[check] capnp version: $(capnp --version)"

echo "[check] cargo fmt --check"
cargo fmt -- --check

echo "[check] cargo test --lib --bins --tests (RUSTFLAGS=-D warnings)"
RUSTFLAGS='-D warnings' cargo test --lib --bins --tests
