#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[check] cargo fmt --check"
cargo fmt -- --check

echo "[check] cargo test --lib --bins --tests (RUSTFLAGS=-D warnings)"
RUSTFLAGS='-D warnings' cargo test --lib --bins --tests
