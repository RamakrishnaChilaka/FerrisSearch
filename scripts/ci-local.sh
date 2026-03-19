#!/usr/bin/env bash
set -euo pipefail

echo "=== 1/4 Check formatting ==="
cargo fmt --check

echo "=== 2/4 Clippy lints ==="
cargo clippy --all-targets --all-features -- -D warnings

echo "=== 3/4 Build ==="
cargo build

echo "=== 4/4 Run tests ==="
cargo test

echo ""
echo "✅ All CI checks passed!"
