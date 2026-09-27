#!/usr/bin/env bash
set -euo pipefail

echo "=== 1/5 Check formatting ==="
cargo fmt --check

echo "=== 2/5 Clippy lints ==="
cargo clippy --all-targets --all-features -- -D warnings

echo "=== 3/5 Build ==="
cargo build

echo "=== 4/5 Run tests ==="
cargo test

echo "=== 5/5 Run bounded TLA+ checks ==="
./scripts/tla/check.sh

# ── Change-locality advisory ──────────────────────────────────────────
# Flag PRs that touch many top-level directories — a sign of coupling.
if git rev-parse --verify origin/main >/dev/null 2>&1; then
    CHANGED_DIRS=$(git diff --name-only origin/main 2>/dev/null | sed 's|/[^/]*$||' | sort -u | wc -l)
    if [ "$CHANGED_DIRS" -gt 8 ]; then
        echo ""
        echo "⚠️  This branch touches $CHANGED_DIRS directories against origin/main."
        echo "   Consider whether the change can be split into smaller, more focused PRs."
    fi
fi

echo ""
echo "✅ All CI checks passed!"
