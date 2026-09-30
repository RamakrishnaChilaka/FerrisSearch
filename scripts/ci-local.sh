#!/usr/bin/env bash
set -euo pipefail

echo "=== 1/7 Check formatting ==="
cargo fmt --check

echo "=== 2/7 Clippy lints ==="
cargo clippy --all-targets --all-features -- -D warnings

echo "=== 3/7 Build ==="
cargo build

echo "=== 4/7 Run tests ==="
cargo test

echo "=== 5/7 Build D1 protocol trace harness ==="
cargo test --features protocol-trace --test d1_protocol_trace --no-run

echo "=== 6/7 Validate seeded D1 protocol traces ==="
./scripts/tla/test_d1_protocol_trace_ci.sh

echo "=== 7/7 Run bounded TLA+ checks ==="
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
