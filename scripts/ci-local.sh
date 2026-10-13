#!/usr/bin/env bash
set -euo pipefail

echo "=== 1/8 Check OpenSearch compatibility inventory ==="
python3 scripts/check_opensearch_compatibility.py

echo "=== 2/8 Check formatting ==="
cargo fmt --check

echo "=== 3/8 Clippy lints ==="
cargo clippy --all-targets --all-features -- -D warnings

echo "=== 4/8 Build ==="
cargo build

echo "=== 5/8 Run tests ==="
cargo test

echo "=== 6/8 Build write fault-test harnesses ==="
cargo test --features protocol-trace --lib --test d1_protocol_trace --test stale_primary_failover --no-run

echo "=== 7/8 Validate write fault evidence ==="
./scripts/tla/test_d1_protocol_trace_ci.sh

echo "=== 8/8 Run bounded TLA+ checks ==="
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
