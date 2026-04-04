#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "=============================================="
echo "  SQL Parser Comparison Benchmark"
echo "=============================================="
echo ""

# Build everything with -O3
echo "=== Building release ==="
sed 's/-g -O2/-O3/' Makefile > /tmp/Makefile.release
make -f /tmp/Makefile.release clean >/dev/null 2>&1
make -f /tmp/Makefile.release lib >/dev/null 2>&1

# Build libpg_query if available
if [ -f third_party/libpg_query/Makefile ]; then
    echo "=== Building libpg_query ==="
    (cd third_party/libpg_query && make -j$(nproc) >/dev/null 2>&1) || echo "libpg_query build failed (optional)"
fi

# Run our parser vs libpg_query
if [ -f third_party/libpg_query/libpg_query.a ]; then
    echo ""
    echo "=== ParserSQL vs libpg_query (PostgreSQL's parser) ==="
    echo ""
    make -f /tmp/Makefile.release bench-compare 2>&1 | grep "^BM_" | column -t
else
    echo "libpg_query not built, skipping comparison"
fi

# Run sqlparser-rs benchmark if Rust is available
if command -v cargo &>/dev/null && [ -d bench/sqlparser_rs_bench ]; then
    echo ""
    echo "=== sqlparser-rs (Rust) ==="
    echo ""
    (cd bench/sqlparser_rs_bench && cargo bench 2>&1 | grep "time:" | head -20)
else
    echo ""
    echo "Rust/cargo not available, skipping sqlparser-rs benchmark"
fi
