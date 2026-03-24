#!/bin/bash
# run_benchmarks.sh — Build release, run benchmarks, generate report
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPORT_FILE="${1:-benchmark_report.md}"
CORPUS_DIR="/tmp/sql_corpora"

cd "$PROJECT_DIR"

echo "=== Building release (-O3) ==="
# Create a release makefile (copy Makefile.new, swap -g -O2 for -O3)
sed 's/-g -O2/-O3/' Makefile.new > /tmp/Makefile.release
make -f /tmp/Makefile.release clean >/dev/null 2>&1
make -f /tmp/Makefile.release lib >/dev/null 2>&1
make -f /tmp/Makefile.release test 2>&1 | tail -1  # verify tests pass
make -f /tmp/Makefile.release build-corpus-test >/dev/null 2>&1

echo "=== Running benchmarks ==="
make -f /tmp/Makefile.release bench 2>&1 | grep "^BM_" > /tmp/bench_results.txt

# Also run with JSON output for machine parsing
./run_bench --benchmark_format=json > /tmp/bench_results.json 2>/dev/null || true

echo "=== Downloading corpus data (if needed) ==="
# Download corpora if not already present
mkdir -p "$CORPUS_DIR"

if [ ! -d "$CORPUS_DIR/sqlglot" ]; then
    echo "Downloading SQLGlot..."
    git clone --depth 1 -q https://github.com/tobymao/sqlglot.git "$CORPUS_DIR/sqlglot"
fi

if [ ! -d "$CORPUS_DIR/cockroach" ]; then
    echo "Downloading CockroachDB testdata..."
    git clone --depth 1 --sparse -q https://github.com/cockroachdb/cockroach.git "$CORPUS_DIR/cockroach"
    cd "$CORPUS_DIR/cockroach" && git sparse-checkout set pkg/sql/parser/testdata 2>/dev/null && cd "$PROJECT_DIR"
fi

if [ ! -d "$CORPUS_DIR/pg_tpch" ]; then
    echo "Downloading TPC-H..."
    git clone --depth 1 -q https://github.com/tvondra/pg_tpch.git "$CORPUS_DIR/pg_tpch"
fi

if [ ! -d "$CORPUS_DIR/postgres" ]; then
    echo "Downloading PostgreSQL regression suite..."
    git clone --depth 1 --sparse -q https://github.com/postgres/postgres.git "$CORPUS_DIR/postgres"
    cd "$CORPUS_DIR/postgres" && git sparse-checkout set src/test/regress/sql 2>/dev/null && cd "$PROJECT_DIR"
fi

if [ ! -d "$CORPUS_DIR/vitess" ]; then
    echo "Downloading Vitess..."
    git clone --depth 1 --sparse -q https://github.com/vitessio/vitess.git "$CORPUS_DIR/vitess"
    cd "$CORPUS_DIR/vitess" && git sparse-checkout set go/vt/sqlparser 2>/dev/null && cd "$PROJECT_DIR"
fi

if [ ! -d "$CORPUS_DIR/tidb" ]; then
    echo "Downloading TiDB parser..."
    git clone --depth 1 --sparse -q https://github.com/pingcap/tidb.git "$CORPUS_DIR/tidb"
    cd "$CORPUS_DIR/tidb" && git sparse-checkout set pkg/parser 2>/dev/null && cd "$PROJECT_DIR"
fi

echo "=== Preparing corpus files ==="

# SQLGlot - one query per line
cat "$CORPUS_DIR/sqlglot/tests/fixtures/identity.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/tpch.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/tpcds.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/optimizer/tpch.sql" 2>/dev/null | \
    grep -v '^$' | grep -v '^--' > /tmp/corpus_sqlglot.sql 2>/dev/null || true

# CockroachDB - extract SQL lines
grep -h "^[A-Z]" "$CORPUS_DIR/cockroach/pkg/sql/parser/testdata/"* 2>/dev/null | \
    grep -v "^--" | grep -v "^#" | grep -v "^$" | head -5000 > /tmp/corpus_cockroach.sql 2>/dev/null || true

# TPC-H
cat "$CORPUS_DIR/pg_tpch/queries/"*.sql 2>/dev/null | \
    sed 's/--.*$//' | tr '\n' ' ' | sed 's/;/;\n/g' | \
    grep -v '^\s*$' | sed 's/^\s*//' > /tmp/corpus_tpch.sql 2>/dev/null || true

# PostgreSQL regression
cat "$CORPUS_DIR/postgres/src/test/regress/sql/"*.sql 2>/dev/null | \
    sed 's/--.*$//' | tr '\n' ' ' | sed 's/;/;\n/g' | \
    grep -v '^\s*$' | sed 's/^\s*//' > /tmp/corpus_pg_regress.sql 2>/dev/null || true

# Vitess
grep -ohP '"((?:select|SELECT|insert|INSERT|update|UPDATE|delete|DELETE|set|SET|create|CREATE|alter|ALTER|drop|DROP|explain|EXPLAIN)[^"]*)"' \
    "$CORPUS_DIR/vitess/go/vt/sqlparser/parse_test.go" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' | head -2000 > /tmp/corpus_vitess.sql 2>/dev/null || true

# TiDB
grep -ohP '"((?:select|SELECT|insert|INSERT|update|UPDATE|delete|DELETE|set|SET|create|CREATE)[^"]*)"' \
    "$CORPUS_DIR/tidb/pkg/parser/parser_test.go" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' | head -3000 > /tmp/corpus_tidb.sql 2>/dev/null || true

echo "=== Running corpus tests ==="

run_corpus() {
    local name="$1"
    local dialect="$2"
    local file="$3"
    if [ -f "$file" ] && [ -s "$file" ]; then
        local count=$(wc -l < "$file")
        local result=$(./corpus_test "$dialect" < "$file" 2>/dev/null)
        local ok=$(echo "$result" | grep "^OK:" | sed 's/^OK:\s*//' | grep -oP '^\d+')
        local partial=$(echo "$result" | grep "^PARTIAL:" | sed 's/^PARTIAL:\s*//' | grep -oP '^\d+')
        local error=$(echo "$result" | grep "^ERROR:" | sed 's/^ERROR:\s*//' | grep -oP '^\d+')
        local ok_pct=$(echo "$result" | grep "^OK:" | grep -oP '\([\d.]+%' | tr -d '(')
        echo "| $name | $dialect | $count | ${ok:-0} (${ok_pct:-0%}) | ${partial:-0} | ${error:-0} |"
    fi
}

# Collect corpus results
CORPUS_RESULTS=""
CORPUS_RESULTS+=$(run_corpus "PostgreSQL regression" "pgsql" "/tmp/corpus_pg_regress.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "SQLGlot" "mysql" "/tmp/corpus_sqlglot.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "CockroachDB" "pgsql" "/tmp/corpus_cockroach.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "TPC-H" "pgsql" "/tmp/corpus_tpch.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "Vitess" "mysql" "/tmp/corpus_vitess.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "TiDB" "mysql" "/tmp/corpus_tidb.sql")$'\n'

echo "=== Generating report ==="

# Get system info
HOSTNAME=$(hostname)
CPU=$(lscpu 2>/dev/null | grep "Model name" | sed 's/Model name:\s*//' || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "Unknown")
OS=$(uname -sr)
COMPILER=$(g++ --version | head -1)
GIT_SHA=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
DATE=$(date -u +"%Y-%m-%d %H:%M UTC")
TEST_COUNT=$(make -f /tmp/Makefile.release test 2>&1 | grep "PASSED" | grep -oP '\d+' | head -1)

cat > "$REPORT_FILE" << REPORT
# SQL Parser Performance Report

**Date:** $DATE
**Host:** $HOSTNAME
**CPU:** $CPU
**OS:** $OS
**Compiler:** $COMPILER
**Git:** $GIT_BRANCH @ $GIT_SHA
**Unit tests:** $TEST_COUNT passing

---

## Benchmark Results (Release -O3)

| Operation | Latency | Target | Status |
|---|---|---|---|
$(while IFS= read -r line; do
    name=$(echo "$line" | awk '{print $1}')
    time=$(echo "$line" | awk '{print $2}')
    unit=$(echo "$line" | awk '{print $3}')

    # Map benchmark names to targets
    target=""
    case "$name" in
        BM_Classify_Begin)     target="<100ns" ;;
        BM_Classify_*)         target="<500ns" ;;
        BM_Set_Simple)         target="<300ns" ;;
        BM_Set_Names)          target="<300ns" ;;
        BM_Set_MultiVar)       target="<300ns" ;;
        BM_Set_FunctionRHS)    target="<300ns" ;;
        BM_Select_Simple)      target="<500ns" ;;
        BM_Select_MultiColumn) target="<500ns" ;;
        BM_Select_Join)        target="<2us" ;;
        BM_Select_Complex)     target="<2us" ;;
        BM_Select_MultiJoin)   target="<2us" ;;
        BM_Emit_*)             target="<500ns" ;;
        BM_ArenaReset)         target="<10ns" ;;
        BM_PgSQL_*)            target="—" ;;
    esac

    # Determine met/not met
    status="MET"
    if [ -n "$target" ] && [ "$target" != "—" ]; then
        target_ns=$(echo "$target" | tr -dc '0-9')
        if echo "$target" | grep -q "us"; then
            target_ns=$((target_ns * 1000))
        fi
        time_ns=$(printf "%.0f" "$time")
        if [ "$time_ns" -gt "$target_ns" ] 2>/dev/null; then
            status="MISSED"
        fi
    else
        status="—"
    fi

    echo "| $name | ${time} ${unit} | ${target:-—} | $status |"
done < /tmp/bench_results.txt)

---

## Corpus Test Results

| Corpus | Dialect | Queries | OK | PARTIAL | ERROR |
|---|---|---|---|---|---|
$CORPUS_RESULTS

---

*Generated by \`scripts/run_benchmarks.sh\`*
REPORT

echo "Report written to: $REPORT_FILE"
echo ""
cat "$REPORT_FILE"
