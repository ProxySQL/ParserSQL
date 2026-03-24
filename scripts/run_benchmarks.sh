#!/bin/bash
# run_benchmarks.sh — Build release, run benchmarks, run corpus tests, generate report
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPORT_FILE="${1:-benchmark_report.md}"
CORPUS_DIR="/tmp/sql_corpora"

cd "$PROJECT_DIR"

echo "=== Building release (-O3) ==="
sed 's/-g -O2/-O3/' Makefile.new > /tmp/Makefile.release
make -f /tmp/Makefile.release clean >/dev/null 2>&1
make -f /tmp/Makefile.release lib >/dev/null 2>&1
make -f /tmp/Makefile.release test 2>&1 | tail -1
make -f /tmp/Makefile.release build-corpus-test >/dev/null 2>&1

echo "=== Running benchmarks ==="
make -f /tmp/Makefile.release bench 2>&1 | grep "^BM_" > /tmp/bench_results.txt
./run_bench --benchmark_format=json > /tmp/bench_results.json 2>/dev/null || true

echo "=== Downloading corpus data (if needed) ==="
mkdir -p "$CORPUS_DIR"

download_if_missing() {
    local name="$1" dir="$2" url="$3" sparse="$4"
    if [ ! -d "$dir" ]; then
        echo "  Downloading $name..."
        if [ -n "$sparse" ]; then
            git clone --depth 1 --sparse -q "$url" "$dir"
            (cd "$dir" && git sparse-checkout set $sparse 2>/dev/null)
        else
            git clone --depth 1 -q "$url" "$dir"
        fi
    fi
}

download_if_missing "SQLGlot" "$CORPUS_DIR/sqlglot" \
    "https://github.com/tobymao/sqlglot.git"

download_if_missing "CockroachDB" "$CORPUS_DIR/cockroach" \
    "https://github.com/cockroachdb/cockroach.git" \
    "pkg/sql/parser/testdata"

download_if_missing "TPC-H" "$CORPUS_DIR/pg_tpch" \
    "https://github.com/tvondra/pg_tpch.git"

download_if_missing "PostgreSQL regression" "$CORPUS_DIR/postgres" \
    "https://github.com/postgres/postgres.git" \
    "src/test/regress/sql"

download_if_missing "Vitess" "$CORPUS_DIR/vitess" \
    "https://github.com/vitessio/vitess.git" \
    "go/vt/sqlparser"

download_if_missing "TiDB" "$CORPUS_DIR/tidb" \
    "https://github.com/pingcap/tidb.git" \
    "pkg/parser"

download_if_missing "sqlparser-rs" "$CORPUS_DIR/sqlparser-rs" \
    "https://github.com/apache/datafusion-sqlparser-rs.git"

download_if_missing "MySQL Server" "$CORPUS_DIR/mysql_server" \
    "https://github.com/mysql/mysql-server.git" \
    "mysql-test/t"

echo "=== Preparing corpus files ==="

# 1. PostgreSQL regression suite — plain .sql files, semicolon-separated
echo "  Preparing PostgreSQL regression..."
cat "$CORPUS_DIR/postgres/src/test/regress/sql/"*.sql 2>/dev/null | \
    sed 's/--.*$//' | tr '\n' ' ' | sed 's/;/;\n/g' | \
    grep -v '^\s*$' | sed 's/^\s*//' > /tmp/corpus_pg_regress.sql 2>/dev/null || true

# 2. MySQL MTR test suite — filter MTR directives from .test files
echo "  Preparing MySQL MTR..."
cat "$CORPUS_DIR/mysql_server/mysql-test/t/"select*.test \
    "$CORPUS_DIR/mysql_server/mysql-test/t/"insert*.test \
    "$CORPUS_DIR/mysql_server/mysql-test/t/"update*.test \
    "$CORPUS_DIR/mysql_server/mysql-test/t/"delete*.test \
    "$CORPUS_DIR/mysql_server/mysql-test/t/"set*.test 2>/dev/null | \
    grep -v '^--' | grep -v '^#' | grep -v '^\s*$' | \
    grep -v '^source' | grep -v '^echo' | grep -v '^error' | grep -v '^let' | \
    grep -v '^eval' | grep -v '^if' | grep -v '^while' | grep -v '^enable' | \
    grep -v '^disable' | grep -v '^connect' | grep -v '^disconnect' | \
    grep -v '^send' | grep -v '^reap' | grep -v '^sleep' | grep -v '^replace' | \
    grep -v '^remove' | grep -v '^write' | grep -v '^copy' | grep -v '^perl' | \
    grep -v '^end' | grep -v '^dec' | grep -v '^inc' | grep -v '^die' | \
    grep -v '^result' | grep -v '^sorted' | grep -v '^vertical' | \
    sed 's/;.*/;/' | \
    grep -iE '^(SELECT|INSERT|UPDATE|DELETE|SET|CREATE|ALTER|DROP|EXPLAIN|SHOW|USE|BEGIN|COMMIT|ROLLBACK|REPLACE|CALL|LOAD|TRUNCATE|GRANT|REVOKE|LOCK|UNLOCK|PREPARE|EXECUTE|DEALLOCATE|DO|DESCRIBE|DESC)' \
    > /tmp/corpus_mysql_mtr.sql 2>/dev/null || true

# 3. SQLGlot — one query per line across fixture files
echo "  Preparing SQLGlot..."
cat "$CORPUS_DIR/sqlglot/tests/fixtures/identity.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/tpch.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/tpcds.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/optimizer/tpch.sql" \
    "$CORPUS_DIR/sqlglot/tests/fixtures/pretty.sql" 2>/dev/null | \
    grep -v '^$' | grep -v '^--' > /tmp/corpus_sqlglot.sql 2>/dev/null || true

# 4. CockroachDB — extract SQL lines from testdata
echo "  Preparing CockroachDB..."
grep -rh "^[A-Z]" "$CORPUS_DIR/cockroach/pkg/sql/parser/testdata/" 2>/dev/null | \
    grep -v "^--" | grep -v "^#" | grep -v "^$" > /tmp/corpus_cockroach.sql 2>/dev/null || true

# 5. TPC-H — multi-line SQL queries need careful extraction
echo "  Preparing TPC-H..."
for f in "$CORPUS_DIR/pg_tpch/queries/"*.sql; do
    [ -f "$f" ] || continue
    # Read entire file, strip comments, join lines, split on ;
    sed 's/--.*$//' "$f" | tr '\n' ' ' | sed 's/;/;\n/g' | \
        grep -v '^\s*$' | sed 's/^\s*//'
done > /tmp/corpus_tpch_pgsql.sql 2>/dev/null || true
# TPC-H works as MySQL too
cp /tmp/corpus_tpch_pgsql.sql /tmp/corpus_tpch_mysql.sql 2>/dev/null || true

# 6. sqlparser-rs — extract SQL from Rust test files
echo "  Preparing sqlparser-rs..."
# MySQL tests
grep -ohP '"((?:SELECT|INSERT|UPDATE|DELETE|SET|CREATE|ALTER|DROP|EXPLAIN|WITH|CALL|REPLACE|SHOW|GRANT|TRUNCATE|USE|BEGIN|COMMIT)[^"]*)"' \
    "$CORPUS_DIR/sqlparser-rs/tests/sqlparser_mysql.rs" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' | sed 's/\\"/"/g' > /tmp/corpus_sqlparserrs_mysql.sql 2>/dev/null || true
# PostgreSQL tests
grep -ohP '"((?:SELECT|INSERT|UPDATE|DELETE|SET|CREATE|ALTER|DROP|EXPLAIN|WITH|CALL|REPLACE|SHOW|GRANT|TRUNCATE|USE|BEGIN|COMMIT)[^"]*)"' \
    "$CORPUS_DIR/sqlparser-rs/tests/sqlparser_postgres.rs" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' | sed 's/\\"/"/g' > /tmp/corpus_sqlparserrs_pgsql.sql 2>/dev/null || true
# Common tests (test as both dialects)
grep -ohP '"((?:SELECT|INSERT|UPDATE|DELETE|SET|CREATE|ALTER|DROP|EXPLAIN|WITH|CALL|REPLACE|SHOW|GRANT|TRUNCATE|USE|BEGIN|COMMIT)[^"]*)"' \
    "$CORPUS_DIR/sqlparser-rs/tests/sqlparser_common.rs" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' | sed 's/\\"/"/g' > /tmp/corpus_sqlparserrs_common.sql 2>/dev/null || true

# 7. Vitess — extract SQL from Go test
echo "  Preparing Vitess..."
grep -ohP '"((?:select|SELECT|insert|INSERT|update|UPDATE|delete|DELETE|set|SET|create|CREATE|alter|ALTER|drop|DROP|explain|EXPLAIN)[^"]*)"' \
    "$CORPUS_DIR/vitess/go/vt/sqlparser/parse_test.go" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' > /tmp/corpus_vitess.sql 2>/dev/null || true

# 8. TiDB — extract SQL from Go test
echo "  Preparing TiDB..."
grep -ohP '"((?:select|SELECT|insert|INSERT|update|UPDATE|delete|DELETE|set|SET|create|CREATE|alter|ALTER|drop|DROP)[^"]*)"' \
    "$CORPUS_DIR/tidb/pkg/parser/parser_test.go" 2>/dev/null | \
    sed 's/^"//' | sed 's/"$//' > /tmp/corpus_tidb.sql 2>/dev/null || true

echo "=== Running corpus tests ==="

run_corpus() {
    local name="$1" dialect="$2" file="$3"
    if [ -f "$file" ] && [ -s "$file" ]; then
        local result
        result=$(./corpus_test "$dialect" < "$file" 2>/dev/null)
        local total=$(echo "$result" | grep "^Total" | grep -oP '\d+')
        local ok=$(echo "$result" | grep "^OK:" | grep -oP '\d+(?= \()')
        local partial=$(echo "$result" | grep "^PARTIAL:" | grep -oP '\d+(?= \()')
        local error=$(echo "$result" | grep "^ERROR:" | grep -oP '\d+(?= \()')
        local ok_pct=$(echo "$result" | grep "^OK:" | grep -oP '\([\d.]+%' | tr -d '(')
        echo "  $name ($dialect): $total queries — ${ok:-0} OK (${ok_pct:-0%}), ${partial:-0} PARTIAL, ${error:-0} ERROR" >&2
        echo "| $name | $dialect | ${total:-0} | ${ok:-0} (${ok_pct:-0%}) | ${partial:-0} | ${error:-0} |"
    fi
}

CORPUS_RESULTS=""
CORPUS_RESULTS+=$(run_corpus "PostgreSQL regression"  "pgsql" "/tmp/corpus_pg_regress.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "MySQL MTR"              "mysql" "/tmp/corpus_mysql_mtr.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "CockroachDB"            "pgsql" "/tmp/corpus_cockroach.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "SQLGlot"                "mysql" "/tmp/corpus_sqlglot.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "TPC-H"                  "pgsql" "/tmp/corpus_tpch_pgsql.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "TPC-H"                  "mysql" "/tmp/corpus_tpch_mysql.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "sqlparser-rs MySQL"      "mysql" "/tmp/corpus_sqlparserrs_mysql.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "sqlparser-rs PostgreSQL"  "pgsql" "/tmp/corpus_sqlparserrs_pgsql.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "sqlparser-rs Common"      "mysql" "/tmp/corpus_sqlparserrs_common.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "Vitess"                  "mysql" "/tmp/corpus_vitess.sql")$'\n'
CORPUS_RESULTS+=$(run_corpus "TiDB"                    "mysql" "/tmp/corpus_tidb.sql")$'\n'

# Compute totals
TOTAL_QUERIES=0
TOTAL_OK=0
TOTAL_PARTIAL=0
TOTAL_ERROR=0
while IFS='|' read -r _ _ _ queries ok partial error _; do
    q=$(echo "$queries" | tr -dc '0-9')
    o=$(echo "$ok" | grep -oP '^\s*\d+' | tr -d ' ')
    p=$(echo "$partial" | tr -dc '0-9')
    e=$(echo "$error" | tr -dc '0-9')
    TOTAL_QUERIES=$((TOTAL_QUERIES + ${q:-0}))
    TOTAL_OK=$((TOTAL_OK + ${o:-0}))
    TOTAL_PARTIAL=$((TOTAL_PARTIAL + ${p:-0}))
    TOTAL_ERROR=$((TOTAL_ERROR + ${e:-0}))
done <<< "$CORPUS_RESULTS"

echo "=== Generating report ==="

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
$CORPUS_RESULTS| **TOTAL** | | **$TOTAL_QUERIES** | **$TOTAL_OK** | **$TOTAL_PARTIAL** | **$TOTAL_ERROR** |

---

*Generated by \`scripts/run_benchmarks.sh\`*
REPORT

echo ""
echo "Report written to: $REPORT_FILE"
cat "$REPORT_FILE"
