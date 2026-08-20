#!/bin/bash
# RANGE + LIST + 2PC against the two PostgreSQL shards.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

if ! docker exec parsersql-pg-shard1 pg_isready -Upostgres &>/dev/null 2>&1; then
    echo "ERROR: PG shards not running. Start them with: ./scripts/start_pg_sharding_demo.sh"
    exit 1
fi

if [ ! -f ./sqlengine ]; then
    echo "Building sqlengine..."
    make build-sqlengine
fi

PG1='pgsql://postgres:test@127.0.0.1:16432/testdb?name=pg1'
PG2='pgsql://postgres:test@127.0.0.1:16433/testdb?name=pg2'
TXN_LOG="${TMPDIR:-/tmp}/parsersql-pg-demo.txn"

run_sql() {
    local desc="$1"
    local sql="$2"
    echo "----------------------------------------------"
    echo "QUERY: $desc"
    echo "SQL:   $sql"
    echo ""
    echo "$sql" | ./sqlengine \
        --backend "$PG1" \
        --backend "$PG2" \
        --shard "users:id:range:5=pg1,10=pg2" \
        --shard "regions:name:list:us-east=pg1,us-west=pg2" \
        --shard "orders:id:range:105=pg1,110=pg2" \
        --txn-log "$TXN_LOG" \
        2>&1
    echo ""
}

echo "=============================================="
echo "  PostgreSQL LIST + RANGE + 2PC demo"
echo "=============================================="
echo "  pg1 :16432  users 1-5 / us-east"
echo "  pg2 :16433  users 6-10 / us-west"
echo ""

run_sql "RANGE point lookup" \
    "SELECT name FROM users WHERE id = 3"

run_sql "RANGE BETWEEN prune" \
    "SELECT name FROM users WHERE id BETWEEN 6 AND 10"

run_sql "LIST point lookup" \
    "SELECT tz FROM regions WHERE name = 'us-west'"

run_sql "Scatter scan" \
    "SELECT COUNT(*) FROM users"

echo "=============================================="
echo "  2PC write across both shards (one engine)"
echo "=============================================="
{
    echo "BEGIN"
    echo "INSERT INTO users (id, name, age) VALUES (0, 'Zero', 1)"
    echo "INSERT INTO users (id, name, age) VALUES (11, 'Eleven', 2)"
    echo "COMMIT"
} | ./sqlengine \
    --backend "$PG1" \
    --backend "$PG2" \
    --shard "users:id:range:5=pg1,10=pg2" \
    --shard "regions:name:list:us-east=pg1,us-west=pg2" \
    --shard "orders:id:range:105=pg1,110=pg2" \
    --txn-log "$TXN_LOG" \
    2>&1
echo ""

run_sql "Read back 2PC inserts" \
    "SELECT id, name FROM users WHERE id IN (0, 11)"

echo "Demo complete. Stop: docker rm -f parsersql-pg-shard1 parsersql-pg-shard2"
