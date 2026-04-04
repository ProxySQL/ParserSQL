#!/bin/bash
# Run distributed query demo against 2 MySQL shards
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Check if shards are running
if ! docker exec parsersql-shard1 mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; then
    echo "ERROR: Shards not running. Start them with: ./scripts/start_sharding_demo.sh"
    exit 1
fi

# Build if needed
if [ ! -f ./sqlengine ]; then
    echo "Building sqlengine..."
    make sqlengine 2>/dev/null
fi

echo "=============================================="
echo "  Distributed SQL Engine - Sharding Demo"
echo "=============================================="
echo ""
echo "Setup: 10 users + 10 orders split across 2 MySQL shards"
echo "  Shard 1 (port 13306): users 1-5 + their orders"
echo "  Shard 2 (port 13307): users 6-10 + their orders"
echo ""
echo "The engine parses SQL, determines which shards to query,"
echo "fetches data from both, and merges results locally."
echo ""

run_query() {
    local desc="$1"
    local sql="$2"
    echo "----------------------------------------------"
    echo "QUERY: $desc"
    echo "SQL:   $sql"
    echo ""
    echo "$sql" | ./sqlengine \
        --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
        --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
        --shard "users:id:shard1,shard2" \
        --shard "orders:id:shard1,shard2" \
        2>&1
    echo ""
}

echo "=============================================="
echo "  1. Scan all rows from both shards"
echo "=============================================="
run_query "All users across both shards" \
    "SELECT * FROM users"

echo "=============================================="
echo "  2. Filter pushdown to shards"
echo "=============================================="
run_query "Engineers only (filter pushed to both shards)" \
    "SELECT name, age, salary FROM users WHERE dept = 'Engineering'"

echo "=============================================="
echo "  3. Distributed aggregation"
echo "=============================================="
run_query "Count + average salary by department (merged from 2 shards)" \
    "SELECT dept, COUNT(*) FROM users GROUP BY dept"

echo "=============================================="
echo "  4. Distributed sort + limit"
echo "=============================================="
run_query "Top 3 highest paid (merge-sort across shards)" \
    "SELECT name, salary FROM users ORDER BY salary DESC LIMIT 3"

echo "=============================================="
echo "  5. Cross-shard join"
echo "=============================================="
run_query "Join users and orders (both fetched from shards, joined locally)" \
    "SELECT u.name, o.total, o.status FROM users u JOIN orders o ON u.id = o.user_id"

echo "=============================================="
echo "  6. Expression evaluation"
echo "=============================================="
run_query "Pure expression (no backend needed)" \
    "SELECT 1 + 2, UPPER('distributed'), COALESCE(NULL, 'sql'), 42 * 3"

echo "=============================================="
echo "  7. Subquery"
echo "=============================================="
run_query "Subquery: users with above-average age" \
    "SELECT name, age FROM users WHERE age > (SELECT AVG(age) FROM users)"

echo "=============================================="
echo "  Demo Complete!"
echo "=============================================="
echo ""
echo "To stop the shards: docker rm -f parsersql-shard1 parsersql-shard2"
