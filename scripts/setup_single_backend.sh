#!/bin/bash
# Setup a single MySQL backend with ALL data (for baseline comparison)
#
# This creates a single-server MySQL instance with the same 10 users
# and 10 orders that the 2-shard setup distributes. Used to measure
# the overhead of distributed query execution vs single-server.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Setting up single MySQL backend (baseline) ==="

# Single backend on port 13308
docker run -d --name parsersql-single \
    -p 13308:3306 \
    -e MYSQL_ROOT_PASSWORD=test \
    -e MYSQL_DATABASE=testdb \
    mysql:8.0 2>/dev/null || true

echo "Waiting for MySQL to be ready..."
until docker exec parsersql-single mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; do
    sleep 1
done
echo "MySQL ready"

echo "Loading ALL data into single backend..."
docker exec -i parsersql-single mysql -uroot -ptest testdb <<'SQL'
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT,
    dept VARCHAR(100),
    salary DECIMAL(10,2)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    total DECIMAL(10,2),
    status VARCHAR(50),
    created_at DATE
);

-- All 10 users (same as shard1 + shard2 combined)
INSERT INTO users VALUES
    (1,  'Alice',  30, 'Engineering', 95000.00),
    (2,  'Bob',    25, 'Sales',       65000.00),
    (3,  'Carol',  35, 'Engineering', 110000.00),
    (4,  'Dave',   28, 'Marketing',   70000.00),
    (5,  'Eve',    32, 'Engineering', 105000.00),
    (6,  'Frank',  45, 'Engineering', 130000.00),
    (7,  'Grace',  29, 'Sales',       68000.00),
    (8,  'Heidi',  38, 'Marketing',   85000.00),
    (9,  'Ivan',   27, 'Engineering',  90000.00),
    (10, 'Judy',   33, 'Sales',       72000.00);

-- All 10 orders
INSERT INTO orders VALUES
    (101, 1,  150.00, 'completed',  '2024-01-15'),
    (102, 2,   75.50, 'pending',    '2024-02-20'),
    (103, 1,  200.00, 'completed',  '2024-03-10'),
    (104, 3,   50.00, 'cancelled',  '2024-01-25'),
    (105, 5,  300.00, 'completed',  '2024-04-05'),
    (106, 6,  500.00, 'completed',  '2024-02-01'),
    (107, 7,  125.00, 'pending',    '2024-03-15'),
    (108, 8,  250.00, 'completed',  '2024-01-30'),
    (109, 9,   80.00, 'completed',  '2024-04-10'),
    (110, 10, 175.00, 'cancelled',  '2024-02-28');
SQL

echo ""
echo "=== Single Backend Ready ==="
echo ""
echo "Port 13308: all 10 users + 10 orders"
echo ""
echo "To run baseline benchmark:"
echo "  ./bench_distributed \\"
echo "      --backend \"mysql://root:test@127.0.0.1:13308/testdb?name=single\" \\"
echo "      --shard \"users:id:single\" \\"
echo "      --shard \"orders:id:single\""
echo ""
echo "To stop: docker rm -f parsersql-single"
