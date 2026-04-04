#!/bin/bash
# Start a 2-shard MySQL demo environment
set -e

echo "=== Starting 2-shard MySQL demo ==="

# Shard 1: users with id 1-5, orders for those users
docker run -d --name parsersql-shard1 \
    -p 13306:3306 \
    -e MYSQL_ROOT_PASSWORD=test \
    -e MYSQL_DATABASE=testdb \
    mysql:8.0 2>/dev/null || true

# Shard 2: users with id 6-10, orders for those users
docker run -d --name parsersql-shard2 \
    -p 13307:3306 \
    -e MYSQL_ROOT_PASSWORD=test \
    -e MYSQL_DATABASE=testdb \
    mysql:8.0 2>/dev/null || true

echo "Waiting for Shard 1..."
until docker exec parsersql-shard1 mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; do sleep 1; done
echo "Shard 1 ready"

echo "Waiting for Shard 2..."
until docker exec parsersql-shard2 mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; do sleep 1; done
echo "Shard 2 ready"

echo "Loading data into Shard 1 (users 1-5)..."
docker exec -i parsersql-shard1 mysql -uroot -ptest testdb <<'SQL'
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

INSERT INTO users VALUES
    (1, 'Alice',   30, 'Engineering', 95000.00),
    (2, 'Bob',     25, 'Sales',       65000.00),
    (3, 'Carol',   35, 'Engineering', 110000.00),
    (4, 'Dave',    28, 'Marketing',   70000.00),
    (5, 'Eve',     32, 'Engineering', 105000.00);

INSERT INTO orders VALUES
    (101, 1, 150.00, 'completed',  '2024-01-15'),
    (102, 2,  75.50, 'pending',    '2024-02-20'),
    (103, 1, 200.00, 'completed',  '2024-03-10'),
    (104, 3,  50.00, 'cancelled',  '2024-01-25'),
    (105, 5, 300.00, 'completed',  '2024-04-05');
SQL

echo "Loading data into Shard 2 (users 6-10)..."
docker exec -i parsersql-shard2 mysql -uroot -ptest testdb <<'SQL'
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

INSERT INTO users VALUES
    (6,  'Frank',  45, 'Engineering', 130000.00),
    (7,  'Grace',  29, 'Sales',        68000.00),
    (8,  'Heidi',  38, 'Marketing',    85000.00),
    (9,  'Ivan',   27, 'Engineering',  90000.00),
    (10, 'Judy',   33, 'Sales',        72000.00);

INSERT INTO orders VALUES
    (106, 6,  500.00, 'completed',  '2024-02-01'),
    (107, 7,  125.00, 'pending',    '2024-03-15'),
    (108, 8,  250.00, 'completed',  '2024-01-30'),
    (109, 9,   80.00, 'completed',  '2024-04-10'),
    (110, 10, 175.00, 'cancelled',  '2024-02-28');
SQL

echo ""
echo "=== Sharding Demo Ready ==="
echo ""
echo "Shard 1 (port 13306): users 1-5  (Alice, Bob, Carol, Dave, Eve)"
echo "Shard 2 (port 13307): users 6-10 (Frank, Grace, Heidi, Ivan, Judy)"
echo ""
echo "Each shard has 5 users and 5 orders."
echo "Total: 10 users, 10 orders across 2 shards."
echo ""
echo "To run the demo:"
echo '  ./scripts/run_sharding_demo.sh'
echo ""
echo "To stop:"
echo '  docker rm -f parsersql-shard1 parsersql-shard2'
