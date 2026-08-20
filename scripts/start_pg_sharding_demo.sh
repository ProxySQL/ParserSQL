#!/bin/bash
# Two PostgreSQL shards for LIST + RANGE + 2PC. Ports 16432/16433
# (15432 is the unit-test backend; 13306 is the MySQL sharding demo).
set -e

echo "=== Starting 2-shard PostgreSQL demo ==="

docker rm -f parsersql-pg-shard1 parsersql-pg-shard2 2>/dev/null || true

docker run -d --name parsersql-pg-shard1 \
    -p 16432:5432 \
    -e POSTGRES_PASSWORD=test \
    -e POSTGRES_DB=testdb \
    postgres:16 \
    -c max_prepared_transactions=16

docker run -d --name parsersql-pg-shard2 \
    -p 16433:5432 \
    -e POSTGRES_PASSWORD=test \
    -e POSTGRES_DB=testdb \
    postgres:16 \
    -c max_prepared_transactions=16

echo "Waiting for PG shard 1..."
until docker exec parsersql-pg-shard1 pg_isready -Upostgres &>/dev/null 2>&1; do sleep 1; done
echo "PG shard 1 ready"

echo "Waiting for PG shard 2..."
until docker exec parsersql-pg-shard2 pg_isready -Upostgres &>/dev/null 2>&1; do sleep 1; done
echo "PG shard 2 ready"

echo "Loading RANGE users 1-5 + LIST region us-east on shard 1..."
docker exec -i parsersql-pg-shard1 psql -Upostgres testdb <<'SQL'
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS regions;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT
);
CREATE TABLE regions (
    name VARCHAR(64) PRIMARY KEY,
    tz VARCHAR(32)
);
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    total NUMERIC(10,2)
);

INSERT INTO users VALUES
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Carol', 35),
    (4, 'Dave', 28),
    (5, 'Eve', 32);
INSERT INTO regions VALUES ('us-east', 'EST');
INSERT INTO orders VALUES (101, 1, 150.00), (102, 3, 50.00);
SQL

echo "Loading RANGE users 6-10 + LIST region us-west on shard 2..."
docker exec -i parsersql-pg-shard2 psql -Upostgres testdb <<'SQL'
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS regions;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT
);
CREATE TABLE regions (
    name VARCHAR(64) PRIMARY KEY,
    tz VARCHAR(32)
);
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    total NUMERIC(10,2)
);

INSERT INTO users VALUES
    (6, 'Frank', 40),
    (7, 'Grace', 22),
    (8, 'Hank', 31),
    (9, 'Ivy', 27),
    (10, 'Jack', 36);
INSERT INTO regions VALUES ('us-west', 'PST');
INSERT INTO orders VALUES (106, 6, 80.00), (107, 8, 120.00);
SQL

echo "PostgreSQL shards ready:"
echo "  pg1 127.0.0.1:16432  users 1-5, region us-east"
echo "  pg2 127.0.0.1:16433  users 6-10, region us-west"
echo "Run: ./scripts/run_pg_sharding_demo.sh"
echo "Stop: docker rm -f parsersql-pg-shard1 parsersql-pg-shard2"
