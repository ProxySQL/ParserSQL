#!/bin/bash
set -e

echo "Starting test backends..."

# Remove any existing containers
docker rm -f parsersql-test-mysql parsersql-test-pgsql 2>/dev/null || true

# Start MySQL 8
docker run -d --name parsersql-test-mysql \
    -p 13306:3306 \
    -e MYSQL_ROOT_PASSWORD=test \
    -e MYSQL_DATABASE=testdb \
    mysql:8.0

# Start PostgreSQL 16
docker run -d --name parsersql-test-pgsql \
    -p 15432:5432 \
    -e POSTGRES_PASSWORD=test \
    -e POSTGRES_DB=testdb \
    postgres:16

# Wait for MySQL to be ready
echo "Waiting for MySQL..."
for i in $(seq 1 60); do
    if docker exec parsersql-test-mysql mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; then
        echo "MySQL ready after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "MySQL failed to start"
        exit 1
    fi
    sleep 1
done

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
for i in $(seq 1 60); do
    if docker exec parsersql-test-pgsql pg_isready -Upostgres &>/dev/null 2>&1; then
        echo "PostgreSQL ready after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "PostgreSQL failed to start"
        exit 1
    fi
    sleep 1
done

# Load test data
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Loading MySQL test data..."
docker exec -i parsersql-test-mysql mysql -uroot -ptest testdb < "${SCRIPT_DIR}/test_data_mysql.sql"
echo "Loading PostgreSQL test data..."
docker exec -i parsersql-test-pgsql psql -Upostgres testdb < "${SCRIPT_DIR}/test_data_pgsql.sql"

echo "Backends ready!"
