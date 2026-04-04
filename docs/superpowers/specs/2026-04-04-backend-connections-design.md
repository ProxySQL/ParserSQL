# Backend Connections — Design Specification

## Overview

Replace the MockRemoteExecutor with real MySQL and PostgreSQL backend connections. The distributed query engine can now run queries against actual databases over the network.

Sub-project 10. Depends on: distributed planner (sub-project 8), DML execution (sub-project 9).

### Goals

- **MySQLRemoteExecutor** — connect to MySQL backends via libmysqlclient, run queries, convert results to Row/Value
- **PgSQLRemoteExecutor** — connect to PostgreSQL backends via libpq, run queries, convert results
- **MultiRemoteExecutor** — unified executor that routes to MySQL or PostgreSQL by backend dialect
- **Type conversion** — map MySQL/PostgreSQL wire types to our Value tags accurately
- **Docker-based test infrastructure** — spin up real MySQL + PostgreSQL for integration tests
- **Full end-to-end** — parse SQL, distribute, run against real backends, merged results

### Constraints

- C++17
- Link against libmysqlclient and libpq (system packages)
- Single persistent connection per backend (no pool — prototype)
- Sequential query execution (no parallelism — correct but not optimal)
- Tests skip gracefully if databases not available

### Non-Goals

- Connection pooling (future — ProxySQL already has this)
- Async/parallel query execution (future)
- SSL/TLS connections (future)
- Prepared statements over the wire (future)
- Custom wire protocol implementation (use libraries)

---

## BackendConfig

```cpp
struct BackendConfig {
    std::string name;       // logical name: "shard_1", "analytics_db"
    std::string host;
    uint16_t port;
    std::string user;
    std::string password;
    std::string database;
    Dialect dialect;         // MySQL or PostgreSQL
};
```

---

## MySQLRemoteExecutor

```cpp
class MySQLRemoteExecutor : public RemoteExecutor {
public:
    MySQLRemoteExecutor(Arena& arena);
    ~MySQLRemoteExecutor();

    void add_backend(const BackendConfig& config);
    ResultSet execute(const char* backend_name, StringRef sql) override;
    DmlResult execute_dml(const char* backend_name, StringRef sql) override;
    void disconnect_all();

private:
    struct Connection {
        BackendConfig config;
        MYSQL* conn = nullptr;
        bool connected = false;
    };

    std::unordered_map<std::string, Connection> backends_;
    Arena& arena_;

    Connection& get_or_connect(const std::string& name);
    ResultSet mysql_result_to_resultset(MYSQL_RES* res);
    Value mysql_field_to_value(const char* data, unsigned long length,
                               enum_field_types type, bool is_null);
};
```

### Connection lifecycle

- `add_backend()` stores the config. Does NOT connect yet (lazy).
- `get_or_connect()` connects on first use via `mysql_real_connect()`.
- If connection drops, attempts one reconnect before failing.
- `disconnect_all()` closes all connections. Called in destructor.

### MySQL type to Value conversion

All values arrive as strings in text protocol mode. Conversion by field type:

| MySQL C API type | Value tag | Conversion |
|---|---|---|
| MYSQL_TYPE_TINY/SHORT/LONG/LONGLONG | TAG_INT64 | strtoll(data) |
| MYSQL_TYPE_FLOAT/DOUBLE | TAG_DOUBLE | strtod(data) |
| MYSQL_TYPE_DECIMAL/NEWDECIMAL | TAG_STRING | arena string copy |
| MYSQL_TYPE_STRING/VAR_STRING | TAG_STRING | arena string copy |
| MYSQL_TYPE_BLOB | TAG_BYTES | arena copy |
| MYSQL_TYPE_DATE | TAG_DATE | parse "YYYY-MM-DD" to days since epoch |
| MYSQL_TYPE_DATETIME | TAG_DATETIME | parse "YYYY-MM-DD HH:MM:SS" to microseconds |
| MYSQL_TYPE_TIMESTAMP | TAG_TIMESTAMP | parse "YYYY-MM-DD HH:MM:SS" to microseconds |
| MYSQL_TYPE_TIME | TAG_TIME | parse "HH:MM:SS" to microseconds |
| NULL | TAG_NULL | value_null() |

---

## PgSQLRemoteExecutor

```cpp
class PgSQLRemoteExecutor : public RemoteExecutor {
public:
    PgSQLRemoteExecutor(Arena& arena);
    ~PgSQLRemoteExecutor();

    void add_backend(const BackendConfig& config);
    ResultSet execute(const char* backend_name, StringRef sql) override;
    DmlResult execute_dml(const char* backend_name, StringRef sql) override;
    void disconnect_all();

private:
    struct Connection {
        BackendConfig config;
        PGconn* conn = nullptr;
        bool connected = false;
    };

    std::unordered_map<std::string, Connection> backends_;
    Arena& arena_;

    Connection& get_or_connect(const std::string& name);
    ResultSet pg_result_to_resultset(PGresult* res);
    Value pg_field_to_value(const char* data, int length, Oid type, bool is_null);
};
```

### PostgreSQL type OID to Value conversion

| PgSQL OID | Value tag | Conversion |
|---|---|---|
| INT2OID (21) / INT4OID (23) / INT8OID (20) | TAG_INT64 | strtoll |
| FLOAT4OID (700) / FLOAT8OID (701) | TAG_DOUBLE | strtod |
| NUMERICOID (1700) | TAG_STRING | string copy |
| TEXTOID (25) / VARCHAROID (1043) / BPCHAROID (1042) | TAG_STRING | arena copy |
| BOOLOID (16) | TAG_BOOL | "t" = true, "f" = false |
| DATEOID (1082) | TAG_DATE | parse "YYYY-MM-DD" |
| TIMESTAMPOID (1114) | TAG_DATETIME | parse datetime string |
| TIMESTAMPTZOID (1184) | TAG_TIMESTAMP | parse with timezone |
| TIMEOID (1083) | TAG_TIME | parse "HH:MM:SS" |
| JSONOID (114) / JSONBOID (3802) | TAG_JSON | string copy |
| NULL | TAG_NULL | value_null() |

---

## MultiRemoteExecutor

```cpp
class MultiRemoteExecutor : public RemoteExecutor {
public:
    MultiRemoteExecutor(Arena& arena);
    ~MultiRemoteExecutor();

    void add_backend(const BackendConfig& config);
    ResultSet execute(const char* backend_name, StringRef sql) override;
    DmlResult execute_dml(const char* backend_name, StringRef sql) override;
    void disconnect_all();

private:
    MySQLRemoteExecutor mysql_exec_;
    PgSQLRemoteExecutor pgsql_exec_;
    std::unordered_map<std::string, Dialect> backend_dialects_;
};
```

Routes each call to the correct protocol-specific executor based on the backend's registered dialect.

---

## Date/Time Parsing Utilities

Shared between both executors:

```cpp
namespace sql_engine {
namespace datetime_parse {
    int32_t parse_date(const char* s);         // "YYYY-MM-DD" -> days since epoch
    int64_t parse_datetime(const char* s);     // "YYYY-MM-DD HH:MM:SS[.uuuuuu]" -> microseconds
    int64_t parse_time(const char* s);         // "HH:MM:SS[.uuuuuu]" -> microseconds
    int32_t days_since_epoch(int year, int month, int day);  // calendar math
} // namespace datetime_parse
} // namespace sql_engine
```

Simple, no timezone library dependency. Timezone handling deferred.

---

## Docker Test Infrastructure

### scripts/start_test_backends.sh

Starts MySQL 8 and PostgreSQL 16 containers on non-standard ports (13306, 15432) to avoid conflicts with local installations. Waits for readiness, loads test data.

### scripts/stop_test_backends.sh

Removes test containers.

### scripts/test_data_mysql.sql / test_data_pgsql.sql

Creates `users` table (id INT, name VARCHAR, age INT, dept VARCHAR) and `orders` table (id INT, user_id INT, total DECIMAL, status VARCHAR) with 5 users and 4 orders.

### Test skip logic

```cpp
#define SKIP_IF_NO_MYSQL() if (!mysql_available()) { GTEST_SKIP() << "MySQL not available"; }
#define SKIP_IF_NO_PGSQL() if (!pgsql_available()) { GTEST_SKIP() << "PostgreSQL not available"; }
```

CI runs with Docker service containers. Local dev can skip integration tests gracefully.

---

## File Organization

```
include/sql_engine/
    backend_config.h             -- BackendConfig struct
    datetime_parse.h             -- Date/time string parsing
    mysql_remote_executor.h      -- MySQLRemoteExecutor
    pgsql_remote_executor.h      -- PgSQLRemoteExecutor
    multi_remote_executor.h      -- MultiRemoteExecutor

src/sql_engine/
    mysql_remote_executor.cpp    -- MySQL implementation
    pgsql_remote_executor.cpp    -- PostgreSQL implementation
    multi_remote_executor.cpp    -- Routing implementation
    datetime_parse.cpp           -- Date/time parsing

scripts/
    start_test_backends.sh       -- Docker setup
    stop_test_backends.sh        -- Docker teardown
    test_data_mysql.sql          -- MySQL test schema + data
    test_data_pgsql.sql          -- PostgreSQL test schema + data

tests/
    test_mysql_executor.cpp      -- MySQL integration tests
    test_pgsql_executor.cpp      -- PostgreSQL integration tests
    test_distributed_real.cpp    -- Full distributed pipeline

.github/workflows/ci.yml        -- Add integration test job with Docker services
```

---

## Testing Strategy

### MySQL executor tests

SKIP_IF_NO_MYSQL for all tests.

- Connect to backend, verify connection
- SELECT * FROM users -> 5 rows, correct column count
- SELECT with WHERE -> filtered results
- SELECT with ORDER BY + LIMIT -> correct order and count
- Type conversion: INT, VARCHAR, DECIMAL, DATE, DATETIME, NULL
- INSERT -> affected_rows = 1, SELECT back confirms
- UPDATE -> affected_rows matches
- DELETE -> affected_rows matches
- Bad SQL -> error in DmlResult, no crash
- Connection to wrong port -> error, no crash

### PostgreSQL executor tests

SKIP_IF_NO_PGSQL. Same structure plus:

- BOOLEAN -> TAG_BOOL
- JSON -> TAG_JSON
- TIMESTAMP WITH TIME ZONE -> TAG_TIMESTAMP

### Full distributed tests

SKIP_IF_NO_MYSQL and SKIP_IF_NO_PGSQL.

- Shard map with users split across 2 MySQL backends
- Parse -> plan -> optimize -> distribute -> execute against real backends
- Verify: SELECT * returns all rows from all shards
- Verify: Aggregate merged correctly
- Verify: Cross-backend JOIN correct
- Verify: INSERT routed to correct shard
- Verify: UPDATE scatter correct

### CI workflow

GitHub Actions with MySQL and PostgreSQL service containers. Integration tests run as a separate job after unit tests pass.

---

## Performance Targets

| Operation | Target |
|---|---|
| Connection establishment | <50ms (network) |
| Simple SELECT (5 rows, LAN) | <1ms total |
| Type conversion per row (10 cols) | <1us |
| Date/time string parsing | <100ns per field |
