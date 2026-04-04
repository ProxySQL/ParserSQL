# DML Execution (INSERT/UPDATE/DELETE) — Design Specification

## Overview

Extends the query engine with INSERT, UPDATE, and DELETE execution — both locally (against in-memory data sources) and distributed (routed to remote backends). Completes the engine's ability to handle all core SQL operations.

Sub-project 9. Depends on: executor (sub-project 7), distributed planner (sub-project 8), parser (INSERT/UPDATE/DELETE parsers already exist).

### Goals

- **MutableDataSource interface** — write operations (insert, update_where, delete_where)
- **DML plan nodes** — INSERT_PLAN, UPDATE_PLAN, DELETE_PLAN
- **Local DML execution** — execute against InMemoryMutableDataSource
- **Distributed DML** — route INSERTs by shard key, scatter UPDATEs/DELETEs when needed
- **DmlResult** — affected row count, success/error status

### Constraints

- C++17, arena-compatible
- No transactions (atomic multi-statement deferred)
- No auto-increment / sequence support yet
- No ON DUPLICATE KEY / ON CONFLICT handling yet (parse-only, not executed)

---

## MutableDataSource

```cpp
class MutableDataSource : public DataSource {
public:
    virtual bool insert(const Row& row) = 0;
    virtual uint64_t delete_where(std::function<bool(const Row&)> predicate) = 0;
    virtual uint64_t update_where(std::function<bool(const Row&)> predicate,
                                   std::function<void(Row&)> updater) = 0;
};
```

### InMemoryMutableDataSource

Extends the existing InMemoryDataSource with write support:

```cpp
class InMemoryMutableDataSource : public MutableDataSource {
public:
    InMemoryMutableDataSource(const TableInfo* table);

    // DataSource interface (read)
    const TableInfo* table_info() const override;
    void open() override;
    bool next(Row& out) override;
    void close() override;

    // MutableDataSource interface (write)
    bool insert(const Row& row) override;
    uint64_t delete_where(std::function<bool(const Row&)> predicate) override;
    uint64_t update_where(std::function<bool(const Row&)> predicate,
                           std::function<void(Row&)> updater) override;

    // Utility
    size_t row_count() const;
};
```

Internally stores rows in a `std::vector<Row>`. INSERT appends. DELETE removes matching rows. UPDATE modifies matching rows in place.

---

## DML Plan Nodes

```cpp
enum class PlanNodeType : uint8_t {
    // ... existing ...
    INSERT_PLAN,
    UPDATE_PLAN,
    DELETE_PLAN,
};
```

### INSERT_PLAN

```cpp
struct {
    const TableInfo* table;
    const AstNode** columns;       // column names (nullable = all columns in order)
    uint16_t column_count;
    const AstNode** value_rows;    // array of NODE_VALUES_ROW pointers
    uint16_t row_count;
    PlanNode* select_source;       // INSERT ... SELECT (nullable, mutually exclusive with value_rows)
} insert_plan;
```

### UPDATE_PLAN

```cpp
struct {
    const TableInfo* table;
    const AstNode** set_columns;   // column name AST nodes
    const AstNode** set_exprs;     // new value expression AST nodes (parallel array)
    uint16_t set_count;
    const AstNode* where_expr;     // WHERE condition (nullable = update all)
} update_plan;
```

### DELETE_PLAN

```cpp
struct {
    const TableInfo* table;
    const AstNode* where_expr;     // WHERE condition (nullable = delete all)
} delete_plan;
```

---

## DmlResult

```cpp
struct DmlResult {
    uint64_t affected_rows = 0;
    uint64_t last_insert_id = 0;
    bool success = false;
    std::string error_message;
};
```

---

## DML Plan Builder

```cpp
template <Dialect D>
class DmlPlanBuilder {
public:
    DmlPlanBuilder(const Catalog& catalog, Arena& arena);

    PlanNode* build_insert(const AstNode* insert_ast);
    PlanNode* build_update(const AstNode* update_ast);
    PlanNode* build_delete(const AstNode* delete_ast);
};
```

Translates INSERT/UPDATE/DELETE AST nodes into DML plan nodes. Resolves table names via catalog.

---

## DML Execution

### PlanExecutor extensions

```cpp
template <Dialect D>
class PlanExecutor {
public:
    // ... existing ...
    ResultSet execute(PlanNode* plan);

    // New
    DmlResult execute_dml(PlanNode* plan);
    void add_mutable_data_source(const char* table_name, MutableDataSource* source);
};
```

### INSERT execution

1. Look up MutableDataSource for the target table
2. For VALUES: evaluate each expression in each row, build Row, call `source->insert(row)`
3. For INSERT ... SELECT: execute the SELECT sub-plan, insert each result row
4. Return DmlResult with affected_rows = number of rows inserted

### UPDATE execution

1. Look up MutableDataSource for the target table
2. Build predicate from WHERE expression: `[&](const Row& row) -> bool { evaluate WHERE against row }`
3. Build updater from SET list: `[&](Row& row) { for each SET col=expr: evaluate expr, row.set(col_ordinal, result) }`
4. Call `source->update_where(predicate, updater)`
5. Return DmlResult with affected_rows

### DELETE execution

1. Look up MutableDataSource for the target table
2. Build predicate from WHERE expression
3. Call `source->delete_where(predicate)`
4. Return DmlResult with affected_rows

---

## Distributed DML

### DistributedPlanner extensions

```cpp
template <Dialect D>
class DistributedPlanner {
    // ... existing distribute() for SELECT ...

    // New
    PlanNode* distribute_dml(PlanNode* dml_plan);
};
```

### INSERT routing

- **Unsharded table:** Single RemoteScan with INSERT SQL to the backend
- **Sharded table:** Examine the shard key value in each VALUES row. Group rows by target shard. Generate one INSERT per shard with its subset of rows.
- **INSERT ... SELECT:** Execute SELECT distributedly first, then route each result row to the correct shard

### UPDATE routing

- **Unsharded:** Send UPDATE to single backend
- **Shard key in WHERE** (e.g., `WHERE user_id = 42`): Route to specific shard
- **No shard key in WHERE:** Scatter UPDATE to ALL shards. Sum affected_rows from all backends.

### DELETE routing

- Same logic as UPDATE: shard key → specific shard, otherwise → scatter to all

### Remote DML SQL generation

```cpp
template <Dialect D>
class RemoteQueryBuilder {
    // ... existing ...
    StringRef build_insert(const TableInfo* table, const AstNode** columns, uint16_t col_count,
                           const AstNode** value_rows, uint16_t row_count);
    StringRef build_update(const TableInfo* table, const AstNode** set_cols,
                           const AstNode** set_exprs, uint16_t set_count,
                           const AstNode* where_expr);
    StringRef build_delete(const TableInfo* table, const AstNode* where_expr);
};
```

### RemoteExecutor extension

```cpp
class RemoteExecutor {
public:
    virtual ResultSet execute(const char* backend_name, StringRef sql) = 0;
    virtual DmlResult execute_dml(const char* backend_name, StringRef sql) = 0;
};
```

---

## File Organization

```
include/sql_engine/
    mutable_data_source.h        — MutableDataSource + InMemoryMutableDataSource
    dml_result.h                 — DmlResult struct
    dml_plan_builder.h           — AST → DML plan nodes

    (modify) plan_node.h         — INSERT_PLAN, UPDATE_PLAN, DELETE_PLAN
    (modify) plan_executor.h     — execute_dml(), add_mutable_data_source()
    (modify) remote_executor.h   — execute_dml()
    (modify) remote_query_builder.h — build_insert/update/delete()
    (modify) distributed_planner.h  — distribute_dml()

tests/
    test_dml.cpp                 — Local DML against InMemoryMutableDataSource
    test_distributed_dml.cpp     — Distributed DML routing + correctness
```

---

## Testing Strategy

### Local DML (test_dml.cpp)

Set up InMemoryMutableDataSource with initial data. Execute DML, verify with SELECT.

- INSERT single row → affected_rows = 1, SELECT confirms row present
- INSERT multiple rows (multi-value) → affected_rows = N
- INSERT ... SELECT → rows copied from source table
- UPDATE with WHERE → only matching rows changed, affected_rows correct
- UPDATE without WHERE → all rows changed
- UPDATE with expression (SET age = age + 1) → computed correctly
- DELETE with WHERE → matching rows removed, affected_rows correct
- DELETE without WHERE → all rows gone, affected_rows = total
- INSERT then DELETE then SELECT → verify correct state
- NULL in INSERT values → row has NULL in that column
- UPDATE SET column = NULL → column becomes NULL

### Distributed DML (test_distributed_dml.cpp)

MockRemoteExecutor with 3 backends.

- INSERT to unsharded → single backend receives INSERT
- INSERT to sharded → routed to correct shard by shard key value
- Multi-row INSERT to sharded → rows grouped by shard
- UPDATE unsharded → single backend
- UPDATE sharded with shard key in WHERE → single shard targeted
- UPDATE sharded without shard key → scattered to all shards, affected_rows summed
- DELETE unsharded → single backend
- DELETE sharded with shard key → single shard
- DELETE sharded scatter → all shards
- Correctness: INSERT distributedly then SELECT to verify all rows present

---

## Performance Targets

| Operation | Target |
|---|---|
| INSERT single row (local) | <1us |
| INSERT 100 rows (local) | <50us |
| DELETE with WHERE (100 rows, 10 match) | <20us |
| UPDATE with WHERE (100 rows, 10 match) | <30us |
| Distributed INSERT routing (3 shards) | <10us (excluding network) |
