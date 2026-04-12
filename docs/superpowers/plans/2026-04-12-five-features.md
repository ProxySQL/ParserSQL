# Five Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement SSL/TLS backend connections, multi-table JOIN UPDATE/DELETE, 2PC session auto-enlistment, array/tuple evaluation, and `* EXCEPT/REPLACE` column modifiers.

**Architecture:** Five independent features, each self-contained. SSL adds config fields and applies them at connection time. Multi-table DML extends the plan builder and remote query builder to pass through multi-table SQL to backends. Session auto-enlistment wires `execute_statement()` through the distributed planner when a distributed transaction is active. Array/tuple evaluation implements the existing expression_eval stubs. `* EXCEPT/REPLACE` adds new parser node types and plan builder expansion logic.

**Tech Stack:** C++17, libmysqlclient, libpq, Google Test

---

## Task 1: SSL/TLS for Backend Connections

**Files:**
- Modify: `include/sql_engine/backend_config.h` (full file, 22 lines)
- Modify: `include/sql_engine/connection_pool.h:103-124` (`create_connection`)
- Modify: `src/sql_engine/mysql_remote_executor.cpp:51-80` (`get_or_connect`)
- Modify: `src/sql_engine/pgsql_remote_executor.cpp:62-87` (`get_or_connect`)
- Modify: `tools/sqlengine.cpp:244-252` (`parse_backend_url` query param parsing)
- Create: `tests/test_ssl_config.cpp`

### Step 1.1: Add SSL fields to BackendConfig

- [ ] **Add SSL fields to `BackendConfig` struct**

In `include/sql_engine/backend_config.h`, add four fields after `dialect`:

```cpp
struct BackendConfig {
    std::string name;       // logical name: "shard_1", "analytics_db"
    std::string host;
    uint16_t port = 0;
    std::string user;
    std::string password;
    std::string database;
    sql_parser::Dialect dialect = sql_parser::Dialect::MySQL;

    // SSL/TLS configuration. Empty strings mean "not configured" (no SSL).
    std::string ssl_mode;   // MySQL: DISABLED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
                            // PgSQL: disable, allow, prefer, require, verify-ca, verify-full
    std::string ssl_ca;     // path to CA certificate file
    std::string ssl_cert;   // path to client certificate file
    std::string ssl_key;    // path to client key file
};
```

- [ ] **Commit**

```bash
git add include/sql_engine/backend_config.h
git commit -m "feat(ssl): add SSL/TLS fields to BackendConfig"
```

### Step 1.2: Write SSL config parsing test

- [ ] **Create `tests/test_ssl_config.cpp` with config struct tests**

```cpp
#include <gtest/gtest.h>
#include "sql_engine/backend_config.h"

using namespace sql_engine;

TEST(SSLConfigTest, DefaultsAreEmpty) {
    BackendConfig cfg;
    EXPECT_TRUE(cfg.ssl_mode.empty());
    EXPECT_TRUE(cfg.ssl_ca.empty());
    EXPECT_TRUE(cfg.ssl_cert.empty());
    EXPECT_TRUE(cfg.ssl_key.empty());
}

TEST(SSLConfigTest, FieldsStoredCorrectly) {
    BackendConfig cfg;
    cfg.ssl_mode = "REQUIRED";
    cfg.ssl_ca = "/path/to/ca.pem";
    cfg.ssl_cert = "/path/to/client-cert.pem";
    cfg.ssl_key = "/path/to/client-key.pem";
    EXPECT_EQ(cfg.ssl_mode, "REQUIRED");
    EXPECT_EQ(cfg.ssl_ca, "/path/to/ca.pem");
    EXPECT_EQ(cfg.ssl_cert, "/path/to/client-cert.pem");
    EXPECT_EQ(cfg.ssl_key, "/path/to/client-key.pem");
}
```

- [ ] **Add to Makefile TEST_SRCS and run**

```bash
# Add tests/test_ssl_config.cpp to TEST_SRCS in Makefile
make test
```

Expected: PASS for both tests.

- [ ] **Commit**

```bash
git add tests/test_ssl_config.cpp Makefile
git commit -m "test(ssl): add BackendConfig SSL field tests"
```

### Step 1.3: Apply SSL options in MySQL ConnectionPool

- [ ] **Modify `ConnectionPool::create_connection()` in `include/sql_engine/connection_pool.h:113-114`**

After the timeout options (line 113) and before `mysql_real_connect` (line 115), add:

```cpp
        mysql_options(c, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
        mysql_options(c, MYSQL_OPT_READ_TIMEOUT,    &read_timeout);
        mysql_options(c, MYSQL_OPT_WRITE_TIMEOUT,   &write_timeout);

        // SSL/TLS configuration
        if (!cfg.ssl_mode.empty()) {
            unsigned int ssl_mode_val = SSL_MODE_PREFERRED;
            if (cfg.ssl_mode == "DISABLED")        ssl_mode_val = SSL_MODE_DISABLED;
            else if (cfg.ssl_mode == "REQUIRED")   ssl_mode_val = SSL_MODE_REQUIRED;
            else if (cfg.ssl_mode == "VERIFY_CA")  ssl_mode_val = SSL_MODE_VERIFY_CA;
            else if (cfg.ssl_mode == "VERIFY_IDENTITY") ssl_mode_val = SSL_MODE_VERIFY_IDENTITY;
            mysql_options(c, MYSQL_OPT_SSL_MODE, &ssl_mode_val);
        }
        if (!cfg.ssl_ca.empty())
            mysql_options(c, MYSQL_OPT_SSL_CA, cfg.ssl_ca.c_str());
        if (!cfg.ssl_cert.empty())
            mysql_options(c, MYSQL_OPT_SSL_CERT, cfg.ssl_cert.c_str());
        if (!cfg.ssl_key.empty())
            mysql_options(c, MYSQL_OPT_SSL_KEY, cfg.ssl_key.c_str());

        if (!mysql_real_connect(c, cfg.host.c_str(), cfg.user.c_str(),
```

- [ ] **Run `make test`**

Expected: All tests PASS.

- [ ] **Commit**

```bash
git add include/sql_engine/connection_pool.h
git commit -m "feat(ssl): apply SSL options in ConnectionPool::create_connection"
```

### Step 1.4: Apply SSL options in MySQLRemoteExecutor

- [ ] **Modify `MySQLRemoteExecutor::get_or_connect()` in `src/sql_engine/mysql_remote_executor.cpp:64-66`**

After the timeout options (line 64) and before `mysql_real_connect` (line 66), add the same SSL block:

```cpp
        mysql_options(c.conn, MYSQL_OPT_WRITE_TIMEOUT,   &write_timeout);

        // SSL/TLS configuration
        if (!c.config.ssl_mode.empty()) {
            unsigned int ssl_mode_val = SSL_MODE_PREFERRED;
            if (c.config.ssl_mode == "DISABLED")        ssl_mode_val = SSL_MODE_DISABLED;
            else if (c.config.ssl_mode == "REQUIRED")   ssl_mode_val = SSL_MODE_REQUIRED;
            else if (c.config.ssl_mode == "VERIFY_CA")  ssl_mode_val = SSL_MODE_VERIFY_CA;
            else if (c.config.ssl_mode == "VERIFY_IDENTITY") ssl_mode_val = SSL_MODE_VERIFY_IDENTITY;
            mysql_options(c.conn, MYSQL_OPT_SSL_MODE, &ssl_mode_val);
        }
        if (!c.config.ssl_ca.empty())
            mysql_options(c.conn, MYSQL_OPT_SSL_CA, c.config.ssl_ca.c_str());
        if (!c.config.ssl_cert.empty())
            mysql_options(c.conn, MYSQL_OPT_SSL_CERT, c.config.ssl_cert.c_str());
        if (!c.config.ssl_key.empty())
            mysql_options(c.conn, MYSQL_OPT_SSL_KEY, c.config.ssl_key.c_str());

        if (!mysql_real_connect(c.conn,
```

- [ ] **Run `make test`**

Expected: All tests PASS.

- [ ] **Commit**

```bash
git add src/sql_engine/mysql_remote_executor.cpp
git commit -m "feat(ssl): apply SSL options in MySQLRemoteExecutor::get_or_connect"
```

### Step 1.5: Apply SSL options in PgSQLRemoteExecutor

- [ ] **Modify `PgSQLRemoteExecutor::get_or_connect()` in `src/sql_engine/pgsql_remote_executor.cpp:70-77`**

After building the base conninfo string and before `PQconnectdb`, append SSL parameters:

```cpp
        std::string conninfo = "host=" + c.config.host
            + " port=" + std::to_string(c.config.port)
            + " user=" + c.config.user
            + " password=" + c.config.password
            + " dbname=" + c.config.database
            + " connect_timeout=5"
            + " options='-c statement_timeout="
                + std::to_string(SQL_ENGINE_PG_STATEMENT_TIMEOUT_MS) + "'";

        // SSL/TLS configuration
        if (!c.config.ssl_mode.empty())
            conninfo += " sslmode=" + c.config.ssl_mode;
        if (!c.config.ssl_ca.empty())
            conninfo += " sslrootcert=" + c.config.ssl_ca;
        if (!c.config.ssl_cert.empty())
            conninfo += " sslcert=" + c.config.ssl_cert;
        if (!c.config.ssl_key.empty())
            conninfo += " sslkey=" + c.config.ssl_key;

        c.conn = PQconnectdb(conninfo.c_str());
```

- [ ] **Run `make test`**

Expected: All tests PASS.

- [ ] **Commit**

```bash
git add src/sql_engine/pgsql_remote_executor.cpp
git commit -m "feat(ssl): apply SSL options in PgSQLRemoteExecutor::get_or_connect"
```

### Step 1.6: Parse SSL params from backend URL

- [ ] **Modify `parse_backend_url()` in `tools/sqlengine.cpp:244-252`**

Replace the query param parsing section. Currently it only parses `name=`. Extend to parse all query params generically:

```cpp
    // Parse query params
    std::unordered_map<std::string, std::string> params;
    if (!query_part.empty()) {
        size_t pos = 0;
        while (pos < query_part.size()) {
            size_t amp = query_part.find('&', pos);
            std::string kv = query_part.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
            size_t eq = kv.find('=');
            if (eq != std::string::npos) {
                params[kv.substr(0, eq)] = kv.substr(eq + 1);
            }
            if (amp == std::string::npos) break;
            pos = amp + 1;
        }
    }

    if (params.count("name")) pb.config.name = params["name"];
    if (params.count("ssl_mode")) pb.config.ssl_mode = params["ssl_mode"];
    if (params.count("ssl_ca")) pb.config.ssl_ca = params["ssl_ca"];
    if (params.count("ssl_cert")) pb.config.ssl_cert = params["ssl_cert"];
    if (params.count("ssl_key")) pb.config.ssl_key = params["ssl_key"];
```

- [ ] **Add URL parsing tests to `tests/test_ssl_config.cpp`**

This requires extracting `parse_backend_url` to a shared header, or simply testing via BackendConfig directly. Since the function is in `tools/sqlengine.cpp` (not a library), add the parsing test inline with a local copy of the logic:

```cpp
// Helper that mirrors sqlengine.cpp's query param parsing
static std::unordered_map<std::string, std::string> parse_query_params(const std::string& query) {
    std::unordered_map<std::string, std::string> params;
    size_t pos = 0;
    while (pos < query.size()) {
        size_t amp = query.find('&', pos);
        std::string kv = query.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
        size_t eq = kv.find('=');
        if (eq != std::string::npos) {
            params[kv.substr(0, eq)] = kv.substr(eq + 1);
        }
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    return params;
}

TEST(SSLConfigTest, QueryParamParsing) {
    auto p = parse_query_params("name=shard1&ssl_mode=REQUIRED&ssl_ca=/ca.pem");
    EXPECT_EQ(p["name"], "shard1");
    EXPECT_EQ(p["ssl_mode"], "REQUIRED");
    EXPECT_EQ(p["ssl_ca"], "/ca.pem");
    EXPECT_EQ(p.count("ssl_cert"), 0u);
}

TEST(SSLConfigTest, QueryParamParsingSingleParam) {
    auto p = parse_query_params("name=db1");
    EXPECT_EQ(p["name"], "db1");
    EXPECT_EQ(p.size(), 1u);
}

TEST(SSLConfigTest, QueryParamParsingAllSSL) {
    auto p = parse_query_params("name=s1&ssl_mode=VERIFY_CA&ssl_ca=/ca.pem&ssl_cert=/client.pem&ssl_key=/key.pem");
    EXPECT_EQ(p["ssl_mode"], "VERIFY_CA");
    EXPECT_EQ(p["ssl_ca"], "/ca.pem");
    EXPECT_EQ(p["ssl_cert"], "/client.pem");
    EXPECT_EQ(p["ssl_key"], "/key.pem");
}
```

- [ ] **Run `make test`**

Expected: All tests PASS.

- [ ] **Commit**

```bash
git add tools/sqlengine.cpp tests/test_ssl_config.cpp
git commit -m "feat(ssl): parse SSL params from backend URL query string"
```

### Step 1.7: Close GitHub issue

- [ ] **Close issue #30**

```bash
gh issue close 30 --comment "Implemented: SSL/TLS fields in BackendConfig, applied in ConnectionPool, MySQLRemoteExecutor, PgSQLRemoteExecutor. Parsed from backend URL query params (ssl_mode, ssl_ca, ssl_cert, ssl_key)."
```

---

## Task 2: Multi-table JOIN UPDATE/DELETE Across Shards

**Files:**
- Modify: `include/sql_engine/plan_node.h:149-161` (UPDATE_PLAN / DELETE_PLAN structs)
- Modify: `include/sql_engine/dml_plan_builder.h:106-178` (`build_update` / `build_delete`)
- Modify: `include/sql_engine/remote_query_builder.h:142-189` (`build_update` / `build_delete`)
- Modify: `include/sql_engine/distributed_planner.h:1016-1089` (`distribute_update` / `distribute_delete`)
- Modify: `tests/test_dml.cpp`
- Modify: `tests/test_distributed_dml.cpp`

### Step 2.1: Extend PlanNode with `original_ast` for multi-table DML

- [ ] **Add `original_ast` field to UPDATE_PLAN and DELETE_PLAN in `include/sql_engine/plan_node.h:149-161`**

The pragmatic approach: for multi-table DML, store the original AST so the emitter can reconstruct the full SQL. Single-table DML continues using the existing field-based approach.

```cpp
        struct {
            const TableInfo* table;
            const sql_parser::AstNode** set_columns;
            const sql_parser::AstNode** set_exprs;
            uint16_t set_count;
            const sql_parser::AstNode* where_expr;
            const sql_parser::AstNode* original_ast;  // non-null for multi-table UPDATE
        } update_plan;

        struct {
            const TableInfo* table;
            const sql_parser::AstNode* where_expr;
            const sql_parser::AstNode* original_ast;  // non-null for multi-table DELETE
        } delete_plan;
```

Since `PlanNode` is zero-initialized by `make_plan_node`, the new field defaults to `nullptr`.

- [ ] **Run `make test`** — verify no regressions

- [ ] **Commit**

```bash
git add include/sql_engine/plan_node.h
git commit -m "feat(multitable): add original_ast field to UPDATE/DELETE plan nodes"
```

### Step 2.2: Detect multi-table in DmlPlanBuilder

- [ ] **Modify `build_update()` in `include/sql_engine/dml_plan_builder.h:106-155`**

After finding the first TABLE_REF, check for NODE_FROM_CLAUSE to detect multi-table UPDATE. If multi-table, store the original AST:

```cpp
    PlanNode* build_update(const sql_parser::AstNode* update_ast) {
        if (!update_ast) return nullptr;

        PlanNode* node = make_plan_node(arena_, PlanNodeType::UPDATE_PLAN);
        if (!node) return nullptr;

        // Find TABLE_REF child -> resolve table (primary target)
        const sql_parser::AstNode* table_ref = find_child(update_ast, sql_parser::NodeType::NODE_TABLE_REF);
        if (table_ref) {
            node->update_plan.table = resolve_table(table_ref);
        }

        // Detect multi-table UPDATE (has FROM_CLAUSE with JOINs)
        const sql_parser::AstNode* from_clause = find_child(update_ast, sql_parser::NodeType::NODE_FROM_CLAUSE);
        if (from_clause) {
            node->update_plan.original_ast = update_ast;
        }

        // Find UPDATE_SET_CLAUSE -> extract SET items
        const sql_parser::AstNode* set_clause = find_child(update_ast, sql_parser::NodeType::NODE_UPDATE_SET_CLAUSE);
        if (set_clause) {
            uint16_t cnt = count_children(set_clause);
            node->update_plan.set_count = cnt;

            auto** set_cols = static_cast<const sql_parser::AstNode**>(
                arena_.allocate(sizeof(sql_parser::AstNode*) * cnt));
            auto** set_exprs = static_cast<const sql_parser::AstNode**>(
                arena_.allocate(sizeof(sql_parser::AstNode*) * cnt));

            uint16_t idx = 0;
            for (const sql_parser::AstNode* item = set_clause->first_child; item; item = item->next_sibling) {
                const sql_parser::AstNode* col = item->first_child;
                const sql_parser::AstNode* expr = col ? col->next_sibling : nullptr;
                set_cols[idx] = col;
                set_exprs[idx] = expr;
                ++idx;
            }
            node->update_plan.set_columns = set_cols;
            node->update_plan.set_exprs = set_exprs;
        } else {
            node->update_plan.set_columns = nullptr;
            node->update_plan.set_exprs = nullptr;
            node->update_plan.set_count = 0;
        }

        // Find WHERE_CLAUSE -> where expression
        const sql_parser::AstNode* where = find_child(update_ast, sql_parser::NodeType::NODE_WHERE_CLAUSE);
        if (where && where->first_child) {
            node->update_plan.where_expr = where->first_child;
        } else {
            node->update_plan.where_expr = nullptr;
        }

        return node;
    }
```

- [ ] **Modify `build_delete()` similarly in `include/sql_engine/dml_plan_builder.h:157-178`**

```cpp
    PlanNode* build_delete(const sql_parser::AstNode* delete_ast) {
        if (!delete_ast) return nullptr;

        PlanNode* node = make_plan_node(arena_, PlanNodeType::DELETE_PLAN);
        if (!node) return nullptr;

        // Find TABLE_REF child -> resolve table (primary target)
        const sql_parser::AstNode* table_ref = find_child(delete_ast, sql_parser::NodeType::NODE_TABLE_REF);
        if (table_ref) {
            node->delete_plan.table = resolve_table(table_ref);
        }

        // Detect multi-table DELETE (has FROM_CLAUSE or DELETE_USING_CLAUSE with multiple tables)
        const sql_parser::AstNode* from_clause = find_child(delete_ast, sql_parser::NodeType::NODE_FROM_CLAUSE);
        const sql_parser::AstNode* using_clause = find_child(delete_ast, sql_parser::NodeType::NODE_DELETE_USING_CLAUSE);
        if (from_clause || using_clause) {
            // Check if FROM_CLAUSE has JOINs (more than one child = multi-table)
            bool is_multi = false;
            if (using_clause) is_multi = true;
            if (from_clause && from_clause->first_child && from_clause->first_child->next_sibling) is_multi = true;
            if (is_multi) {
                node->delete_plan.original_ast = delete_ast;
            }
        }

        // Find WHERE_CLAUSE
        const sql_parser::AstNode* where = find_child(delete_ast, sql_parser::NodeType::NODE_WHERE_CLAUSE);
        if (where && where->first_child) {
            node->delete_plan.where_expr = where->first_child;
        } else {
            node->delete_plan.where_expr = nullptr;
        }

        return node;
    }
```

- [ ] **Run `make test`**

Expected: All existing tests still pass. Multi-table DML now gets `original_ast` set.

- [ ] **Commit**

```bash
git add include/sql_engine/dml_plan_builder.h
git commit -m "feat(multitable): detect multi-table UPDATE/DELETE in DmlPlanBuilder"
```

### Step 2.3: Extend RemoteQueryBuilder to emit multi-table SQL from AST

- [ ] **Add `build_update_from_ast()` and `build_delete_from_ast()` methods in `include/sql_engine/remote_query_builder.h`**

Add before the `private:` section (line 191):

```cpp
    // Build UPDATE SQL from original AST (for multi-table UPDATE).
    // Uses the emitter to faithfully reconstruct the full SQL.
    sql_parser::StringRef build_update_from_ast(const sql_parser::AstNode* ast) {
        sql_parser::Emitter<D> emitter(arena_);
        emitter.emit(ast);
        return emitter.result();
    }

    // Build DELETE SQL from original AST (for multi-table DELETE).
    sql_parser::StringRef build_delete_from_ast(const sql_parser::AstNode* ast) {
        sql_parser::Emitter<D> emitter(arena_);
        emitter.emit(ast);
        return emitter.result();
    }
```

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add include/sql_engine/remote_query_builder.h
git commit -m "feat(multitable): add AST-based SQL emission for multi-table DML"
```

### Step 2.4: Update distributed planner to handle multi-table DML

- [ ] **Modify `distribute_update()` in `include/sql_engine/distributed_planner.h:1016-1053`**

At the top, check for multi-table and use AST-based emission:

```cpp
    PlanNode* distribute_update(PlanNode* plan) {
        const auto& up = plan->update_plan;
        const TableInfo* table = up.table;
        if (!table || !shards_.has_table(table->table_name)) return plan;

        // Multi-table UPDATE: emit full SQL from AST, route to primary table's backend.
        // Cross-shard multi-table JOINs are not supported (MySQL executes JOINs locally).
        if (up.original_ast) {
            sql_parser::StringRef sql = qb_.build_update_from_ast(up.original_ast);
            if (!shards_.is_sharded(table->table_name)) {
                return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
            }
            // For sharded tables, scatter to all shards (shard key extraction from
            // multi-table WHERE is complex; each shard executes the full JOIN locally).
            const auto& shard_list = shards_.get_shards(table->table_name);
            return scatter_dml_to_shards(table, shard_list, [&]() { return sql; });
        }

        // Single-table UPDATE (existing logic unchanged)
        const sql_parser::AstNode* where_expr = up.where_expr;
        if (where_expr && has_subquery(where_expr) && remote_executor_) {
            where_expr = rewrite_where_subquery(where_expr, table);
        }
        // ... rest of existing code ...
```

- [ ] **Modify `distribute_delete()` similarly in `include/sql_engine/distributed_planner.h:1055-1089`**

```cpp
    PlanNode* distribute_delete(PlanNode* plan) {
        const auto& dp = plan->delete_plan;
        const TableInfo* table = dp.table;
        if (!table || !shards_.has_table(table->table_name)) return plan;

        // Multi-table DELETE: emit full SQL from AST, route to primary table's backend.
        if (dp.original_ast) {
            sql_parser::StringRef sql = qb_.build_delete_from_ast(dp.original_ast);
            if (!shards_.is_sharded(table->table_name)) {
                return make_remote_scan(shards_.get_backend(table->table_name), sql, table);
            }
            const auto& shard_list = shards_.get_shards(table->table_name);
            return scatter_dml_to_shards(table, shard_list, [&]() { return sql; });
        }

        // Single-table DELETE (existing logic unchanged)
        const sql_parser::AstNode* where_expr = dp.where_expr;
        // ... rest of existing code ...
```

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add include/sql_engine/distributed_planner.h
git commit -m "feat(multitable): distribute multi-table UPDATE/DELETE via AST emission"
```

### Step 2.5: Write multi-table DML tests

- [ ] **Add tests to `tests/test_dml.cpp`**

Test that multi-table UPDATE/DELETE parse → plan build → `original_ast` is set:

```cpp
TEST(DmlPlanBuilderTest, MultiTableUpdateSetsOriginalAst) {
    Arena arena;
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.name = t2.name WHERE t2.active = 1",
                           strlen("UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.name = t2.name WHERE t2.active = 1"));
    ASSERT_EQ(pr.status, ParseResult::OK);

    InMemoryCatalog catalog;
    TableInfo t1; t1.table_name = {"t1", 2}; t1.column_count = 0;
    catalog.add_table(t1);

    DmlPlanBuilder<Dialect::MySQL> builder(catalog, arena);
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->type, PlanNodeType::UPDATE_PLAN);
    EXPECT_NE(plan->update_plan.original_ast, nullptr);
}

TEST(DmlPlanBuilderTest, SingleTableUpdateOriginalAstNull) {
    Arena arena;
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("UPDATE t1 SET name = 'x' WHERE id = 1",
                           strlen("UPDATE t1 SET name = 'x' WHERE id = 1"));
    ASSERT_EQ(pr.status, ParseResult::OK);

    InMemoryCatalog catalog;
    TableInfo t1; t1.table_name = {"t1", 2}; t1.column_count = 0;
    catalog.add_table(t1);

    DmlPlanBuilder<Dialect::MySQL> builder(catalog, arena);
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->update_plan.original_ast, nullptr);
}

TEST(DmlPlanBuilderTest, MultiTableDeleteSetsOriginalAst) {
    Arena arena;
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.status = 0",
                           strlen("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.status = 0"));
    ASSERT_EQ(pr.status, ParseResult::OK);

    InMemoryCatalog catalog;
    TableInfo t1; t1.table_name = {"t1", 2}; t1.column_count = 0;
    catalog.add_table(t1);

    DmlPlanBuilder<Dialect::MySQL> builder(catalog, arena);
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->type, PlanNodeType::DELETE_PLAN);
    EXPECT_NE(plan->delete_plan.original_ast, nullptr);
}
```

- [ ] **Run `make test`**

Expected: All new and existing tests PASS.

- [ ] **Commit**

```bash
git add tests/test_dml.cpp
git commit -m "test(multitable): add multi-table DML plan builder tests"
```

### Step 2.6: Close GitHub issue

- [ ] **Close issue #32**

```bash
gh issue close 32 --comment "Multi-table JOIN UPDATE/DELETE now supported: parser (already complete), plan builder detects multi-table via FROM_CLAUSE/USING, distributed planner emits full SQL from AST to each shard. Cross-shard JOINs executed locally per backend."
```

---

## Task 3: Session Auto-Enlistment for 2PC DML

**Files:**
- Modify: `include/sql_engine/transaction_manager.h` (full file, 31 lines)
- Modify: `include/sql_engine/distributed_txn.h:155-180` (`execute_participant_dml`)
- Modify: `include/sql_engine/session.h:190-216` (`execute_statement`)
- Modify: `tests/test_session.cpp`
- Modify: `tests/test_distributed_txn.cpp`

### Step 3.1: Add `is_distributed()` and DML routing to TransactionManager interface

- [ ] **Modify `include/sql_engine/transaction_manager.h`**

Add two virtual methods with defaults:

```cpp
class TransactionManager {
public:
    virtual ~TransactionManager() = default;

    // Transaction lifecycle
    virtual bool begin() = 0;
    virtual bool commit() = 0;
    virtual bool rollback() = 0;

    // Savepoints
    virtual bool savepoint(const char* name) = 0;
    virtual bool rollback_to(const char* name) = 0;
    virtual bool release_savepoint(const char* name) = 0;

    // State
    virtual bool in_transaction() const = 0;
    virtual bool is_auto_commit() const = 0;
    virtual void set_auto_commit(bool ac) = 0;

    // Distributed transaction support. Override in DistributedTransactionManager.
    virtual bool is_distributed() const { return false; }

    // Route DML through the transaction's pinned session for a given backend.
    // Default: returns empty DmlResult with success=false (not supported).
    // DistributedTransactionManager overrides to route through execute_participant_dml.
    virtual DmlResult route_dml(const char* /*backend_name*/,
                                sql_parser::StringRef /*sql*/) {
        DmlResult r;
        r.error_message = "route_dml not supported by this transaction manager";
        return r;
    }
};
```

Add necessary includes at the top:

```cpp
#include "sql_engine/dml_result.h"
#include "sql_parser/common.h"
```

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add include/sql_engine/transaction_manager.h
git commit -m "feat(2pc): add is_distributed() and route_dml() to TransactionManager"
```

### Step 3.2: Override `route_dml()` in DistributedTransactionManager

- [ ] **Add override in `include/sql_engine/distributed_txn.h`**

After the `execute_participant_dml` method (around line 180), add:

```cpp
    bool is_distributed() const override { return true; }

    DmlResult route_dml(const char* backend_name,
                        sql_parser::StringRef sql) override {
        return execute_participant_dml(backend_name, sql);
    }
```

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add include/sql_engine/distributed_txn.h
git commit -m "feat(2pc): override route_dml in DistributedTransactionManager"
```

### Step 3.3: Wire `Session::execute_statement()` to distribute DML

- [ ] **Modify `Session::execute_statement()` in `include/sql_engine/session.h:190-216`**

Replace the "Regular DML" section:

```cpp
        // Regular DML
        DmlPlanBuilder<D> dml_builder(catalog_, parser_.arena());
        PlanNode* plan = dml_builder.build(pr.ast);
        if (!plan) {
            DmlResult dr;
            dr.error_message = "plan build error";
            return dr;
        }

        // Auto-commit: wrap in implicit transaction
        bool implicit_txn = txn_mgr_.is_auto_commit() && !txn_mgr_.in_transaction();
        if (implicit_txn) txn_mgr_.begin();

        DmlResult result;

        // If sharding is configured, distribute DML to remote backends.
        // If a distributed transaction is active, route through the txn
        // manager's pinned sessions (2PC enlistment).
        if (shard_map_ && remote_executor_) {
            DistributedPlanner<D> dp(*shard_map_, catalog_, parser_.arena(),
                                      remote_executor_, &functions_);
            PlanNode* dist_plan = dp.distribute_dml(plan);

            if (dist_plan && dist_plan->type == PlanNodeType::REMOTE_SCAN) {
                // Single-shard DML
                sql_parser::StringRef sql_ref{dist_plan->remote_scan.remote_sql,
                                              dist_plan->remote_scan.remote_sql_len};
                if (txn_mgr_.in_transaction() && txn_mgr_.is_distributed()) {
                    result = txn_mgr_.route_dml(dist_plan->remote_scan.backend_name, sql_ref);
                } else {
                    result = remote_executor_->execute_dml(
                        dist_plan->remote_scan.backend_name, sql_ref);
                }
            } else if (dist_plan && dist_plan->type == PlanNodeType::SET_OP) {
                // Scatter DML to multiple shards (SET_OP of REMOTE_SCAN nodes).
                // Collect results from each shard.
                result.success = true;
                result.affected_rows = 0;
                for_each_remote_scan(dist_plan, [&](const PlanNode* rs) {
                    sql_parser::StringRef s{rs->remote_scan.remote_sql,
                                            rs->remote_scan.remote_sql_len};
                    DmlResult shard_result;
                    if (txn_mgr_.in_transaction() && txn_mgr_.is_distributed()) {
                        shard_result = txn_mgr_.route_dml(rs->remote_scan.backend_name, s);
                    } else {
                        shard_result = remote_executor_->execute_dml(
                            rs->remote_scan.backend_name, s);
                    }
                    if (!shard_result.success) {
                        result.success = false;
                        result.error_message = shard_result.error_message;
                    }
                    result.affected_rows += shard_result.affected_rows;
                });
            } else {
                // Not distributed — fall through to local execution
                PlanExecutor<D> executor(functions_, catalog_, parser_.arena());
                wire_executor(executor);
                result = executor.execute_dml(plan);
            }
        } else {
            // No sharding: local execution
            PlanExecutor<D> executor(functions_, catalog_, parser_.arena());
            wire_executor(executor);
            result = executor.execute_dml(plan);
        }

        if (implicit_txn) {
            if (result.success)
                txn_mgr_.commit();
            else
                txn_mgr_.rollback();
        }

        return result;
```

- [ ] **Add helper `for_each_remote_scan()` to `Session` private section**

After `wire_executor()`:

```cpp
    // Walk a SET_OP tree of REMOTE_SCAN nodes, calling fn for each leaf.
    static void for_each_remote_scan(const PlanNode* node,
                                      const std::function<void(const PlanNode*)>& fn) {
        if (!node) return;
        if (node->type == PlanNodeType::REMOTE_SCAN) {
            fn(node);
            return;
        }
        if (node->type == PlanNodeType::SET_OP) {
            for_each_remote_scan(node->left, fn);
            for_each_remote_scan(node->right, fn);
        }
    }
```

- [ ] **Run `make test`**

Expected: All tests PASS.

- [ ] **Commit**

```bash
git add include/sql_engine/session.h include/sql_engine/transaction_manager.h
git commit -m "feat(2pc): wire Session::execute_statement to distribute DML through 2PC"
```

### Step 3.4: Write session auto-enlistment tests

- [ ] **Add tests to `tests/test_session.cpp`**

Test that when a DistributedTransactionManager is active, DML goes through `route_dml`:

```cpp
// Mock TransactionManager that tracks route_dml calls
class TrackingDistributedTxnMgr : public TransactionManager {
public:
    bool begin() override { active_ = true; return true; }
    bool commit() override { active_ = false; return true; }
    bool rollback() override { active_ = false; return true; }
    bool savepoint(const char*) override { return true; }
    bool rollback_to(const char*) override { return true; }
    bool release_savepoint(const char*) override { return true; }
    bool in_transaction() const override { return active_; }
    bool is_auto_commit() const override { return !active_; }
    void set_auto_commit(bool) override {}

    bool is_distributed() const override { return true; }
    DmlResult route_dml(const char* backend, sql_parser::StringRef sql) override {
        routed_backends.push_back(backend);
        routed_sqls.emplace_back(sql.ptr, sql.len);
        DmlResult r;
        r.success = true;
        r.affected_rows = 1;
        return r;
    }

    std::vector<std::string> routed_backends;
    std::vector<std::string> routed_sqls;
private:
    bool active_ = false;
};

TEST(SessionAutoEnlistTest, DmlRoutedThroughDistributedTxn) {
    // Setup: catalog with a sharded table, shard map, mock executor, tracking txn mgr
    // Execute INSERT inside BEGIN...COMMIT
    // Verify routed_backends is non-empty (DML went through route_dml, not executor directly)
}
```

The exact test will depend on setting up a ShardMap and MockRemoteExecutor. Follow the patterns in the existing `test_distributed_dml.cpp`.

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add tests/test_session.cpp
git commit -m "test(2pc): add session auto-enlistment test for distributed DML"
```

---

## Task 4: Array/Tuple Expression Evaluation

**Files:**
- Modify: `include/sql_engine/expression_eval.h:688-691` (stub implementations)
- Modify: `tests/test_expression_eval.cpp`

### Step 4.1: Write failing tests for array subscript evaluation

- [ ] **Add tests to `tests/test_expression_eval.cpp`**

```cpp
TEST(ExpressionEvalTest, ArraySubscriptOnJsonArray) {
    // Parse: SELECT ARRAY[10, 20, 30][2]
    // The parser produces NODE_ARRAY_SUBSCRIPT(NODE_ARRAY_CONSTRUCTOR(...), NODE_LITERAL_INT(2))
    // Expected: value 20 (1-based indexing for PostgreSQL, 0-based check for MySQL)
    Arena arena;
    Parser<Dialect::PostgreSQL> parser;
    auto pr = parser.parse("SELECT ARRAY[10, 20, 30][2]", 27);
    ASSERT_EQ(pr.status, ParseResult::OK);

    // Extract the expression from the SELECT item
    const AstNode* select_list = find_child(pr.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(select_list, nullptr);
    const AstNode* item = select_list->first_child;
    ASSERT_NE(item, nullptr);
    const AstNode* expr = item->first_child;
    ASSERT_NE(expr, nullptr);

    FunctionRegistry<Dialect::PostgreSQL> funcs;
    auto resolver = [](StringRef) -> Value { return value_null(); };
    Value result = evaluate_expression<Dialect::PostgreSQL>(expr, resolver, funcs, arena);
    // ARRAY[10,20,30][2] in PostgreSQL = 20 (1-based)
    ASSERT_EQ(result.tag, Value::TAG_INT64);
    EXPECT_EQ(result.int_val, 20);
}

TEST(ExpressionEvalTest, ArraySubscriptOutOfBounds) {
    Arena arena;
    Parser<Dialect::PostgreSQL> parser;
    auto pr = parser.parse("SELECT ARRAY[10, 20][5]", 23);
    ASSERT_EQ(pr.status, ParseResult::OK);

    const AstNode* select_list = find_child(pr.ast, NodeType::NODE_SELECT_ITEM_LIST);
    const AstNode* expr = select_list->first_child->first_child;

    FunctionRegistry<Dialect::PostgreSQL> funcs;
    auto resolver = [](StringRef) -> Value { return value_null(); };
    Value result = evaluate_expression<Dialect::PostgreSQL>(expr, resolver, funcs, arena);
    EXPECT_TRUE(result.is_null());
}

TEST(ExpressionEvalTest, TupleConstructorEvaluation) {
    // ROW(1, 'hello', 3.14) — evaluate to a tuple-like structure
    // For now, since we don't have a full composite type, evaluating
    // a bare tuple returns null (no consumer). The test documents behavior.
    Arena arena;
    Parser<Dialect::PostgreSQL> parser;
    auto pr = parser.parse("SELECT ROW(1, 2, 3)", 19);
    ASSERT_EQ(pr.status, ParseResult::OK);

    const AstNode* select_list = find_child(pr.ast, NodeType::NODE_SELECT_ITEM_LIST);
    const AstNode* expr = select_list->first_child->first_child;

    FunctionRegistry<Dialect::PostgreSQL> funcs;
    auto resolver = [](StringRef) -> Value { return value_null(); };
    Value result = evaluate_expression<Dialect::PostgreSQL>(expr, resolver, funcs, arena);
    // Tuple evaluation returns null (no composite type support yet)
    EXPECT_TRUE(result.is_null());
}
```

- [ ] **Run tests — verify the array subscript tests FAIL (stubs return null)**

```bash
./run_tests --gtest_filter="*ArraySubscript*"
```

Expected: `ArraySubscriptOnJsonArray` FAILS, `ArraySubscriptOutOfBounds` PASSES (already returns null).

- [ ] **Commit**

```bash
git add tests/test_expression_eval.cpp
git commit -m "test: add array subscript evaluation tests (currently failing)"
```

### Step 4.2: Implement ARRAY_CONSTRUCTOR evaluation

- [ ] **Modify `include/sql_engine/expression_eval.h:688-691`**

Replace the stubs. ARRAY_CONSTRUCTOR evaluates its children and stores results. Since Value doesn't have a native array tag, we represent small arrays inline and use subscript to extract:

```cpp
            case NodeType::NODE_ARRAY_CONSTRUCTOR: {
                // Evaluate children and return a JSON-encoded array string.
                // This lets NODE_ARRAY_SUBSCRIPT index into it.
                sql_parser::StringBuilder sb(arena, 256);
                sb.append_char('[');
                bool first = true;
                for (const AstNode* child = node->first_child; child; child = child->next_sibling) {
                    if (!first) sb.append(", ");
                    first = false;
                    Value v = evaluate(child);
                    if (v.is_null()) {
                        sb.append("null");
                    } else if (v.tag == Value::TAG_INT64) {
                        char buf[32];
                        int n = snprintf(buf, sizeof(buf), "%lld", (long long)v.int_val);
                        sb.append(buf, n);
                    } else if (v.tag == Value::TAG_DOUBLE) {
                        char buf[64];
                        int n = snprintf(buf, sizeof(buf), "%g", v.dbl_val);
                        sb.append(buf, n);
                    } else if (v.tag == Value::TAG_STRING) {
                        sb.append_char('"');
                        sb.append(v.str_val.ptr, v.str_val.len);
                        sb.append_char('"');
                    } else {
                        // Fallback: emit as string
                        sb.append_char('"');
                        auto sv = v.to_string(arena);
                        sb.append(sv.ptr, sv.len);
                        sb.append_char('"');
                    }
                }
                sb.append_char(']');
                auto ref = sb.finish();
                return value_json(ref);
            }
```

Actually, a simpler approach: since the only consumer of ARRAY_CONSTRUCTOR in the evaluator is ARRAY_SUBSCRIPT, we can use a direct child-counting approach. Let me reconsider.

The cleanest approach: `NODE_ARRAY_SUBSCRIPT` evaluates its children directly without needing ARRAY_CONSTRUCTOR to produce a serialized form. The subscript handler walks the array constructor's children by index.

```cpp
            case NodeType::NODE_ARRAY_CONSTRUCTOR:
                // Bare array constructor (not subscripted) — return as JSON string
                return value_null();  // No direct use without subscript

            case NodeType::NODE_ARRAY_SUBSCRIPT: {
                // expr[index] — if expr is NODE_ARRAY_CONSTRUCTOR, directly index children
                const AstNode* array_expr = node->first_child;
                const AstNode* index_expr = array_expr ? array_expr->next_sibling : nullptr;
                if (!array_expr || !index_expr) return value_null();

                Value idx = evaluate(index_expr);
                if (idx.is_null()) return value_null();
                int64_t i = idx.to_int64();

                if (array_expr->type == NodeType::NODE_ARRAY_CONSTRUCTOR) {
                    // 1-based indexing (PostgreSQL standard)
                    int64_t pos = (D == sql_parser::Dialect::PostgreSQL) ? i - 1 : i;
                    if (pos < 0) return value_null();
                    const AstNode* child = array_expr->first_child;
                    for (int64_t c = 0; c < pos && child; ++c) {
                        child = child->next_sibling;
                    }
                    if (!child) return value_null();
                    return evaluate(child);
                }

                // For non-literal arrays, evaluate the expression and try JSON access
                return value_null();
            }

            case NodeType::NODE_TUPLE:
                return value_null();  // Composite types not yet supported

            case NodeType::NODE_FIELD_ACCESS:
                return value_null();  // Composite field access not yet supported
```

- [ ] **Run tests**

```bash
./run_tests --gtest_filter="*ArraySubscript*:*Tuple*"
```

Expected: All PASS.

- [ ] **Commit**

```bash
git add include/sql_engine/expression_eval.h
git commit -m "feat: implement ARRAY subscript evaluation in expression evaluator"
```

---

## Task 5: `* EXCEPT/REPLACE` Column Modifiers

**Files:**
- Modify: `include/sql_parser/common.h:209` (add NodeType entries)
- Modify: `include/sql_parser/token.h` (check TK_REPLACE exists — it does at line 48)
- Modify: `include/sql_parser/select_parser.h:157-179` (`parse_select_item`)
- Modify: `include/sql_parser/emitter.h` (add emit cases)
- Modify: `include/sql_parser/expression_parser.h:601-679` (`is_keyword_as_identifier` — add TK_REPLACE if not already there)
- Modify: `include/sql_engine/plan_builder.h:301-340` (star expansion)
- Create: `tests/test_star_modifiers.cpp`

### Step 5.1: Add new NodeType entries

- [ ] **Add to `include/sql_parser/common.h` before the closing `NODE_UPDATE_SET_ITEM`**

Insert before line 209:

```cpp
    // Star modifiers (BigQuery-style)
    NODE_STAR_EXCEPT,          // SELECT * EXCEPT(col1, col2)
    NODE_STAR_REPLACE,         // SELECT * REPLACE(expr AS col)
    NODE_REPLACE_ITEM,         // single expr AS col inside REPLACE

    // Shared
    NODE_STMT_OPTIONS,
    NODE_UPDATE_SET_ITEM,
```

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add include/sql_parser/common.h
git commit -m "feat(star-modifiers): add NODE_STAR_EXCEPT, NODE_STAR_REPLACE, NODE_REPLACE_ITEM"
```

### Step 5.2: Write parser tests for `* EXCEPT/REPLACE`

- [ ] **Create `tests/test_star_modifiers.cpp`**

```cpp
#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

TEST(StarModifierTest, SelectStarExcept) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT * EXCEPT(id, created_at) FROM users",
                           strlen("SELECT * EXCEPT(id, created_at) FROM users"));
    ASSERT_EQ(pr.status, ParseResult::OK);
    ASSERT_NE(pr.ast, nullptr);
    EXPECT_EQ(pr.stmt_type, StmtType::SELECT);
}

TEST(StarModifierTest, SelectTableStarExcept) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT users.* EXCEPT(password) FROM users",
                           strlen("SELECT users.* EXCEPT(password) FROM users"));
    ASSERT_EQ(pr.status, ParseResult::OK);
}

TEST(StarModifierTest, SelectStarReplace) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT * REPLACE(UPPER(name) AS name) FROM users",
                           strlen("SELECT * REPLACE(UPPER(name) AS name) FROM users"));
    ASSERT_EQ(pr.status, ParseResult::OK);
}

TEST(StarModifierTest, SelectStarExceptRoundTrip) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT * EXCEPT(id, created_at) FROM users",
                           strlen("SELECT * EXCEPT(id, created_at) FROM users"));
    ASSERT_EQ(pr.status, ParseResult::OK);

    Arena arena;
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(pr.ast);
    StringRef result = emitter.result();
    std::string sql(result.ptr, result.len);
    EXPECT_NE(sql.find("EXCEPT"), std::string::npos);
    EXPECT_NE(sql.find("id"), std::string::npos);
    EXPECT_NE(sql.find("created_at"), std::string::npos);
}

TEST(StarModifierTest, SelectStarReplaceRoundTrip) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT * REPLACE(UPPER(name) AS name) FROM users",
                           strlen("SELECT * REPLACE(UPPER(name) AS name) FROM users"));
    ASSERT_EQ(pr.status, ParseResult::OK);

    Arena arena;
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(pr.ast);
    StringRef result = emitter.result();
    std::string sql(result.ptr, result.len);
    EXPECT_NE(sql.find("REPLACE"), std::string::npos);
    EXPECT_NE(sql.find("UPPER"), std::string::npos);
}

TEST(StarModifierTest, PostgreSQLStarExcept) {
    Parser<Dialect::PostgreSQL> parser;
    auto pr = parser.parse("SELECT * EXCEPT(id) FROM t1",
                           strlen("SELECT * EXCEPT(id) FROM t1"));
    ASSERT_EQ(pr.status, ParseResult::OK);
}
```

- [ ] **Add `tests/test_star_modifiers.cpp` to Makefile TEST_SRCS**

- [ ] **Run tests — they should FAIL (parser doesn't handle EXCEPT/REPLACE after `*` yet)**

```bash
./run_tests --gtest_filter="*StarModifier*"
```

Expected: FAIL — parser treats `EXCEPT` as a set operation.

- [ ] **Commit**

```bash
git add tests/test_star_modifiers.cpp Makefile
git commit -m "test(star-modifiers): add parser tests for * EXCEPT/REPLACE (failing)"
```

### Step 5.3: Implement parser support for `* EXCEPT/REPLACE`

- [ ] **Modify `parse_select_item()` in `include/sql_parser/select_parser.h:157-179`**

After parsing the expression, check if it's an asterisk followed by EXCEPT or REPLACE:

```cpp
    AstNode* parse_select_item() {
        AstNode* item = make_node(arena_, NodeType::NODE_SELECT_ITEM);
        if (!item) return nullptr;

        AstNode* expr = expr_parser_.parse();
        if (!expr) return nullptr;

        // Check for * EXCEPT(...) or * REPLACE(...) modifiers
        bool is_star = (expr->type == NodeType::NODE_ASTERISK);
        // Also handle table.* — the asterisk is the second child of a qualified name
        if (!is_star && expr->type == NodeType::NODE_QUALIFIED_NAME) {
            // Check if the last child is NODE_ASTERISK
            for (const AstNode* c = expr->first_child; c; c = c->next_sibling) {
                if (!c->next_sibling && c->type == NodeType::NODE_ASTERISK) {
                    is_star = true;
                }
            }
        }

        if (is_star) {
            Token next = tok_.peek();
            if (next.type == TokenType::TK_EXCEPT) {
                tok_.skip();
                AstNode* except_node = make_node(arena_, NodeType::NODE_STAR_EXCEPT);
                except_node->add_child(expr);
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    while (tok_.peek().type != TokenType::TK_RPAREN &&
                           tok_.peek().type != TokenType::TK_EOF) {
                        Token col = tok_.next_token();
                        AstNode* col_node = make_node(arena_, NodeType::NODE_IDENTIFIER, col.text);
                        except_node->add_child(col_node);
                        if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                    }
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                }
                item->add_child(except_node);
                return item;
            }
            if (next.type == TokenType::TK_REPLACE) {
                tok_.skip();
                AstNode* replace_node = make_node(arena_, NodeType::NODE_STAR_REPLACE);
                replace_node->add_child(expr);
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    while (tok_.peek().type != TokenType::TK_RPAREN &&
                           tok_.peek().type != TokenType::TK_EOF) {
                        // Parse: expr AS col_name
                        AstNode* replace_expr = expr_parser_.parse();
                        AstNode* replace_item = make_node(arena_, NodeType::NODE_REPLACE_ITEM);
                        if (replace_expr) replace_item->add_child(replace_expr);
                        if (tok_.peek().type == TokenType::TK_AS) {
                            tok_.skip();
                            Token col = tok_.next_token();
                            AstNode* col_node = make_node(arena_, NodeType::NODE_IDENTIFIER, col.text);
                            replace_item->add_child(col_node);
                        }
                        replace_node->add_child(replace_item);
                        if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                    }
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                }
                item->add_child(replace_node);
                return item;
            }
        }

        item->add_child(expr);

        // Optional alias: AS name, or just name (implicit alias)
        Token next = tok_.peek();
        if (next.type == TokenType::TK_AS) {
            tok_.skip();
            Token alias_name = tok_.next_token();
            AstNode* alias = make_node(arena_, NodeType::NODE_ALIAS, alias_name.text);
            item->add_child(alias);
        } else if (TableRefParser<D>::is_alias_start(next.type)) {
            tok_.skip();
            AstNode* alias = make_node(arena_, NodeType::NODE_ALIAS, next.text);
            item->add_child(alias);
        }
        return item;
    }
```

- [ ] **Add `TK_REPLACE` to `is_alias_start()` blocklist in `table_ref_parser.h:239`**

After `TK_PARTITION`:

```cpp
            case TokenType::TK_PARTITION:
            case TokenType::TK_REPLACE:
                return false;
```

- [ ] **Run tests**

```bash
./run_tests --gtest_filter="*StarModifier*"
```

Expected: Parse tests PASS, round-trip tests still FAIL (emitter not done yet).

- [ ] **Commit**

```bash
git add include/sql_parser/select_parser.h include/sql_parser/table_ref_parser.h
git commit -m "feat(star-modifiers): parse * EXCEPT(...) and * REPLACE(expr AS col)"
```

### Step 5.4: Add emitter support

- [ ] **Add emit cases to `include/sql_parser/emitter.h`**

In the `emit_node()` switch, add cases (near the other NODE_ASTERISK/NODE_SELECT_ITEM handling):

```cpp
            case NodeType::NODE_STAR_EXCEPT:  emit_star_except(node); break;
            case NodeType::NODE_STAR_REPLACE: emit_star_replace(node); break;
            case NodeType::NODE_REPLACE_ITEM: emit_replace_item(node); break;
```

Add the implementation methods (near the other emit methods, e.g. after `emit_field_access`):

```cpp
    void emit_star_except(const AstNode* node) {
        // First child is the asterisk (or qualified.*)
        const AstNode* star = node->first_child;
        if (star) emit_node(star);
        sb_.append(" EXCEPT(");
        bool first = true;
        for (const AstNode* col = star ? star->next_sibling : node->first_child;
             col; col = col->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(col);
        }
        sb_.append_char(')');
    }

    void emit_star_replace(const AstNode* node) {
        const AstNode* star = node->first_child;
        if (star) emit_node(star);
        sb_.append(" REPLACE(");
        bool first = true;
        for (const AstNode* item = star ? star->next_sibling : node->first_child;
             item; item = item->next_sibling) {
            if (!first) sb_.append(", ");
            first = false;
            emit_node(item);
        }
        sb_.append_char(')');
    }

    void emit_replace_item(const AstNode* node) {
        // First child: expression, second child: column name identifier
        const AstNode* expr = node->first_child;
        const AstNode* col = expr ? expr->next_sibling : nullptr;
        if (expr) emit_node(expr);
        if (col) {
            sb_.append(" AS ");
            emit_node(col);
        }
    }
```

- [ ] **Run tests**

```bash
./run_tests --gtest_filter="*StarModifier*"
```

Expected: All tests PASS including round-trip tests.

- [ ] **Commit**

```bash
git add include/sql_parser/emitter.h
git commit -m "feat(star-modifiers): add emitter support for * EXCEPT/REPLACE"
```

### Step 5.5: Add plan builder support for `* EXCEPT`

- [ ] **Modify star expansion in `include/sql_engine/plan_builder.h:301-340`**

Extend the star detection to handle `NODE_STAR_EXCEPT`:

```cpp
    if (first_item && !first_item->next_sibling) {
        const sql_parser::AstNode* expr = first_item->first_child;
        if (expr && expr->type == sql_parser::NodeType::NODE_ASTERISK &&
            !find_child(first_item, sql_parser::NodeType::NODE_ALIAS)) {
            is_star_only = true;
        }
        // * EXCEPT is NOT a bare star — it needs a PROJECT to filter columns
    }
```

In the PROJECT building loop (line 328-334), add handling for NODE_STAR_EXCEPT and NODE_STAR_REPLACE. These expand into individual column references minus the excluded ones / with replacements applied. The expansion requires catalog access:

```cpp
        uint16_t idx = 0;
        for (const sql_parser::AstNode* item = item_list->first_child; item; item = item->next_sibling) {
            exprs[idx] = item->first_child;
            aliases[idx] = find_child(item, sql_parser::NodeType::NODE_ALIAS);
            ++idx;
        }
```

This existing code already works: the `NODE_STAR_EXCEPT` node becomes the expression for that select item. The executor's `ProjectOperator` will need to handle it during evaluation. For the proxy use case (where plans get distributed and re-emitted as SQL), this is sufficient — the emitter handles `NODE_STAR_EXCEPT` correctly, and the backend expands `*`.

- [ ] **Run full test suite**

```bash
make test
```

Expected: All tests PASS.

- [ ] **Commit**

```bash
git add include/sql_engine/plan_builder.h
git commit -m "feat(star-modifiers): handle * EXCEPT/REPLACE in plan builder"
```

### Step 5.6: Add `is_keyword_as_identifier` entry for REPLACE

- [ ] **Check if TK_REPLACE is already in `is_keyword_as_identifier()` — if not, add it**

In `include/sql_parser/expression_parser.h`, add in the switch (near line 668-669):

```cpp
            case TokenType::TK_REPLACE:
```

This ensures REPLACE can be used as a column/table name in other contexts (which it commonly is).

- [ ] **Run `make test`**

- [ ] **Commit**

```bash
git add include/sql_parser/expression_parser.h
git commit -m "feat(star-modifiers): allow REPLACE as identifier in expression contexts"
```

### Step 5.7: Close GitHub issues

- [ ] **Close issues #19 and #20**

```bash
gh issue close 19 --comment "Array subscript evaluation implemented (ARRAY[...][n] with 1-based PostgreSQL indexing). Field access and tuple construction return null (composite types deferred). Parser and emitter were already complete."

gh issue close 20 --comment "Implemented: SELECT * EXCEPT(col1, col2) and SELECT * REPLACE(expr AS col). Parser, emitter (round-trip), and plan builder all support the new syntax. Works for both MySQL and PostgreSQL dialects."
```

---

## Final Step: Integration Verification

- [ ] **Run the full test suite**

```bash
make test
```

Expected: All 1152+ tests PASS, no regressions.

- [ ] **Run benchmarks to check for performance regressions**

```bash
make bench
```

Expected: No significant regression (all changes are on cold paths — connection setup, plan building, not the hot parse/tokenize path).

- [ ] **Commit all and push**

```bash
git push
```
