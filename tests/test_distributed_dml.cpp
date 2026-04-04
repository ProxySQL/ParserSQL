// test_distributed_dml.cpp -- Distributed DML routing + correctness tests

#include <gtest/gtest.h>
#include "sql_engine/distributed_planner.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/parser.h"
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <memory>

using namespace sql_engine;
using namespace sql_parser;

// ---- Helpers ----

static StringRef arena_str(Arena& arena, const char* s) {
    uint32_t len = static_cast<uint32_t>(std::strlen(s));
    char* buf = static_cast<char*>(arena.allocate(len));
    std::memcpy(buf, s, len);
    return StringRef{buf, len};
}

static Row build_row(Arena& arena, std::initializer_list<Value> vals) {
    uint16_t n = static_cast<uint16_t>(vals.size());
    Row r = make_row(arena, n);
    uint16_t i = 0;
    for (auto& v : vals) r.set(i++, v);
    return r;
}

// ---- MockRemoteExecutor for DML ----

struct DmlBackendData {
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Arena data_arena{65536, 1048576};
    std::map<std::string, InMemoryMutableDataSource*> mutable_sources;
    std::vector<std::unique_ptr<MutableDataSource>> owned_sources;
    std::vector<std::string> executed_sqls; // log of executed SQL

    DmlBackendData() {
        functions.register_builtins();
    }

    void add_table(const char* table_name, std::initializer_list<ColumnDef> columns) {
        catalog.add_table("", table_name, columns);
        const TableInfo* ti = catalog.get_table(
            StringRef{table_name, static_cast<uint32_t>(std::strlen(table_name))});
        auto* src = new InMemoryMutableDataSource(ti, data_arena);
        mutable_sources[table_name] = src;
        owned_sources.emplace_back(src);
    }

    void add_table_with_rows(const char* table_name,
                              std::initializer_list<ColumnDef> columns,
                              std::vector<Row> rows) {
        catalog.add_table("", table_name, columns);
        const TableInfo* ti = catalog.get_table(
            StringRef{table_name, static_cast<uint32_t>(std::strlen(table_name))});
        auto* src = new InMemoryMutableDataSource(ti, data_arena, std::move(rows));
        mutable_sources[table_name] = src;
        owned_sources.emplace_back(src);
    }
};

class DmlMockRemoteExecutor : public RemoteExecutor {
public:
    void add_backend(const std::string& name) {
        backends_[name] = std::make_unique<DmlBackendData>();
    }

    DmlBackendData* get_backend(const std::string& name) {
        auto it = backends_.find(name);
        return it != backends_.end() ? it->second.get() : nullptr;
    }

    void add_table_to_all(const char* table_name, std::initializer_list<ColumnDef> columns) {
        for (auto& [bname, bd] : backends_) {
            bd->add_table(table_name, columns);
        }
    }

    ResultSet execute(const char* backend_name, StringRef sql) override {
        auto it = backends_.find(backend_name);
        if (it == backends_.end()) return {};

        DmlBackendData* bd = it->second.get();
        bd->executed_sqls.emplace_back(sql.ptr, sql.len);

        std::string sql_str(sql.ptr, sql.len);

        // Detect DML vs SELECT
        if (is_dml(sql_str)) {
            // For execute() with DML, just run it silently (some tests route through execute)
            execute_dml_internal(bd, sql_str);
            return {};
        }

        // Parse and execute SELECT
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql_str.c_str(), sql_str.size());
        if (pr.status != ParseResult::OK || !pr.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(bd->catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        if (!plan) return {};

        PlanExecutor<Dialect::MySQL> executor(bd->functions, bd->catalog, parser.arena());
        for (auto& [tname, src] : bd->mutable_sources) {
            executor.add_mutable_data_source(tname.c_str(), src);
        }
        return executor.execute(plan);
    }

    DmlResult execute_dml(const char* backend_name, StringRef sql) override {
        auto it = backends_.find(backend_name);
        if (it == backends_.end()) {
            DmlResult r;
            r.error_message = "unknown backend";
            return r;
        }

        DmlBackendData* bd = it->second.get();
        bd->executed_sqls.emplace_back(sql.ptr, sql.len);

        std::string sql_str(sql.ptr, sql.len);
        return execute_dml_internal(bd, sql_str);
    }

    // Count rows across all backends for a given table
    size_t total_row_count(const char* table_name) {
        size_t total = 0;
        for (auto& [bname, bd] : backends_) {
            auto it = bd->mutable_sources.find(table_name);
            if (it != bd->mutable_sources.end()) {
                total += it->second->row_count();
            }
        }
        return total;
    }

    // Get all executed SQL for a backend
    const std::vector<std::string>& get_executed_sqls(const std::string& backend) {
        static const std::vector<std::string> empty;
        auto it = backends_.find(backend);
        return it != backends_.end() ? it->second->executed_sqls : empty;
    }

    // Count backends that received at least one SQL
    size_t backends_with_traffic() {
        size_t count = 0;
        for (auto& [bname, bd] : backends_) {
            if (!bd->executed_sqls.empty()) ++count;
        }
        return count;
    }

    void clear_sql_logs() {
        for (auto& [bname, bd] : backends_) {
            bd->executed_sqls.clear();
        }
    }

private:
    std::map<std::string, std::unique_ptr<DmlBackendData>> backends_;

    static bool is_dml(const std::string& sql) {
        // Check first keyword
        size_t i = 0;
        while (i < sql.size() && sql[i] == ' ') ++i;
        std::string prefix;
        while (i < sql.size() && sql[i] != ' ' && sql[i] != '\t') {
            char c = sql[i];
            if (c >= 'a' && c <= 'z') c -= 32;
            prefix += c;
            ++i;
        }
        return prefix == "INSERT" || prefix == "UPDATE" || prefix == "DELETE";
    }

    DmlResult execute_dml_internal(DmlBackendData* bd, const std::string& sql_str) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql_str.c_str(), sql_str.size());
        if (pr.status != ParseResult::OK || !pr.ast) {
            DmlResult r;
            r.error_message = "parse error: " + sql_str;
            return r;
        }

        DmlPlanBuilder<Dialect::MySQL> dml_builder(bd->catalog, parser.arena());
        PlanNode* plan = dml_builder.build(pr.ast);
        if (!plan) {
            DmlResult r;
            r.error_message = "plan build error";
            return r;
        }

        PlanExecutor<Dialect::MySQL> executor(bd->functions, bd->catalog, parser.arena());
        for (auto& [tname, src] : bd->mutable_sources) {
            executor.add_mutable_data_source(tname.c_str(), src);
        }
        return executor.execute_dml(plan);
    }
};

// ---- Test fixture ----

class DistributedDmlTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    ShardMap shard_map;
    DmlMockRemoteExecutor mock_executor;

    const TableInfo* users_table = nullptr;
    const TableInfo* orders_table = nullptr;

    void SetUp() override {
        functions.register_builtins();

        // Global catalog
        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        catalog.add_table("", "orders", {
            {"order_id", SqlType::make_int(),    false},
            {"user_id",  SqlType::make_int(),    true},
            {"amount",   SqlType::make_int(),    true},
        });
        orders_table = catalog.get_table(StringRef{"orders", 6});

        // Shard map: users is sharded on "id" across 3 backends
        shard_map.add_table({"users", "id", {{"shard0"}, {"shard1"}, {"shard2"}}});
        // orders is unsharded on a single backend
        shard_map.add_table({"orders", "", {{"orders_backend"}}});

        // Set up backends
        mock_executor.add_backend("shard0");
        mock_executor.add_backend("shard1");
        mock_executor.add_backend("shard2");
        mock_executor.add_backend("orders_backend");

        // Add tables to backends
        mock_executor.add_table_to_all("users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });

        mock_executor.get_backend("orders_backend")->add_table("orders", {
            {"order_id", SqlType::make_int(),    false},
            {"user_id",  SqlType::make_int(),    true},
            {"amount",   SqlType::make_int(),    true},
        });
    }

    // Distribute a DML statement
    PlanNode* parse_and_distribute_dml(const char* sql) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK || !pr.ast) return nullptr;

        DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
        PlanNode* plan = dml_builder.build(pr.ast);
        if (!plan) return nullptr;

        DistributedPlanner<Dialect::MySQL> dist(shard_map, catalog, parser.arena());
        return dist.distribute_dml(plan);
    }

    // Execute a distributed DML: distribute + walk the plan tree to find REMOTE_SCANs,
    // execute each one via the mock executor
    DmlResult execute_distributed_dml(const char* sql) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK || !pr.ast) {
            DmlResult r;
            r.error_message = "parse error";
            return r;
        }

        DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
        PlanNode* plan = dml_builder.build(pr.ast);
        if (!plan) {
            DmlResult r;
            r.error_message = "plan build error";
            return r;
        }

        DistributedPlanner<Dialect::MySQL> dist(shard_map, catalog, parser.arena());
        PlanNode* dist_plan = dist.distribute_dml(plan);

        // Execute all REMOTE_SCAN nodes in the distributed plan
        return execute_remote_dml_plan(dist_plan);
    }

    DmlResult execute_remote_dml_plan(PlanNode* plan) {
        DmlResult total;
        total.success = true;
        collect_and_execute_remote_scans(plan, total);
        return total;
    }

    void collect_and_execute_remote_scans(PlanNode* node, DmlResult& total) {
        if (!node) return;

        if (node->type == PlanNodeType::REMOTE_SCAN) {
            StringRef sql{node->remote_scan.remote_sql, node->remote_scan.remote_sql_len};
            DmlResult r = mock_executor.execute_dml(node->remote_scan.backend_name, sql);
            total.affected_rows += r.affected_rows;
            if (!r.success) {
                total.success = false;
                total.error_message = r.error_message;
            }
            return;
        }

        if (node->type == PlanNodeType::SET_OP) {
            collect_and_execute_remote_scans(node->left, total);
            collect_and_execute_remote_scans(node->right, total);
            return;
        }

        collect_and_execute_remote_scans(node->left, total);
        collect_and_execute_remote_scans(node->right, total);
    }

    // Execute a distributed SELECT for verification
    ResultSet execute_distributed_select(const char* sql) {
        Parser<Dialect::MySQL> p;
        auto pr = p.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK || !pr.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(catalog, p.arena());
        PlanNode* plan = builder.build(pr.ast);
        if (!plan) return {};

        DistributedPlanner<Dialect::MySQL> dist(shard_map, catalog, p.arena());
        PlanNode* dist_plan = dist.distribute(plan);

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, p.arena());
        executor.set_remote_executor(&mock_executor);
        return executor.execute(dist_plan);
    }
};

// INSERT to unsharded table -> single backend
TEST_F(DistributedDmlTest, InsertUnsharded) {
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (1, 10, 100)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);

    // Only orders_backend should have received the SQL
    EXPECT_EQ(mock_executor.get_executed_sqls("orders_backend").size(), 1u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard0").size(), 0u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard1").size(), 0u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard2").size(), 0u);

    // Verify the row exists
    EXPECT_EQ(mock_executor.total_row_count("orders"), 1u);
}

// INSERT to sharded table -> route by shard key value
TEST_F(DistributedDmlTest, InsertShardedSingleRow) {
    mock_executor.clear_sql_logs();

    // id=3 should route to shard 3%3=0
    auto result = execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);

    // Only one shard should have the row
    EXPECT_EQ(mock_executor.total_row_count("users"), 1u);

    // Specifically shard0 (3 % 3 == 0)
    auto* s0 = mock_executor.get_backend("shard0");
    auto it0 = s0->mutable_sources.find("users");
    EXPECT_EQ(it0->second->row_count(), 1u);
}

// Multi-row INSERT to sharded -> rows grouped by shard
TEST_F(DistributedDmlTest, InsertShardedMultiRow) {
    mock_executor.clear_sql_logs();

    // id=0 -> shard0, id=1 -> shard1, id=2 -> shard2, id=3 -> shard0
    auto result = execute_distributed_dml(
        "INSERT INTO users (id, name, age) VALUES (0, 'Zero', 10), (1, 'One', 20), (2, 'Two', 30), (3, 'Three', 40)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 4u);
    EXPECT_EQ(mock_executor.total_row_count("users"), 4u);

    // shard0 should have id=0,3 (2 rows); shard1 has id=1 (1 row); shard2 has id=2 (1 row)
    auto* s0 = mock_executor.get_backend("shard0");
    auto* s1 = mock_executor.get_backend("shard1");
    auto* s2 = mock_executor.get_backend("shard2");
    EXPECT_EQ(s0->mutable_sources["users"]->row_count(), 2u);
    EXPECT_EQ(s1->mutable_sources["users"]->row_count(), 1u);
    EXPECT_EQ(s2->mutable_sources["users"]->row_count(), 1u);
}

// UPDATE unsharded -> single backend
TEST_F(DistributedDmlTest, UpdateUnsharded) {
    // First insert a row into orders
    execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (1, 10, 100)");
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("UPDATE orders SET amount = 200 WHERE order_id = 1");
    EXPECT_TRUE(result.success);

    // Only orders_backend should have traffic
    EXPECT_EQ(mock_executor.get_executed_sqls("orders_backend").size(), 1u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard0").size(), 0u);
}

// UPDATE sharded with shard key in WHERE -> single shard targeted
TEST_F(DistributedDmlTest, UpdateShardedTargeted) {
    // Insert a row: id=3 -> shard0
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("UPDATE users SET age = 18 WHERE id = 3");
    EXPECT_TRUE(result.success);

    // Only shard0 should have received the UPDATE (3 % 3 == 0)
    EXPECT_EQ(mock_executor.get_executed_sqls("shard0").size(), 1u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard1").size(), 0u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard2").size(), 0u);
}

// UPDATE sharded without shard key -> scatter to all shards
TEST_F(DistributedDmlTest, UpdateShardedScatter) {
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("UPDATE users SET age = 99 WHERE age > 10");
    EXPECT_TRUE(result.success);

    // All 3 shards should have received the UPDATE
    EXPECT_GE(mock_executor.get_executed_sqls("shard0").size(), 1u);
    EXPECT_GE(mock_executor.get_executed_sqls("shard1").size(), 1u);
    EXPECT_GE(mock_executor.get_executed_sqls("shard2").size(), 1u);
}

// DELETE unsharded -> single backend
TEST_F(DistributedDmlTest, DeleteUnsharded) {
    execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (1, 10, 100)");
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("DELETE FROM orders WHERE order_id = 1");
    EXPECT_TRUE(result.success);

    EXPECT_EQ(mock_executor.get_executed_sqls("orders_backend").size(), 1u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard0").size(), 0u);
}

// DELETE sharded with shard key -> single shard
TEST_F(DistributedDmlTest, DeleteShardedTargeted) {
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("DELETE FROM users WHERE id = 3");
    EXPECT_TRUE(result.success);

    // Only shard0 (3 % 3 == 0)
    EXPECT_EQ(mock_executor.get_executed_sqls("shard0").size(), 1u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard1").size(), 0u);
    EXPECT_EQ(mock_executor.get_executed_sqls("shard2").size(), 0u);
}

// DELETE sharded scatter -> all shards
TEST_F(DistributedDmlTest, DeleteShardedScatter) {
    mock_executor.clear_sql_logs();

    auto result = execute_distributed_dml("DELETE FROM users WHERE age > 10");
    EXPECT_TRUE(result.success);

    EXPECT_GE(mock_executor.get_executed_sqls("shard0").size(), 1u);
    EXPECT_GE(mock_executor.get_executed_sqls("shard1").size(), 1u);
    EXPECT_GE(mock_executor.get_executed_sqls("shard2").size(), 1u);
}

// Correctness: INSERT distributed then SELECT to verify
TEST_F(DistributedDmlTest, InsertThenSelectVerify) {
    // Insert rows to different shards
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (0, 'Alice', 25)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (1, 'Bob', 30)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (2, 'Carol', 17)");

    // Verify total count
    EXPECT_EQ(mock_executor.total_row_count("users"), 3u);

    // Each shard should have exactly 1 row
    EXPECT_EQ(mock_executor.get_backend("shard0")->mutable_sources["users"]->row_count(), 1u);
    EXPECT_EQ(mock_executor.get_backend("shard1")->mutable_sources["users"]->row_count(), 1u);
    EXPECT_EQ(mock_executor.get_backend("shard2")->mutable_sources["users"]->row_count(), 1u);

    // Verify data via distributed SELECT
    auto rs = execute_distributed_select("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 3u);
}

// ==================================================================
// Cross-shard subquery DML tests (Gap 2)
// ==================================================================

// DELETE FROM users WHERE id IN (SELECT user_id FROM orders)
// users on shards, orders on orders_backend
TEST_F(DistributedDmlTest, DISABLED_DmlWithCrossShardSubquery) {
    // Insert users: id=0 -> shard0, id=1 -> shard1, id=2 -> shard2
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (0, 'Alice', 25)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (1, 'Bob', 30)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (2, 'Carol', 17)");

    // Insert orders referencing users 0 and 1
    execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (100, 0, 500)");
    execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 300)");

    EXPECT_EQ(mock_executor.total_row_count("users"), 3u);
    EXPECT_EQ(mock_executor.total_row_count("orders"), 2u);

    mock_executor.clear_sql_logs();

    // Use the extended planner with remote executor for cross-shard subquery
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("DELETE FROM users WHERE id IN (SELECT user_id FROM orders)",
                           std::strlen("DELETE FROM users WHERE id IN (SELECT user_id FROM orders)"));
    ASSERT_EQ(pr.status, ParseResult::OK);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dist(shard_map, catalog, parser.arena(),
                                             &mock_executor, &functions);
    PlanNode* dist_plan = dist.distribute_dml(plan);

    // Execute the distributed plan
    DmlResult total;
    total.success = true;
    collect_and_execute_remote_scans(dist_plan, total);
    EXPECT_TRUE(total.success);

    // Users 0 and 1 should be deleted; user 2 should remain
    EXPECT_EQ(mock_executor.total_row_count("users"), 1u);

    // The remaining user should be Carol (id=2) on shard2
    auto* s2 = mock_executor.get_backend("shard2");
    EXPECT_EQ(s2->mutable_sources["users"]->row_count(), 1u);
}

// UPDATE users SET age = 99 WHERE id IN (SELECT user_id FROM orders)
TEST_F(DistributedDmlTest, DISABLED_UpdateWithCrossShardSubquery) {
    // Insert users
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (0, 'Alice', 25)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (1, 'Bob', 30)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (2, 'Carol', 17)");

    // Insert orders referencing users 0 and 1
    execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (100, 0, 500)");
    execute_distributed_dml("INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 300)");

    mock_executor.clear_sql_logs();

    // Use the extended planner with remote executor
    Parser<Dialect::MySQL> parser;
    const char* sql = "UPDATE users SET age = 99 WHERE id IN (SELECT user_id FROM orders)";
    auto pr = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(pr.status, ParseResult::OK);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dist(shard_map, catalog, parser.arena(),
                                             &mock_executor, &functions);
    PlanNode* dist_plan = dist.distribute_dml(plan);

    DmlResult total;
    total.success = true;
    collect_and_execute_remote_scans(dist_plan, total);
    EXPECT_TRUE(total.success);

    // Users 0 and 1 should have age=99 now
    // Total user count unchanged
    EXPECT_EQ(mock_executor.total_row_count("users"), 3u);

    // Verify age updates by checking shard0 (id=0) and shard1 (id=1)
    auto* s0 = mock_executor.get_backend("shard0");
    auto* s1 = mock_executor.get_backend("shard1");
    ASSERT_EQ(s0->mutable_sources["users"]->row_count(), 1u);
    ASSERT_EQ(s1->mutable_sources["users"]->row_count(), 1u);
    // age is column index 2
    EXPECT_EQ(s0->mutable_sources["users"]->rows()[0].get(2).int_val, 99);
    EXPECT_EQ(s1->mutable_sources["users"]->rows()[0].get(2).int_val, 99);
    // Carol should be unchanged
    auto* s2 = mock_executor.get_backend("shard2");
    EXPECT_EQ(s2->mutable_sources["users"]->rows()[0].get(2).int_val, 17);
}

// ==================================================================
// INSERT ... SELECT distributed (Gap 3)
// ==================================================================

// INSERT INTO orders SELECT * FROM users (simplified: source sharded, target unsharded)
TEST_F(DistributedDmlTest, InsertSelectDistributed) {
    // Insert source data across shards
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (0, 'Alice', 25)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (1, 'Bob', 30)");
    execute_distributed_dml("INSERT INTO users (id, name, age) VALUES (2, 'Carol', 17)");

    EXPECT_EQ(mock_executor.total_row_count("users"), 3u);

    // Also add "users" table to orders_backend for reading via INSERT...SELECT target
    mock_executor.get_backend("orders_backend")->add_table("users", {
        {"id",   SqlType::make_int(),        false},
        {"name", SqlType::make_varchar(255), true},
        {"age",  SqlType::make_int(),        true},
    });

    // Set up a target table "user_archive" on orders_backend
    catalog.add_table("", "user_archive", {
        {"id",   SqlType::make_int(),        false},
        {"name", SqlType::make_varchar(255), true},
        {"age",  SqlType::make_int(),        true},
    });
    shard_map.add_table({"user_archive", "", {{"orders_backend"}}});
    mock_executor.get_backend("orders_backend")->add_table("user_archive", {
        {"id",   SqlType::make_int(),        false},
        {"name", SqlType::make_varchar(255), true},
        {"age",  SqlType::make_int(),        true},
    });

    mock_executor.clear_sql_logs();

    // INSERT INTO user_archive SELECT * FROM users
    // users is sharded, user_archive is on orders_backend
    Parser<Dialect::MySQL> parser;
    const char* sql = "INSERT INTO user_archive SELECT * FROM users";
    auto pr = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(pr.status, ParseResult::OK);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dist(shard_map, catalog, parser.arena(),
                                             &mock_executor, &functions);
    PlanNode* dist_plan = dist.distribute_dml(plan);

    // Execute
    DmlResult total;
    total.success = true;
    collect_and_execute_remote_scans(dist_plan, total);
    EXPECT_TRUE(total.success);

    // user_archive on orders_backend should now have 3 rows
    EXPECT_EQ(mock_executor.get_backend("orders_backend")->mutable_sources["user_archive"]->row_count(), 3u);
}
