// test_distributed_planner.cpp — Tests for distributed query planner
//
// Uses MockRemoteExecutor that parses+executes SQL internally to validate
// both plan decomposition and generated SQL correctness.
// Tests all 6 decomposition cases with correctness verification.

#include <gtest/gtest.h>
#include "sql_engine/distributed_planner.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/optimizer.h"
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
#include <cmath>

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

// ---- MockRemoteExecutor ----
// Each backend has its own catalog + data source.
// execute() parses the incoming SQL, builds plan, executes locally.

struct BackendData {
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    std::map<std::string, std::vector<Row>> table_rows;
    Arena data_arena{65536, 1048576};

    BackendData() {
        functions.register_builtins();
    }
};

class MockRemoteExecutor : public RemoteExecutor {
public:
    void add_backend(const std::string& name) {
        backends_[name] = std::make_unique<BackendData>();
    }

    BackendData* get_backend(const std::string& name) {
        auto it = backends_.find(name);
        return it != backends_.end() ? it->second.get() : nullptr;
    }

    void add_table_to_backend(const std::string& backend_name,
                               const char* table_name,
                               std::initializer_list<ColumnDef> columns) {
        auto* bd = get_backend(backend_name);
        if (!bd) return;
        bd->catalog.add_table("", table_name, columns);
    }

    void add_rows_to_backend(const std::string& backend_name,
                              const char* table_name,
                              std::vector<Row> rows) {
        auto* bd = get_backend(backend_name);
        if (!bd) return;
        bd->table_rows[table_name] = std::move(rows);
    }

    ResultSet execute(const char* backend_name, StringRef sql) override {
        auto it = backends_.find(backend_name);
        if (it == backends_.end()) return {};

        BackendData* bd = it->second.get();

        // Parse the SQL
        Parser<Dialect::MySQL> parser;
        std::string sql_str(sql.ptr, sql.len);
        auto pr = parser.parse(sql_str.c_str(), sql_str.size());
        if (pr.status != ParseResult::OK || !pr.ast) {
            return {};
        }

        // Build plan
        PlanBuilder<Dialect::MySQL> builder(bd->catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        if (!plan) return {};

        // Execute
        PlanExecutor<Dialect::MySQL> executor(bd->functions, bd->catalog, parser.arena());

        // Register data sources
        for (auto& [tname, rows] : bd->table_rows) {
            const TableInfo* ti = bd->catalog.get_table(
                StringRef{tname.c_str(), static_cast<uint32_t>(tname.size())});
            if (ti) {
                auto* ds = new InMemoryDataSource(ti, rows);
                executor.add_data_source(tname.c_str(), ds);
                temp_sources_.push_back(std::unique_ptr<DataSource>(ds));
            }
        }

        return executor.execute(plan);
    }

private:
    std::map<std::string, std::unique_ptr<BackendData>> backends_;
    std::vector<std::unique_ptr<DataSource>> temp_sources_;
};

// ---- Test fixture ----

class DistributedPlannerTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;        // "global" catalog for the planner
    FunctionRegistry<Dialect::MySQL> functions;
    ShardMap shard_map;
    MockRemoteExecutor mock_executor;

    // For local (reference) execution
    InMemoryCatalog local_catalog;
    FunctionRegistry<Dialect::MySQL> local_functions;

    // Users table: 15 rows split across 3 shards
    const TableInfo* users_table = nullptr;
    std::vector<Row> all_users;

    // Orders table: unsharded, on a separate backend
    const TableInfo* orders_table = nullptr;
    std::vector<Row> all_orders;

    void SetUp() override {
        functions.register_builtins();
        local_functions.register_builtins();

        // Define users table in global catalog
        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
            {"dept", SqlType::make_varchar(50),  true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        // Define orders table in global catalog
        catalog.add_table("", "orders", {
            {"order_id",  SqlType::make_int(),        false},
            {"user_id",   SqlType::make_int(),        true},
            {"amount",    SqlType::make_int(),        true},
        });
        orders_table = catalog.get_table(StringRef{"orders", 6});

        // Same tables in local catalog
        local_catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
            {"dept", SqlType::make_varchar(50),  true},
        });
        local_catalog.add_table("", "orders", {
            {"order_id",  SqlType::make_int(),        false},
            {"user_id",   SqlType::make_int(),        true},
            {"amount",    SqlType::make_int(),        true},
        });

        // Build user data: 15 rows
        all_users = {
            build_row(data_arena, {value_int(1),  value_string(arena_str(data_arena, "Alice")),   value_int(25), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(2),  value_string(arena_str(data_arena, "Bob")),     value_int(30), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(3),  value_string(arena_str(data_arena, "Carol")),   value_int(17), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(4),  value_string(arena_str(data_arena, "Dave")),    value_int(22), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(5),  value_string(arena_str(data_arena, "Eve")),     value_int(35), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(6),  value_string(arena_str(data_arena, "Frank")),   value_int(28), value_string(arena_str(data_arena, "HR"))}),
            build_row(data_arena, {value_int(7),  value_string(arena_str(data_arena, "Grace")),   value_int(19), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(8),  value_string(arena_str(data_arena, "Hank")),    value_int(45), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(9),  value_string(arena_str(data_arena, "Ivy")),     value_int(23), value_string(arena_str(data_arena, "HR"))}),
            build_row(data_arena, {value_int(10), value_string(arena_str(data_arena, "Jack")),    value_int(31), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(11), value_string(arena_str(data_arena, "Karen")),   value_int(27), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(12), value_string(arena_str(data_arena, "Leo")),     value_int(40), value_string(arena_str(data_arena, "HR"))}),
            build_row(data_arena, {value_int(13), value_string(arena_str(data_arena, "Mia")),     value_int(21), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(14), value_string(arena_str(data_arena, "Nate")),    value_int(33), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(15), value_string(arena_str(data_arena, "Olivia")),  value_int(29), value_string(arena_str(data_arena, "HR"))}),
        };

        // Orders data
        all_orders = {
            build_row(data_arena, {value_int(101), value_int(1), value_int(500)}),
            build_row(data_arena, {value_int(102), value_int(2), value_int(300)}),
            build_row(data_arena, {value_int(103), value_int(5), value_int(1000)}),
            build_row(data_arena, {value_int(104), value_int(8), value_int(750)}),
            build_row(data_arena, {value_int(105), value_int(10), value_int(200)}),
        };

        // Configure shard map
        // users: sharded across 3 backends
        shard_map.add_table(TableShardConfig{
            "users", "id",
            {ShardInfo{"shard_1"}, ShardInfo{"shard_2"}, ShardInfo{"shard_3"}}
        });
        // orders: unsharded on backend_orders
        shard_map.add_table(TableShardConfig{
            "orders", "",
            {ShardInfo{"backend_orders"}}
        });

        // Set up mock executor backends
        mock_executor.add_backend("shard_1");
        mock_executor.add_backend("shard_2");
        mock_executor.add_backend("shard_3");
        mock_executor.add_backend("backend_orders");

        auto user_cols = std::initializer_list<ColumnDef>{
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
            {"dept", SqlType::make_varchar(50),  true},
        };

        mock_executor.add_table_to_backend("shard_1", "users", user_cols);
        mock_executor.add_table_to_backend("shard_2", "users", user_cols);
        mock_executor.add_table_to_backend("shard_3", "users", user_cols);

        mock_executor.add_table_to_backend("backend_orders", "orders", {
            {"order_id",  SqlType::make_int(),        false},
            {"user_id",   SqlType::make_int(),        true},
            {"amount",    SqlType::make_int(),        true},
        });

        // Split users across shards: 5 each
        std::vector<Row> shard1_rows(all_users.begin(), all_users.begin() + 5);
        std::vector<Row> shard2_rows(all_users.begin() + 5, all_users.begin() + 10);
        std::vector<Row> shard3_rows(all_users.begin() + 10, all_users.begin() + 15);

        mock_executor.add_rows_to_backend("shard_1", "users", shard1_rows);
        mock_executor.add_rows_to_backend("shard_2", "users", shard2_rows);
        mock_executor.add_rows_to_backend("shard_3", "users", shard3_rows);

        mock_executor.add_rows_to_backend("backend_orders", "orders", all_orders);
    }

    // Stabilize result: deep-copy all row data into data_arena so they outlive parsers
    void stabilize_result(ResultSet& rs) {
        for (auto& row : rs.rows) {
            // Copy the values array into data_arena
            Value* new_vals = static_cast<Value*>(
                data_arena.allocate(sizeof(Value) * row.column_count));
            for (uint16_t i = 0; i < row.column_count; ++i) {
                new_vals[i] = row.values[i];
                // Deep-copy string values
                if (new_vals[i].tag == Value::TAG_STRING &&
                    new_vals[i].str_val.ptr && new_vals[i].str_val.len > 0) {
                    char* buf = static_cast<char*>(
                        data_arena.allocate(new_vals[i].str_val.len));
                    std::memcpy(buf, new_vals[i].str_val.ptr, new_vals[i].str_val.len);
                    new_vals[i].str_val.ptr = buf;
                }
            }
            row.values = new_vals;
        }
    }

    // Execute locally: all data in one place
    ResultSet execute_local(const char* sql) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK || !pr.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(local_catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        if (!plan) return {};

        PlanExecutor<Dialect::MySQL> executor(local_functions, local_catalog, parser.arena());

        const TableInfo* ut = local_catalog.get_table(StringRef{"users", 5});
        auto* users_ds = new InMemoryDataSource(ut, all_users);
        executor.add_data_source("users", users_ds);

        const TableInfo* ot = local_catalog.get_table(StringRef{"orders", 6});
        auto* orders_ds = new InMemoryDataSource(ot, all_orders);
        executor.add_data_source("orders", orders_ds);

        auto rs = executor.execute(plan);
        stabilize_result(rs);
        delete users_ds;
        delete orders_ds;
        return rs;
    }

    // Execute distributed: data split across mock backends
    ResultSet execute_distributed(const char* sql) {
        Parser<Dialect::MySQL> parser;
        auto pr = parser.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK || !pr.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(pr.ast);
        if (!plan) return {};

        // Distribute
        DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
        PlanNode* dist_plan = dp.distribute(plan);

        // Execute distributed plan
        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.set_remote_executor(&mock_executor);
        auto rs = executor.execute(dist_plan);
        stabilize_result(rs);
        return rs;
    }

    // Compare two result sets (unordered comparison)
    static bool compare_results_unordered(const ResultSet& a, const ResultSet& b) {
        if (a.row_count() != b.row_count()) return false;
        auto to_set = [](const ResultSet& rs) {
            std::multiset<std::string> s;
            for (const auto& row : rs.rows) {
                s.insert(row_to_string(row));
            }
            return s;
        };
        return to_set(a) == to_set(b);
    }

    // Compare two result sets (ordered comparison)
    static bool compare_results_ordered(const ResultSet& a, const ResultSet& b) {
        if (a.row_count() != b.row_count()) return false;
        for (size_t i = 0; i < a.row_count(); ++i) {
            if (row_to_string(a.rows[i]) != row_to_string(b.rows[i])) return false;
        }
        return true;
    }

    static std::string row_to_string(const Row& row) {
        std::string s;
        for (uint16_t i = 0; i < row.column_count; ++i) {
            if (i > 0) s += "|";
            const Value& v = row.values[i];
            if (v.is_null()) { s += "NULL"; continue; }
            switch (v.tag) {
                case Value::TAG_BOOL: s += v.bool_val ? "T" : "F"; break;
                case Value::TAG_INT64: s += std::to_string(v.int_val); break;
                case Value::TAG_UINT64: s += std::to_string(v.uint_val); break;
                case Value::TAG_DOUBLE: {
                    // Round to avoid floating point noise
                    char buf[64];
                    snprintf(buf, sizeof(buf), "%.6g", v.double_val);
                    s += buf;
                    break;
                }
                case Value::TAG_STRING:
                    s.append(v.str_val.ptr, v.str_val.len);
                    break;
                default: s += "?"; break;
            }
        }
        return s;
    }

    // Walk plan tree and find nodes of a given type
    static void find_nodes(PlanNode* node, PlanNodeType type, std::vector<PlanNode*>& out) {
        if (!node) return;
        if (node->type == type) out.push_back(node);
        if (node->type == PlanNodeType::MERGE_AGGREGATE) {
            // Traverse only the children array, not left/right
            for (uint16_t i = 0; i < node->merge_aggregate.child_count; ++i)
                find_nodes(node->merge_aggregate.children[i], type, out);
            return;
        }
        if (node->type == PlanNodeType::MERGE_SORT) {
            for (uint16_t i = 0; i < node->merge_sort.child_count; ++i)
                find_nodes(node->merge_sort.children[i], type, out);
            return;
        }
        find_nodes(node->left, type, out);
        find_nodes(node->right, type, out);
    }
};

// ==========================================
// Case 1: Unsharded passthrough
// ==========================================

TEST_F(DistributedPlannerTest, Case1_UnshardedPassthrough_PlanStructure) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT * FROM orders", 20);
    ASSERT_EQ(pr.status, ParseResult::OK);

    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
    PlanNode* dist = dp.distribute(plan);
    ASSERT_NE(dist, nullptr);

    // Should be a single REMOTE_SCAN
    EXPECT_EQ(dist->type, PlanNodeType::REMOTE_SCAN);
    EXPECT_STREQ(dist->remote_scan.backend_name, "backend_orders");
}

TEST_F(DistributedPlannerTest, Case1_UnshardedPassthrough_Correctness) {
    auto local_rs = execute_local("SELECT * FROM orders");
    auto dist_rs = execute_distributed("SELECT * FROM orders");

    EXPECT_EQ(local_rs.row_count(), 5u);
    EXPECT_EQ(dist_rs.row_count(), 5u);
    EXPECT_TRUE(compare_results_unordered(local_rs, dist_rs));
}

TEST_F(DistributedPlannerTest, Case1_UnshardedWithFilter_Correctness) {
    auto local_rs = execute_local("SELECT * FROM orders WHERE amount > 300");
    auto dist_rs = execute_distributed("SELECT * FROM orders WHERE amount > 300");

    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());
    EXPECT_TRUE(compare_results_unordered(local_rs, dist_rs));
}

// ==========================================
// Case 2: Sharded scan (3 shards)
// ==========================================

TEST_F(DistributedPlannerTest, Case2_ShardedScan_PlanStructure) {
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse("SELECT * FROM users", 19);
    ASSERT_EQ(pr.status, ParseResult::OK);

    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
    PlanNode* dist = dp.distribute(plan);
    ASSERT_NE(dist, nullptr);

    // Should have 3 REMOTE_SCAN nodes connected by SET_OP (UNION ALL)
    std::vector<PlanNode*> remote_scans;
    find_nodes(dist, PlanNodeType::REMOTE_SCAN, remote_scans);
    EXPECT_EQ(remote_scans.size(), 3u);

    std::vector<PlanNode*> set_ops;
    find_nodes(dist, PlanNodeType::SET_OP, set_ops);
    EXPECT_EQ(set_ops.size(), 2u); // left-deep chain of 2 UNION ALLs for 3 shards
}

TEST_F(DistributedPlannerTest, Case2_ShardedScan_Correctness) {
    auto local_rs = execute_local("SELECT * FROM users");
    auto dist_rs = execute_distributed("SELECT * FROM users");

    EXPECT_EQ(local_rs.row_count(), 15u);
    EXPECT_EQ(dist_rs.row_count(), 15u);
    EXPECT_TRUE(compare_results_unordered(local_rs, dist_rs));
}

TEST_F(DistributedPlannerTest, Case2_ShardedScanWithFilter_Correctness) {
    auto local_rs = execute_local("SELECT * FROM users WHERE age > 25");
    auto dist_rs = execute_distributed("SELECT * FROM users WHERE age > 25");

    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());
    EXPECT_TRUE(compare_results_unordered(local_rs, dist_rs));
}

// ==========================================
// Case 3: Sharded aggregate with GROUP BY
// ==========================================

TEST_F(DistributedPlannerTest, Case3_ShardedAggregate_PlanStructure) {
    const char* sql = "SELECT dept, COUNT(*) FROM users GROUP BY dept";
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(pr.status, ParseResult::OK);

    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
    PlanNode* dist = dp.distribute(plan);
    ASSERT_NE(dist, nullptr);

    // Should have a MERGE_AGGREGATE with 3 REMOTE_SCAN children
    std::vector<PlanNode*> merges;
    find_nodes(dist, PlanNodeType::MERGE_AGGREGATE, merges);
    EXPECT_GE(merges.size(), 1u);

    std::vector<PlanNode*> remote_scans;
    find_nodes(dist, PlanNodeType::REMOTE_SCAN, remote_scans);
    EXPECT_EQ(remote_scans.size(), 3u);
}

TEST_F(DistributedPlannerTest, Case3_ShardedAggregate_COUNT_Correctness) {
    // COUNT(*) GROUP BY dept
    auto local_rs = execute_local("SELECT dept, COUNT(*) FROM users GROUP BY dept");
    auto dist_rs = execute_distributed("SELECT dept, COUNT(*) FROM users GROUP BY dept");

    // Both should have groups
    ASSERT_GT(local_rs.row_count(), 0u);
    ASSERT_GT(dist_rs.row_count(), 0u);
    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());

    // Compare: local gives exact counts, distributed merges them
    // Build maps dept->count for each
    std::map<std::string, int64_t> local_map, dist_map;
    for (const auto& row : local_rs.rows) {
        ASSERT_GE(row.column_count, 2);
        Value v0 = row.get(0);
        ASSERT_EQ(v0.tag, Value::TAG_STRING) << "Expected string for dept column";
        std::string dept(v0.str_val.ptr, v0.str_val.len);
        local_map[dept] = row.get(1).to_int64();
    }
    for (const auto& row : dist_rs.rows) {
        ASSERT_GE(row.column_count, 2);
        Value v0 = row.get(0);
        ASSERT_EQ(v0.tag, Value::TAG_STRING) << "Expected string for dept column in distributed result";
        std::string dept(v0.str_val.ptr, v0.str_val.len);
        // MergeAggregate SUM_OF_COUNTS returns double, convert
        dist_map[dept] = static_cast<int64_t>(row.get(1).to_double());
    }

    EXPECT_EQ(local_map.size(), dist_map.size());
    for (auto& [dept, count] : local_map) {
        EXPECT_EQ(count, dist_map[dept]) << "Mismatch for dept: " << dept;
    }
}

TEST_F(DistributedPlannerTest, Case3_ShardedAggregate_SUM_Correctness) {
    auto local_rs = execute_local("SELECT dept, SUM(age) FROM users GROUP BY dept");
    auto dist_rs = execute_distributed("SELECT dept, SUM(age) FROM users GROUP BY dept");

    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());

    std::map<std::string, double> local_map, dist_map;
    for (const auto& row : local_rs.rows) {
        std::string dept(row.get(0).str_val.ptr, row.get(0).str_val.len);
        local_map[dept] = row.get(1).to_double();
    }
    for (const auto& row : dist_rs.rows) {
        std::string dept(row.get(0).str_val.ptr, row.get(0).str_val.len);
        dist_map[dept] = row.get(1).to_double();
    }

    for (auto& [dept, sum] : local_map) {
        EXPECT_NEAR(sum, dist_map[dept], 0.01) << "Mismatch for dept: " << dept;
    }
}

TEST_F(DistributedPlannerTest, Case3_ShardedAggregate_MIN_MAX_Correctness) {
    auto local_rs = execute_local("SELECT dept, MIN(age), MAX(age) FROM users GROUP BY dept");
    auto dist_rs = execute_distributed("SELECT dept, MIN(age), MAX(age) FROM users GROUP BY dept");

    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());

    std::map<std::string, std::pair<double, double>> local_map, dist_map;
    for (const auto& row : local_rs.rows) {
        std::string dept(row.get(0).str_val.ptr, row.get(0).str_val.len);
        local_map[dept] = {row.get(1).to_double(), row.get(2).to_double()};
    }
    for (const auto& row : dist_rs.rows) {
        std::string dept(row.get(0).str_val.ptr, row.get(0).str_val.len);
        dist_map[dept] = {row.get(1).to_double(), row.get(2).to_double()};
    }

    for (auto& [dept, vals] : local_map) {
        EXPECT_NEAR(vals.first, dist_map[dept].first, 0.01) << "MIN mismatch: " << dept;
        EXPECT_NEAR(vals.second, dist_map[dept].second, 0.01) << "MAX mismatch: " << dept;
    }
}

// ==========================================
// Case 4: Sharded sort + limit
// ==========================================

TEST_F(DistributedPlannerTest, Case4_ShardedSortLimit_PlanStructure) {
    const char* sql = "SELECT * FROM users ORDER BY name LIMIT 5";
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(pr.status, ParseResult::OK);

    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
    PlanNode* dist = dp.distribute(plan);
    ASSERT_NE(dist, nullptr);

    // Should have LIMIT -> MERGE_SORT with 3 REMOTE_SCAN children
    // Top node should be LIMIT
    EXPECT_EQ(dist->type, PlanNodeType::LIMIT);

    std::vector<PlanNode*> merge_sorts;
    find_nodes(dist, PlanNodeType::MERGE_SORT, merge_sorts);
    EXPECT_GE(merge_sorts.size(), 1u);

    std::vector<PlanNode*> remote_scans;
    find_nodes(dist, PlanNodeType::REMOTE_SCAN, remote_scans);
    EXPECT_EQ(remote_scans.size(), 3u);

    // Each remote scan should include ORDER BY and LIMIT
    for (auto* rs : remote_scans) {
        std::string sql_str(rs->remote_scan.remote_sql, rs->remote_scan.remote_sql_len);
        EXPECT_NE(sql_str.find("ORDER BY"), std::string::npos) << "Missing ORDER BY in: " << sql_str;
        EXPECT_NE(sql_str.find("LIMIT"), std::string::npos) << "Missing LIMIT in: " << sql_str;
    }
}

TEST_F(DistributedPlannerTest, Case4_ShardedSortLimit_Correctness) {
    auto local_rs = execute_local("SELECT * FROM users ORDER BY name LIMIT 5");
    auto dist_rs = execute_distributed("SELECT * FROM users ORDER BY name LIMIT 5");

    EXPECT_EQ(local_rs.row_count(), 5u);
    EXPECT_EQ(dist_rs.row_count(), 5u);

    // For ORDER BY queries, results must be in the same order
    EXPECT_TRUE(compare_results_ordered(local_rs, dist_rs));
}

TEST_F(DistributedPlannerTest, Case4_ShardedSortDesc_Correctness) {
    auto local_rs = execute_local("SELECT * FROM users ORDER BY age DESC LIMIT 3");
    auto dist_rs = execute_distributed("SELECT * FROM users ORDER BY age DESC LIMIT 3");

    EXPECT_EQ(local_rs.row_count(), 3u);
    EXPECT_EQ(dist_rs.row_count(), 3u);
    EXPECT_TRUE(compare_results_ordered(local_rs, dist_rs));
}

// ==========================================
// Case 5: Cross-backend join
// ==========================================

TEST_F(DistributedPlannerTest, Case5_CrossBackendJoin_PlanStructure) {
    const char* sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(pr.status, ParseResult::OK);

    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
    PlanNode* dist = dp.distribute(plan);
    ASSERT_NE(dist, nullptr);

    // Should be a local JOIN with RemoteScans
    EXPECT_EQ(dist->type, PlanNodeType::JOIN);

    std::vector<PlanNode*> remote_scans;
    find_nodes(dist, PlanNodeType::REMOTE_SCAN, remote_scans);
    // 3 for sharded users + 1 for orders = 4, or some variation
    EXPECT_GE(remote_scans.size(), 2u);
}

TEST_F(DistributedPlannerTest, Case5_CrossBackendJoin_Correctness) {
    // For cross-backend join, local catalog needs both tables
    auto local_rs = execute_local("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    auto dist_rs = execute_distributed("SELECT * FROM users JOIN orders ON users.id = orders.user_id");

    // Both should have 5 matching rows (orders reference user_ids 1,2,5,8,10)
    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());
    EXPECT_TRUE(compare_results_unordered(local_rs, dist_rs));
}

// ==========================================
// Case 6: Sharded DISTINCT
// ==========================================

TEST_F(DistributedPlannerTest, Case6_ShardedDistinct_PlanStructure) {
    const char* sql = "SELECT DISTINCT dept FROM users";
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(pr.status, ParseResult::OK);

    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    ASSERT_NE(plan, nullptr);

    DistributedPlanner<Dialect::MySQL> dp(shard_map, catalog, parser.arena());
    PlanNode* dist = dp.distribute(plan);
    ASSERT_NE(dist, nullptr);

    // Should have DISTINCT at top with RemoteScans below
    // The remote SQL should include DISTINCT
    std::vector<PlanNode*> remote_scans;
    find_nodes(dist, PlanNodeType::REMOTE_SCAN, remote_scans);
    EXPECT_EQ(remote_scans.size(), 3u);

    for (auto* rs : remote_scans) {
        std::string sql_str(rs->remote_scan.remote_sql, rs->remote_scan.remote_sql_len);
        EXPECT_NE(sql_str.find("DISTINCT"), std::string::npos) << "Missing DISTINCT in: " << sql_str;
    }
}

TEST_F(DistributedPlannerTest, Case6_ShardedDistinct_Correctness) {
    auto local_rs = execute_local("SELECT DISTINCT dept FROM users");
    auto dist_rs = execute_distributed("SELECT DISTINCT dept FROM users");

    // Should be 3 distinct departments: Engineering, Sales, HR
    EXPECT_EQ(local_rs.row_count(), dist_rs.row_count());
    EXPECT_TRUE(compare_results_unordered(local_rs, dist_rs));
}

// ==========================================
// Additional tests
// ==========================================

TEST_F(DistributedPlannerTest, ShardMapBasics) {
    EXPECT_TRUE(shard_map.is_sharded(StringRef{"users", 5}));
    EXPECT_FALSE(shard_map.is_sharded(StringRef{"orders", 6}));
    EXPECT_EQ(shard_map.get_shards(StringRef{"users", 5}).size(), 3u);
    EXPECT_EQ(shard_map.get_shards(StringRef{"orders", 6}).size(), 1u);
    EXPECT_TRUE(shard_map.has_table(StringRef{"users", 5}));
    EXPECT_TRUE(shard_map.has_table(StringRef{"orders", 6}));
    EXPECT_FALSE(shard_map.has_table(StringRef{"nonexistent", 11}));
}

TEST_F(DistributedPlannerTest, RemoteQueryBuilder_Basic) {
    Arena arena(4096, 65536);
    RemoteQueryBuilder<Dialect::MySQL> qb(arena);

    auto sql = qb.build_select(users_table, nullptr, nullptr, 0,
                                nullptr, 0, nullptr, nullptr, 0, -1, false);
    std::string s(sql.ptr, sql.len);
    EXPECT_NE(s.find("SELECT"), std::string::npos);
    EXPECT_NE(s.find("users"), std::string::npos);
}

TEST_F(DistributedPlannerTest, Case2_ShardedScan_AllRowsPresent) {
    auto dist_rs = execute_distributed("SELECT * FROM users");
    EXPECT_EQ(dist_rs.row_count(), 15u);

    // Verify all IDs 1-15 are present
    std::set<int64_t> ids;
    for (const auto& row : dist_rs.rows) {
        ids.insert(row.get(0).int_val);
    }
    for (int64_t i = 1; i <= 15; ++i) {
        EXPECT_TRUE(ids.count(i)) << "Missing user id: " << i;
    }
}
