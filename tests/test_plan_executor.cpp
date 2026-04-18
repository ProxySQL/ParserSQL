#include <gtest/gtest.h>
#include "sql_engine/plan_executor.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_parser/parser.h"
#include <cstring>
#include <string>
#include <vector>
#include <set>

using namespace sql_engine;
using namespace sql_parser;

// Helper: allocate a persistent string in an arena
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

class PlanExecutorTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Parser<Dialect::MySQL> parser;

    const TableInfo* users_table = nullptr;
    InMemoryDataSource* users_source = nullptr;

    void SetUp() override {
        functions.register_builtins();

        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
            {"dept", SqlType::make_varchar(50),  true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        std::vector<Row> rows = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice")),   value_int(25), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob")),     value_int(30), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(3), value_string(arena_str(data_arena, "Carol")),   value_int(17), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(4), value_string(arena_str(data_arena, "Dave")),    value_int(22), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(5), value_string(arena_str(data_arena, "Eve")),     value_int(35), value_string(arena_str(data_arena, "Engineering"))}),
        };
        users_source = new InMemoryDataSource(users_table, std::move(rows));
    }

    void TearDown() override {
        delete users_source;
    }

    ResultSet run_query(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(r.ast);
        if (!plan) return {};

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_data_source("users", users_source);
        return executor.execute(plan);
    }
};

// SELECT * FROM users → all 5 rows
TEST_F(PlanExecutorTest, SelectStarFromUsers) {
    auto rs = run_query("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 4);
    // First row: Alice
    EXPECT_EQ(rs.rows[0].get(0).int_val, 1);
}

// SELECT name FROM users WHERE age > 18 → filtered + projected
TEST_F(PlanExecutorTest, SelectFilteredProjected) {
    auto rs = run_query("SELECT name FROM users WHERE age > 18");
    // Alice(25), Bob(30), Dave(22), Eve(35) = 4 rows
    EXPECT_EQ(rs.row_count(), 4u);
    EXPECT_EQ(rs.column_count, 1);
    // All results should be strings (names)
    for (const auto& row : rs.rows) {
        EXPECT_EQ(row.get(0).tag, Value::TAG_STRING);
    }
}

// SELECT * FROM users ORDER BY age DESC LIMIT 2 → sorted + limited
TEST_F(PlanExecutorTest, SelectSortedLimited) {
    auto rs = run_query("SELECT * FROM users ORDER BY age DESC LIMIT 2");
    EXPECT_EQ(rs.row_count(), 2u);
    // First should be Eve (35), second Bob (30)
    EXPECT_EQ(rs.rows[0].get(2).int_val, 35); // age
    EXPECT_EQ(rs.rows[1].get(2).int_val, 30); // age
}

// SELECT dept, COUNT(*) FROM users GROUP BY dept → aggregated
TEST_F(PlanExecutorTest, SelectGroupByDept) {
    auto rs = run_query("SELECT dept, COUNT(*) FROM users GROUP BY dept");
    EXPECT_EQ(rs.row_count(), 2u); // Engineering and Sales
    EXPECT_EQ(rs.column_count, 2);

    // Collect results
    std::set<std::string> depts;
    int64_t total = 0;
    for (const auto& row : rs.rows) {
        std::string dept(row.get(0).str_val.ptr, row.get(0).str_val.len);
        depts.insert(dept);
        total += row.get(1).int_val;
    }
    EXPECT_EQ(depts.size(), 2u);
    EXPECT_TRUE(depts.count("Engineering"));
    EXPECT_TRUE(depts.count("Sales"));
    EXPECT_EQ(total, 5); // 3 + 2
}

// SELECT 1 + 2 → single row [3]
TEST_F(PlanExecutorTest, SelectComputedNoFrom) {
    auto rs = run_query("SELECT 1 + 2");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).int_val, 3);
}

// SELECT DISTINCT dept FROM users → deduplicated
TEST_F(PlanExecutorTest, SelectDistinctDept) {
    auto rs = run_query("SELECT DISTINCT dept FROM users");
    EXPECT_EQ(rs.row_count(), 2u); // Engineering and Sales
    std::set<std::string> depts;
    for (const auto& row : rs.rows) {
        std::string dept(row.get(0).str_val.ptr, row.get(0).str_val.len);
        depts.insert(dept);
    }
    EXPECT_EQ(depts.size(), 2u);
}

// SELECT name FROM users WHERE name LIKE 'A%' → LIKE filter
TEST_F(PlanExecutorTest, SelectWithLike) {
    auto rs = run_query("SELECT name FROM users WHERE name LIKE 'A%'");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).tag, Value::TAG_STRING);
    std::string name(rs.rows[0].get(0).str_val.ptr, rs.rows[0].get(0).str_val.len);
    EXPECT_EQ(name, "Alice");
}

// Bug #23: SELECT name FROM users ORDER BY age DESC -- sort by age, return only name
TEST_F(PlanExecutorTest, OrderByColumnNotInSelectList) {
    auto rs = run_query("SELECT name FROM users ORDER BY age DESC");
    EXPECT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 1);
    // Sorted by age DESC: Eve(35), Bob(30), Alice(25), Dave(22), Carol(17)
    std::string first(rs.rows[0].get(0).str_val.ptr, rs.rows[0].get(0).str_val.len);
    std::string last(rs.rows[4].get(0).str_val.ptr, rs.rows[4].get(0).str_val.len);
    EXPECT_EQ(first, "Eve");
    EXPECT_EQ(last, "Carol");
}

// Bug #24: SELECT * FROM (SELECT COUNT(*) AS cnt FROM users) AS t
TEST_F(PlanExecutorTest, DerivedTableWithAggregate) {
    auto rs = run_query("SELECT * FROM (SELECT COUNT(*) AS cnt FROM users) AS t");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_FALSE(rs.rows[0].get(0).is_null());
    EXPECT_EQ(rs.rows[0].get(0).int_val, 5);
}

// Bug #24: SELECT * FROM (SELECT dept, COUNT(*) AS cnt FROM users GROUP BY dept) AS t
TEST_F(PlanExecutorTest, DerivedTableWithGroupByAggregate) {
    auto rs = run_query("SELECT * FROM (SELECT dept, COUNT(*) AS cnt FROM users GROUP BY dept) AS t");
    EXPECT_EQ(rs.row_count(), 2u);
    int64_t total = 0;
    for (const auto& row : rs.rows) {
        total += row.get(1).int_val;
    }
    EXPECT_EQ(total, 5); // 3 Engineering + 2 Sales
}

// ==========================================
// #28: Hash join operator tests
// ==========================================

class HashJoinTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Parser<Dialect::MySQL> parser;

    const TableInfo* users_table = nullptr;
    InMemoryDataSource* users_source = nullptr;
    const TableInfo* orders_table = nullptr;
    InMemoryDataSource* orders_source = nullptr;

    void SetUp() override {
        functions.register_builtins();

        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        catalog.add_table("", "orders", {
            {"order_id",  SqlType::make_int(),  false},
            {"user_id",   SqlType::make_int(),  true},
            {"amount",    SqlType::make_int(),  true},
        });
        orders_table = catalog.get_table(StringRef{"orders", 6});

        std::vector<Row> users = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice"))}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob"))}),
            build_row(data_arena, {value_int(3), value_string(arena_str(data_arena, "Carol"))}),
        };
        users_source = new InMemoryDataSource(users_table, std::move(users));

        std::vector<Row> orders = {
            build_row(data_arena, {value_int(101), value_int(1), value_int(500)}),
            build_row(data_arena, {value_int(102), value_int(1), value_int(300)}),
            build_row(data_arena, {value_int(103), value_int(2), value_int(750)}),
            build_row(data_arena, {value_int(104), value_int(99), value_int(100)}),
        };
        orders_source = new InMemoryDataSource(orders_table, std::move(orders));
    }

    void TearDown() override {
        delete users_source;
        delete orders_source;
    }

    ResultSet run_query(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(r.ast);
        if (!plan) return {};

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_data_source("users", users_source);
        executor.add_data_source("orders", orders_source);
        return executor.execute(plan);
    }
};

TEST_F(HashJoinTest, InnerEquiJoin_BasicResults) {
    // Hash join should produce the same results as nested-loop for equi-join
    auto rs = run_query("SELECT users.id, users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");
    // Alice has 2 orders, Bob has 1 order. Carol has none. user_id=99 has no user.
    EXPECT_EQ(rs.row_count(), 3u);
    int64_t total_amount = 0;
    for (const auto& row : rs.rows) {
        total_amount += row.get(2).int_val;
    }
    EXPECT_EQ(total_amount, 500 + 300 + 750);
}

TEST_F(HashJoinTest, InnerEquiJoin_MultipleMatches) {
    // Alice (id=1) has 2 orders -- verify both appear
    auto rs = run_query("SELECT users.name, orders.order_id FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1");
    EXPECT_EQ(rs.row_count(), 2u);
}

TEST_F(HashJoinTest, LeftEquiJoin_IncludesUnmatched) {
    // LEFT JOIN: Carol (id=3) has no orders -- should appear with NULLs
    auto rs = run_query("SELECT users.id, users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id");
    EXPECT_EQ(rs.row_count(), 4u); // Alice(2) + Bob(1) + Carol(1 with NULLs)
    // Find Carol's row
    bool found_carol = false;
    for (const auto& row : rs.rows) {
        if (row.get(0).int_val == 3) {
            found_carol = true;
            EXPECT_TRUE(row.get(2).is_null()) << "Carol's amount should be NULL";
        }
    }
    EXPECT_TRUE(found_carol) << "Carol should appear in LEFT JOIN results";
}

TEST_F(HashJoinTest, RightJoinIncludesUnmatchedRightRows) {
    auto rs = run_query(
        "SELECT users.id, users.name, orders.order_id, orders.user_id, orders.amount "
        "FROM users RIGHT JOIN orders ON users.id = orders.user_id");

    EXPECT_EQ(rs.row_count(), 4u);

    bool found_unmatched_order = false;
    int matched_rows = 0;
    for (const auto& row : rs.rows) {
        if (row.get(2).int_val == 104) {
            found_unmatched_order = true;
            EXPECT_TRUE(row.get(0).is_null());
            EXPECT_TRUE(row.get(1).is_null());
            EXPECT_EQ(row.get(3).int_val, 99);
            EXPECT_EQ(row.get(4).int_val, 100);
        } else {
            matched_rows++;
        }
    }

    EXPECT_EQ(matched_rows, 3);
    EXPECT_TRUE(found_unmatched_order);
}

TEST_F(HashJoinTest, FullJoinIncludesUnmatchedRowsFromBothSides) {
    auto rs = run_query(
        "SELECT users.id, users.name, orders.order_id, orders.user_id, orders.amount "
        "FROM users FULL JOIN orders ON users.id = orders.user_id");

    EXPECT_EQ(rs.row_count(), 5u);

    bool found_unmatched_user = false;
    bool found_unmatched_order = false;
    for (const auto& row : rs.rows) {
        if (!row.get(0).is_null() && row.get(0).int_val == 3) {
            found_unmatched_user = true;
            EXPECT_TRUE(row.get(2).is_null());
            EXPECT_TRUE(row.get(3).is_null());
            EXPECT_TRUE(row.get(4).is_null());
        }
        if (row.get(2).int_val == 104) {
            found_unmatched_order = true;
            EXPECT_TRUE(row.get(0).is_null());
            EXPECT_TRUE(row.get(1).is_null());
            EXPECT_EQ(row.get(3).int_val, 99);
            EXPECT_EQ(row.get(4).int_val, 100);
        }
    }

    EXPECT_TRUE(found_unmatched_user);
    EXPECT_TRUE(found_unmatched_order);
}
