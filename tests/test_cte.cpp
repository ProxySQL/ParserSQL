#include <gtest/gtest.h>
#include "sql_engine/plan_executor.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_parser/parser.h"
#include <cstring>
#include <string>
#include <vector>

using namespace sql_engine;
using namespace sql_parser;

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

class CteTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Parser<Dialect::MySQL> parser;

    InMemoryDataSource* users_source = nullptr;

    void SetUp() override {
        functions.register_builtins();

        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
            {"dept", SqlType::make_varchar(50),  true},
        });

        std::vector<Row> rows = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice")),   value_int(25), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob")),     value_int(30), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(3), value_string(arena_str(data_arena, "Carol")),   value_int(17), value_string(arena_str(data_arena, "Engineering"))}),
            build_row(data_arena, {value_int(4), value_string(arena_str(data_arena, "Dave")),    value_int(22), value_string(arena_str(data_arena, "Sales"))}),
            build_row(data_arena, {value_int(5), value_string(arena_str(data_arena, "Eve")),     value_int(35), value_string(arena_str(data_arena, "Engineering"))}),
        };
        users_source = new InMemoryDataSource(
            catalog.get_table(StringRef{"users", 5}), std::move(rows));
    }

    void TearDown() override {
        delete users_source;
    }

    ResultSet run_query(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return {};

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_data_source("users", users_source);
        return executor.execute_with_cte(r.ast);
    }
};

// Simple CTE: WITH active AS (SELECT * FROM users WHERE age > 18) SELECT * FROM active
TEST_F(CteTest, SimpleCte) {
    auto rs = run_query("WITH active AS (SELECT * FROM users WHERE age > 18) SELECT * FROM active");
    // Alice(25), Bob(30), Dave(22), Eve(35) = 4 rows
    ASSERT_EQ(rs.row_count(), 4u);
}

// CTE with aggregation
TEST_F(CteTest, CteWithAggregation) {
    auto rs = run_query(
        "WITH dept_counts AS ("
        "  SELECT dept, COUNT(*) AS cnt FROM users GROUP BY dept"
        ") SELECT * FROM dept_counts WHERE cnt > 2");
    // Engineering has 3 users, Sales has 2
    // Only Engineering has cnt > 2
    ASSERT_EQ(rs.row_count(), 1u);
    // The dept should be Engineering
    EXPECT_EQ(rs.rows[0].get(0).tag, Value::TAG_STRING);
}

// Multiple CTEs
TEST_F(CteTest, MultipleCtes) {
    auto rs = run_query(
        "WITH eng AS (SELECT * FROM users WHERE dept = 'Engineering'), "
        "     sales AS (SELECT * FROM users WHERE dept = 'Sales') "
        "SELECT * FROM eng");
    // Engineering: Alice, Carol, Eve = 3 rows
    ASSERT_EQ(rs.row_count(), 3u);
}

// CTE referenced in a filtered query
TEST_F(CteTest, CteWithFilter) {
    auto rs = run_query(
        "WITH adults AS (SELECT name, age FROM users WHERE age >= 25) "
        "SELECT * FROM adults WHERE age >= 30");
    // Bob(30), Eve(35) = 2 rows
    ASSERT_EQ(rs.row_count(), 2u);
}
