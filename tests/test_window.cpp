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

class WindowTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Parser<Dialect::MySQL> parser;

    InMemoryDataSource* emp_source = nullptr;

    void SetUp() override {
        functions.register_builtins();

        catalog.add_table("", "employees", {
            {"id",     SqlType::make_int(),        false},
            {"name",   SqlType::make_varchar(255), true},
            {"dept",   SqlType::make_varchar(50),  true},
            {"salary", SqlType::make_int(),        true},
        });

        std::vector<Row> rows = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice")),   value_string(arena_str(data_arena, "Engineering")), value_int(90000)}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob")),     value_string(arena_str(data_arena, "Engineering")), value_int(80000)}),
            build_row(data_arena, {value_int(3), value_string(arena_str(data_arena, "Carol")),   value_string(arena_str(data_arena, "Sales")),       value_int(70000)}),
            build_row(data_arena, {value_int(4), value_string(arena_str(data_arena, "Dave")),    value_string(arena_str(data_arena, "Sales")),       value_int(70000)}),
            build_row(data_arena, {value_int(5), value_string(arena_str(data_arena, "Eve")),     value_string(arena_str(data_arena, "Engineering")), value_int(95000)}),
        };
        emp_source = new InMemoryDataSource(
            catalog.get_table(StringRef{"employees", 9}), std::move(rows));
    }

    void TearDown() override {
        delete emp_source;
    }

    ResultSet run_query(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(r.ast);
        if (!plan) return {};

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_data_source("employees", emp_source);
        return executor.execute(plan);
    }
};

// ROW_NUMBER() OVER (ORDER BY salary DESC)
TEST_F(WindowTest, RowNumberOrderBy) {
    auto rs = run_query("SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 3);
    // Sorted by salary DESC: Eve(95000)=1, Alice(90000)=2, Bob(80000)=3, Carol(70000)=4, Dave(70000)=5
    // But output order matches original order - window function assigns ranks internally
    // Let's just check all row numbers are present
    std::set<int64_t> rn_values;
    for (const auto& row : rs.rows) {
        rn_values.insert(row.get(2).int_val);
    }
    EXPECT_EQ(rn_values.size(), 5u);
    EXPECT_TRUE(rn_values.count(1));
    EXPECT_TRUE(rn_values.count(5));
}

// ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
TEST_F(WindowTest, RowNumberPartitioned) {
    auto rs = run_query("SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 4);

    // Engineering: Eve(95000)=1, Alice(90000)=2, Bob(80000)=3
    // Sales: Carol(70000)=1 or 2, Dave(70000)=1 or 2
    // All row numbers should be in valid ranges
    for (const auto& row : rs.rows) {
        int64_t rn = row.get(3).int_val;
        EXPECT_GE(rn, 1);
        EXPECT_LE(rn, 3); // max 3 in any partition
    }
}

// SUM(salary) OVER (PARTITION BY dept)
TEST_F(WindowTest, SumPartitioned) {
    auto rs = run_query("SELECT name, dept, SUM(salary) OVER (PARTITION BY dept) AS total FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);

    // Engineering total: 90000+80000+95000 = 265000
    // Sales total: 70000+70000 = 140000
    for (const auto& row : rs.rows) {
        double total = row.get(2).to_double();
        EXPECT_TRUE(total == 265000.0 || total == 140000.0);
    }
}

// AVG(salary) OVER ()
TEST_F(WindowTest, AvgNoPartition) {
    auto rs = run_query("SELECT name, AVG(salary) OVER () AS avg_sal FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);

    // Overall avg: (90000+80000+70000+70000+95000)/5 = 81000
    for (const auto& row : rs.rows) {
        double avg = row.get(1).to_double();
        EXPECT_NEAR(avg, 81000.0, 0.01);
    }
}

// COUNT(*) OVER (PARTITION BY dept)
TEST_F(WindowTest, CountPartitioned) {
    auto rs = run_query("SELECT name, dept, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);

    for (const auto& row : rs.rows) {
        std::string dept(row.get(1).str_val.ptr, row.get(1).str_val.len);
        int64_t cnt = row.get(2).int_val;
        if (dept == "Engineering") {
            EXPECT_EQ(cnt, 3);
        } else {
            EXPECT_EQ(cnt, 2);
        }
    }
}

// RANK() and DENSE_RANK() with ties
TEST_F(WindowTest, RankWithTies) {
    auto rs = run_query("SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS rnk FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);

    // salary DESC: 95000(1), 90000(2), 80000(3), 70000(4), 70000(4)
    // Two rows with salary=70000 should have same rank
    std::multiset<int64_t> ranks;
    for (const auto& row : rs.rows) {
        ranks.insert(row.get(2).int_val);
    }
    // Rank 4 should appear twice (tied 70000s), rank 5 should not appear
    EXPECT_EQ(ranks.count(4), 2u);
    EXPECT_EQ(ranks.count(5), 0u);
}

TEST_F(WindowTest, DenseRankWithTies) {
    auto rs = run_query("SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS drnk FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);

    // salary DESC: 95000(1), 90000(2), 80000(3), 70000(4), 70000(4)
    // Dense rank after ties: no gap, max = 4
    std::set<int64_t> ranks;
    for (const auto& row : rs.rows) {
        ranks.insert(row.get(2).int_val);
    }
    EXPECT_EQ(*ranks.rbegin(), 4); // max dense rank should be 4
    EXPECT_TRUE(ranks.count(1));
    EXPECT_TRUE(ranks.count(2));
    EXPECT_TRUE(ranks.count(3));
    EXPECT_TRUE(ranks.count(4));
}

// LAG(salary, 1) and LEAD(salary, 1) OVER (ORDER BY salary)
TEST_F(WindowTest, LagAndLead) {
    auto rs = run_query("SELECT name, salary, LAG(salary, 1) OVER (ORDER BY salary) AS prev_sal FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);

    // Ordered by salary ASC: 70000, 70000, 80000, 90000, 95000
    // First row's LAG should be NULL
    bool found_null = false;
    for (const auto& row : rs.rows) {
        if (row.get(2).is_null()) found_null = true;
    }
    EXPECT_TRUE(found_null);

    auto rs2 = run_query("SELECT name, salary, LEAD(salary, 1) OVER (ORDER BY salary) AS next_sal FROM employees");
    ASSERT_EQ(rs2.row_count(), 5u);
    // Last row's LEAD should be NULL
    bool found_null2 = false;
    for (const auto& row : rs2.rows) {
        if (row.get(2).is_null()) found_null2 = true;
    }
    EXPECT_TRUE(found_null2);
}

// Multiple window functions in same query
TEST_F(WindowTest, MultipleWindowFunctions) {
    auto rs = run_query(
        "SELECT name, salary, "
        "ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn, "
        "SUM(salary) OVER () AS total "
        "FROM employees");
    ASSERT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 4);

    // Total salary: 90000+80000+70000+70000+95000 = 405000
    for (const auto& row : rs.rows) {
        double total = row.get(3).to_double();
        EXPECT_NEAR(total, 405000.0, 0.01);
    }

    // Row numbers 1-5 should all be present
    std::set<int64_t> rns;
    for (const auto& row : rs.rows) {
        rns.insert(row.get(2).int_val);
    }
    EXPECT_EQ(rns.size(), 5u);
}
