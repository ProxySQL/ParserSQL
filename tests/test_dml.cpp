// test_dml.cpp -- Local DML execution tests (INSERT, UPDATE, DELETE)

#include <gtest/gtest.h>
#include "sql_engine/plan_executor.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_engine/function_registry.h"
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

class DmlTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Parser<Dialect::MySQL> parser;

    const TableInfo* users_table = nullptr;
    InMemoryMutableDataSource* users_source = nullptr;

    void SetUp() override {
        functions.register_builtins();

        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        std::vector<Row> initial = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice")), value_int(25)}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob")),   value_int(30)}),
            build_row(data_arena, {value_int(3), value_string(arena_str(data_arena, "Carol")), value_int(17)}),
        };
        users_source = new InMemoryMutableDataSource(users_table, data_arena, std::move(initial));
    }

    void TearDown() override {
        delete users_source;
    }

    DmlResult run_dml(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) {
            DmlResult dr;
            dr.error_message = "parse error";
            return dr;
        }

        DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
        PlanNode* plan = dml_builder.build(r.ast);
        if (!plan) {
            DmlResult dr;
            dr.error_message = "plan build error";
            return dr;
        }

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_mutable_data_source("users", users_source);
        return executor.execute_dml(plan);
    }

    ResultSet run_select(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return {};

        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(r.ast);
        if (!plan) return {};

        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_mutable_data_source("users", users_source);
        return executor.execute(plan);
    }
};

// INSERT single row
TEST_F(DmlTest, InsertSingleRow) {
    EXPECT_EQ(users_source->row_count(), 3u);

    auto result = run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);
    EXPECT_EQ(users_source->row_count(), 4u);

    // Verify the inserted row via SELECT
    auto rs = run_select("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 4u);
    // Last row should be Dave
    EXPECT_EQ(rs.rows[3].get(0).int_val, 4);
    EXPECT_EQ(rs.rows[3].get(2).int_val, 22);
}

// INSERT multiple rows
TEST_F(DmlTest, InsertMultipleRows) {
    auto result = run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22), (5, 'Eve', 35)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 2u);
    EXPECT_EQ(users_source->row_count(), 5u);
}

// UPDATE with WHERE
TEST_F(DmlTest, UpdateWithWhere) {
    auto result = run_dml("UPDATE users SET age = 26 WHERE id = 1");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);

    // Verify: Alice's age should be 26
    auto rs = run_select("SELECT * FROM users WHERE id = 1");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 26);
}

// UPDATE without WHERE (all rows)
TEST_F(DmlTest, UpdateWithoutWhere) {
    auto result = run_dml("UPDATE users SET age = 99");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 3u);

    // All ages should be 99
    auto rs = run_select("SELECT * FROM users");
    for (const auto& row : rs.rows) {
        EXPECT_EQ(row.get(2).int_val, 99);
    }
}

// UPDATE with expression: SET age = age + 1
TEST_F(DmlTest, UpdateWithExpression) {
    auto result = run_dml("UPDATE users SET age = age + 1 WHERE id = 1");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);

    auto rs = run_select("SELECT * FROM users WHERE id = 1");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 26);  // was 25, now 26
}

// DELETE with WHERE
TEST_F(DmlTest, DeleteWithWhere) {
    auto result = run_dml("DELETE FROM users WHERE id = 2");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);
    EXPECT_EQ(users_source->row_count(), 2u);

    // Bob should be gone
    auto rs = run_select("SELECT * FROM users WHERE id = 2");
    EXPECT_EQ(rs.row_count(), 0u);
}

// DELETE without WHERE (all rows)
TEST_F(DmlTest, DeleteWithoutWhere) {
    auto result = run_dml("DELETE FROM users");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 3u);
    EXPECT_EQ(users_source->row_count(), 0u);
}

// INSERT then UPDATE then DELETE -> verify final state
TEST_F(DmlTest, InsertUpdateDelete) {
    // Insert a new row
    auto r1 = run_dml("INSERT INTO users (id, name, age) VALUES (10, 'Zoe', 40)");
    EXPECT_TRUE(r1.success);
    EXPECT_EQ(users_source->row_count(), 4u);

    // Update that row
    auto r2 = run_dml("UPDATE users SET age = 41 WHERE id = 10");
    EXPECT_TRUE(r2.success);
    EXPECT_EQ(r2.affected_rows, 1u);

    // Delete the original rows
    auto r3 = run_dml("DELETE FROM users WHERE id < 10");
    EXPECT_TRUE(r3.success);
    EXPECT_EQ(r3.affected_rows, 3u);
    EXPECT_EQ(users_source->row_count(), 1u);

    // Verify final state: only Zoe with age 41
    auto rs = run_select("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).int_val, 10);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 41);
}

// NULL handling in INSERT
TEST_F(DmlTest, InsertWithNull) {
    auto result = run_dml("INSERT INTO users (id, name, age) VALUES (5, NULL, 20)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(users_source->row_count(), 4u);

    // Verify: the new row should have NULL name
    auto rs = run_select("SELECT * FROM users");
    EXPECT_EQ(rs.rows[3].get(1).is_null(), true);
    EXPECT_EQ(rs.rows[3].get(2).int_val, 20);
}

// UPDATE SET column = NULL
TEST_F(DmlTest, UpdateSetNull) {
    auto result = run_dml("UPDATE users SET name = NULL WHERE id = 1");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);

    auto rs = run_select("SELECT * FROM users WHERE id = 1");
    EXPECT_EQ(rs.row_count(), 1u);
    EXPECT_TRUE(rs.rows[0].get(1).is_null());
}

// DELETE with complex WHERE
TEST_F(DmlTest, DeleteWithComplexWhere) {
    auto result = run_dml("DELETE FROM users WHERE age >= 25");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 2u);  // Alice(25) and Bob(30)
    EXPECT_EQ(users_source->row_count(), 1u);

    auto rs = run_select("SELECT * FROM users");
    EXPECT_EQ(rs.rows[0].get(0).int_val, 3);  // Only Carol remains
}

// INSERT without explicit column list
TEST_F(DmlTest, InsertWithoutColumnList) {
    auto result = run_dml("INSERT INTO users VALUES (6, 'Frank', 28)");
    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.affected_rows, 1u);
    EXPECT_EQ(users_source->row_count(), 4u);

    auto rs = run_select("SELECT * FROM users");
    EXPECT_EQ(rs.rows[3].get(0).int_val, 6);
}

// ---- Multi-table UPDATE/DELETE plan node tests ----

// Multi-table UPDATE sets original_ast in the plan node
TEST_F(DmlTest, MultiTableUpdateSetsOriginalAst) {
    const char* sql = "UPDATE users u JOIN orders o ON u.id = o.user_id SET u.age = 30 WHERE o.total > 100";
    parser.reset();
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::UPDATE_PLAN);
    EXPECT_NE(plan->update_plan.original_ast, nullptr);
    EXPECT_EQ(plan->update_plan.original_ast, r.ast);
}

// Single-table UPDATE leaves original_ast as nullptr
TEST_F(DmlTest, SingleTableUpdateOriginalAstNull) {
    const char* sql = "UPDATE users SET age = 30 WHERE id = 1";
    parser.reset();
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::UPDATE_PLAN);
    EXPECT_EQ(plan->update_plan.original_ast, nullptr);
}

// Multi-table DELETE (MySQL syntax) sets original_ast
TEST_F(DmlTest, MultiTableDeleteSetsOriginalAst) {
    const char* sql = "DELETE u FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100";
    parser.reset();
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::DELETE_PLAN);
    EXPECT_NE(plan->delete_plan.original_ast, nullptr);
    EXPECT_EQ(plan->delete_plan.original_ast, r.ast);
}

// Single-table DELETE leaves original_ast as nullptr
TEST_F(DmlTest, SingleTableDeleteOriginalAstNull) {
    const char* sql = "DELETE FROM users WHERE id = 1";
    parser.reset();
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);

    DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
    PlanNode* plan = dml_builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::DELETE_PLAN);
    EXPECT_EQ(plan->delete_plan.original_ast, nullptr);
}
