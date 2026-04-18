#include <gtest/gtest.h>
#include "sql_engine/operator.h"
#include "sql_engine/data_source.h"
#include "sql_engine/operators/scan_op.h"
#include "sql_engine/operators/filter_op.h"
#include "sql_engine/operators/project_op.h"
#include "sql_engine/operators/join_op.h"
#include "sql_engine/operators/aggregate_op.h"
#include "sql_engine/operators/sort_op.h"
#include "sql_engine/operators/limit_op.h"
#include "sql_engine/operators/distinct_op.h"
#include "sql_engine/operators/set_op_op.h"
#include "sql_engine/operators/merge_aggregate_op.h"
#include "sql_engine/engine_limits.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/plan_builder.h"
#include "sql_parser/parser.h"
#include <vector>
#include <cstring>

using namespace sql_engine;
using namespace sql_parser;

// =====================================================================
// Helper: build rows for a table from a catalog
// =====================================================================

static Row build_row(Arena& arena, std::initializer_list<Value> vals) {
    uint16_t n = static_cast<uint16_t>(vals.size());
    Row r = make_row(arena, n);
    uint16_t i = 0;
    for (auto& v : vals) r.set(i++, v);
    return r;
}

// Helper: allocate a persistent string in the arena
static StringRef arena_str(Arena& arena, const char* s) {
    uint32_t len = static_cast<uint32_t>(std::strlen(s));
    char* buf = static_cast<char*>(arena.allocate(len));
    std::memcpy(buf, s, len);
    return StringRef{buf, len};
}

// =====================================================================
// ScanOperator tests
// =====================================================================

class ScanOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    const TableInfo* table = nullptr;

    void SetUp() override {
        catalog.add_table("", "t", {
            {"id",   SqlType::make_int(), false},
            {"name", SqlType::make_varchar(100), true},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(ScanOpTest, YieldsAllRows) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice"))}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "Bob"))}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);
    scan.open();

    Row r{};
    ASSERT_TRUE(scan.next(r));
    EXPECT_EQ(r.get(0).int_val, 1);
    ASSERT_TRUE(scan.next(r));
    EXPECT_EQ(r.get(0).int_val, 2);
    EXPECT_FALSE(scan.next(r));
    scan.close();
}

TEST_F(ScanOpTest, EmptySource) {
    InMemoryDataSource ds(table, {});
    ScanOperator scan(&ds);
    scan.open();
    Row r{};
    EXPECT_FALSE(scan.next(r));
    scan.close();
}

// =====================================================================
// FilterOperator tests
// =====================================================================

class FilterOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    const TableInfo* table = nullptr;
    Parser<Dialect::MySQL> parser;

    void SetUp() override {
        functions.register_builtins();
        catalog.add_table("", "t", {
            {"id",   SqlType::make_int(), false},
            {"name", SqlType::make_varchar(100), true},
            {"age",  SqlType::make_int(), true},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }

    // Parse "SELECT * FROM t WHERE <expr>" and extract the WHERE expression
    const AstNode* parse_where_expr(const char* sql) {
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return nullptr;
        // Walk to find WHERE clause
        for (const AstNode* c = r.ast->first_child; c; c = c->next_sibling) {
            if (c->type == NodeType::NODE_WHERE_CLAUSE) return c->first_child;
        }
        return nullptr;
    }
};

TEST_F(FilterOpTest, KeepsMatching) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice")), value_int(25)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "Bob")),   value_int(15)}),
        build_row(arena, {value_int(3), value_string(arena_str(arena, "Carol")), value_int(30)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    const AstNode* expr = parse_where_expr("SELECT * FROM t WHERE age > 18");
    ASSERT_NE(expr, nullptr);

    std::vector<const TableInfo*> tables = {table};
    FilterOperator<Dialect::MySQL> filter(&scan, expr, catalog, tables, functions, arena);
    filter.open();

    Row r{};
    ASSERT_TRUE(filter.next(r));
    EXPECT_EQ(r.get(0).int_val, 1); // Alice, age 25
    ASSERT_TRUE(filter.next(r));
    EXPECT_EQ(r.get(0).int_val, 3); // Carol, age 30
    EXPECT_FALSE(filter.next(r));
    filter.close();
}

TEST_F(FilterOpTest, FiltersAll) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice")), value_int(10)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "Bob")),   value_int(12)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    const AstNode* expr = parse_where_expr("SELECT * FROM t WHERE age > 100");
    ASSERT_NE(expr, nullptr);

    std::vector<const TableInfo*> tables = {table};
    FilterOperator<Dialect::MySQL> filter(&scan, expr, catalog, tables, functions, arena);
    filter.open();

    Row r{};
    EXPECT_FALSE(filter.next(r));
    filter.close();
}

TEST_F(FilterOpTest, NullHandling) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice")), value_null()}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "Bob")),   value_int(25)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    const AstNode* expr = parse_where_expr("SELECT * FROM t WHERE age > 18");
    ASSERT_NE(expr, nullptr);

    std::vector<const TableInfo*> tables = {table};
    FilterOperator<Dialect::MySQL> filter(&scan, expr, catalog, tables, functions, arena);
    filter.open();

    Row r{};
    ASSERT_TRUE(filter.next(r));
    EXPECT_EQ(r.get(0).int_val, 2); // Only Bob (NULL > 18 = NULL = falsy)
    EXPECT_FALSE(filter.next(r));
    filter.close();
}

// =====================================================================
// ProjectOperator tests
// =====================================================================

class ProjectOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    const TableInfo* table = nullptr;
    Parser<Dialect::MySQL> parser;

    void SetUp() override {
        functions.register_builtins();
        catalog.add_table("", "t", {
            {"id",   SqlType::make_int(), false},
            {"name", SqlType::make_varchar(100), true},
            {"age",  SqlType::make_int(), true},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(ProjectOpTest, ColumnSubset) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice")), value_int(25)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    // Parse "SELECT name FROM t" to get the expression
    auto r = parser.parse("SELECT name FROM t", 18);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->type, PlanNodeType::PROJECT);

    std::vector<const TableInfo*> tables = {table};
    ProjectOperator<Dialect::MySQL> proj(&scan, plan->project.exprs, plan->project.count,
                                          catalog, tables, functions, arena);
    proj.open();

    Row out{};
    ASSERT_TRUE(proj.next(out));
    EXPECT_EQ(out.column_count, 1);
    EXPECT_EQ(out.get(0).tag, Value::TAG_STRING);
    EXPECT_FALSE(proj.next(out));
    proj.close();
}

TEST_F(ProjectOpTest, ComputedExpression) {
    // SELECT 1 + 2 (no FROM)
    auto r = parser.parse("SELECT 1 + 2", 12);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->type, PlanNodeType::PROJECT);
    ASSERT_EQ(plan->left, nullptr); // no FROM

    std::vector<const TableInfo*> tables;
    ProjectOperator<Dialect::MySQL> proj(nullptr, plan->project.exprs, plan->project.count,
                                          catalog, tables, functions, arena);
    proj.open();

    Row out{};
    ASSERT_TRUE(proj.next(out));
    EXPECT_EQ(out.column_count, 1);
    EXPECT_EQ(out.get(0).tag, Value::TAG_INT64);
    EXPECT_EQ(out.get(0).int_val, 3);
    EXPECT_FALSE(proj.next(out));
    proj.close();
}

TEST_F(ProjectOpTest, NoFromSelectLiteral) {
    auto r = parser.parse("SELECT 42", 9);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);

    std::vector<const TableInfo*> tables;
    ProjectOperator<Dialect::MySQL> proj(nullptr, plan->project.exprs, plan->project.count,
                                          catalog, tables, functions, arena);
    proj.open();

    Row out{};
    ASSERT_TRUE(proj.next(out));
    EXPECT_EQ(out.get(0).int_val, 42);
    EXPECT_FALSE(proj.next(out));
    proj.close();
}

// =====================================================================
// JoinOperator tests
// =====================================================================

class JoinOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    const TableInfo* users_table = nullptr;
    const TableInfo* orders_table = nullptr;
    Parser<Dialect::MySQL> parser;

    void SetUp() override {
        functions.register_builtins();
        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(), false},
            {"name", SqlType::make_varchar(100), true},
        });
        catalog.add_table("", "orders", {
            {"oid",     SqlType::make_int(), false},
            {"user_id", SqlType::make_int(), true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});
        orders_table = catalog.get_table(StringRef{"orders", 6});
    }
};

TEST_F(JoinOpTest, InnerJoinMatch) {
    std::vector<Row> users = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice"))}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "Bob"))}),
    };
    std::vector<Row> orders = {
        build_row(arena, {value_int(10), value_int(1)}),
        build_row(arena, {value_int(11), value_int(1)}),
    };
    InMemoryDataSource ds_users(users_table, users);
    InMemoryDataSource ds_orders(orders_table, orders);
    ScanOperator scan_left(&ds_users);
    ScanOperator scan_right(&ds_orders);

    // Parse join condition: id = user_id
    auto r = parser.parse("SELECT * FROM users JOIN orders ON id = user_id", 48);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->type, PlanNodeType::JOIN);

    std::vector<const TableInfo*> left_tables = {users_table};
    std::vector<const TableInfo*> right_tables = {orders_table};
    NestedLoopJoinOperator<Dialect::MySQL> join(
        &scan_left, &scan_right, JOIN_INNER, plan->join.condition,
        2, 2, catalog, left_tables, right_tables, functions, arena);

    join.open();
    Row out{};
    int count = 0;
    while (join.next(out)) {
        EXPECT_EQ(out.column_count, 4);
        EXPECT_EQ(out.get(0).int_val, 1); // Alice's id
        count++;
    }
    EXPECT_EQ(count, 2); // Alice matches both orders
    join.close();
}

TEST_F(JoinOpTest, InnerJoinNoMatch) {
    std::vector<Row> users = {
        build_row(arena, {value_int(99), value_string(arena_str(arena, "Nobody"))}),
    };
    std::vector<Row> orders = {
        build_row(arena, {value_int(10), value_int(1)}),
    };
    InMemoryDataSource ds_users(users_table, users);
    InMemoryDataSource ds_orders(orders_table, orders);
    ScanOperator scan_left(&ds_users);
    ScanOperator scan_right(&ds_orders);

    auto r = parser.parse("SELECT * FROM users JOIN orders ON id = user_id", 48);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);

    std::vector<const TableInfo*> lt = {users_table}, rt = {orders_table};
    NestedLoopJoinOperator<Dialect::MySQL> join(
        &scan_left, &scan_right, JOIN_INNER, plan->join.condition,
        2, 2, catalog, lt, rt, functions, arena);

    join.open();
    Row out{};
    EXPECT_FALSE(join.next(out));
    join.close();
}

TEST_F(JoinOpTest, LeftJoinNulls) {
    std::vector<Row> users = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "Alice"))}),
        build_row(arena, {value_int(99), value_string(arena_str(arena, "Nobody"))}),
    };
    std::vector<Row> orders = {
        build_row(arena, {value_int(10), value_int(1)}),
    };
    InMemoryDataSource ds_users(users_table, users);
    InMemoryDataSource ds_orders(orders_table, orders);
    ScanOperator scan_left(&ds_users);
    ScanOperator scan_right(&ds_orders);

    auto r = parser.parse("SELECT * FROM users LEFT JOIN orders ON id = user_id", 53);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);

    std::vector<const TableInfo*> lt = {users_table}, rt = {orders_table};
    NestedLoopJoinOperator<Dialect::MySQL> join(
        &scan_left, &scan_right, JOIN_LEFT, plan->join.condition,
        2, 2, catalog, lt, rt, functions, arena);

    join.open();
    Row out{};
    ASSERT_TRUE(join.next(out)); // Alice + order
    EXPECT_EQ(out.get(0).int_val, 1);
    EXPECT_FALSE(out.get(2).is_null()); // oid = 10

    ASSERT_TRUE(join.next(out)); // Nobody + NULLs
    EXPECT_EQ(out.get(0).int_val, 99);
    EXPECT_TRUE(out.get(2).is_null()); // oid = NULL
    EXPECT_TRUE(out.get(3).is_null()); // user_id = NULL

    EXPECT_FALSE(join.next(out));
    join.close();
}

TEST_F(JoinOpTest, CrossJoin) {
    std::vector<Row> users = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A"))}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B"))}),
    };
    std::vector<Row> orders = {
        build_row(arena, {value_int(10), value_int(1)}),
        build_row(arena, {value_int(11), value_int(2)}),
    };
    InMemoryDataSource ds_users(users_table, users);
    InMemoryDataSource ds_orders(orders_table, orders);
    ScanOperator scan_left(&ds_users);
    ScanOperator scan_right(&ds_orders);

    std::vector<const TableInfo*> lt = {users_table}, rt = {orders_table};
    NestedLoopJoinOperator<Dialect::MySQL> join(
        &scan_left, &scan_right, JOIN_CROSS, nullptr,
        2, 2, catalog, lt, rt, functions, arena);

    join.open();
    Row out{};
    int count = 0;
    while (join.next(out)) count++;
    EXPECT_EQ(count, 4); // 2 * 2
    join.close();
}

TEST_F(JoinOpTest, RightJoinIncludesUnmatchedRightRows) {
    std::vector<Row> users = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A"))}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B"))}),
    };
    std::vector<Row> orders = {
        build_row(arena, {value_int(10), value_int(1)}),
        build_row(arena, {value_int(11), value_int(99)}),
    };
    InMemoryDataSource ds_users(users_table, users);
    InMemoryDataSource ds_orders(orders_table, orders);
    ScanOperator scan_left(&ds_users);
    ScanOperator scan_right(&ds_orders);

    auto r = parser.parse("SELECT * FROM users RIGHT JOIN orders ON id = user_id", 54);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);

    std::vector<const TableInfo*> lt = {users_table}, rt = {orders_table};
    NestedLoopJoinOperator<Dialect::MySQL> join(
        &scan_left, &scan_right, JOIN_RIGHT, plan->join.condition,
        2, 2, catalog, lt, rt, functions, arena);

    join.open();
    Row out{};
    int row_count = 0;
    bool found_matched = false;
    bool found_unmatched_right = false;
    while (join.next(out)) {
        row_count++;
        if (!out.get(0).is_null() && out.get(0).int_val == 1) {
            found_matched = true;
            EXPECT_EQ(out.get(2).int_val, 10);
            EXPECT_EQ(out.get(3).int_val, 1);
        }
        if (out.get(2).int_val == 11) {
            found_unmatched_right = true;
            EXPECT_TRUE(out.get(0).is_null());
            EXPECT_TRUE(out.get(1).is_null());
            EXPECT_EQ(out.get(3).int_val, 99);
        }
    }
    EXPECT_EQ(row_count, 2);
    EXPECT_TRUE(found_matched);
    EXPECT_TRUE(found_unmatched_right);
    join.close();
}

TEST_F(JoinOpTest, FullJoinIncludesUnmatchedRowsFromBothSides) {
    std::vector<Row> users = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A"))}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B"))}),
    };
    std::vector<Row> orders = {
        build_row(arena, {value_int(10), value_int(1)}),
        build_row(arena, {value_int(11), value_int(99)}),
    };
    InMemoryDataSource ds_users(users_table, users);
    InMemoryDataSource ds_orders(orders_table, orders);
    ScanOperator scan_left(&ds_users);
    ScanOperator scan_right(&ds_orders);

    auto r = parser.parse("SELECT * FROM users FULL JOIN orders ON id = user_id", 53);
    ASSERT_EQ(r.status, ParseResult::OK);
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(r.ast);
    ASSERT_NE(plan, nullptr);

    std::vector<const TableInfo*> lt = {users_table}, rt = {orders_table};
    NestedLoopJoinOperator<Dialect::MySQL> join(
        &scan_left, &scan_right, JOIN_FULL, plan->join.condition,
        2, 2, catalog, lt, rt, functions, arena);

    join.open();
    Row out{};
    int row_count = 0;
    bool found_matched = false;
    bool found_unmatched_left = false;
    bool found_unmatched_right = false;
    while (join.next(out)) {
        row_count++;
        if (!out.get(0).is_null() && out.get(0).int_val == 1) {
            found_matched = true;
            EXPECT_EQ(out.get(2).int_val, 10);
            EXPECT_EQ(out.get(3).int_val, 1);
        }
        if (!out.get(0).is_null() && out.get(0).int_val == 2) {
            found_unmatched_left = true;
            EXPECT_TRUE(out.get(2).is_null());
            EXPECT_TRUE(out.get(3).is_null());
        }
        if (out.get(2).int_val == 11) {
            found_unmatched_right = true;
            EXPECT_TRUE(out.get(0).is_null());
            EXPECT_TRUE(out.get(1).is_null());
            EXPECT_EQ(out.get(3).int_val, 99);
        }
    }
    EXPECT_EQ(row_count, 3);
    EXPECT_TRUE(found_matched);
    EXPECT_TRUE(found_unmatched_left);
    EXPECT_TRUE(found_unmatched_right);
    join.close();
}

// =====================================================================
// AggregateOperator tests
// =====================================================================

class AggregateOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    const TableInfo* table = nullptr;
    Parser<Dialect::MySQL> parser;

    void SetUp() override {
        functions.register_builtins();
        catalog.add_table("", "t", {
            {"id",   SqlType::make_int(), false},
            {"dept", SqlType::make_varchar(50), true},
            {"salary", SqlType::make_int(), true},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(AggregateOpTest, CountStar) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(100)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "A")), value_int(200)}),
        build_row(arena, {value_int(3), value_string(arena_str(arena, "B")), value_int(150)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    // Build COUNT(*) AST node manually
    AstNode* count_fn = make_node(arena, NodeType::NODE_FUNCTION_CALL, StringRef{"COUNT", 5});
    AstNode* star = make_node(arena, NodeType::NODE_ASTERISK);
    count_fn->add_child(star);

    const AstNode* agg_exprs[] = {count_fn};
    std::vector<const TableInfo*> tables = {table};

    AggregateOperator<Dialect::MySQL> agg(
        &scan, nullptr, 0, agg_exprs, 1,
        catalog, tables, functions, arena);

    agg.open();
    Row out{};
    ASSERT_TRUE(agg.next(out));
    EXPECT_EQ(out.get(0).int_val, 3);
    EXPECT_FALSE(agg.next(out));
    agg.close();
}

TEST_F(AggregateOpTest, SumAggregate) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(100)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "A")), value_int(200)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    // Build SUM(salary) AST
    AstNode* sum_fn = make_node(arena, NodeType::NODE_FUNCTION_CALL, StringRef{"SUM", 3});
    AstNode* col = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"salary", 6});
    sum_fn->add_child(col);

    const AstNode* agg_exprs[] = {sum_fn};
    std::vector<const TableInfo*> tables = {table};

    AggregateOperator<Dialect::MySQL> agg(
        &scan, nullptr, 0, agg_exprs, 1,
        catalog, tables, functions, arena);

    agg.open();
    Row out{};
    ASSERT_TRUE(agg.next(out));
    EXPECT_DOUBLE_EQ(out.get(0).double_val, 300.0);
    EXPECT_FALSE(agg.next(out));
    agg.close();
}

TEST_F(AggregateOpTest, AvgAggregate) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(100)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "A")), value_int(200)}),
        build_row(arena, {value_int(3), value_string(arena_str(arena, "A")), value_int(300)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    AstNode* avg_fn = make_node(arena, NodeType::NODE_FUNCTION_CALL, StringRef{"AVG", 3});
    AstNode* col = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"salary", 6});
    avg_fn->add_child(col);

    const AstNode* agg_exprs[] = {avg_fn};
    std::vector<const TableInfo*> tables = {table};

    AggregateOperator<Dialect::MySQL> agg(
        &scan, nullptr, 0, agg_exprs, 1,
        catalog, tables, functions, arena);

    agg.open();
    Row out{};
    ASSERT_TRUE(agg.next(out));
    EXPECT_DOUBLE_EQ(out.get(0).double_val, 200.0);
    agg.close();
}

TEST_F(AggregateOpTest, GroupByMultipleGroups) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(100)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B")), value_int(200)}),
        build_row(arena, {value_int(3), value_string(arena_str(arena, "A")), value_int(300)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    // GROUP BY dept with COUNT(*)
    AstNode* gb_expr = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"dept", 4});
    const AstNode* gb_arr[] = {gb_expr};

    AstNode* count_fn = make_node(arena, NodeType::NODE_FUNCTION_CALL, StringRef{"COUNT", 5});
    AstNode* star = make_node(arena, NodeType::NODE_ASTERISK);
    count_fn->add_child(star);
    const AstNode* agg_arr[] = {count_fn};

    std::vector<const TableInfo*> tables = {table};

    AggregateOperator<Dialect::MySQL> agg(
        &scan, gb_arr, 1, agg_arr, 1,
        catalog, tables, functions, arena);

    agg.open();
    Row out{};
    int group_count = 0;
    int64_t total_count = 0;
    while (agg.next(out)) {
        group_count++;
        total_count += out.get(1).int_val; // COUNT(*)
    }
    EXPECT_EQ(group_count, 2); // A and B
    EXPECT_EQ(total_count, 3); // 2 + 1
    agg.close();
}

TEST_F(AggregateOpTest, WholeTableAggregate) {
    // Empty table, COUNT(*) should return 0
    InMemoryDataSource ds(table, {});
    ScanOperator scan(&ds);

    AstNode* count_fn = make_node(arena, NodeType::NODE_FUNCTION_CALL, StringRef{"COUNT", 5});
    AstNode* star = make_node(arena, NodeType::NODE_ASTERISK);
    count_fn->add_child(star);
    const AstNode* agg_arr[] = {count_fn};
    std::vector<const TableInfo*> tables = {table};

    AggregateOperator<Dialect::MySQL> agg(
        &scan, nullptr, 0, agg_arr, 1,
        catalog, tables, functions, arena);

    agg.open();
    Row out{};
    ASSERT_TRUE(agg.next(out)); // whole-table agg should produce one row
    EXPECT_EQ(out.get(0).int_val, 0);
    EXPECT_FALSE(agg.next(out));
    agg.close();
}

// =====================================================================
// SortOperator tests
// =====================================================================

class SortOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    const TableInfo* table = nullptr;

    void SetUp() override {
        functions.register_builtins();
        catalog.add_table("", "t", {
            {"id",   SqlType::make_int(), false},
            {"name", SqlType::make_varchar(100), true},
            {"age",  SqlType::make_int(), true},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(SortOpTest, AscSort) {
    std::vector<Row> data = {
        build_row(arena, {value_int(3), value_string(arena_str(arena, "C")), value_int(30)}),
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(10)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B")), value_int(20)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    AstNode* key = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"age", 3});
    const AstNode* keys[] = {key};
    uint8_t dirs[] = {0}; // ASC

    std::vector<const TableInfo*> tables = {table};
    SortOperator<Dialect::MySQL> sort(&scan, keys, dirs, 1, catalog, tables, functions, arena);
    sort.open();

    Row out{};
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 10);
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 20);
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 30);
    EXPECT_FALSE(sort.next(out));
    sort.close();
}

TEST_F(SortOpTest, DescSort) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(10)}),
        build_row(arena, {value_int(3), value_string(arena_str(arena, "C")), value_int(30)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B")), value_int(20)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    AstNode* key = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"age", 3});
    const AstNode* keys[] = {key};
    uint8_t dirs[] = {1}; // DESC

    std::vector<const TableInfo*> tables = {table};
    SortOperator<Dialect::MySQL> sort(&scan, keys, dirs, 1, catalog, tables, functions, arena);
    sort.open();

    Row out{};
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 30);
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 20);
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 10);
    sort.close();
}

TEST_F(SortOpTest, MultiKeySort) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1), value_string(arena_str(arena, "A")), value_int(20)}),
        build_row(arena, {value_int(2), value_string(arena_str(arena, "B")), value_int(20)}),
        build_row(arena, {value_int(3), value_string(arena_str(arena, "A")), value_int(10)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);

    AstNode* key1 = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"age", 3});
    AstNode* key2 = make_node(arena, NodeType::NODE_COLUMN_REF, StringRef{"name", 4});
    const AstNode* keys[] = {key1, key2};
    uint8_t dirs[] = {0, 0}; // ASC, ASC

    std::vector<const TableInfo*> tables = {table};
    SortOperator<Dialect::MySQL> sort(&scan, keys, dirs, 2, catalog, tables, functions, arena);
    sort.open();

    Row out{};
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 10); // age=10
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 20); // age=20, name=A
    EXPECT_EQ(out.get(0).int_val, 1);
    ASSERT_TRUE(sort.next(out));
    EXPECT_EQ(out.get(2).int_val, 20); // age=20, name=B
    EXPECT_EQ(out.get(0).int_val, 2);
    sort.close();
}

// =====================================================================
// LimitOperator tests
// =====================================================================

class LimitOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    const TableInfo* table = nullptr;

    void SetUp() override {
        catalog.add_table("", "t", {
            {"id", SqlType::make_int(), false},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(LimitOpTest, CountOnly) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(2)}),
        build_row(arena, {value_int(3)}),
        build_row(arena, {value_int(4)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);
    LimitOperator limit(&scan, 2, 0);

    limit.open();
    Row out{};
    ASSERT_TRUE(limit.next(out));
    EXPECT_EQ(out.get(0).int_val, 1);
    ASSERT_TRUE(limit.next(out));
    EXPECT_EQ(out.get(0).int_val, 2);
    EXPECT_FALSE(limit.next(out));
    limit.close();
}

TEST_F(LimitOpTest, OffsetAndCount) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(2)}),
        build_row(arena, {value_int(3)}),
        build_row(arena, {value_int(4)}),
        build_row(arena, {value_int(5)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);
    LimitOperator limit(&scan, 2, 2);

    limit.open();
    Row out{};
    ASSERT_TRUE(limit.next(out));
    EXPECT_EQ(out.get(0).int_val, 3);
    ASSERT_TRUE(limit.next(out));
    EXPECT_EQ(out.get(0).int_val, 4);
    EXPECT_FALSE(limit.next(out));
    limit.close();
}

TEST_F(LimitOpTest, OffsetBeyondData) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(2)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);
    LimitOperator limit(&scan, 10, 100);

    limit.open();
    Row out{};
    EXPECT_FALSE(limit.next(out));
    limit.close();
}

// =====================================================================
// DistinctOperator tests
// =====================================================================

class DistinctOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    const TableInfo* table = nullptr;

    void SetUp() override {
        catalog.add_table("", "t", {
            {"val", SqlType::make_int(), false},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(DistinctOpTest, RemovesDuplicates) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(2)}),
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(3)}),
        build_row(arena, {value_int(2)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);
    DistinctOperator dist(&scan);

    dist.open();
    Row out{};
    int count = 0;
    while (dist.next(out)) count++;
    EXPECT_EQ(count, 3); // 1, 2, 3
    dist.close();
}

TEST_F(DistinctOpTest, AllUnique) {
    std::vector<Row> data = {
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(2)}),
        build_row(arena, {value_int(3)}),
    };
    InMemoryDataSource ds(table, data);
    ScanOperator scan(&ds);
    DistinctOperator dist(&scan);

    dist.open();
    Row out{};
    int count = 0;
    while (dist.next(out)) count++;
    EXPECT_EQ(count, 3);
    dist.close();
}

// =====================================================================
// SetOpOperator tests
// =====================================================================

class SetOpOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    const TableInfo* table = nullptr;

    void SetUp() override {
        catalog.add_table("", "t", {
            {"val", SqlType::make_int(), false},
        });
        table = catalog.get_table(StringRef{"t", 1});
    }
};

TEST_F(SetOpOpTest, UnionAll) {
    std::vector<Row> left = {
        build_row(arena, {value_int(1)}),
        build_row(arena, {value_int(2)}),
    };
    std::vector<Row> right = {
        build_row(arena, {value_int(2)}),
        build_row(arena, {value_int(3)}),
    };
    InMemoryDataSource ds_left(table, left);
    InMemoryDataSource ds_right(table, right);
    ScanOperator scan_left(&ds_left);
    ScanOperator scan_right(&ds_right);

    SetOpOperator setop(&scan_left, &scan_right, SET_OP_UNION, true);
    setop.open();

    Row out{};
    int count = 0;
    while (setop.next(out)) count++;
    EXPECT_EQ(count, 4); // all rows, including duplicate 2
    setop.close();
}

// Regression test for silent column-count mismatch in set operations.
// Before the validation fix, SetOpOperator::row_key would iterate each row's
// own column_count, producing truncated/misaligned keys and silently-wrong
// INTERSECT/EXCEPT results. The fix throws a clear runtime_error instead.
TEST_F(SetOpOpTest, RejectsColumnCountMismatchUnionAll) {
    // Build a two-column table for the right side so it has a different arity.
    catalog.add_table("", "t2", {
        {"a", SqlType::make_int(), false},
        {"b", SqlType::make_int(), false},
    });
    const TableInfo* table2 = catalog.get_table(StringRef{"t2", 2});

    std::vector<Row> left = {
        build_row(arena, {value_int(1)}),
    };
    std::vector<Row> right = {
        build_row(arena, {value_int(10), value_int(20)}),
    };
    InMemoryDataSource ds_left(table, left);
    InMemoryDataSource ds_right(table2, right);
    ScanOperator scan_left(&ds_left);
    ScanOperator scan_right(&ds_right);

    SetOpOperator setop(&scan_left, &scan_right, SET_OP_UNION, true);
    setop.open();

    Row out{};
    // First row from left establishes column_count = 1.
    ASSERT_TRUE(setop.next(out));
    EXPECT_EQ(out.column_count, 1);
    // Next row comes from right with column_count = 2 -- must throw.
    EXPECT_THROW(setop.next(out), std::runtime_error);
    setop.close();
}

TEST_F(SetOpOpTest, RejectsColumnCountMismatchIntersect) {
    // For INTERSECT, right is materialized at open() time first, so the
    // right's column count becomes the expected. We then feed a left row
    // with a different count.
    catalog.add_table("", "t3", {
        {"a", SqlType::make_int(), false},
        {"b", SqlType::make_int(), false},
    });
    const TableInfo* table2 = catalog.get_table(StringRef{"t3", 2});

    std::vector<Row> left = {
        build_row(arena, {value_int(1)}),
    };
    std::vector<Row> right = {
        build_row(arena, {value_int(10), value_int(20)}),
    };
    InMemoryDataSource ds_left(table, left);
    InMemoryDataSource ds_right(table2, right);
    ScanOperator scan_left(&ds_left);
    ScanOperator scan_right(&ds_right);

    SetOpOperator setop(&scan_left, &scan_right, SET_OP_INTERSECT, false);
    setop.open();  // materializes right (2 cols), sets expected = 2

    Row out{};
    // Left row has 1 column, should throw.
    EXPECT_THROW(setop.next(out), std::runtime_error);
    setop.close();
}

// =====================================================================
// MergeAggregateOperator schema validation
// =====================================================================
//
// MergeAggregate combines partial aggregate rows from N shards. The expected
// row layout is [group_key_count group keys] + [merge_op_count partial aggs].
// Previously, mismatched schemas were silently truncated, producing wrong
// aggregates. Now we throw a clear error.

class MergeAggOpTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    const TableInfo* table = nullptr;

    void SetUp() override {
        // Layout: dept (group key, varchar), partial_count (int), partial_sum (double)
        catalog.add_table("", "agg_in", {
            {"dept",          SqlType::make_varchar(50), true},
            {"partial_count", SqlType::make_int(),       true},
            {"partial_sum",   SqlType::make_int(),       true},
        });
        table = catalog.get_table(StringRef{"agg_in", 6});
    }
};

TEST_F(MergeAggOpTest, MergesCorrectlyWhenSchemaMatches) {
    // group_key_count=1 (dept), merge_op_count=2 (count, sum)
    std::vector<Row> shard1_rows = {
        build_row(arena, {value_string(arena_str(arena, "Eng")), value_int(2), value_int(100)}),
        build_row(arena, {value_string(arena_str(arena, "Sales")), value_int(1), value_int(50)}),
    };
    std::vector<Row> shard2_rows = {
        build_row(arena, {value_string(arena_str(arena, "Eng")), value_int(3), value_int(200)}),
    };
    InMemoryDataSource ds1(table, shard1_rows);
    InMemoryDataSource ds2(table, shard2_rows);
    ScanOperator scan1(&ds1), scan2(&ds2);

    uint8_t merge_ops[] = {
        static_cast<uint8_t>(MergeOp::SUM_OF_COUNTS),
        static_cast<uint8_t>(MergeOp::SUM_OF_SUMS),
    };
    MergeAggregateOperator op(
        std::vector<Operator*>{&scan1, &scan2},
        /*group_key_count=*/1,
        merge_ops, /*merge_op_count=*/2,
        arena);

    op.open();
    int got = 0;
    Row out{};
    while (op.next(out)) {
        ++got;
        EXPECT_EQ(out.column_count, 3);  // dept + merged count + merged sum
    }
    EXPECT_EQ(got, 2);  // Eng and Sales
    op.close();
}

TEST_F(MergeAggOpTest, RejectsRowWithFewerColumnsThanExpected) {
    // The plan promised 1 group key + 2 partial aggs = 3 columns, but the
    // shard only sends 2 columns. Previously this would be silently truncated.
    catalog.add_table("", "bad_in", {
        {"dept", SqlType::make_varchar(50), true},
        {"only_count", SqlType::make_int(), true},
    });
    const TableInfo* bad = catalog.get_table(StringRef{"bad_in", 6});

    std::vector<Row> bad_rows = {
        build_row(arena, {value_string(arena_str(arena, "Eng")), value_int(2)}),
    };
    InMemoryDataSource ds(bad, bad_rows);
    ScanOperator scan(&ds);

    uint8_t merge_ops[] = {
        static_cast<uint8_t>(MergeOp::SUM_OF_COUNTS),
        static_cast<uint8_t>(MergeOp::SUM_OF_SUMS),
    };
    MergeAggregateOperator op(
        std::vector<Operator*>{&scan},
        /*group_key_count=*/1,
        merge_ops, /*merge_op_count=*/2,
        arena);

    EXPECT_THROW(op.open(), std::runtime_error);
}

TEST_F(MergeAggOpTest, RejectsRowWithMoreColumnsThanExpected) {
    catalog.add_table("", "wide_in", {
        {"dept",          SqlType::make_varchar(50), true},
        {"partial_count", SqlType::make_int(),       true},
        {"partial_sum",   SqlType::make_int(),       true},
        {"unexpected",    SqlType::make_int(),       true},
    });
    const TableInfo* wide = catalog.get_table(StringRef{"wide_in", 7});

    std::vector<Row> wide_rows = {
        build_row(arena, {value_string(arena_str(arena, "Eng")), value_int(2),
                          value_int(100), value_int(999)}),
    };
    InMemoryDataSource ds(wide, wide_rows);
    ScanOperator scan(&ds);

    uint8_t merge_ops[] = {
        static_cast<uint8_t>(MergeOp::SUM_OF_COUNTS),
        static_cast<uint8_t>(MergeOp::SUM_OF_SUMS),
    };
    MergeAggregateOperator op(
        std::vector<Operator*>{&scan},
        /*group_key_count=*/1,
        merge_ops, /*merge_op_count=*/2,
        arena);

    EXPECT_THROW(op.open(), std::runtime_error);
}

// =====================================================================
// engine_limits.h: row cap helper
// =====================================================================
//
// The materializing operators all call check_operator_row_limit() before
// growing their working set. We can't easily exercise the 10M default cap
// from a test (it would require gigabytes of input rows), so we test the
// helper directly with small limits to verify the throw + message.

TEST(EngineLimits, ThrowsAtLimit) {
    EXPECT_THROW(check_operator_row_limit(5, 5, "TestOp"), std::runtime_error);
    EXPECT_THROW(check_operator_row_limit(100, 5, "TestOp"), std::runtime_error);
}

TEST(EngineLimits, AllowsBelowLimit) {
    EXPECT_NO_THROW(check_operator_row_limit(0, 10, "TestOp"));
    EXPECT_NO_THROW(check_operator_row_limit(9, 10, "TestOp"));
}

TEST(EngineLimits, ErrorMessageNamesOperator) {
    try {
        check_operator_row_limit(100, 50, "FooOp");
        FAIL() << "expected throw";
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("FooOp"), std::string::npos);
        EXPECT_NE(msg.find("50"), std::string::npos);
    }
}
