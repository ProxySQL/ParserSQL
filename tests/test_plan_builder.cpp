#include <gtest/gtest.h>
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_node.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_parser/parser.h"
#include <cstring>
#include <string>

using namespace sql_engine;
using namespace sql_parser;

class PlanBuilderTest : public ::testing::Test {
protected:
    InMemoryCatalog catalog;
    Parser<Dialect::MySQL> parser;

    void SetUp() override {
        catalog.add_table("", "users", {
            {"id",     SqlType::make_int(),        false},
            {"name",   SqlType::make_varchar(255), true},
            {"age",    SqlType::make_int(),        true},
            {"status", SqlType::make_varchar(50),  true},
            {"active", SqlType::make_int(),        true},
        });
        catalog.add_table("", "orders", {
            {"order_id", SqlType::make_int(),    false},
            {"user_id",  SqlType::make_int(),    true},
            {"total",    SqlType::make_double(), true},
        });
        catalog.add_table("", "t1", {
            {"id",   SqlType::make_int(), false},
            {"val",  SqlType::make_varchar(100), true},
        });
        catalog.add_table("", "t2", {
            {"id",   SqlType::make_int(), false},
            {"val",  SqlType::make_varchar(100), true},
        });
    }

    PlanNode* parse_and_build(const char* sql) {
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return nullptr;
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        return builder.build(r.ast);
    }
};

// SELECT * FROM users → Scan(users)
TEST_F(PlanBuilderTest, SelectStarFromTable) {
    PlanNode* plan = parse_and_build("SELECT * FROM users");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::SCAN);
    ASSERT_NE(plan->scan.table, nullptr);
    EXPECT_TRUE(plan->scan.table->table_name.equals_ci("users", 5));
}

// SELECT name FROM users → Project → Scan
TEST_F(PlanBuilderTest, SelectColumnFromTable) {
    PlanNode* plan = parse_and_build("SELECT name FROM users");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::PROJECT);
    EXPECT_EQ(plan->project.count, 1);

    PlanNode* child = plan->left;
    ASSERT_NE(child, nullptr);
    EXPECT_EQ(child->type, PlanNodeType::SCAN);
    ASSERT_NE(child->scan.table, nullptr);
    EXPECT_TRUE(child->scan.table->table_name.equals_ci("users", 5));
}

// SELECT * FROM users WHERE id = 1 → Filter → Scan
TEST_F(PlanBuilderTest, SelectWithWhere) {
    PlanNode* plan = parse_and_build("SELECT * FROM users WHERE id = 1");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::FILTER);
    ASSERT_NE(plan->filter.expr, nullptr);

    PlanNode* child = plan->left;
    ASSERT_NE(child, nullptr);
    EXPECT_EQ(child->type, PlanNodeType::SCAN);
}

// SELECT name, age FROM users WHERE age > 18 → Project → Filter → Scan
TEST_F(PlanBuilderTest, SelectColumnsWithWhere) {
    PlanNode* plan = parse_and_build("SELECT name, age FROM users WHERE age > 18");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::PROJECT);
    EXPECT_EQ(plan->project.count, 2);

    PlanNode* filter = plan->left;
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(filter->type, PlanNodeType::FILTER);

    PlanNode* scan = filter->left;
    ASSERT_NE(scan, nullptr);
    EXPECT_EQ(scan->type, PlanNodeType::SCAN);
}

// SELECT * FROM users ORDER BY name LIMIT 10 → Limit → Sort → Scan
TEST_F(PlanBuilderTest, SelectWithOrderByAndLimit) {
    PlanNode* plan = parse_and_build("SELECT * FROM users ORDER BY name LIMIT 10");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::LIMIT);
    EXPECT_EQ(plan->limit.count, 10);
    EXPECT_EQ(plan->limit.offset, 0);

    PlanNode* sort = plan->left;
    ASSERT_NE(sort, nullptr);
    EXPECT_EQ(sort->type, PlanNodeType::SORT);
    EXPECT_EQ(sort->sort.count, 1);
    EXPECT_EQ(sort->sort.directions[0], 0); // ASC

    PlanNode* scan = sort->left;
    ASSERT_NE(scan, nullptr);
    EXPECT_EQ(scan->type, PlanNodeType::SCAN);
}

// SELECT status, COUNT(*) FROM users GROUP BY status → Project → Aggregate → Scan
TEST_F(PlanBuilderTest, SelectWithGroupBy) {
    PlanNode* plan = parse_and_build("SELECT status, COUNT(*) FROM users GROUP BY status");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::PROJECT);

    PlanNode* agg = plan->left;
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->type, PlanNodeType::AGGREGATE);
    EXPECT_EQ(agg->aggregate.group_count, 1);

    PlanNode* scan = agg->left;
    ASSERT_NE(scan, nullptr);
    EXPECT_EQ(scan->type, PlanNodeType::SCAN);
}

// SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5
// → Project → Filter → Aggregate → Scan
TEST_F(PlanBuilderTest, SelectWithGroupByAndHaving) {
    PlanNode* plan = parse_and_build(
        "SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::PROJECT);

    PlanNode* filter = plan->left;
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(filter->type, PlanNodeType::FILTER);
    ASSERT_NE(filter->filter.expr, nullptr);

    PlanNode* agg = filter->left;
    ASSERT_NE(agg, nullptr);
    EXPECT_EQ(agg->type, PlanNodeType::AGGREGATE);

    PlanNode* scan = agg->left;
    ASSERT_NE(scan, nullptr);
    EXPECT_EQ(scan->type, PlanNodeType::SCAN);
}

// SELECT * FROM users u JOIN orders o ON u.id = o.user_id → Join(Scan, Scan)
TEST_F(PlanBuilderTest, SelectWithJoin) {
    PlanNode* plan = parse_and_build(
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::JOIN);
    EXPECT_EQ(plan->join.join_type, JOIN_INNER);
    ASSERT_NE(plan->join.condition, nullptr);

    ASSERT_NE(plan->left, nullptr);
    EXPECT_EQ(plan->left->type, PlanNodeType::SCAN);
    ASSERT_NE(plan->left->scan.table, nullptr);
    EXPECT_TRUE(plan->left->scan.table->table_name.equals_ci("users", 5));

    ASSERT_NE(plan->right, nullptr);
    EXPECT_EQ(plan->right->type, PlanNodeType::SCAN);
    ASSERT_NE(plan->right->scan.table, nullptr);
    EXPECT_TRUE(plan->right->scan.table->table_name.equals_ci("orders", 6));
}

// SELECT DISTINCT name FROM users → Distinct → Project → Scan
TEST_F(PlanBuilderTest, SelectDistinct) {
    PlanNode* plan = parse_and_build("SELECT DISTINCT name FROM users");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::DISTINCT);

    PlanNode* proj = plan->left;
    ASSERT_NE(proj, nullptr);
    EXPECT_EQ(proj->type, PlanNodeType::PROJECT);

    PlanNode* scan = proj->left;
    ASSERT_NE(scan, nullptr);
    EXPECT_EQ(scan->type, PlanNodeType::SCAN);
}

// SELECT * FROM t1 UNION ALL SELECT * FROM t2 → SetOp(Scan, Scan)
TEST_F(PlanBuilderTest, UnionAll) {
    PlanNode* plan = parse_and_build("SELECT * FROM t1 UNION ALL SELECT * FROM t2");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::SET_OP);
    EXPECT_EQ(plan->set_op.op, SET_OP_UNION);
    EXPECT_TRUE(plan->set_op.all);

    ASSERT_NE(plan->left, nullptr);
    EXPECT_EQ(plan->left->type, PlanNodeType::SCAN);

    ASSERT_NE(plan->right, nullptr);
    EXPECT_EQ(plan->right->type, PlanNodeType::SCAN);
}

// SELECT 1 + 2 → Project with null child (no FROM)
TEST_F(PlanBuilderTest, SelectExpressionNoFrom) {
    PlanNode* plan = parse_and_build("SELECT 1 + 2");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::PROJECT);
    EXPECT_EQ(plan->left, nullptr);  // No child -- leaf node
    EXPECT_EQ(plan->project.count, 1);
}

// LEFT JOIN
TEST_F(PlanBuilderTest, LeftJoin) {
    PlanNode* plan = parse_and_build(
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::JOIN);
    EXPECT_EQ(plan->join.join_type, JOIN_LEFT);
}

// CROSS JOIN
TEST_F(PlanBuilderTest, CrossJoin) {
    PlanNode* plan = parse_and_build("SELECT * FROM users CROSS JOIN orders");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::JOIN);
    EXPECT_EQ(plan->join.join_type, JOIN_CROSS);
}

// LIMIT with OFFSET
TEST_F(PlanBuilderTest, LimitWithOffset) {
    PlanNode* plan = parse_and_build("SELECT * FROM users LIMIT 10 OFFSET 5");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::LIMIT);
    EXPECT_EQ(plan->limit.count, 10);
    EXPECT_EQ(plan->limit.offset, 5);
}

// ORDER BY DESC
TEST_F(PlanBuilderTest, OrderByDesc) {
    PlanNode* plan = parse_and_build("SELECT * FROM users ORDER BY name DESC");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::SORT);
    EXPECT_EQ(plan->sort.count, 1);
    EXPECT_EQ(plan->sort.directions[0], 1); // DESC
}

// Verify alias in Project
TEST_F(PlanBuilderTest, ProjectWithAlias) {
    PlanNode* plan = parse_and_build("SELECT name AS n FROM users");
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->type, PlanNodeType::PROJECT);
    EXPECT_EQ(plan->project.count, 1);
    ASSERT_NE(plan->project.aliases[0], nullptr);
}
