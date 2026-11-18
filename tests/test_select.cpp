#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

class MySQLSelectTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
    }

    const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }
};

// ========== Basic SELECT ==========

TEST_F(MySQLSelectTest, SelectLiteral) {
    auto r = parser.parse("SELECT 1", 8);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SELECT_STMT);
}

TEST_F(MySQLSelectTest, SelectStar) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
}

TEST_F(MySQLSelectTest, SelectColumns) {
    auto r = parser.parse("SELECT id, name, email FROM users", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    EXPECT_EQ(child_count(items), 3);
}

TEST_F(MySQLSelectTest, SelectWithAlias) {
    auto r = parser.parse("SELECT id AS user_id, name AS user_name FROM users", 50);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    // Each item should have an alias child
    auto* first_item = items->first_child;
    ASSERT_NE(first_item, nullptr);
    EXPECT_EQ(first_item->type, NodeType::NODE_SELECT_ITEM);
    auto* alias = find_child(first_item, NodeType::NODE_ALIAS);
    ASSERT_NE(alias, nullptr);
}

TEST_F(MySQLSelectTest, SelectImplicitAlias) {
    // Alias without AS keyword
    auto r = parser.parse("SELECT id user_id FROM users", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectDistinct) {
    auto r = parser.parse("SELECT DISTINCT name FROM users", 31);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_SELECT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLSelectTest, SelectSqlCalcFoundRows) {
    auto r = parser.parse("SELECT SQL_CALC_FOUND_ROWS * FROM users", 40);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromQualifiedTable) {
    auto r = parser.parse("SELECT * FROM mydb.users", 24);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromTableAlias) {
    auto r = parser.parse("SELECT u.id FROM users u", 24);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromTableAsAlias) {
    auto r = parser.parse("SELECT u.id FROM users AS u", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectFromMultipleTables) {
    auto r = parser.parse("SELECT * FROM users, orders", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
    EXPECT_GE(child_count(from), 2);
}

TEST_F(MySQLSelectTest, SelectExpression) {
    auto r = parser.parse("SELECT 1 + 2, 'hello', NOW()", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, SelectNoFrom) {
    auto r = parser.parse("SELECT 1, 'a', NOW()", 20);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    // No FROM clause
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    EXPECT_EQ(from, nullptr);
}

// ========== JOINs ==========

TEST_F(MySQLSelectTest, InnerJoin) {
    auto r = parser.parse("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", 66);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
}

TEST_F(MySQLSelectTest, LeftJoin) {
    auto r = parser.parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id", 65);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSelectTest, RightJoin) {
    auto r = parser.parse("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id", 66);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, LeftOuterJoin) {
    auto r = parser.parse("SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id", 71);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, CrossJoin) {
    auto r = parser.parse("SELECT * FROM users CROSS JOIN orders", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, NaturalJoin) {
    auto r = parser.parse("SELECT * FROM users NATURAL JOIN orders", 39);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, JoinUsing) {
    auto r = parser.parse("SELECT * FROM users JOIN orders USING (user_id)", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, MultipleJoins) {
    const char* sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN items ON orders.id = items.order_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, JoinWithAlias) {
    auto r = parser.parse("SELECT * FROM users u JOIN orders o ON u.id = o.user_id", 55);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== WHERE ==========

TEST_F(MySQLSelectTest, WhereSimple) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* where = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    ASSERT_NE(where, nullptr);
}

TEST_F(MySQLSelectTest, WhereComplex) {
    auto r = parser.parse("SELECT * FROM users WHERE age > 18 AND status = 'active'", 56);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereIn) {
    auto r = parser.parse("SELECT * FROM users WHERE id IN (1, 2, 3)", 42);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereBetween) {
    auto r = parser.parse("SELECT * FROM users WHERE age BETWEEN 18 AND 65", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereLike) {
    auto r = parser.parse("SELECT * FROM users WHERE name LIKE '%john%'", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereIsNull) {
    auto r = parser.parse("SELECT * FROM users WHERE email IS NULL", 39);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, WhereSubquery) {
    auto r = parser.parse("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", 60);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== GROUP BY / HAVING ==========

TEST_F(MySQLSelectTest, GroupBy) {
    auto r = parser.parse("SELECT status, COUNT(*) FROM users GROUP BY status", 51);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* gb = find_child(r.ast, NodeType::NODE_GROUP_BY_CLAUSE);
    ASSERT_NE(gb, nullptr);
}

TEST_F(MySQLSelectTest, GroupByMultiple) {
    auto r = parser.parse("SELECT dept, status, COUNT(*) FROM users GROUP BY dept, status", 62);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, GroupByHaving) {
    auto r = parser.parse("SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5", 71);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* having = find_child(r.ast, NodeType::NODE_HAVING_CLAUSE);
    ASSERT_NE(having, nullptr);
}

// ========== ORDER BY ==========

TEST_F(MySQLSelectTest, OrderBy) {
    auto r = parser.parse("SELECT * FROM users ORDER BY name", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ob = find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE);
    ASSERT_NE(ob, nullptr);
}

TEST_F(MySQLSelectTest, OrderByDesc) {
    auto r = parser.parse("SELECT * FROM users ORDER BY created_at DESC", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, OrderByMultiple) {
    auto r = parser.parse("SELECT * FROM users ORDER BY last_name ASC, first_name ASC", 58);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* ob = find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE);
    ASSERT_NE(ob, nullptr);
    EXPECT_EQ(child_count(ob), 2);
}

// ========== LIMIT ==========

TEST_F(MySQLSelectTest, Limit) {
    auto r = parser.parse("SELECT * FROM users LIMIT 10", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* limit = find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE);
    ASSERT_NE(limit, nullptr);
}

TEST_F(MySQLSelectTest, LimitOffset) {
    auto r = parser.parse("SELECT * FROM users LIMIT 10 OFFSET 20", 38);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* limit = find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE);
    ASSERT_NE(limit, nullptr);
    EXPECT_EQ(child_count(limit), 2);
}

TEST_F(MySQLSelectTest, LimitCommaOffset) {
    // MySQL syntax: LIMIT offset, count
    auto r = parser.parse("SELECT * FROM users LIMIT 20, 10", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* limit = find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE);
    ASSERT_NE(limit, nullptr);
    EXPECT_EQ(child_count(limit), 2);
}

// ========== FOR UPDATE / FOR SHARE ==========

TEST_F(MySQLSelectTest, ForUpdate) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR UPDATE", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
    auto* lock = find_child(r.ast, NodeType::NODE_LOCKING_CLAUSE);
    ASSERT_NE(lock, nullptr);
}

TEST_F(MySQLSelectTest, ForShare) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR SHARE", 43);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, ForUpdateNowait) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT", 51);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, ForUpdateSkipLocked) {
    auto r = parser.parse("SELECT * FROM users WHERE id = 1 FOR UPDATE SKIP LOCKED", 56);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== Complex queries ==========

TEST_F(MySQLSelectTest, FullQuery) {
    const char* sql = "SELECT u.id, u.name, COUNT(o.id) AS order_count "
                      "FROM users u "
                      "LEFT JOIN orders o ON u.id = o.user_id "
                      "WHERE u.status = 'active' "
                      "GROUP BY u.id, u.name "
                      "HAVING COUNT(o.id) > 5 "
                      "ORDER BY order_count DESC "
                      "LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_FROM_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_WHERE_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_GROUP_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_HAVING_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLSelectTest, SubqueryInFrom) {
    const char* sql = "SELECT t.id FROM (SELECT id FROM users WHERE active = 1) AS t";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, MultiStatement) {
    const char* sql = "SELECT 1; SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    EXPECT_TRUE(r.has_remaining());
}

TEST_F(MySQLSelectTest, SelectWithSemicolon) {
    auto r = parser.parse("SELECT * FROM users;", 20);
    EXPECT_EQ(r.status, ParseResult::OK);
}

// ========== Bulk data-driven tests ==========

struct SelectTestCase {
    const char* sql;
    const char* description;
};

static const SelectTestCase select_bulk_cases[] = {
    {"SELECT 1", "literal"},
    {"SELECT 1, 2, 3", "multiple literals"},
    {"SELECT 'hello'", "string literal"},
    {"SELECT NULL", "null"},
    {"SELECT TRUE", "true"},
    {"SELECT FALSE", "false"},
    {"SELECT NOW()", "function call"},
    {"SELECT 1 + 2", "arithmetic"},
    {"SELECT *", "star"},
    {"SELECT * FROM t", "star from table"},
    {"SELECT a FROM t", "single column"},
    {"SELECT a, b, c FROM t", "multiple columns"},
    {"SELECT a AS x FROM t", "alias with AS"},
    {"SELECT t.a FROM t", "qualified column"},
    {"SELECT t.* FROM t", "qualified star"},
    {"SELECT DISTINCT a FROM t", "distinct"},
    {"SELECT ALL a FROM t", "all"},
    {"SELECT SQL_CALC_FOUND_ROWS * FROM t", "sql_calc_found_rows"},
    {"SELECT * FROM db.t", "qualified table"},
    {"SELECT * FROM t AS alias", "table alias with AS"},
    {"SELECT * FROM t alias", "table alias implicit"},
    {"SELECT * FROM t1, t2", "comma join"},
    {"SELECT * FROM t1 JOIN t2 ON t1.id = t2.id", "inner join"},
    {"SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id", "left join"},
    {"SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id", "right join"},
    {"SELECT * FROM t1 CROSS JOIN t2", "cross join"},
    {"SELECT * FROM t1 NATURAL JOIN t2", "natural join"},
    {"SELECT * FROM t1 JOIN t2 USING (id)", "join using"},
    {"SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id", "left outer join"},
    {"SELECT * FROM t WHERE a = 1", "where equal"},
    {"SELECT * FROM t WHERE a > 1 AND b < 10", "where and"},
    {"SELECT * FROM t WHERE a IN (1,2,3)", "where in"},
    {"SELECT * FROM t WHERE a IS NULL", "where is null"},
    {"SELECT * FROM t WHERE a IS NOT NULL", "where is not null"},
    {"SELECT * FROM t WHERE a BETWEEN 1 AND 10", "where between"},
    {"SELECT * FROM t WHERE a LIKE '%x%'", "where like"},
    {"SELECT * FROM t WHERE a NOT IN (1,2)", "where not in"},
    {"SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)", "where exists"},
    {"SELECT a, COUNT(*) FROM t GROUP BY a", "group by"},
    {"SELECT a, b, COUNT(*) FROM t GROUP BY a, b", "group by multiple"},
    {"SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1", "having"},
    {"SELECT * FROM t ORDER BY a", "order by"},
    {"SELECT * FROM t ORDER BY a DESC", "order by desc"},
    {"SELECT * FROM t ORDER BY a ASC, b DESC", "order by multiple"},
    {"SELECT * FROM t LIMIT 10", "limit"},
    {"SELECT * FROM t LIMIT 10 OFFSET 5", "limit offset"},
    {"SELECT * FROM t LIMIT 5, 10", "limit comma"},
    {"SELECT * FROM t WHERE a = 1 FOR UPDATE", "for update"},
    {"SELECT * FROM t WHERE a = 1 FOR SHARE", "for share"},
    {"SELECT * FROM t FOR UPDATE NOWAIT", "for update nowait"},
    {"SELECT * FROM t FOR UPDATE SKIP LOCKED", "for update skip locked"},
    {"SELECT COUNT(*), SUM(a), AVG(b), MIN(c), MAX(d) FROM t", "aggregate functions"},
    {"SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM t", "case when"},
    {"SELECT * FROM (SELECT 1) AS t", "subquery in from"},
    {"SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.b = t3.b", "multiple joins"},
    {"SELECT a FROM t WHERE b = (SELECT MAX(b) FROM t2)", "scalar subquery in where"},
};

TEST(MySQLSelectBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : select_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::SELECT)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== PostgreSQL SELECT ==========

class PgSQLSelectTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;
};

TEST_F(PgSQLSelectTest, BasicSelect) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSelectTest, LimitOffset) {
    auto r = parser.parse("SELECT * FROM users LIMIT 10 OFFSET 5", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(PgSQLSelectTest, ForUpdate) {
    auto r = parser.parse("SELECT * FROM users FOR UPDATE", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
}

TEST_F(MySQLSelectTest, MySqlModifiersAreExplicitOptionsWithOriginalSpelling) {
    for (const std::string option : {"high_priority", "SQL_NO_CACHE", "SQL_CACHE", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_BUFFER_RESULT", "STRAIGHT_JOIN", "DISTINCTROW"}) {
        const std::string sql = "SELECT " + option + " a FROM t";
        auto r = parser.parse(sql.data(), sql.size());
        ASSERT_EQ(r.status, ParseResult::OK) << sql;
        ASSERT_TRUE(r.full_input) << sql;
        ASSERT_NE(r.ast, nullptr);
        const auto* opts = find_child(r.ast, NodeType::NODE_SELECT_OPTIONS);
        ASSERT_NE(opts, nullptr) << sql;
        ASSERT_NE(opts->first_child, nullptr);
        EXPECT_EQ(std::string(opts->first_child->value().ptr, opts->first_child->value().len), option);
        const auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
        ASSERT_NE(items, nullptr);
        ASSERT_NE(items->first_child, nullptr);
        EXPECT_EQ(find_child(items->first_child, NodeType::NODE_ALIAS), nullptr) << sql;
    }
}

TEST_F(MySQLSelectTest, QuotedMySqlModifierNamesRemainColumns) {
    for (const std::string option : {"HIGH_PRIORITY", "SQL_NO_CACHE", "SQL_CACHE", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_BUFFER_RESULT", "STRAIGHT_JOIN", "DISTINCTROW"}) {
        const std::string sql = "SELECT `" + option + "` FROM t";
        auto r = parser.parse(sql.data(), sql.size());
        ASSERT_EQ(r.status, ParseResult::OK) << sql;
        ASSERT_TRUE(r.full_input) << sql;
        ASSERT_NE(r.ast, nullptr);
        EXPECT_EQ(find_child(r.ast, NodeType::NODE_SELECT_OPTIONS), nullptr) << sql;
        const auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
        ASSERT_NE(items, nullptr);
        ASSERT_NE(items->first_child, nullptr);
        ASSERT_NE(items->first_child->first_child, nullptr);
        EXPECT_EQ(std::string(items->first_child->first_child->value().ptr,
                              items->first_child->first_child->value().len), option);
    }
}

TEST_F(MySQLSelectTest, UnsupportedMySqlTypedLiteralsAndBinaryExpressionsAreNotComplete) {
    for (const std::string expr : {"DATE '2020-01-01'", "TIME '12:34:56'", "TIMESTAMP '2020-01-01 12:34:56'", "N'hello'", "BINARY 'hello'", "BINARY col", "BINARY(col)", "BINARY CURRENT_DATE", "BINARY @x"}) {
        for (const std::string sql : {"SELECT " + expr, "SELECT 1 FROM t WHERE a = " + expr}) {
            auto r = parser.parse(sql.data(), sql.size());
            EXPECT_EQ(r.stmt_type, StmtType::SELECT) << sql;
            EXPECT_FALSE(r.ok() && r.full_input) << sql;
        }
    }
}

TEST_F(MySQLSelectTest, TypedLiteralRefusalPreservesValidColumnsFunctionsAndAliases) {
    for (const std::string sql : {"SELECT `DATE` 'label' FROM t", "SELECT `TIME` 'label' FROM t", "SELECT `TIMESTAMP` 'label' FROM t", "SELECT `BINARY` 'label' FROM t", "SELECT `N` 'label' FROM t", "SELECT N 'label' FROM t", "SELECT date_col AS 'date' FROM t", "SELECT DATE AS 'date' FROM t", "SELECT DATE('2020-01-01'), TIME('12:34:56'), TIMESTAMP('2020-01-01')", "SELECT date_col, time_col, timestamp_col, binary_col FROM t"}) {
        auto r = parser.parse(sql.data(), sql.size());
        EXPECT_EQ(r.status, ParseResult::OK) << sql;
        EXPECT_TRUE(r.full_input) << sql;
    }
}

TEST_F(MySQLSelectTest, SelectItemExpressionSource) {
    const char* sql = "SELECT /*lead*/ 1, (1 + 2) AS `x`, COUNT( * ) c, t.col /*end*/ FROM t";
    auto r = parser.parse(sql, strlen(sql));
    ASSERT_TRUE(r.ok());
    ASSERT_TRUE(r.has_comments);
    auto* items = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    const char* expected[] = {"1", "(1 + 2)", "COUNT( * )", "t.col"};
    auto* item = items->first_child;
    for (auto value : expected) {
        ASSERT_NE(item, nullptr);
        EXPECT_EQ(std::string(item->source().ptr, item->source().len), value);
        item = item->next_sibling;
    }
    EXPECT_EQ(item, nullptr);
}

TEST_F(MySQLSelectTest, BatchCommentsAndSource) {
    const char* sql = "SELECT 1; /*lead*/ SELECT (2 + 3) x; SELECT '/*no*/'";
    auto batch = parser.parse_all(sql, strlen(sql));
    ASSERT_EQ(batch.statements.size(), 3u);
    EXPECT_FALSE(batch.statements[0].result.has_comments);
    EXPECT_TRUE(batch.statements[1].result.has_comments);
    EXPECT_FALSE(batch.statements[2].result.has_comments);
    auto* items = find_child(batch.statements[1].result.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(items, nullptr);
    auto span = items->first_child->source();
    EXPECT_EQ(std::string(span.ptr, span.len), "(2 + 3)");
    EXPECT_EQ(span.ptr, strstr(sql, "(2 + 3)"));
}

TEST_F(MySQLSelectTest, NestedSelectItemSources) {
    const char* sql = "SELECT (SELECT f( 1 )) AS `nested`, 1 /*inside*/ + 2 /*tail*/";
    auto r = parser.parse(sql, strlen(sql));
    ASSERT_TRUE(r.ok());
    auto* outer = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
    ASSERT_NE(outer, nullptr);
    auto* first = outer->first_child;
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(std::string(first->source().ptr, first->source().len), "(SELECT f( 1 ))");
    auto* second = first->next_sibling;
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(std::string(second->source().ptr, second->source().len), "1 /*inside*/ + 2");
    const AstNode* inner = nullptr;
    auto search = [&](auto&& self, const AstNode* n) -> void {
        if (n->type == NodeType::NODE_SELECT_ITEM && n != first && n != second) inner = n;
        for (auto* c = n->first_child; c; c = c->next_sibling) self(self, c);
    };
    search(search, first);
    ASSERT_NE(inner, nullptr);
    EXPECT_EQ(std::string(inner->source().ptr, inner->source().len), "f( 1 )");
}

TEST_F(MySQLSelectTest, CommentsMetadataOnErrorsAndHints) {
    for (const char* sql : {"SELECT /*+ BKA(t) */ 1", "SELECT 1 /*!50000 + 2 */", "SELECT 1 /* unterminated"}) {
        auto r = parser.parse(sql, strlen(sql));
        EXPECT_TRUE(r.has_comments) << sql;
        auto batch = parser.parse_all(sql, strlen(sql));
        ASSERT_EQ(batch.statements.size(), 1u);
        EXPECT_TRUE(batch.statements[0].result.has_comments) << sql;
    }
    const char* quoted = "SELECT '/* no */', `-- no`";
    EXPECT_FALSE(parser.parse(quoted, strlen(quoted)).has_comments);
}

TEST(PostgreSQLSelectProvenance, CommentsAndQuotedControls) {
    Parser<Dialect::PostgreSQL> parser;
    const char* commented = "SELECT 1 /* outer /* inner */ end */";
    EXPECT_TRUE(parser.parse(commented, strlen(commented)).has_comments);
    auto batch = parser.parse_all(commented, strlen(commented));
    ASSERT_EQ(batch.statements.size(), 1u);
    EXPECT_TRUE(batch.statements[0].result.has_comments);
    const char* quoted = "SELECT '/* no */', \"-- no\"";
    EXPECT_FALSE(parser.parse(quoted, strlen(quoted)).has_comments);
}

TEST(MySQLSelectDepth, BoundsRecursiveExpressionsAndSubqueries) {
    for (const std::string kind : {"paren", "not", "function", "select"}) {
        for (int depth : {16, 512}) {
            std::string sql = "SELECT ";
            for (int i = 0; i < depth; ++i) {
                if (kind == "paren") sql += "(";
                else if (kind == "not") sql += "NOT ";
                else if (kind == "function") sql += "ABS(";
                else sql += "(SELECT ";
            }
            sql += "1";
            if (kind != "not") sql.append(depth, ')');
            Parser<Dialect::MySQL> parser;
            auto result = parser.parse(sql.data(), sql.size());
            if (depth == 16) EXPECT_TRUE(result.ok() && result.full_input) << kind;
            else EXPECT_EQ(result.status, ParseResult::ERROR) << kind;
            auto next = parser.parse("SELECT 1", 8);
            EXPECT_TRUE(next.ok() && next.full_input) << kind;
        }
    }
}

TEST(MySQLSelectDepth, ConfigBatchAndFlatLists) {
    ParserConfig config;
    config.max_parse_depth = 8;
    Parser<Dialect::MySQL> parser(config);
    const std::string nested = "SELECT " + std::string(16, '(') + "1" + std::string(16, ')');
    auto rejected = parser.parse(nested.data(), nested.size());
    EXPECT_EQ(rejected.status, ParseResult::ERROR);
    EXPECT_FALSE(rejected.full_input);
    EXPECT_EQ(std::string(rejected.error.message.ptr, rejected.error.message.len),
              "SQL parse depth limit exceeded");
    const std::string batch_sql = nested + "; SELECT 1";
    auto batch = parser.parse_all(batch_sql.data(), batch_sql.size());
    ASSERT_EQ(batch.statements.size(), 2u);
    EXPECT_EQ(batch.statements[0].result.status, ParseResult::ERROR);
    EXPECT_FALSE(batch.statements[0].result.full_input);
    EXPECT_TRUE(batch.statements[1].result.ok());
    EXPECT_TRUE(batch.statements[1].result.full_input);
    std::string flat = "SELECT 1";
    for (int i = 0; i < 200; ++i) flat += ", 1";
    auto result = parser.parse(flat.data(), flat.size());
    EXPECT_TRUE(result.ok() && result.full_input);
    auto control = parser.parse("SELECT ((1 + 2))", 16);
    EXPECT_TRUE(control.ok() && control.full_input);
}

TEST(PostgreSQLSelectDepth, BoundsRecursiveExpressionsAndSubqueries) {
    for (const std::string kind : {"paren", "not", "function", "select"}) {
        for (int depth : {16, 512}) {
            std::string sql = "SELECT ";
            for (int i = 0; i < depth; ++i) {
                if (kind == "paren") sql += "(";
                else if (kind == "not") sql += "NOT ";
                else if (kind == "function") sql += "ABS(";
                else sql += "(SELECT ";
            }
            sql += "1";
            if (kind != "not") sql.append(depth, ')');
            Parser<Dialect::PostgreSQL> parser;
            auto result = parser.parse(sql.data(), sql.size());
            if (depth == 16) EXPECT_TRUE(result.ok() && result.full_input) << kind;
            else {
                EXPECT_EQ(result.status, ParseResult::ERROR) << kind;
                EXPECT_FALSE(result.full_input) << kind;
            }
            auto next = parser.parse("SELECT 1", 8);
            EXPECT_TRUE(next.ok() && next.full_input) << kind;
        }
    }
}

TEST(MySQLSelectDepth, ConfigurationBoundaries) {
    const std::string nested = "SELECT " + std::string(256, '(') + "1" + std::string(256, ')');
    ParserConfig config;
    config.max_parse_depth = 100000;
    Parser<Dialect::MySQL> capped(config);
    auto result = capped.parse(nested.data(), nested.size());
    EXPECT_EQ(result.status, ParseResult::ERROR);
    EXPECT_FALSE(result.full_input);
    config.max_parse_depth = 0;
    Parser<Dialect::MySQL> zero(config);
    result = zero.parse("SELECT 1", 8);
    EXPECT_EQ(result.status, ParseResult::ERROR);
    EXPECT_FALSE(result.full_input);
    config.max_parse_depth = 4;
    Parser<Dialect::MySQL> lower(config);
    result = lower.parse("SELECT 1", 8);
    EXPECT_TRUE(result.ok() && result.full_input);
    result = lower.parse(nested.data(), nested.size());
    EXPECT_EQ(result.status, ParseResult::ERROR);
    EXPECT_FALSE(result.full_input);
}
