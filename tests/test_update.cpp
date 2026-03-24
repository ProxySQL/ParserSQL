#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLUpdateTest : public ::testing::Test {
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

    std::string round_trip(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        return std::string(result.ptr, result.len);
    }
};

// ========== Basic UPDATE ==========

TEST_F(MySQLUpdateTest, SimpleUpdate) {
    auto r = parser.parse("UPDATE users SET name = 'Alice' WHERE id = 1", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::UPDATE);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_UPDATE_STMT);
}

TEST_F(MySQLUpdateTest, UpdateMultipleColumns) {
    const char* sql = "UPDATE users SET name = 'Alice', email = 'a@b.com' WHERE id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* set_clause = find_child(r.ast, NodeType::NODE_UPDATE_SET_CLAUSE);
    ASSERT_NE(set_clause, nullptr);
    EXPECT_EQ(child_count(set_clause), 2);
}

TEST_F(MySQLUpdateTest, UpdateNoWhere) {
    auto r = parser.parse("UPDATE users SET active = 0", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* where = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    EXPECT_EQ(where, nullptr);
}

TEST_F(MySQLUpdateTest, UpdateQualifiedTable) {
    auto r = parser.parse("UPDATE mydb.users SET name = 'x' WHERE id = 1", 46);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL Options ==========

TEST_F(MySQLUpdateTest, UpdateLowPriority) {
    auto r = parser.parse("UPDATE LOW_PRIORITY users SET name = 'x' WHERE id = 1", 54);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_STMT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLUpdateTest, UpdateIgnore) {
    auto r = parser.parse("UPDATE IGNORE users SET name = 'x' WHERE id = 1", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLUpdateTest, UpdateLowPriorityIgnore) {
    auto r = parser.parse("UPDATE LOW_PRIORITY IGNORE users SET name = 'x' WHERE id = 1", 61);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL ORDER BY + LIMIT ==========

TEST_F(MySQLUpdateTest, UpdateOrderByLimit) {
    const char* sql = "UPDATE users SET rank = rank + 1 WHERE active = 1 ORDER BY score DESC LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLUpdateTest, UpdateLimit) {
    auto r = parser.parse("UPDATE users SET active = 0 LIMIT 100", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

// ========== MySQL Multi-Table UPDATE ==========

TEST_F(MySQLUpdateTest, MultiTableJoin) {
    const char* sql = "UPDATE users u JOIN orders o ON u.id = o.user_id "
                      "SET u.total = u.total + o.amount WHERE o.status = 'shipped'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLUpdateTest, MultiTableCommaJoin) {
    const char* sql = "UPDATE users, orders SET users.total = orders.amount "
                      "WHERE users.id = orders.user_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLUpdateTest, MultiTableLeftJoin) {
    const char* sql = "UPDATE users u LEFT JOIN orders o ON u.id = o.user_id "
                      "SET u.has_orders = 0 WHERE o.id IS NULL";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== PostgreSQL UPDATE ==========

class PgSQLUpdateTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

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

    std::string round_trip(const char* sql) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        return std::string(result.ptr, result.len);
    }
};

TEST_F(PgSQLUpdateTest, SimpleUpdate) {
    auto r = parser.parse("UPDATE users SET name = 'Alice' WHERE id = 1", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::UPDATE);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateFrom) {
    const char* sql = "UPDATE users SET total = orders.amount FROM orders WHERE users.id = orders.user_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* from = find_child(r.ast, NodeType::NODE_FROM_CLAUSE);
    ASSERT_NE(from, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateFromMultipleTables) {
    const char* sql = "UPDATE users SET total = o.amount "
                      "FROM orders o, payments p "
                      "WHERE users.id = o.user_id AND o.id = p.order_id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateReturning) {
    const char* sql = "UPDATE users SET name = 'Alice' WHERE id = 1 RETURNING id, name";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
    EXPECT_EQ(child_count(ret), 2);
}

TEST_F(PgSQLUpdateTest, UpdateReturningStar) {
    const char* sql = "UPDATE users SET name = 'Alice' WHERE id = 1 RETURNING *";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateFromReturning) {
    const char* sql = "UPDATE users SET total = o.amount FROM orders o "
                      "WHERE users.id = o.user_id RETURNING users.id, users.total";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_FROM_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE), nullptr);
}

TEST_F(PgSQLUpdateTest, UpdateWithAlias) {
    const char* sql = "UPDATE users AS u SET name = 'Alice' WHERE u.id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Bulk data-driven tests ==========

struct UpdateTestCase {
    const char* sql;
    const char* description;
};

static const UpdateTestCase mysql_update_bulk_cases[] = {
    {"UPDATE t SET a = 1", "simple no where"},
    {"UPDATE t SET a = 1 WHERE b = 2", "simple with where"},
    {"UPDATE t SET a = 1, b = 2 WHERE c = 3", "multi column"},
    {"UPDATE t SET a = a + 1 WHERE b > 0", "expression value"},
    {"UPDATE t SET a = 'hello' WHERE b = 1", "string value"},
    {"UPDATE db.t SET a = 1", "qualified table"},
    {"UPDATE LOW_PRIORITY t SET a = 1", "low priority"},
    {"UPDATE IGNORE t SET a = 1", "ignore"},
    {"UPDATE LOW_PRIORITY IGNORE t SET a = 1", "low priority ignore"},
    {"UPDATE t SET a = 1 ORDER BY b LIMIT 10", "order by limit"},
    {"UPDATE t SET a = 1 LIMIT 100", "limit only"},
    {"UPDATE t1 JOIN t2 ON t1.id = t2.fk SET t1.a = t2.b", "join update"},
    {"UPDATE t1, t2 SET t1.a = t2.b WHERE t1.id = t2.fk", "comma join update"},
    {"UPDATE t1 LEFT JOIN t2 ON t1.id = t2.fk SET t1.a = 0 WHERE t2.id IS NULL", "left join"},
    {"UPDATE t SET a = NOW()", "function in value"},
    {"UPDATE t SET a = NULL WHERE b = 1", "set null"},
    {"UPDATE t SET a = CASE WHEN b > 0 THEN 1 ELSE 0 END", "case expression"},
};

TEST(MySQLUpdateBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_update_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::UPDATE)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

static const UpdateTestCase pgsql_update_bulk_cases[] = {
    {"UPDATE t SET a = 1", "simple no where"},
    {"UPDATE t SET a = 1 WHERE b = 2", "simple with where"},
    {"UPDATE t SET a = 1, b = 2 WHERE c = 3", "multi column"},
    {"UPDATE t AS x SET a = 1 WHERE x.b = 2", "alias"},
    {"UPDATE t SET a = 1 FROM t2 WHERE t.id = t2.fk", "from clause"},
    {"UPDATE t SET a = t2.b FROM t2, t3 WHERE t.id = t2.fk AND t2.id = t3.fk", "from multi"},
    {"UPDATE t SET a = 1 WHERE b = 2 RETURNING *", "returning star"},
    {"UPDATE t SET a = 1 WHERE b = 2 RETURNING a, b", "returning cols"},
    {"UPDATE t SET a = 1 FROM t2 WHERE t.id = t2.fk RETURNING t.a", "from + returning"},
};

TEST(PgSQLUpdateBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_update_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::UPDATE)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const UpdateTestCase mysql_update_roundtrip_cases[] = {
    {"UPDATE t SET a = 1 WHERE b = 2", "simple"},
    {"UPDATE t SET a = 1, b = 'x' WHERE c = 3", "multi col"},
    {"UPDATE LOW_PRIORITY IGNORE t SET a = 1", "options"},
    {"UPDATE t SET a = 1 ORDER BY b DESC LIMIT 10", "order by limit"},
};

TEST(MySQLUpdateRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_update_roundtrip_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        ASSERT_NE(r.ast, nullptr)
            << "Parse failed: " << tc.description << "\n  SQL: " << tc.sql;
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        std::string out(result.ptr, result.len);
        EXPECT_EQ(out, std::string(tc.sql))
            << "Round-trip mismatch: " << tc.description;
    }
}

static const UpdateTestCase pgsql_update_roundtrip_cases[] = {
    {"UPDATE t SET a = 1 WHERE b = 2", "simple"},
    {"UPDATE t SET a = 1 FROM t2 WHERE t.id = t2.fk", "from clause"},
    {"UPDATE t SET a = 1 WHERE b = 2 RETURNING *", "returning"},
};

TEST(PgSQLUpdateRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_update_roundtrip_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        ASSERT_NE(r.ast, nullptr)
            << "Parse failed: " << tc.description << "\n  SQL: " << tc.sql;
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        StringRef result = emitter.result();
        std::string out(result.ptr, result.len);
        EXPECT_EQ(out, std::string(tc.sql))
            << "Round-trip mismatch: " << tc.description;
    }
}
