#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLDeleteTest : public ::testing::Test {
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

// ========== Basic DELETE ==========

TEST_F(MySQLDeleteTest, SimpleDelete) {
    auto r = parser.parse("DELETE FROM users WHERE id = 1", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_DELETE_STMT);
}

TEST_F(MySQLDeleteTest, DeleteNoWhere) {
    auto r = parser.parse("DELETE FROM users", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* where = find_child(r.ast, NodeType::NODE_WHERE_CLAUSE);
    EXPECT_EQ(where, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteQualifiedTable) {
    auto r = parser.parse("DELETE FROM mydb.users WHERE id = 1", 35);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteComplexWhere) {
    const char* sql = "DELETE FROM users WHERE status = 'inactive' AND last_login < '2020-01-01'";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL Options ==========

TEST_F(MySQLDeleteTest, DeleteLowPriority) {
    auto r = parser.parse("DELETE LOW_PRIORITY FROM users WHERE id = 1", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* opts = find_child(r.ast, NodeType::NODE_STMT_OPTIONS);
    ASSERT_NE(opts, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteQuick) {
    auto r = parser.parse("DELETE QUICK FROM users WHERE id = 1", 36);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteIgnore) {
    auto r = parser.parse("DELETE IGNORE FROM users WHERE id = 1", 37);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, DeleteAllOptions) {
    auto r = parser.parse("DELETE LOW_PRIORITY QUICK IGNORE FROM users WHERE id = 1", 56);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL ORDER BY + LIMIT ==========

TEST_F(MySQLDeleteTest, DeleteOrderByLimit) {
    const char* sql = "DELETE FROM users WHERE active = 0 ORDER BY created_at ASC LIMIT 100";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLDeleteTest, DeleteLimitOnly) {
    auto r = parser.parse("DELETE FROM users WHERE active = 0 LIMIT 100", 45);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

// ========== MySQL Multi-Table Form 1: DELETE t1, t2 FROM ... ==========

TEST_F(MySQLDeleteTest, MultiTableForm1Single) {
    const char* sql = "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.status = 0";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, MultiTableForm1Multiple) {
    const char* sql = "DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t1.status = 0";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== MySQL Multi-Table Form 2: DELETE FROM t1, t2 USING ... ==========

TEST_F(MySQLDeleteTest, MultiTableForm2) {
    const char* sql = "DELETE FROM t1, t2 USING t1 JOIN t2 ON t1.id = t2.fk WHERE t1.status = 0";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLDeleteTest, MultiTableForm2Single) {
    const char* sql = "DELETE FROM t1 USING t1 JOIN t2 ON t1.id = t2.fk WHERE t2.bad = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== PostgreSQL DELETE ==========

class PgSQLDeleteTest : public ::testing::Test {
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

TEST_F(PgSQLDeleteTest, SimpleDelete) {
    auto r = parser.parse("DELETE FROM users WHERE id = 1", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteUsing) {
    const char* sql = "DELETE FROM users USING orders WHERE users.id = orders.user_id AND orders.bad = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* using_clause = find_child(r.ast, NodeType::NODE_DELETE_USING_CLAUSE);
    ASSERT_NE(using_clause, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteUsingMultiple) {
    const char* sql = "DELETE FROM t1 USING t2, t3 WHERE t1.id = t2.fk AND t2.id = t3.fk";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteReturning) {
    const char* sql = "DELETE FROM users WHERE id = 1 RETURNING *";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteReturningColumns) {
    const char* sql = "DELETE FROM users WHERE id = 1 RETURNING id, name";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* ret = find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE);
    ASSERT_NE(ret, nullptr);
    EXPECT_EQ(child_count(ret), 2);
}

TEST_F(PgSQLDeleteTest, DeleteUsingReturning) {
    const char* sql = "DELETE FROM users USING orders "
                      "WHERE users.id = orders.user_id RETURNING users.id";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_DELETE_USING_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_RETURNING_CLAUSE), nullptr);
}

TEST_F(PgSQLDeleteTest, DeleteWithAlias) {
    const char* sql = "DELETE FROM users AS u WHERE u.id = 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Bulk data-driven tests ==========

struct DeleteTestCase {
    const char* sql;
    const char* description;
};

static const DeleteTestCase mysql_delete_bulk_cases[] = {
    {"DELETE FROM t", "simple no where"},
    {"DELETE FROM t WHERE a = 1", "simple with where"},
    {"DELETE FROM t WHERE a > 1 AND b < 10", "complex where"},
    {"DELETE FROM db.t WHERE a = 1", "qualified table"},
    {"DELETE LOW_PRIORITY FROM t WHERE a = 1", "low priority"},
    {"DELETE QUICK FROM t WHERE a = 1", "quick"},
    {"DELETE IGNORE FROM t WHERE a = 1", "ignore"},
    {"DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE a = 1", "all options"},
    {"DELETE FROM t WHERE a = 1 ORDER BY b LIMIT 10", "order by limit"},
    {"DELETE FROM t WHERE a = 1 LIMIT 100", "limit only"},
    {"DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.x = 0", "multi-table form 1"},
    {"DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.fk", "multi-table form 1 multi target"},
    {"DELETE FROM t1 USING t1 JOIN t2 ON t1.id = t2.fk WHERE t2.x = 0", "multi-table form 2"},
    {"DELETE FROM t1, t2 USING t1 JOIN t2 ON t1.id = t2.fk", "multi-table form 2 multi target"},
};

TEST(MySQLDeleteBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_delete_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

static const DeleteTestCase pgsql_delete_bulk_cases[] = {
    {"DELETE FROM t", "simple no where"},
    {"DELETE FROM t WHERE a = 1", "simple with where"},
    {"DELETE FROM t WHERE a > 1 AND b < 10", "complex where"},
    {"DELETE FROM t AS x WHERE x.a = 1", "alias"},
    {"DELETE FROM t USING t2 WHERE t.id = t2.fk", "using single"},
    {"DELETE FROM t USING t2, t3 WHERE t.id = t2.fk AND t2.id = t3.fk", "using multi"},
    {"DELETE FROM t WHERE a = 1 RETURNING *", "returning star"},
    {"DELETE FROM t WHERE a = 1 RETURNING a, b", "returning cols"},
    {"DELETE FROM t USING t2 WHERE t.id = t2.fk RETURNING t.a", "using + returning"},
};

TEST(PgSQLDeleteBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_delete_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT)
            << "Failed: " << tc.description;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const DeleteTestCase mysql_delete_roundtrip_cases[] = {
    {"DELETE FROM t WHERE a = 1", "simple"},
    {"DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE a = 1", "all options"},
    {"DELETE FROM t WHERE a = 1 ORDER BY b LIMIT 10", "order by limit"},
};

TEST(MySQLDeleteRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : mysql_delete_roundtrip_cases) {
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

static const DeleteTestCase pgsql_delete_roundtrip_cases[] = {
    {"DELETE FROM t WHERE a = 1", "simple"},
    {"DELETE FROM t USING t2 WHERE t.id = t2.fk", "using"},
    {"DELETE FROM t WHERE a = 1 RETURNING *", "returning"},
};

TEST(PgSQLDeleteRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : pgsql_delete_roundtrip_cases) {
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
