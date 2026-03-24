#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

class MySQLCompoundTest : public ::testing::Test {
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

// ========== Simple SELECT (no compound) ==========
// CompoundQueryParser must return bare NODE_SELECT_STMT when no set operator follows

TEST_F(MySQLCompoundTest, PlainSelectUnchanged) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    // Should be NODE_SELECT_STMT, NOT NODE_COMPOUND_QUERY
    EXPECT_EQ(r.ast->type, NodeType::NODE_SELECT_STMT);
}

// ========== UNION ==========

TEST_F(MySQLCompoundTest, SimpleUnion) {
    const char* sql = "SELECT 1 UNION SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
    auto* setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(setop, nullptr);
}

TEST_F(MySQLCompoundTest, UnionAll) {
    const char* sql = "SELECT 1 UNION ALL SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, UnionThreeSelects) {
    const char* sql = "SELECT 1 UNION SELECT 2 UNION SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, UnionWithOrderBy) {
    const char* sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
}

TEST_F(MySQLCompoundTest, UnionWithLimit) {
    const char* sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

TEST_F(MySQLCompoundTest, UnionWithOrderByAndLimit) {
    const char* sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_ORDER_BY_CLAUSE), nullptr);
    EXPECT_NE(find_child(r.ast, NodeType::NODE_LIMIT_CLAUSE), nullptr);
}

// ========== INTERSECT ==========

TEST_F(MySQLCompoundTest, SimpleIntersect) {
    const char* sql = "SELECT 1 INTERSECT SELECT 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, IntersectAll) {
    const char* sql = "SELECT 1 INTERSECT ALL SELECT 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== EXCEPT ==========

TEST_F(MySQLCompoundTest, SimpleExcept) {
    const char* sql = "SELECT 1 EXCEPT SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, ExceptAll) {
    const char* sql = "SELECT 1 EXCEPT ALL SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Precedence: INTERSECT > UNION/EXCEPT ==========

TEST_F(MySQLCompoundTest, IntersectBindsTighterThanUnion) {
    // SELECT 1 UNION SELECT 2 INTERSECT SELECT 3
    // Should parse as: SELECT 1 UNION (SELECT 2 INTERSECT SELECT 3)
    const char* sql = "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);

    // The top-level set operation should be UNION
    auto* top_setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(top_setop, nullptr);
    // The value should contain "UNION"
    StringRef op_text = top_setop->value();
    EXPECT_TRUE(op_text.equals_ci("UNION", 5));

    // The right child of UNION should be a SET_OPERATION (INTERSECT)
    const AstNode* left = top_setop->first_child;
    ASSERT_NE(left, nullptr);
    const AstNode* right = left->next_sibling;
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(right->type, NodeType::NODE_SET_OPERATION);
    StringRef right_op = right->value();
    EXPECT_TRUE(right_op.equals_ci("INTERSECT", 9));
}

TEST_F(MySQLCompoundTest, IntersectBindsTighterThanExcept) {
    // SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3
    // Should parse as: SELECT 1 EXCEPT (SELECT 2 INTERSECT SELECT 3)
    const char* sql = "SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    auto* top_setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(top_setop, nullptr);
    StringRef op_text = top_setop->value();
    EXPECT_TRUE(op_text.equals_ci("EXCEPT", 6));
}

// ========== Parenthesized nesting ==========

TEST_F(MySQLCompoundTest, ParenthesizedUnion) {
    const char* sql = "(SELECT 1) UNION (SELECT 2)";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(MySQLCompoundTest, ParenthesizedOverridesPrecedence) {
    // (SELECT 1 UNION SELECT 2) INTERSECT SELECT 3
    // Parentheses force UNION to be evaluated first
    const char* sql = "(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);

    auto* top_setop = find_child(r.ast, NodeType::NODE_SET_OPERATION);
    ASSERT_NE(top_setop, nullptr);
    StringRef op_text = top_setop->value();
    EXPECT_TRUE(op_text.equals_ci("INTERSECT", 9));
}

// ========== Complex compound queries ==========

TEST_F(MySQLCompoundTest, UnionWithFullSelects) {
    const char* sql = "SELECT a, b FROM t1 WHERE x = 1 UNION ALL SELECT a, b FROM t2 WHERE y = 2 ORDER BY a LIMIT 10";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

// ========== PostgreSQL compound queries ==========

class PgSQLCompoundTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

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

TEST_F(PgSQLCompoundTest, SimpleUnion) {
    const char* sql = "SELECT 1 UNION SELECT 2";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_COMPOUND_QUERY);
}

TEST_F(PgSQLCompoundTest, IntersectExcept) {
    const char* sql = "SELECT 1 INTERSECT SELECT 2 EXCEPT SELECT 3";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLCompoundTest, UnionReturnsCorrectDialect) {
    const char* sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

// ========== Bulk data-driven tests ==========

struct CompoundTestCase {
    const char* sql;
    const char* description;
};

static const CompoundTestCase compound_bulk_cases[] = {
    {"SELECT 1 UNION SELECT 2", "simple union"},
    {"SELECT 1 UNION ALL SELECT 2", "union all"},
    {"SELECT 1 UNION SELECT 2 UNION SELECT 3", "triple union"},
    {"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3", "triple union all"},
    {"SELECT 1 INTERSECT SELECT 2", "simple intersect"},
    {"SELECT 1 INTERSECT ALL SELECT 2", "intersect all"},
    {"SELECT 1 EXCEPT SELECT 2", "simple except"},
    {"SELECT 1 EXCEPT ALL SELECT 2", "except all"},
    {"SELECT 1 UNION SELECT 2 INTERSECT SELECT 3", "union + intersect precedence"},
    {"SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3", "except + intersect precedence"},
    {"(SELECT 1) UNION (SELECT 2)", "parenthesized"},
    {"(SELECT 1 UNION SELECT 2) INTERSECT SELECT 3", "paren override"},
    {"SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a", "trailing order by"},
    {"SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10", "trailing limit"},
    {"SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 5", "trailing order by + limit"},
    {"SELECT * FROM t1 WHERE x = 1 UNION SELECT * FROM t2 WHERE y = 2", "union with where"},
};

TEST(MySQLCompoundBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : compound_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

TEST(PgSQLCompoundBulk, AllCasesParseSuccessfully) {
    Parser<Dialect::PostgreSQL> parser;
    for (const auto& tc : compound_bulk_cases) {
        auto r = parser.parse(tc.sql, strlen(tc.sql));
        EXPECT_EQ(r.status, ParseResult::OK)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
        EXPECT_NE(r.ast, nullptr)
            << "Failed: " << tc.description << "\n  SQL: " << tc.sql;
    }
}

// ========== Round-trip tests ==========

static const CompoundTestCase compound_roundtrip_cases[] = {
    {"SELECT 1 UNION SELECT 2", "simple union"},
    {"SELECT 1 UNION ALL SELECT 2", "union all"},
    {"SELECT 1 INTERSECT SELECT 2", "intersect"},
    {"SELECT 1 EXCEPT SELECT 2", "except"},
    {"SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a", "with order by"},
    {"SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10", "with limit"},
};

TEST(MySQLCompoundRoundTrip, AllCasesRoundTrip) {
    Parser<Dialect::MySQL> parser;
    for (const auto& tc : compound_roundtrip_cases) {
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
