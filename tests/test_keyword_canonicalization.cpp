#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

class MySQLKeywordCanonicalizationTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    const AstNode* find_descendant(const AstNode* node, NodeType type) {
        if (!node) return nullptr;
        if (node->type == type) return node;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (const AstNode* hit = find_descendant(c, type)) return hit;
        }
        return nullptr;
    }

    std::string value_of(const char* sql, NodeType type) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        const AstNode* node = find_descendant(r.ast, type);
        if (!node) return "[NOT_FOUND]";
        return std::string(node->value_ptr, node->value_len);
    }

    std::string child_value_of(const char* sql, NodeType type, int index) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        const AstNode* node = find_descendant(r.ast, type);
        if (!node) return "[NOT_FOUND]";
        const AstNode* child = node->first_child;
        for (int i = 0; i < index && child; ++i) child = child->next_sibling;
        if (!child) return "[NO_CHILD]";
        return std::string(child->value_ptr, child->value_len);
    }
};

class PgSQLKeywordCanonicalizationTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;

    const AstNode* find_descendant(const AstNode* node, NodeType type) {
        if (!node) return nullptr;
        if (node->type == type) return node;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (const AstNode* hit = find_descendant(c, type)) return hit;
        }
        return nullptr;
    }

    std::string value_of(const char* sql, NodeType type) {
        auto r = parser.parse(sql, strlen(sql));
        if (!r.ast) return "[PARSE_FAILED]";
        const AstNode* node = find_descendant(r.ast, type);
        if (!node) return "[NOT_FOUND]";
        return std::string(node->value_ptr, node->value_len);
    }
};

// ========== Set operators ==========

TEST_F(MySQLKeywordCanonicalizationTest, SetOperators) {
    EXPECT_EQ(value_of("SELECT 1 union SELECT 2", NodeType::NODE_SET_OPERATION), "UNION");
    EXPECT_EQ(value_of("SELECT 1 UnIoN SELECT 2", NodeType::NODE_SET_OPERATION), "UNION");
    EXPECT_EQ(value_of("SELECT 1 intersect SELECT 2", NodeType::NODE_SET_OPERATION), "INTERSECT");
    EXPECT_EQ(value_of("SELECT 1 except SELECT 2", NodeType::NODE_SET_OPERATION), "EXCEPT");
}

// ========== Expression operators ==========

TEST_F(MySQLKeywordCanonicalizationTest, BinaryOperators) {
    EXPECT_EQ(value_of("SELECT a FROM t WHERE a and b", NodeType::NODE_BINARY_OP), "AND");
    EXPECT_EQ(value_of("SELECT a FROM t WHERE a or b", NodeType::NODE_BINARY_OP), "OR");
    EXPECT_EQ(value_of("SELECT a FROM t WHERE b like 'x'", NodeType::NODE_BINARY_OP), "LIKE");
}

TEST_F(MySQLKeywordCanonicalizationTest, UnaryOperators) {
    EXPECT_EQ(value_of("SELECT not a FROM t", NodeType::NODE_UNARY_OP), "NOT");
}

// ========== Literals ==========

TEST_F(MySQLKeywordCanonicalizationTest, NullLiteral) {
    EXPECT_EQ(value_of("SELECT null", NodeType::NODE_LITERAL_NULL), "NULL");
}

TEST_F(MySQLKeywordCanonicalizationTest, NullLiteralKeepsLosslessSource) {
    const char* sql = "SELECT null";
    auto r = parser.parse(sql, strlen(sql));
    ASSERT_NE(r.ast, nullptr);
    const AstNode* node = find_descendant(r.ast, NodeType::NODE_LITERAL_NULL);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(std::string(node->source().ptr, node->source().len), "null");
}

// ========== Clause keywords ==========

TEST_F(MySQLKeywordCanonicalizationTest, JoinTypes) {
    EXPECT_EQ(value_of("SELECT a FROM t1 inner join t2 ON t1.a = t2.b",
                       NodeType::NODE_JOIN_CLAUSE), "INNER JOIN");
    EXPECT_EQ(value_of("SELECT a FROM t1 left outer join t2 ON t1.a = t2.b",
                       NodeType::NODE_JOIN_CLAUSE), "LEFT OUTER JOIN");
}

TEST_F(MySQLKeywordCanonicalizationTest, OrderByDirection) {
    EXPECT_EQ(child_value_of("SELECT a FROM t ORDER BY a desc",
                             NodeType::NODE_ORDER_BY_ITEM, 1), "DESC");
    EXPECT_EQ(child_value_of("SELECT a FROM t ORDER BY a asc",
                             NodeType::NODE_ORDER_BY_ITEM, 1), "ASC");
}

TEST_F(MySQLKeywordCanonicalizationTest, SelectOptions) {
    EXPECT_EQ(child_value_of("SELECT distinct a FROM t",
                             NodeType::NODE_SELECT_OPTIONS, 0), "DISTINCT");
}

TEST_F(MySQLKeywordCanonicalizationTest, LockStrength) {
    EXPECT_EQ(child_value_of("SELECT a FROM t for update",
                             NodeType::NODE_LOCKING_CLAUSE, 0), "UPDATE");
}

// ========== Function names ==========

TEST_F(MySQLKeywordCanonicalizationTest, FunctionNames) {
    EXPECT_EQ(value_of("SELECT count(*) FROM t", NodeType::NODE_FUNCTION_CALL), "COUNT");
    EXPECT_EQ(value_of("SELECT MaX(a) FROM t", NodeType::NODE_FUNCTION_CALL), "MAX");
}

TEST_F(PgSQLKeywordCanonicalizationTest, UndelimitedFunctionNamesFoldDown) {
    EXPECT_EQ(value_of("SELECT MYFUNC(a) FROM t", NodeType::NODE_FUNCTION_CALL), "myfunc");
    EXPECT_EQ(value_of("SELECT MyFunc(a) FROM t", NodeType::NODE_FUNCTION_CALL), "myfunc");
}

TEST_F(PgSQLKeywordCanonicalizationTest, DelimitedFunctionNameKeepsItsOwnSpelling) {
    // PostgreSQL folds undelimited names down, so "MYFUNC" is a different function.
    EXPECT_EQ(value_of("SELECT \"MYFUNC\"(a) FROM t", NodeType::NODE_FUNCTION_CALL), "MYFUNC");
}

// ========== Identifiers are not keywords ==========

TEST_F(MySQLKeywordCanonicalizationTest, IdentifiersKeepTheirCase) {
    // Table and column names are case-sensitive wherever the filesystem is.
    EXPECT_EQ(value_of("SELECT a FROM MyTable", NodeType::NODE_IDENTIFIER), "MyTable");
    EXPECT_EQ(value_of("SELECT MyCol FROM t", NodeType::NODE_COLUMN_REF), "MyCol");
}
