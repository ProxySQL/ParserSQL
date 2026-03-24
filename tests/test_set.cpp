#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

class MySQLSetTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;

    // Helper to count children of a node
    int child_count(const AstNode* node) {
        int n = 0;
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
        return n;
    }
};

TEST_F(MySQLSetTest, SetSimpleVariable) {
    auto r = parser.parse("SET autocommit = 1", 18);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SET_STMT);
    // One assignment child
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_VAR_ASSIGNMENT);
}

TEST_F(MySQLSetTest, SetMultipleVariables) {
    auto r = parser.parse("SET autocommit = 1, wait_timeout = 28800", 41);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(child_count(r.ast), 2);
}

TEST_F(MySQLSetTest, SetGlobalVariable) {
    auto r = parser.parse("SET GLOBAL max_connections = 100", 31);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    AstNode* assignment = r.ast->first_child;
    ASSERT_NE(assignment, nullptr);
    // First child of assignment is the var target
    AstNode* target = assignment->first_child;
    ASSERT_NE(target, nullptr);
    EXPECT_EQ(target->type, NodeType::NODE_VAR_TARGET);
}

TEST_F(MySQLSetTest, SetSessionVariable) {
    auto r = parser.parse("SET SESSION wait_timeout = 600", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetDoubleAtVariable) {
    auto r = parser.parse("SET @@session.wait_timeout = 600", 32);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetUserVariable) {
    auto r = parser.parse("SET @my_var = 42", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetNames) {
    auto r = parser.parse("SET NAMES utf8mb4", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    EXPECT_EQ(r.ast->type, NodeType::NODE_SET_STMT);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_NAMES);
}

TEST_F(MySQLSetTest, SetNamesCollate) {
    auto r = parser.parse("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci", 44);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_NAMES);
    // Should have 2 children: charset and collation
    EXPECT_EQ(child_count(r.ast->first_child), 2);
}

TEST_F(MySQLSetTest, SetCharacterSet) {
    auto r = parser.parse("SET CHARACTER SET utf8", 21);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_CHARSET);
}

TEST_F(MySQLSetTest, SetCharset) {
    auto r = parser.parse("SET CHARSET utf8", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_CHARSET);
}

TEST_F(MySQLSetTest, SetTransaction) {
    auto r = parser.parse("SET TRANSACTION READ ONLY", 25);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
    ASSERT_NE(r.ast->first_child, nullptr);
    EXPECT_EQ(r.ast->first_child->type, NodeType::NODE_SET_TRANSACTION);
}

TEST_F(MySQLSetTest, SetTransactionIsolation) {
    auto r = parser.parse("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", 48);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetGlobalTransaction) {
    auto r = parser.parse("SET GLOBAL TRANSACTION READ WRITE", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetExpressionRHS) {
    auto r = parser.parse("SET @x = 1 + 2", 14);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetColonEqual) {
    auto r = parser.parse("SET @x := 42", 12);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetNamesDefault) {
    auto r = parser.parse("SET NAMES DEFAULT", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(MySQLSetTest, SetWithSemicolon) {
    const char* sql = "SET autocommit = 0; BEGIN";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    EXPECT_TRUE(r.has_remaining());
}

// ========== PostgreSQL SET Tests ==========

class PgSQLSetTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;
};

TEST_F(PgSQLSetTest, SetVarToValue) {
    auto r = parser.parse("SET client_encoding TO 'UTF8'", 29);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSetTest, SetVarEqualValue) {
    auto r = parser.parse("SET work_mem = '256MB'", 22);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSetTest, SetLocalVar) {
    auto r = parser.parse("SET LOCAL timezone = 'UTC'", 25);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}

TEST_F(PgSQLSetTest, SetNamesPostgres) {
    auto r = parser.parse("SET NAMES 'UTF8'", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    ASSERT_NE(r.ast, nullptr);
}
