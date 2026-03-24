#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/expression_parser.h"

using namespace sql_parser;

// Helper: parse an expression from a SQL string using a fresh parser context.
// We use the tokenizer directly since expression parsing is an internal function.
class ExpressionTest : public ::testing::Test {
protected:
    Arena arena{4096};
    Tokenizer<Dialect::MySQL> tok;

    AstNode* parse_expr(const char* sql) {
        tok.reset(sql, strlen(sql));
        ExpressionParser<Dialect::MySQL> ep(tok, arena);
        return ep.parse();
    }
};

// ===== Task 1: Literals and Identifiers =====

TEST_F(ExpressionTest, IntegerLiteral) {
    AstNode* node = parse_expr("42");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "42");
}

TEST_F(ExpressionTest, FloatLiteral) {
    AstNode* node = parse_expr("3.14");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_FLOAT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "3.14");
}

TEST_F(ExpressionTest, StringLiteral) {
    AstNode* node = parse_expr("'hello'");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "hello");
}

TEST_F(ExpressionTest, NullLiteral) {
    AstNode* node = parse_expr("NULL");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_NULL);
}

TEST_F(ExpressionTest, TrueLiteral) {
    AstNode* node = parse_expr("TRUE");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "TRUE");
}

TEST_F(ExpressionTest, FalseLiteral) {
    AstNode* node = parse_expr("FALSE");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "FALSE");
}

TEST_F(ExpressionTest, SimpleIdentifier) {
    AstNode* node = parse_expr("my_column");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_COLUMN_REF);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "my_column");
}

TEST_F(ExpressionTest, QualifiedIdentifier) {
    AstNode* node = parse_expr("t.col");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_QUALIFIED_NAME);
    // first child = table, second child = column
    ASSERT_NE(node->first_child, nullptr);
    ASSERT_NE(node->first_child->next_sibling, nullptr);
}

TEST_F(ExpressionTest, Asterisk) {
    AstNode* node = parse_expr("*");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_ASTERISK);
}

TEST_F(ExpressionTest, Placeholder) {
    AstNode* node = parse_expr("?");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_PLACEHOLDER);
}

TEST_F(ExpressionTest, DefaultKeyword) {
    AstNode* node = parse_expr("DEFAULT");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IDENTIFIER);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "DEFAULT");
}

TEST_F(ExpressionTest, UserVariable) {
    AstNode* node = parse_expr("@my_var");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_COLUMN_REF);
}

TEST_F(ExpressionTest, ParenthesizedExpression) {
    AstNode* node = parse_expr("(42)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_LITERAL_INT);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "42");
}

// ===== Task 2: Binary Operators, IS NULL, BETWEEN, IN, Functions =====

TEST_F(ExpressionTest, BinaryAdd) {
    AstNode* node = parse_expr("1 + 2");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "+");
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_LITERAL_INT);
    ASSERT_NE(node->first_child->next_sibling, nullptr);
    EXPECT_EQ(node->first_child->next_sibling->type, NodeType::NODE_LITERAL_INT);
}

TEST_F(ExpressionTest, Precedence_MulOverAdd) {
    // 1 + 2 * 3 should parse as 1 + (2 * 3)
    AstNode* node = parse_expr("1 + 2 * 3");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "+");
    // Right child should be 2*3
    AstNode* right = node->first_child->next_sibling;
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(right->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(right->value_ptr, right->value_len), "*");
}

TEST_F(ExpressionTest, ComparisonEqual) {
    AstNode* node = parse_expr("x = 1");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "=");
}

TEST_F(ExpressionTest, LogicalAnd) {
    AstNode* node = parse_expr("a = 1 AND b = 2");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "AND");
}

TEST_F(ExpressionTest, LogicalOr) {
    AstNode* node = parse_expr("a = 1 OR b = 2");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "OR");
}

TEST_F(ExpressionTest, UnaryMinus) {
    AstNode* node = parse_expr("-42");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP);
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_LITERAL_INT);
}

TEST_F(ExpressionTest, UnaryNot) {
    AstNode* node = parse_expr("NOT x");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP);
}

TEST_F(ExpressionTest, IsNull) {
    AstNode* node = parse_expr("x IS NULL");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IS_NULL);
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_COLUMN_REF);
}

TEST_F(ExpressionTest, IsNotNull) {
    AstNode* node = parse_expr("x IS NOT NULL");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IS_NOT_NULL);
}

TEST_F(ExpressionTest, Between) {
    AstNode* node = parse_expr("x BETWEEN 1 AND 10");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BETWEEN);
    // 3 children: expr, low, high
    ASSERT_NE(node->first_child, nullptr);
    ASSERT_NE(node->first_child->next_sibling, nullptr);
    ASSERT_NE(node->first_child->next_sibling->next_sibling, nullptr);
}

TEST_F(ExpressionTest, InList) {
    AstNode* node = parse_expr("x IN (1, 2, 3)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_IN_LIST);
    // Children: expr, val1, val2, val3 = 4 children
    int count = 0;
    for (AstNode* c = node->first_child; c; c = c->next_sibling) ++count;
    EXPECT_EQ(count, 4);
}

TEST_F(ExpressionTest, FunctionCall) {
    AstNode* node = parse_expr("COUNT(*)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_FUNCTION_CALL);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "COUNT");
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_ASTERISK);
}

TEST_F(ExpressionTest, FunctionCallMultiArg) {
    AstNode* node = parse_expr("COALESCE(a, b, 0)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_FUNCTION_CALL);
    int count = 0;
    for (AstNode* c = node->first_child; c; c = c->next_sibling) ++count;
    EXPECT_EQ(count, 3);
}

TEST_F(ExpressionTest, NestedParens) {
    AstNode* node = parse_expr("(1 + 2) * 3");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "*");
    // Left child should be 1+2
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->first_child->value_ptr, node->first_child->value_len), "+");
}

TEST_F(ExpressionTest, LikeOperator) {
    AstNode* node = parse_expr("name LIKE '%test%'");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(std::string(node->value_ptr, node->value_len), "LIKE");
}

TEST_F(ExpressionTest, StringConcat) {
    AstNode* node = parse_expr("a || b");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_BINARY_OP);
}

TEST_F(ExpressionTest, NotIn) {
    AstNode* node = parse_expr("x NOT IN (1, 2)");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP); // NOT wraps IN_LIST
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_IN_LIST);
}

TEST_F(ExpressionTest, NotBetween) {
    AstNode* node = parse_expr("x NOT BETWEEN 1 AND 10");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP); // NOT wraps BETWEEN
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_BETWEEN);
}

TEST_F(ExpressionTest, NotLike) {
    AstNode* node = parse_expr("name NOT LIKE '%test'");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_UNARY_OP); // NOT wraps LIKE
    ASSERT_NE(node->first_child, nullptr);
    EXPECT_EQ(node->first_child->type, NodeType::NODE_BINARY_OP);
}

TEST_F(ExpressionTest, CaseWhenSimple) {
    AstNode* node = parse_expr("CASE WHEN x = 1 THEN 'a' ELSE 'b' END");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_CASE_WHEN);
}

TEST_F(ExpressionTest, CaseWhenSearched) {
    AstNode* node = parse_expr("CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_CASE_WHEN);
}

TEST_F(ExpressionTest, ZeroArgFunction) {
    AstNode* node = parse_expr("NOW()");
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->type, NodeType::NODE_FUNCTION_CALL);
    EXPECT_EQ(node->first_child, nullptr); // no args
}
