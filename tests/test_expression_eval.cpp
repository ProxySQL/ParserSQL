#include <gtest/gtest.h>
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include <string>

using namespace sql_engine;
using namespace sql_parser;

// Test fixture with arena, registry, and a simple column resolver.
class ExprEvalTest : public ::testing::Test {
protected:
    Arena arena{4096};
    FunctionRegistry<Dialect::MySQL> mysql_funcs;
    FunctionRegistry<Dialect::PostgreSQL> pg_funcs;

    void SetUp() override {
        mysql_funcs.register_builtins();
        pg_funcs.register_builtins();
    }

    // Default resolver: resolve "x" -> 10, "y" -> 20, "name" -> "hello"
    std::function<Value(StringRef)> resolver = [](StringRef name) -> Value {
        if (name.equals_ci("x", 1)) return value_int(10);
        if (name.equals_ci("y", 1)) return value_int(20);
        if (name.equals_ci("name", 4)) return value_string(StringRef{"hello", 5});
        if (name.equals_ci("flag", 4)) return value_bool(true);
        if (name.equals_ci("nothing", 7)) return value_null();
        if (name.equals_ci("t.col", 5)) return value_int(42);
        return value_null();
    };

    // Helper: make a leaf node
    AstNode* leaf(NodeType type, const char* val) {
        return make_node(arena, type, StringRef{val, static_cast<uint32_t>(std::strlen(val))});
    }

    // Helper: make a node with children
    AstNode* node_with_children(NodeType type, const char* val,
                                std::initializer_list<AstNode*> children) {
        AstNode* n = make_node(arena, type,
            val ? StringRef{val, static_cast<uint32_t>(std::strlen(val))} : StringRef{});
        for (AstNode* c : children) n->add_child(c);
        return n;
    }

    // Helper: evaluate with MySQL dialect
    Value eval_mysql(AstNode* node) {
        return evaluate_expression<Dialect::MySQL>(node, resolver, mysql_funcs, arena);
    }

    // Helper: evaluate with PostgreSQL dialect
    Value eval_pg(AstNode* node) {
        return evaluate_expression<Dialect::PostgreSQL>(node, resolver, pg_funcs, arena);
    }
};

// ===== Leaf Nodes =====

TEST_F(ExprEvalTest, LiteralInt) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "42"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, LiteralIntNegative) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "-7"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, -7);
}

TEST_F(ExprEvalTest, LiteralIntTrue) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "TRUE"));
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, LiteralIntFalse) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "FALSE"));
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

TEST_F(ExprEvalTest, LiteralFloat) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_FLOAT, "3.14"));
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.14);
}

TEST_F(ExprEvalTest, LiteralString) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_STRING, "hello"));
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello");
}

TEST_F(ExprEvalTest, LiteralNull) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_NULL, "NULL"));
    EXPECT_TRUE(v.is_null());
}

TEST_F(ExprEvalTest, ColumnRef) {
    auto v = eval_mysql(leaf(NodeType::NODE_COLUMN_REF, "x"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 10);
}

TEST_F(ExprEvalTest, ColumnRefUnknown) {
    auto v = eval_mysql(leaf(NodeType::NODE_COLUMN_REF, "unknown_col"));
    EXPECT_TRUE(v.is_null());
}

TEST_F(ExprEvalTest, Identifier) {
    auto v = eval_mysql(leaf(NodeType::NODE_IDENTIFIER, "y"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 20);
}

TEST_F(ExprEvalTest, Asterisk) {
    auto v = eval_mysql(leaf(NodeType::NODE_ASTERISK, "*"));
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "*");
}

TEST_F(ExprEvalTest, Placeholder) {
    auto v = eval_mysql(leaf(NodeType::NODE_PLACEHOLDER, "?"));
    EXPECT_TRUE(v.is_null());
}

TEST_F(ExprEvalTest, QualifiedName) {
    AstNode* tbl = leaf(NodeType::NODE_IDENTIFIER, "t");
    AstNode* col = leaf(NodeType::NODE_IDENTIFIER, "col");
    AstNode* qn = node_with_children(NodeType::NODE_QUALIFIED_NAME, nullptr, {tbl, col});
    auto v = eval_mysql(qn);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, ExpressionWrapper) {
    AstNode* inner = leaf(NodeType::NODE_LITERAL_INT, "99");
    AstNode* wrapper = node_with_children(NodeType::NODE_EXPRESSION, nullptr, {inner});
    auto v = eval_mysql(wrapper);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 99);
}

TEST_F(ExprEvalTest, NullExpr) {
    EXPECT_TRUE(eval_mysql(nullptr).is_null());
}

// ===== Unary Operators =====

TEST_F(ExprEvalTest, UnaryMinus) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "42");
    AstNode* neg = node_with_children(NodeType::NODE_UNARY_OP, "-", {child});
    auto v = eval_mysql(neg);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, -42);
}

TEST_F(ExprEvalTest, UnaryMinusDouble) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_FLOAT, "3.14");
    AstNode* neg = node_with_children(NodeType::NODE_UNARY_OP, "-", {child});
    auto v = eval_mysql(neg);
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, -3.14);
}

TEST_F(ExprEvalTest, UnaryMinusNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* neg = node_with_children(NodeType::NODE_UNARY_OP, "-", {child});
    EXPECT_TRUE(eval_mysql(neg).is_null());
}

TEST_F(ExprEvalTest, UnaryNotTrue) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    auto v = eval_mysql(not_node);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

TEST_F(ExprEvalTest, UnaryNotFalse) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    auto v = eval_mysql(not_node);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, UnaryNotNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    EXPECT_TRUE(eval_mysql(not_node).is_null());
}

TEST_F(ExprEvalTest, UnaryNotInt) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "0");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    auto v = eval_mysql(not_node);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);  // NOT 0 = TRUE
}

// ===== Binary Arithmetic =====

TEST_F(ExprEvalTest, Add) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "+", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(ExprEvalTest, SubtractDouble) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_FLOAT, "10.5");
    AstNode* r = leaf(NodeType::NODE_LITERAL_FLOAT, "3.2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "-", {l, r});
    auto v = eval_mysql(op);
    EXPECT_NEAR(v.double_val, 7.3, 1e-9);
}

TEST_F(ExprEvalTest, Multiply) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "6");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "7");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "*", {l, r});
    EXPECT_EQ(eval_mysql(op).int_val, 42);
}

TEST_F(ExprEvalTest, Divide) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "/", {l, r});
    EXPECT_EQ(eval_mysql(op).int_val, 3);  // integer division
}

TEST_F(ExprEvalTest, DivisionByZeroReturnsNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "0");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "/", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, Modulo) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "%", {l, r});
    EXPECT_EQ(eval_mysql(op).int_val, 1);
}

TEST_F(ExprEvalTest, ArithmeticNullPropagation) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "+", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, IntDoubleCoercion) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_FLOAT, "2.5");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "+", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.5);
}

// ===== Comparison =====

TEST_F(ExprEvalTest, Equal) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "=", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, NotEqual) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<>", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LessThan) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, GreaterThan) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, ">", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LessThanOrEqual) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<=", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, GreaterThanOrEqual) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, ">=", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, ComparisonNullPropagation) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "=", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, StringComparison) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "abc");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "def");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

// ===== Logical =====

TEST_F(ExprEvalTest, AndTrueTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, AndFalseShortCircuit) {
    // FALSE AND <anything> = FALSE, right side should not matter
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, AndNullTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, AndNullFalse) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, OrFalseTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, OrTrueShortCircuit) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, OrNullFalse) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, OrNullNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

// ===== IS / IS NOT =====

TEST_F(ExprEvalTest, IsTrueOnTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsTrueOnFalse) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsTrueOnNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsFalseOnFalse) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsNotFalseOnTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS NOT", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsNotTrueOnNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS NOT", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

// ===== LIKE =====

TEST_F(ExprEvalTest, LikeMatch) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "hel%");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "LIKE", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LikeNoMatch) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "xyz%");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "LIKE", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LikeNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "%");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "LIKE", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

// ===== || : concat in PgSQL, OR in MySQL =====

TEST_F(ExprEvalTest, DoublePipePgConcat) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, " world");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "||", {l, r});
    auto v = eval_pg(op);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello world");
}

TEST_F(ExprEvalTest, DoublePipeMySQLOr) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "||", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

// ===== IS NULL / IS NOT NULL =====

TEST_F(ExprEvalTest, IsNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* n = node_with_children(NodeType::NODE_IS_NULL, nullptr, {child});
    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, IsNullOnNonNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_IS_NULL, nullptr, {child});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, IsNotNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_IS_NOT_NULL, nullptr, {child});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, IsNotNullOnNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* n = node_with_children(NodeType::NODE_IS_NOT_NULL, nullptr, {child});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

// ===== BETWEEN =====

TEST_F(ExprEvalTest, BetweenInRange) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "5");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, BetweenOutOfRange) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "15");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, BetweenBoundaryInclusive) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, BetweenNull) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_TRUE(eval_mysql(n).is_null());
}

// ===== IN list =====

TEST_F(ExprEvalTest, InListMatch) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* v2   = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* v3   = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1, v2, v3});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, InListNoMatch) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "5");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* v2   = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1, v2});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, InListNullExpr) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1});
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, InListNullInValues) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "5");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* v2   = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1, v2});
    // No match, but has NULL -> result is NULL
    EXPECT_TRUE(eval_mysql(n).is_null());
}

// ===== CASE/WHEN =====

TEST_F(ExprEvalTest, SearchedCaseWhen) {
    // CASE WHEN TRUE THEN 1 ELSE 2 END  (flags=0: searched)
    AstNode* when_cond = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* then_val  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* else_val  = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {when_cond, then_val, else_val});
    n->flags = 0;
    auto v = eval_mysql(n);
    EXPECT_EQ(v.int_val, 1);
}

TEST_F(ExprEvalTest, SearchedCaseElse) {
    // CASE WHEN FALSE THEN 1 ELSE 2 END
    AstNode* when_cond = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* then_val  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* else_val  = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {when_cond, then_val, else_val});
    n->flags = 0;
    auto v = eval_mysql(n);
    EXPECT_EQ(v.int_val, 2);
}

TEST_F(ExprEvalTest, SearchedCaseNoMatch) {
    // CASE WHEN FALSE THEN 1 END -> NULL
    AstNode* when_cond = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* then_val  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {when_cond, then_val});
    n->flags = 0;
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, SimpleCaseWhen) {
    // CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END
    AstNode* case_expr = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* when1 = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* then1 = leaf(NodeType::NODE_LITERAL_STRING, "one");
    AstNode* when2 = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* then2 = leaf(NodeType::NODE_LITERAL_STRING, "two");
    AstNode* else_val = leaf(NodeType::NODE_LITERAL_STRING, "other");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {case_expr, when1, then1, when2, then2, else_val});
    n->flags = 1;  // simple CASE
    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "one");
}

TEST_F(ExprEvalTest, SimpleCaseElse) {
    // CASE 99 WHEN 1 THEN 'one' ELSE 'other' END
    AstNode* case_expr = leaf(NodeType::NODE_LITERAL_INT, "99");
    AstNode* when1 = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* then1 = leaf(NodeType::NODE_LITERAL_STRING, "one");
    AstNode* else_val = leaf(NodeType::NODE_LITERAL_STRING, "other");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {case_expr, when1, then1, else_val});
    n->flags = 1;
    auto v = eval_mysql(n);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "other");
}

// ===== Function Calls =====

TEST_F(ExprEvalTest, FunctionCallAbs) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_INT, "-42");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "ABS", {arg});
    auto v = eval_mysql(fn);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, FunctionCallUpper) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "UPPER", {arg});
    auto v = eval_mysql(fn);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "HELLO");
}

TEST_F(ExprEvalTest, FunctionCallCoalesce) {
    AstNode* a1 = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* a2 = leaf(NodeType::NODE_LITERAL_INT, "42");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "COALESCE", {a1, a2});
    auto v = eval_mysql(fn);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, FunctionCallUnknown) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "NOEXIST", {arg});
    EXPECT_TRUE(eval_mysql(fn).is_null());
}

TEST_F(ExprEvalTest, FunctionCallConcat) {
    AstNode* a1 = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* a2 = leaf(NodeType::NODE_LITERAL_STRING, " ");
    AstNode* a3 = leaf(NodeType::NODE_LITERAL_STRING, "world");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "CONCAT", {a1, a2, a3});
    auto v = eval_mysql(fn);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello world");
}

TEST_F(ExprEvalTest, FunctionCallLength) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "LENGTH", {arg});
    auto v = eval_mysql(fn);
    EXPECT_EQ(v.int_val, 5);
}

// ===== Deferred Nodes =====
// TEMP FIELD ACCESS ANCHOR
// field access tests
// inserted test block below

TEST_F(ExprEvalTest, SubqueryReturnsNull) {
    AstNode* n = make_node(arena, NodeType::NODE_SUBQUERY);
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, TupleReturnsTupleValue) {
    // Was TupleReturnsNull until compound value support landed. Now NODE_TUPLE
    // evaluates each child and yields a TAG_TUPLE whose elements survive in the
    // arena, so downstream operators can inspect them.
    AstNode* n = make_node(arena, NodeType::NODE_TUPLE);
    n->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    n->add_child(leaf(NodeType::NODE_LITERAL_STRING, "hello"));

    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_TUPLE);
    ASSERT_NE(v.compound_val, nullptr);
    EXPECT_EQ(v.compound_val->count, 2u);
    EXPECT_EQ(v.compound_val->elements[0].int_val, 10);
    EXPECT_EQ(std::string(v.compound_val->elements[1].str_val.ptr,
                          v.compound_val->elements[1].str_val.len), "hello");
}

TEST_F(ExprEvalTest, ArrayConstructorReturnsArrayValue) {
    // Was ArrayConstructorReturnsNull. NODE_ARRAY_CONSTRUCTOR now produces a
    // TAG_ARRAY; the previous "subscript at parse time" special path is no
    // longer necessary because the runtime array carries its own elements.
    AstNode* n = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    n->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    n->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_ARRAY);
    ASSERT_NE(v.compound_val, nullptr);
    EXPECT_EQ(v.compound_val->count, 2u);
    EXPECT_EQ(v.compound_val->elements[1].int_val, 20);
}

// ===== Array Subscript Evaluation =====

TEST_F(ExprEvalTest, ArraySubscriptPg1Based) {
    // ARRAY[10, 20, 30][2] with PostgreSQL -> 20 (1-based)
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "30"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "2"));

    auto v = eval_pg(subscript);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 20);
}

TEST_F(ExprEvalTest, ArraySubscriptPgFirstElement) {
    // ARRAY[10, 20, 30][1] with PostgreSQL -> 10
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "30"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "1"));

    auto v = eval_pg(subscript);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 10);
}

TEST_F(ExprEvalTest, ArraySubscriptOutOfBounds) {
    // ARRAY[10, 20][5] -> null
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "5"));

    EXPECT_TRUE(eval_pg(subscript).is_null());
}

TEST_F(ExprEvalTest, ArraySubscriptPgZeroOutOfBounds) {
    // ARRAY[10, 20][0] with PostgreSQL -> null (1-based, so 0 is out of bounds)
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "0"));

    EXPECT_TRUE(eval_pg(subscript).is_null());
}

TEST_F(ExprEvalTest, ArraySubscriptPgStringElements) {
    // ARRAY['hello', 'world'][1] with PostgreSQL -> 'hello'
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_STRING, "hello"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_STRING, "world"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "1"));

    auto v = eval_pg(subscript);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello");
}

TEST_F(ExprEvalTest, ArraySubscriptNullIndex) {
    // ARRAY[10, 20][NULL] -> null
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(make_node(arena, NodeType::NODE_LITERAL_NULL));

    EXPECT_TRUE(eval_pg(subscript).is_null());
}

TEST_F(ExprEvalTest, ArraySubscriptNoChildren) {
    // Malformed: no children -> null
    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    EXPECT_TRUE(eval_pg(subscript).is_null());
}

TEST_F(ExprEvalTest, ArraySubscriptMysql0Based) {
    // MySQL uses 0-based indexing: ARRAY[10, 20, 30][0] -> 10
    AstNode* arr = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));
    arr->add_child(leaf(NodeType::NODE_LITERAL_INT, "30"));

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(arr);
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "0"));

    auto v = eval_mysql(subscript);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 10);
}

TEST_F(ExprEvalTest, ArraySubscriptFromResolvedRuntimeArray) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("arr", 3)) {
            Value elems[3] = {value_int(10), value_int(20), value_int(30)};
            return value_array(arena, elems, 3);
        }
        return value_null();
    };

    AstNode* subscript = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    subscript->add_child(leaf(NodeType::NODE_COLUMN_REF, "arr"));
    subscript->add_child(leaf(NodeType::NODE_LITERAL_INT, "1"));

    auto v = eval_pg(subscript);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 10);
}

TEST_F(ExprEvalTest, NestedArraySubscriptUsesRuntimeValues) {
    AstNode* inner1 = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    inner1->add_child(leaf(NodeType::NODE_LITERAL_INT, "10"));
    inner1->add_child(leaf(NodeType::NODE_LITERAL_INT, "20"));

    AstNode* inner2 = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    inner2->add_child(leaf(NodeType::NODE_LITERAL_INT, "30"));
    inner2->add_child(leaf(NodeType::NODE_LITERAL_INT, "40"));

    AstNode* outer = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    outer->add_child(inner1);
    outer->add_child(inner2);

    AstNode* first = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    first->add_child(outer);
    first->add_child(leaf(NodeType::NODE_LITERAL_INT, "2"));

    AstNode* second = make_node(arena, NodeType::NODE_ARRAY_SUBSCRIPT);
    second->add_child(first);
    second->add_child(leaf(NodeType::NODE_LITERAL_INT, "1"));

    auto v = eval_pg(second);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 30);
}

// ===== Field access on named tuples =====

TEST_F(ExprEvalTest, FieldAccessByNameReturnsField) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("rec", 3)) {
            Value elems[2] = {value_int(7), value_string(StringRef{"alice", 5})};
            StringRef names[2] = {StringRef{"id", 2}, StringRef{"name", 4}};
            return value_named_tuple(arena, elems, names, 2);
        }
        return value_null();
    };

    AstNode* fa = make_node(arena, NodeType::NODE_FIELD_ACCESS,
                             StringRef{"name", 4});
    fa->add_child(leaf(NodeType::NODE_COLUMN_REF, "rec"));

    auto v = eval_mysql(fa);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "alice");
}

TEST_F(ExprEvalTest, FieldAccessUnknownFieldReturnsNull) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("rec", 3)) {
            Value elems[1] = {value_int(7)};
            StringRef names[1] = {StringRef{"id", 2}};
            return value_named_tuple(arena, elems, names, 1);
        }
        return value_null();
    };

    AstNode* fa = make_node(arena, NodeType::NODE_FIELD_ACCESS,
                             StringRef{"missing", 7});
    fa->add_child(leaf(NodeType::NODE_COLUMN_REF, "rec"));

    EXPECT_TRUE(eval_mysql(fa).is_null());
}

TEST_F(ExprEvalTest, FieldAccessOnUnnamedTupleReturnsNull) {
    resolver = [this](StringRef name) -> Value {
        if (name.equals_ci("rec", 3)) {
            Value elems[1] = {value_int(7)};
            return value_tuple(arena, elems, 1);  // no field_names
        }
        return value_null();
    };

    AstNode* fa = make_node(arena, NodeType::NODE_FIELD_ACCESS,
                             StringRef{"id", 2});
    fa->add_child(leaf(NodeType::NODE_COLUMN_REF, "rec"));

    EXPECT_TRUE(eval_mysql(fa).is_null());
}
