#include <gtest/gtest.h>
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/parser.h"
#include "sql_parser/common.h"
#include <string>

using namespace sql_engine;
using namespace sql_parser;

// Integration test fixture: parse SQL, navigate to expression, evaluate.
class EvalIntegrationTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> mysql_parser;
    FunctionRegistry<Dialect::MySQL> mysql_funcs;

    Parser<Dialect::PostgreSQL> pg_parser;
    FunctionRegistry<Dialect::PostgreSQL> pg_funcs;

    // No columns needed for these tests -- resolver always returns NULL.
    std::function<Value(StringRef)> no_columns = [](StringRef) -> Value {
        return value_null();
    };

    void SetUp() override {
        mysql_funcs.register_builtins();
        pg_funcs.register_builtins();
    }

    // Parse a SELECT, navigate to the first select item's expression, evaluate.
    // SELECT <expr>  ->  AST: SELECT_STMT -> SELECT_ITEM_LIST -> SELECT_ITEM -> expr
    //
    // Note: We copy string results before returning since arena memory is reused.
    // For tests, we just avoid calling reset() until TearDown.
    Value eval_select_mysql(const char* sql) {
        auto r = mysql_parser.parse(sql, std::strlen(sql));
        EXPECT_EQ(r.status, ParseResult::OK) << "Failed to parse: " << sql;
        if (!r.ast) return value_null();

        // Navigate: SELECT_STMT -> SELECT_ITEM_LIST -> first SELECT_ITEM -> first child (expression)
        const AstNode* item_list = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
        if (!item_list || !item_list->first_child) return value_null();
        const AstNode* first_item = item_list->first_child;

        // The select item's first child is the expression
        const AstNode* expr = first_item->first_child;
        if (!expr) return value_null();

        return evaluate_expression<Dialect::MySQL>(
            expr, no_columns, mysql_funcs, mysql_parser.arena());
    }

    Value eval_select_pg(const char* sql) {
        auto r = pg_parser.parse(sql, std::strlen(sql));
        EXPECT_EQ(r.status, ParseResult::OK) << "Failed to parse: " << sql;
        if (!r.ast) return value_null();

        const AstNode* item_list = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
        if (!item_list || !item_list->first_child) return value_null();
        const AstNode* first_item = item_list->first_child;
        const AstNode* expr = first_item->first_child;
        if (!expr) return value_null();

        return evaluate_expression<Dialect::PostgreSQL>(
            expr, no_columns, pg_funcs, pg_parser.arena());
    }

    // Helper to find a child node by type
    static const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }

    // Helper to safely extract string from Value (copies before arena could be reused)
    static std::string str(const Value& v) {
        if (v.tag != Value::TAG_STRING) return "";
        return std::string(v.str_val.ptr, v.str_val.len);
    }
};

// ===== Literal Evaluation =====

TEST_F(EvalIntegrationTest, SelectInteger) {
    auto v = eval_select_mysql("SELECT 42");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectFloat) {
    auto v = eval_select_mysql("SELECT 3.14");
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.14);
}

TEST_F(EvalIntegrationTest, SelectString) {
    auto v = eval_select_mysql("SELECT 'hello'");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "hello");
}

TEST_F(EvalIntegrationTest, SelectNull) {
    auto v = eval_select_mysql("SELECT NULL");
    EXPECT_TRUE(v.is_null());
}

TEST_F(EvalIntegrationTest, SelectTrue) {
    auto v = eval_select_mysql("SELECT TRUE");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectFalse) {
    auto v = eval_select_mysql("SELECT FALSE");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

// ===== Arithmetic =====

TEST_F(EvalIntegrationTest, SelectOnePlusTwo) {
    auto v = eval_select_mysql("SELECT 1 + 2");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(EvalIntegrationTest, SelectIntegerDivision) {
    auto v = eval_select_mysql("SELECT 10 / 3");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(EvalIntegrationTest, SelectFloatDivision) {
    auto v = eval_select_mysql("SELECT 10.0 / 3");
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_NEAR(v.double_val, 3.333333, 0.001);
}

TEST_F(EvalIntegrationTest, SelectArithmeticPrecedence) {
    auto v = eval_select_mysql("SELECT 2 + 3 * 4");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 14);
}

TEST_F(EvalIntegrationTest, SelectDivisionByZero) {
    auto v = eval_select_mysql("SELECT 1 / 0");
    EXPECT_TRUE(v.is_null());
}

TEST_F(EvalIntegrationTest, SelectModulo) {
    auto v = eval_select_mysql("SELECT 10 % 3");
    EXPECT_EQ(v.int_val, 1);
}

// ===== Comparison =====

TEST_F(EvalIntegrationTest, SelectComparison) {
    auto v = eval_select_mysql("SELECT 1 < 2");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectEqualityTrue) {
    auto v = eval_select_mysql("SELECT 42 = 42");
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectEqualityFalse) {
    auto v = eval_select_mysql("SELECT 1 = 2");
    EXPECT_FALSE(v.bool_val);
}

// ===== Function Calls =====

TEST_F(EvalIntegrationTest, SelectUpperFunction) {
    auto v = eval_select_mysql("SELECT UPPER('hello')");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "HELLO");
}

TEST_F(EvalIntegrationTest, SelectCoalesceFunction) {
    auto v = eval_select_mysql("SELECT COALESCE(NULL, 42)");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectCoalesceMultipleNulls) {
    auto v = eval_select_mysql("SELECT COALESCE(NULL, NULL, 42)");
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectAbsFunction) {
    auto v = eval_select_mysql("SELECT ABS(-42)");
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectConcatFunction) {
    auto v = eval_select_mysql("SELECT CONCAT('hello', ' ', 'world')");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "hello world");
}

TEST_F(EvalIntegrationTest, SelectLengthFunction) {
    auto v = eval_select_mysql("SELECT LENGTH('hello')");
    EXPECT_EQ(v.int_val, 5);
}

// ===== CASE/WHEN =====

TEST_F(EvalIntegrationTest, SelectSearchedCase) {
    auto v = eval_select_mysql("SELECT CASE WHEN 1 > 2 THEN 'a' ELSE 'b' END");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "b");
}

TEST_F(EvalIntegrationTest, SelectSimpleCase) {
    auto v = eval_select_mysql("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "one");
}

TEST_F(EvalIntegrationTest, SelectSimpleCaseSecondBranch) {
    auto v = eval_select_mysql("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "two");
}

TEST_F(EvalIntegrationTest, SelectSimpleCaseElse) {
    auto v = eval_select_mysql("SELECT CASE 99 WHEN 1 THEN 'one' ELSE 'other' END");
    EXPECT_EQ(str(v), "other");
}

// ===== IN =====

TEST_F(EvalIntegrationTest, SelectInList) {
    auto v = eval_select_mysql("SELECT 1 IN (1, 2, 3)");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectNotInList) {
    auto v = eval_select_mysql("SELECT 5 IN (1, 2, 3)");
    EXPECT_FALSE(v.bool_val);
}

// ===== BETWEEN =====

TEST_F(EvalIntegrationTest, SelectBetween) {
    auto v = eval_select_mysql("SELECT 5 BETWEEN 1 AND 10");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectNotBetween) {
    auto v = eval_select_mysql("SELECT 15 BETWEEN 1 AND 10");
    EXPECT_FALSE(v.bool_val);
}

// ===== IS NULL =====

TEST_F(EvalIntegrationTest, SelectIsNull) {
    auto v = eval_select_mysql("SELECT NULL IS NULL");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectIsNotNull) {
    auto v = eval_select_mysql("SELECT 1 IS NOT NULL");
    EXPECT_TRUE(v.bool_val);
}

// ===== LIKE =====

TEST_F(EvalIntegrationTest, SelectLike) {
    auto v = eval_select_mysql("SELECT 'test' LIKE 't%'");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectLikeNoMatch) {
    auto v = eval_select_mysql("SELECT 'test' LIKE 'x%'");
    EXPECT_FALSE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectLikeUnderscore) {
    auto v = eval_select_mysql("SELECT 'test' LIKE 'tes_'");
    EXPECT_TRUE(v.bool_val);
}

// ===== Logical Operators =====

TEST_F(EvalIntegrationTest, SelectAndOr) {
    auto v = eval_select_mysql("SELECT TRUE AND FALSE");
    EXPECT_FALSE(v.bool_val);

    v = eval_select_mysql("SELECT TRUE OR FALSE");
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectNotExpression) {
    auto v = eval_select_mysql("SELECT NOT TRUE");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

// ===== Unary Minus =====

TEST_F(EvalIntegrationTest, SelectUnaryMinus) {
    auto v = eval_select_mysql("SELECT -42");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, -42);
}

// ===== PostgreSQL-specific =====

TEST_F(EvalIntegrationTest, PgSelectConcat) {
    auto v = eval_select_pg("SELECT 'hello' || ' world'");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(str(v), "hello world");
}

TEST_F(EvalIntegrationTest, PgSelectCaseSensitiveLike) {
    auto v = eval_select_pg("SELECT 'Hello' LIKE 'hello'");
    EXPECT_FALSE(v.bool_val);  // PostgreSQL is case-sensitive
}

// ===== Complex Expressions =====

TEST_F(EvalIntegrationTest, NestedArithmetic) {
    // Test operator precedence without leading paren (parser limitation)
    auto v = eval_select_mysql("SELECT 2 + 3 * 7");
    EXPECT_EQ(v.int_val, 23);
}

TEST_F(EvalIntegrationTest, NestedFunctionCalls) {
    auto v = eval_select_mysql("SELECT ABS(-1) + ABS(-2)");
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(EvalIntegrationTest, NullArithmeticPropagation) {
    auto v = eval_select_mysql("SELECT NULL + 1");
    EXPECT_TRUE(v.is_null());
}
