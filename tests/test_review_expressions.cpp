#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/emitter.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/expression_eval.h"
#include <cstring>
#include <string>

using namespace sql_parser;

TEST(ReviewExpressions, NestedOrdinaryCallsFitBoundedArena) {
    for (const char* name : {"f", "schema.f", "\"custom\""}) {
        for (unsigned depth : {32u, 64u}) {
            SCOPED_TRACE(name);
            SCOPED_TRACE(depth);
            std::string sql = "SELECT ";
            for (unsigned i = 0; i < depth; ++i) sql += std::string(name) + "(";
            sql += "x";
            sql.append(depth, ')');
            ParserConfig config;
            config.arena_block_size = 4096;
            config.arena_max_size = 16384;
            Parser<Dialect::PostgreSQL> parser(config);
            auto result = parser.parse(sql.data(), sql.size());
            ASSERT_TRUE(result.ok());
            ASSERT_TRUE(result.full_input);
            EXPECT_LT(parser.arena().bytes_used(), 8192u);
        }
    }
}

TEST(ReviewExpressions, TypedLiteralLookaheadPreservesTypeSyntax) {
    for (const char* sql : {"SELECT numeric(10, 2) '1.2'", "SELECT double precision '1.2'",
         "SELECT national character varying(8) 'abc'", "SELECT bit varying(3) '101'",
         "SELECT timestamp(3) with time zone '2000-01-01'", "SELECT time without time zone '12:00'",
         "SELECT schema.custom(f(1), 2 + 3) 'value'", "SELECT \"Custom\"(f(1)) 'value'",
         "SELECT schema.\"Custom\"(ARRAY[1,2][1]) 'value'",
         "SELECT f(g('not a type'), h(1,2)), schema.f(x), \"custom\"(x)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        Parser<Dialect::PostgreSQL> again;
        auto output = emitter.result();
        auto roundtrip = again.parse(output.ptr, output.len);
        EXPECT_TRUE(roundtrip.ok());
        EXPECT_TRUE(roundtrip.full_input) << std::string(output.ptr, output.len);
    }
    for (const char* sql : {"SELECT custom(1 +) 'value'", "SELECT custom(,) 'value'",
         "SELECT custom(f(1) 'value'", "SELECT numeric(1,) 'value'",
         "SELECT timestamp(3) with zone 'value'", "SELECT national varying 'value'"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(ReviewExpressions, MysqlRowsAndRangeFramesRemainGuarded) {
    for (const char* frame : {"ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
         "ROWS UNBOUNDED PRECEDING", "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
         "RANGE INTERVAL 1 DAY PRECEDING", "RANGE BETWEEN INTERVAL '2:30' HOUR_MINUTE PRECEDING AND CURRENT ROW"}) {
        const std::string sql = std::string("SELECT sum(x) OVER (ORDER BY x ") + frame + ") FROM t";
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto output = emitter.result();
        Parser<Dialect::MySQL> again;
        auto roundtrip = again.parse(output.ptr, output.len);
        EXPECT_TRUE(roundtrip.ok());
        EXPECT_TRUE(roundtrip.full_input) << std::string(output.ptr, output.len);
    }
    for (const char* frame : {"GROUPS 1 PRECEDING", "ROWS CURRENT ROW EXCLUDE TIES",
         "ROWS BETWEEN CURRENT ROW AND 1 PRECEDING", "ROWS UNBOUNDED FOLLOWING"}) {
        const std::string sql = std::string("SELECT sum(x) OVER (ORDER BY x ") + frame + ") FROM t";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        EXPECT_FALSE(result.ok() && result.full_input) << sql;
    }
}

TEST(ReviewExpressions, PostgresConcatUsesSupportedBinaryPath) {
    const char* sql = "SELECT 'a' || 'b'";
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    EXPECT_TRUE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));

    Tokenizer<Dialect::PostgreSQL> tokenizer;
    tokenizer.reset(sql + 7, std::strlen(sql) - 7);
    ExpressionParser<Dialect::PostgreSQL> expression_parser(tokenizer, parser.arena());
    AstNode* expression = expression_parser.parse_complete();
    ASSERT_NE(expression, nullptr);
    sql_engine::FunctionRegistry<Dialect::PostgreSQL> functions;
    auto value = sql_engine::evaluate_expression<Dialect::PostgreSQL>(expression,
        [](StringRef) { return sql_engine::value_null(); }, functions, parser.arena());
    ASSERT_EQ(value.tag, sql_engine::Value::TAG_STRING);
    EXPECT_EQ(std::string(value.str_val.ptr, value.str_val.len), "ab");
}

TEST(ReviewExpressions, PostgresConcatKeepsGenericOperatorPrecedence) {
    Arena arena;
    Tokenizer<Dialect::PostgreSQL> tokenizer;
    const char* sql = "a || b + c = d";
    tokenizer.reset(sql, std::strlen(sql));
    ExpressionParser<Dialect::PostgreSQL> parser(tokenizer, arena);
    AstNode* expr = parser.parse_complete();
    ASSERT_NE(expr, nullptr);
    EXPECT_TRUE(expr->value().equals_ci("=", 1));
    AstNode* concat = expr->first_child;
    ASSERT_NE(concat, nullptr);
    EXPECT_TRUE(concat->value().equals_ci("||", 2));
    ASSERT_NE(concat->first_child, nullptr);
    AstNode* addition = concat->first_child->next_sibling;
    ASSERT_NE(addition, nullptr);
    EXPECT_TRUE(addition->value().equals_ci("+", 1));
    EXPECT_EQ(tokenizer.peek().type, TokenType::TK_EOF);
}

TEST(ReviewExpressions, PostgresUserOperatorsRemainRecognizedAndGuarded) {
    for (const char* sql : {"SELECT a ||| b", "SELECT a ||@ b", "SELECT || a",
         "SELECT a OPERATOR(pg_catalog.||) b", "SELECT a || ANY(ARRAY['x'])"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
    }
}
