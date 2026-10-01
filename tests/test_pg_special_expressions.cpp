#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
using namespace sql_parser;

TEST(PgSpecialExpressions, RejectsBareWildcardsAsSpecialOperands) {
    for (const char* sql : {"SELECT EXTRACT(year FROM *)", "SELECT SUBSTRING(* FROM 1)",
         "SELECT SUBSTRING(s FROM *)", "SELECT SUBSTRING(s FROM 1 FOR *)", "SELECT TRIM(*)",
         "SELECT TRIM(* FROM s)", "SELECT TRIM('x' FROM *)", "SELECT x AT TIME ZONE *",
         "SELECT * AT TIME ZONE x", "SELECT 1 OPERATOR(pg_catalog.+) *", "SELECT * OPERATOR(+) 1",
         "SELECT a IS DISTINCT FROM *", "SELECT * IS DISTINCT FROM a", "SELECT NORMALIZE(*)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
}

TEST(PgSpecialExpressions, QuantifiedOperandsAndNormalizationRetainSpecialSyntax) {
    for (const char* sql : {"SELECT x = ANY(ARRAY[1,2]), y <> ALL(a), z > SOME(SELECT v FROM t)",
         "SELECT x OPERATOR(pg_catalog.=) ANY(a), x NOT LIKE ANY(patterns)",
         "SELECT normalize('text'), normalize(s, NFC), normalize(s, NFKD)",
         "SELECT CURRENT_TIMESTAMP(3), CURRENT_TIME(2), LOCALTIMESTAMP(4), LOCALTIME(1)",
         "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE RETURNING merge_action()"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        ASSERT_TRUE(result.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto output = emitter.result();
        Parser<Dialect::PostgreSQL> again;
        auto roundtrip = again.parse(output.ptr, output.len);
        EXPECT_EQ(roundtrip.status, ParseResult::OK);
        EXPECT_TRUE(roundtrip.full_input);
    }
    for (const char* sql : {"SELECT ANY(a)", "SELECT (ALL(a))", "SELECT SOME(a)",
         "SELECT x = ANY()", "SELECT x = ANY(a,b)", "SELECT x = ALL(DELETE FROM t)",
         "SELECT normalize()", "SELECT normalize(s, 'NFC')", "SELECT normalize(s, bogus)",
         "SELECT CURRENT_TIME()", "SELECT CURRENT_TIMESTAMP(1+2)", "SELECT merge_action(1)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
}

TEST(PgSpecialExpressions, AdditionalSpecialFormsAndGrammarBoundaries) {
    for (const char* sql : {
        "SELECT trim(trailing ' ' FROM substring(version() FROM '^[^0-9]*'))",
        "SELECT trim(BOTH 'x' FROM s), trim(LEADING FROM s), trim(TRAILING s)",
        "SELECT trim(FROM s), trim(s, 'x'), trim('x' FROM s, t)",
        "SELECT x IS DISTINCT FROM y, x IS NOT DISTINCT FROM y + 1",
        "SELECT ARRAY(SELECT x FROM t), ARRAY(VALUES (1), (2))",
        "SELECT ARRAY(WITH q AS (SELECT 1) SELECT * FROM q)",
        "INSERT INTO sql_implementation_info VALUES ('17','DBMS NAME',NULL,(select trim(trailing ' ' from substring(version() from '^[^0-9]*'))),NULL)"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        ASSERT_TRUE(result.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto output = emitter.result();
        Parser<Dialect::PostgreSQL> again;
        auto roundtrip = again.parse(output.ptr, output.len);
        EXPECT_EQ(roundtrip.status, ParseResult::OK);
        EXPECT_TRUE(roundtrip.full_input) << std::string(output.ptr, output.len);
        if (std::strncmp(sql, "SELECT", 6) == 0) {
            EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
        }
    }
    for (const char* sql : {"SELECT CHECK(x)", "CREATE TABLE t (id int DEFAULT CHECK(id > 0))",
         "SELECT ()", "SELECT trim()", "SELECT trim(FROM)", "SELECT trim(BOTH 'x' FROM)",
         "SELECT x IS DISTINCT y", "SELECT x IS NOT DISTINCT FROM", "SELECT ARRAY()",
         "SELECT ARRAY(1)", "SELECT ARRAY(DELETE FROM t RETURNING x)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
}

TEST(PgSpecialExpressions, RejectsIncompleteCaseAndArraySyntax) {
    for (const char* sql : {"SELECT JSON_OBJECT(ARRAY['a','b')", "SELECT ARRAY[1,]",
         "SELECT ARRAY[1 +]", "SELECT CASE WHEN x THEN y", "SELECT CASE END",
         "SELECT CASE x END", "SELECT CASE WHEN x y END", "SELECT CASE WHEN x THEN END",
         "SELECT CASE WHEN x THEN y ELSE END"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
    for (const char* sql : {"SELECT CASE x WHEN 1 THEN 2 ELSE 3 END",
         "SELECT CASE WHEN x THEN ARRAY[1,2] END", "SELECT ARRAY[]"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        EXPECT_TRUE(result.full_input);
    }
}

TEST(PgSpecialExpressions, ParsesAndRoundTripsSpecialSyntax) {
    for (const char* sql : {
        "SELECT EXTRACT(year FROM stamp), EXTRACT('epoch' FROM stamp + INTERVAL '1 day')",
        "SELECT SUBSTRING(s FROM 2 FOR n + 1), SUBSTRING(s FOR n FROM 2)",
        "SELECT SUBSTRING(s FROM pattern), SUBSTRING(s FOR n)",
        "SELECT SUBSTRING(s SIMILAR pattern ESCAPE '#'), substring(s, 2, 3)",
        "SELECT stamp AT TIME ZONE zone AT LOCAL, stamp + delta AT TIME ZONE 'UTC'",
        "SELECT INTERVAL '1 day', INTERVAL '2' DAY TO SECOND(3), INTERVAL(2) '1.5'",
        "SELECT INTERVAL '1' YEAR TO MONTH + INTERVAL '2' MONTH",
        "SELECT 1 OPERATOR(pg_catalog.+) 2 * 3, OPERATOR(pg_catalog.-) 4 + 5",
        "SELECT 1 OPERATOR(\"Schema\".===) 2, 1 OPERATOR(+) 2"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        ASSERT_TRUE(result.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto output = emitter.result();
        Parser<Dialect::PostgreSQL> reparsed;
        auto roundtrip = reparsed.parse(output.ptr, output.len);
        EXPECT_EQ(roundtrip.status, ParseResult::OK);
        EXPECT_TRUE(roundtrip.full_input) << std::string(output.ptr, output.len);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
    }
}

TEST(PgSpecialExpressions, RejectsMalformedSpecialSyntax) {
    for (const char* sql : {
        "SELECT EXTRACT(year stamp)", "SELECT EXTRACT(year FROM)", "SELECT EXTRACT(1 FROM stamp)",
        "SELECT EXTRACT(year, stamp)", "SELECT SUBSTRING(s FROM)", "SELECT SUBSTRING(s FOR)",
        "SELECT SUBSTRING(s SIMILAR pattern)", "SELECT SUBSTRING(s FROM 1 FROM 2)",
        "SELECT stamp AT TIME zone", "SELECT stamp AT TIME ZONE", "SELECT stamp AT TIME",
        "SELECT INTERVAL 1", "SELECT INTERVAL 1.5", "SELECT INTERVAL '1' DAY TO YEAR", "SELECT INTERVAL(2) '1' DAY",
        "SELECT INTERVAL '1' SECOND(1,2)", "SELECT INTERVAL '1' DAY TO",
        "SELECT 1 OPERATOR(pg_catalog.foo) 2", "SELECT 1 OPERATOR(pg_catalog.+)",
        "SELECT OPERATOR(pg_catalog.+)", "SELECT 1 OPERATOR(pg_catalog.) 2"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
}

TEST(PgSpecialExpressions, RejectsNonQueryExistsOperands) {
    for (const char* sql : {"SELECT EXISTS(DELETE FROM t RETURNING x)",
         "SELECT EXISTS(INSERT INTO t VALUES (1))", "SELECT EXISTS(1)", "SELECT EXISTS"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
}

static std::string special_value(const AstNode* node) {
    return std::string(node->value_ptr, node->value_len);
}

static const AstNode* special_expression(const AstNode* node) {
    if (!node) return nullptr;
    if (node->type == NodeType::NODE_SELECT_ITEM) return node->first_child;
    for (auto* child = node->first_child; child; child = child->next_sibling)
        if (auto* found = special_expression(child)) return found;
    return nullptr;
}

TEST(PgSpecialExpressions, TimeZoneAndQualifiedOperatorsUsePostgresPrecedence) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT 2 ^ stamp AT TIME ZONE zone COLLATE c";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.full_input);
    auto* exponent = special_expression(result.ast);
    ASSERT_NE(exponent, nullptr);
    ASSERT_EQ(exponent->type, NodeType::NODE_BINARY_OP);
    EXPECT_EQ(special_value(exponent), "^");
    auto* at = exponent->first_child->next_sibling;
    ASSERT_EQ(at->type, NodeType::NODE_PG_TIME_ZONE);
    EXPECT_EQ(special_value(at->first_child), "stamp");
    EXPECT_EQ(special_value(at->first_child->next_sibling), "COLLATE");

    sql = "SELECT 1 OPERATOR(pg_catalog.+) 2 + 3";
    result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.full_input);
    auto* op = special_expression(result.ast);
    ASSERT_NE(op, nullptr);
    EXPECT_EQ(special_value(op), "OPERATOR(pg_catalog.+)");
    EXPECT_EQ(special_value(op->first_child->next_sibling), "+");
}

TEST(PgSpecialExpressions, ExposesOperandsAndKeepsSyntaxOutOfParameterization) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT SUBSTRING(s FROM 2 FOR n + 1)";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.full_input);
    auto* expression = special_expression(result.ast);
    ASSERT_NE(expression, nullptr);
    ASSERT_EQ(expression->type, NodeType::NODE_PG_SUBSTRING);
    EXPECT_EQ(special_value(expression->first_child), "s");
    EXPECT_EQ(special_value(expression->first_child->next_sibling), "2");
    EXPECT_EQ(special_value(expression->first_child->next_sibling->next_sibling), "+");
    Arena output;
    EXPECT_FALSE(parameterize_ast<Dialect::PostgreSQL>(result, output).ok());

    sql = "SELECT INTERVAL '2' DAY TO SECOND(3), 4";
    result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.full_input);
    expression = special_expression(result.ast);
    ASSERT_EQ(expression->type, NodeType::NODE_PG_INTERVAL);
    ASSERT_EQ(expression->first_child->type, NodeType::NODE_LITERAL_STRING);
    EXPECT_EQ(special_value(expression->first_child), "2");
    EXPECT_EQ(special_value(expression->first_child->next_sibling), "DAY TO SECOND(3)");
}

TEST(PgSpecialExpressions, KeepsKeywordAliasesAndQuotedFunctionNames) {
    for (const char* sql : {"SELECT stamp AT", "SELECT value OPERATOR", "SELECT operator FROM t",
         "SELECT interval FROM t", "SELECT \"extract\"(1, 2)", "SELECT schema.extract(1, 2)",
         "SELECT \"substring\"(s, 1, 2)", "SELECT substring(s, 1, 2)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        EXPECT_TRUE(result.full_input);
    }
}

TEST(PgSpecialExpressions, ValidatesExpressionTypeModifiersWithoutBindingTheirConstants) {
    for (const char* sql : {"SELECT x::custom_type(1 + 2, 'flag')", "SELECT x::numeric(2 * 3, 1)",
         "SELECT CAST(x AS schema.typ(lower('X'), (4 + 2)))", "SELECT custom_type(1 + 2) 'x'",
         "SELECT x::bit(1 + 2)", "SELECT numeric(2 + 3, 1) '4.2'"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        ASSERT_TRUE(result.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto emitted = emitter.result();
        Parser<Dialect::PostgreSQL> again;
        EXPECT_TRUE(again.parse(emitted.ptr, emitted.len).full_input);
    }
    for (const char* sql : {"SELECT x::custom_type(1 +)", "SELECT x::custom_type(,2)",
         "SELECT x::custom_type(1,)", "SELECT x::timestamp(1 + 2)", "SELECT x::varchar(1 + 2)",
         "SELECT x::custom_type(a b)", "SELECT x::custom_type(*)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT x::custom_type(1 + 2, 'flag'), 7";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.full_input);
    Arena output;
    auto parameterized = parameterize_ast<Dialect::PostgreSQL>(result, output);
    ASSERT_TRUE(parameterized.ok());
    EXPECT_EQ(parameterized.parameters.size(), 1u);
}
