#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>
using namespace sql_parser;

static std::string emit_pg(Parser<Dialect::PostgreSQL>& parser, const AstNode* ast) {
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(ast);
    auto sql = emitter.result();
    return std::string(sql.ptr, sql.len);
}

TEST(PgExpressions, CastsPreserveCompleteTypesAndExpressions) {
    const std::pair<const char*, const char*> cases[] = {
        {"SELECT 1::int", "SELECT CAST(1 AS int)"},
        {"SELECT -1::int + 2", "SELECT -CAST(1 AS int) + 2"},
        {"SELECT 1::int::text", "SELECT CAST(CAST(1 AS int) AS text)"},
        {"SELECT CAST(1 + 2 AS numeric(12,2))", "SELECT CAST(1 + 2 AS numeric(12,2))"},
        {"SELECT x::pg_catalog.int4[]", "SELECT CAST(x AS pg_catalog.int4[])"},
        {"SELECT x::\"Types\".\"Some Type\"[2][]", "SELECT CAST(x AS \"Types\".\"Some Type\"[2][])"},
        {"SELECT x::double precision", "SELECT CAST(x AS double precision)"},
        {"SELECT x::character varying(20)", "SELECT CAST(x AS character varying(20))"},
        {"SELECT x::timestamp(3) with time zone", "SELECT CAST(x AS timestamp(3) with time zone)"},
        {"SELECT x::time without time zone", "SELECT CAST(x AS time without time zone)"},
        {"SELECT x::interval day to second(3)", "SELECT CAST(x AS interval day to second(3))"},
        {"SELECT x::integer ARRAY[3]", "SELECT CAST(x AS integer ARRAY[3])"},
        {"SELECT '42'::int", "SELECT CAST('42' AS int)"},
        {"SELECT date '2026-09-22'", "SELECT CAST('2026-09-22' AS date)"},
        {"SELECT timestamp(3) with time zone '2026-09-22'", "SELECT CAST('2026-09-22' AS timestamp(3) with time zone)"},
        {"SELECT numeric(12,2) '1.25'", "SELECT CAST('1.25' AS numeric(12,2))"},
    };
    for (auto c : cases) {
        SCOPED_TRACE(c.first);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(c.first, std::strlen(c.first));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        EXPECT_EQ(emit_pg(parser, r.ast), c.second);
    }
}

TEST(PgExpressions, RejectsMalformedCasts) {
    for (const char* sql : {"SELECT 1::", "SELECT 1::, 2", "SELECT 1::int[", "SELECT 1::int[x]",
         "SELECT 1::numeric(12,)", "SELECT 1::timestamp with zone", "SELECT CAST(1 AS)",
         "SELECT CAST(1 integer)", "SELECT CAST(1 + AS int)", "SELECT CAST(1 AS int",
         "SELECT 1::\"s\".", "SELECT 1::double", "SELECT 1::interval day to year", "SELECT 1::int(2)", "SELECT 1::timestamp(1,2)",
         "SELECT 1::interval(3) day", "SELECT 1::double precision(3)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgExpressions, ParameterizesCastValuesButNotTypeModifiers) {
    const char* sql = "SELECT $3::numeric(12,2), CAST(7 AS integer), date '2026-09-22'";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    Arena output;
    auto p = parameterize_ast<Dialect::PostgreSQL>(r, output);
    ASSERT_TRUE(p.ok());
    ASSERT_EQ(p.parameters.size(), 2u);
    EXPECT_EQ(p.parameters[0].index, 4u);
    EXPECT_EQ(p.parameters[1].index, 5u);
    EXPECT_EQ(emit_pg(parser, p.ast), "SELECT CAST($3 AS numeric(12,2)), CAST($4 AS integer), CAST($5 AS date)");
}

TEST(PgExpressions, CastsAreRejectedByLocalPlanner) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT CAST(1 AS text)";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    sql_engine::InMemoryCatalog catalog;
    Arena arena;
    sql_engine::PlanBuilder<Dialect::PostgreSQL> builder(catalog, arena);
    EXPECT_EQ(builder.build(r.ast), nullptr);
}

TEST(PgExpressions, PreservesTypedLiteralLengthSemantics) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT char 'abcd', bit '1010', \"char\" 'x'";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    EXPECT_EQ(emit_pg(parser, r.ast), "SELECT CAST('abcd' AS pg_catalog.bpchar), CAST('1010' AS pg_catalog.bit), CAST('x' AS \"char\")");
}

TEST(PgExpressions, TokenizesPostgresOperatorBoundaries) {
    const char* sql = "a->>b @> c ? 'x' <-> d =-1 +/* comment */2 ?-3 @-- comment\n4";
    const char* expected[] = {"a", "->>", "b", "@>", "c", "?", "x", "<->", "d", "=", "-", "1", "+", "2", "?-", "3", "@", "4"};
    Tokenizer<Dialect::PostgreSQL> tok;
    tok.reset(sql, std::strlen(sql));
    for (const char* want : expected) {
        auto t = tok.next_token();
        EXPECT_EQ(std::string(t.text.ptr, t.text.len), want);
    }
    EXPECT_EQ(tok.next_token().type, TokenType::TK_EOF);
    Tokenizer<Dialect::MySQL> mysql;
    mysql.reset("?", 1);
    EXPECT_EQ(mysql.next_token().type, TokenType::TK_QUESTION);
}

TEST(PgExpressions, ParsesPostgresOperatorsAndCollation) {
    const char* queries[] = {
        "SELECT data -> 'items' ->> 0 FROM t",
        "SELECT data @> other AND data ? 'name' FROM t",
        "SELECT tags && other, path @? '$.a', text ~* 'a', text !~ 'b' FROM t",
        "SELECT p <-> q FROM t ORDER BY p <-> q",
        "SELECT @ -1, |/ 4, 2 ^ 3 ^ 2",
        "SELECT x << 2, x & y, x || y FROM t",
        "SELECT 'x'::text COLLATE \"C\"",
        "SELECT x COLLATE pg_catalog.\"C\" FROM t",
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        auto emitted = emit_pg(parser, r.ast);
        Parser<Dialect::PostgreSQL> second;
        auto roundtrip = second.parse(emitted.data(), emitted.size());
        ASSERT_TRUE(roundtrip.full_input);
        EXPECT_EQ(emit_pg(second, roundtrip.ast), emitted);
    }
}

static const AstNode* pg_select_expression(const AstNode* root) {
    for (const AstNode* c = root->first_child; c; c = c->next_sibling)
        if (c->type == NodeType::NODE_SELECT_ITEM_LIST)
            return c->first_child ? c->first_child->first_child : nullptr;
    return nullptr;
}

TEST(PgExpressions, UsesPostgresPrecedenceAndAssociativity) {
    const std::pair<const char*, const char*> cases[] = {
        {"SELECT 1 + 2 || 3", "||"},
        {"SELECT 1 << 2 + 3", "<<"},
        {"SELECT 2 * 3 ^ 4", "*"},
        {"SELECT a = b IS TRUE", "IS"},
        {"SELECT a = b IN (1)", "="},
        {"SELECT @ 1 + 2", "@"},
        {"SELECT a ~ b = c", "="},
    };
    for (auto c : cases) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(c.first, std::strlen(c.first));
        ASSERT_TRUE(r.full_input) << c.first;
        auto expr = pg_select_expression(r.ast);
        ASSERT_NE(expr, nullptr);
        EXPECT_EQ(std::string(expr->value().ptr, expr->value().len), c.second) << c.first;
    }
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT 2 ^ 3 ^ 4";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    auto expr = pg_select_expression(r.ast);
    ASSERT_NE(expr, nullptr);
    ASSERT_NE(expr->first_child, nullptr);
    EXPECT_EQ(expr->first_child->type, NodeType::NODE_BINARY_OP); // (2 ^ 3) ^ 4
}

TEST(PgExpressions, RejectsIncompleteOperatorsAndUnsupportedExecution) {
    for (const char* sql : {"SELECT x @>", "SELECT x ->>", "SELECT @", "SELECT x ~* FROM t",
         "SELECT x COLLATE", "SELECT x COLLATE 1", "SELECT 1 => 2"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
    for (const char* sql : {"SELECT 1 <-> 2", "SELECT 2 ^ 3", "SELECT 'x' COLLATE \"C\""}) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(r.full_input);
        sql_engine::InMemoryCatalog catalog;
        Arena arena;
        sql_engine::PlanBuilder<Dialect::PostgreSQL> builder(catalog, arena);
        EXPECT_EQ(builder.build(r.ast), nullptr);
    }
}

TEST(PgExpressions, SeparatesAdjacentPrefixOperatorsDuringEmission) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT @ -1, - -2";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    EXPECT_EQ(emit_pg(parser, r.ast), "SELECT @ -1, - -2");
}

TEST(PgExpressions, CastAstExposesExpressionForRewriting) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT (1 + 2)::numeric(12,2)::text";
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    auto outer = const_cast<AstNode*>(pg_select_expression(r.ast));
    ASSERT_NE(outer, nullptr);
    ASSERT_EQ(outer->type, NodeType::NODE_TYPE_CAST);
    auto inner = outer->first_child;
    ASSERT_EQ(inner->type, NodeType::NODE_TYPE_CAST);
    ASSERT_EQ(inner->first_child->type, NodeType::NODE_EXPRESSION);
    auto sum = inner->first_child->first_child;
    ASSERT_EQ(sum->type, NodeType::NODE_BINARY_OP);
    sum->first_child->set_value(StringRef{"9", 1});
    EXPECT_EQ(emit_pg(parser, r.ast), "SELECT CAST(CAST((9 + 2) AS numeric(12,2)) AS text)");
}

TEST(PgExpressions, TypedCharacterLengthIgnoresSpacingAndComments) {
    for (const char* sql : {"SELECT NATIONAL  CHAR 'abcd'", "SELECT NATIONAL /*x*/ CHARACTER 'abcd'"}) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(r.full_input);
        EXPECT_EQ(emit_pg(parser, r.ast), "SELECT CAST('abcd' AS pg_catalog.bpchar)");
    }
}

TEST(PgExpressions, NamedArgumentsKeepNamesAndBindOnlyValues) {
    for (const char* sql : {"SELECT f(a => 1, \"B\" := 2)", "SELECT * FROM f(a => 1)", "CALL f(a => 1)"}) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(r.full_input) << sql;
        auto emitted = emit_pg(parser, r.ast);
        EXPECT_NE(emitted.find("a => 1"), std::string::npos);
    }
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT f(a => 1, \"B\" := 2)";
    auto r = parser.parse(sql, std::strlen(sql));
    Arena output;
    auto p = parameterize_ast<Dialect::PostgreSQL>(r, output);
    ASSERT_TRUE(p.ok());
    EXPECT_EQ(emit_pg(parser, p.ast), "SELECT f(a => $1, \"B\" => $2)");
    EXPECT_EQ(p.parameters.size(), 2u);
    for (const char* invalid : {"SELECT f(a =>)", "SELECT f(a :=)", "SELECT a => 1", "SELECT a := 1"}) {
        auto bad = parser.parse(invalid, std::strlen(invalid));
        EXPECT_FALSE(bad.status == ParseResult::OK && bad.full_input) << invalid;
    }
}

TEST(PgExpressions, CastsComposeWithCollationAndPredicates) {
    const std::pair<const char*, const char*> cases[] = {
        {"SELECT x COLLATE \"C\"::text", "SELECT CAST(x COLLATE \"C\" AS text)"},
        {"SELECT x IS NULL::int", "SELECT CAST(x IS NULL AS int)"},
        {"SELECT NCHAR VARYING 'abcd'", "SELECT CAST('abcd' AS NCHAR VARYING)"},
    };
    for (auto c : cases) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(c.first, std::strlen(c.first));
        ASSERT_TRUE(r.full_input) << c.first;
        EXPECT_EQ(emit_pg(parser, r.ast), c.second);
    }
}

TEST(PgExpressions, SupportsKeywordComponentsInQualifiedTypes) {
    for (const char* sql : {"SELECT 1::data", "SELECT 1::schema.int4", "SELECT 1::public.select"}) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_TRUE(r.full_input) << sql;
    }
}

TEST(PgExpressions, RejectsMalformedNamedArguments) {
    for (const char* sql : {"SELECT f(a => 1,)", "CALL f(a => 1,)", "SELECT * FROM f(a => 1,)",
         "SELECT f(on => 1)", "SELECT f(character => 1)", "SELECT f(returning => 1)",
         "SELECT f(check => 1)", "SELECT f(numeric => 1)"}) {
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input) << sql;
    }
    const char* valid = "SELECT f(\"on\" => 1, schema => 2, data => 3, left => 4)";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(valid, std::strlen(valid));
    ASSERT_TRUE(r.full_input);
    EXPECT_EQ(emit_pg(parser, r.ast), valid);
}

TEST(PgExpressions, QualifiedCallsAndAggregateModifiersRoundTrip) {
    const std::pair<const char*, const char*> cases[] = {
        {"SELECT pg_catalog.abs(-1)", "SELECT pg_catalog.abs(-1)"},
        {"SELECT \"Schema\".\"Func\"(x => 2)", "SELECT \"Schema\".\"Func\"(x => 2)"},
        {"SELECT count(DISTINCT x)", "SELECT count(DISTINCT x)"},
        {"SELECT sum(ALL x)", "SELECT sum(ALL x)"},
        {"SELECT array_agg(DISTINCT x ORDER BY x DESC NULLS FIRST)", "SELECT array_agg(DISTINCT x ORDER BY x DESC NULLS FIRST)"},
        {"SELECT string_agg(x, ',' ORDER BY x, y DESC NULLS LAST)", "SELECT string_agg(x, ',' ORDER BY x, y DESC NULLS LAST)"},
        {"SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x DESC)", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x DESC)"},
        {"SELECT rank() WITHIN GROUP (ORDER BY x) FILTER (WHERE x > 0)", "SELECT rank() WITHIN GROUP (ORDER BY x) FILTER (WHERE x > 0)"},
        {"SELECT pg_catalog.sum(ALL x) FILTER (WHERE x > 0) OVER ()", "SELECT pg_catalog.sum(ALL x) FILTER (WHERE x > 0) OVER ()"},
    };
    for (auto c : cases) {
        SCOPED_TRACE(c.first);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(c.first, std::strlen(c.first));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        EXPECT_EQ(emit_pg(parser, r.ast), c.second);
        Parser<Dialect::PostgreSQL> second;
        auto sql = emit_pg(parser, r.ast);
        auto rr = second.parse(sql.data(), sql.size());
        ASSERT_TRUE(rr.full_input);
        EXPECT_EQ(emit_pg(second, rr.ast), sql);
        sql_engine::InMemoryCatalog catalog;
        Arena arena;
        sql_engine::PlanBuilder<Dialect::PostgreSQL> builder(catalog, arena);
        EXPECT_EQ(builder.build(r.ast), nullptr);
    }
}

TEST(PgExpressions, RejectsMalformedAggregateAndCallSyntax) {
    for (const char* sql : {"SELECT f(1", "SELECT f(1,)", "SELECT count(DISTINCT)",
         "SELECT count(ALL)", "SELECT count(DISTINCT *)", "SELECT sum(ALL *)",
         "SELECT f(ORDER BY x)", "SELECT f(x ORDER x)", "SELECT f(x ORDER BY)",
         "SELECT f(x ORDER BY y NULLS)", "SELECT f(x ORDER BY y,)",
         "SELECT f(x) WITHIN GROUP ()", "SELECT f(x) WITHIN (ORDER BY y)",
         "SELECT f(DISTINCT x) WITHIN GROUP (ORDER BY y)",
         "SELECT f(x ORDER BY y) WITHIN GROUP (ORDER BY z)",
         "SELECT s.(1)", "SELECT s.1(2)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgExpressions, AggregateOrderingConstantsAreParametersNotOrdinals) {
    const char* sql = "SELECT array_agg(DISTINCT x ORDER BY 1), percentile_cont(0.5) WITHIN GROUP (ORDER BY 2) FROM t ORDER BY 1";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_TRUE(r.full_input);
    Arena output;
    auto p = parameterize_ast<Dialect::PostgreSQL>(r, output);
    ASSERT_TRUE(p.ok());
    ASSERT_EQ(p.parameters.size(), 3u);
    EXPECT_EQ(emit_pg(parser, p.ast), "SELECT array_agg(DISTINCT x ORDER BY $1), percentile_cont($2) WITHIN GROUP (ORDER BY $3) FROM t ORDER BY 1");
}

TEST(PgExpressions, CteColumnListsAndQueryBodiesRoundTrip) {
    const char* queries[] = {
        "WITH t(x, y) AS (VALUES (1, 2), (3, 4)) SELECT x FROM t",
        "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t",
        "WITH \"T\"(\"X\") AS MATERIALIZED (SELECT 1) TABLE \"T\"",
        "WITH t AS NOT MATERIALIZED (TABLE source) SELECT * FROM t",
        "WITH t(x) AS (WITH s(y) AS (VALUES (1)) SELECT y FROM s) SELECT x FROM t",
        "WITH t(x) AS (SELECT 1), u(y) AS (TABLE t) VALUES (2)",
        "SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(x, y)",
        "SELECT * FROM LATERAL (WITH t(x) AS (VALUES (1)) SELECT x FROM t) AS q",
        "SELECT (SELECT 1 UNION ALL SELECT 2)",
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        auto emitted = emit_pg(parser, r.ast);
        EXPECT_EQ(emitted, sql);
        Parser<Dialect::PostgreSQL> second;
        auto rr = second.parse(emitted.data(), emitted.size());
        ASSERT_EQ(rr.status, ParseResult::OK);
        ASSERT_TRUE(rr.full_input);
        EXPECT_EQ(emit_pg(second, rr.ast), emitted);
    }
}

TEST(PgExpressions, RejectsMalformedCtes) {
    for (const char* sql : {"WITH t() AS (SELECT 1) SELECT 1", "WITH t(x,) AS (SELECT 1) SELECT 1",
         "WITH t(x AS (SELECT 1) SELECT 1", "WITH t (SELECT 1) SELECT 1",
         "WITH t AS SELECT 1", "WITH t AS () SELECT 1", "WITH t AS (SELECT 1)",
         "WITH t AS MATERIALIZED SELECT 1", "WITH t AS NOT (SELECT 1) SELECT 1",
         "WITH t AS (SELECT 1 SELECT 2", "WITH t AS (SELECT 1), SELECT 2"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgExpressions, CteStructurePreservesBodyColumnsAndMaterialization) {
    const char* sql = "WITH t(x, \"Y\") AS NOT MATERIALIZED (VALUES (1, 2)) SELECT * FROM t";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_TRUE(r.full_input);
    ASSERT_EQ(r.ast->type, NodeType::NODE_CTE);
    auto* def = r.ast->first_child;
    ASSERT_NE(def, nullptr);
    EXPECT_EQ(def->type, NodeType::NODE_CTE_DEFINITION);
    ASSERT_NE(def->first_child, nullptr);
    EXPECT_EQ(def->first_child->type, NodeType::NODE_COMPOUND_QUERY);
    ASSERT_NE(def->first_child->next_sibling, nullptr);
    EXPECT_EQ(def->first_child->next_sibling->first_child->value(), (StringRef{"x", 1}));
    sql_engine::InMemoryCatalog catalog;
    Arena arena;
    sql_engine::PlanBuilder<Dialect::PostgreSQL> builder(catalog, arena);
    EXPECT_EQ(builder.build(r.ast), nullptr);
}

TEST(PgExpressions, MalformedSubqueriesReportErrorsWithoutCrashing) {
    for (const char* sql : {"SELECT EXISTS (SELECT 1", "SELECT EXISTS (WITH t AS () SELECT 1)",
         "SELECT EXISTS (VALUES (1,))", "SELECT (SELECT 1", "SELECT x IN (SELECT 1",
         "SELECT * FROM (WITH t AS () SELECT 1) AS q"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgExpressions, AggregateAndSubqueryGrammarBoundaries) {
    for (const char* sql : {"SELECT f(ALL 0.5) WITHIN GROUP (ORDER BY x)",
         "SELECT f(*) WITHIN GROUP (ORDER BY x)",
         "SELECT (SELECT x FROM t ORDER BY x NULLS FIRST)",
         "WITH t AS (SELECT x FROM s ORDER BY x NULLS LAST) SELECT * FROM t"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        EXPECT_EQ(emit_pg(parser, r.ast), sql);
    }
    for (const char* sql : {"SELECT count(* ORDER BY x)", "SELECT f(*, x)", "SELECT f(x, *)", "SELECT f(DISTINCT 1, *)",
         "SELECT count(x ORDER BY *)", "SELECT f(x) WITHIN GROUP (ORDER BY *)",
         "SELECT (VALUES (1) ORDER BY 2 +)", "SELECT (TABLE t LIMIT 1 +)",
         "WITH a AS (SELECT 1) WITH b AS (SELECT 2) SELECT 3"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgExpressions, RejectsLocalExecutionOfUnsupportedCteBodies) {
    for (const char* sql : {"WITH t AS (VALUES (1)) SELECT 2",
         "WITH t AS (WITH u AS (SELECT 1) SELECT * FROM u) SELECT 2",
         "SELECT (WITH t AS (SELECT 1) SELECT * FROM t)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        sql_engine::InMemoryCatalog catalog;
        Arena arena;
        sql_engine::PlanBuilder<Dialect::PostgreSQL> builder(catalog, arena);
        EXPECT_EQ(builder.build(r.ast), nullptr);
    }
}

TEST(PgExpressions, CteNamesPreserveEmbeddedQuotes) {
    const char* sql = "WITH \"a\"\"b\" AS (SELECT 1) SELECT * FROM \"a\"\"b\"";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.full_input);
    EXPECT_EQ(emit_pg(parser, r.ast), sql);
}
