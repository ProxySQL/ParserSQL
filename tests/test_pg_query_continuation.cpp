#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_walk.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>
using namespace sql_parser;

static std::string query_roundtrip(const char* sql, bool guarded = true) {
    Parser<Dialect::PostgreSQL> p;
    auto r = p.parse(sql, std::strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK) << sql;
    EXPECT_TRUE(r.full_input) << sql;
    if (!r.ok() || !r.full_input) return {};
    if (guarded) EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast)) << sql;
    Emitter<Dialect::PostgreSQL> emitter(p.arena()); emitter.emit(r.ast);
    auto output = emitter.result();
    std::string text(output.ptr, output.len);
    Parser<Dialect::PostgreSQL> again; auto second = again.parse(text.data(), text.size());
    EXPECT_EQ(second.status, ParseResult::OK) << text;
    EXPECT_TRUE(second.full_input) << text;
    return text;
}
TEST(PgQueryContinuation, NestedJoinsPreserveAssociation) {
    for (const char* sql : {
        "SELECT * FROM (a LEFT JOIN b ON a.x = b.x) AS j(x, y)",
        "SELECT * FROM a LEFT JOIN (b JOIN c USING (x)) ON a.x = b.x",
        "SELECT * FROM a JOIN b JOIN c ON b.x = c.x ON a.x = b.x",
        "SELECT * FROM (a NATURAL LEFT JOIN b) CROSS JOIN c",
        "SELECT * FROM a JOIN b USING (x) AS merged",
        "SELECT * FROM a, b JOIN c ON b.x = c.x",
        "SELECT * FROM ((a JOIN b ON true)) AS j"
    }) query_roundtrip(sql, false);
    query_roundtrip("SELECT * FROM a LEFT JOIN (b JOIN c ON b.x = c.x) ON a.x = b.x");
}
TEST(PgQueryContinuation, TableSamplingAndRowsFrom) {
    for (const char* sql : {
        "SELECT * FROM s.t AS x TABLESAMPLE BERNOULLI (5.5) REPEATABLE (0)",
        "SELECT * FROM ONLY t TABLESAMPLE system_rows(10)",
        "SELECT * FROM t TABLESAMPLE like(1)",
        "SELECT * FROM t TABLESAMPLE between.method(1)",
        "SELECT * FROM a.b.c.d()",
        "SELECT * FROM ROWS FROM (f(), g(1)) WITH ORDINALITY AS t(a, b, n)",
        "SELECT * FROM LATERAL ROWS FROM (f(x) AS (a int), g() AS (b text)) AS t"
    }) query_roundtrip(sql);
}
TEST(PgQueryContinuation, SortingLockingAndInto) {
    for (const char* sql : {
        "SELECT x FROM t ORDER BY x USING > NULLS LAST",
        "SELECT x FROM t ORDER BY x USING OPERATOR(pg_catalog.<)",
        "SELECT * FROM t FOR NO KEY UPDATE OF t NOWAIT",
        "SELECT * FROM t FOR KEY SHARE SKIP LOCKED",
        "SELECT * FROM t FOR UPDATE OF t FOR SHARE OF t",
        "SELECT * FROM t FOR UPDATE LIMIT 2",
        "SELECT x INTO TEMPORARY TABLE tmp FROM t",
        "SELECT 1 INTO UNLOGGED s.t",
        "SELECT 1 INTO LOCAL TEMP t", "SELECT 1 INTO GLOBAL TEMPORARY TABLE t"
    }) query_roundtrip(sql);
}
TEST(PgQueryContinuation, RejectsMalformedQueryProductions) {
    for (const char* sql : {
        "SELECT * FROM a JOIN b", "SELECT * FROM a CROSS JOIN b ON true",
        "SELECT * FROM a NATURAL JOIN b USING (x)", "SELECT * FROM a LEFT INNER JOIN b ON true",
        "SELECT * FROM a JOIN b USING ()", "SELECT * FROM a JOIN b USING (x,) AS j",
        "SELECT * FROM a JOIN b USING (x) AS j(a)", "SELECT * FROM a JOIN b ON *",
        "SELECT * FROM t TABLESAMPLE", "SELECT * FROM t TABLESAMPLE f()",
        "SELECT * FROM t TABLESAMPLE f(1) REPEATABLE ()", "SELECT * FROM f() TABLESAMPLE f(1)",
        "SELECT * FROM ROWS FROM ()", "SELECT * FROM ROWS FROM (t)",
        "SELECT * FROM t ORDER BY x USING foo", "SELECT * FROM t ORDER BY x USING > DESC",
        "SELECT * FROM t FOR NO UPDATE", "SELECT * FROM t FOR KEY UPDATE", "SELECT * FROM t FOR SHARE SKIP",
        "SELECT * FROM t FOR UPDATE OF", "SELECT * FROM t FOR UPDATE NOWAIT SKIP LOCKED",
        "SELECT 1 INTO", "SELECT 1 INTO TEMP UNLOGGED t",
        "SELECT 1 INTO LOCAL t", "SELECT 1 INTO GLOBAL UNLOGGED t",
        "SELECT * FROM t TABLESAMPLE between(1)", "SELECT * FROM t TABLESAMPLE like.method(1)",
        "SELECT * FROM a.b.c.d", "SELECT 1 INTO a.b.c.d",
        "SELECT * FROM t FOR UPDATE OF a.b.c.d", "SELECT * t FOR UPDATE",
        "SELECT * FROM a CROSS JOIN b true"
    }) {
        Parser<Dialect::PostgreSQL> p; auto r = p.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input) << sql;
    }
}

TEST(PgQueryContinuation, QuotedNamesAndAllocationFailures) {
    for (const char* sql : {
        "SELECT x FROM t ORDER BY x USING OPERATOR(pg_catalog.<) NULLS FIRST",
        "SELECT * FROM \"a\" JOIN \"b\" USING (\"x\") AS \"merged\"",
        "SELECT * FROM t AS \"sample\" TABLESAMPLE \"method\"(1) REPEATABLE (2)",
        "SELECT * FROM ROWS FROM (f() AS (\"x\" int), g()) WITH ORDINALITY AS \"r\"",
        "SELECT 1 INTO TEMP \"Schema\".\"Table\"",
        "SELECT * FROM \"Table\" FOR NO KEY UPDATE OF \"Table\" NOWAIT",
        "SELECT * FROM a LEFT JOIN (b JOIN c ON b.x = c.x) ON a.x = b.x"
    }) {
        SCOPED_TRACE(sql);
        query_roundtrip(sql);
        auto count=[](const AstNode* ast) {
            size_t total=0;
            walk_ast(ast,[&](const AstNode&,const AstVisitContext&) { ++total; return AstVisitAction::Continue; });
            return total;
        };
        Parser<Dialect::PostgreSQL> baseline; auto valid=baseline.parse(sql,std::strlen(sql));
        ASSERT_TRUE(valid.ok() && valid.full_input); const auto expected=count(valid.ast);
        for (size_t capacity=48; capacity<=1920; capacity+=48) {
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size=capacity; config.arena_max_size=capacity;
            Parser<Dialect::PostgreSQL> parser(config); auto result=parser.parse(sql,std::strlen(sql));
            if (result.ok() && result.full_input) EXPECT_EQ(count(result.ast),expected);
        }
    }
}

TEST(PgQueryContinuation, ExplainPreservesInnerCompletion) {
    for (const char* sql : {
        "EXPLAIN (COSTS OFF, VERBOSE, FORMAT JSON) SELECT * FROM (a JOIN b ON true)",
        "EXPLAIN ANALYZE VERBOSE SELECT 1", "EXPLAIN (SELECT 1)",
        "EXPLAIN CREATE TABLE t AS SELECT 1", "EXPLAIN CREATE MATERIALIZED VIEW v AS SELECT 1"
    }) query_roundtrip(sql, false);
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "EXPLAIN SELECT * FROM a JOIN b";
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok());
}

TEST(PgQueryContinuation, RejectsMalformedExplainOptions) {
    for (const char* sql : {"EXPLAIN () SELECT 1", "EXPLAIN (COSTS OFF,) SELECT 1",
        "EXPLAIN (COSTS OFF VERBOSE) SELECT 1", "EXPLAIN VERBOSE ANALYZE SELECT 1",
        "EXPLAIN FORMAT JSON SELECT 1", "EXPLAIN CREATE TABLE t(x int)",
        "EXPLAIN CREATE VIEW v AS SELECT 1", "EXPLAIN ALTER TABLE t ADD x int"}) {
        Parser<Dialect::PostgreSQL> parser; auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input) << sql;
    }
}

TEST(PgQueryContinuation, StarExceptDistinguishesSetOperationsFromExtension) {
    query_roundtrip("SELECT * FROM a EXCEPT SELECT * FROM b", false);
    query_roundtrip("SELECT * FROM a EXCEPT (SELECT * FROM b)", false);
    Parser<Dialect::PostgreSQL> parser;
    const char* extension = "SELECT * EXCEPT(id) FROM t";
    auto result = parser.parse(extension, std::strlen(extension));
    EXPECT_TRUE(result.ok() && result.full_input);
}
