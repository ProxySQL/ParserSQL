#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_walk.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>
using namespace sql_parser;
static std::string render(Parser<Dialect::PostgreSQL>& p, AstNode* ast) {
    Emitter<Dialect::PostgreSQL> e(p.arena()); e.emit(ast);
    return std::string(e.result().ptr, e.result().len);
}
TEST(PgQueryGaps, GroupingSetsRetainNestedStructure) {
    for (const char* sql : {"SELECT a, count(*) FROM t GROUP BY GROUPING SETS ((a), ())",
         "SELECT a, b FROM t GROUP BY ROLLUP (a, b), CUBE (a, b)",
         "SELECT a FROM t GROUP BY DISTINCT GROUPING SETS (ROLLUP (a), ())"}) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> p;
        auto r=p.parse(sql,strlen(sql)); ASSERT_EQ(r.status,ParseResult::OK); ASSERT_TRUE(r.full_input);
        EXPECT_EQ(render(p,r.ast),sql);
        sql_engine::InMemoryCatalog c; Arena a; sql_engine::PlanBuilder<Dialect::PostgreSQL> b(c,a);
        EXPECT_EQ(b.build(r.ast),nullptr);
    }
}
TEST(PgQueryGaps, PaginationComposesWithQueries) {
    const std::pair<const char*,const char*> cases[]={
        {"SELECT x FROM t OFFSET 10 LIMIT 5","SELECT x FROM t LIMIT 5 OFFSET 10"},
        {"SELECT 1 OFFSET 2","SELECT 1 OFFSET 2"},
        {"SELECT 1 OFFSET (1 + 2) ROWS", "SELECT 1 OFFSET (1 + 2)"},
        {"SELECT 1 FETCH FIRST -2 ROWS ONLY", "SELECT 1 FETCH FIRST -2 ROWS ONLY"},
        {"(SELECT 1 ORDER BY 1) FETCH FIRST ROW WITH TIES", "(SELECT 1 ORDER BY 1) FETCH FIRST 1 ROWS WITH TIES"},
        {"SELECT 1 LIMIT ALL OFFSET 2","SELECT 1 LIMIT NULL OFFSET 2"},
        {"VALUES (1), (2) OFFSET 1 ROW FETCH NEXT 3 ROWS ONLY","VALUES (1), (2) OFFSET 1 FETCH FIRST 3 ROWS ONLY"},
        {"SELECT x FROM t ORDER BY x FETCH FIRST ROW WITH TIES","SELECT x FROM t ORDER BY x FETCH FIRST 1 ROWS WITH TIES"},
        {"SELECT x FROM t FETCH FIRST (2 + 1) ROWS ONLY OFFSET 2","SELECT x FROM t OFFSET 2 FETCH FIRST (2 + 1) ROWS ONLY"},
    };
    for(auto c:cases){SCOPED_TRACE(c.first); Parser<Dialect::PostgreSQL> p; auto r=p.parse(c.first,strlen(c.first));
        ASSERT_EQ(r.status,ParseResult::OK); ASSERT_TRUE(r.full_input); EXPECT_EQ(render(p,r.ast),c.second);}
}
TEST(PgQueryGaps, TableFunctionColumnsAndOrdinality) {
    for(const char* sql:{"SELECT * FROM unnest(a) WITH ORDINALITY AS t(x, n)",
        "SELECT * FROM f() AS t(x integer, \"Y\" numeric(4,2))",
        "SELECT * FROM f() AS (x integer, y text)",
        "SELECT * FROM f() WITH ORDINALITY AS t(x int)",
        "SELECT * FROM LATERAL f(x) WITH ORDINALITY AS t(a, b)"}) {
        SCOPED_TRACE(sql); Parser<Dialect::PostgreSQL> p; auto r=p.parse(sql,strlen(sql));
        ASSERT_EQ(r.status,ParseResult::OK); ASSERT_TRUE(r.full_input); EXPECT_EQ(render(p,r.ast),sql);
    }
}
TEST(PgQueryGaps, RejectsIncompleteClauseSyntax) {
    for(const char* sql:{"SELECT 1 FETCH FIRST ROW WITH TIES", "SELECT 1 OFFSET", "SELECT 1 LIMIT", "SELECT 1 OFFSET 1 OFFSET 2",
        "SELECT 1 FETCH FIRST 2 + 3 ROWS ONLY", "SELECT 1 FETCH FIRST ROW", "SELECT 1 LIMIT 1 FETCH FIRST ROW ONLY",
        "SELECT a FROM t GROUP BY GROUPING SETS ()", "SELECT a FROM t GROUP BY ROLLUP ()",
        "SELECT a FROM t GROUP BY GROUPING SETS (a,)", "SELECT * FROM f() WITH ORDINALITY AS t(x int, y)",
        "SELECT * FROM f() AS (x)", "SELECT * FROM f() AS t(x int, y)",
        "SELECT * FROM t WITH ORDINALITY", "SELECT * FROM f() WITH potato",
        "SELECT n 1 FROM t", "SELECT n AS 1 FROM t", "SELECT * FROM t AS 1",
        "SELECT 1 OFFSET 1 + 2 ROWS", "SELECT 1 FETCH FIRST 1::int ROWS ONLY",
        "SELECT 1 FETCH FIRST -x ROWS ONLY",
        "SELECT JSON_ARRAY(SELECT doc FROM FORMAT JSON RETURNING json)"}) {
        SCOPED_TRACE(sql);Parser<Dialect::PostgreSQL> p;auto r=p.parse(sql,strlen(sql));
        EXPECT_FALSE(r.status==ParseResult::OK && r.full_input);
    }
}

TEST(PgQueryGaps, RejectsGroupingWithoutByAndBareParenthesizedRelations) {
    for (const char* sql : {"SELECT a FROM t GROUP GROUPING SETS ((a), ())",
                           "SELECT a FROM t GROUP ROLLUP (a,b)",
                           "SELECT * FROM (s.t) AS x", "SELECT * FROM ((SELECT 1) AS x) AS y"}) {
        Parser<Dialect::PostgreSQL> p; auto r = p.parse(sql, strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input) << sql;
    }
}

TEST(PgQueryGaps, GroupingNamesRemainOrdinaryColumnsWithoutParentheses) {
    const char* sql = "SELECT rollup FROM t GROUP BY rollup";
    Parser<Dialect::PostgreSQL> p;
    auto r = p.parse(sql, strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_TRUE(r.full_input);
    EXPECT_EQ(render(p, r.ast), sql);
    EXPECT_TRUE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
}

TEST(PgQueryGaps, NestedQueriesAndOnlySources) {
    const std::pair<const char*, const char*> cases[] = {
        {"SELECT 1 UNION (WITH x AS (SELECT 2) SELECT * FROM x)", "SELECT 1 UNION (WITH x AS (SELECT 2) SELECT * FROM x)"},
        {"SELECT * FROM ((SELECT 1)) q", "SELECT * FROM (SELECT 1) AS q"},
        {"SELECT * FROM ONLY (s.t) AS x", "SELECT * FROM ONLY s.t AS x"},
        {"SELECT * FROM ONLY s.t", "SELECT * FROM ONLY s.t"}
    };
    for (auto c : cases) {
        SCOPED_TRACE(c.first); Parser<Dialect::PostgreSQL> p;
        auto r = p.parse(c.first, strlen(c.first));
        ASSERT_EQ(r.status, ParseResult::OK); ASSERT_TRUE(r.full_input);
        EXPECT_EQ(render(p, r.ast), c.second);
    }
    for (const char* sql : {"SELECT 1 UNION WITH x AS (SELECT 2) SELECT * FROM x",
                           "SELECT * FROM ONLY f()", "SELECT * FROM ONLY (s.t"}) {
        Parser<Dialect::PostgreSQL> p; auto r = p.parse(sql, strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input) << sql;
    }
}

TEST(PgQueryGaps, ExplicitInheritedRelationRetainsStar) {
    const char* sql = "SELECT * FROM s.t * AS x WHERE x.a > 1";
    Parser<Dialect::PostgreSQL> p; auto r = p.parse(sql, strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK); ASSERT_TRUE(r.full_input);
    EXPECT_EQ(render(p, r.ast), sql);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
}

TEST(PgQueryGaps, ReservedQualifiedRelationComponent) {
    const char* sql = "SELECT * FROM public.select";
    Parser<Dialect::PostgreSQL> p; auto r = p.parse(sql, strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK); ASSERT_TRUE(r.full_input);
    EXPECT_EQ(render(p, r.ast), sql);
}

TEST(PgQueryGaps, AllocationFailureDoesNotDropQueryClauses) {
    for (const char* sql : {"SELECT a FROM t GROUP BY GROUPING SETS ((a), ()) OFFSET 2",
                           "SELECT * FROM f() WITH ORDINALITY AS t(x, n) ORDER BY x FETCH FIRST 3 ROWS WITH TIES"}) {
        Parser<Dialect::PostgreSQL> baseline; auto complete = baseline.parse(sql, strlen(sql));
        ASSERT_EQ(complete.status, ParseResult::OK); ASSERT_TRUE(complete.full_input);
        size_t expected = 0;
        walk_ast(complete.ast, [&](const AstNode&, const AstVisitContext&) { ++expected; return AstVisitAction::Continue; });
        for (size_t capacity = 48; capacity <= 2048; capacity += 48) {
            ParserConfig config; config.arena_block_size = capacity; config.arena_max_size = capacity;
            Parser<Dialect::PostgreSQL> p(config); auto r = p.parse(sql, strlen(sql));
            if (r.status == ParseResult::OK && r.full_input) {
                size_t actual = 0;
                walk_ast(r.ast, [&](const AstNode&, const AstVisitContext&) { ++actual; return AstVisitAction::Continue; });
                EXPECT_EQ(actual, expected) << sql << " capacity=" << capacity;
            }
        }
    }
}

TEST(PgQueryGaps, CaseExpressionsReconstructBranches) {
    for (const char* sql : {"SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END FROM t",
                           "SELECT CASE WHEN x > 0 THEN x ELSE 0 END FROM t",
                           "SELECT CASE WHEN true THEN 1 END"}) {
        Parser<Dialect::PostgreSQL> p; auto r = p.parse(sql, strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK); ASSERT_TRUE(r.full_input);
        EXPECT_EQ(render(p, r.ast), sql);
    }
}
