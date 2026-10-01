#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

static size_t policy_stats_nodes(const AstNode* ast) {
    size_t count = 0;
    walk_ast(ast, [&](const AstNode&, const AstVisitContext&) {
        ++count; return AstVisitAction::Continue;
    });
    return count;
}

static void policy_stats_accept(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(result.ast->type, NodeType::NODE_PG_DDL_STMT);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(result.ast);
    auto output = emitter.result();
    Parser<Dialect::PostgreSQL> reparsed;
    auto again = reparsed.parse(output.ptr, output.len);
    EXPECT_TRUE(again.ok() && again.full_input) << std::string(output.ptr, output.len);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(result.ast));
    Arena out;
    EXPECT_EQ(parameterize_ast<Dialect::PostgreSQL>(result, out).error, AstError::UnsupportedRoot);
}

TEST(PgPolicyStatistics, Policies) {
    for (const char* sql : {
        "CREATE POLICY p ON public.t",
        "CREATE POLICY \"Policy\" ON \"S\".\"T\" AS RESTRICTIVE FOR SELECT TO alice, CURRENT_USER USING (a > 0) WITH CHECK (b IS NOT NULL);",
        "CREATE POLICY p ON t AS PERMISSIVE FOR ALL TO PUBLIC USING (id IN (SELECT id FROM allowed))",
        "CREATE POLICY p ON t FOR INSERT TO \"Role\" WITH CHECK (true)",
        "CREATE POLICY p ON t AS \"restrictive\" TO \"NONE\"",
        "ALTER POLICY p ON t TO bob, SESSION_USER USING (a = 1) WITH CHECK (b > 2)",
        "ALTER POLICY p ON t USING (a = (SELECT max(a) FROM x))",
        "ALTER POLICY p ON t",
        "ALTER POLICY p ON t RENAME TO q",
        "DROP POLICY IF EXISTS p ON public.t CASCADE"
    }) policy_stats_accept(sql);
}

TEST(PgPolicyStatistics, ExtendedStatistics) {
    for (const char* sql : {
        "CREATE STATISTICS ON a, b FROM t",
        "CREATE STATISTICS (mcv) ON a, b FROM t",
        "CREATE STATISTICS IF NOT EXISTS public.s (mcv, ndistinct, dependencies) ON a, b FROM public.t",
        "CREATE STATISTICS s ON lower(a), (b + 1), ((c).d IS NOT NULL) FROM t",
        "CREATE STATISTICS s ON COALESCE(a,b), NULLIF(a,b) FROM t",
        "CREATE STATISTICS s ON a FROM t x, public.u AS y",
        "CREATE STATISTICS s ON a FROM t JOIN u ON t.id = u.id",
        "CREATE STATISTICS s ON a FROM t TABLESAMPLE SYSTEM (1)",
        "ALTER STATISTICS public.s SET STATISTICS 100",
        "ALTER STATISTICS IF EXISTS s SET STATISTICS -1",
        "ALTER STATISTICS s SET STATISTICS DEFAULT",
        "ALTER STATISTICS s OWNER TO CURRENT_USER",
        "ALTER STATISTICS s SET SCHEMA other",
        "ALTER STATISTICS s RENAME TO other",
        "DROP STATISTICS IF EXISTS public.s, other CASCADE;"
    }) policy_stats_accept(sql);
}

TEST(PgPolicyStatistics, RejectsMalformedProductions) {
    for (const char* sql : {
        "CREATE POLICY p ON t AS UNKNOWN", "CREATE POLICY p ON t AS \"RESTRICTIVE\"",
        "CREATE POLICY p ON t AS 'permissive'",
        "CREATE POLICY p ON t TO \"none\"", "CREATE POLICY p ON t TO NONE",
        "CREATE POLICY p ON t FOR MERGE",
        "CREATE POLICY p ON t TO", "CREATE POLICY p ON t USING ()",
        "CREATE POLICY p ON t WITH CHECK ()", "CREATE POLICY p ON t USING (a) TO bob",
        "ALTER POLICY p ON t FOR SELECT",
        "ALTER POLICY p ON t TO bob,", "ALTER POLICY p ON t RENAME TO",
        "CREATE STATISTICS IF NOT EXISTS ON a FROM t",
        "CREATE STATISTICS s (mcv,) ON a FROM t", "CREATE STATISTICS s () ON a FROM t",
        "CREATE STATISTICS s ON FROM t", "CREATE STATISTICS s ON a, FROM t",
        "CREATE STATISTICS s ON a FROM", "CREATE STATISTICS s ON a FROM t,",
        "CREATE STATISTICS s ON a + b FROM t", "CREATE STATISTICS s ON (a +) FROM t",
        "ALTER STATISTICS s SET STATISTICS", "ALTER STATISTICS s SET STATISTICS 2147483648",
        "ALTER STATISTICS s SET STATISTICS 1.5", "ALTER STATISTICS s SET STATISTICS 1 OWNER TO bob",
        "ALTER STATISTICS IF EXISTS s RENAME TO t", "ALTER STATISTICS IF EXISTS s OWNER TO bob",
        "DROP POLICY p", "DROP STATISTICS s,"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(PgPolicyStatistics, StructuredTraversalAndEditing) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "CREATE POLICY \"P\" ON public.t TO alice USING (a = 1)";
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok() && result.full_input);
    bool quoted = false;
    AstNode* literal = nullptr;
    walk_ast(result.ast, [&](const AstNode& node, const AstVisitContext&) {
        if (node.type == NodeType::NODE_IDENTIFIER && node.value() == StringRef{"P", 1})
            quoted = (node.flags & FLAG_IDENT_DELIMITED) != 0;
        if (node.type == NodeType::NODE_LITERAL_INT && node.value() == StringRef{"1", 1})
            literal = const_cast<AstNode*>(&node);
        return AstVisitAction::Continue;
    });
    EXPECT_TRUE(quoted);
    ASSERT_NE(literal, nullptr);
    literal->set_value(StringRef{"2", 1}); literal->set_source({});
    Emitter<Dialect::PostgreSQL> emitter(parser.arena()); emitter.emit(result.ast);
    auto output = emitter.result();
    EXPECT_NE(std::string(output.ptr, output.len).find("a = 2"), std::string::npos);
}

TEST(PgPolicyStatistics, ConstrainedArenaRetainsSuccessfulSyntax) {
    for (const char* sql : {
        "CREATE POLICY p ON t TO alice, bob USING (a > 0) WITH CHECK (b > 0)",
        "CREATE STATISTICS s (mcv, ndistinct) ON a, (b + 1) FROM t"
    }) {
        Parser<Dialect::PostgreSQL> baseline;
        auto expected = baseline.parse(sql, std::strlen(sql));
        ASSERT_TRUE(expected.ok() && expected.full_input);
        const auto count = policy_stats_nodes(expected.ast);
        for (size_t capacity = 48; capacity <= 1920; capacity += 48) {
            SCOPED_TRACE(sql);
            SCOPED_TRACE(capacity);
            ParserConfig config; config.arena_block_size = capacity; config.arena_max_size = capacity;
            Parser<Dialect::PostgreSQL> parser(config);
            auto result = parser.parse(sql, std::strlen(sql));
            if (result.ok() && result.full_input) {
                EXPECT_EQ(policy_stats_nodes(result.ast), count);
            }
        }
    }
}
