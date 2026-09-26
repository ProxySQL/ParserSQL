#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <cstdlib>
#include <string>

using namespace sql_parser;

static void accepts_continuation(const char* sql, bool guarded = true) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(parsed.status, ParseResult::OK);
    ASSERT_TRUE(parsed.full_input);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(parsed.ast);
    auto output = emitter.result();
    Parser<Dialect::PostgreSQL> again;
    auto roundtrip = again.parse(output.ptr, output.len);
    EXPECT_EQ(roundtrip.status, ParseResult::OK);
    EXPECT_TRUE(roundtrip.full_input) << std::string(output.ptr, output.len);
    if (guarded) EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(parsed.ast));
}

static void rejects_continuation(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::PostgreSQL> parser;
    auto parsed = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(parsed.status == ParseResult::OK && parsed.full_input);
}

TEST(PgExpressionContinuation, VariadicCallsRetainFinalArgumentAndNamedValues) {
    for (const char* sql : {"SELECT f(VARIADIC ARRAY[1,2])", "SELECT f(1, VARIADIC values_array)",
         "SELECT f(VARIADIC args => ARRAY[1,2])", "SELECT f(VARIADIC arr ORDER BY x)",
         "SELECT * FROM f(1, VARIADIC arr)", "SELECT schema.f(VARIADIC ARRAY(SELECT x FROM t))"})
        accepts_continuation(sql);
    for (const char* sql : {"SELECT f(VARIADIC)", "SELECT f(VARIADIC *)", "SELECT f(VARIADIC arr, 1)",
         "SELECT f(VARIADIC VARIADIC arr)", "SELECT f(DISTINCT VARIADIC arr)",
         "SELECT f(ALL VARIADIC arr)", "SELECT coalesce(VARIADIC arr)",
         "SELECT f(VARIADIC arr) WITHIN GROUP (ORDER BY x)"}) rejects_continuation(sql);
}

TEST(PgExpressionContinuation, ArraySlicesKeepOptionalBoundsAndDimensions) {
    for (const char* sql : {"SELECT a[:3], b[1:], c[:], d[1:3]", "SELECT a[i + 1:j * 2][2:4]",
         "SELECT (ARRAY[1,2,3])[1:2]", "SELECT a[1:3][2], a[1][2:3]", "SELECT a[(SELECT 1):n]"})
        accepts_continuation(sql);
    for (const char* sql : {"SELECT a[]", "SELECT a[1:", "SELECT a[:1:2]", "SELECT a[1,2]",
         "SELECT a[*:2]", "SELECT a[1:*]", "SELECT a[:+]", "SELECT a[1"}) rejects_continuation(sql);
}

TEST(PgExpressionContinuation, QualifiedFieldsAndOperatorsRequireCompleteOperands) {
    for (const char* sql : {"SELECT * FROM a LEFT JOIN (b JOIN c USING(x)) ON a.x = b.",
         "SELECT * FROM t ORDER BY x > DESC", "SELECT * FROM t TABLESAMPLE f(5 . 5)",
         "SELECT a.", "SELECT a.1", "SELECT (a).", "SELECT a +", "SELECT x > FROM t"})
        rejects_continuation(sql);
    for (const char* sql : {"SELECT value[1].r", "SELECT a[1:3].r.s", "SELECT (a + b).field",
         "SELECT schema.table_name.column_name", "SELECT a[1].\"select\""})
        accepts_continuation(sql, false);
}

TEST(PgExpressionContinuation, PatternPredicatesRetainEscapeAndNegation) {
    for (const char* sql : {"SELECT s ILIKE p", "SELECT s NOT ILIKE p ESCAPE e",
         "SELECT s SIMILAR TO p", "SELECT s NOT SIMILAR TO p ESCAPE '#'",
         "SELECT s LIKE p ESCAPE e", "SELECT s NOT LIKE p ESCAPE e",
         "SELECT s ILIKE ANY(patterns)", "SELECT s NOT ILIKE ALL(SELECT p FROM patterns)",
         "SELECT a ILIKE ANY(b) LIKE c", "SELECT a ILIKE ANY(b) ILIKE ANY(c)",
         "SELECT a ILIKE (b ILIKE ANY(c))", "SELECT a LIKE (b LIKE ALL(c))",
         "SELECT NOT s ILIKE p || '%' ESCAPE e IS FALSE", "SELECT s SIMILAR TO (p || '%') ESCAPE e + f"})
        accepts_continuation(sql);
    for (const char* sql : {"SELECT s NOT ILIKE", "SELECT s SIMILAR TO", "SELECT s SIMILAR p",
         "SELECT s LIKE p ESCAPE", "SELECT s ILIKE *", "SELECT s SIMILAR TO *", "SELECT s LIKE p ESCAPE *",
         "SELECT s ILIKE p ESCAPE e ESCAPE f", "SELECT s SIMILAR TO ANY(patterns)",
         "SELECT s ILIKE ANY(patterns) ESCAPE e", "SELECT s ILIKE p ILIKE q", "SELECT a LIKE b ILIKE c",
         "SELECT a ILIKE b ILIKE ANY(c)", "SELECT a LIKE b ILIKE ALL(c)",
         "SELECT a ILIKE b LIKE ANY(c)", "SELECT a LIKE b LIKE ALL(c)"}) rejects_continuation(sql);
}

TEST(PgExpressionContinuation, PositionAndOverlayUseSqlArgumentBoundaries) {
    for (const char* sql : {"SELECT POSITION('a' IN s)", "SELECT POSITION(a + b IN s || t)",
         "SELECT POSITION((x IN (1,2)) IN s)", "SELECT OVERLAY(s PLACING 'x' FROM 2 FOR n + 1)",
         "SELECT OVERLAY(s PLACING repl FROM startpos)", "SELECT overlay(), overlay(s, 'x', 2, 1)",
         "SELECT overlay(s => 'abc', replacement => 'x')"}) accepts_continuation(sql);
    for (const char* sql : {"SELECT POSITION()", "SELECT POSITION(a, s)", "SELECT POSITION(a IN)",
         "SELECT POSITION(* IN s)", "SELECT POSITION(a IN *)", "SELECT POSITION(NOT a IN s)",
         "SELECT OVERLAY(s PLACING x)", "SELECT OVERLAY(s PLACING x FROM)",
         "SELECT OVERLAY(s PLACING x FROM 1 FOR)", "SELECT OVERLAY(* PLACING x FROM 1)",
         "SELECT OVERLAY(s PLACING * FROM 1)", "SELECT overlay(*)", "SELECT overlay(VARIADIC arr)"})
        rejects_continuation(sql);
}

TEST(PgExpressionContinuation, JsonPredicatesPreserveTypeAndUniqueKeysSyntax) {
    for (const char* sql : {"SELECT doc IS JSON", "SELECT a IS JSON IS JSON", "SELECT doc IS NOT JSON VALUE",
         "SELECT doc IS JSON OBJECT WITH UNIQUE KEYS", "SELECT doc IS JSON ARRAY WITHOUT UNIQUE",
         "SELECT doc IS JSON SCALAR WITH UNIQUE", "SELECT (doc || suffix) IS JSON WITHOUT UNIQUE KEYS",
         "SELECT NOT doc IS JSON OBJECT AND flag", "SELECT doc IS JSON \"OBJECT\"", "SELECT doc IS JSON ARRAY OBJECT"})
        accepts_continuation(sql);
    for (const char* sql : {"SELECT doc IS JSON WITH KEYS", "SELECT doc IS JSON WITHOUT",
         "SELECT doc IS JSON OBJECT WITH UNIQUE KEYS WITHOUT UNIQUE KEYS", "SELECT * IS JSON",
         "SELECT doc IS JSON WITH \"UNIQUE\" KEYS"}) rejects_continuation(sql);
}

TEST(PgExpressionContinuation, BitAndHexPrefixesAreLiteralSyntax) {
    for (const char* sql : {"SELECT POSITION(B'101101' IN B'001011001011')",
         "SELECT OVERLAY(B'0101011100' PLACING '001' FROM 20)", "SELECT X'1a', x'FF', b'01', B''",
         "SELECT b '101', \"b\"'101'"}) accepts_continuation(sql, false);
    accepts_continuation("SELECT B'01', X'ff'");
}

TEST(PgExpressionContinuation, QuotedKeywordsRemainNames) {
    for (const char* sql : {"SELECT f(\"variadic\")", "SELECT \"position\"(a,s)",
         "SELECT schema.position(a,s)", "SELECT \"overlay\"(s,x,1)", "SELECT s AS \"ilike\"",
         "SELECT s ILIKE FROM t", "SELECT s LIKE", "SELECT s SIMILAR"})
        accepts_continuation(sql, false);
}

TEST(PgExpressionContinuation, SortOperatorsWorkInsideAggregatesAndWindows) {
    for (const char* sql : {"SELECT array_agg(x ORDER BY y USING <)",
         "SELECT array_agg(VARIADIC a ORDER BY x USING OPERATOR(pg_catalog.>) NULLS LAST)",
         "SELECT first_value(x) OVER (ORDER BY y USING >)",
         "SELECT first_value(x) OVER (ORDER BY y USING OPERATOR(\"ops\".<) NULLS FIRST)"})
        accepts_continuation(sql);
    for (const char* sql : {"SELECT array_agg(x ORDER BY y USING)",
         "SELECT array_agg(x ORDER BY y USING foo)", "SELECT array_agg(x ORDER BY y USING LIKE)",
         "SELECT first_value(x) OVER (ORDER BY y USING OPERATOR(ops.foo))"}) rejects_continuation(sql);
}

static const AstNode* continuation_node(const AstNode* node, NodeType type) {
    if (!node || node->type == type) return node;
    for (const auto* child = node->first_child; child; child = child->next_sibling)
        if (const auto* found = continuation_node(child, type)) return found;
    return nullptr;
}

TEST(PgExpressionContinuation, OperandsRemainStructuredForTraversalAndRewriting) {
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "SELECT a[1:n + 2] ILIKE pattern ESCAPE escape_value AND ready";
    auto parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.full_input);
    const auto* predicate = continuation_node(parsed.ast, NodeType::NODE_PG_PATTERN_PREDICATE);
    ASSERT_NE(predicate, nullptr);
    const auto* slice = predicate->first_child;
    ASSERT_NE(slice, nullptr);
    ASSERT_EQ(slice->type, NodeType::NODE_PG_ARRAY_SLICE);
    EXPECT_EQ(slice->flags, 3);
    ASSERT_NE(slice->first_child, nullptr);
    ASSERT_NE(slice->first_child->next_sibling, nullptr);
    EXPECT_EQ(slice->first_child->next_sibling->type, NodeType::NODE_LITERAL_INT);
    ASSERT_NE(slice->first_child->next_sibling->next_sibling, nullptr);
    EXPECT_EQ(slice->first_child->next_sibling->next_sibling->type, NodeType::NODE_BINARY_OP);
    ASSERT_NE(slice->next_sibling, nullptr);
    ASSERT_NE(slice->next_sibling->next_sibling, nullptr);
    EXPECT_TRUE(slice->next_sibling->value().equals_ci("pattern", 7));
    EXPECT_TRUE(slice->next_sibling->next_sibling->value().equals_ci("escape_value", 12));
    Arena output;
    EXPECT_FALSE(parameterize_ast<Dialect::PostgreSQL>(parsed, output).ok());

    sql = "SELECT f(VARIADIC args => ARRAY[1,2])";
    parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.full_input);
    const auto* variadic = continuation_node(parsed.ast, NodeType::NODE_PG_VARIADIC_ARGUMENT);
    ASSERT_NE(variadic, nullptr);
    ASSERT_NE(variadic->first_child, nullptr);
    EXPECT_EQ(variadic->first_child->type, NodeType::NODE_NAMED_ARGUMENT);
    ASSERT_NE(variadic->first_child->first_child, nullptr);
    EXPECT_EQ(variadic->first_child->first_child->type, NodeType::NODE_ARRAY_CONSTRUCTOR);
}

TEST(PgExpressionContinuation, ExhaustedArenasNeverDropCompletedExpressionSyntax) {
    EXPECT_EXIT(([] {
        for (const char* sql : {"SELECT f(VARIADIC ARRAY[1,2])", "SELECT a[1:3]", "SELECT a[:]",
             "SELECT s ILIKE p ESCAPE e", "SELECT POSITION('a' IN s)",
             "SELECT OVERLAY(s PLACING 'x' FROM 1 FOR 2)", "SELECT overlay(s,x,1)",
             "SELECT doc IS JSON OBJECT WITH UNIQUE KEYS",
             "SELECT array_agg(x ORDER BY y USING < NULLS LAST)",
             "SELECT first_value(x) OVER (ORDER BY y USING > NULLS FIRST)"}) {
            Parser<Dialect::PostgreSQL> baseline;
            auto original = baseline.parse(sql, std::strlen(sql));
            if (!original.ok() || !original.full_input) std::exit(2);
            Arena initial;
            Emitter<Dialect::PostgreSQL> canonical(initial);
            canonical.emit(original.ast);
            std::string expected(canonical.result().ptr, canonical.result().len);
            for (size_t bytes = 48; bytes <= 2304; bytes += 48) {
                ParserConfig config;
                config.arena_block_size = config.arena_max_size = bytes;
                Parser<Dialect::PostgreSQL> parser(config);
                auto parsed = parser.parse(sql, std::strlen(sql));
                if (!parsed.ok() || !parsed.full_input) continue;
                Arena output;
                Emitter<Dialect::PostgreSQL> emitter(output);
                emitter.emit(parsed.ast);
                if (std::string(emitter.result().ptr, emitter.result().len) != expected) std::exit(3);
            }
        }
        std::exit(0);
    }()), ::testing::ExitedWithCode(0), "");
}
