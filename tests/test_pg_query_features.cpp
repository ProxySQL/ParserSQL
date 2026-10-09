#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/compound_query_parser.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>
using namespace sql_parser;

TEST(PgQueryFeatures, EmitsStructuredQueryExtensions) {
    const char* queries[] = {
        "SELECT DISTINCT ON (a, b) a, b FROM t ORDER BY a",
        "SELECT sum(x) FILTER (WHERE x > 0) FROM t",
        "SELECT sum(x) FILTER (WHERE x > 0) OVER (ORDER BY x) FROM t",
        "SELECT * FROM t, LATERAL (SELECT t.x) AS s",
        "SELECT * FROM LATERAL generate_series(1, 3) AS g",
        "SELECT sum(x) OVER w FROM t WINDOW w AS (PARTITION BY y ORDER BY x)",
        "SELECT sum(x) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t WINDOW w AS (ORDER BY x)",
        "SELECT sum(x) OVER (ORDER BY x RANGE UNBOUNDED PRECEDING) FROM t",
        "SELECT sum(x) OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
        "SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        "SELECT sum(x) OVER (ORDER BY x ROWS CURRENT ROW EXCLUDE NO OTHERS) FROM t",
        "SELECT sum(x) OVER (ORDER BY x ROWS CURRENT ROW EXCLUDE GROUP) FROM t"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        auto emitted = emitter.result();
        EXPECT_EQ(std::string(emitted.ptr, emitted.len), sql);
    }
}

TEST(PgQueryFeatures, RejectsMalformedExtensions) {
    const char* queries[] = {
        "SELECT DISTINCT ON (x +) x",
        "SELECT DISTINCT ON () x", "SELECT DISTINCT ON (x,) x",
        "SELECT sum(x) FILTER (WHERE x +)", "SELECT sum(x) OVER (ORDER BY x +)",
        "SELECT sum(x) FILTER ()", "SELECT sum(x) FILTER (WHERE)",
        "SELECT * FROM LATERAL (t)",
        "SELECT * FROM LATERAL generate_series(1,) AS g",
        "SELECT * FROM LATERAL t", "SELECT * FROM LATERAL",
        "SELECT sum(x) OVER", "SELECT sum(x) OVER (ORDER x)",
        "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND)",
        "SELECT sum(x) OVER (ROWS UNBOUNDED FOLLOWING)",
        "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)",
        "SELECT sum(x) OVER (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW)",
        "SELECT sum(x) OVER (ROWS CURRENT ROW EXCLUDE)",
        "SELECT sum(x) OVER (ROWS (1 +) PRECEDING)",
        "SELECT sum(x) OVER w WINDOW w AS () ,",
        "SELECT sum(x) OVER w WINDOW w ()"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgQueryFeatures, PlannerRejectsUnsupportedExtensions) {
    const char* queries[] = {
        "SELECT DISTINCT ON (1) 1", "SELECT sum(1) FILTER (WHERE 1 = 1)",
        "SELECT sum(1) OVER (ROWS CURRENT ROW)",
        "SELECT sum(1) OVER w WINDOW w AS ()", "SELECT * FROM LATERAL (SELECT 1) AS s"
    };
    sql_engine::InMemoryCatalog catalog;
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        sql_engine::PlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
        EXPECT_EQ(builder.build(r.ast), nullptr);
    }
}

TEST(PgQueryFeatures, PreservesAliasesAndQuotedWindowNames) {
    const char* queries[] = {
        "SELECT \"My Column\" AS \"My Alias\" FROM \"MyTable\"",
        "SELECT \"a\"\"b\" FROM \"MyTable\"",
        "SELECT (r).\"My Field\"",
        "SELECT * FROM \"MyTable\"",
        "SELECT * FROM \"MySchema\".\"MyTable\"",
        "SELECT * FROM LATERAL generate_series(1, 3) AS g(x)",
        "SELECT * FROM LATERAL public.generate_series(1, 3) AS g(x)",
        "SELECT * FROM LATERAL \"F\"(1) AS \"G\"(\"X\")",
        "SELECT * FROM LATERAL (SELECT 1) AS \"Q\"(\"N\")",
        "SELECT sum(x) OVER \"W\" WINDOW \"W\" AS (ORDER BY x)",
        "SELECT sum(x) OVER (ORDER BY x DESC NULLS FIRST ROWS CURRENT ROW)"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        auto emitted = emitter.result();
        EXPECT_EQ(std::string(emitted.ptr, emitted.len), sql);
    }
}

TEST(PgQueryFeatures, RetainsWindowExpressionsForTransformation) {
    const char* sql = "SELECT sum(x) FILTER (WHERE x > 0) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_TRUE(r.full_input);
    auto* window = r.ast->first_child->first_child->first_child;
    ASSERT_EQ(window->type, NodeType::NODE_WINDOW_FUNCTION);
    auto* filter = window->first_child;
    ASSERT_EQ(filter->type, NodeType::NODE_AGGREGATE_FILTER);
    auto* predicate = filter->first_child->next_sibling;
    ASSERT_EQ(predicate->type, NodeType::NODE_BINARY_OP);
    predicate->first_child->next_sibling->set_value(StringRef{"5", 1});
    auto* frame = filter->next_sibling->first_child;
    ASSERT_EQ(frame->type, NodeType::NODE_WINDOW_FRAME);
    ASSERT_NE(frame->first_child->first_child, nullptr);
    frame->first_child->first_child->set_value(StringRef{"3", 1});
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(r.ast);
    auto emitted = emitter.result();
    EXPECT_EQ(std::string(emitted.ptr, emitted.len),
        "SELECT sum(x) FILTER (WHERE x > 5) OVER (ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)");
}

TEST(PgQueryFeatures, AllowsPostgresEmptyTargetListBeforeNamedWindow) {
    const char* sql = "SELECT FROM t WINDOW w AS (ORDER BY x)";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_TRUE(r.full_input);
}

TEST(PgQueryFeatures, TokenizerRespectsPostgresStringBoundaries) {
    const char* sql = "'a\\'; SELECT 2";
    Tokenizer<Dialect::PostgreSQL> tok;
    tok.reset(sql, std::strlen(sql));
    Token string = tok.next_token();
    EXPECT_EQ(string.type, TokenType::TK_STRING);
    EXPECT_EQ(std::string(string.source.ptr, string.source.len), "'a\\'");
    EXPECT_EQ(tok.next_token().type, TokenType::TK_SEMICOLON);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);

    const char* escaped = "E'a\\\';b'; SELECT 2";
    tok.reset(escaped, std::strlen(escaped));
    string = tok.next_token();
    EXPECT_EQ(string.type, TokenType::TK_STRING);
    EXPECT_EQ(std::string(string.source.ptr, string.source.len), "E'a\\\';b'");
    EXPECT_EQ(tok.next_token().type, TokenType::TK_SEMICOLON);
    EXPECT_EQ(tok.next_token().type, TokenType::TK_SELECT);
}

TEST(PgQueryFeatures, PreservesEscapeStringEmission) {
    const char* sql = "SELECT E'a\\\';b'";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    ASSERT_TRUE(r.full_input);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(r.ast);
    auto emitted = emitter.result();
    EXPECT_EQ(std::string(emitted.ptr, emitted.len), sql);
}

TEST(PgQueryFeatures, TokenizerRetainsDollarStringSourceAndRejectsTruncation) {
    for (const char* sql : {"$$a;b$$;", "$tag$a;b$tag$;"}) {
        SCOPED_TRACE(sql);
        Tokenizer<Dialect::PostgreSQL> tok;
        tok.reset(sql, std::strlen(sql));
        auto token = tok.next_token();
        EXPECT_EQ(token.type, TokenType::TK_STRING);
        EXPECT_EQ(std::string(token.source.ptr, token.source.len), std::string(sql, std::strlen(sql) - 1));
        EXPECT_EQ(tok.next_token().type, TokenType::TK_SEMICOLON);
    }
    for (const char* sql : {"$$unterminated", "$tag$unterminated"}) {
        Tokenizer<Dialect::PostgreSQL> tok;
        tok.reset(sql, std::strlen(sql));
        EXPECT_EQ(tok.next_token().type, TokenType::TK_ERROR);
    }
}

TEST(PgQueryFeatures, StrictQueryModeRejectsIncompleteOperandsButAllowsEmptyTargets) {
    for (const char* sql : {"SELECT 1 +", "SELECT 1 WHERE x =", "SELECT 1 ORDER BY x +", "SELECT 1 LIMIT 1 +"}) {
        SCOPED_TRACE(sql);
        Tokenizer<Dialect::PostgreSQL> tok;
        tok.reset(sql, std::strlen(sql));
        Arena arena;
        CompoundQueryParser<Dialect::PostgreSQL> query(tok, arena, true);
        EXPECT_EQ(query.parse(TokenType::TK_EOF), nullptr);
    }
    Tokenizer<Dialect::PostgreSQL> tok;
    const char* sql = "SELECT FROM t";
    tok.reset(sql, std::strlen(sql));
    Arena arena;
    CompoundQueryParser<Dialect::PostgreSQL> query(tok, arena, true);
    EXPECT_NE(query.parse(TokenType::TK_EOF), nullptr);
}

TEST(PgQueryFeatures, PreservesQuotedFunctionNames) {
    const char* sql = "SELECT \"MyFunc\"(1)";
    Parser<Dialect::PostgreSQL> parser;
    auto r = parser.parse(sql, std::strlen(sql));
    ASSERT_EQ(r.status, ParseResult::OK);
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(r.ast);
    auto emitted = emitter.result();
    EXPECT_EQ(std::string(emitted.ptr, emitted.len), sql);
}

TEST(PgQueryFeatures, NormalizesMysqlCommaLimitToCountThenOffset) {
    for (const char* sql : {"SELECT a FROM t LIMIT 2, 5", "SELECT 1 UNION SELECT 2 LIMIT 2, 5"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(r.ast);
        auto emitted = emitter.result();
        std::string out(emitted.ptr, emitted.len);
        EXPECT_EQ(out.substr(out.find("LIMIT")), "LIMIT 5 OFFSET 2");
    }
}
