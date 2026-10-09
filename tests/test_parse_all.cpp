#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

TEST(ParseAll, RetainsAllStatementsAndSourceBoundaries) {
    Parser<Dialect::PostgreSQL> parser;
    std::string sql = " ; SELECT ';' AS x; /* ; */ SELECT $$a;b$$; -- ;\n SELECT 3; ";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_TRUE(batch.ok());
    ASSERT_EQ(batch.statements.size(), 3u);
    for (const auto& statement : batch.statements) {
        EXPECT_EQ(statement.source.ptr, sql.data() + statement.offset);
        ASSERT_NE(statement.result.ast, nullptr);
        ASSERT_TRUE(statement.result.full_input);
    }
    // Earlier ASTs remain valid after later statements were parsed.
    Emitter<Dialect::PostgreSQL> emitter(parser.arena());
    emitter.emit(batch.statements[0].result.ast);
    auto emitted = emitter.result();
    EXPECT_EQ(std::string(emitted.ptr, emitted.len), "SELECT ';' AS x");
}

TEST(ParseAll, KeepsFailuresAndFollowingStatements) {
    Parser<Dialect::PostgreSQL> parser;
    std::string sql = "SELECT 1; COPY users FROM; SELECT 2";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_EQ(batch.statements.size(), 3u);
    EXPECT_FALSE(batch.ok());
    EXPECT_EQ(batch.statements[1].result.status, ParseResult::ERROR);
    EXPECT_TRUE(batch.statements[2].result.full_input);
    EXPECT_NE(batch.statements[2].result.ast, nullptr);
}

TEST(ParseAll, LexicalErrorsAndCommentOnlyInput) {
    Parser<Dialect::PostgreSQL> parser;
    for (const std::string sql : {"SELECT 1; SELECT 'unterminated", "/* unclosed"}) {
        auto batch = parser.parse_all(sql.data(), sql.size());
        ASSERT_FALSE(batch.statements.empty());
        EXPECT_FALSE(batch.ok());
        EXPECT_EQ(batch.statements.back().result.status, ParseResult::ERROR);
    }
    std::string empty = " ; -- only comments\n /* ; */ ; ";
    auto batch = parser.parse_all(empty.data(), empty.size());
    EXPECT_TRUE(batch.ok());
    EXPECT_TRUE(batch.statements.empty());
}

TEST(ParseAll, MySqlEscapesAndVariables) {
    Parser<Dialect::MySQL> parser;
    std::string sql = "SELECT 'a;\\\'b', @x; SELECT 2";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_EQ(batch.statements.size(), 2u);
    EXPECT_TRUE(batch.ok());
    EXPECT_TRUE(batch.statements[0].result.has_user_variables);
    EXPECT_FALSE(batch.statements[1].result.has_user_variables);
}

TEST(ParseAll, UnknownStatementsDoNotMakeSuccessfulBatch) {
    Parser<Dialect::PostgreSQL> parser;
    std::string sql = "SELECT 1; gibberish; SELECT 2";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_EQ(batch.statements.size(), 3u);
    EXPECT_FALSE(batch.ok());
    EXPECT_EQ(batch.statements[1].result.stmt_type, StmtType::UNKNOWN);
}
