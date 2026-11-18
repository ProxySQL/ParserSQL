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

TEST(ParseAll, ExecutableCommentProvenanceIsPerStatementEvenOnErrors) {
    Parser<Dialect::MySQL> parser;
    std::string sql = "/*! lead */ SELECT 1; SELECT 2 /*M! + 1 */; SELECT 3; SELECT ) /*! trailing */; SELECT 4";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_EQ(batch.statements.size(), 5u);
    EXPECT_TRUE(batch.statements[0].result.has_executable_comments);
    EXPECT_TRUE(batch.statements[1].result.has_executable_comments);
    EXPECT_FALSE(batch.statements[2].result.has_executable_comments);
    EXPECT_TRUE(batch.statements[3].result.has_executable_comments);
    EXPECT_FALSE(batch.statements[4].result.has_executable_comments);
}

TEST(ParseAll, SingleParseExecutableCommentProvenanceIncludesRemainder) {
    Parser<Dialect::MySQL> parser;
    for (const std::string sql : {"/*! lead */ SELECT 1", "SELECT 1 /*! + 1 */", "SELECT ) /*! trailing */", "SELECT 1 /*! @x */"}) {
        EXPECT_TRUE(parser.parse(sql.data(), sql.size()).has_executable_comments) << sql;
    }
    for (const std::string sql : {"SELECT '/*! x */', `/*M! x */`", "SELECT 1 /* ordinary */"}) {
        EXPECT_FALSE(parser.parse(sql.data(), sql.size()).has_executable_comments) << sql;
    }
    Parser<Dialect::PostgreSQL> pg;
    std::string sql = "SELECT 1 /* outer /*! nested */ end */";
    EXPECT_FALSE(pg.parse(sql.data(), sql.size()).has_executable_comments);
}

TEST(ParseAll, ExecutableCommentProvenanceSurvivesLexicalError) {
    Parser<Dialect::MySQL> parser;
    std::string sql = "SELECT 1 /*! @x */; SELECT 2; SELECT 3 /*M! unterminated";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_EQ(batch.statements.size(), 3u);
    EXPECT_EQ(batch.statements[0].result.status, ParseResult::ERROR);
    EXPECT_TRUE(batch.statements[0].result.has_executable_comments);
    EXPECT_FALSE(batch.statements[1].result.has_executable_comments);
    EXPECT_EQ(batch.statements[2].result.status, ParseResult::ERROR);
    EXPECT_TRUE(batch.statements[2].result.has_executable_comments);
}

TEST(ParseAll, ExecutableCommentOnlyFragmentsAreReported) {
    Parser<Dialect::MySQL> parser;
    for (const std::string comment : {"/*! SELECT ? */", "/*M! SELECT ? */", "/*! SET @a=1 */", "/*M! SET @a=1 */"}) {
        for (const std::string sql : {comment, comment + "; SELECT 1", "SELECT 1; " + comment}) {
            auto batch = parser.parse_all(sql.data(), sql.size());
            const bool leading_statement = sql.compare(0, 6, "SELECT") == 0;
            const size_t index = leading_statement ? 1 : 0;
            ASSERT_EQ(batch.statements.size(), sql == comment ? 1u : 2u) << sql;
            EXPECT_FALSE(batch.ok()) << sql;
            const auto& fragment = batch.statements[index];
            EXPECT_EQ(fragment.result.status, ParseResult::ERROR) << sql;
            EXPECT_TRUE(fragment.result.has_executable_comments) << sql;
            EXPECT_EQ(fragment.source.ptr, sql.data() + fragment.offset);
            const std::string source(fragment.source.ptr, fragment.source.len);
            EXPECT_EQ(fragment.offset, leading_statement ? 9u : 0u) << sql;
            EXPECT_EQ(source, leading_statement ? " " + comment :
                (sql == comment ? comment : comment + ";")) << sql;
            EXPECT_EQ(fragment.result.has_user_variables, comment.find('@') != std::string::npos) << sql;
            if (batch.statements.size() == 2)
                EXPECT_FALSE(batch.statements[1 - index].result.has_executable_comments) << sql;
        }
    }
}

TEST(ParseAll, OrdinaryCommentOnlyFragmentsRemainIgnored) {
    Parser<Dialect::MySQL> parser;
    const std::string sql = "/* ordinary */; SELECT 1; /* trailing */";
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_EQ(batch.statements.size(), 1u);
    EXPECT_TRUE(batch.ok());
    EXPECT_FALSE(batch.statements[0].result.has_executable_comments);
}
