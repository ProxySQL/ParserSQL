#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string mysql_emit(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    auto output = emitter.result();
    return std::string(output.ptr, output.len);
}
}

TEST(MySQLGrammar, JsonArrowsPreserveExtractionAndUnquoting) {
    for (const char* sql : {
         "SELECT j -> '$.name', j ->> '$.name' FROM t",
         "SELECT JSON_TYPE(t.j -> '$[0]') FROM t",
         "SELECT -j ->> '$.n' + 1 FROM t WHERE j -> '$.n' = 2",
         "SELECT `t`.`j` ->> '$.name' FROM t",
         "UPDATE t SET n = j ->> '$.n' WHERE j -> '$.n' = 1"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        Arena destination;
        EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, destination).ok());
        std::string emitted = mysql_emit(result.ast, parser.arena());
        Parser<Dialect::MySQL> again;
        auto reparsed = again.parse(emitted.data(), emitted.size());
        ASSERT_TRUE(reparsed.ok());
        ASSERT_TRUE(reparsed.full_input);
        EXPECT_EQ(mysql_emit(reparsed.ast, again.arena()), emitted);
    }
}

TEST(MySQLGrammar, JsonArrowsRequireColumnAndLiteralPath) {
    for (const char* sql : {"SELECT j ->", "SELECT j ->>", "SELECT j -> 1",
         "SELECT j -> ?", "SELECT j -> p", "SELECT 1 -> '$.x'",
         "SELECT f(j) -> '$.x'", "SELECT (j) -> '$.x'",
         "SELECT j -> '$.x' -> '$.y'", "SELECT j ->>> '$.x'"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLGrammar, MinusAndShiftTokenizationUnaffected) {
    const char* sql = "SELECT a - 1, b >> 2, c > 3 FROM t";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
    Tokenizer<Dialect::MySQL> tokenizer;
    const char input[] = {'j', '-', '>'};
    tokenizer.reset(input, sizeof(input));
    EXPECT_EQ(tokenizer.next_token().type, TokenType::TK_IDENTIFIER);
    auto arrow = tokenizer.next_token();
    EXPECT_EQ(std::string(arrow.text.ptr, arrow.text.len), "->");
    EXPECT_EQ(tokenizer.next_token().type, TokenType::TK_EOF);
}

TEST(MySQLGrammar, NamedAndInheritedWindowsRoundTrip) {
    for (const char* sql : {
         "SELECT SUM(n) OVER w FROM t WINDOW w AS (PARTITION BY id ORDER BY n)",
         "SELECT NTILE(1) OVER w FROM t WINDOW w AS ()",
         "SELECT SUM(n) OVER (`base` ORDER BY n ROWS 1 PRECEDING) FROM t WINDOW `base` AS (PARTITION BY id)",
         "SELECT ROW_NUMBER() OVER w WINDOW w AS ()",
         "SELECT SUM(n) OVER w2 FROM t WINDOW w1 AS (PARTITION BY id), w2 AS (w1 ORDER BY n) ORDER BY id"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        std::string output = mysql_emit(result.ast, parser.arena());
        Parser<Dialect::MySQL> again;
        auto reparsed = again.parse(output.data(), output.size());
        EXPECT_TRUE(reparsed.ok() && reparsed.full_input);
    }
}

TEST(MySQLGrammar, NamedWindowsRequireCompleteDefinitions) {
    for (const char* sql : {"SELECT SUM(n) OVER w FROM t WINDOW w AS",
         "SELECT SUM(n) OVER w FROM t WINDOW w ()", "SELECT SUM(n) OVER w WINDOW w AS (ORDER n)",
         "SELECT SUM(n) OVER w WINDOW w AS (),", "SELECT SUM(n) OVER w WINDOW w AS (ROWS 1)",
         "SELECT SUM(n) OVER w WINDOW w AS (GROUPS 1 PRECEDING)",
         "SELECT SUM(n) OVER w WINDOW w AS (ROWS CURRENT ROW EXCLUDE TIES)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLGrammar, RejectEmptySelectTargets) {
    for (const char* sql : {"SELECT", "SELECT FROM", "SELECT FROM t", "SELECT 1, FROM t"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLGrammar, GroupConcatPreservesModifiersAndSeparator) {
    for (const char* sql : {
         "SELECT GROUP_CONCAT(DISTINCT n) FROM t",
         "SELECT GROUP_CONCAT(n, id ORDER BY n DESC, id ASC SEPARATOR ';') FROM t",
         "SELECT GROUP_CONCAT(n SEPARATOR '') FROM t",
         "SELECT GROUP_CONCAT(DISTINCT n ORDER BY n DESC SEPARATOR ';') FROM t GROUP BY id",
         "SELECT GROUP_CONCAT(n ORDER BY n) FROM t"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        Arena destination;
        EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, destination).ok());
        std::string output = mysql_emit(result.ast, parser.arena());
        Parser<Dialect::MySQL> again;
        auto reparsed = again.parse(output.data(), output.size());
        EXPECT_TRUE(reparsed.ok() && reparsed.full_input);
    }
}

TEST(MySQLGrammar, GroupConcatRejectsMissingOrMisplacedOperands) {
    for (const char* sql : {"SELECT GROUP_CONCAT()", "SELECT GROUP_CONCAT(DISTINCT)",
         "SELECT GROUP_CONCAT(*)", "SELECT GROUP_CONCAT(n,)",
         "SELECT GROUP_CONCAT(n ORDER n)", "SELECT GROUP_CONCAT(n ORDER BY)",
         "SELECT GROUP_CONCAT(n ORDER BY n NULLS FIRST)",
         "SELECT GROUP_CONCAT(n SEPARATOR 1)", "SELECT GROUP_CONCAT(n SEPARATOR)",
         "SELECT GROUP_CONCAT(n SEPARATOR ';' ORDER BY n)",
         "SELECT GROUP_CONCAT(n +)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLGrammar, GroupByWithRollupPreservesGroupingSemantics) {
    const char* sql = "SELECT id, SUM(n) FROM t GROUP BY id WITH ROLLUP HAVING SUM(n) > 1 ORDER BY id";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    for (const char* invalid : {"SELECT n FROM t GROUP BY WITH ROLLUP",
         "SELECT n FROM t GROUP BY n, WITH ROLLUP", "SELECT n FROM t GROUP BY n WITH",
         "SELECT n FROM t GROUP BY n WITH CUBE"}) {
        SCOPED_TRACE(invalid);
        auto bad = parser.parse(invalid, std::strlen(invalid));
        EXPECT_FALSE(bad.ok() && bad.full_input);
    }
}

TEST(MySQLGrammar, JsonArrowsAcceptDatabaseQualifiedColumns) {
    const char* sql = "SELECT db.t.j -> '$.x', `db`.`t`.`j` ->> '$.x' FROM db.t";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
}

TEST(MySQLGrammar, JsonPathAndGroupConcatSeparatorPreserveStringDelimiters) {
    for (const char* sql : {
         R"(SELECT j -> "$.'x'" FROM t)",
         R"(SELECT GROUP_CONCAT(n SEPARATOR "it's") FROM t)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
        std::string output = mysql_emit(result.ast, parser.arena());
        Parser<Dialect::MySQL> again;
        auto reparsed = again.parse(output.data(), output.size());
        EXPECT_TRUE(reparsed.ok() && reparsed.full_input);
    }
}

TEST(MySQLGrammar, ReviewedMySQLSyntaxUsesNativeOperandRules) {
    for (const char* sql : {"SELECT GROUP_CONCAT(1 SEPARATOR X'2C')",
         "SELECT GROUP_CONCAT(1 SEPARATOR 0x2C)", "SELECT GROUP_CONCAT(1 SEPARATOR b'00101100')",
         "SELECT SUM(1) OVER format WINDOW format AS ()",
         "SELECT SUM(1) OVER (format) WINDOW format AS ()"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_TRUE(result.ok());
        EXPECT_TRUE(result.full_input);
        EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
    }
    for (const char* sql : {"SELECT SUM(1) OVER w WINDOW w AS (ORDER BY 1 NULLS FIRST)",
         "SELECT GROUP_CONCAT(t.*) FROM t", "SELECT j.+ -> '$.a' FROM t",
         "SELECT j.1 -> '$.a' FROM t", "SELECT t.* -> '$.a' FROM t"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLGrammar, JsonArrowQuotedStarIsAColumn) {
    const char* sql = "SELECT t.`*` -> '$.a' FROM t";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(result.full_input);
    EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
}

TEST(MySQLGrammar, DatabaseQualifiedColumnsRequireRemoteExecution) {
    for (const char* sql : {"SELECT db.t.j FROM db.t", "SELECT n FROM t WHERE db.t.n = 1"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_EQ(mysql_emit(result.ast, parser.arena()), sql);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    }
}
