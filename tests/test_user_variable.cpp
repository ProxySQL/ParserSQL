#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/user_variable.h"

#include <cstring>

using namespace sql_parser;

namespace {
UserVariableUsage classify(const char* sql) {
    Parser<Dialect::MySQL> parser;
    ParseResult result = parser.parse(sql, std::strlen(sql));
    return classify_mysql_user_variable_usage(result);
}
}

TEST(MySQLUserVariableUsage, IgnoresStringsAndComments) {
    EXPECT_EQ(classify("SELECT 1"), UserVariableUsage::NO_USER_VARIABLE);
    EXPECT_EQ(classify("SELECT '@x'"), UserVariableUsage::NO_USER_VARIABLE);
    EXPECT_EQ(classify("SELECT 1 /* @x */"), UserVariableUsage::NO_USER_VARIABLE);
}

TEST(MySQLUserVariableUsage, AllowsDirectReadsAndPredicates) {
    EXPECT_EQ(classify("SELECT @x"), UserVariableUsage::READ_ONLY);
    EXPECT_EQ(classify("SELECT -@x + 1 WHERE @y = 2 AND @z IS NOT NULL"),
              UserVariableUsage::READ_ONLY);
}

TEST(MySQLUserVariableUsage, RejectsWritesAndIntoTargets) {
    EXPECT_EQ(classify("SET @x=1"), UserVariableUsage::UNSAFE_OR_UNKNOWN);
    EXPECT_EQ(classify("SELECT @x:=1"), UserVariableUsage::UNSAFE_OR_UNKNOWN);
    EXPECT_EQ(classify("SELECT id INTO @x FROM test.uv_source"),
              UserVariableUsage::UNSAFE_OR_UNKNOWN);
}

TEST(MySQLUserVariableUsage, RejectsSensitiveOrUnknownContexts) {
    const char* cases[] = {
        "SELECT COALESCE(@x, 1)",
        "CALL p(@x)",
        "DO @x",
        "SELECT ? + @x",
        "SELECT (@x IN (SELECT id FROM t))",
        "SELECT @x FROM (SELECT 1) AS s",
    };
    for (const char* sql : cases) {
        SCOPED_TRACE(sql);
        EXPECT_EQ(classify(sql), UserVariableUsage::UNSAFE_OR_UNKNOWN);
    }
}

TEST(MySQLUserVariableUsage, RejectsMalformedAndIncompleteParses) {
    const char* cases[] = {
        "SELECT @", "SELECT @x trailing extra", "SELECT @x; SELECT 1",
        "SET @x=", "SELECT @'unterminated",
    };
    for (const char* sql : cases) {
        SCOPED_TRACE(sql);
        EXPECT_EQ(classify(sql), UserVariableUsage::UNSAFE_OR_UNKNOWN);
    }
}

TEST(MySQLUserVariableUsage, HandlesMySQLCommentBoundariesConservatively) {
    EXPECT_EQ(classify("SELECT 1--@x"), UserVariableUsage::READ_ONLY);
    EXPECT_EQ(classify("/*!40101 SET @x=1 */"),
              UserVariableUsage::UNSAFE_OR_UNKNOWN);
    EXPECT_EQ(classify("/*M!100100 SET @x=1 */"),
              UserVariableUsage::UNSAFE_OR_UNKNOWN);
    EXPECT_EQ(classify("/*M! SET @x=1 */"),
              UserVariableUsage::UNSAFE_OR_UNKNOWN);
    EXPECT_EQ(classify("SELECT @x /* unterminated"),
              UserVariableUsage::UNSAFE_OR_UNKNOWN);
    EXPECT_EQ(classify("SET @x=1 /* unterminated"),
              UserVariableUsage::UNSAFE_OR_UNKNOWN);
}

TEST(MySQLUserVariableUsage, UnterminatedBlockCommentIsNotFullInput) {
    Parser<Dialect::MySQL> parser;
    const char* sql = "SET @x=1 /* unterminated";
    ParseResult result = parser.parse(sql, std::strlen(sql));
    EXPECT_EQ(result.status, ParseResult::ERROR);
    EXPECT_FALSE(result.full_input);
}
