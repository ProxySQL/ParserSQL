#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include <cstring>
#include <string>

using namespace sql_parser;

TEST(PR67Ddl, ProcedureNamesRemainIdentifiersBeforeParameters) {
    // MySQL's native lexer treats adjacent COUNT(, SUM( and NOW( as functions.
    for (const char* sql : {
         "CREATE PROCEDURE count () SELECT 1",
         "CREATE PROCEDURE sum () SELECT 1",
         "CREATE PROCEDURE now () SELECT 1",
         "CREATE PROCEDURE `count` () SELECT 1"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input && parsed.ast);
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(parsed.ast);
        const auto output = emitter.result();
        EXPECT_EQ(std::string(output.ptr, output.len), sql);
        Parser<Dialect::MySQL> second;
        auto reparsed = second.parse(output.ptr, output.len);
        EXPECT_TRUE(reparsed.ok() && reparsed.full_input && reparsed.ast);
    }
}

TEST(PR67Ddl, RestrictedDefaultsRejectDefaultKeywordOperands) {
    // gram.y uses b_expr for CREATE column/domain defaults, which excludes DEFAULT.
    for (const char* sql : {
         "CREATE TABLE t (id int DEFAULT DEFAULT)",
         "CREATE TABLE t (id int DEFAULT -DEFAULT)",
         "CREATE TABLE t (id int DEFAULT 1 + DEFAULT)",
         "CREATE TABLE t (id int DEFAULT DEFAULT::int)",
         "CREATE DOMAIN d AS int DEFAULT DEFAULT NOT NULL CHECK (VALUE > 0)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(parsed.ok() && parsed.full_input && parsed.ast);
    }
}

TEST(PR67Ddl, DefaultsPreserveIndependentFullExpressionContexts) {
    // PostgreSQL permits DEFAULT in a_expr and rejects inappropriate uses during
    // semantic analysis. Parentheses, functions and ALTER defaults use a_expr.
    for (const char* sql : {
         "CREATE TABLE t (id int DEFAULT \"default\")",
         "CREATE TABLE t (id int DEFAULT t.default)",
         "CREATE TABLE t (ok boolean DEFAULT (1 IS NULL))",
         "CREATE TABLE t (id int DEFAULT (DEFAULT))",
         "CREATE TABLE t (id int DEFAULT coalesce(DEFAULT, 1))",
         "CREATE TABLE t (id int DEFAULT CAST(DEFAULT AS int))",
         "CREATE TABLE t (ok boolean DEFAULT CAST(1 IS NULL AS boolean))",
         "CREATE TABLE t (id int DEFAULT (DEFAULT)::int)",
         "CREATE TABLE t (id int DEFAULT (SELECT DEFAULT))",
         "ALTER TABLE t ALTER COLUMN id SET DEFAULT DEFAULT",
         "ALTER DOMAIN d SET DEFAULT DEFAULT"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input && parsed.ast);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(parsed.ast);
        const auto output = emitter.result();
        Parser<Dialect::PostgreSQL> second;
        auto reparsed = second.parse(output.ptr, output.len);
        EXPECT_TRUE(reparsed.ok() && reparsed.full_input && reparsed.ast);
    }
}
