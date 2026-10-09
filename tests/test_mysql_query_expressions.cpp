#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string emit_query(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    auto sql = emitter.result();
    return std::string(sql.ptr, sql.len);
}

void check_query(const char* sql, const char* expected = nullptr) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(result.stmt_type, StmtType::SELECT);
    auto emitted = emit_query(result.ast, parser.arena());
    EXPECT_EQ(emitted, expected ? expected : sql);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
    Parser<Dialect::MySQL> again;
    auto reparsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    EXPECT_EQ(emit_query(reparsed.ast, again.arena()), emitted);
}

void reject_query(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}

TEST(MySQLQueryExpressions, TableQueriesRetainNamesAndTails) {
    for (const char* sql : {"TABLE t", "TABLE db.t", "TABLE `db`.`select`",
         "TABLE db.select", "TABLE t ORDER BY id DESC LIMIT 2 OFFSET 1",
         "(TABLE t)", "TABLE t FOR UPDATE NOWAIT"}) check_query(sql);
    check_query("TABLE t LIMIT 1, 2", "TABLE t LIMIT 2 OFFSET 1");
}

TEST(MySQLQueryExpressions, ValuesQueriesRetainExplicitRowsAndTails) {
    for (const char* sql : {"VALUES ROW(1, 'a'), ROW(2, 'b')",
         "VALUES ROW(1 + 2, NULL, TRUE)",
         "VALUES ROW(2), ROW(1) ORDER BY column_0 LIMIT 1",
         "(VALUES ROW(1))", "VALUES ROW()", "VALUES ROW(DEFAULT)",
         "VALUES ROW(1) FOR UPDATE", "TABLE t LOCK IN SHARE MODE"}) check_query(sql);
    check_query("VALUES ROW(1), ROW(2) LIMIT 0, 1", "VALUES ROW(1), ROW(2) LIMIT 1 OFFSET 0");
}

TEST(MySQLQueryExpressions, CompoundQueriesKeepPrecedenceAndGrouping) {
    for (const char* sql : {"TABLE t UNION ALL VALUES ROW(1, 2)",
         "VALUES ROW(1) UNION SELECT 2 INTERSECT TABLE u",
         "TABLE t EXCEPT ALL TABLE u ORDER BY id LIMIT 2",
         "(VALUES ROW(1) UNION VALUES ROW(2)) INTERSECT VALUES ROW(2)",
         "TABLE t UNION (TABLE u ORDER BY id LIMIT 1)"}) check_query(sql);
}

TEST(MySQLQueryExpressions, DerivedAndLateralSourcesRetainQueryBodies) {
    for (const char* sql : {"SELECT * FROM (TABLE t) AS d",
         "SELECT * FROM (VALUES ROW(1, 2)) AS d(x, y)",
         "SELECT * FROM LATERAL (TABLE t) AS d",
         "SELECT * FROM t, LATERAL (VALUES ROW(t.id)) AS d(n)",
         "SELECT * FROM LATERAL ((VALUES ROW(1))) AS d(n)"}) check_query(sql);
}

TEST(MySQLQueryExpressions, ExpressionSubqueriesRetainTableAndValuesQueries) {
    for (const char* sql : {"SELECT (VALUES ROW(1))",
         "SELECT * FROM t WHERE id IN (VALUES ROW(1), ROW(2))",
         "SELECT * FROM t WHERE EXISTS (TABLE u)",
         "SELECT (TABLE u LIMIT 1)"}) check_query(sql);
}

TEST(MySQLQueryExpressions, RejectIncompleteOrDialectSpecificForms) {
    for (const char* sql : {"TABLE", "TABLE 1", "TABLE 't'", "TABLE PRIMARY",
         "TABLE db.", "TABLE a.b.c", "TABLE ONLY t", "TABLE t *",
         "TABLE t AS a", "TABLE t PARTITION (p0)", "TABLE t USE INDEX (idx)",
         "TABLE t WHERE id = 1", "TABLE t ORDER id", "TABLE t ORDER BY",
         "TABLE t LIMIT", "TABLE t UNION", "TABLE t UNION SELECT",
         "VALUES", "VALUES (1)", "VALUES ROW 1", "VALUES ROW(1,)",
         "VALUES ROW(,1)", "VALUES ROW(1),", "VALUES ROW(1), (2)",
         "VALUES ROW(*)", "VALUES ROW(1 +)", "VALUES ROW(1) ORDER BY",
         "VALUES ROW(1) FOR garbage", "SELECT * FROM (TABLE t)",
         "SELECT * FROM LATERAL (VALUES ROW(1))"}) reject_query(sql);
}

TEST(MySQLQueryExpressions, ParameterizationPreservesRowSyntaxAndOrderConstants) {
    const char* sql = "VALUES ROW(7, 'a'), ROW(8, 'b') ORDER BY 1";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    Arena output;
    auto parameters = parameterize_ast<Dialect::MySQL>(result, output);
    ASSERT_TRUE(parameters.ok());
    EXPECT_EQ(parameters.parameters.size(), 4u);
    EXPECT_EQ(emit_query(parameters.ast, output), "VALUES ROW(?, ?), ROW(?, ?) ORDER BY 1");
    EXPECT_EQ(emit_query(result.ast, parser.arena()), sql);
}

TEST(MySQLQueryExpressions, ExistingInsertValuesFunctionIsNotAQueryStart) {
    const char* sql = "INSERT INTO t (id) VALUES (1) ON DUPLICATE KEY UPDATE id = (VALUES(id))";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    EXPECT_EQ(emit_query(result.ast, parser.arena()), sql);
}

TEST(MySQLQueryExpressions, LimitsAndValueOperandsRequireMySQLSyntax) {
    for (const char* sql : {"TABLE t LIMIT ? OFFSET ?", "TABLE t LIMIT n", "VALUES ROW(1) LIMIT ?"})
        check_query(sql);
    for (const char* sql : {"TABLE t LIMIT -1", "TABLE t LIMIT +1", "TABLE t LIMIT 1 + 2",
         "TABLE t LIMIT 1.5", "TABLE t LIMIT NULL", "TABLE t LIMIT '1'",
         "VALUES ROW(1) LIMIT 1, -1", "VALUES ROW(1) LIMIT 2 OFFSET 1 + 1",
         "TABLE t LIMIT 18446744073709551616",
         "VALUES ROW(t.*)", "VALUES ROW(db.t.*)",
         "TABLE t LOCK IN SHARE", "TABLE t LOCK SHARE MODE"}) reject_query(sql);
}

TEST(MySQLQueryExpressions, NestedQueryParenthesesNeverDropBodies) {
    for (const char* sql : {"SELECT EXISTS ((VALUES ROW(1)))",
         "SELECT * FROM ((VALUES ROW(1))) AS d", "SELECT * FROM ((TABLE t)) AS d",
         "SELECT (WITH c AS (VALUES ROW(1)) TABLE c)"}) check_query(sql);
}

TEST(MySQLQueryExpressions, ScalarSubqueriesDoNotTurnTheirEnclosingExpressionIntoAQuery) {
    for (const char* sql : {"SELECT ((SELECT 1) + 2)", "SELECT 1 IN ((SELECT 1), 2)",
         "SELECT ((SELECT 1) IS NULL)", "SELECT 1 ORDER BY ((SELECT 1) + 2)",
         "SELECT (((SELECT 1) + 2))", "SELECT 1 IN (((SELECT 1) + 2))",
         "SELECT ((((SELECT 1) + 2)))", "SELECT 1 IN (((SELECT 1)), 2)",
         "SELECT EXISTS (((SELECT 1) UNION SELECT (1) + 2))"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_EQ(emit_query(result.ast, parser.arena()), sql);
    }
}

TEST(MySQLQueryExpressions, DefaultAndStarsAreNotArbitraryValueExpressions) {
    check_query("VALUES ROW(t.`*`, t.default, `DEFAULT`)");
    check_query("VALUES ROW(COUNT(*))");
    check_query("SELECT 1 LOCK IN SHARE MODE");
    for (const char* sql : {"VALUES ROW(DEFAULT + 1)", "VALUES ROW((DEFAULT))",
         "VALUES ROW(-DEFAULT)", "VALUES ROW(1 + DEFAULT)",
         "VALUES ROW(COALESCE(DEFAULT, 1))", "VALUES ROW(t.* + 1)",
         "VALUES ROW(1) ORDER BY *", "VALUES ROW(1) ORDER BY t.*"}) reject_query(sql);
}

TEST(MySQLQueryExpressions, CteQueryBodiesAndColumnNamesStayStructured) {
    for (const char* sql : {"WITH c AS (TABLE t) TABLE c",
         "WITH c(x) AS (VALUES ROW(1), ROW(2)) TABLE c",
         "WITH `c`(`x`) AS (VALUES ROW(1)) SELECT * FROM `c`",
         "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) TABLE c",
         "SELECT * FROM (WITH c AS (TABLE t) TABLE c) AS d"}) check_query(sql);
    for (const char* sql : {"WITH c AS (TABLE t)", "WITH c (TABLE t) TABLE c",
         "WITH c AS TABLE t TABLE c", "WITH c() AS (TABLE t) TABLE c",
         "WITH c(x,) AS (TABLE t) TABLE c", "WITH c(1) AS (TABLE t) TABLE c",
         "WITH PRIMARY AS (TABLE t) TABLE c", "WITH c AS (VALUES ROW(1 +)) TABLE c"}) reject_query(sql);
}
