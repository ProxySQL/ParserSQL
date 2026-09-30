#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;
namespace {
std::string emit_mysql_explain(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena); emitter.emit(ast);
    auto out = emitter.result(); return std::string(out.ptr, out.len);
}
void check_mysql_explain(const char* sql, const char* expected = nullptr, bool into = true) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input); ASSERT_NE(result.ast, nullptr);
    auto emitted = emit_mysql_explain(result.ast, parser.arena());
    EXPECT_EQ(emitted, expected ? expected : sql);
    if (into) {
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        Arena bound; EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, bound).ok());
        EXPECT_EQ(classify_mysql_user_variable_usage(result), UserVariableUsage::UNSAFE_OR_UNKNOWN);
    }
    Parser<Dialect::MySQL> again;
    auto parsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(parsed.ok()); ASSERT_TRUE(parsed.full_input);
    EXPECT_EQ(emit_mysql_explain(parsed.ast, again.arena()), emitted);
}
void reject_mysql_explain(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}
TEST(MySQLExplainInto, PreservesOptionsAndDestinationSyntax) {
    for (const char* sql : {"EXPLAIN ANALYZE FORMAT = JSON INTO @plan SELECT 1",
         "EXPLAIN FORMAT = JSON INTO @plan SELECT 1",
         "EXPLAIN FORMAT = 'JSON' INTO @'query plan' SELECT 1",
         "EXPLAIN FORMAT = `JSON` INTO @`a``b` SELECT 1",
         "EXPLAIN FORMAT = JSON INTO @\"a.b\" SELECT 1",
         "EXPLAIN FORMAT = JSON INTO @a.b SELECT 1",
         "EXPLAIN FORMAT = JSON INTO @123 SELECT 1",
         "EXPLAIN FORMAT = JSON INTO @NULL SELECT 1"}) check_mysql_explain(sql);
}
TEST(MySQLExplainInto, RetainsGrammarBeforeServerSemanticValidation) {
    check_mysql_explain("EXPLAIN INTO @plan SELECT 1");
    check_mysql_explain("EXPLAIN FORMAT = TREE INTO @plan SELECT 1");
    check_mysql_explain("EXPLAIN ANALYZE INTO @plan SELECT 1");
}
TEST(MySQLExplainInto, QueryAndDmlBodiesRemainStructured) {
    for (const char* sql : {"EXPLAIN FORMAT = JSON INTO @plan WITH c AS (SELECT 1 AS id) SELECT id FROM c",
         "EXPLAIN FORMAT = JSON INTO @plan TABLE t",
         "EXPLAIN FORMAT = JSON INTO @plan VALUES ROW(1), ROW(2)",
         "EXPLAIN FORMAT = JSON INTO @plan (SELECT 1 UNION SELECT 2)",
         "EXPLAIN FORMAT = JSON INTO @plan INSERT INTO t VALUES (1)",
         "EXPLAIN FORMAT = JSON INTO @plan UPDATE t SET id = 1",
         "EXPLAIN FORMAT = JSON INTO @plan DELETE FROM t WHERE id = 1"}) check_mysql_explain(sql);
    check_mysql_explain("EXPLAIN WITH c AS (SELECT 1 AS id) SELECT id FROM c", nullptr, false);
    check_mysql_explain("EXPLAIN TABLE t", nullptr, false);
    check_mysql_explain("EXPLAIN (SELECT 1);", "EXPLAIN (SELECT 1)", false);
}
TEST(MySQLExplainInto, RejectsIncompleteOrMisorderedOptions) {
    for (const char* sql : {"EXPLAIN ANALYZE ANALYZE SELECT 1", "EXPLAIN FORMAT JSON SELECT 1",
         "EXPLAIN FORMAT =", "EXPLAIN FORMAT = 1 SELECT 1", "EXPLAIN FORMAT = NULL SELECT 1",
         "EXPLAIN FORMAT = JSON ANALYZE SELECT 1", "EXPLAIN VERBOSE SELECT 1",
         "EXPLAIN FORMAT = JSON FORMAT = TREE SELECT 1", "EXPLAIN INTO SELECT 1",
         "EXPLAIN INTO plan SELECT 1", "EXPLAIN INTO @@plan SELECT 1", "EXPLAIN INTO @ SELECT 1",
         "EXPLAIN INTO @plan", "EXPLAIN INTO @plan FORMAT = JSON SELECT 1",
         "EXPLAIN INTO @plan INTO @other SELECT 1", "EXPLAIN ANALYZE users",
         "EXPLAIN FORMAT = JSON users", "EXPLAIN CREATE TABLE t(id INT)",
         "EXPLAIN SET @a = 1", "EXPLAIN CALL p()", "EXPLAIN INTO @plan SELECT 1 +",
         "EXPLAIN INTO @plan INSERT INTO t VALUES (1,)"}) reject_mysql_explain(sql);
}
TEST(MySQLExplainInto, DescribeSynonymsAndQuotedTableNames) {
    check_mysql_explain("DESC FORMAT = JSON INTO @plan SELECT 1", "EXPLAIN FORMAT = JSON INTO @plan SELECT 1");
    check_mysql_explain("DESCRIBE ANALYZE SELECT 1", "EXPLAIN ANALYZE SELECT 1", false);
    check_mysql_explain("DESCRIBE `db`.`t` `select`", nullptr, false);
    check_mysql_explain("EXPLAIN `db`.`t` 'id%'", "DESCRIBE `db`.`t` 'id%'", false);
}
TEST(MySQLExplainInto, DestinationSurvivesClone) {
    Arena owned; AstNode* ast = nullptr;
    {
        std::string sql = "EXPLAIN FORMAT = JSON INTO @'a''b' SELECT 1";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input);
        auto clone = clone_ast(result.ast, owned); ASSERT_TRUE(clone.ok()); ast = clone.ast;
        parser.reset(); sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit_mysql_explain(ast, owned), "EXPLAIN FORMAT = JSON INTO @'a''b' SELECT 1");
}

TEST(MySQLExplainInto, FormatRemainsATableNameWithoutAnEqualsSign) {
    check_mysql_explain("EXPLAIN format", "DESCRIBE format", false);
    check_mysql_explain("EXPLAIN format.id", "DESCRIBE format.id", false);
    check_mysql_explain("EXPLAIN format id", "DESCRIBE format id", false);
    check_mysql_explain("EXPLAIN format 'id%'", "DESCRIBE format 'id%'", false);
    check_mysql_explain("DESC format", "DESCRIBE format", false);
}

TEST(MySQLExplainInto, DescriptionFiltersRetainHexAndBitLiteralSyntax) {
    check_mysql_explain("EXPLAIN t X'6964'", "DESCRIBE t X'6964'", false);
    check_mysql_explain("EXPLAIN t 0x6964", "DESCRIBE t 0x6964", false);
    check_mysql_explain("EXPLAIN t B'01101001'", "DESCRIBE t B'01101001'", false);
    check_mysql_explain("EXPLAIN t 0b01101001", "DESCRIBE t 0b01101001", false);
    reject_mysql_explain("EXPLAIN t X'1'");
    reject_mysql_explain("EXPLAIN t B'2'");
}
