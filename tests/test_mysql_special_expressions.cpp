#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string emit_special(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    auto sql = emitter.result();
    return std::string(sql.ptr, sql.len);
}

void check_special(const char* sql, const char* expected = nullptr, bool guarded = true) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    const auto emitted = emit_special(result.ast, parser.arena());
    EXPECT_EQ(emitted, expected ? expected : sql);
    if (guarded) {
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
        Arena output;
        EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, output).ok());
    }
    Parser<Dialect::MySQL> again;
    const auto reparsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    EXPECT_EQ(emit_special(reparsed.ast, again.arena()), emitted);
}

void reject_special(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}

TEST(MySQLSpecialExpressions, ExtractRetainsAllIntervalUnits) {
    for (const char* unit : {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE",
         "SECOND", "MICROSECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE", "DAY_SECOND",
         "DAY_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "HOUR_MICROSECOND",
         "MINUTE_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND"}) {
        auto sql = std::string("SELECT EXTRACT(") + unit + " FROM d) FROM t";
        check_special(sql.c_str());
    }
    check_special("SELECT EXTRACT(YEAR FROM COALESCE(d, '2026-01-01')) FROM t");
    check_special("SELECT EXTRACT(MONTH FROM (SELECT d FROM t LIMIT 1))");
}

TEST(MySQLSpecialExpressions, ExtractRejectsInvalidFieldsAndOperands) {
    for (const char* sql : {"SELECT EXTRACT()", "SELECT EXTRACT(YEAR)",
         "SELECT EXTRACT(YEAR d)", "SELECT EXTRACT(YEAR FROM)",
         "SELECT EXTRACT(YEAR FROM d +)", "SELECT EXTRACT(YEAR FROM *)",
         "SELECT EXTRACT(YEAR FROM t.*)", "SELECT EXTRACT(YEAR FROM DEFAULT)",
         "SELECT EXTRACT('YEAR' FROM d)", "SELECT EXTRACT(`YEAR` FROM d)",
         "SELECT EXTRACT(EPOCH FROM d)", "SELECT EXTRACT(TIMEZONE FROM d)",
         "SELECT EXTRACT (YEAR FROM d)", "SELECT EXTRACT(YEAR, d)", "SELECT EXTRACT(YEAR FROM d, 1)"}) reject_special(sql);
}

TEST(MySQLSpecialExpressions, SubstringKeywordFormsRetainOperandsAndAlias) {
    for (const char* sql : {"SELECT SUBSTRING(n FROM 2 FOR 3) FROM t",
         "SELECT SUBSTRING(n FROM -2) FROM t", "SELECT MID(n FROM 2) FROM t", "SELECT SUBSTR(n FROM 2 FOR 3) FROM t",
         "SELECT SUBSTRING(CONCAT(n, 'x') FROM 1 + 1 FOR LENGTH(n) - 1) FROM t",
         "SELECT SUBSTRING((SELECT n FROM t LIMIT 1) FROM 2)",
         "SELECT SUBSTRING(n FROM (1 IN (1, 2)) FOR 2) FROM t"}) check_special(sql);
}

TEST(MySQLSpecialExpressions, SubstringCommaFormsKeepExistingCalls) {
    for (const char* sql : {"SELECT SUBSTRING(n, 2) FROM t", "SELECT SUBSTRING(n, 2, 3) FROM t",
         "SELECT SUBSTR(n, -2, 1) FROM t"}) check_special(sql, nullptr, false);
}

TEST(MySQLSpecialExpressions, SubstringRejectsWrongArityAndPostgresOnlySyntax) {
    for (const char* sql : {"SELECT SUBSTRING()", "SELECT SUBSTRING(n)",
         "SELECT SUBSTRING(n, 1, 2, 3)", "SELECT SUBSTRING(n, 1,)",
         "SELECT SUBSTRING(n FROM)", "SELECT SUBSTRING(n FROM 1 FOR)",
         "SELECT SUBSTRING(n FOR 2)", "SELECT SUBSTRING(n FOR 2 FROM 1)",
         "SELECT SUBSTRING(n SIMILAR 'x' ESCAPE '#')", "SELECT SUBSTRING(* FROM 1)",
         "SELECT SUBSTRING(t.* FROM 1)", "SELECT SUBSTRING(n FROM DEFAULT)",
         "SELECT SUBSTRING(n FROM 1 +)", "SELECT SUBSTRING(n, 1 FOR 2)",
         "SELECT SUBSTRING(n FROM 1, 2)", "SELECT SUBSTRING (n FROM 2)",
         "SELECT SUBSTR (n FROM 2)", "SELECT MID (n FROM 2)"}) reject_special(sql);
}

TEST(MySQLSpecialExpressions, MatchRetainsEverySearchMode) {
    for (const char* sql : {"SELECT MATCH(body) AGAINST ('mysql') FROM t",
         "SELECT MATCH(body) AGAINST ('mysql' IN BOOLEAN MODE) FROM t",
         "SELECT MATCH(body) AGAINST ('mysql' IN NATURAL LANGUAGE MODE) FROM t",
         "SELECT MATCH(body) AGAINST ('mysql' WITH QUERY EXPANSION) FROM t",
         "SELECT MATCH(body) AGAINST ('mysql' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION) FROM t"})
        check_special(sql);
}

TEST(MySQLSpecialExpressions, MatchRetainsColumnListsAndQualification) {
    check_special("SELECT MATCH(t.body, t.n) AGAINST ('mysql') FROM t");
    check_special("SELECT MATCH(`db`.`t`.`body`) AGAINST (?) FROM `db`.`t`");
    check_special("SELECT MATCH body, n AGAINST ('mysql') FROM t",
                  "SELECT MATCH(body, n) AGAINST ('mysql') FROM t");
}

TEST(MySQLSpecialExpressions, MatchSeparatesSearchOperandFromMode) {
    for (const char* sql : {"SELECT MATCH(body) AGAINST (CONCAT('my', 'sql') IN BOOLEAN MODE) FROM t",
         "SELECT MATCH(body) AGAINST (1 | 2 IN BOOLEAN MODE) FROM t",
         "SELECT MATCH(body) AGAINST (('x' IN ('x', 'y')) IN BOOLEAN MODE) FROM t",
         "SELECT MATCH(body) AGAINST ('mysql') > 0 FROM t",
         "SELECT * FROM t WHERE MATCH(body) AGAINST ('mysql' IN BOOLEAN MODE) AND id > 0"})
        check_special(sql);
}

TEST(MySQLSpecialExpressions, MatchRejectsMalformedTargetsAndModes) {
    for (const char* sql : {"SELECT MATCH() AGAINST ('x')", "SELECT MATCH(body)",
         "SELECT MATCH(body,) AGAINST ('x')", "SELECT MATCH(*) AGAINST ('x')",
         "SELECT MATCH(t.*) AGAINST ('x')", "SELECT MATCH(1) AGAINST ('x')",
         "SELECT MATCH(body + 1) AGAINST ('x')", "SELECT MATCH(a.b.c.d) AGAINST ('x')",
         "SELECT MATCH(body) AGAINST ()", "SELECT MATCH(body) AGAINST ('x' IN BOOLEAN)",
         "SELECT MATCH(body) AGAINST ('x' IN NATURAL MODE)",
         "SELECT MATCH(body) AGAINST ('x' WITH EXPANSION)",
         "SELECT MATCH(body) AGAINST ('x' IN BOOLEAN MODE WITH QUERY EXPANSION)",
         "SELECT MATCH(body) AGAINST ('x' IN UNKNOWN MODE)",
         "SELECT MATCH(body) AGAINST ('x' OR 'y')", "SELECT MATCH(body) AGAINST (*)",
         "SELECT MATCH(body) AGAINST (DEFAULT)"}) reject_special(sql);
}

TEST(MySQLSpecialExpressions, DmlOperandsKeepUnsupportedSemanticsGuarded) {
    for (const char* sql : {"UPDATE t SET n = EXTRACT(YEAR FROM d)",
         "DELETE FROM t WHERE MATCH(body) AGAINST ('x')",
         "INSERT INTO t (n) VALUES (SUBSTRING('abc' FROM 2))"}) check_special(sql);
}

TEST(MySQLSpecialExpressions, CloneOwnsSpecialSyntaxAndTraversesOperands) {
    Arena output;
    AstNode* copy = nullptr;
    {
        std::string sql = "SELECT EXTRACT(YEAR FROM d), SUBSTR(body FROM 2 FOR 3), MATCH(body) AGAINST ('mysql' IN BOOLEAN MODE) FROM t";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        unsigned references = 0;
        auto walked = walk_ast(result.ast, [&](const AstNode& node, const AstVisitContext&) {
            if (node.type == NodeType::NODE_COLUMN_REF) ++references;
            return AstVisitAction::Continue;
        });
        EXPECT_EQ(walked.status, AstWalkStatus::Completed);
        EXPECT_EQ(references, 3u);
        auto cloned = clone_ast(result.ast, output);
        ASSERT_TRUE(cloned.ok());
        copy = cloned.ast;
        parser.reset();
        sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit_special(copy, output), "SELECT EXTRACT(YEAR FROM d), SUBSTR(body FROM 2 FOR 3), MATCH(body) AGAINST ('mysql' IN BOOLEAN MODE) FROM t");
}

TEST(MySQLSpecialExpressions, RejectsIncompleteGroupedAndSubqueryOperands) {
    for (const char* sql : {"SELECT EXTRACT(YEAR FROM ())", "SELECT SUBSTRING(() FROM 1)",
         "SELECT MATCH(body) AGAINST (() IN BOOLEAN MODE)",
         "SELECT EXTRACT(YEAR FROM EXISTS())", "SELECT SUBSTRING(EXISTS(1) FROM 1)",
         "SELECT MATCH(body) AGAINST (EXISTS() IN BOOLEAN MODE)",
         "SELECT EXTRACT(YEAR FROM (1,))", "SELECT SUBSTRING((1,2 FROM 1)",
         "SELECT SUBSTRING((SELECT 1 FROM 1)", "SELECT EXTRACT(YEAR FROM ROW())",
         "SELECT EXTRACT(YEAR FROM ROW(1))", "SELECT SUBSTRING(ROW(1,2 FROM 1)"}) reject_special(sql);
    check_special("SELECT EXTRACT(YEAR FROM (1, 2))"); // grammar-valid; native type checking is separate
    check_special("SELECT EXTRACT(YEAR FROM EXISTS (SELECT 1))");
}

TEST(MySQLSpecialExpressions, CaseOperandsRequireAllBranchesAndDelimiters) {
    for (const char* sql : {"SELECT EXTRACT(YEAR FROM CASE WHEN 1 THEN 2)",
         "SELECT SUBSTRING(n FROM CASE WHEN 1 2 END)",
         "SELECT MATCH(body) AGAINST (CASE WHEN THEN 'x' END)",
         "SELECT EXTRACT(YEAR FROM CASE 1 END)",
         "SELECT EXTRACT(YEAR FROM CASE WHEN 1 THEN ELSE 2 END)",
         "SELECT EXTRACT(YEAR FROM CASE WHEN 1 THEN 2 ELSE END)"}) reject_special(sql);
    check_special("SELECT EXTRACT(YEAR FROM CASE WHEN id = 1 THEN d ELSE NULL END) FROM t");
    check_special("SELECT SUBSTRING(n FROM CASE id WHEN 1 THEN 2 ELSE 3 END) FROM t");
}
