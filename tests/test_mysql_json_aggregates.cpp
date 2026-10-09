#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string emit_json_aggregate(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    const auto sql = emitter.result();
    return std::string(sql.ptr, sql.len);
}

void check_json_aggregate(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    const auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    const auto emitted = emit_json_aggregate(result.ast, parser.arena());
    EXPECT_EQ(emitted, sql);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
    Arena output;
    EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, output).ok());
    Parser<Dialect::MySQL> again;
    const auto reparsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    EXPECT_EQ(emit_json_aggregate(reparsed.ast, again.arena()), emitted);
}

void reject_json_aggregate(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    const auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}

TEST(MySQLJsonAggregates, RejectsWrongArityAndNonValueOperands) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG()", "SELECT JSON_ARRAYAGG(1, 2)",
         "SELECT JSON_ARRAYAGG(1,)", "SELECT JSON_OBJECTAGG()", "SELECT JSON_OBJECTAGG(1)",
         "SELECT JSON_OBJECTAGG(1, 2, 3)", "SELECT JSON_OBJECTAGG(1,)",
         "SELECT JSON_ARRAYAGG(*)", "SELECT JSON_ARRAYAGG(t.*)", "SELECT JSON_ARRAYAGG(DEFAULT)",
         "SELECT JSON_OBJECTAGG(*, 1)", "SELECT JSON_OBJECTAGG(1, t.*)",
         "SELECT JSON_OBJECTAGG(1, DEFAULT)", "SELECT JSON_ARRAYAGG(DISTINCT 1)",
         "SELECT JSON_OBJECTAGG(DISTINCT 1, 2)", "SELECT JSON_OBJECTAGG(1, DISTINCT 2)",
         "SELECT JSON_ARRAYAGG(ALL)", "SELECT JSON_OBJECTAGG(1, ALL)",
         "SELECT JSON_ARRAYAGG(ALL ALL 1)"}) reject_json_aggregate(sql);
}

TEST(MySQLJsonAggregates, RejectsIncompleteOperandsAndDelimiters) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG(1 +)", "SELECT JSON_OBJECTAGG(1 +, 2)",
         "SELECT JSON_OBJECTAGG(1, 2 +)", "SELECT JSON_ARRAYAGG(1",
         "SELECT JSON_OBJECTAGG(1, 2", "SELECT JSON_ARRAYAGG(())",
         "SELECT JSON_ARRAYAGG((1,))", "SELECT JSON_ARRAYAGG(EXISTS())",
         "SELECT JSON_ARRAYAGG(ROW())", "SELECT JSON_ARRAYAGG(ROW(1))",
         "SELECT JSON_ARRAYAGG(CASE WHEN 1 THEN 2)",
         "SELECT JSON_OBJECTAGG(1, CASE WHEN THEN 2 END)"}) reject_json_aggregate(sql);
}

TEST(MySQLJsonAggregates, PreservesArgumentsAndOptionalAll) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG(n) FROM t", "SELECT json_arrayagg(n) FROM t",
         "SELECT JSON_OBJECTAGG(k, n) FROM t", "SELECT JSON_ARRAYAGG(COALESCE(n, 0)) FROM t",
         "SELECT JSON_OBJECTAGG(CONCAT(k, 'x'), CASE WHEN n > 0 THEN n ELSE NULL END) FROM t",
         "SELECT JSON_ARRAYAGG((SELECT 1))", "SELECT JSON_ARRAYAGG(EXISTS (SELECT 1))",
         "SELECT JSON_ARRAYAGG((1, 2))", "SELECT JSON_OBJECTAGG(`*`, `DEFAULT`) FROM t",
         "SELECT JSON_ARRAYAGG(ALL n) FROM t", "SELECT JSON_OBJECTAGG(ALL k, ALL n) FROM t",
         "SELECT JSON_OBJECTAGG(k, ALL n) FROM t"}) check_json_aggregate(sql);
}

TEST(MySQLJsonAggregates, PreservesNineSevenArrayNullClauses) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG(n NULL ON NULL) FROM t",
         "SELECT JSON_ARRAYAGG(n ABSENT ON NULL) FROM t", "SELECT JSON_ARRAYAGG(NULL NULL ON NULL)",
         "SELECT JSON_ARRAYAGG(NULL ABSENT ON NULL)", "SELECT JSON_ARRAYAGG(absent ABSENT ON NULL) FROM t",
         "SELECT JSON_ARRAYAGG(ALL n ABSENT ON NULL) FROM t"}) check_json_aggregate(sql);
}

TEST(MySQLJsonAggregates, RejectsInvalidNullAndAggregateClauses) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG(n NULL)", "SELECT JSON_ARRAYAGG(n ABSENT)",
         "SELECT JSON_ARRAYAGG(n NULL ON)", "SELECT JSON_ARRAYAGG(n ABSENT ON)",
         "SELECT JSON_ARRAYAGG(n NULL ON n)", "SELECT JSON_ARRAYAGG(n ABSENT ON n)",
         "SELECT JSON_ARRAYAGG(n ABSENT NULL)", "SELECT JSON_ARRAYAGG(n NULL NULL)",
         "SELECT JSON_ARRAYAGG(n NULL ON NULL ABSENT ON NULL)",
         "SELECT JSON_ARRAYAGG(n `ABSENT` ON NULL)",
         "SELECT JSON_OBJECTAGG(k, n NULL ON NULL)", "SELECT JSON_OBJECTAGG(k, n ABSENT ON NULL)",
         "SELECT JSON_ARRAYAGG(n ORDER BY n)", "SELECT JSON_OBJECTAGG(k, n ORDER BY n)",
         "SELECT JSON_ARRAYAGG(n RETURNING JSON)", "SELECT JSON_ARRAYAGG(n SEPARATOR ',')"})
        reject_json_aggregate(sql);
}

TEST(MySQLJsonAggregates, SupportsWindowDefinitionsAndReferences) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG(n) OVER () FROM t",
         "SELECT JSON_OBJECTAGG(k, n) OVER (PARTITION BY k ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
         "SELECT JSON_ARRAYAGG(n ABSENT ON NULL) OVER w FROM t WINDOW w AS (ORDER BY n)"})
        check_json_aggregate(sql);
    for (const char* sql : {"SELECT JSON_ARRAYAGG(n) OVER", "SELECT JSON_OBJECTAGG(k, n) OVER (",
         "SELECT JSON_ARRAYAGG(n) OVER (PARTITION BY)", "SELECT JSON_ARRAYAGG(n) OVER (ORDER BY)"})
        reject_json_aggregate(sql);
}

TEST(MySQLJsonAggregates, WhitespaceAndQuotedNamesKeepOrdinaryFunctionGrammar) {
    for (const char* sql : {"SELECT JSON_ARRAYAGG ()", "SELECT JSON_ARRAYAGG (1, 2)",
         "SELECT JSON_OBJECTAGG (1)", "SELECT JSON_ARRAYAGG/**/(1, 2)",
         "SELECT `JSON_ARRAYAGG`(1, 2)", "SELECT `JSON_OBJECTAGG`()"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        const auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        if (!std::strchr(sql, '`')) {
            EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
            EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
            Arena output;
            EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, output).ok());
        }
        const auto emitted = emit_json_aggregate(result.ast, parser.arena());
        EXPECT_EQ(emitted, std::strstr(sql, "/**/") ? "SELECT JSON_ARRAYAGG (1, 2)" : sql);
        Parser<Dialect::MySQL> again;
        const auto reparsed = again.parse(emitted.data(), emitted.size());
        EXPECT_TRUE(reparsed.ok());
        EXPECT_TRUE(reparsed.full_input);
    }
    reject_json_aggregate("SELECT JSON_ARRAYAGG (n ABSENT ON NULL)");
}

TEST(MySQLJsonAggregates, CloneOwnsSyntaxAndWalkVisitsEveryOperand) {
    Arena output;
    AstNode* copy = nullptr;
    {
        std::string sql = "SELECT JSON_OBJECTAGG(k, n), JSON_ARRAYAGG(n ABSENT ON NULL) FROM t";
        Parser<Dialect::MySQL> parser;
        const auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        unsigned references = 0;
        auto walked = walk_ast(result.ast, [&](const AstNode& node, const AstVisitContext&) {
            if (node.type == NodeType::NODE_COLUMN_REF) ++references;
            return AstVisitAction::Continue;
        });
        EXPECT_EQ(walked.status, AstWalkStatus::Completed);
        EXPECT_EQ(references, 3u);
        const auto cloned = clone_ast(result.ast, output);
        ASSERT_TRUE(cloned.ok());
        copy = cloned.ast;
        parser.reset();
        sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit_json_aggregate(copy, output),
              "SELECT JSON_OBJECTAGG(k, n), JSON_ARRAYAGG(n ABSENT ON NULL) FROM t");
}

TEST(MySQLJsonAggregates, LeavesPostgresSqlJsonGrammarUnchanged) {
    const char* sql = "SELECT JSON_ARRAYAGG(n ABSENT ON NULL), JSON_OBJECTAGG(k VALUE n) FROM t";
    Parser<Dialect::PostgreSQL> parser;
    const auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(result.full_input);
}
