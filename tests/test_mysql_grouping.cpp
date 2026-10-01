#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;
namespace {
std::string emit_grouping(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena); emitter.emit(ast);
    auto out = emitter.result(); return std::string(out.ptr, out.len);
}
void check_grouping(const char* sql, const char* expected = nullptr) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok()); ASSERT_TRUE(parsed.full_input); ASSERT_NE(parsed.ast, nullptr);
    auto emitted = emit_grouping(parsed.ast, parser.arena());
    EXPECT_EQ(emitted, expected ? expected : sql);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(parsed.ast));
    Arena bound;
    EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(parsed, bound).ok());
    Parser<Dialect::MySQL> again;
    auto result = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input);
    EXPECT_EQ(emit_grouping(result.ast, again.arena()), emitted);
}
void reject_grouping(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}
TEST(MySQLGrouping, GroupingSetsPreserveParenthesesAndEmptySets) {
    for (const char* sql : {"SELECT id, COUNT(*) FROM t GROUP BY GROUPING SETS ((id), ())",
         "SELECT id, n, COUNT(*) FROM t GROUP BY GROUPING SETS ((id, n), (id), (n), ())",
         "SELECT COUNT(*) FROM t GROUP BY GROUPING SETS (())",
         "SELECT id + 1, COUNT(*) FROM t GROUP BY GROUPING SETS ((id + 1), ()) HAVING COUNT(*) > 1"})
        check_grouping(sql);
}
TEST(MySQLGrouping, RollupAndCubeAreStructuredGroupingOperations) {
    check_grouping("SELECT id, n FROM t GROUP BY ROLLUP(id, n)", "SELECT id, n FROM t GROUP BY ROLLUP (id, n)");
    check_grouping("SELECT id, n FROM t GROUP BY CUBE(id, n)", "SELECT id, n FROM t GROUP BY CUBE (id, n)");
    check_grouping("SELECT id FROM t GROUP BY ROLLUP (id) ORDER BY id LIMIT 2");
}
TEST(MySQLGrouping, RejectsIncompleteOrNonNativeSetLists) {
    for (const char* sql : {"SELECT id FROM t GROUP BY GROUPING SETS ()",
         "SELECT id FROM t GROUP BY GROUPING SETS (id)",
         "SELECT id FROM t GROUP BY GROUPING SETS ((id),)",
         "SELECT id FROM t GROUP BY GROUPING SETS ((id)",
         "SELECT id FROM t GROUP BY GROUPING SETS ((id,))",
         "SELECT id FROM t GROUP BY GROUPING SETS ((id), CUBE(n))",
         "SELECT id FROM t GROUP BY ROLLUP ()", "SELECT id FROM t GROUP BY CUBE ()",
         "SELECT id FROM t GROUP BY ROLLUP (id,)", "SELECT id FROM t GROUP BY ROLLUP (id), n",
         "SELECT id FROM t GROUP BY ROLLUP (id) WITH ROLLUP",
         "SELECT id FROM t GROUP BY GROUPING SETS ((id)), n",
         "SELECT id FROM t GROUP BY CUBE (*)", "SELECT id FROM t GROUP BY ROLLUP (DEFAULT)",
         "SELECT id FROM t GROUP BY GROUPING SETS ((id +), ())"}) reject_grouping(sql);
}
TEST(MySQLGrouping, ClonedSetsRetainTheirExpressionChildren) {
    Arena clone_arena; AstNode* ast = nullptr;
    {
        std::string sql = "SELECT `id` FROM t GROUP BY GROUPING SETS ((`id`), ())";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input);
        auto clone = clone_ast(result.ast, clone_arena);
        ASSERT_TRUE(clone.ok()); ast = clone.ast;
        parser.reset(); sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit_grouping(ast, clone_arena), "SELECT `id` FROM t GROUP BY GROUPING SETS ((`id`), ())");
}

TEST(MySQLGrouping, RejectsNestedGroupingKeywordsButRetainsQuotedFunctions) {
    for (const char* sql : {"SELECT id FROM t GROUP BY ROLLUP(CUBE(id))",
         "SELECT id FROM t GROUP BY ROLLUP(ROLLUP(id))",
         "SELECT id FROM t GROUP BY GROUPING SETS ((ROLLUP(id)))",
         "SELECT id FROM t GROUP BY (ROLLUP(id))",
         "SELECT id FROM t GROUP BY ROLLUP(id + CUBE(n))",
         "SELECT id FROM t GROUP BY id, ROLLUP(n)"}) reject_grouping(sql);
    check_grouping("SELECT id FROM t GROUP BY ROLLUP (`CUBE`(id))");
    check_grouping("SELECT id FROM t GROUP BY GROUPING SETS ((`ROLLUP`(id)), ())");
}
