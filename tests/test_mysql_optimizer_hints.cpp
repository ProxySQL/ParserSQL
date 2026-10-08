#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_parser/ast_transform.h"
#include <cstring>
#include <string>
using namespace sql_parser;
namespace {
std::string hint_emit(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena); emitter.emit(ast);
    auto sql = emitter.result(); return {sql.ptr, sql.len};
}
}
TEST(MySQLOptimizerHints, PreservesNativeStatementPositions) {
    for (const char* sql : {
        "SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t",
        "SELECT /*+ QB_NAME(main) NO_INDEX(t) */ DISTINCT n FROM t",
        "SELECT (SELECT /*+ QB_NAME(inner_q) */ 1)",
        "INSERT /*+ SET_VAR(sort_buffer_size=16384) */ INTO t VALUES (1)",
        "REPLACE /*+ NO_INDEX(t) */ INTO t VALUES (1)",
        "UPDATE /*+ NO_INDEX(t) */ t SET n = 2",
        "DELETE /*+ NO_INDEX(t) */ FROM t",
        "DELETE /*+ NO_INDEX(t) */ t FROM t JOIN u ON (t.id = u.id)",
        "SELECT /*+ FUTURE_HINT(@q x) */ 1"}) {
        SCOPED_TRACE(sql); Parser<Dialect::MySQL> p;
        auto r = p.parse(sql, std::strlen(sql)); ASSERT_TRUE(r.ok()); ASSERT_TRUE(r.full_input);
        ASSERT_NE(r.ast, nullptr); auto emitted = hint_emit(r.ast, p.arena()); EXPECT_EQ(emitted, sql);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(r.ast));
        Arena a; EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(r, a).ok());
        Parser<Dialect::MySQL> again; auto rr=again.parse(emitted.data(), emitted.size());
        EXPECT_TRUE(rr.ok()); EXPECT_TRUE(rr.full_input);
    }
}
TEST(MySQLOptimizerHints, IgnoresCommentsOutsideNativeHintPositions) {
    for (const char* sql : {"/*+ NO_INDEX(t) */ SELECT 1", "SELECT 1 /*+ NO_INDEX(t) */",
        "SELECT /* ordinary */ /*+ NO_INDEX(t) */ 1", "SELECT /*+ A() */ /*+ B() */ 1"}) {
        SCOPED_TRACE(sql); Parser<Dialect::MySQL> p; auto r=p.parse(sql, std::strlen(sql));
        ASSERT_TRUE(r.ok()); ASSERT_TRUE(r.full_input);
        EXPECT_EQ(hint_emit(r.ast,p.arena()), std::strstr(sql,"A()") ? "SELECT /*+ A() */ 1" : "SELECT 1");
    }
}
TEST(MySQLOptimizerHints, RejectsUnterminatedComment) {
    const char* sql="SELECT /*+ MAX_EXECUTION_TIME(1)";
    Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
    EXPECT_FALSE(r.ok() && r.full_input && r.ast);
}

TEST(MySQLOptimizerHints, RejectsDirectExecutionAndClonesCommentOwnership) {
    Arena owned;
    AstNode* copy = nullptr;
    {
        std::string sql = "SELECT /*+ QB_NAME(main) */ 1";
        Parser<Dialect::MySQL> p; auto r = p.parse(sql.data(), sql.size());
        auto cloned = clone_ast(r.ast, owned); ASSERT_TRUE(cloned.ok()); copy = cloned.ast;
    }
    EXPECT_EQ(hint_emit(copy, owned), "SELECT /*+ QB_NAME(main) */ 1");
    sql_engine::InMemoryCatalog catalog;
    Arena plans;
    sql_engine::DmlPlanBuilder<Dialect::MySQL> builder(catalog, plans);
    for (const char* sql : {"UPDATE /*+ NO_INDEX(t) */ t SET n = 1", "DELETE /*+ NO_INDEX(t) */ FROM t"}) {
        Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql)); ASSERT_TRUE(r.ok());
        EXPECT_EQ(builder.build(r.ast), nullptr);
        if (r.ast->type == NodeType::NODE_UPDATE_STMT) EXPECT_EQ(builder.build_update(r.ast), nullptr);
        else EXPECT_EQ(builder.build_delete(r.ast), nullptr);
    }
}

TEST(MySQLOptimizerHints, QualifiedKeywordNamesDoNotOpenHintPositions) {
    for (const char* name : {"select", "insert", "replace", "update", "delete"}) {
        std::string sql = std::string("SELECT t.") + name + " /*+ NO_INDEX(t) */ FROM t";
        Parser<Dialect::MySQL> p; auto r = p.parse(sql.data(), sql.size());
        ASSERT_TRUE(r.ok()); ASSERT_TRUE(r.full_input);
        EXPECT_EQ(hint_emit(r.ast, p.arena()), std::string("SELECT t.") + name + " FROM t");
    }
}

TEST(MySQLOptimizerHints, ReplaceFunctionDiscardsUnusedKeywordHint) {
    const char* sql = "SELECT REPLACE /*+ MAX_EXECUTION_TIME(1) */ ('abc', 'a', 'x')";
    Parser<Dialect::MySQL> p; auto r = p.parse(sql, std::strlen(sql));
    ASSERT_TRUE(r.ok()); ASSERT_TRUE(r.full_input);
    EXPECT_EQ(hint_emit(r.ast, p.arena()), "SELECT REPLACE('abc', 'a', 'x')");
}
