#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/mutable_data_source.h"

using namespace sql_parser;
using namespace sql_engine;

TEST(Pr67Engine, RejectsUnsupportedMysqlWriteOperandsAtEveryBuilderEntry) {
    InMemoryCatalog catalog;
    for (const char* sql : {
        "UPDATE t PARTITION(p0) SET n=9",
        "DELETE FROM t PARTITION(p0)",
        "UPDATE t SET n='abc' COLLATE utf8mb4_bin",
        "UPDATE t SET n=CONVERT('9', SIGNED)",
        "DELETE FROM t WHERE CONVERT(n, SIGNED)=9",
        "UPDATE t SET n=EXTRACT(YEAR FROM '2026-01-01')",
        "DELETE FROM t WHERE n->>'$.id'='9'"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        DmlPlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        EXPECT_EQ(builder.build(parsed.ast), nullptr);
        if (parsed.stmt_type == StmtType::UPDATE)
            EXPECT_EQ(builder.build_update(parsed.ast), nullptr);
        else EXPECT_EQ(builder.build_delete(parsed.ast), nullptr);
    }
}

TEST(Pr67Engine, SimpleMysqlWritesStillExecute) {
    InMemoryCatalog catalog;
    catalog.add_table("", "t", {{"n", SqlType::make_int(), false}});
    const auto* table = catalog.get_table({"t", 1});
    Arena data;
    Row row = make_row(data, 1);
    row.set(0, value_int(3));
    InMemoryMutableDataSource source(table, data, {row});
    FunctionRegistry<Dialect::MySQL> functions;
    functions.register_builtins();
    Parser<Dialect::MySQL> parser;
    const char* sql = "UPDATE t SET n=n+2 WHERE n=3";
    auto parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok() && parsed.full_input);
    DmlPlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    auto* plan = builder.build(parsed.ast);
    ASSERT_NE(plan, nullptr);
    PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
    executor.add_mutable_data_source("t", &source);
    auto updated = executor.execute_dml(plan);
    ASSERT_TRUE(updated.success);
    EXPECT_EQ(updated.affected_rows, 1u);
    ASSERT_EQ(source.rows().size(), 1u);
    EXPECT_EQ(source.rows()[0].get(0).int_val, 5);
    sql = "DELETE FROM t WHERE n=5";
    parsed = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(parsed.ok() && parsed.full_input);
    plan = builder.build(parsed.ast);
    ASSERT_NE(plan, nullptr);
    auto removed = executor.execute_dml(plan);
    EXPECT_TRUE(removed.success);
    EXPECT_EQ(removed.affected_rows, 1u);
    EXPECT_TRUE(source.rows().empty());
}

TEST(Pr67Engine, MysqlMultitableWritesRetainTheirOriginalAstForRouting) {
    InMemoryCatalog catalog;
    for (const char* sql : {
        "UPDATE t JOIN u ON t.id = u.id SET t.n = 9 WHERE u.n > 0",
        "UPDATE t, u SET t.n = u.n WHERE t.id = u.id",
        "DELETE t FROM t JOIN u ON t.id = u.id WHERE u.n > 0",
        "DELETE FROM t USING t JOIN u ON t.id = u.id WHERE u.n > 0"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        DmlPlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        auto* plan = builder.build(parsed.ast);
        ASSERT_NE(plan, nullptr);
        auto* original = parsed.stmt_type == StmtType::UPDATE ?
            plan->update_plan.original_ast : plan->delete_plan.original_ast;
        ASSERT_EQ(original, parsed.ast);
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(original);
        EXPECT_EQ(std::string(emitter.result().ptr, emitter.result().len), sql);
    }
}

TEST(Pr67Engine, PostgresEscapeStringsStayParsedButCannotExecuteLocally) {
    InMemoryCatalog catalog;
    for (const char* sql : {
        "SELECT E'\\n'", "SELECT e'\\x41'", "SELECT ABS(1) WHERE E'\\n' = '\\n'",
        "INSERT INTO t(n) VALUES(E'\\n')", "UPDATE t SET n=E'\\x41'",
        "DELETE FROM t WHERE n=E'\\n'"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        PlanBuilder<Dialect::PostgreSQL> query(catalog, parser.arena());
        DmlPlanBuilder<Dialect::PostgreSQL> dml(catalog, parser.arena());
        EXPECT_FALSE(PlanBuilder<Dialect::PostgreSQL>::supports_query_features(parsed.ast));
        EXPECT_FALSE(PlanBuilder<Dialect::PostgreSQL>::supports_dml_features(parsed.ast));
        EXPECT_EQ(query.build(parsed.ast), nullptr);
        EXPECT_EQ(dml.build(parsed.ast), nullptr);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(parsed.ast);
        auto emitted = emitter.result();
        std::string output(emitted.ptr, emitted.len);
        EXPECT_TRUE(output.find("E'") != std::string::npos || output.find("e'") != std::string::npos);
        Arena parameters;
        EXPECT_TRUE(parameterize_ast<Dialect::PostgreSQL>(parsed, parameters).ok());
    }
}

TEST(Pr67Engine, PostgresOrdinaryAndDollarQuotedStringsStillPlan) {
    InMemoryCatalog catalog;
    for (const char* sql : {
        "SELECT '\\n'", "SELECT $q$\\n$q$", "SELECT $$E'\\n'$$",
        "INSERT INTO t(n) VALUES($q$\\n$q$)", "UPDATE t SET n='text'", "DELETE FROM t WHERE n='text'"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto parsed = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        if (parsed.stmt_type == StmtType::SELECT) {
            PlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
            EXPECT_NE(builder.build(parsed.ast), nullptr);
        } else {
            DmlPlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
            EXPECT_NE(builder.build(parsed.ast), nullptr);
        }
    }
}
