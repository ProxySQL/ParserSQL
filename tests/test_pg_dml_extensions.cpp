#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>
#include <cstdlib>

using namespace sql_parser;

TEST(PgDmlExtensions, RejectsWildcardsOutsideReturningProjection) {
    for (const char* sql : {"UPDATE t SET a = *", "DELETE FROM t WHERE *", "INSERT INTO t VALUES(*)",
         "MERGE INTO t USING s ON * WHEN MATCHED THEN DELETE",
         "MERGE INTO t USING s ON true WHEN MATCHED AND * THEN DELETE"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "UPDATE t SET a=1 RETURNING *";
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_EQ(result.status, ParseResult::OK);
    EXPECT_TRUE(result.full_input);
}

TEST(PgDmlExtensions, ConflictTargetsRequireIndexElements) {
    for (const char* sql : {"INSERT INTO t VALUES (1) ON CONFLICT (a + b) DO NOTHING",
         "INSERT INTO t VALUES (1) ON CONFLICT (1) DO NOTHING",
         "INSERT INTO t VALUES (1) ON CONFLICT (a.b) DO NOTHING"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.status == ParseResult::OK && result.full_input);
    }
    for (const char* sql : {"INSERT INTO t VALUES (1) ON CONFLICT ((a + b)) DO NOTHING",
         "INSERT INTO t VALUES (1) ON CONFLICT (lower(a)) DO NOTHING",
         "INSERT INTO t VALUES (1) ON CONFLICT (a) DO NOTHING"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(result.status, ParseResult::OK);
        EXPECT_TRUE(result.full_input);
    }
}

TEST(PgDmlExtensions, ExhaustedArenasFailWithoutCrashingOrDroppingSyntax) {
    EXPECT_EXIT(([] {
        const char* queries[] = {
            "UPDATE t SET a=1", "UPDATE t SET (a,b)=(1,2) RETURNING a AS x",
            "UPDATE s.t SET a[1].b=2 WHERE x=1", "DELETE FROM ONLY t RETURNING *",
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a=2 RETURNING a",
            "INSERT INTO t DEFAULT VALUES", "INSERT INTO t OVERRIDING USER VALUE VALUES (1)",
            "MERGE INTO t USING s ON true WHEN MATCHED AND true THEN UPDATE SET a=1 WHEN NOT MATCHED THEN INSERT (a) VALUES (2)",
            "WITH RECURSIVE c(x) AS (VALUES (1)) SEARCH DEPTH FIRST BY x SET ordering CYCLE x SET cyclic USING path SELECT * FROM c"
        };
        for (const char* sql : queries) {
            Parser<Dialect::PostgreSQL> baseline;
            auto expected = baseline.parse(sql, std::strlen(sql));
            if (expected.status != ParseResult::OK || !expected.full_input) std::exit(3);
            Arena output;
            Emitter<Dialect::PostgreSQL> canonical(output);
            canonical.emit(expected.ast);
            std::string spelling(canonical.result().ptr, canonical.result().len);
            for (size_t bytes = 48; bytes <= 3072; bytes += 48) {
                ParserConfig config;
                config.arena_block_size = config.arena_max_size = bytes;
                Parser<Dialect::PostgreSQL> parser(config);
                auto result = parser.parse(sql, std::strlen(sql));
                if (result.status == ParseResult::OK && result.full_input) {
                    Arena emit_arena;
                    Emitter<Dialect::PostgreSQL> emitter(emit_arena);
                    emitter.emit(result.ast);
                    if (std::string(emitter.result().ptr, emitter.result().len) != spelling) std::exit(5);
                }
            }
        }
        std::string large = "UPDATE t SET a=1";
        for (int i = 1; i < 10000; ++i) large += ",a=1";
        Parser<Dialect::PostgreSQL> parser;
        auto result = parser.parse(large.data(), large.size());
        if (result.status == ParseResult::OK) std::exit(6);
        std::exit(0);
    }()), ::testing::ExitedWithCode(0), "");
}

TEST(PgDmlExtensions, PreservesMergeActionsAndReturning) {
    const char* queries[] = {
        "MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED AND s.x > 0 THEN UPDATE SET x = s.x WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED BY TARGET THEN INSERT (id, x) VALUES (s.id, s.x) RETURNING t.id",
        "MERGE INTO t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN INSERT DEFAULT VALUES RETURNING *"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        EXPECT_EQ(r.stmt_type, StmtType::MERGE);
        EXPECT_EQ(r.ast->type, NodeType::NODE_MERGE_STMT);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        EXPECT_EQ(std::string(emitter.result().ptr, emitter.result().len), sql);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
    }
}

TEST(PgDmlExtensions, ClassifiesWithMainDmlAndRejectsLocalExecution) {
    struct Case { const char* sql; StmtType type; } cases[] = {
        {"WITH c AS (SELECT 1 AS x) INSERT INTO t SELECT x FROM c RETURNING x", StmtType::INSERT},
        {"WITH c AS (DELETE FROM t RETURNING x) SELECT x FROM c", StmtType::SELECT},
        {"WITH c AS (INSERT INTO t VALUES (1) RETURNING x) UPDATE t SET x = 2 RETURNING x", StmtType::UPDATE},
        {"WITH c AS (UPDATE t SET x = 2 RETURNING x) DELETE FROM t WHERE x IN (SELECT x FROM c)", StmtType::DELETE_STMT},
        {"WITH c AS (SELECT 1) MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE", StmtType::MERGE}
    };
    for (auto c : cases) {
        SCOPED_TRACE(c.sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(c.sql, std::strlen(c.sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        EXPECT_EQ(r.stmt_type, c.type);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::PostgreSQL>::supports_query_features(r.ast));
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        EXPECT_EQ(std::string(emitter.result().ptr, emitter.result().len), c.sql);
    }
}

TEST(PgDmlExtensions, PreservesSearchCycleAndInsertVariants) {
    const char* queries[] = {
        "WITH RECURSIVE c(x) AS (VALUES (1)) SEARCH DEPTH FIRST BY x SET ordering CYCLE x SET cyclic TO true DEFAULT false USING path SELECT * FROM c",
        "WITH RECURSIVE c(x) AS (VALUES (1)) SEARCH BREADTH FIRST BY x SET ordering CYCLE x SET cyclic USING path SELECT * FROM c",
        "WITH RECURSIVE c(x) AS (VALUES (1)) CYCLE x SET cyclic TO point '(1,1)' DEFAULT point '(0,0)' USING path SELECT * FROM c",
        "INSERT INTO t (id) OVERRIDING SYSTEM VALUE VALUES (1)",
        "INSERT INTO t OVERRIDING USER VALUE SELECT 1 UNION ALL SELECT 2",
        "INSERT INTO t WITH c AS (SELECT 1) SELECT * FROM c",
        "INSERT INTO t VALUES (DEFAULT, (SELECT 1)) RETURNING *"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        EXPECT_EQ(std::string(emitter.result().ptr, emitter.result().len), sql);
    }
}

TEST(PgDmlExtensions, RejectsMalformedAndNestedModifyingStatements) {
    const char* queries[] = {
        "MERGE INTO t USING s ON t.id = s.id", "MERGE INTO t USING s ON true WHEN MATCHED THEN INSERT VALUES (1)",
        "MERGE INTO t USING s ON true WHEN NOT MATCHED THEN DELETE", "MERGE INTO t USING s ON true WHEN MATCHED THEN UPDATE SET x",
        "INSERT INTO t", "INSERT INTO t VALUES", "INSERT INTO t VALUES ()", "INSERT INTO t VALUES (1,)",
        "INSERT INTO t OVERRIDING SYSTEM VALUES (1)", "UPDATE t SET x", "DELETE t",
        "DELETE FROM t USING", "UPDATE t SET a = 1 FROM", "DELETE FROM t USING s,",
        "DELETE FROM t USING 123", "UPDATE t SET a = 1 FROM s.",
        "SELECT EXISTS(DELETE FROM t RETURNING x)",
        "SELECT (WITH c AS (SELECT 1) DELETE FROM t RETURNING x)",
        "SELECT * FROM (WITH c AS (SELECT 1) INSERT INTO t VALUES (1) RETURNING x) AS q",
        "SELECT (WITH c AS (DELETE FROM t RETURNING x) SELECT x FROM c)",
        "WITH c AS (SELECT 1) SEARCH SIDEWAYS FIRST BY x SET y SELECT * FROM c"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(r.status == ParseResult::OK && r.full_input);
    }
}

TEST(PgDmlExtensions, PreservesExtendedTargetsValuesAndReturning) {
    const char* queries[] = {
        "INSERT INTO t VALUES (1) UNION ALL VALUES (2) ORDER BY 1",
        "INSERT INTO t (a[1], b.c) VALUES (2, 3)",
        "INSERT INTO t (a[1].b[2]) VALUES (3)",
        "UPDATE t SET a[1] = 2, b.c = 3 RETURNING a",
        "UPDATE t SET (a, b) = (1, 2) RETURNING WITH (OLD AS o, NEW AS n) o.a, n.b",
        "DELETE FROM ONLY t WHERE CURRENT OF c RETURNING WITH (OLD AS o) o.*",
        "MERGE INTO t USING s ON true WHEN NOT MATCHED THEN INSERT OVERRIDING SYSTEM VALUE VALUES (1) RETURNING merge_action()",
        "WITH c AS (MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE RETURNING *) SELECT * FROM c",
        "WITH c AS (SELECT 1) MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        Emitter<Dialect::PostgreSQL> emitter(parser.arena());
        emitter.emit(r.ast);
        EXPECT_EQ(std::string(emitter.result().ptr, emitter.result().len), sql);
    }
}

TEST(PgDmlExtensions, LocalDmlPlannerRejectsUnsupportedSemantics) {
    sql_engine::InMemoryCatalog catalog;
    const char* queries[] = {
        "INSERT INTO t OVERRIDING USER VALUE VALUES (1)",
        "INSERT INTO t VALUES (DEFAULT)",
        "UPDATE t SET (a, b) = (1, 2)",
        "UPDATE t SET a = DEFAULT",
        "DELETE FROM t RETURNING WITH (OLD AS o) o.*",
        "WITH c AS (DELETE FROM t RETURNING *) SELECT * FROM c"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::PostgreSQL> parser;
        auto r = parser.parse(sql, std::strlen(sql));
        ASSERT_EQ(r.status, ParseResult::OK);
        ASSERT_TRUE(r.full_input);
        sql_engine::DmlPlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
        EXPECT_EQ(builder.build(r.ast), nullptr);
    }
    Parser<Dialect::PostgreSQL> parser;
    const char* sql = "INSERT INTO t VALUES (1)";
    auto r = parser.parse(sql, std::strlen(sql));
    sql_engine::DmlPlanBuilder<Dialect::PostgreSQL> builder(catalog, parser.arena());
    EXPECT_NE(builder.build(r.ast), nullptr);
}
