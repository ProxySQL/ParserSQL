#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string emit_insert_source(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    auto out = emitter.result();
    return std::string(out.ptr, out.len);
}
void check_insert_source(const char* sql, const char* expected = nullptr, bool guarded = false) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(std::string(result.table_name.ptr, result.table_name.len), "t");
    const auto emitted = emit_insert_source(result.ast, parser.arena());
    EXPECT_EQ(emitted, expected ? expected : sql);
    if (guarded) EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
    Parser<Dialect::MySQL> again;
    auto reparsed = again.parse(emitted.data(), emitted.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    EXPECT_EQ(emit_insert_source(reparsed.ast, again.arena()), emitted);
}
void reject_insert_source(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}

TEST(MySQLInsertSources, ValuesRowAndColumnAliasesRoundTrip) {
    for (const char* sql : {
        "INSERT INTO t (id, n) VALUES (1, 2) AS new ON DUPLICATE KEY UPDATE n = new.n",
        "INSERT INTO t (id, n) VALUES (1, 2), (3, 4) AS new (a, b) ON DUPLICATE KEY UPDATE n = b",
        "INSERT INTO t VALUES (1, 2) AS `new``row` (`a`, `b``c`) ON DUPLICATE KEY UPDATE n = `new``row`.`b``c`",
        "INSERT INTO t VALUES (1) AS new",
        "INSERT INTO t VALUES () AS new",
        "INSERT INTO t () VALUES () AS new"}) check_insert_source(sql, nullptr, true);
}

TEST(MySQLInsertSources, SetAliasesAndAssignmentOperators) {
    check_insert_source("INSERT INTO t SET id = 1, n = DEFAULT AS new (a, b) ON DUPLICATE KEY UPDATE n = b", nullptr, true);
    check_insert_source("INSERT INTO t SET t.id := 1, db.t.n := 2 AS new ON DUPLICATE KEY UPDATE t.n := new.n",
        "INSERT INTO t SET t.id = 1, db.t.n = 2 AS new ON DUPLICATE KEY UPDATE t.n = new.n", true);
    check_insert_source("INSERT INTO t VALUE (1, 2) AS new", "INSERT INTO t VALUES (1, 2) AS new", true);
}

TEST(MySQLInsertSources, RejectsMalformedAliasNamesAndColumns) {
    for (const char* sql : {"INSERT INTO t VALUES (1) AS", "INSERT INTO t VALUES (1) AS SELECT",
         "INSERT INTO t VALUES (1) AS 'new'", "INSERT INTO t VALUES (1) AS db.new",
         "INSERT INTO t VALUES (1) AS new()", "INSERT INTO t VALUES (1) AS new(a,)",
         "INSERT INTO t VALUES (1) AS new(a.b)", "INSERT INTO t VALUES (1) AS new('a')",
         "INSERT INTO t VALUES (1) AS new(a", "INSERT INTO t VALUES (1) new",
         "INSERT INTO t VALUES (1) AS new AS other", "REPLACE INTO t VALUES (1) AS new",
         "REPLACE INTO t SET id = 1 AS new", "INSERT INTO t VALUES ROW(1) AS new"}) reject_insert_source(sql);
}

TEST(MySQLInsertSources, RejectsIncompleteRowsAndClauses) {
    for (const char* sql : {"INSERT INTO t", "INSERT INTO t VALUES", "INSERT INTO t VALUES (1",
         "INSERT INTO t VALUES (1,)", "INSERT INTO t VALUES (1),", "INSERT INTO t VALUES (,1)",
         "INSERT INTO t VALUES (*) AS new", "INSERT INTO t VALUES (DEFAULT + 1) AS new",
         "INSERT INTO t SET", "INSERT INTO t SET id", "INSERT INTO t SET id 1",
         "INSERT INTO t SET id =", "INSERT INTO t SET id = 1,", "INSERT INTO t SET id = 1 + AS new",
         "INSERT INTO t VALUES (1) AS new ON", "INSERT INTO t VALUES (1) AS new ON DUPLICATE",
         "INSERT INTO t VALUES (1) AS new ON DUPLICATE KEY", "INSERT INTO t VALUES (1) AS new ON DUPLICATE KEY UPDATE",
         "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE n", "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE n =",
         "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE n = 2,",
         "INSERT INTO t SET a.b.c.d = 1", "INSERT INTO t SET * = 1"}) reject_insert_source(sql);
}

TEST(MySQLInsertSources, NativeTargetAndOptionGrammar) {
    for (const char* sql : {"INSERT LOW_PRIORITY IGNORE INTO t PARTITION (p0) (id) VALUES (1) AS new",
         "INSERT INTO db.t (t.id, db.t.n) VALUES (1, 2) AS new"}) check_insert_source(sql, nullptr, true);
    for (const char* sql : {"INSERT IGNORE LOW_PRIORITY INTO t VALUES (1)",
         "INSERT LOW_PRIORITY HIGH_PRIORITY INTO t VALUES (1)", "INSERT IGNORE IGNORE INTO t VALUES (1)",
         "REPLACE IGNORE INTO t VALUES (1)", "REPLACE HIGH_PRIORITY INTO t VALUES (1)",
         "REPLACE INTO t VALUES (1) ON DUPLICATE KEY UPDATE id = 2",
         "INSERT INTO t AS old VALUES (1)", "INSERT INTO t old VALUES (1)",
         "INSERT INTO t USE INDEX (i) VALUES (1)", "INSERT INTO t (id,) VALUES (1)",
         "INSERT INTO t (1) VALUES (1)", "INSERT INTO t (id) SET id = 1"}) reject_insert_source(sql);
}

TEST(MySQLInsertSources, CompleteQuerySourcesRoundTrip) {
    for (const char* sql : {"INSERT INTO t (id, n) SELECT 1, 2 UNION ALL SELECT 3, 4",
         "INSERT INTO t (id, n) (SELECT 1, 2)", "INSERT INTO t (id, n) VALUES ROW(1, 2), ROW(3, 4)",
         "INSERT INTO t TABLE src", "INSERT INTO t WITH c AS (SELECT 1 AS id) SELECT id FROM c",
         "INSERT INTO t (id) SELECT (SELECT 1)",
         "INSERT INTO t (id) (SELECT 1) ON DUPLICATE KEY UPDATE id = 2"}) check_insert_source(sql);
}

TEST(MySQLInsertSources, AliasesSurviveCloneAndPreventUnsafeParameterization) {
    Arena output;
    AstNode* copy = nullptr;
    {
        std::string sql = "INSERT INTO t VALUES (1, 2) AS `new` (`a`, `b`) ON DUPLICATE KEY UPDATE n = `new`.`b`";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input);
        Arena params;
        EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, params).ok());
        auto cloned = clone_ast(result.ast, output);
        ASSERT_TRUE(cloned.ok()); copy = cloned.ast;
        parser.reset(); sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(emit_insert_source(copy, output),
        "INSERT INTO t VALUES (1, 2) AS `new` (`a`, `b`) ON DUPLICATE KEY UPDATE n = `new`.`b`");
}

TEST(MySQLInsertSources, DirectPlannerRejectsUnsupportedInsertSemantics) {
    for (const char* sql : {"INSERT INTO t VALUES (1) AS new",
         "INSERT INTO t SELECT 1 UNION ALL SELECT 2", "INSERT INTO t (SELECT 1)",
         "INSERT INTO t WITH c AS (SELECT 1 AS id) SELECT id FROM c",
         "INSERT INTO t TABLE src", "INSERT INTO t VALUES ROW(1)",
         "INSERT INTO t PARTITION (p0) VALUES (1)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input);
        sql_engine::InMemoryCatalog catalog;
        Arena plans;
        sql_engine::DmlPlanBuilder<Dialect::MySQL> builder(catalog, plans);
        EXPECT_EQ(builder.build(result.ast), nullptr);
        EXPECT_EQ(builder.build_insert(result.ast), nullptr);
    }
}

TEST(MySQLInsertSources, DoubledBackticksAreOneIdentifierToken) {
    const char* sql = "`new``row`";
    Tokenizer<Dialect::MySQL> tokenizer; tokenizer.reset(sql, std::strlen(sql));
    auto token = tokenizer.next_token();
    EXPECT_EQ(token.type, TokenType::TK_IDENTIFIER);
    EXPECT_EQ(std::string(token.text.ptr, token.text.len), "new``row");
    EXPECT_EQ(token.source.len, std::strlen(sql));
    EXPECT_EQ(tokenizer.next_token().type, TokenType::TK_EOF);
    for (const char* malformed : {"INSERT INTO t VALUES (1) AS `new``", "INSERT INTO t VALUES (1) AS new(`a``)"})
        reject_insert_source(malformed);
}

TEST(MySQLInsertSources, FunctionTokenAliasesKeepLexicalSeparation) {
    for (const char* alias : {"ADDDATE", "BIT_AND", "BIT_OR", "BIT_XOR", "CAST", "COUNT",
         "CURDATE", "CURTIME", "DATE_ADD", "DATE_SUB", "EXTRACT", "GROUP_CONCAT",
         "JSON_OBJECTAGG", "JSON_ARRAYAGG", "MAX", "MID", "MIN", "NOW", "POSITION",
         "SESSION_USER", "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "ST_COLLECT",
         "SUBDATE", "SUBSTR", "SUBSTRING", "SUM", "SYSDATE", "SYSTEM_USER", "TRIM",
         "VARIANCE", "VAR_POP", "VAR_SAMP"}) {
        const std::string prefix = std::string("INSERT INTO t VALUES (1) AS ") + alias;
        check_insert_source((prefix + " (a)").c_str(), nullptr, true);
        reject_insert_source((prefix + "(a)").c_str());
    }
    check_insert_source("INSERT INTO t VALUES (1) AS `count`(a)",
                        "INSERT INTO t VALUES (1) AS `count` (a)", true);
    check_insert_source("INSERT INTO t VALUES (1) AS count/**/(a)",
                        "INSERT INTO t VALUES (1) AS count (a)", true);
}
