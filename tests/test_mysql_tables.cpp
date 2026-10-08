#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string emit_mysql(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    auto text = emitter.result();
    return std::string(text.ptr, text.len);
}
void check_query(const char* sql, bool parameterizable = false) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(emit_mysql(result.ast, parser.arena()), sql);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
    Arena destination;
    if (!parameterizable) EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, destination).ok());
    std::string output = emit_mysql(result.ast, parser.arena());
    Parser<Dialect::MySQL> again;
    auto reparsed = again.parse(output.data(), output.size());
    ASSERT_TRUE(reparsed.ok());
    ASSERT_TRUE(reparsed.full_input);
    EXPECT_EQ(emit_mysql(reparsed.ast, again.arena()), output);
}
void check_rejected(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input);
}
}

TEST(MySQLTables, PreservePartitionSelectionAndClauseOrder) {
    for (const char* sql : {
         "SELECT * FROM pt PARTITION (p0)",
         "SELECT a.id FROM db.pt PARTITION (`p0`, p1) AS a WHERE a.id = 1",
         "SELECT * FROM pt PARTITION (p0) JOIN u AS b ON pt.id = b.id",
         "UPDATE pt PARTITION (p0) AS a SET a.n = 2 WHERE a.id = 1",
         "DELETE FROM pt PARTITION (p0) WHERE id = 1",
         "DELETE FROM pt AS a PARTITION (p0) WHERE a.id = 1",
         "DELETE a FROM pt PARTITION (p0) AS a JOIN u ON a.id = u.id"}) check_query(sql);
}

TEST(MySQLTables, PreserveIndexHintKindsScopesAndEmptyUse) {
    for (const char* sql : {
         "SELECT * FROM t USE INDEX ()",
         "SELECT * FROM t FORCE INDEX (idx) WHERE id = 1",
         "SELECT a.id FROM t AS a IGNORE KEY FOR JOIN (idx, PRIMARY)",
         "SELECT * FROM t USE INDEX FOR ORDER BY (idx) IGNORE INDEX FOR GROUP BY (PRIMARY)",
         "SELECT * FROM pt PARTITION (p0) AS a FORCE KEY FOR ORDER BY (idx)",
         "UPDATE t AS a USE INDEX (idx) SET a.n = 2 WHERE a.id = 1",
         "DELETE a FROM t AS a FORCE INDEX (idx) JOIN u ON a.id = u.id"}) check_query(sql);
}

TEST(MySQLTables, RejectMalformedTableModifiers) {
    for (const char* sql : {
         "SELECT * FROM pt PARTITION ()", "SELECT * FROM pt PARTITION (p0,)",
         "SELECT * FROM pt PARTITION (1)", "SELECT * FROM pt PARTITION ('p0')",
         "SELECT * FROM pt PARTITION (p0", "SELECT * FROM pt AS a PARTITION (p0)",
         "SELECT * FROM pt PARTITION (p0) PARTITION (p1)",
         "SELECT * FROM t FORCE INDEX ()", "SELECT * FROM t IGNORE KEY ()",
         "SELECT * FROM t USE INDEX (idx,)", "SELECT * FROM t USE INDEX (1)",
         "SELECT * FROM t USE INDEX ('idx')", "SELECT * FROM t FORCE INDEX idx",
         "SELECT * FROM t USE INDEX FOR ORDER (idx)",
         "SELECT * FROM t USE INDEX FOR WHERE (idx)",
         "SELECT * FROM t FORCE BOGUS (idx)",
         "SELECT * FROM t USE INDEX (idx) AS a",
         "DELETE FROM pt PARTITION (p0) USING pt",
         "DELETE FROM t USE INDEX (idx)"}) check_rejected(sql);
}

TEST(MySQLTables, ParseLateralDerivedQueriesAndColumnAliases) {
    for (const char* sql : {
         "SELECT * FROM t, LATERAL (SELECT t.id) AS d",
         "SELECT d.n FROM t JOIN LATERAL (SELECT t.n + 1 AS n) AS d ON d.n > 0",
         "SELECT * FROM LATERAL (SELECT 1 AS x UNION ALL SELECT 2) AS d",
         "SELECT * FROM LATERAL (SELECT 1, 2) AS d(x, `y`)",
         "SELECT * FROM (SELECT 1, 2) AS d(x, y)"}) check_query(sql, true);
}

TEST(MySQLTables, LateralRequiresCompleteQueryAndAlias) {
    for (const char* sql : {
         "SELECT * FROM LATERAL t", "SELECT * FROM LATERAL f(1) AS d",
         "SELECT * FROM LATERAL (t) AS d", "SELECT * FROM LATERAL (SELECT 1)",
         "SELECT * FROM LATERAL (SELECT 1 AS d", "SELECT * FROM LATERAL (SELECT) AS d",
         "SELECT * FROM LATERAL (SELECT 1 +) AS d",
         "SELECT * FROM LATERAL (SELECT 1 UNION ALL) AS d",
         "SELECT * FROM LATERAL (SELECT 1) AS d()",
         "SELECT * FROM LATERAL (SELECT 1) AS d(x,)",
         "SELECT * FROM LATERAL (SELECT 1) AS d(1)",
         "SELECT * FROM LATERAL (SELECT 1) AS d USE INDEX (idx)"}) check_rejected(sql);
}

TEST(MySQLTables, DerivedAliasesRequireNamesAndSourceQueriesStayComplete) {
    for (const char* sql : {
         "SELECT * FROM LATERAL (SELECT 1) 123",
         "SELECT * FROM LATERAL (SELECT 1) AS 123",
         "SELECT * FROM LATERAL (SELECT 1) 'd'",
         "SELECT * FROM LATERAL (SELECT 1) AS 'd'",
         "SELECT * FROM LATERAL (SELECT 1 FROM WHERE) AS d",
         "SELECT * FROM LATERAL (SELECT 1 WHERE) AS d",
         "SELECT * FROM LATERAL (SELECT 1 ORDER BY) AS d",
         "SELECT * FROM LATERAL (SELECT 1 LIMIT) AS d"}) check_rejected(sql);
}

TEST(MySQLTables, DelimitedNamesDoNotBecomeSyntaxAndUseMayHaveNoIndexes) {
    for (const char* sql : {
         "SELECT * FROM pt PARTITION (`p0`) AS `force` USE KEY FOR JOIN ()",
         "SELECT * FROM t AS `use` FORCE INDEX (`idx`)",
         "SELECT * FROM LATERAL ((SELECT 1 AS x)) AS `lateral`(`x`)"}) check_query(sql, true);
}

TEST(MySQLTables, ReservedNamesRequireDelimitersExceptPrimaryIndex) {
    for (const char* word : {"PRIMARY", "LATERAL", "WINDOW", "SYSTEM", "CUME_DIST", "ROW_NUMBER", "SQLSTATE"}) {
        for (const char* prefix : {"SELECT * FROM t PARTITION (", "SELECT * FROM LATERAL (SELECT 1) AS d("}) {
            check_rejected((std::string(prefix) + word + ")").c_str());
            check_query((std::string(prefix) + "`" + word + "`)").c_str(), true);
        }
        check_rejected((std::string("SELECT * FROM LATERAL (SELECT 1) AS ") + word).c_str());
        if (std::string(word) != "PRIMARY")
            check_rejected((std::string("SELECT * FROM t FORCE INDEX (") + word + ")").c_str());
    }
}

TEST(MySQLTables, DerivedQueriesPreserveLockingAndRequireCompleteClauses) {
    for (const char* sql : {
         "SELECT * FROM LATERAL (SELECT 1 FOR UPDATE NOWAIT) AS d",
         "SELECT * FROM LATERAL (SELECT 1 FOR SHARE SKIP LOCKED) AS d",
         "SELECT * FROM LATERAL (SELECT * FROM t FOR UPDATE OF t NOWAIT) AS d",
         "SELECT * FROM LATERAL (SELECT 1 FOR SHARE OF db.t, t.*, db.t.* SKIP LOCKED) AS d"})
        check_query(sql, true);
    for (const char* sql : {
         "SELECT (SELECT 1 FOR UPDATE NOWAIT)",
         "SELECT (SELECT 1 FOR SHARE SKIP LOCKED)"}) {
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_EQ(emit_mysql(result.ast, parser.arena()), sql);
    }
    for (const char* sql : {
         "SELECT * FROM LATERAL (SELECT 1 GROUP 1) AS d",
         "SELECT * FROM LATERAL (SELECT 1 FOR garbage) AS d",
         "SELECT * FROM LATERAL (SELECT 1 FOR UPDATE SKIP) AS d",
         "SELECT * FROM LATERAL (SELECT 1 FOR UPDATE OF) AS d",
         "SELECT * FROM LATERAL (SELECT 1 FOR UPDATE OF t,) AS d"}) check_rejected(sql);
}
