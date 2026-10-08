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

void check_dml(const char* sql, StmtType type, const char* expected = nullptr) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_NE(result.ast, nullptr);
    EXPECT_EQ(result.stmt_type, type);
    Arena output;
    EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, output).ok());
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

void reject_dml(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
}


TEST(MySQLCteDml, CteUpdateRecursive) {
    check_dml("WITH RECURSIVE c(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM c WHERE id<3) UPDATE t SET n=2 WHERE id IN (SELECT id FROM c)", StmtType::UPDATE,
              "WITH RECURSIVE c(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM c WHERE id < 3) UPDATE t SET n = 2 WHERE id IN (SELECT id FROM c)");
}

TEST(MySQLCteDml, CteUpdateQuotedOptionsPartition) {
    check_dml("WITH `select`(`key`) AS (SELECT 1) UPDATE LOW_PRIORITY IGNORE pt PARTITION (p0) AS p SET p.n=3 WHERE p.id IN (SELECT `key` FROM `select`) ORDER BY p.id LIMIT 1", StmtType::UPDATE,
              "WITH `select`(`key`) AS (SELECT 1) UPDATE LOW_PRIORITY IGNORE pt PARTITION (p0) AS p SET p.n = 3 WHERE p.id IN (SELECT `key` FROM `select`) ORDER BY p.id LIMIT 1");
}

TEST(MySQLCteDml, CteUpdateComma) {
    check_dml("WITH c AS (SELECT 1 AS id) UPDATE t, u SET t.n=3, u.n=4 WHERE t.id=u.id AND t.id IN (SELECT id FROM c)", StmtType::UPDATE,
              "WITH c AS (SELECT 1 AS id) UPDATE t, u SET t.n = 3, u.n = 4 WHERE t.id = u.id AND t.id IN (SELECT id FROM c)");
}

TEST(MySQLCteDml, CteUpdateColonDefault) {
    check_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET n:=DEFAULT WHERE id IN (SELECT id FROM c)", StmtType::UPDATE,
              "WITH c AS (SELECT 1 AS id) UPDATE t SET n = DEFAULT WHERE id IN (SELECT id FROM c)");
}

TEST(MySQLCteDml, CteDeleteTargetsFrom) {
    check_dml("WITH c AS (SELECT 1 AS id) DELETE t.*, u.* FROM t JOIN u ON t.id=u.id JOIN c ON c.id=t.id", StmtType::DELETE_STMT,
              "WITH c AS (SELECT 1 AS id) DELETE t.*, u.* FROM t JOIN u ON t.id = u.id JOIN c ON c.id = t.id");
}

TEST(MySQLCteDml, CteDeleteTargetsUsing) {
    check_dml("WITH c AS (SELECT 1 AS id) DELETE FROM t, u USING t JOIN u ON t.id=u.id JOIN c ON c.id=t.id", StmtType::DELETE_STMT,
              "WITH c AS (SELECT 1 AS id) DELETE FROM t, u USING t JOIN u ON t.id = u.id JOIN c ON c.id = t.id");
}

TEST(MySQLCteDml, CteDeleteRecursive) {
    check_dml("WITH RECURSIVE c(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM c WHERE id<3) DELETE FROM t WHERE id IN (SELECT id FROM c)", StmtType::DELETE_STMT,
              "WITH RECURSIVE c(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM c WHERE id < 3) DELETE FROM t WHERE id IN (SELECT id FROM c)");
}

TEST(MySQLCteDml, CteDeletePartitionOptions) {
    check_dml("WITH c AS (SELECT 1 AS id) DELETE IGNORE QUICK LOW_PRIORITY QUICK FROM pt AS p PARTITION (p0) WHERE p.id IN (SELECT id FROM c) ORDER BY p.id DESC LIMIT 1", StmtType::DELETE_STMT,
              "WITH c AS (SELECT 1 AS id) DELETE IGNORE QUICK LOW_PRIORITY QUICK FROM pt AS p PARTITION (p0) WHERE p.id IN (SELECT id FROM c) ORDER BY p.id DESC LIMIT 1");
}

TEST(MySQLCteDml, BadCteUpdateMissingSet) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t WHERE id=1");
}

TEST(MySQLCteDml, BadCteUpdateEmptySet) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET WHERE id=1");
}

TEST(MySQLCteDml, BadCteUpdateExpressionLhs) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET n+1=2");
}

TEST(MySQLCteDml, BadCteUpdateTupleLhs) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET (id,n)=(1,2)");
}

TEST(MySQLCteDml, BadCteUpdateWildcardLhs) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET t.*=2");
}

TEST(MySQLCteDml, BadCteUpdateFourPartLhs) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET a.b.t.n=2");
}

TEST(MySQLCteDml, BadCteUpdateEmptyWhere) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE t SET n=2 WHERE");
}

TEST(MySQLCteDml, BadCteUpdateOptionOrder) {
    reject_dml("WITH c AS (SELECT 1 AS id) UPDATE IGNORE LOW_PRIORITY t SET n=2");
}

TEST(MySQLCteDml, BadCteDeleteMissingFrom) {
    reject_dml("WITH c AS (SELECT 1 AS id) DELETE t WHERE id=1");
}

TEST(MySQLCteDml, BadCteDeleteEmptyWhere) {
    reject_dml("WITH c AS (SELECT 1 AS id) DELETE FROM t WHERE");
}

TEST(MySQLCteDml, BadCteDeleteMultiLimit) {
    reject_dml("WITH c AS (SELECT 1 AS id) DELETE t FROM t JOIN c ON t.id=c.id LIMIT 1");
}

TEST(MySQLCteDml, BadCteBodyUpdate) {
    reject_dml("WITH c AS (UPDATE t SET n=2) SELECT * FROM c");
}

TEST(MySQLCteDml, BadCteBodyDelete) {
    reject_dml("WITH c AS (DELETE FROM t) SELECT * FROM c");
}

TEST(MySQLCteDml, BadSubqueryWithUpdate) {
    reject_dml("SELECT (WITH c AS (SELECT 1 AS id) UPDATE t SET n=2)");
}

TEST(MySQLCteDml, BadSubqueryWithDelete) {
    reject_dml("SELECT * FROM (WITH c AS (SELECT 1 AS id) DELETE FROM t) AS d");
}

TEST(MySQLCteDml, BadWithInsert) {
    reject_dml("WITH c AS (SELECT 1 AS id) INSERT INTO u (id) SELECT id FROM c");
}

TEST(MySQLCteDml, StandaloneRequiredPartsAreValidatedToo) {
    for (const char* sql : {"UPDATE t", "UPDATE t SET", "UPDATE t SET n", "UPDATE t SET n=",
         "UPDATE t SET n=1,", "UPDATE t SET n+1=2", "UPDATE t SET t.*=2",
         "UPDATE t SET n=DEFAULT+1", "UPDATE t SET n=*", "UPDATE t SET n=1 WHERE",
         "UPDATE t SET n=1 ORDER n", "UPDATE t SET n=1 ORDER BY",
         "UPDATE t SET n=1 LIMIT", "UPDATE t SET n=1 LIMIT -1",
         "UPDATE t SET rank=1",
         "UPDATE t SET n=1 LIMIT 1.5", "UPDATE t SET n=1 LIMIT 18446744073709551616",
         "DELETE", "DELETE FROM", "DELETE t", "DELETE FROM t, u", "DELETE FROM t.*",
         "DELETE FROM t USING", "DELETE t FROM", "DELETE FROM t WHERE",
         "DELETE FROM t ORDER n", "DELETE FROM t ORDER BY", "DELETE FROM t LIMIT",
         "DELETE FROM t LIMIT -1", "DELETE FROM t LIMIT 1.5"}) reject_dml(sql);
}

TEST(MySQLCteDml, QualifiedAssignmentAndDeleteTargetsRoundTrip) {
    check_dml("WITH c AS (SELECT 1) UPDATE db.t SET db.t.n := 2 LIMIT ?", StmtType::UPDATE,
              "WITH c AS (SELECT 1) UPDATE db.t SET db.t.n = 2 LIMIT ?");
    check_dml("WITH c AS (SELECT 1) DELETE db.t.* FROM db.t", StmtType::DELETE_STMT);
    check_dml("WITH c AS (SELECT 1) DELETE FROM db.t.* USING db.t", StmtType::DELETE_STMT);
}

TEST(MySQLCteDml, JoinedDmlRequiresCompleteConditions) {
    for (const char* sql : {"UPDATE t JOIN u ON SET t.n=2", "UPDATE t JOIN u USING SET t.n=2",
         "UPDATE t JOIN u USING () SET t.n=2", "UPDATE t JOIN u USING (id,) SET t.n=2",
         "UPDATE t JOIN u USING (id SET t.n=2", "UPDATE t LEFT u SET t.n=2",
         "DELETE t FROM t JOIN u ON", "DELETE t FROM t JOIN u USING",
         "DELETE t FROM t JOIN u USING ()", "DELETE t FROM t JOIN u USING (1)",
         "DELETE t FROM t JOIN u USING (id,)", "DELETE t FROM t JOIN u USING (id"}) reject_dml(sql);
    check_dml("WITH c AS (SELECT 1) UPDATE t JOIN u USING (`id`) SET t.n = 2", StmtType::UPDATE);
    check_dml("WITH c AS (SELECT 1) DELETE t FROM t JOIN u USING (`id`)", StmtType::DELETE_STMT);
}

TEST(MySQLCteDml, RoutingMetadataUsesMainStatementTarget) {
    for (const char* prefix : {"", "WITH c AS (SELECT 1) "}) {
        for (const char* body : {"UPDATE db.t SET n=2", "DELETE FROM db.t",
             "DELETE db.t.* FROM db.t", "DELETE FROM db.t.* USING db.t"}) {
            std::string sql = std::string(prefix) + body;
            SCOPED_TRACE(sql);
            Parser<Dialect::MySQL> parser;
            auto result = parser.parse(sql.data(), sql.size());
            ASSERT_TRUE(result.ok());
            ASSERT_TRUE(result.full_input);
            EXPECT_TRUE(result.table_name.equals_ci("t", 1));
            EXPECT_TRUE(result.schema_name.equals_ci("db", 2));
        }
        std::string sql = std::string(prefix) + "DELETE t.* FROM t";
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        EXPECT_TRUE(result.table_name.equals_ci("t", 1));
        EXPECT_TRUE(result.schema_name.empty());
    }
}
