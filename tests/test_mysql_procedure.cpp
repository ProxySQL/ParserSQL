#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/ast_transform.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
std::string procedure_emit(const AstNode* ast, Arena& arena) {
    Emitter<Dialect::MySQL> emitter(arena);
    emitter.emit(ast);
    const auto out = emitter.result();
    return std::string(out.ptr, out.len);
}
}

TEST(MySQLProcedure, StructuredBodiesAndCharacteristicsRoundTrip) {
    for (const char* sql : {
         "CREATE PROCEDURE p () BEGIN SELECT 1; END",
         "CREATE PROCEDURE p () BEGIN END",
         "CREATE PROCEDURE p (IN a INT, OUT b DECIMAL(10,2), INOUT c VARCHAR(20)) BEGIN SET b = a + 1; SELECT c; END",
         "CREATE PROCEDURE `db`.`p` (a INT) LANGUAGE SQL NOT DETERMINISTIC SQL SECURITY INVOKER COMMENT 'can''t' READS SQL DATA BEGIN SELECT a; END",
         "CREATE PROCEDURE p () DETERMINISTIC CONTAINS SQL BEGIN BEGIN SELECT 1; END; SELECT 2; END",
         "CREATE PROCEDURE p () MODIFIES SQL DATA BEGIN INSERT INTO t (n) VALUES (1); UPDATE t SET n = 2 WHERE n = 1; DELETE FROM t WHERE n = 2; END",
         "CREATE PROCEDURE p () NO SQL SELECT 1",
         "CREATE PROCEDURE p (OUT n INT) SQL SECURITY DEFINER SET n = 1",
         "CREATE PROCEDURE p (IN `select` INT) BEGIN SET `select` = 2, @answer = 3; SELECT `select`; END",
         "CREATE PROCEDURE p (s VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin) BEGIN SELECT s; END",
         "CREATE PROCEDURE p () BEGIN WITH c AS (SELECT 1 AS n) SELECT n FROM c; END",
         "CREATE PROCEDURE p (s VARCHAR(20) COLLATE BINARY) SELECT s"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        EXPECT_EQ(result.ast->type, NodeType::NODE_MYSQL_CREATE_PROCEDURE);
        const auto emitted = procedure_emit(result.ast, parser.arena());
        EXPECT_EQ(emitted, sql);
        Parser<Dialect::MySQL> second;
        auto reparsed = second.parse(emitted.data(), emitted.size());
        ASSERT_TRUE(reparsed.ok() && reparsed.full_input);
        EXPECT_EQ(procedure_emit(reparsed.ast, second.arena()), emitted);
    }
}

TEST(MySQLProcedure, RejectsMalformedParametersCharacteristicsAndBodies) {
    for (const char* sql : {
         "CREATE PROCEDURE", "CREATE PROCEDURE p", "CREATE PROCEDURE p( BEGIN END",
         "CREATE PROCEDURE p(IN) BEGIN END", "CREATE PROCEDURE p(a) BEGIN END",
         "CREATE PROCEDURE p(a invented_type) BEGIN END", "CREATE PROCEDURE p(a INT,) BEGIN END",
         "CREATE PROCEDURE p(a INT DEFAULT 1) BEGIN END", "CREATE PROCEDURE p() LANGUAGE BEGIN END",
         "CREATE PROCEDURE p() READS DATA BEGIN END", "CREATE PROCEDURE p() SQL SECURITY BEGIN END",
         "CREATE PROCEDURE p() COMMENT 1 BEGIN END", "CREATE PROCEDURE p() BEGIN",
         "CREATE PROCEDURE p() BEGIN SELECT 1 END", "CREATE PROCEDURE p() BEGIN SELECT FROM; END",
         "CREATE PROCEDURE p() BEGIN SELECT 1; garbage; END", "CREATE PROCEDURE p() BEGIN ; END",
         "CREATE PROCEDURE p() BEGIN SET n 1; END", "CREATE PROCEDURE p() BEGIN SET n =; END",
         "CREATE PROCEDURE p() BEGIN BEGIN SELECT 1; END END",
         "CREATE PROCEDURE p() BEGIN SELECT 1; END garbage",
         "CREATE PROCEDURE p() BEGIN IF 1 THEN SELECT 1; END IF; END",
         "CREATE PROCEDURE p() BEGIN DECLARE n INT; SELECT n; END",
         "CREATE PROCEDURE p() SET x = *",
         "CREATE PROCEDURE p() BEGIN SET x = *; END",
         "CREATE PROCEDURE p() SET x = t.*",
         "CREATE PROCEDURE p() SET x = 1 + *",
         "CREATE PROCEDURE p(s VARCHAR(20) COLLATE _utf8mb4) SELECT s"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input && result.ast);
    }
}

TEST(MySQLProcedure, CloneOwnsParametersAndBodyAndTraversalReachesStatements) {
    Arena destination;
    AstNode* copied = nullptr;
    const char* expected = "CREATE PROCEDURE p (IN a INT) COMMENT 'hello' BEGIN SET a = a + 1; SELECT a; END";
    {
        std::string sql = expected;
        Parser<Dialect::MySQL> parser;
        auto parsed = parser.parse(sql.data(), sql.size());
        ASSERT_TRUE(parsed.ok() && parsed.full_input);
        ASSERT_NE(parsed.ast, nullptr);
        size_t selects = 0, sets = 0;
        auto walked = walk_ast(parsed.ast, [&](const AstNode& n, const AstVisitContext&) {
            selects += n.type == NodeType::NODE_SELECT_STMT;
            sets += n.type == NodeType::NODE_SET_STMT;
            return AstVisitAction::Continue;
        });
        EXPECT_EQ(walked.status, AstWalkStatus::Completed);
        EXPECT_EQ(selects, 1u);
        EXPECT_EQ(sets, 1u);
        auto cloned = clone_ast(parsed.ast, destination);
        ASSERT_TRUE(cloned.ok());
        copied = cloned.ast;
        parser.reset();
        sql.assign(sql.size(), '!');
    }
    EXPECT_EQ(procedure_emit(copied, destination), expected);
}

TEST(MySQLProcedure, BatchKeepsNestedBodiesAndCaseExpressionsTogether) {
    const std::string routine =
        "CREATE PROCEDURE p () BEGIN SELECT CASE WHEN 1 THEN ';END;' ELSE 'x' END; "
        "BEGIN SELECT 2; END; END";
    const std::string sql = "SELECT 0; " + routine + "; SELECT 3;";
    Parser<Dialect::MySQL> parser;
    auto batch = parser.parse_all(sql.data(), sql.size());
    ASSERT_TRUE(batch.ok());
    ASSERT_EQ(batch.statements.size(), 3u);
    EXPECT_EQ(procedure_emit(batch.statements[0].result.ast, parser.arena()), "SELECT 0");
    EXPECT_EQ(procedure_emit(batch.statements[1].result.ast, parser.arena()), routine);
    EXPECT_EQ(procedure_emit(batch.statements[2].result.ast, parser.arena()), "SELECT 3");
    for (const auto& statement : batch.statements) {
        EXPECT_EQ(statement.source.ptr, sql.data() + statement.offset);
        EXPECT_TRUE(statement.result.full_input);
    }
    EXPECT_EQ(batch.statements[1].offset, sql.find("CREATE"));
    EXPECT_EQ(batch.statements[2].offset, sql.find("SELECT 3"));
}

TEST(MySQLProcedure, NestingLimitRejectsExcessiveBlocks) {
    std::string sql = "CREATE PROCEDURE p() ";
    for (unsigned i = 0; i != 65; ++i) sql += "BEGIN ";
    sql += "SELECT 1; ";
    for (unsigned i = 0; i != 64; ++i) sql += "END; ";
    sql += "END";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql.data(), sql.size());
    EXPECT_FALSE(result.ok() && result.full_input && result.ast);
}
