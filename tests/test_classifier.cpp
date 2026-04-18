#include <gtest/gtest.h>
#include "sql_parser/parser.h"

using namespace sql_parser;

// ========== MySQL Classifier Tests ==========

class MySQLClassifierTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> parser;
};

TEST_F(MySQLClassifierTest, ClassifySelect) {
    auto r = parser.parse("SELECT * FROM users", 19);
    // Dedicated parser tests validate AST details; classifier tests only need
    // to confirm statement type routing here.
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
}

TEST_F(MySQLClassifierTest, ClassifyInsert) {
    auto r = parser.parse("INSERT INTO users VALUES (1, 'a')", 33);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyInsertQualified) {
    auto r = parser.parse("INSERT INTO mydb.users VALUES (1)", 33);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.schema_name.ptr, r.schema_name.len), "mydb");
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyUpdate) {
    auto r = parser.parse("UPDATE users SET name='x'", 25);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::UPDATE);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyDelete) {
    auto r = parser.parse("DELETE FROM users WHERE id=1", 28);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DELETE_STMT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifySet) {
    auto r = parser.parse("SET autocommit=0", 16);
    EXPECT_EQ(r.stmt_type, StmtType::SET);
}

TEST_F(MySQLClassifierTest, ClassifyUse) {
    auto r = parser.parse("USE mydb", 8);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::USE);
    EXPECT_EQ(std::string(r.database_name.ptr, r.database_name.len), "mydb");
}

TEST_F(MySQLClassifierTest, ClassifyBegin) {
    auto r = parser.parse("BEGIN", 5);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::BEGIN);
}

TEST_F(MySQLClassifierTest, ClassifyStartTransaction) {
    auto r = parser.parse("START TRANSACTION", 17);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::START_TRANSACTION);
}

TEST_F(MySQLClassifierTest, ClassifyCommit) {
    auto r = parser.parse("COMMIT", 6);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::COMMIT);
}

TEST_F(MySQLClassifierTest, ClassifyRollback) {
    auto r = parser.parse("ROLLBACK", 8);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::ROLLBACK);
}

TEST_F(MySQLClassifierTest, ClassifyCreateTable) {
    auto r = parser.parse("CREATE TABLE users (id INT)", 27);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::CREATE);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyDropTable) {
    auto r = parser.parse("DROP TABLE users", 16);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DROP);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyShow) {
    auto r = parser.parse("SHOW TABLES", 11);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::SHOW);
}

TEST_F(MySQLClassifierTest, ClassifyReplace) {
    auto r = parser.parse("REPLACE INTO users VALUES (1)", 29);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::REPLACE);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(MySQLClassifierTest, ClassifyGrant) {
    auto r = parser.parse("GRANT SELECT ON users TO 'app'", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::GRANT);
}

TEST_F(MySQLClassifierTest, ClassifyRevoke) {
    auto r = parser.parse("REVOKE ALL ON users FROM 'app'", 30);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::REVOKE);
}

TEST_F(MySQLClassifierTest, ClassifyLock) {
    auto r = parser.parse("LOCK TABLES users WRITE", 23);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::LOCK);
}

TEST_F(MySQLClassifierTest, ClassifyDeallocate) {
    auto r = parser.parse("DEALLOCATE PREPARE stmt1", 24);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::DEALLOCATE);
}

TEST_F(MySQLClassifierTest, ClassifyExplain) {
    auto r = parser.parse("EXPLAIN SELECT 1", 16);
    EXPECT_EQ(r.stmt_type, StmtType::EXPLAIN);
}

TEST_F(MySQLClassifierTest, ClassifyUnknown) {
    auto r = parser.parse("HANDLER t1 OPEN", 15);
    EXPECT_EQ(r.stmt_type, StmtType::UNKNOWN);
}

TEST_F(MySQLClassifierTest, EmptyInput) {
    auto r = parser.parse("", 0);
    EXPECT_EQ(r.status, ParseResult::ERROR);
    EXPECT_EQ(r.stmt_type, StmtType::UNKNOWN);
}

TEST_F(MySQLClassifierTest, MultiStatement) {
    const char* sql = "BEGIN; SELECT 1";
    auto r = parser.parse(sql, strlen(sql));
    EXPECT_EQ(r.stmt_type, StmtType::BEGIN);
    EXPECT_TRUE(r.has_remaining());
    // remaining should point to " SELECT 1"
    EXPECT_GT(r.remaining.len, 0u);
}

TEST_F(MySQLClassifierTest, CaseInsensitive) {
    auto r = parser.parse("insert into USERS values (1)", 28);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "USERS");
}

// ========== PostgreSQL Classifier Tests ==========

class PgSQLClassifierTest : public ::testing::Test {
protected:
    Parser<Dialect::PostgreSQL> parser;
};

TEST_F(PgSQLClassifierTest, ClassifySelect) {
    auto r = parser.parse("SELECT * FROM users", 19);
    EXPECT_EQ(r.stmt_type, StmtType::SELECT);
}

TEST_F(PgSQLClassifierTest, ClassifyInsert) {
    auto r = parser.parse("INSERT INTO users VALUES (1)", 28);
    EXPECT_EQ(r.stmt_type, StmtType::INSERT);
    EXPECT_EQ(std::string(r.table_name.ptr, r.table_name.len), "users");
}

TEST_F(PgSQLClassifierTest, ClassifyReset) {
    auto r = parser.parse("RESET ALL", 9);
    EXPECT_EQ(r.status, ParseResult::OK);
    EXPECT_EQ(r.stmt_type, StmtType::RESET);
}
