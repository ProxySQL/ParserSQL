#include <gtest/gtest.h>
#include "sql_engine/pgsql_remote_executor.h"
#include "sql_engine/backend_config.h"
#include "sql_parser/arena.h"

#include <libpq-fe.h>

namespace {

static const char* PGSQL_HOST = "127.0.0.1";
static const uint16_t PGSQL_PORT = 15432;
static const char* PGSQL_USER = "postgres";
static const char* PGSQL_PASS = "test";
static const char* PGSQL_DB = "testdb";

bool pgsql_available() {
    std::string conninfo = std::string("host=") + PGSQL_HOST
        + " port=" + std::to_string(PGSQL_PORT)
        + " user=" + PGSQL_USER
        + " password=" + PGSQL_PASS
        + " dbname=" + PGSQL_DB
        + " connect_timeout=2";
    PGconn* conn = PQconnectdb(conninfo.c_str());
    bool ok = (PQstatus(conn) == CONNECTION_OK);
    PQfinish(conn);
    return ok;
}

#define SKIP_IF_NO_PGSQL() if (!pgsql_available()) { GTEST_SKIP() << "PostgreSQL not available on port 15432"; }

sql_engine::BackendConfig make_pgsql_config(const char* name = "test_pgsql") {
    sql_engine::BackendConfig cfg;
    cfg.name = name;
    cfg.host = PGSQL_HOST;
    cfg.port = PGSQL_PORT;
    cfg.user = PGSQL_USER;
    cfg.password = PGSQL_PASS;
    cfg.database = PGSQL_DB;
    cfg.dialect = sql_parser::Dialect::PostgreSQL;
    return cfg;
}

class PgSQLExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        arena_ = std::make_unique<sql_parser::Arena>(65536, 1048576);
        exec_ = std::make_unique<sql_engine::PgSQLRemoteExecutor>(*arena_);
        exec_->add_backend(make_pgsql_config());
    }

    void TearDown() override {
        exec_->disconnect_all();
        exec_.reset();
        arena_.reset();
    }

    std::unique_ptr<sql_parser::Arena> arena_;
    std::unique_ptr<sql_engine::PgSQLRemoteExecutor> exec_;
};

TEST_F(PgSQLExecutorTest, SelectAllUsers) {
    SKIP_IF_NO_PGSQL();
    sql_parser::StringRef sql{"SELECT * FROM users", 19};
    auto rs = exec_->execute("test_pgsql", sql);
    EXPECT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 4u);
}

TEST_F(PgSQLExecutorTest, SelectWithWhere) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT * FROM users WHERE dept = 'Engineering'";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    EXPECT_EQ(rs.row_count(), 3u);
}

TEST_F(PgSQLExecutorTest, SelectWithOrderByLimit) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT * FROM users ORDER BY age ASC LIMIT 2";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    EXPECT_EQ(rs.row_count(), 2u);
    ASSERT_GE(rs.rows[0].column_count, 3u);
    EXPECT_EQ(rs.rows[0].get(2).tag, sql_engine::Value::TAG_INT64);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 25);
}

TEST_F(PgSQLExecutorTest, TypeConversion) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT id, name, age, dept FROM users WHERE id = 1";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    ASSERT_EQ(rs.row_count(), 1u);

    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_INT64);
    EXPECT_EQ(rs.rows[0].get(0).int_val, 1);

    EXPECT_EQ(rs.rows[0].get(1).tag, sql_engine::Value::TAG_STRING);
    EXPECT_EQ(std::string(rs.rows[0].get(1).str_val.ptr, rs.rows[0].get(1).str_val.len), "Alice");

    EXPECT_EQ(rs.rows[0].get(2).tag, sql_engine::Value::TAG_INT64);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 30);
}

TEST_F(PgSQLExecutorTest, BooleanType) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT true AS t, false AS f";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_BOOL);
    EXPECT_TRUE(rs.rows[0].get(0).bool_val);
    EXPECT_EQ(rs.rows[0].get(1).tag, sql_engine::Value::TAG_BOOL);
    EXPECT_FALSE(rs.rows[0].get(1).bool_val);
}

TEST_F(PgSQLExecutorTest, TimestampType) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT TIMESTAMP '2024-06-15 14:30:00' AS ts";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_DATETIME);
    EXPECT_NE(rs.rows[0].get(0).datetime_val, 0);
}

TEST_F(PgSQLExecutorTest, NullHandling) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT NULL::int AS x";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_TRUE(rs.rows[0].get(0).is_null());
}

TEST_F(PgSQLExecutorTest, InsertAndReadBack) {
    SKIP_IF_NO_PGSQL();
    const char* ins = "INSERT INTO users VALUES (99, 'Test', 40, 'QA')";
    sql_parser::StringRef ins_sql{ins, static_cast<uint32_t>(strlen(ins))};
    auto dml = exec_->execute_dml("test_pgsql", ins_sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 1u);

    const char* sel = "SELECT * FROM users WHERE id = 99";
    sql_parser::StringRef sel_sql{sel, static_cast<uint32_t>(strlen(sel))};
    auto rs = exec_->execute("test_pgsql", sel_sql);
    EXPECT_EQ(rs.row_count(), 1u);

    const char* del = "DELETE FROM users WHERE id = 99";
    sql_parser::StringRef del_sql{del, static_cast<uint32_t>(strlen(del))};
    exec_->execute_dml("test_pgsql", del_sql);
}

TEST_F(PgSQLExecutorTest, UpdateAffectedRows) {
    SKIP_IF_NO_PGSQL();
    const char* q = "UPDATE users SET age = age + 1 WHERE dept = 'Sales'";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto dml = exec_->execute_dml("test_pgsql", sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 2u);

    const char* revert = "UPDATE users SET age = age - 1 WHERE dept = 'Sales'";
    sql_parser::StringRef rev_sql{revert, static_cast<uint32_t>(strlen(revert))};
    exec_->execute_dml("test_pgsql", rev_sql);
}

TEST_F(PgSQLExecutorTest, DeleteAffectedRows) {
    SKIP_IF_NO_PGSQL();
    const char* ins = "INSERT INTO users VALUES (88, 'Temp', 20, 'Temp')";
    sql_parser::StringRef ins_sql{ins, static_cast<uint32_t>(strlen(ins))};
    exec_->execute_dml("test_pgsql", ins_sql);

    const char* q = "DELETE FROM users WHERE id = 88";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto dml = exec_->execute_dml("test_pgsql", sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 1u);
}

TEST_F(PgSQLExecutorTest, BadSqlNoCrash) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECTT INVALID SYNTAX!!!";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    EXPECT_TRUE(rs.empty());

    auto dml = exec_->execute_dml("test_pgsql", sql);
    EXPECT_FALSE(dml.success);
}

TEST_F(PgSQLExecutorTest, BadBackendNameNoCrash) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT 1";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("nonexistent_backend", sql);
    EXPECT_TRUE(rs.empty());
}

TEST_F(PgSQLExecutorTest, ConnectionToWrongPortNoCrash) {
    sql_engine::BackendConfig cfg;
    cfg.name = "bad_pgsql";
    cfg.host = "127.0.0.1";
    cfg.port = 19999;
    cfg.user = "postgres";
    cfg.password = "test";
    cfg.database = "testdb";
    cfg.dialect = sql_parser::Dialect::PostgreSQL;
    exec_->add_backend(cfg);

    const char* q = "SELECT 1";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("bad_pgsql", sql);
    EXPECT_TRUE(rs.empty());
}

TEST_F(PgSQLExecutorTest, JsonType) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT '{\"key\": \"value\"}'::json AS j";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_JSON);
}

TEST_F(PgSQLExecutorTest, DateType) {
    SKIP_IF_NO_PGSQL();
    const char* q = "SELECT DATE '2024-06-15' AS d";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_pgsql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_DATE);
}

} // namespace
