#include <gtest/gtest.h>
#include "sql_engine/mysql_remote_executor.h"
#include "sql_engine/backend_config.h"
#include "sql_parser/arena.h"

#include <mysql/mysql.h>

namespace {

static const char* TEST_MYSQL_HOST = "127.0.0.1";
static const uint16_t TEST_MYSQL_PORT = 13306;
static const char* TEST_MYSQL_USER = "root";
static const char* TEST_MYSQL_PASS = "test";
static const char* TEST_MYSQL_DB = "testdb";

bool mysql_available() {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return false;
    unsigned int timeout = 2;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    bool ok = mysql_real_connect(conn, TEST_MYSQL_HOST, TEST_MYSQL_USER, TEST_MYSQL_PASS,
                                  TEST_MYSQL_DB, TEST_MYSQL_PORT, nullptr, 0) != nullptr;
    mysql_close(conn);
    return ok;
}

#define SKIP_IF_NO_MYSQL() if (!mysql_available()) { GTEST_SKIP() << "MySQL not available on port 13306"; }

sql_engine::BackendConfig make_mysql_config(const char* name = "test_mysql") {
    sql_engine::BackendConfig cfg;
    cfg.name = name;
    cfg.host = TEST_MYSQL_HOST;
    cfg.port = TEST_MYSQL_PORT;
    cfg.user = TEST_MYSQL_USER;
    cfg.password = TEST_MYSQL_PASS;
    cfg.database = TEST_MYSQL_DB;
    cfg.dialect = sql_parser::Dialect::MySQL;
    return cfg;
}

class MySQLExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        arena_ = std::make_unique<sql_parser::Arena>(65536, 1048576);
        exec_ = std::make_unique<sql_engine::MySQLRemoteExecutor>(*arena_);
        exec_->add_backend(make_mysql_config());
    }

    void TearDown() override {
        exec_->disconnect_all();
        exec_.reset();
        arena_.reset();
    }

    std::unique_ptr<sql_parser::Arena> arena_;
    std::unique_ptr<sql_engine::MySQLRemoteExecutor> exec_;
};

TEST_F(MySQLExecutorTest, SelectAllUsers) {
    SKIP_IF_NO_MYSQL();
    sql_parser::StringRef sql{"SELECT * FROM users", 19};
    auto rs = exec_->execute("test_mysql", sql);
    EXPECT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 4u);
    EXPECT_EQ(rs.column_names.size(), 4u);
}

TEST_F(MySQLExecutorTest, SelectWithWhere) {
    SKIP_IF_NO_MYSQL();
    const char* q = "SELECT * FROM users WHERE dept = 'Engineering'";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_mysql", sql);
    EXPECT_EQ(rs.row_count(), 3u);
}

TEST_F(MySQLExecutorTest, SelectWithOrderByLimit) {
    SKIP_IF_NO_MYSQL();
    const char* q = "SELECT * FROM users ORDER BY age ASC LIMIT 2";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_mysql", sql);
    EXPECT_EQ(rs.row_count(), 2u);
    // First row should be Bob (age 25)
    ASSERT_GE(rs.rows[0].column_count, 3u);
    EXPECT_EQ(rs.rows[0].get(2).tag, sql_engine::Value::TAG_INT64);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 25);
}

TEST_F(MySQLExecutorTest, TypeConversion) {
    SKIP_IF_NO_MYSQL();
    // Test INT type
    const char* q = "SELECT id, name, age, dept FROM users WHERE id = 1";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_mysql", sql);
    ASSERT_EQ(rs.row_count(), 1u);

    // id should be INT64
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_INT64);
    EXPECT_EQ(rs.rows[0].get(0).int_val, 1);

    // name should be STRING
    EXPECT_EQ(rs.rows[0].get(1).tag, sql_engine::Value::TAG_STRING);
    EXPECT_EQ(std::string(rs.rows[0].get(1).str_val.ptr, rs.rows[0].get(1).str_val.len), "Alice");

    // age should be INT64
    EXPECT_EQ(rs.rows[0].get(2).tag, sql_engine::Value::TAG_INT64);
    EXPECT_EQ(rs.rows[0].get(2).int_val, 30);
}

TEST_F(MySQLExecutorTest, DecimalType) {
    SKIP_IF_NO_MYSQL();
    const char* q = "SELECT total FROM orders WHERE id = 101";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_mysql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    // DECIMAL comes as numeric decimal type
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_DECIMAL);
}

TEST_F(MySQLExecutorTest, NullHandling) {
    SKIP_IF_NO_MYSQL();
    const char* q = "SELECT NULL AS x";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_mysql", sql);
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_TRUE(rs.rows[0].get(0).is_null());
}

TEST_F(MySQLExecutorTest, InsertAndReadBack) {
    SKIP_IF_NO_MYSQL();
    // Insert
    const char* ins = "INSERT INTO users VALUES (99, 'Test', 40, 'QA')";
    sql_parser::StringRef ins_sql{ins, static_cast<uint32_t>(strlen(ins))};
    auto dml = exec_->execute_dml("test_mysql", ins_sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 1u);

    // Read back
    const char* sel = "SELECT * FROM users WHERE id = 99";
    sql_parser::StringRef sel_sql{sel, static_cast<uint32_t>(strlen(sel))};
    auto rs = exec_->execute("test_mysql", sel_sql);
    EXPECT_EQ(rs.row_count(), 1u);

    // Cleanup
    const char* del = "DELETE FROM users WHERE id = 99";
    sql_parser::StringRef del_sql{del, static_cast<uint32_t>(strlen(del))};
    exec_->execute_dml("test_mysql", del_sql);
}

TEST_F(MySQLExecutorTest, UpdateAffectedRows) {
    SKIP_IF_NO_MYSQL();
    const char* q = "UPDATE users SET age = age + 1 WHERE dept = 'Sales'";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto dml = exec_->execute_dml("test_mysql", sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 2u);

    // Revert
    const char* revert = "UPDATE users SET age = age - 1 WHERE dept = 'Sales'";
    sql_parser::StringRef rev_sql{revert, static_cast<uint32_t>(strlen(revert))};
    exec_->execute_dml("test_mysql", rev_sql);
}

TEST_F(MySQLExecutorTest, DeleteAffectedRows) {
    SKIP_IF_NO_MYSQL();
    // Insert a test row first
    const char* ins = "INSERT INTO users VALUES (88, 'Temp', 20, 'Temp')";
    sql_parser::StringRef ins_sql{ins, static_cast<uint32_t>(strlen(ins))};
    exec_->execute_dml("test_mysql", ins_sql);

    const char* q = "DELETE FROM users WHERE id = 88";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto dml = exec_->execute_dml("test_mysql", sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 1u);
}

TEST_F(MySQLExecutorTest, BadSqlNosCrash) {
    SKIP_IF_NO_MYSQL();
    const char* q = "SELECTT INVALID SYNTAX!!!";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    // Should not crash, just return empty or error
    auto rs = exec_->execute("test_mysql", sql);
    EXPECT_TRUE(rs.empty());

    auto dml = exec_->execute_dml("test_mysql", sql);
    EXPECT_FALSE(dml.success);
    EXPECT_FALSE(dml.error_message.empty());
}

TEST_F(MySQLExecutorTest, BadBackendNameNoCrash) {
    SKIP_IF_NO_MYSQL();
    const char* q = "SELECT 1";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("nonexistent_backend", sql);
    EXPECT_TRUE(rs.empty());
}

TEST_F(MySQLExecutorTest, ConnectionToWrongPortNoCrash) {
    sql_engine::BackendConfig cfg;
    cfg.name = "bad_mysql";
    cfg.host = "127.0.0.1";
    cfg.port = 19999;  // unlikely to have anything here
    cfg.user = "root";
    cfg.password = "test";
    cfg.database = "testdb";
    cfg.dialect = sql_parser::Dialect::MySQL;
    exec_->add_backend(cfg);

    const char* q = "SELECT 1";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("bad_mysql", sql);
    EXPECT_TRUE(rs.empty());
}

TEST_F(MySQLExecutorTest, DateTimeTypes) {
    SKIP_IF_NO_MYSQL();
    // Test DATE, DATETIME, TIME types via literal expressions
    const char* q = "SELECT CAST('2024-06-15' AS DATE) AS d, "
                    "CAST('2024-06-15 14:30:00' AS DATETIME) AS dt, "
                    "CAST('14:30:00' AS TIME) AS t";
    sql_parser::StringRef sql{q, static_cast<uint32_t>(strlen(q))};
    auto rs = exec_->execute("test_mysql", sql);
    ASSERT_EQ(rs.row_count(), 1u);

    // DATE
    EXPECT_EQ(rs.rows[0].get(0).tag, sql_engine::Value::TAG_DATE);

    // DATETIME
    EXPECT_EQ(rs.rows[0].get(1).tag, sql_engine::Value::TAG_DATETIME);

    // TIME
    EXPECT_EQ(rs.rows[0].get(2).tag, sql_engine::Value::TAG_TIME);
}

} // namespace
