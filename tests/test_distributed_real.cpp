// test_distributed_real.cpp — Integration tests with real MySQL/PostgreSQL backends
//
// Tests the full distributed pipeline: parse SQL, plan, distribute, execute
// against real database backends. Skips gracefully if backends are not available.

#include <gtest/gtest.h>
#include "sql_engine/mysql_remote_executor.h"
#include "sql_engine/pgsql_remote_executor.h"
#include "sql_engine/multi_remote_executor.h"
#include "sql_engine/backend_config.h"
#include "sql_engine/distributed_planner.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/optimizer.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/parser.h"

#include <mysql/mysql.h>
#include <libpq-fe.h>
#include <cstring>
#include <string>

using namespace sql_engine;
using namespace sql_parser;

namespace {

static const char* TEST_MY_HOST = "127.0.0.1";
static const uint16_t TEST_MY_PORT = 13306;
static const char* TEST_PG_HOST = "127.0.0.1";
static const uint16_t TEST_PG_PORT = 15432;

bool mysql_available() {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return false;
    unsigned int timeout = 2;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    bool ok = mysql_real_connect(conn, TEST_MY_HOST, "root", "test",
                                  "testdb", TEST_MY_PORT, nullptr, 0) != nullptr;
    mysql_close(conn);
    return ok;
}

bool pgsql_available() {
    std::string conninfo = std::string("host=") + TEST_PG_HOST
        + " port=" + std::to_string(TEST_PG_PORT)
        + " user=postgres password=test dbname=testdb connect_timeout=2";
    PGconn* conn = PQconnectdb(conninfo.c_str());
    bool ok = (PQstatus(conn) == CONNECTION_OK);
    PQfinish(conn);
    return ok;
}

#define SKIP_IF_NO_MYSQL() if (!mysql_available()) { GTEST_SKIP() << "MySQL not available"; }
#define SKIP_IF_NO_PGSQL() if (!pgsql_available()) { GTEST_SKIP() << "PostgreSQL not available"; }

// Helper to register catalog matching our test schema
void register_test_catalog(InMemoryCatalog& catalog) {
    catalog.add_table("", "users", {
        {"id",   SqlType::make_int(),        false},
        {"name", SqlType::make_varchar(255), true},
        {"age",  SqlType::make_int(),        true},
        {"dept", SqlType::make_varchar(100), true}
    });
    catalog.add_table("", "orders", {
        {"id",      SqlType::make_int(),          false},
        {"user_id", SqlType::make_int(),          true},
        {"total",   SqlType::make_decimal(10, 2), true},
        {"status",  SqlType::make_varchar(50),    true}
    });
}

// ---- Distributed pipeline test with real MySQL backend ----

class DistributedRealMySQLTest : public ::testing::Test {
protected:
    void SetUp() override {
        exec_ = std::make_unique<MySQLRemoteExecutor>();

        BackendConfig cfg;
        cfg.name = "shard_1";
        cfg.host = TEST_MY_HOST;
        cfg.port = TEST_MY_PORT;
        cfg.user = "root";
        cfg.password = "test";
        cfg.database = "testdb";
        cfg.dialect = Dialect::MySQL;
        exec_->add_backend(cfg);

        register_test_catalog(catalog_);
        functions_.register_builtins();

        // Set up shard map: unsharded, single backend
        TableShardConfig users_cfg;
        users_cfg.table_name = "users";
        users_cfg.shards.push_back(ShardInfo{"shard_1"});
        shard_map_.add_table(users_cfg);

        TableShardConfig orders_cfg;
        orders_cfg.table_name = "orders";
        orders_cfg.shards.push_back(ShardInfo{"shard_1"});
        shard_map_.add_table(orders_cfg);
    }

    void TearDown() override {
        exec_->disconnect_all();
    }

    ResultSet run_distributed_query(const char* sql_str) {
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql_str, std::strlen(sql_str));
        if (result.status != ParseResult::OK || !result.ast) {
            return ResultSet{};
        }

        PlanBuilder<Dialect::MySQL> builder(catalog_, parser.arena());
        PlanNode* plan = builder.build(result.ast);
        if (!plan) return ResultSet{};

        DistributedPlanner<Dialect::MySQL> dp(shard_map_, catalog_, parser.arena());
        PlanNode* dist_plan = dp.distribute(plan);
        if (!dist_plan) return ResultSet{};

        PlanExecutor<Dialect::MySQL> executor(functions_, catalog_, parser.arena());
        executor.set_remote_executor(exec_.get());
        return executor.execute(dist_plan);
    }

    std::unique_ptr<MySQLRemoteExecutor> exec_;
    InMemoryCatalog catalog_;
    FunctionRegistry<Dialect::MySQL> functions_;
    ShardMap shard_map_;
};

TEST_F(DistributedRealMySQLTest, SelectAllUsersViaDistributed) {
    SKIP_IF_NO_MYSQL();
    auto rs = run_distributed_query("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 5u);
    EXPECT_EQ(rs.column_count, 4u);
}

TEST_F(DistributedRealMySQLTest, SelectWithWhereViaDistributed) {
    SKIP_IF_NO_MYSQL();
    auto rs = run_distributed_query("SELECT * FROM users WHERE age > 28");
    // Alice(30), Carol(35), Eve(32) = 3 rows
    EXPECT_EQ(rs.row_count(), 3u);
}

TEST_F(DistributedRealMySQLTest, SelectWithOrderByLimitViaDistributed) {
    SKIP_IF_NO_MYSQL();
    auto rs = run_distributed_query("SELECT * FROM users ORDER BY age LIMIT 3");
    EXPECT_EQ(rs.row_count(), 3u);
}

TEST_F(DistributedRealMySQLTest, SelectFromOrders) {
    SKIP_IF_NO_MYSQL();
    auto rs = run_distributed_query("SELECT * FROM orders");
    EXPECT_EQ(rs.row_count(), 4u);
}

// ---- Multi-backend test ----

class MultiExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        exec_ = std::make_unique<MultiRemoteExecutor>();
    }

    void TearDown() override {
        exec_->disconnect_all();
    }

    std::unique_ptr<MultiRemoteExecutor> exec_;
};

TEST_F(MultiExecutorTest, MySQLViaMulti) {
    SKIP_IF_NO_MYSQL();
    BackendConfig cfg;
    cfg.name = "mysql_1";
    cfg.host = TEST_MY_HOST;
    cfg.port = TEST_MY_PORT;
    cfg.user = "root";
    cfg.password = "test";
    cfg.database = "testdb";
    cfg.dialect = Dialect::MySQL;
    exec_->add_backend(cfg);

    sql_parser::StringRef sql{"SELECT * FROM users", 19};
    auto rs = exec_->execute("mysql_1", sql);
    EXPECT_EQ(rs.row_count(), 5u);
}

TEST_F(MultiExecutorTest, PgSQLViaMulti) {
    SKIP_IF_NO_PGSQL();
    BackendConfig cfg;
    cfg.name = "pgsql_1";
    cfg.host = TEST_PG_HOST;
    cfg.port = TEST_PG_PORT;
    cfg.user = "postgres";
    cfg.password = "test";
    cfg.database = "testdb";
    cfg.dialect = Dialect::PostgreSQL;
    exec_->add_backend(cfg);

    sql_parser::StringRef sql{"SELECT * FROM users", 19};
    auto rs = exec_->execute("pgsql_1", sql);
    EXPECT_EQ(rs.row_count(), 5u);
}

TEST_F(MultiExecutorTest, BothBackends) {
    SKIP_IF_NO_MYSQL();
    SKIP_IF_NO_PGSQL();

    BackendConfig mysql_cfg;
    mysql_cfg.name = "mysql_1";
    mysql_cfg.host = TEST_MY_HOST;
    mysql_cfg.port = TEST_MY_PORT;
    mysql_cfg.user = "root";
    mysql_cfg.password = "test";
    mysql_cfg.database = "testdb";
    mysql_cfg.dialect = Dialect::MySQL;
    exec_->add_backend(mysql_cfg);

    BackendConfig pgsql_cfg;
    pgsql_cfg.name = "pgsql_1";
    pgsql_cfg.host = TEST_PG_HOST;
    pgsql_cfg.port = TEST_PG_PORT;
    pgsql_cfg.user = "postgres";
    pgsql_cfg.password = "test";
    pgsql_cfg.database = "testdb";
    pgsql_cfg.dialect = Dialect::PostgreSQL;
    exec_->add_backend(pgsql_cfg);

    sql_parser::StringRef sql{"SELECT * FROM users", 19};

    auto rs_mysql = exec_->execute("mysql_1", sql);
    EXPECT_EQ(rs_mysql.row_count(), 5u);

    auto rs_pgsql = exec_->execute("pgsql_1", sql);
    EXPECT_EQ(rs_pgsql.row_count(), 5u);
}

TEST_F(MultiExecutorTest, DmlViaMySQLMulti) {
    SKIP_IF_NO_MYSQL();
    BackendConfig cfg;
    cfg.name = "mysql_1";
    cfg.host = TEST_MY_HOST;
    cfg.port = TEST_MY_PORT;
    cfg.user = "root";
    cfg.password = "test";
    cfg.database = "testdb";
    cfg.dialect = Dialect::MySQL;
    exec_->add_backend(cfg);

    const char* ins = "INSERT INTO users VALUES (77, 'Multi', 50, 'Test')";
    sql_parser::StringRef ins_sql{ins, static_cast<uint32_t>(strlen(ins))};
    auto dml = exec_->execute_dml("mysql_1", ins_sql);
    EXPECT_TRUE(dml.success);
    EXPECT_EQ(dml.affected_rows, 1u);

    // Cleanup
    const char* del = "DELETE FROM users WHERE id = 77";
    sql_parser::StringRef del_sql{del, static_cast<uint32_t>(strlen(del))};
    exec_->execute_dml("mysql_1", del_sql);
}

TEST_F(MultiExecutorTest, UnknownBackendReturnsEmpty) {
    sql_parser::StringRef sql{"SELECT 1", 8};
    auto rs = exec_->execute("unknown", sql);
    EXPECT_TRUE(rs.empty());
}

} // namespace
