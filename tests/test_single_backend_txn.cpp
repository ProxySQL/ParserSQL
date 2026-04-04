// test_single_backend_txn.cpp — Single-backend transaction tests
// These tests require a running MySQL instance. Skipped if not available.

#include <gtest/gtest.h>
#include "sql_engine/single_backend_txn.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/dml_result.h"
#include "sql_parser/common.h"

#include <string>
#include <cstring>
#include <cstdlib>

using namespace sql_engine;
using namespace sql_parser;

// Check if MySQL is available via environment variable
static bool mysql_available() {
    const char* host = std::getenv("MYSQL_TEST_HOST");
    return host != nullptr && host[0] != '\0';
}

#define SKIP_IF_NO_MYSQL()                                                 \
    do {                                                                    \
        if (!mysql_available()) {                                           \
            GTEST_SKIP() << "MYSQL_TEST_HOST not set, skipping MySQL test"; \
        }                                                                   \
    } while (0)

// Mock RemoteExecutor for unit testing without a real backend
class TxnMockRemoteExecutor : public RemoteExecutor {
public:
    ResultSet execute(const char* /*backend_name*/, sql_parser::StringRef /*sql*/) override {
        return {};
    }

    DmlResult execute_dml(const char* /*backend_name*/, sql_parser::StringRef sql) override {
        DmlResult r;
        last_sql_.assign(sql.ptr, sql.len);
        sql_history_.push_back(last_sql_);
        r.success = !fail_next_;
        fail_next_ = false;
        return r;
    }

    void set_fail_next(bool fail) { fail_next_ = fail; }
    const std::string& last_sql() const { return last_sql_; }
    const std::vector<std::string>& sql_history() const { return sql_history_; }
    void clear_history() { sql_history_.clear(); }

private:
    std::string last_sql_;
    std::vector<std::string> sql_history_;
    bool fail_next_ = false;
};

// Unit tests with mock executor
TEST(SingleBackendTxn, BeginSendsSql) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    EXPECT_FALSE(txn.in_transaction());
    EXPECT_TRUE(txn.begin());
    EXPECT_TRUE(txn.in_transaction());
    EXPECT_EQ(mock.last_sql(), "BEGIN");
}

TEST(SingleBackendTxn, CommitSendsSql) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    txn.begin();
    EXPECT_TRUE(txn.commit());
    EXPECT_FALSE(txn.in_transaction());
    EXPECT_EQ(mock.last_sql(), "COMMIT");
}

TEST(SingleBackendTxn, RollbackSendsSql) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    txn.begin();
    EXPECT_TRUE(txn.rollback());
    EXPECT_FALSE(txn.in_transaction());
    EXPECT_EQ(mock.last_sql(), "ROLLBACK");
}

TEST(SingleBackendTxn, SavepointSendsSql) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    txn.begin();
    EXPECT_TRUE(txn.savepoint("sp1"));
    EXPECT_EQ(mock.last_sql(), "SAVEPOINT sp1");
}

TEST(SingleBackendTxn, RollbackToSavepoint) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    txn.begin();
    txn.savepoint("sp1");
    EXPECT_TRUE(txn.rollback_to("sp1"));
    EXPECT_EQ(mock.last_sql(), "ROLLBACK TO SAVEPOINT sp1");
}

TEST(SingleBackendTxn, ReleaseSavepoint) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    txn.begin();
    txn.savepoint("sp1");
    EXPECT_TRUE(txn.release_savepoint("sp1"));
    EXPECT_EQ(mock.last_sql(), "RELEASE SAVEPOINT sp1");
}

TEST(SingleBackendTxn, SavepointWithoutTxnFails) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    EXPECT_FALSE(txn.savepoint("sp1"));
}

TEST(SingleBackendTxn, BeginFailure) {
    TxnMockRemoteExecutor mock;
    mock.set_fail_next(true);
    SingleBackendTransactionManager txn(mock, "mysql_1");

    EXPECT_FALSE(txn.begin());
    EXPECT_FALSE(txn.in_transaction());
}

TEST(SingleBackendTxn, BackendName) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "pg_backend_1");
    EXPECT_EQ(txn.backend_name(), "pg_backend_1");
}

TEST(SingleBackendTxn, AutoCommitState) {
    TxnMockRemoteExecutor mock;
    SingleBackendTransactionManager txn(mock, "mysql_1");

    EXPECT_TRUE(txn.is_auto_commit());
    txn.set_auto_commit(false);
    EXPECT_FALSE(txn.is_auto_commit());
}

// Integration test (requires real MySQL)
TEST(SingleBackendTxnIntegration, BeginCommitRollback) {
    SKIP_IF_NO_MYSQL();
    // This test would use a real MySQL executor.
    // Skipped by default since it requires MySQL.
}
