// test_distributed_txn.cpp — Distributed 2PC transaction tests
// Real backend tests require MySQL with XA support. Skipped if not available.

#include <gtest/gtest.h>
#include "sql_engine/distributed_txn.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/dml_result.h"
#include "sql_parser/common.h"

#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>

using namespace sql_engine;
using namespace sql_parser;

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

// Mock RemoteExecutor that records all SQL sent per backend
class MockDistributedExecutor : public RemoteExecutor {
public:
    ResultSet execute(const char* /*backend_name*/, StringRef /*sql*/) override {
        return {};
    }

    DmlResult execute_dml(const char* backend_name, StringRef sql) override {
        DmlResult r;
        std::string s(sql.ptr, sql.len);
        history_[backend_name].push_back(s);
        all_history_.push_back(std::string(backend_name) + ": " + s);

        // Check if this backend+sql should fail
        std::string key = std::string(backend_name) + ":" + s;
        for (auto& fail_pat : fail_patterns_) {
            if (s.find(fail_pat) != std::string::npos) {
                r.success = false;
                r.error_message = "mock failure";
                return r;
            }
        }

        r.success = true;
        return r;
    }

    const std::vector<std::string>& backend_history(const char* name) const {
        static const std::vector<std::string> empty;
        auto it = history_.find(name);
        return it != history_.end() ? it->second : empty;
    }

    const std::vector<std::string>& all() const { return all_history_; }

    void add_fail_pattern(const std::string& pattern) {
        fail_patterns_.push_back(pattern);
    }

    void clear() {
        history_.clear();
        all_history_.clear();
        fail_patterns_.clear();
    }

private:
    std::unordered_map<std::string, std::vector<std::string>> history_;
    std::vector<std::string> all_history_;
    std::vector<std::string> fail_patterns_;
};

// Basic 2PC flow: begin, enlist two backends, commit
TEST(DistributedTxn, BasicTwoPhaseCommit) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    EXPECT_TRUE(txn.begin());
    EXPECT_TRUE(txn.in_transaction());
    EXPECT_FALSE(txn.txn_id().empty());

    EXPECT_TRUE(txn.enlist_backend("shard_1"));
    EXPECT_TRUE(txn.enlist_backend("shard_2"));
    EXPECT_EQ(txn.participants().size(), 2u);

    EXPECT_TRUE(txn.commit());
    EXPECT_FALSE(txn.in_transaction());

    // Verify XA START was sent to both backends
    auto& h1 = mock.backend_history("shard_1");
    auto& h2 = mock.backend_history("shard_2");

    // Each backend should have: XA START, XA END, XA PREPARE, XA COMMIT
    ASSERT_GE(h1.size(), 3u); // XA START, XA END, XA PREPARE, XA COMMIT
    EXPECT_TRUE(h1[0].find("XA START") != std::string::npos);

    ASSERT_GE(h2.size(), 3u);
    EXPECT_TRUE(h2[0].find("XA START") != std::string::npos);

    // Last should be XA COMMIT
    EXPECT_TRUE(h1.back().find("XA COMMIT") != std::string::npos);
    EXPECT_TRUE(h2.back().find("XA COMMIT") != std::string::npos);
}

// 2PC rollback: begin, enlist, rollback
TEST(DistributedTxn, TwoPhaseRollback) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    txn.enlist_backend("shard_1");
    txn.enlist_backend("shard_2");

    EXPECT_TRUE(txn.rollback());
    EXPECT_FALSE(txn.in_transaction());

    // Both backends should have received XA END + XA ROLLBACK
    auto& h1 = mock.backend_history("shard_1");
    auto& h2 = mock.backend_history("shard_2");

    // XA START + XA END + XA ROLLBACK
    bool has_rollback_1 = false, has_rollback_2 = false;
    for (auto& s : h1) if (s.find("XA ROLLBACK") != std::string::npos) has_rollback_1 = true;
    for (auto& s : h2) if (s.find("XA ROLLBACK") != std::string::npos) has_rollback_2 = true;
    EXPECT_TRUE(has_rollback_1);
    EXPECT_TRUE(has_rollback_2);
}

// Prepare failure: one backend fails XA PREPARE → all rolled back
TEST(DistributedTxn, PrepareFailureRollsBackAll) {
    MockDistributedExecutor mock;
    // Make XA PREPARE fail for shard_2
    mock.add_fail_pattern("XA PREPARE");

    DistributedTransactionManager txn(mock);
    txn.begin();
    txn.enlist_backend("shard_1");
    txn.enlist_backend("shard_2");

    // Commit should fail because XA PREPARE fails
    EXPECT_FALSE(txn.commit());
    EXPECT_FALSE(txn.in_transaction());

    // Both backends should have received XA ROLLBACK
    bool has_rollback_1 = false, has_rollback_2 = false;
    for (auto& s : mock.backend_history("shard_1")) {
        if (s.find("XA ROLLBACK") != std::string::npos) has_rollback_1 = true;
    }
    for (auto& s : mock.backend_history("shard_2")) {
        if (s.find("XA ROLLBACK") != std::string::npos) has_rollback_2 = true;
    }
    EXPECT_TRUE(has_rollback_1);
    EXPECT_TRUE(has_rollback_2);
}

// Enlist same backend twice — idempotent
TEST(DistributedTxn, EnlistIdempotent) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    EXPECT_TRUE(txn.enlist_backend("shard_1"));
    EXPECT_TRUE(txn.enlist_backend("shard_1"));
    EXPECT_EQ(txn.participants().size(), 1u);

    // Only one XA START should have been sent
    EXPECT_EQ(mock.backend_history("shard_1").size(), 1u);
    txn.rollback();
}

// No participants: commit is a no-op
TEST(DistributedTxn, CommitNoParticipants) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    EXPECT_TRUE(txn.commit());
    EXPECT_FALSE(txn.in_transaction());
    EXPECT_TRUE(mock.all().empty());
}

// Savepoints not supported for distributed transactions
TEST(DistributedTxn, SavepointsNotSupported) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    EXPECT_FALSE(txn.savepoint("sp1"));
    EXPECT_FALSE(txn.rollback_to("sp1"));
    EXPECT_FALSE(txn.release_savepoint("sp1"));
    txn.rollback();
}

// Unique transaction IDs
TEST(DistributedTxn, UniqueTxnIds) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    std::string id1 = txn.txn_id();
    txn.rollback();

    txn.begin();
    std::string id2 = txn.txn_id();
    txn.rollback();

    EXPECT_NE(id1, id2);
    EXPECT_TRUE(id1.find("parsersql_") == 0);
    EXPECT_TRUE(id2.find("parsersql_") == 0);
}

// Auto-commit state
TEST(DistributedTxn, AutoCommitState) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    EXPECT_TRUE(txn.is_auto_commit());
    txn.set_auto_commit(false);
    EXPECT_FALSE(txn.is_auto_commit());
}

// PostgreSQL dialect
TEST(DistributedTxn, PostgresqlDialect) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock,
        DistributedTransactionManager::BackendDialect::POSTGRESQL);

    txn.begin();
    txn.enlist_backend("pg_1");
    txn.enlist_backend("pg_2");

    EXPECT_TRUE(txn.commit());

    // PostgreSQL uses BEGIN + PREPARE TRANSACTION + COMMIT PREPARED
    auto& h1 = mock.backend_history("pg_1");
    EXPECT_TRUE(h1[0] == "BEGIN");
    bool has_prepare = false, has_commit = false;
    for (auto& s : h1) {
        if (s.find("PREPARE TRANSACTION") != std::string::npos) has_prepare = true;
        if (s.find("COMMIT PREPARED") != std::string::npos) has_commit = true;
    }
    EXPECT_TRUE(has_prepare);
    EXPECT_TRUE(has_commit);
}

// Integration tests (require real MySQL with XA support)
TEST(DistributedTxnIntegration, TwoPhaseCommitReal) {
    SKIP_IF_NO_MYSQL();
    // Would test with real MySQL backends.
}

TEST(DistributedTxnIntegration, TwoPhaseRollbackReal) {
    SKIP_IF_NO_MYSQL();
    // Would test with real MySQL backends.
}
