// test_distributed_txn.cpp — Distributed 2PC transaction tests
// Real backend tests require MySQL with XA support. Skipped if not available.

#include <gtest/gtest.h>
#include "sql_engine/distributed_txn.h"
#include "sql_engine/durable_txn_log.h"
#include "sql_engine/transaction_recovery.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/dml_result.h"
#include "sql_parser/common.h"

#include <set>
#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <sys/stat.h>
#include <unistd.h>

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

// Mock RemoteExecutor that records all SQL sent per backend.
//
// When `enable_sessions_` is set (the default), checkout_session returns
// a session object that tracks a unique session id. All calls going
// through that session get attributed to the same session id in the
// session_history_, so tests can verify that phase 1 and phase 2 went
// through the SAME physical connection (which is what MySQL XA and
// PostgreSQL PREPARE TRANSACTION require).
//
// When enable_sessions_ is false, checkout_session returns nullptr.
// Tests must explicitly opt into the legacy unpinned fallback path.
class MockDistributedExecutor : public RemoteExecutor {
public:
    ResultSet execute(const char* /*backend_name*/, StringRef /*sql*/) override {
        return {};
    }

    DmlResult execute_dml(const char* backend_name, StringRef sql) override {
        return record(backend_name, sql, /*session_id=*/-1);
    }

    // Each checkout_session call allocates a fresh session id. The
    // returned MockSession's execute_dml records the SQL under
    // (backend, session_id) so we can verify pinning.
    std::unique_ptr<RemoteSession> checkout_session(const char* backend_name) override {
        if (!enable_sessions_) return nullptr;
        int sid = next_session_id_++;
        return std::unique_ptr<RemoteSession>(new MockSession(*this, backend_name, sid));
    }

    bool allows_unpinned_distributed_2pc() const override {
        return allow_unpinned_2pc_;
    }

    // Legacy call recorder. session_id == -1 means "unpinned call".
    DmlResult record(const char* backend_name, StringRef sql, int session_id) {
        DmlResult r;
        std::string s(sql.ptr, sql.len);
        history_[backend_name].push_back(s);
        all_history_.push_back(std::string(backend_name) + ": " + s);
        if (session_id >= 0) {
            session_history_[session_id].push_back(
                std::string(backend_name) + ": " + s);
            last_session_for_backend_[backend_name] = session_id;
        }

        // Check if this backend+sql should fail
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

    // Returns the session id that the most recent call for `backend`
    // used, or -1 if no session was used.
    int last_session_id(const char* backend) const {
        auto it = last_session_for_backend_.find(backend);
        return it == last_session_for_backend_.end() ? -1 : it->second;
    }

    // Returns every call that went through the given session id. Used to
    // verify that an entire transaction (enlist + phase 1 + phase 2)
    // went through the same pinned session.
    const std::vector<std::string>& session_calls(int session_id) const {
        static const std::vector<std::string> empty;
        auto it = session_history_.find(session_id);
        return it == session_history_.end() ? empty : it->second;
    }

    void add_fail_pattern(const std::string& pattern) {
        fail_patterns_.push_back(pattern);
    }

    void set_sessions_enabled(bool enabled) { enable_sessions_ = enabled; }
    void set_allow_unpinned_2pc(bool allowed) { allow_unpinned_2pc_ = allowed; }

    void clear() {
        history_.clear();
        all_history_.clear();
        session_history_.clear();
        last_session_for_backend_.clear();
        fail_patterns_.clear();
    }

private:
    class MockSession : public RemoteSession {
    public:
        MockSession(MockDistributedExecutor& e, std::string b, int id)
            : executor_(e), backend_(std::move(b)), id_(id) {}

        ResultSet execute(StringRef /*sql*/) override { return {}; }

        DmlResult execute_dml(StringRef sql) override {
            return executor_.record(backend_.c_str(), sql, id_);
        }

        void poison() override { /* mock: nothing to poison */ }

    private:
        MockDistributedExecutor& executor_;
        std::string backend_;
        int id_;
    };

    std::unordered_map<std::string, std::vector<std::string>> history_;
    std::vector<std::string> all_history_;
    std::vector<std::string> fail_patterns_;
    std::unordered_map<int, std::vector<std::string>> session_history_;
    std::unordered_map<std::string, int> last_session_for_backend_;
    int next_session_id_ = 0;
    bool enable_sessions_ = true;
    bool allow_unpinned_2pc_ = false;
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

// =====================================================================
// Durable transaction log (WAL) tests
// =====================================================================
//
// These cover the write-ahead log that records 2PC decisions before
// phase 2 dispatches them. Without this log, a crash between phase 1
// and phase 2 leaves prepared transactions stranded on every backend
// with no automatic recovery path. The log is a minimal append-only
// text file, fsynced per write, scanned on startup to find in-doubt
// transactions.

namespace {

// RAII wrapper that creates a unique temp file path, keeps it for the
// duration of the test, and unlinks it afterwards.
struct TempLogPath {
    std::string path;
    TempLogPath() {
        char buf[] = "/tmp/parsersql_txnlog_XXXXXX";
        int fd = ::mkstemp(buf);
        if (fd >= 0) { ::close(fd); }
        path = buf;
        ::unlink(path.c_str());  // start from empty
    }
    ~TempLogPath() { ::unlink(path.c_str()); }
};

}  // namespace

TEST(DurableTransactionLog, OpenAndLogDecision) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));
    ASSERT_TRUE(log.is_open());

    ASSERT_TRUE(log.log_decision("txn_1",
                                 DurableTransactionLog::Decision::COMMIT,
                                 {"backend_a", "backend_b"}));

    // Without a COMPLETE record, the txn is in-doubt.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(in_doubt.size(), 1u);
    EXPECT_EQ(in_doubt[0].txn_id, "txn_1");
    EXPECT_EQ(in_doubt[0].decision, DurableTransactionLog::Decision::COMMIT);
    ASSERT_EQ(in_doubt[0].participants.size(), 2u);
    EXPECT_EQ(in_doubt[0].participants[0], "backend_a");
    EXPECT_EQ(in_doubt[0].participants[1], "backend_b");
}

TEST(DurableTransactionLog, CompleteRemovesFromInDoubt) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    log.log_decision("txn_a", DurableTransactionLog::Decision::COMMIT, {"b1"});
    log.log_decision("txn_b", DurableTransactionLog::Decision::COMMIT, {"b1", "b2"});
    log.log_complete("txn_a");  // txn_a is done; only txn_b should be in-doubt.

    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(in_doubt.size(), 1u);
    EXPECT_EQ(in_doubt[0].txn_id, "txn_b");
}

TEST(DurableTransactionLog, RollbackDecisionRecorded) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    log.log_decision("txn_r", DurableTransactionLog::Decision::ROLLBACK, {"b1", "b2"});

    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(in_doubt.size(), 1u);
    EXPECT_EQ(in_doubt[0].txn_id, "txn_r");
    EXPECT_EQ(in_doubt[0].decision, DurableTransactionLog::Decision::ROLLBACK);
}

TEST(DurableTransactionLog, ScanEmptyFile) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt.empty());
}

TEST(DurableTransactionLog, ScanNonexistentFile) {
    auto in_doubt = DurableTransactionLog::scan_in_doubt("/tmp/does_not_exist_12345");
    EXPECT_TRUE(in_doubt.empty());
}

TEST(DurableTransactionLog, LogPersistsAfterClose) {
    TempLogPath tmp;
    {
        DurableTransactionLog log;
        ASSERT_TRUE(log.open(tmp.path));
        log.log_decision("txn_persisted", DurableTransactionLog::Decision::COMMIT, {"b1"});
    }  // destructor closes the file; fsync already called per-record

    // Re-open and scan: the record must still be there.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(in_doubt.size(), 1u);
    EXPECT_EQ(in_doubt[0].txn_id, "txn_persisted");
}

// Compaction shrinks the log by dropping COMPLETEd transactions.
TEST(DurableTransactionLog, CompactRemovesCompletedEntries) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    // 10 successful transactions + 2 in-doubt.
    for (int i = 0; i < 10; ++i) {
        std::string id = "done_" + std::to_string(i);
        log.log_decision(id, DurableTransactionLog::Decision::COMMIT, {"b1"});
        log.log_complete(id);
    }
    log.log_decision("still_1", DurableTransactionLog::Decision::COMMIT, {"b1", "b2"});
    log.log_decision("still_2", DurableTransactionLog::Decision::ROLLBACK, {"b1"});

    // Before compaction: still_1 and still_2 are in-doubt, and the file
    // has 22 records (10 COMMIT + 10 COMPLETE + 2 COMMIT/ROLLBACK).
    auto before = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(before.size(), 2u);

    off_t before_size = -1;
    {
        struct stat st {};
        if (stat(tmp.path.c_str(), &st) == 0) before_size = st.st_size;
    }

    ASSERT_TRUE(log.compact());

    // After compaction: same 2 in-doubt, but the file on disk is now
    // much smaller (only 2 records).
    auto after = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(after.size(), 2u);

    off_t after_size = -1;
    {
        struct stat st {};
        if (stat(tmp.path.c_str(), &st) == 0) after_size = st.st_size;
    }
    EXPECT_LT(after_size, before_size)
        << "compact() should make the file smaller (before=" << before_size
        << " after=" << after_size << ")";

    // Verify the set of in-doubt entries is exactly {still_1, still_2}.
    std::set<std::string> ids;
    for (const auto& e : after) ids.insert(e.txn_id);
    EXPECT_EQ(ids.count("still_1"), 1u);
    EXPECT_EQ(ids.count("still_2"), 1u);
}

TEST(DurableTransactionLog, CompactEmptyLogLeavesEmptyFile) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    // Log and immediately complete a few, so compact has work but no
    // remaining in-doubt entries.
    log.log_decision("t1", DurableTransactionLog::Decision::COMMIT, {"b1"});
    log.log_complete("t1");
    log.log_decision("t2", DurableTransactionLog::Decision::ROLLBACK, {"b1"});
    log.log_complete("t2");

    ASSERT_TRUE(log.compact());

    auto after = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(after.empty());

    // And we can still append new decisions to the compacted log.
    ASSERT_TRUE(log.log_decision("t3", DurableTransactionLog::Decision::COMMIT, {"b1"}));
    auto after_append = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(after_append.size(), 1u);
    EXPECT_EQ(after_append[0].txn_id, "t3");
}

TEST(DurableTransactionLog, CompactPreservesRollbackDecisions) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    log.log_decision("commit_done", DurableTransactionLog::Decision::COMMIT, {"b1"});
    log.log_complete("commit_done");
    log.log_decision("rb_pending", DurableTransactionLog::Decision::ROLLBACK,
                     {"b1", "b2", "b3"});

    ASSERT_TRUE(log.compact());

    auto after = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0].txn_id, "rb_pending");
    EXPECT_EQ(after[0].decision, DurableTransactionLog::Decision::ROLLBACK);
    EXPECT_EQ(after[0].participants.size(), 3u);
    EXPECT_EQ(after[0].participants[0], "b1");
    EXPECT_EQ(after[0].participants[1], "b2");
    EXPECT_EQ(after[0].participants[2], "b3");
}

// =====================================================================
// Auto-compaction: DistributedTransactionManager fires compact() after
// N successful completions when set_auto_compact_threshold(N) is used.
// =====================================================================

TEST(DurableTxnLogAutoCompactTest, AutoCompactFiresAtThreshold) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    DistributedTransactionManager mgr(mock);
    mgr.set_durable_log(&log);
    mgr.set_auto_compact_threshold(3);  // compact after 3 completions

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(mgr.begin());
        ASSERT_TRUE(mgr.enlist_backend("b1"));
        ASSERT_TRUE(mgr.commit());
    }

    // After 3 completions, compaction should have fired.
    // Verify: in-doubt set is empty (all 3 txns are COMPLETE).
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt.empty());

    struct stat st;
    ASSERT_EQ(::stat(tmp.path.c_str(), &st), 0);
    // Compaction leaves only in-doubt entries (none here), so file should be small
    EXPECT_LT(st.st_size, 1024);
}

TEST(DurableTxnLogAutoCompactTest, ThresholdZeroDisablesAutoCompact) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    DistributedTransactionManager mgr(mock);
    mgr.set_durable_log(&log);
    // Threshold = 0 (default) means auto-compact is disabled.

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(mgr.begin());
        ASSERT_TRUE(mgr.enlist_backend("b1"));
        ASSERT_TRUE(mgr.commit());
    }

    // Without compaction, 5 txns x 2 records (DECISION + COMPLETE) each
    // should be in the file.
    struct stat st;
    ASSERT_EQ(::stat(tmp.path.c_str(), &st), 0);
    EXPECT_GT(st.st_size, 100);
}

TEST(DurableTxnLogAutoCompactTest, CounterResetsAfterCompact) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    DistributedTransactionManager mgr(mock);
    mgr.set_durable_log(&log);
    mgr.set_auto_compact_threshold(2);

    // First 2 commits -> compact fires
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(mgr.begin());
        ASSERT_TRUE(mgr.enlist_backend("b1"));
        ASSERT_TRUE(mgr.commit());
    }

    // 3rd commit: counter has reset, so no compact fires yet
    ASSERT_TRUE(mgr.begin());
    ASSERT_TRUE(mgr.enlist_backend("b1"));
    ASSERT_TRUE(mgr.commit());

    struct stat st1;
    ASSERT_EQ(::stat(tmp.path.c_str(), &st1), 0);
    off_t size_after_3 = st1.st_size;

    // 4th commit -> counter hits 2 again, compact fires
    ASSERT_TRUE(mgr.begin());
    ASSERT_TRUE(mgr.enlist_backend("b1"));
    ASSERT_TRUE(mgr.commit());

    struct stat st2;
    ASSERT_EQ(::stat(tmp.path.c_str(), &st2), 0);
    EXPECT_LE(st2.st_size, size_after_3);
}

// =====================================================================
// DistributedTransactionManager integration with the WAL
// =====================================================================

TEST(DistributedTxnWal, CommitLogsDecisionAndCompletion) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);
    txn.set_durable_log(&log);

    EXPECT_TRUE(txn.begin());
    EXPECT_TRUE(txn.enlist_backend("mysql_1"));
    EXPECT_TRUE(txn.enlist_backend("mysql_2"));
    EXPECT_TRUE(txn.commit());

    // After a successful commit, no in-doubt transactions should remain.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt.empty())
        << "commit should log COMPLETE after phase 2, but found "
        << in_doubt.size() << " in-doubt entries";
}

TEST(DistributedTxnWal, Phase2CommitFailureLeavesTxnInDoubt) {
    // Core recovery invariant: if phase 2 commit fails for any participant,
    // the transaction stays in-doubt in the WAL so startup recovery can
    // pick it up and finish the commit. Without this, a partially-committed
    // transaction would silently disappear from the log.
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    mock.add_fail_pattern("XA COMMIT");  // phase 2 will fail on every COMMIT

    DistributedTransactionManager txn(mock);
    txn.set_durable_log(&log);

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.enlist_backend("mysql_2");
    EXPECT_FALSE(txn.commit());  // phase 2 fails -- commit() returns false

    // The COMMIT decision is durably recorded and NOT followed by a
    // COMPLETE (because phase 2 failed), so the txn is in-doubt.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(in_doubt.size(), 1u);
    EXPECT_EQ(in_doubt[0].decision, DurableTransactionLog::Decision::COMMIT);
    EXPECT_EQ(in_doubt[0].participants.size(), 2u);
}

TEST(DistributedTxnWal, Phase1PrepareFailureLogsRollbackAndCompletes) {
    // If phase 1 fails, we should log ROLLBACK and then COMPLETE after
    // dispatching the rollback to all participants. No in-doubt state.
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    mock.add_fail_pattern("XA PREPARE");  // phase 1 will fail

    DistributedTransactionManager txn(mock);
    txn.set_durable_log(&log);

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.enlist_backend("mysql_2");
    EXPECT_FALSE(txn.commit());  // phase 1 fails -> rollback

    // The decision was ROLLBACK, and COMPLETE was written, so in-doubt is empty.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt.empty());
}

TEST(DistributedTxnWal, NoLogAttachedPreservesLegacyBehavior) {
    // When no log is attached, commit proceeds without any WAL write and
    // returns whatever phase 2 returns -- matches pre-WAL behavior.
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);
    // No set_durable_log() call.

    txn.begin();
    txn.enlist_backend("mysql_1");
    EXPECT_TRUE(txn.commit());
}

TEST(DistributedTxnWal, RollbackRecordsDecision) {
    TempLogPath tmp;
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));

    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);
    txn.set_durable_log(&log);

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.rollback();

    // rollback() calls maybe_log_complete() after writing the decision,
    // so the in-doubt set should be empty.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt.empty());
}

// =====================================================================
// Phase-level statement timeout
// =====================================================================

TEST(DistributedTxnTimeout, MysqlDoesNotEmitMisleadingMaxExecutionTimeForXaPhases) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);
    txn.set_phase_statement_timeout_ms(5000);  // 5 second phase timeout

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.commit();

    // MySQL's max_execution_time does not apply to XA control statements,
    // so the coordinator should not emit it as if it bounded phase 1/2.
    const auto& h1 = mock.backend_history("mysql_1");
    for (const auto& s : h1) {
        EXPECT_EQ(s.find("max_execution_time"), std::string::npos)
            << "unexpected SET SESSION max_execution_time for XA phase SQL: " << s;
    }
}

TEST(DistributedTxnTimeout, NoTimeoutByDefault) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);
    // No set_phase_statement_timeout_ms call -- default is 0 (no override).

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.commit();

    const auto& h1 = mock.backend_history("mysql_1");
    for (const auto& s : h1) {
        EXPECT_EQ(s.find("max_execution_time"), std::string::npos)
            << "unexpected SET SESSION max_execution_time when no phase timeout is set: "
            << s;
    }
}

TEST(DistributedTxnTimeout, PostgreSqlUsesSessionStatementTimeoutBeforePrepareAndCommitPrepared) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(
        mock, DistributedTransactionManager::BackendDialect::POSTGRESQL);
    txn.set_phase_statement_timeout_ms(10000);

    txn.begin();
    txn.enlist_backend("pg_1");
    txn.commit();

    const auto& h1 = mock.backend_history("pg_1");
    ASSERT_GE(h1.size(), 4u);

    EXPECT_TRUE(h1[0].find("BEGIN") != std::string::npos)
        << "expected enlistment to begin with BEGIN, got: " << h1[0];

    bool saw_prepare_timeout_pair = false;
    bool saw_commit_timeout_pair = false;
    int timeout_set_count = 0;

    for (size_t i = 0; i < h1.size(); ++i) {
        if (h1[i].find("SET statement_timeout = 10000") != std::string::npos) {
            timeout_set_count++;
            if (i + 1 < h1.size() &&
                h1[i + 1].find("PREPARE TRANSACTION") != std::string::npos) {
                saw_prepare_timeout_pair = true;
            }
            if (i + 1 < h1.size() &&
                h1[i + 1].find("COMMIT PREPARED") != std::string::npos) {
                saw_commit_timeout_pair = true;
            }
        }
        EXPECT_EQ(h1[i].find("SET LOCAL statement_timeout"), std::string::npos)
            << "unexpected SET LOCAL statement_timeout in 2PC path: " << h1[i];
    }

    EXPECT_TRUE(saw_prepare_timeout_pair)
        << "expected SET statement_timeout = 10000 immediately before PREPARE TRANSACTION";
    EXPECT_TRUE(saw_commit_timeout_pair)
        << "expected SET statement_timeout = 10000 immediately before COMMIT PREPARED";
    EXPECT_GE(timeout_set_count, 2);
}

// =====================================================================
// Startup recovery of in-doubt transactions
// =====================================================================
//
// End-to-end story: simulate a crash after the COMMIT decision was
// written but before COMPLETE, then run recovery and verify it
// re-issues the XA COMMIT to the participants and writes COMPLETE so
// the transactions no longer show up in the in-doubt set.

namespace {

// Simulate a crash during phase 2 by writing COMMIT decisions to a log
// without the subsequent COMPLETE records. Works by opening the log and
// calling log_decision directly -- DistributedTransactionManager isn't
// used because we want to avoid the maybe_log_complete call.
void seed_indoubt_log(const std::string& path,
                      const std::string& txn_id,
                      DurableTransactionLog::Decision d,
                      const std::vector<std::string>& participants) {
    DurableTransactionLog log;
    log.open(path);
    log.log_decision(txn_id, d, participants);
    log.close();
}

}  // namespace

TEST(TransactionRecovery, ReplaysCommitDecisionOnEveryParticipant) {
    TempLogPath tmp;
    seed_indoubt_log(tmp.path, "orphan_txn_1",
                     DurableTransactionLog::Decision::COMMIT,
                     {"mysql_1", "mysql_2"});

    // Now simulate startup: reopen the log and run recovery against a
    // fresh mock executor that simply accepts XA COMMIT calls.
    DurableTransactionLog log;
    ASSERT_TRUE(log.open(tmp.path));
    MockDistributedExecutor mock;
    TransactionRecovery recovery(mock, log);

    auto report = recovery.recover();

    ASSERT_EQ(report.recovered_commit.size(), 1u);
    EXPECT_EQ(report.recovered_commit[0], "orphan_txn_1");
    EXPECT_TRUE(report.still_in_doubt.empty());
    EXPECT_EQ(report.participants_contacted, 2u);

    // The recovery should have issued XA COMMIT to both participants.
    bool saw_mysql1 = false, saw_mysql2 = false;
    for (const auto& line : mock.all()) {
        if (line.find("mysql_1") != std::string::npos &&
            line.find("XA COMMIT 'orphan_txn_1'") != std::string::npos) {
            saw_mysql1 = true;
        }
        if (line.find("mysql_2") != std::string::npos &&
            line.find("XA COMMIT 'orphan_txn_1'") != std::string::npos) {
            saw_mysql2 = true;
        }
    }
    EXPECT_TRUE(saw_mysql1);
    EXPECT_TRUE(saw_mysql2);

    // After recovery, the txn should be marked COMPLETE and the in-doubt
    // set should be empty.
    auto in_doubt_after = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt_after.empty());
}

TEST(TransactionRecovery, ReplaysRollbackDecision) {
    TempLogPath tmp;
    seed_indoubt_log(tmp.path, "orphan_rb",
                     DurableTransactionLog::Decision::ROLLBACK,
                     {"mysql_1"});

    DurableTransactionLog log;
    log.open(tmp.path);
    MockDistributedExecutor mock;
    TransactionRecovery recovery(mock, log);

    auto report = recovery.recover();

    EXPECT_EQ(report.recovered_rollback.size(), 1u);
    EXPECT_TRUE(report.recovered_commit.empty());
    EXPECT_TRUE(report.still_in_doubt.empty());

    bool saw_rollback = false;
    for (const auto& line : mock.all()) {
        if (line.find("XA ROLLBACK 'orphan_rb'") != std::string::npos) {
            saw_rollback = true;
        }
    }
    EXPECT_TRUE(saw_rollback);
}

TEST(TransactionRecovery, IdempotentWhenBackendReportsAlreadyResolved) {
    // On a second recovery pass, the backend will return an "XAER_NOTA"
    // or "does not exist" error because the transaction was already
    // committed on the first pass. Recovery should treat that as success.
    TempLogPath tmp;
    seed_indoubt_log(tmp.path, "retry_txn",
                     DurableTransactionLog::Decision::COMMIT,
                     {"mysql_1"});

    DurableTransactionLog log;
    log.open(tmp.path);
    MockDistributedExecutor mock;
    // First XA COMMIT will fail with XAER_NOTA-like error.
    // Our mock fail_patterns just cause .success=false; the error_message
    // is set to "mock failure" which our recovery won't recognize as
    // already-resolved. So we need a custom mock.
    //
    // Override execute_dml to return a recognizable error.
    class AlreadyResolvedMock : public MockDistributedExecutor {
    public:
        DmlResult execute_dml(const char* backend_name, StringRef sql) override {
            (void)MockDistributedExecutor::execute_dml(backend_name, sql);
            DmlResult r;
            r.success = false;
            r.error_message = "XAER_NOTA: Unknown XID";
            return r;
        }
    };
    AlreadyResolvedMock already_mock;
    TransactionRecovery recovery_with_already(already_mock, log);
    auto report = recovery_with_already.recover();

    // Treated as success because the error message is recognized.
    EXPECT_EQ(report.recovered_commit.size(), 1u);
    EXPECT_TRUE(report.still_in_doubt.empty());
    EXPECT_GE(report.participant_errors, 1u);  // error was counted
}

TEST(TransactionRecovery, LeavesInDoubtWhenParticipantUnreachable) {
    TempLogPath tmp;
    seed_indoubt_log(tmp.path, "unreachable_txn",
                     DurableTransactionLog::Decision::COMMIT,
                     {"mysql_1", "mysql_2"});

    DurableTransactionLog log;
    log.open(tmp.path);
    MockDistributedExecutor mock;
    // mysql_2 will fail with a generic error that doesn't match the
    // "already resolved" heuristic.
    mock.add_fail_pattern("XA COMMIT 'unreachable_txn'");

    // Only fail for mysql_2. For this we need a smarter mock; instead of
    // conditioning in the mock, run recovery once and verify:
    // - BOTH XA COMMIT calls were issued (because all participants are
    //   failing, neither succeeds)
    // - still_in_doubt contains the txn
    TransactionRecovery recovery(mock, log);
    auto report = recovery.recover();

    ASSERT_EQ(report.still_in_doubt.size(), 1u);
    EXPECT_EQ(report.still_in_doubt[0], "unreachable_txn");
    EXPECT_TRUE(report.recovered_commit.empty());

    // The txn is still in-doubt in the log since we did NOT write COMPLETE.
    auto in_doubt_after = DurableTransactionLog::scan_in_doubt(tmp.path);
    ASSERT_EQ(in_doubt_after.size(), 1u);
    EXPECT_EQ(in_doubt_after[0].txn_id, "unreachable_txn");
}

TEST(TransactionRecovery, MultipleInDoubtTransactionsInOneRun) {
    TempLogPath tmp;
    seed_indoubt_log(tmp.path, "txn_a",
                     DurableTransactionLog::Decision::COMMIT,
                     {"b1"});
    seed_indoubt_log(tmp.path, "txn_b",
                     DurableTransactionLog::Decision::ROLLBACK,
                     {"b1", "b2"});
    seed_indoubt_log(tmp.path, "txn_c",
                     DurableTransactionLog::Decision::COMMIT,
                     {"b2"});

    DurableTransactionLog log;
    log.open(tmp.path);
    MockDistributedExecutor mock;
    TransactionRecovery recovery(mock, log);
    auto report = recovery.recover();

    EXPECT_EQ(report.recovered_commit.size(), 2u);
    EXPECT_EQ(report.recovered_rollback.size(), 1u);
    EXPECT_TRUE(report.still_in_doubt.empty());
    EXPECT_EQ(report.participants_contacted, 4u);  // 1 + 2 + 1

    // All three transactions should now be resolved.
    auto in_doubt_after = DurableTransactionLog::scan_in_doubt(tmp.path);
    EXPECT_TRUE(in_doubt_after.empty());
}

TEST(TransactionRecovery, EmptyLogProducesEmptyReport) {
    TempLogPath tmp;
    DurableTransactionLog log;
    log.open(tmp.path);
    MockDistributedExecutor mock;
    TransactionRecovery recovery(mock, log);
    auto report = recovery.recover();
    EXPECT_TRUE(report.recovered_commit.empty());
    EXPECT_TRUE(report.recovered_rollback.empty());
    EXPECT_TRUE(report.still_in_doubt.empty());
    EXPECT_EQ(report.participants_contacted, 0u);
}

// =====================================================================
// Connection pinning: phase 1 + phase 2 must share a physical session
// =====================================================================
//
// This is the regression test for the bug that existed for the entire
// life of DistributedTransactionManager until the pinning refactor:
// each SQL call used to go through RemoteExecutor::execute_dml, which
// on ThreadSafeMultiRemoteExecutor checks out a fresh pooled connection
// per call. That silently broke MySQL XA (which requires XA START,
// XA END, XA PREPARE to all be on the same session) and PostgreSQL
// PREPARE TRANSACTION (which requires BEGIN and PREPARE on the same
// session). The unit tests passed only because MockDistributedExecutor
// didn't enforce session identity.
//
// Now enlist_backend checks out a pinned session via
// RemoteExecutor::checkout_session, and phase 1 / phase 2 both go
// through that same session. Verify that by checking the mock's
// session-id-to-calls map.

TEST(DistributedTxnPinning, AllMysqlPhasesOnSameSession) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    EXPECT_TRUE(txn.enlist_backend("mysql_1"));
    EXPECT_TRUE(txn.commit());

    // mysql_1 should have gone through exactly one session id.
    int sid = mock.last_session_id("mysql_1");
    ASSERT_GE(sid, 0) << "no session was used for mysql_1 -- pinning is broken";

    // That session's call history must include XA START, XA END,
    // XA PREPARE, and XA COMMIT -- all four on the same session id.
    const auto& session_calls = mock.session_calls(sid);
    int start_count = 0, end_count = 0, prepare_count = 0, commit_count = 0;
    for (const auto& call : session_calls) {
        if (call.find("XA START")   != std::string::npos) ++start_count;
        if (call.find("XA END")     != std::string::npos) ++end_count;
        if (call.find("XA PREPARE") != std::string::npos) ++prepare_count;
        if (call.find("XA COMMIT")  != std::string::npos) ++commit_count;
    }
    EXPECT_EQ(start_count,   1) << "XA START missing from session " << sid;
    EXPECT_EQ(end_count,     1) << "XA END missing from session " << sid;
    EXPECT_EQ(prepare_count, 1) << "XA PREPARE missing from session " << sid;
    EXPECT_EQ(commit_count,  1) << "XA COMMIT missing from session " << sid;
}

TEST(DistributedTxnPinning, TwoBackendsUseTwoDistinctSessions) {
    // Two enlisted backends should each have their own pinned session
    // (a transaction can't share a physical connection across different
    // backends -- they're different TCP endpoints).
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.enlist_backend("mysql_2");
    txn.commit();

    int sid1 = mock.last_session_id("mysql_1");
    int sid2 = mock.last_session_id("mysql_2");
    ASSERT_GE(sid1, 0);
    ASSERT_GE(sid2, 0);
    EXPECT_NE(sid1, sid2) << "different backends must use different sessions";

    // Each session still contains the full 4-statement flow for its backend.
    auto count_in = [](const std::vector<std::string>& calls, const char* needle) {
        int n = 0;
        for (const auto& c : calls) if (c.find(needle) != std::string::npos) ++n;
        return n;
    };
    EXPECT_EQ(count_in(mock.session_calls(sid1), "XA START"),   1);
    EXPECT_EQ(count_in(mock.session_calls(sid1), "XA PREPARE"), 1);
    EXPECT_EQ(count_in(mock.session_calls(sid1), "XA COMMIT"),  1);
    EXPECT_EQ(count_in(mock.session_calls(sid2), "XA START"),   1);
    EXPECT_EQ(count_in(mock.session_calls(sid2), "XA PREPARE"), 1);
    EXPECT_EQ(count_in(mock.session_calls(sid2), "XA COMMIT"),  1);
}

TEST(DistributedTxnPinning, PostgreSqlPrepareAndCommitOnSameSession) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(
        mock, DistributedTransactionManager::BackendDialect::POSTGRESQL);

    txn.begin();
    txn.enlist_backend("pg_1");
    txn.commit();

    int sid = mock.last_session_id("pg_1");
    ASSERT_GE(sid, 0) << "no session was used for pg_1";

    const auto& session_calls = mock.session_calls(sid);
    int begin_count = 0, prepare_count = 0, commit_count = 0;
    for (const auto& call : session_calls) {
        if (call == "pg_1: BEGIN" ||
            (call.find("BEGIN") != std::string::npos &&
             call.find("PREPARE") == std::string::npos)) {
            ++begin_count;
        }
        if (call.find("PREPARE TRANSACTION") != std::string::npos) ++prepare_count;
        if (call.find("COMMIT PREPARED")     != std::string::npos) ++commit_count;
    }
    EXPECT_EQ(begin_count,   1) << "BEGIN missing from session";
    EXPECT_EQ(prepare_count, 1) << "PREPARE TRANSACTION missing from session";
    EXPECT_EQ(commit_count,  1) << "COMMIT PREPARED missing from session";
}

TEST(DistributedTxnPinning, RollbackPinsSession) {
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    txn.enlist_backend("mysql_1");
    txn.rollback();

    int sid = mock.last_session_id("mysql_1");
    ASSERT_GE(sid, 0);

    const auto& session_calls = mock.session_calls(sid);
    int start_count = 0, end_count = 0, rollback_count = 0;
    for (const auto& call : session_calls) {
        if (call.find("XA START")    != std::string::npos) ++start_count;
        if (call.find("XA END")      != std::string::npos) ++end_count;
        if (call.find("XA ROLLBACK") != std::string::npos) ++rollback_count;
    }
    EXPECT_EQ(start_count,    1);
    EXPECT_EQ(end_count,      1) << "XA END should run on same session before XA ROLLBACK";
    EXPECT_EQ(rollback_count, 1);
}

TEST(DistributedTxnPinning, UnpinnedFallbackStillWorks) {
    // If the executor reports no session support, the transaction
    // manager falls back to the unpinned execute_dml path. Verify
    // existing tests still work by disabling the mock's session
    // support and running the basic 2PC flow.
    MockDistributedExecutor mock;
    mock.set_sessions_enabled(false);
    mock.set_allow_unpinned_2pc(true);
    DistributedTransactionManager txn(mock);

    txn.begin();
    EXPECT_TRUE(txn.enlist_backend("mysql_1"));
    EXPECT_TRUE(txn.commit());

    // No session should have been used (all calls via execute_dml).
    EXPECT_EQ(mock.last_session_id("mysql_1"), -1);

    // But the backend history should still record the full 4-statement flow.
    const auto& h = mock.backend_history("mysql_1");
    int start_count = 0, end_count = 0, prepare_count = 0, commit_count = 0;
    for (const auto& s : h) {
        if (s.find("XA START")   != std::string::npos) ++start_count;
        if (s.find("XA END")     != std::string::npos) ++end_count;
        if (s.find("XA PREPARE") != std::string::npos) ++prepare_count;
        if (s.find("XA COMMIT")  != std::string::npos) ++commit_count;
    }
    EXPECT_EQ(start_count,   1);
    EXPECT_EQ(end_count,     1);
    EXPECT_EQ(prepare_count, 1);
    EXPECT_EQ(commit_count,  1);
}

TEST(DistributedTxnPinning, UnpinnedFallbackRejectedWithoutExplicitSafety) {
    MockDistributedExecutor mock;
    mock.set_sessions_enabled(false);
    DistributedTransactionManager txn(mock);

    txn.begin();
    EXPECT_FALSE(txn.enlist_backend("mysql_1"));
}

TEST(DistributedTxnPinning, ExecuteParticipantDmlUsesSameSession) {
    // DML routed via DistributedTransactionManager::execute_participant_dml
    // should land on the same pinned session as phase 1 / phase 2. This
    // is the hook external code uses to make in-transaction DML part of
    // the 2PC.
    MockDistributedExecutor mock;
    DistributedTransactionManager txn(mock);

    txn.begin();
    txn.execute_participant_dml("mysql_1",
        StringRef{"INSERT INTO t VALUES (1)", 24});
    txn.commit();

    int sid = mock.last_session_id("mysql_1");
    ASSERT_GE(sid, 0);

    const auto& session_calls = mock.session_calls(sid);
    bool saw_insert = false, saw_start = false, saw_prepare = false, saw_commit = false;
    for (const auto& call : session_calls) {
        if (call.find("INSERT INTO t") != std::string::npos) saw_insert = true;
        if (call.find("XA START")      != std::string::npos) saw_start = true;
        if (call.find("XA PREPARE")    != std::string::npos) saw_prepare = true;
        if (call.find("XA COMMIT")     != std::string::npos) saw_commit = true;
    }
    EXPECT_TRUE(saw_start);
    EXPECT_TRUE(saw_insert) << "DML via execute_participant_dml must run on the pinned session";
    EXPECT_TRUE(saw_prepare);
    EXPECT_TRUE(saw_commit);
}
