#ifndef SQL_ENGINE_DISTRIBUTED_TXN_H
#define SQL_ENGINE_DISTRIBUTED_TXN_H

#include "sql_engine/transaction_manager.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/shard_map.h"
#include "sql_parser/common.h"

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <random>
#include <cstdio>

namespace sql_engine {

// DistributedTransactionManager implements two-phase commit (2PC) for
// transactions spanning multiple backends.
//
// MySQL XA protocol:
//   XA START 'txn_id'  → begin on each participant
//   XA END 'txn_id'    → mark end of work
//   XA PREPARE 'txn_id' → phase 1
//   XA COMMIT 'txn_id'  → phase 2 (success)
//   XA ROLLBACK 'txn_id' → phase 2 (failure)
//
// PostgreSQL:
//   BEGIN → PREPARE TRANSACTION 'txn_id' → COMMIT PREPARED 'txn_id'
class DistributedTransactionManager : public TransactionManager {
public:
    // Backend dialect for 2PC protocol selection
    enum class BackendDialect : uint8_t { MYSQL, POSTGRESQL };

    DistributedTransactionManager(RemoteExecutor& executor,
                                   BackendDialect dialect = BackendDialect::MYSQL)
        : executor_(executor), dialect_(dialect) {}

    bool begin() override {
        txn_id_ = generate_txn_id();
        participants_.clear();
        prepared_.clear();
        started_.clear();
        active_ = true;
        return true;
    }

    // Enlist a backend as a transaction participant. Called when DML is
    // executed against a backend. Sends XA START / BEGIN to the backend
    // if not already enlisted.
    bool enlist_backend(const char* backend_name) {
        if (!active_) return false;
        std::string name(backend_name);
        if (started_.count(name)) return true;  // already enlisted

        bool ok = false;
        if (dialect_ == BackendDialect::MYSQL) {
            std::string sql = "XA START '" + txn_id_ + "'";
            ok = send_sql(backend_name, sql);
        } else {
            ok = send_sql(backend_name, "BEGIN");
        }
        if (ok) {
            participants_.push_back(name);
            started_.insert(name);
        }
        return ok;
    }

    bool commit() override {
        if (!active_) return false;
        if (participants_.empty()) {
            active_ = false;
            return true;
        }

        // Phase 1: prepare all participants
        if (!phase1_prepare()) {
            phase2_rollback();
            active_ = false;
            return false;
        }

        // Phase 2: commit all participants
        bool ok = phase2_commit();
        active_ = false;
        return ok;
    }

    bool rollback() override {
        if (!active_) return false;
        phase2_rollback();
        active_ = false;
        return true;
    }

    // Savepoints are not supported for distributed transactions.
    bool savepoint(const char*) override { return false; }
    bool rollback_to(const char*) override { return false; }
    bool release_savepoint(const char*) override { return false; }

    bool in_transaction() const override { return active_; }
    bool is_auto_commit() const override { return auto_commit_; }
    void set_auto_commit(bool ac) override { auto_commit_ = ac; }

    const std::string& txn_id() const { return txn_id_; }
    const std::vector<std::string>& participants() const { return participants_; }

private:
    RemoteExecutor& executor_;
    BackendDialect dialect_;

    std::string txn_id_;
    std::vector<std::string> participants_;
    std::unordered_set<std::string> started_;
    std::unordered_map<std::string, bool> prepared_;
    bool active_ = false;
    bool auto_commit_ = true;

    // Generate a unique transaction ID.
    static std::string generate_txn_id() {
        auto now = std::chrono::steady_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        // Use random suffix to avoid collisions
        static thread_local std::mt19937 rng(
            static_cast<unsigned>(std::chrono::system_clock::now()
                .time_since_epoch().count()));
        std::uniform_int_distribution<uint32_t> dist(0, 999999);
        char buf[64];
        std::snprintf(buf, sizeof(buf), "parsersql_%ld_%06u",
                      static_cast<long>(ns), dist(rng));
        return buf;
    }

    bool send_sql(const char* backend, const std::string& sql) {
        auto r = executor_.execute_dml(backend,
            sql_parser::StringRef{sql.c_str(),
                static_cast<uint32_t>(sql.size())});
        return r.success;
    }

    // Phase 1: XA END + XA PREPARE on all participants (MySQL)
    //          PREPARE TRANSACTION on all participants (PostgreSQL)
    bool phase1_prepare() {
        bool all_ok = true;
        for (auto& p : participants_) {
            bool ok = false;
            if (dialect_ == BackendDialect::MYSQL) {
                std::string end_sql = "XA END '" + txn_id_ + "'";
                ok = send_sql(p.c_str(), end_sql);
                if (ok) {
                    std::string prep_sql = "XA PREPARE '" + txn_id_ + "'";
                    ok = send_sql(p.c_str(), prep_sql);
                }
            } else {
                std::string prep_sql = "PREPARE TRANSACTION '" + txn_id_ + "'";
                ok = send_sql(p.c_str(), prep_sql);
            }
            prepared_[p] = ok;
            if (!ok) all_ok = false;
        }
        return all_ok;
    }

    // Phase 2 (success): XA COMMIT on all participants
    bool phase2_commit() {
        bool all_ok = true;
        for (auto& p : participants_) {
            bool ok = false;
            if (dialect_ == BackendDialect::MYSQL) {
                std::string sql = "XA COMMIT '" + txn_id_ + "'";
                ok = send_sql(p.c_str(), sql);
            } else {
                std::string sql = "COMMIT PREPARED '" + txn_id_ + "'";
                ok = send_sql(p.c_str(), sql);
            }
            if (!ok) all_ok = false;
        }
        return all_ok;
    }

    // Phase 2 (failure): XA ROLLBACK on all participants
    void phase2_rollback() {
        for (auto& p : participants_) {
            if (dialect_ == BackendDialect::MYSQL) {
                // If prepared, XA ROLLBACK; if only started, XA END + XA ROLLBACK
                if (prepared_.count(p) && prepared_[p]) {
                    std::string sql = "XA ROLLBACK '" + txn_id_ + "'";
                    send_sql(p.c_str(), sql);
                } else if (started_.count(p)) {
                    // Try XA END first (may fail if already ended)
                    std::string end_sql = "XA END '" + txn_id_ + "'";
                    send_sql(p.c_str(), end_sql);
                    std::string rb_sql = "XA ROLLBACK '" + txn_id_ + "'";
                    send_sql(p.c_str(), rb_sql);
                }
            } else {
                if (prepared_.count(p) && prepared_[p]) {
                    std::string sql = "ROLLBACK PREPARED '" + txn_id_ + "'";
                    send_sql(p.c_str(), sql);
                } else if (started_.count(p)) {
                    send_sql(p.c_str(), "ROLLBACK");
                }
            }
        }
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_DISTRIBUTED_TXN_H
