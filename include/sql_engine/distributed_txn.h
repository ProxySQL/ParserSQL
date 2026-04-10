#ifndef SQL_ENGINE_DISTRIBUTED_TXN_H
#define SQL_ENGINE_DISTRIBUTED_TXN_H

#include "sql_engine/transaction_manager.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/durable_txn_log.h"
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

    // Attach a durable write-ahead log for 2PC recovery. Optional but
    // strongly recommended for any real workload: without it, a crash
    // between phase 1 and phase 2 leaves prepared transactions on every
    // backend with no automatic recovery path.
    //
    // The log pointer must outlive this manager. Pass nullptr to disable
    // logging (the default -- matches pre-existing behavior).
    void set_durable_log(DurableTransactionLog* log) { txn_log_ = log; }

    // Require the WAL to succeed for commits to proceed. Default: false.
    // When true, if log_decision() fails, we refuse to start phase 2 and
    // roll back instead -- trading availability for durability. When
    // false, a log write failure is logged to stderr but the commit
    // continues (caller might prefer availability over durability).
    void set_require_durable_log(bool required) { require_durable_log_ = required; }

    // Set a tight per-phase statement timeout (in milliseconds). When > 0,
    // the manager issues a SET SESSION max_execution_time (MySQL) or
    // SET LOCAL statement_timeout (PostgreSQL) on each participant BEFORE
    // phase 1 (XA PREPARE / PREPARE TRANSACTION) and again before phase 2
    // (XA COMMIT / COMMIT PREPARED). This is independent of the backend
    // connection's default read/write timeout -- use it to bound 2PC
    // specifically without affecting other queries.
    //
    // 0 (default) means "don't override", fall back to whatever the
    // backend connection's read/write timeout provides.
    //
    // NOTE: because ThreadSafeMultiRemoteExecutor may hand us a different
    // pooled connection for each execute_dml call, the SET must be issued
    // immediately before the statement whose timeout it bounds. We do
    // that by concatenating them with "; " when multi-statement is
    // supported, OR by issuing two separate execute_dml calls and
    // tolerating that the second one may not actually have the timeout
    // in effect (best-effort). For the MVP we use the two-call approach.
    void set_phase_statement_timeout_ms(uint32_t ms) {
        phase_statement_timeout_ms_ = ms;
    }

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
            // Durably record the ROLLBACK decision BEFORE dispatching to
            // participants. If we crash between here and the rollback
            // completing, recovery replays the rollback.
            if (!log_decision_or_fail(DurableTransactionLog::Decision::ROLLBACK)) {
                // Caller asked us to require durable logging and it failed
                // -- leave transactions prepared rather than lose the
                // decision. A DBA will resolve them manually via
                // XA RECOVER + XA ROLLBACK.
                active_ = false;
                return false;
            }
            phase2_rollback();
            // Rollback is best-effort; mark COMPLETE regardless. An in-doubt
            // prepared transaction left after a partial rollback is
            // recorded separately (if we wanted) -- currently we accept
            // that rollback failure is a separate class of operator issue.
            maybe_log_complete();
            active_ = false;
            return false;
        }

        // Durably record the COMMIT decision BEFORE phase 2 dispatches.
        // This is the core durability invariant: a record of "commit this
        // transaction" exists on disk before any participant has been
        // told to commit, so a crash before, during, or after phase 2
        // is recoverable by replaying the committed decision.
        if (!log_decision_or_fail(DurableTransactionLog::Decision::COMMIT)) {
            // The WAL is required and it failed. Roll back in-memory state
            // so the caller sees the commit fail; prepared transactions
            // remain on backends until DBA cleanup.
            active_ = false;
            return false;
        }

        // Phase 2: commit all participants
        bool ok = phase2_commit();
        // Only mark COMPLETE if every participant committed successfully.
        // A partial commit is a heuristic hazard: some participants hold
        // the data committed, others may still be prepared or failed. The
        // transaction remains in-doubt in the log so startup recovery (or
        // a DBA) can finish the job.
        if (ok) {
            maybe_log_complete();
        } else {
            std::fprintf(stderr,
                "[DistributedTransactionManager] phase 2 commit failed for "
                "txn %s; leaving in-doubt in the WAL for recovery.\n",
                txn_id_.c_str());
        }
        active_ = false;
        return ok;
    }

    bool rollback() override {
        if (!active_) return false;
        // Durably record the ROLLBACK decision before dispatching.
        (void)log_decision_or_fail(DurableTransactionLog::Decision::ROLLBACK);
        phase2_rollback();
        // Rollback is best-effort; we mark COMPLETE whether or not every
        // backend acknowledged the rollback. A failed rollback on a
        // prepared transaction leaves the participant in a bad state that
        // a DBA needs to resolve via XA RECOVER.
        maybe_log_complete();
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

    DurableTransactionLog* txn_log_ = nullptr;
    bool require_durable_log_ = false;
    uint32_t phase_statement_timeout_ms_ = 0;

    // Best-effort: set a per-session statement timeout on a backend before
    // issuing a phase-1 or phase-2 SQL. Returns true if the SET succeeded
    // OR if no timeout is configured; false only if the SET itself fails
    // and the caller asked for a real timeout (in which case the caller
    // may want to abort rather than risk an unbounded hang).
    bool maybe_set_statement_timeout(const char* backend) {
        if (phase_statement_timeout_ms_ == 0) return true;
        std::string sql;
        if (dialect_ == BackendDialect::MYSQL) {
            // MySQL 5.7.4+: max_execution_time is in milliseconds and
            // only bounds SELECTs. For DML and XA commands, the client
            // read_timeout is our real protection. We still set this for
            // SELECTs that might be issued between phases.
            sql = "SET SESSION max_execution_time = " +
                  std::to_string(phase_statement_timeout_ms_);
        } else {
            sql = "SET LOCAL statement_timeout = " +
                  std::to_string(phase_statement_timeout_ms_);
        }
        return send_sql(backend, sql);
    }

    // Write the phase-2 decision to the durable log before dispatching.
    // Returns true if the commit/rollback can proceed:
    // - log not configured: true (log-less mode preserves legacy behavior)
    // - log configured and write succeeded: true
    // - log configured and write failed:
    //     - require_durable_log_: false (abort, don't risk a crash
    //       window without a recoverable decision)
    //     - !require_durable_log_: true (write failure logged to stderr,
    //       commit proceeds at the caller's risk)
    bool log_decision_or_fail(DurableTransactionLog::Decision d) {
        if (!txn_log_) return true;
        if (txn_log_->log_decision(txn_id_, d, participants_)) return true;
        if (require_durable_log_) {
            std::fprintf(stderr,
                "[DistributedTransactionManager] WAL write failed for txn %s; "
                "refusing to proceed with phase 2 because require_durable_log is set.\n",
                txn_id_.c_str());
            return false;
        }
        std::fprintf(stderr,
            "[DistributedTransactionManager] WAL write failed for txn %s; "
            "proceeding without durability (set require_durable_log to refuse instead).\n",
            txn_id_.c_str());
        return true;
    }

    void maybe_log_complete() {
        if (txn_log_) txn_log_->log_complete(txn_id_);
    }

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
            // Best-effort per-phase timeout. See note on
            // set_phase_statement_timeout_ms: on ThreadSafeMultiRemoteExecutor
            // the SET may end up on a different pooled connection than
            // the next statement, so this is advisory. The connection-level
            // read/write timeout in connection_pool.h is the real ceiling.
            maybe_set_statement_timeout(p.c_str());

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
            maybe_set_statement_timeout(p.c_str());

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
