#ifndef SQL_ENGINE_DURABLE_TXN_LOG_H
#define SQL_ENGINE_DURABLE_TXN_LOG_H

// Durable write-ahead log for distributed transaction decisions.
//
// The single purpose of this file is 2PC recovery. Without it, the
// DistributedTransactionManager has a fatal gap: between phase 1 (all
// participants return PREPARE OK) and phase 2 (dispatch COMMIT to each),
// the coordinator must remember its decision durably. If the coordinator
// crashes in that window, a restart has no idea which transactions were
// in-flight, which ones were supposed to commit, or which ones were
// supposed to roll back. Prepared transactions remain held on every
// backend until a DBA manually resolves them.
//
// The fix is to write an append-only record containing the transaction id,
// the decision (COMMIT or ROLLBACK), and the list of participants, then
// fsync before phase 2 begins. After phase 2 completes successfully, a
// COMPLETE record is written (also fsynced) to mark the transaction as no
// longer in-doubt. On startup, we scan the log; any txn_id with a
// DECISION record but no matching COMPLETE is in-doubt and needs recovery.
//
// Format (line-based, tab-separated, each record ends with '\n'):
//
//     COMMIT\t<txn_id>\t<participant1>,<participant2>,...\n
//     ROLLBACK\t<txn_id>\t<participant1>,<participant2>,...\n
//     COMPLETE\t<txn_id>\n
//
// Limitations of this MVP:
// - Single log file, no rotation. A long-running coordinator will grow
//   the file indefinitely. Compaction is a follow-up.
// - fsync per record. Fine for modest TPS.
// - The log is text, not binary. Easier to debug, slightly larger on disk.
// - Recovery resolution (actually contacting backends to commit/rollback
//   the in-doubt transactions) lives in scan_in_doubt's callers, not here.
//   This class just produces the list of work the caller must do.

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <fcntl.h>
#include <unistd.h>

namespace sql_engine {

class DurableTransactionLog {
public:
    enum class Decision : uint8_t { COMMIT, ROLLBACK };

    struct InDoubtEntry {
        std::string txn_id;
        Decision decision = Decision::COMMIT;
        std::vector<std::string> participants;
    };

    DurableTransactionLog() = default;
    ~DurableTransactionLog() { close(); }

    DurableTransactionLog(const DurableTransactionLog&) = delete;
    DurableTransactionLog& operator=(const DurableTransactionLog&) = delete;

    // Open the log at `path`, creating it if it doesn't exist. Append mode
    // (records go at the end). Must be called before any log_* methods.
    //
    // Returns true on success. On failure the log is not open; callers
    // should decide whether to proceed without durability (dangerous) or
    // fail the commit outright.
    bool open(const std::string& path) {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ >= 0) return true;
        path_ = path;
        fd_ = ::open(path.c_str(),
                     O_WRONLY | O_CREAT | O_APPEND,
                     0644);
        return fd_ >= 0;
    }

    // Close the log. Destructor calls this automatically.
    void close() {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    bool is_open() const {
        std::lock_guard<std::mutex> lk(mu_);
        return fd_ >= 0;
    }

    // Record the commit-or-rollback decision for a transaction. Writes
    // synchronously and calls fsync before returning so that a crash after
    // this function returns preserves the record. Must be called BEFORE
    // phase 2 dispatches any XA COMMIT / COMMIT PREPARED / XA ROLLBACK to
    // the participants.
    bool log_decision(const std::string& txn_id,
                      Decision decision,
                      const std::vector<std::string>& participants) {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ < 0) return false;
        std::string line;
        line += (decision == Decision::COMMIT) ? "COMMIT\t" : "ROLLBACK\t";
        line += txn_id;
        line += '\t';
        for (size_t i = 0; i < participants.size(); ++i) {
            if (i > 0) line += ',';
            line += participants[i];
        }
        line += '\n';
        return write_and_fsync(line);
    }

    // Record that phase 2 has finished for a transaction (all participants
    // completed the decision). This is the record that removes the
    // transaction from the in-doubt set on recovery.
    bool log_complete(const std::string& txn_id) {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ < 0) return false;
        std::string line = "COMPLETE\t" + txn_id + '\n';
        return write_and_fsync(line);
    }

    // Scan an arbitrary log file and return all transactions that have a
    // decision record but no matching completion record. The caller is
    // expected to iterate these and send the appropriate XA COMMIT or XA
    // ROLLBACK to each listed participant.
    //
    // Does not require the log to be open; safe to call at startup before
    // open() if you just want to inspect an existing file.
    static std::vector<InDoubtEntry> scan_in_doubt(const std::string& path) {
        std::vector<InDoubtEntry> in_doubt;
        // Preserve insertion order: track position in `order` and the
        // entry data in `pending`. COMPLETE records remove entries from
        // both.
        std::unordered_map<std::string, size_t> order;
        std::vector<InDoubtEntry> pending;

        FILE* fp = std::fopen(path.c_str(), "r");
        if (!fp) return in_doubt;

        char buf[4096];
        while (std::fgets(buf, sizeof(buf), fp) != nullptr) {
            size_t n = std::strlen(buf);
            while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r')) {
                buf[--n] = '\0';
            }
            if (n == 0) continue;

            char* tab1 = std::strchr(buf, '\t');
            if (!tab1) continue;
            *tab1++ = '\0';
            const char* kind = buf;

            if (std::strcmp(kind, "COMPLETE") == 0) {
                auto it = order.find(tab1);
                if (it != order.end()) {
                    // Tombstone this slot -- we'll compact below.
                    pending[it->second].txn_id.clear();
                    order.erase(it);
                }
                continue;
            }

            char* tab2 = std::strchr(tab1, '\t');
            std::string txn_id;
            std::string parts_str;
            if (tab2) {
                *tab2++ = '\0';
                txn_id = tab1;
                parts_str = tab2;
            } else {
                txn_id = tab1;
            }

            InDoubtEntry e;
            e.txn_id = txn_id;
            if (std::strcmp(kind, "COMMIT") == 0) {
                e.decision = Decision::COMMIT;
            } else if (std::strcmp(kind, "ROLLBACK") == 0) {
                e.decision = Decision::ROLLBACK;
            } else {
                continue;
            }
            // Split participants on ','
            const char* p = parts_str.c_str();
            const char* start = p;
            while (*p) {
                if (*p == ',') {
                    if (p > start) e.participants.emplace_back(start, p - start);
                    start = p + 1;
                }
                ++p;
            }
            if (p > start) e.participants.emplace_back(start, p - start);

            auto it = order.find(txn_id);
            if (it != order.end()) {
                // Duplicate decision for the same txn_id -- last writer wins.
                pending[it->second] = std::move(e);
            } else {
                order[txn_id] = pending.size();
                pending.push_back(std::move(e));
            }
        }
        std::fclose(fp);

        // Drop tombstoned entries (those whose txn_id was cleared when
        // a COMPLETE arrived).
        for (auto& e : pending) {
            if (!e.txn_id.empty()) in_doubt.push_back(std::move(e));
        }
        return in_doubt;
    }

    // Convenience wrapper: uses the path the log was opened with.
    std::vector<InDoubtEntry> scan_in_doubt() const {
        return scan_in_doubt(path_);
    }

    // Test-only: truncate the log file. Invalidates any in-doubt recovery
    // scan done afterwards; only use in test setup/teardown.
    bool test_truncate() {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ < 0) return false;
        if (::ftruncate(fd_, 0) != 0) return false;
        if (::lseek(fd_, 0, SEEK_SET) == (off_t)-1) return false;
        return true;
    }

private:
    mutable std::mutex mu_;
    int fd_ = -1;
    std::string path_;

    // Write `data` to the file and fsync. Caller must hold mu_.
    bool write_and_fsync(const std::string& data) {
        const char* p = data.data();
        size_t remaining = data.size();
        while (remaining > 0) {
            ssize_t w = ::write(fd_, p, remaining);
            if (w < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            p += w;
            remaining -= static_cast<size_t>(w);
        }
        // fsync is what makes this a WAL and not just a log: we need the
        // kernel to push the data to stable storage before we proceed with
        // phase 2.
        if (::fsync(fd_) != 0) return false;
        return true;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_DURABLE_TXN_LOG_H
