#ifndef SQL_ENGINE_THREAD_SAFE_EXECUTOR_H
#define SQL_ENGINE_THREAD_SAFE_EXECUTOR_H

// Thread-safe remote executor backed by a ConnectionPool.
//
// Multiple threads can call execute() / execute_dml() concurrently for the
// same (or different) backend names. Each call checks out its own MYSQL*
// handle from the pool, runs the query, and checks it back in.
//
// This is the key enabler for parallel shard execution: when SetOp, MergeSort
// or MergeAggregate launch child RemoteScan::open() calls via std::async,
// each thread gets its own connection -- no data races on MYSQL* handles.

#include "sql_engine/remote_executor.h"
#include "sql_engine/connection_pool.h"
#include "sql_engine/backend_config.h"
#include "sql_engine/result_set.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/datetime_parse.h"
#include "sql_engine/value.h"
#include "sql_engine/row.h"
#include "sql_parser/common.h"

#include <mysql/mysql.h>
#include <string>
#include <cstdlib>
#include <cstring>

namespace sql_engine {

// ----------------------------------------------------------------------------
// RAII guards so the pool and MYSQL_RES can't leak on early-return or exception.
// ----------------------------------------------------------------------------

// ConnectionGuard: holds a checked-out connection and returns it to the pool
// on destruction. If poison() was called, the connection is closed and
// discarded instead (correct for query errors and exceptions -- the handle
// may be in an unknown state, so returning it to the pool would poison the
// next user of that backend).
class ConnectionGuard {
public:
    ConnectionGuard(ConnectionPool& pool, std::string name)
        : pool_(pool), name_(std::move(name)), conn_(pool_.checkout(name_)) {}

    ~ConnectionGuard() {
        if (!conn_) return;
        if (poisoned_) {
            // Close and discard: the connection may be in a half-open state
            // (open transaction, partial result, etc.).
            mysql_close(conn_);
        } else {
            pool_.checkin(name_, conn_);
        }
    }

    ConnectionGuard(const ConnectionGuard&) = delete;
    ConnectionGuard& operator=(const ConnectionGuard&) = delete;
    ConnectionGuard(ConnectionGuard&&) = delete;
    ConnectionGuard& operator=(ConnectionGuard&&) = delete;

    MYSQL* get() const { return conn_; }
    void poison() { poisoned_ = true; }

private:
    ConnectionPool& pool_;
    std::string name_;
    MYSQL* conn_;
    bool poisoned_ = false;
};

// ResultGuard: frees a MYSQL_RES on scope exit.
class ResultGuard {
public:
    explicit ResultGuard(MYSQL_RES* res) : res_(res) {}
    ~ResultGuard() { if (res_) mysql_free_result(res_); }
    ResultGuard(const ResultGuard&) = delete;
    ResultGuard& operator=(const ResultGuard&) = delete;
    MYSQL_RES* get() const { return res_; }
private:
    MYSQL_RES* res_;
};

class ThreadSafeMultiRemoteExecutor : public RemoteExecutor {
public:
    ThreadSafeMultiRemoteExecutor() = default;
    ~ThreadSafeMultiRemoteExecutor() override = default;

    void add_backend(const BackendConfig& config) {
        pool_.add_backend(config);
        // Track dialect per backend (currently only MySQL is pooled)
        std::lock_guard<std::mutex> lk(mu_);
        backend_dialects_[config.name] = config.dialect;
    }

    ResultSet execute(const char* backend_name, sql_parser::StringRef sql) override {
        ConnectionGuard guard(pool_, std::string(backend_name));
        ResultSet rs;
        MYSQL* conn = guard.get();
        if (!conn) {
            guard.poison();
            return rs;
        }
        try {
            if (mysql_real_query(conn, sql.ptr,
                                 static_cast<unsigned long>(sql.len)) != 0) {
                // Query failed: the connection may still be usable (most query
                // errors leave it clean), but we poison it to be safe. Better
                // to reconnect than to hand a possibly-poisoned connection to
                // the next caller.
                guard.poison();
                return rs;
            }
            MYSQL_RES* res = mysql_store_result(conn);
            if (!res) {
                // Non-result statement (DML) returned via execute() path, or
                // empty result set -- either way, connection is clean.
                return rs;
            }
            ResultGuard res_guard(res);
            rs = mysql_result_to_resultset(res);
        } catch (...) {
            guard.poison();
            return rs;
        }
        return rs;
    }

    DmlResult execute_dml(const char* backend_name, sql_parser::StringRef sql) override {
        ConnectionGuard guard(pool_, std::string(backend_name));
        DmlResult result;
        MYSQL* conn = guard.get();
        if (!conn) {
            guard.poison();
            result.error_message = "failed to acquire connection";
            return result;
        }
        try {
            if (mysql_real_query(conn, sql.ptr,
                                 static_cast<unsigned long>(sql.len)) != 0) {
                result.error_message = mysql_error(conn);
                guard.poison();
                return result;
            }
            MYSQL_RES* res = mysql_store_result(conn);
            ResultGuard res_guard(res);  // frees res if non-null
            result.affected_rows = mysql_affected_rows(conn);
            result.last_insert_id = mysql_insert_id(conn);
            result.success = true;
        } catch (const std::exception& e) {
            result.error_message = e.what();
            guard.poison();
            return result;
        } catch (...) {
            result.error_message = "unknown exception";
            guard.poison();
            return result;
        }
        return result;
    }

    void disconnect_all() {
        // Pool destructor handles cleanup; nothing needed here.
    }

private:
    ConnectionPool pool_;
    std::mutex mu_;
    std::unordered_map<std::string, sql_parser::Dialect> backend_dialects_;


    static ResultSet mysql_result_to_resultset(MYSQL_RES* res) {
        ResultSet rs;
        unsigned int num_fields = mysql_num_fields(res);
        rs.column_count = static_cast<uint16_t>(num_fields);

        MYSQL_FIELD* fields = mysql_fetch_fields(res);
        for (unsigned int i = 0; i < num_fields; ++i) {
            rs.column_names.emplace_back(fields[i].name);
        }

        MYSQL_ROW mysql_row;
        while ((mysql_row = mysql_fetch_row(res)) != nullptr) {
            unsigned long* lengths = mysql_fetch_lengths(res);
            Row& row = rs.add_heap_row(rs.column_count);
            for (unsigned int i = 0; i < num_fields; ++i) {
                bool is_null = (mysql_row[i] == nullptr);
                Value v = mysql_field_to_value(
                    rs, mysql_row[i], is_null ? 0 : lengths[i],
                    fields[i].type, is_null);
                row.set(static_cast<uint16_t>(i), v);
            }
        }
        return rs;
    }

    static Value mysql_field_to_value(
        ResultSet& rs, const char* data, unsigned long length,
        enum_field_types type, bool is_null)
    {
        if (is_null) return value_null();

        switch (type) {
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_YEAR:
                return value_int(std::strtoll(data, nullptr, 10));

            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
                return value_double(std::strtod(data, nullptr));

            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL: {
                sql_parser::StringRef s = rs.own_string(data, static_cast<uint32_t>(length));
                return value_decimal(s);
            }

            case MYSQL_TYPE_DATE: {
                int32_t days = datetime_parse::parse_date(data);
                return value_date(days);
            }

            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2: {
                int64_t us = datetime_parse::parse_datetime(data);
                return value_datetime(us);
            }

            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2: {
                int64_t us = datetime_parse::parse_datetime(data);
                return value_timestamp(us);
            }

            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_TIME2: {
                int64_t us = datetime_parse::parse_time(data);
                return value_time(us);
            }

            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB: {
                sql_parser::StringRef s = rs.own_string(data, static_cast<uint32_t>(length));
                return value_bytes(s);
            }

            case MYSQL_TYPE_JSON: {
                sql_parser::StringRef s = rs.own_string(data, static_cast<uint32_t>(length));
                return value_json(s);
            }

            default: {
                sql_parser::StringRef s = rs.own_string(data, static_cast<uint32_t>(length));
                return value_string(s);
            }
        }
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_THREAD_SAFE_EXECUTOR_H
