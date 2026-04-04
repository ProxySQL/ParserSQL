#include "sql_engine/multi_remote_executor.h"

namespace sql_engine {

MultiRemoteExecutor::MultiRemoteExecutor(sql_parser::Arena& arena)
    : mysql_exec_(arena), pgsql_exec_(arena) {}

MultiRemoteExecutor::~MultiRemoteExecutor() {
    disconnect_all();
}

void MultiRemoteExecutor::add_backend(const BackendConfig& config) {
    backend_dialects_[config.name] = config.dialect;
    if (config.dialect == sql_parser::Dialect::MySQL) {
        mysql_exec_.add_backend(config);
    } else {
        pgsql_exec_.add_backend(config);
    }
}

ResultSet MultiRemoteExecutor::execute(const char* backend_name, sql_parser::StringRef sql) {
    auto it = backend_dialects_.find(backend_name);
    if (it == backend_dialects_.end()) {
        return ResultSet{};
    }
    if (it->second == sql_parser::Dialect::MySQL) {
        return mysql_exec_.execute(backend_name, sql);
    } else {
        return pgsql_exec_.execute(backend_name, sql);
    }
}

DmlResult MultiRemoteExecutor::execute_dml(const char* backend_name, sql_parser::StringRef sql) {
    auto it = backend_dialects_.find(backend_name);
    if (it == backend_dialects_.end()) {
        DmlResult r;
        r.error_message = "backend not found";
        return r;
    }
    if (it->second == sql_parser::Dialect::MySQL) {
        return mysql_exec_.execute_dml(backend_name, sql);
    } else {
        return pgsql_exec_.execute_dml(backend_name, sql);
    }
}

void MultiRemoteExecutor::disconnect_all() {
    mysql_exec_.disconnect_all();
    pgsql_exec_.disconnect_all();
}

} // namespace sql_engine
