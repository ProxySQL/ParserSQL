#ifndef SQL_ENGINE_REMOTE_EXECUTOR_H
#define SQL_ENGINE_REMOTE_EXECUTOR_H

#include "sql_engine/result_set.h"
#include "sql_engine/dml_result.h"
#include "sql_parser/common.h"

namespace sql_engine {

class RemoteExecutor {
public:
    virtual ~RemoteExecutor() = default;
    virtual ResultSet execute(const char* backend_name, sql_parser::StringRef sql) = 0;
    virtual DmlResult execute_dml(const char* backend_name, sql_parser::StringRef sql) {
        (void)backend_name;
        (void)sql;
        DmlResult r;
        r.error_message = "execute_dml not implemented";
        return r;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_REMOTE_EXECUTOR_H
