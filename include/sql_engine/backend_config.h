#ifndef SQL_ENGINE_BACKEND_CONFIG_H
#define SQL_ENGINE_BACKEND_CONFIG_H

#include "sql_parser/common.h"
#include <cstdint>
#include <string>

namespace sql_engine {

struct BackendConfig {
    std::string name;       // logical name: "shard_1", "analytics_db"
    std::string host;
    uint16_t port = 0;
    std::string user;
    std::string password;
    std::string database;
    sql_parser::Dialect dialect = sql_parser::Dialect::MySQL;
};

} // namespace sql_engine

#endif // SQL_ENGINE_BACKEND_CONFIG_H
