#ifndef SQL_ENGINE_CONNECTION_POOL_H
#define SQL_ENGINE_CONNECTION_POOL_H

// Thread-safe connection pool for MySQL backends.
//
// Each backend name maps to a stack of idle MYSQL* handles. Threads check out
// a handle (creating one on demand), use it, and check it back in. The pool
// grows dynamically -- there is no hard cap -- so N concurrent calls to the
// same backend will use N connections.

#include "sql_engine/backend_config.h"
#include <mysql/mysql.h>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <stdexcept>

namespace sql_engine {

class ConnectionPool {
public:
    ConnectionPool() = default;

    ~ConnectionPool() {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& kv : idle_) {
            for (MYSQL* c : kv.second) {
                if (c) mysql_close(c);
            }
        }
        idle_.clear();
    }

    // Register a backend. Must be called before any checkout for that name.
    void add_backend(const BackendConfig& config) {
        std::lock_guard<std::mutex> lk(mu_);
        configs_[config.name] = config;
    }

    // Obtain a connection for the named backend. Creates a new one if none is
    // idle. The caller MUST call checkin() when done.
    MYSQL* checkout(const std::string& backend) {
        std::lock_guard<std::mutex> lk(mu_);
        auto& stack = idle_[backend];
        if (!stack.empty()) {
            MYSQL* c = stack.back();
            stack.pop_back();
            // Quick liveness check (non-blocking).
            if (mysql_ping(c) != 0) {
                mysql_close(c);
                return create_connection(backend);
            }
            return c;
        }
        return create_connection(backend);
    }

    // Return a connection to the pool for reuse.
    void checkin(const std::string& backend, MYSQL* conn) {
        if (!conn) return;
        std::lock_guard<std::mutex> lk(mu_);
        idle_[backend].push_back(conn);
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, BackendConfig> configs_;
    std::unordered_map<std::string, std::vector<MYSQL*>> idle_;

    // Must be called with mu_ held.
    MYSQL* create_connection(const std::string& backend) {
        auto it = configs_.find(backend);
        if (it == configs_.end()) {
            throw std::runtime_error("ConnectionPool: unknown backend: " + backend);
        }
        const BackendConfig& cfg = it->second;
        MYSQL* c = mysql_init(nullptr);
        if (!c) throw std::runtime_error("mysql_init failed for " + backend);

        unsigned int timeout = 5;
        mysql_options(c, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

        if (!mysql_real_connect(c, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), cfg.database.c_str(),
                                cfg.port, nullptr, 0)) {
            std::string err = mysql_error(c);
            mysql_close(c);
            throw std::runtime_error("ConnectionPool connect failed for " + backend + ": " + err);
        }
        mysql_set_character_set(c, "utf8mb4");
        return c;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_CONNECTION_POOL_H
