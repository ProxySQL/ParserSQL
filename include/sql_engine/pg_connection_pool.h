#ifndef SQL_ENGINE_PG_CONNECTION_POOL_H
#define SQL_ENGINE_PG_CONNECTION_POOL_H

#include "sql_engine/backend_config.h"
#include <libpq-fe.h>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <stdexcept>
#include <memory>

#ifndef SQL_ENGINE_PG_STATEMENT_TIMEOUT_MS
#define SQL_ENGINE_PG_STATEMENT_TIMEOUT_MS 30000
#endif

namespace sql_engine {

class PgConnectionPool {
public:
    PgConnectionPool() = default;

    ~PgConnectionPool() {
        for (auto& kv : backends_) {
            auto& be = *kv.second;
            std::lock_guard<std::mutex> lk(be.mu);
            for (PGconn* c : be.idle) {
                if (c) PQfinish(c);
            }
        }
    }

    void add_backend(const BackendConfig& config) {
        auto be = std::make_unique<Backend>();
        be->config = config;
        backends_[config.name] = std::move(be);
    }

    bool has_backend(const std::string& name) const {
        return backends_.find(name) != backends_.end();
    }

    PGconn* checkout(const std::string& backend) {
        Backend& be = get_backend(backend);
        {
            std::lock_guard<std::mutex> lk(be.mu);
            if (!be.idle.empty()) {
                PGconn* c = be.idle.back();
                be.idle.pop_back();
                if (c && PQstatus(c) == CONNECTION_OK) return c;
                if (c) PQfinish(c);
            }
        }
        return create_connection(be);
    }

    void checkin(const std::string& backend, PGconn* conn) {
        if (!conn) return;
        Backend& be = get_backend(backend);
        std::lock_guard<std::mutex> lk(be.mu);
        be.idle.push_back(conn);
    }

private:
    struct Backend {
        BackendConfig config;
        std::mutex mu;
        std::vector<PGconn*> idle;
    };

    std::unordered_map<std::string, std::unique_ptr<Backend>> backends_;

    Backend& get_backend(const std::string& name) {
        auto it = backends_.find(name);
        if (it == backends_.end()) {
            throw std::runtime_error("PgConnectionPool: unknown backend: " + name);
        }
        return *it->second;
    }

    static PGconn* create_connection(Backend& be) {
        const BackendConfig& cfg = be.config;
        std::string conninfo = "host=" + cfg.host
            + " port=" + std::to_string(cfg.port)
            + " user=" + cfg.user
            + " password=" + cfg.password
            + " dbname=" + cfg.database
            + " connect_timeout=5"
            + " options='-c statement_timeout="
                + std::to_string(SQL_ENGINE_PG_STATEMENT_TIMEOUT_MS) + "'";
        if (!cfg.ssl_mode.empty()) conninfo += " sslmode=" + cfg.ssl_mode;
        if (!cfg.ssl_ca.empty()) conninfo += " sslrootcert=" + cfg.ssl_ca;
        if (!cfg.ssl_cert.empty()) conninfo += " sslcert=" + cfg.ssl_cert;
        if (!cfg.ssl_key.empty()) conninfo += " sslkey=" + cfg.ssl_key;

        PGconn* c = PQconnectdb(conninfo.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            std::string err = PQerrorMessage(c);
            PQfinish(c);
            throw std::runtime_error("PgConnectionPool connect failed for " + cfg.name + ": " + err);
        }
        return c;
    }
};

} // namespace sql_engine

#endif
