#include <gtest/gtest.h>
#include "sql_engine/backend_config.h"

using namespace sql_engine;
using sql_parser::Dialect;

// ---------- BackendConfig defaults ----------

TEST(SSLConfigTest, DefaultFieldsAreEmpty) {
    BackendConfig cfg;
    EXPECT_TRUE(cfg.ssl_mode.empty());
    EXPECT_TRUE(cfg.ssl_ca.empty());
    EXPECT_TRUE(cfg.ssl_cert.empty());
    EXPECT_TRUE(cfg.ssl_key.empty());
}

// ---------- BackendConfig field storage ----------

TEST(SSLConfigTest, FieldsStoreCorrectly) {
    BackendConfig cfg;
    cfg.ssl_mode = "REQUIRED";
    cfg.ssl_ca   = "/path/to/ca.pem";
    cfg.ssl_cert = "/path/to/client-cert.pem";
    cfg.ssl_key  = "/path/to/client-key.pem";

    EXPECT_EQ(cfg.ssl_mode, "REQUIRED");
    EXPECT_EQ(cfg.ssl_ca,   "/path/to/ca.pem");
    EXPECT_EQ(cfg.ssl_cert, "/path/to/client-cert.pem");
    EXPECT_EQ(cfg.ssl_key,  "/path/to/client-key.pem");
}

TEST(SSLConfigTest, PostgreSQLModeValues) {
    BackendConfig cfg;
    cfg.dialect  = Dialect::PostgreSQL;
    cfg.ssl_mode = "verify-full";
    cfg.ssl_ca   = "/etc/ssl/certs/ca.pem";

    EXPECT_EQ(cfg.ssl_mode, "verify-full");
    EXPECT_EQ(cfg.ssl_ca, "/etc/ssl/certs/ca.pem");
}

// ---------- URL query param parsing ----------
// Mirror the parse_backend_url logic from sqlengine.cpp to test SSL param extraction.

namespace {

struct ParsedBackend {
    BackendConfig config;
    bool ok = false;
    std::string error;
};

static ParsedBackend parse_backend_url(const std::string& url) {
    ParsedBackend pb;

    size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        pb.error = "Invalid URL (no scheme): " + url;
        return pb;
    }
    std::string scheme = url.substr(0, scheme_end);
    if (scheme == "mysql") {
        pb.config.dialect = Dialect::MySQL;
    } else if (scheme == "pgsql" || scheme == "postgres" || scheme == "postgresql") {
        pb.config.dialect = Dialect::PostgreSQL;
    } else {
        pb.error = "Unknown scheme: " + scheme;
        return pb;
    }

    std::string rest = url.substr(scheme_end + 3);

    size_t qpos = rest.find('?');
    std::string query_part;
    if (qpos != std::string::npos) {
        query_part = rest.substr(qpos + 1);
        rest = rest.substr(0, qpos);
    }

    // Parse key=value query params
    if (!query_part.empty()) {
        size_t pos = 0;
        while (pos < query_part.size()) {
            size_t amp = query_part.find('&', pos);
            std::string param = query_part.substr(pos,
                amp == std::string::npos ? std::string::npos : amp - pos);
            size_t eq = param.find('=');
            if (eq != std::string::npos) {
                std::string key = param.substr(0, eq);
                std::string val = param.substr(eq + 1);
                if (key == "name")          pb.config.name = val;
                else if (key == "ssl_mode") pb.config.ssl_mode = val;
                else if (key == "ssl_ca")   pb.config.ssl_ca = val;
                else if (key == "ssl_cert") pb.config.ssl_cert = val;
                else if (key == "ssl_key")  pb.config.ssl_key = val;
            }
            if (amp == std::string::npos) break;
            pos = amp + 1;
        }
    }

    // user:pass@host:port/db
    size_t at_pos = rest.find('@');
    if (at_pos != std::string::npos) {
        std::string userpass = rest.substr(0, at_pos);
        rest = rest.substr(at_pos + 1);
        size_t colon = userpass.find(':');
        if (colon != std::string::npos) {
            pb.config.user = userpass.substr(0, colon);
            pb.config.password = userpass.substr(colon + 1);
        } else {
            pb.config.user = userpass;
        }
    }

    size_t slash = rest.find('/');
    std::string hostport;
    if (slash != std::string::npos) {
        hostport = rest.substr(0, slash);
        pb.config.database = rest.substr(slash + 1);
    } else {
        hostport = rest;
    }

    size_t colon = hostport.find(':');
    if (colon != std::string::npos) {
        pb.config.host = hostport.substr(0, colon);
        try {
            pb.config.port = static_cast<uint16_t>(std::stoi(hostport.substr(colon + 1)));
        } catch (...) {
            pb.error = "Invalid port in: " + url;
            return pb;
        }
    } else {
        pb.config.host = hostport;
        pb.config.port = (pb.config.dialect == Dialect::MySQL) ? 3306 : 5432;
    }

    if (pb.config.name.empty())
        pb.config.name = pb.config.host + ":" + std::to_string(pb.config.port);

    pb.ok = true;
    return pb;
}

} // anonymous namespace

TEST(SSLConfigTest, ParseURLWithAllSSLParams) {
    auto pb = parse_backend_url(
        "mysql://root:test@127.0.0.1:13306/testdb"
        "?name=shard1&ssl_mode=REQUIRED&ssl_ca=/path/ca.pem"
        "&ssl_cert=/path/cert.pem&ssl_key=/path/key.pem");

    ASSERT_TRUE(pb.ok);
    EXPECT_EQ(pb.config.name,     "shard1");
    EXPECT_EQ(pb.config.ssl_mode, "REQUIRED");
    EXPECT_EQ(pb.config.ssl_ca,   "/path/ca.pem");
    EXPECT_EQ(pb.config.ssl_cert, "/path/cert.pem");
    EXPECT_EQ(pb.config.ssl_key,  "/path/key.pem");
    EXPECT_EQ(pb.config.host,     "127.0.0.1");
    EXPECT_EQ(pb.config.port,     13306);
    EXPECT_EQ(pb.config.database, "testdb");
}

TEST(SSLConfigTest, ParseURLWithOnlySSLMode) {
    auto pb = parse_backend_url(
        "mysql://user:pass@db.example.com:3306/mydb?ssl_mode=VERIFY_CA");

    ASSERT_TRUE(pb.ok);
    EXPECT_EQ(pb.config.ssl_mode, "VERIFY_CA");
    EXPECT_TRUE(pb.config.ssl_ca.empty());
    EXPECT_TRUE(pb.config.ssl_cert.empty());
    EXPECT_TRUE(pb.config.ssl_key.empty());
}

TEST(SSLConfigTest, ParseURLNoSSLParams) {
    auto pb = parse_backend_url(
        "mysql://root:test@127.0.0.1:3306/testdb?name=shard1");

    ASSERT_TRUE(pb.ok);
    EXPECT_EQ(pb.config.name, "shard1");
    EXPECT_TRUE(pb.config.ssl_mode.empty());
    EXPECT_TRUE(pb.config.ssl_ca.empty());
    EXPECT_TRUE(pb.config.ssl_cert.empty());
    EXPECT_TRUE(pb.config.ssl_key.empty());
}

TEST(SSLConfigTest, ParsePgSQLURLWithSSL) {
    auto pb = parse_backend_url(
        "pgsql://pguser:pgpass@pghost:5432/pgdb"
        "?name=pg1&ssl_mode=verify-full&ssl_ca=/etc/ssl/ca.pem");

    ASSERT_TRUE(pb.ok);
    EXPECT_EQ(pb.config.dialect,  Dialect::PostgreSQL);
    EXPECT_EQ(pb.config.ssl_mode, "verify-full");
    EXPECT_EQ(pb.config.ssl_ca,   "/etc/ssl/ca.pem");
    EXPECT_TRUE(pb.config.ssl_cert.empty());
    EXPECT_TRUE(pb.config.ssl_key.empty());
}

TEST(SSLConfigTest, ParseURLWithSSLVerifyIdentity) {
    auto pb = parse_backend_url(
        "mysql://u:p@h:3306/d?ssl_mode=VERIFY_IDENTITY"
        "&ssl_ca=/ca.pem&ssl_cert=/cert.pem&ssl_key=/key.pem");

    ASSERT_TRUE(pb.ok);
    EXPECT_EQ(pb.config.ssl_mode, "VERIFY_IDENTITY");
    EXPECT_EQ(pb.config.ssl_ca,   "/ca.pem");
    EXPECT_EQ(pb.config.ssl_cert, "/cert.pem");
    EXPECT_EQ(pb.config.ssl_key,  "/key.pem");
}
