#include <gtest/gtest.h>
#include "sql_engine/backend_config.h"
#include "sql_engine/tool_config_parser.h"

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

// ---------- Shared backend/shard parser ----------

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

TEST(SSLConfigTest, ParseShardSpecWithTwoShards) {
    auto ps = parse_shard_spec("users:id:shard1,shard2");

    ASSERT_TRUE(ps.ok);
    EXPECT_EQ(ps.config.table_name, "users");
    EXPECT_EQ(ps.config.shard_key, "id");
    ASSERT_EQ(ps.config.shards.size(), 2u);
    EXPECT_EQ(ps.config.shards[0].backend_name, "shard1");
    EXPECT_EQ(ps.config.shards[1].backend_name, "shard2");
}

TEST(SSLConfigTest, ParseShardSpecRejectsMissingShardList) {
    auto ps = parse_shard_spec("users:id:");

    EXPECT_FALSE(ps.ok);
    EXPECT_EQ(ps.error, "No shards specified in: users:id:");
}

// ---------- ParseShardSpec — routing strategies ----------

TEST(SSLConfigTest, ParseShardSpecDefaultsToHash) {
    auto ps = parse_shard_spec("users:id:shard1,shard2");

    ASSERT_TRUE(ps.ok);
    EXPECT_EQ(ps.config.strategy, RoutingStrategy::HASH);
    EXPECT_TRUE(ps.config.ranges.empty());
    EXPECT_TRUE(ps.config.list.empty());
}

TEST(SSLConfigTest, ParseShardSpecExplicitHash) {
    auto ps = parse_shard_spec("users:id:hash:shard1,shard2");

    ASSERT_TRUE(ps.ok);
    EXPECT_EQ(ps.config.strategy, RoutingStrategy::HASH);
    ASSERT_EQ(ps.config.shards.size(), 2u);
    EXPECT_EQ(ps.config.shards[0].backend_name, "shard1");
    EXPECT_EQ(ps.config.shards[1].backend_name, "shard2");
}

TEST(SSLConfigTest, ParseShardSpecRange) {
    auto ps = parse_shard_spec("users:id:range:5=shard1,10=shard2");

    ASSERT_TRUE(ps.ok);
    EXPECT_EQ(ps.config.strategy, RoutingStrategy::RANGE);
    ASSERT_EQ(ps.config.shards.size(), 2u);
    EXPECT_EQ(ps.config.shards[0].backend_name, "shard1");
    EXPECT_EQ(ps.config.shards[1].backend_name, "shard2");
    ASSERT_EQ(ps.config.ranges.size(), 2u);
    EXPECT_EQ(ps.config.ranges[0].upper_inclusive, 5);
    EXPECT_EQ(ps.config.ranges[0].shard_index, 0u);
    EXPECT_EQ(ps.config.ranges[1].upper_inclusive, 10);
    EXPECT_EQ(ps.config.ranges[1].shard_index, 1u);
}

TEST(SSLConfigTest, ParseShardSpecRangeRepeatedBackendInternsOnce) {
    // 5=a, 10=b, 15=a should produce two distinct backends in
    // the shards vector, with index 0 reused for the third entry.
    auto ps = parse_shard_spec("users:id:range:5=a,10=b,15=a");

    ASSERT_TRUE(ps.ok);
    ASSERT_EQ(ps.config.shards.size(), 2u);
    EXPECT_EQ(ps.config.shards[0].backend_name, "a");
    EXPECT_EQ(ps.config.shards[1].backend_name, "b");
    ASSERT_EQ(ps.config.ranges.size(), 3u);
    EXPECT_EQ(ps.config.ranges[2].shard_index, 0u);
}

TEST(SSLConfigTest, ParseShardSpecRejectsMalformedRangeEntry) {
    auto ps = parse_shard_spec("users:id:range:nobinding");

    EXPECT_FALSE(ps.ok);
    EXPECT_NE(ps.error.find("Invalid range entry"), std::string::npos);
}

TEST(SSLConfigTest, ParseShardSpecRejectsNonIntRangeBound) {
    auto ps = parse_shard_spec("users:id:range:abc=shard1");

    EXPECT_FALSE(ps.ok);
    EXPECT_NE(ps.error.find("integer"), std::string::npos);
}

TEST(SSLConfigTest, ParseShardSpecListInts) {
    auto ps = parse_shard_spec("users:id:list:1=a,2=a,6=b");

    ASSERT_TRUE(ps.ok);
    EXPECT_EQ(ps.config.strategy, RoutingStrategy::LIST);
    ASSERT_EQ(ps.config.list.size(), 3u);
    EXPECT_TRUE(ps.config.list[0].is_int);
    EXPECT_EQ(ps.config.list[0].int_val, 1);
    EXPECT_EQ(ps.config.list[0].shard_index, 0u);
    EXPECT_EQ(ps.config.list[2].int_val, 6);
    EXPECT_EQ(ps.config.list[2].shard_index, 1u);
}

TEST(SSLConfigTest, ParseShardSpecListStrings) {
    auto ps = parse_shard_spec("users:region:list:us-east=a,us-west=b");

    ASSERT_TRUE(ps.ok);
    EXPECT_EQ(ps.config.strategy, RoutingStrategy::LIST);
    ASSERT_EQ(ps.config.list.size(), 2u);
    EXPECT_FALSE(ps.config.list[0].is_int);
    EXPECT_EQ(ps.config.list[0].str_val, "us-east");
    EXPECT_FALSE(ps.config.list[1].is_int);
    EXPECT_EQ(ps.config.list[1].str_val, "us-west");
}

TEST(SSLConfigTest, ParseShardSpecRejectsEmptyRange) {
    auto ps = parse_shard_spec("users:id:range:");

    EXPECT_FALSE(ps.ok);
    EXPECT_NE(ps.error.find("at least one"), std::string::npos);
}

TEST(SSLConfigTest, ParseShardSpecRejectsEmptyList) {
    auto ps = parse_shard_spec("users:id:list:");

    EXPECT_FALSE(ps.ok);
    EXPECT_NE(ps.error.find("at least one"), std::string::npos);
}
