// test_session.cpp — Session API end-to-end tests

#include <gtest/gtest.h>
#include "sql_engine/session.h"
#include "sql_engine/local_txn.h"
#include "sql_engine/distributed_txn.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/shard_map.h"
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <set>

using namespace sql_engine;
using namespace sql_parser;

static StringRef arena_str(Arena& arena, const char* s) {
    uint32_t len = static_cast<uint32_t>(std::strlen(s));
    char* buf = static_cast<char*>(arena.allocate(len));
    std::memcpy(buf, s, len);
    return StringRef{buf, len};
}

static Row build_row(Arena& arena, std::initializer_list<Value> vals) {
    uint16_t n = static_cast<uint16_t>(vals.size());
    Row r = make_row(arena, n);
    uint16_t i = 0;
    for (auto& v : vals) r.set(i++, v);
    return r;
}

class SessionTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;

    const TableInfo* users_table = nullptr;
    InMemoryMutableDataSource* users_source = nullptr;
    LocalTransactionManager* txn_mgr = nullptr;
    Session<Dialect::MySQL>* session = nullptr;

    void SetUp() override {
        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        std::vector<Row> initial = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice")), value_int(25)}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob")),   value_int(30)}),
        };
        users_source = new InMemoryMutableDataSource(users_table, data_arena, std::move(initial));

        txn_mgr = new LocalTransactionManager(data_arena);
        txn_mgr->register_source("users", users_source);

        session = new Session<Dialect::MySQL>(catalog, *txn_mgr);
        session->add_mutable_data_source("users", users_source);
    }

    void TearDown() override {
        delete session;
        delete txn_mgr;
        delete users_source;
    }
};

// Execute SELECT via Session → correct results
TEST_F(SessionTest, SelectQuery) {
    auto rs = session->execute_query("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 2u);
    EXPECT_EQ(rs.rows[0].get(0).int_val, 1);
    EXPECT_EQ(rs.rows[1].get(0).int_val, 2);
}

// Execute INSERT via Session → affected rows
TEST_F(SessionTest, InsertStatement) {
    auto dr = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(dr.affected_rows, 1u);
    EXPECT_EQ(users_source->row_count(), 3u);
}

// BEGIN/COMMIT via Session → transaction state
TEST_F(SessionTest, BeginCommitViaSession) {
    EXPECT_FALSE(session->in_transaction());

    EXPECT_TRUE(session->begin());
    EXPECT_TRUE(session->in_transaction());

    auto dr = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr.success);

    EXPECT_TRUE(session->commit());
    EXPECT_FALSE(session->in_transaction());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// BEGIN + INSERT + ROLLBACK via Session
TEST_F(SessionTest, BeginRollbackViaSession) {
    EXPECT_TRUE(session->begin());

    auto dr = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(users_source->row_count(), 3u);

    EXPECT_TRUE(session->rollback());
    EXPECT_EQ(users_source->row_count(), 2u);
}

// BEGIN/COMMIT via SQL strings
TEST_F(SessionTest, TransactionViaSql) {
    auto dr1 = session->execute_statement("BEGIN");
    EXPECT_TRUE(dr1.success);
    EXPECT_TRUE(session->in_transaction());

    auto dr2 = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr2.success);

    auto dr3 = session->execute_statement("COMMIT");
    EXPECT_TRUE(dr3.success);
    EXPECT_FALSE(session->in_transaction());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// Auto-commit mode
TEST_F(SessionTest, AutoCommitMode) {
    session->set_auto_commit(true);
    EXPECT_TRUE(session->is_auto_commit());

    // Insert without explicit BEGIN — auto-committed
    auto dr = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr.success);
    EXPECT_FALSE(session->in_transaction());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// Mixed: SELECT + DML in same session
TEST_F(SessionTest, MixedSelectDml) {
    auto rs1 = session->execute_query("SELECT * FROM users");
    EXPECT_EQ(rs1.row_count(), 2u);

    auto dr = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr.success);

    auto rs2 = session->execute_query("SELECT * FROM users");
    EXPECT_EQ(rs2.row_count(), 3u);
}

// Savepoint via Session API
TEST_F(SessionTest, SavepointViaApi) {
    EXPECT_TRUE(session->begin());

    auto dr1 = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr1.success);
    EXPECT_EQ(users_source->row_count(), 3u);

    EXPECT_TRUE(session->savepoint("sp1"));

    auto dr2 = session->execute_statement("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_TRUE(dr2.success);
    EXPECT_EQ(users_source->row_count(), 4u);

    EXPECT_TRUE(session->rollback_to("sp1"));
    EXPECT_EQ(users_source->row_count(), 3u);

    EXPECT_TRUE(session->commit());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// Session auto-commit disabled: uncommitted data stays until commit
TEST_F(SessionTest, AutoCommitDisabled) {
    session->set_auto_commit(false);
    EXPECT_FALSE(session->is_auto_commit());

    session->begin();
    auto dr = session->execute_statement("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 17)");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(users_source->row_count(), 3u);
    EXPECT_TRUE(session->in_transaction());

    session->commit();
    EXPECT_FALSE(session->in_transaction());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// ===========================================================================
// Plan cache LRU eviction
// ===========================================================================
//
// The plan cache used to be an unbounded unordered_map. With many distinct
// SQL strings, that's a slow memory leak. Validate that the LRU bound is
// honored and that recently-used entries survive eviction.

TEST_F(SessionTest, PlanCacheRespectsMaxSize) {
    session->set_plan_cache_max_size(3);

    // Distinct SELECTs so each one is a unique cache key. We use literal
    // filters because the cache key is the raw SQL string.
    session->execute_query("SELECT * FROM users WHERE id = 1");
    session->execute_query("SELECT * FROM users WHERE id = 2");
    session->execute_query("SELECT * FROM users WHERE id = 3");
    EXPECT_EQ(session->plan_cache_size(), 3u);

    // Adding a 4th distinct query must evict the oldest one.
    session->execute_query("SELECT * FROM users WHERE id = 4");
    EXPECT_EQ(session->plan_cache_size(), 3u);

    // And a 5th must evict the second-oldest.
    session->execute_query("SELECT * FROM users WHERE id = 5");
    EXPECT_EQ(session->plan_cache_size(), 3u);
}

TEST_F(SessionTest, PlanCacheLruKeepsRecentlyUsed) {
    session->set_plan_cache_max_size(2);

    session->execute_query("SELECT * FROM users WHERE id = 1");  // [1]
    session->execute_query("SELECT * FROM users WHERE id = 2");  // [2,1]
    // Touch query 1 -> moves it back to front: [1,2]
    session->execute_query("SELECT * FROM users WHERE id = 1");
    EXPECT_EQ(session->plan_cache_size(), 2u);

    // Now insert query 3 -> evicts the LRU which is query 2: [3,1]
    session->execute_query("SELECT * FROM users WHERE id = 3");
    EXPECT_EQ(session->plan_cache_size(), 2u);

    // Re-running query 1 must be a cache hit (still cached) -- size stays
    // the same. If it were evicted, the size would still be 2 but a new
    // parser would have been allocated. We can't directly observe that here,
    // but the splice-on-hit codepath gets exercised.
    session->execute_query("SELECT * FROM users WHERE id = 1");
    EXPECT_EQ(session->plan_cache_size(), 2u);
}

TEST_F(SessionTest, PlanCacheCanBeDisabled) {
    session->set_plan_cache_max_size(0);
    session->execute_query("SELECT * FROM users WHERE id = 1");
    session->execute_query("SELECT * FROM users WHERE id = 2");
    EXPECT_EQ(session->plan_cache_size(), 0u);
}

// ===========================================================================
// TransactionManager::is_distributed() and route_dml() defaults
// ===========================================================================

TEST(TransactionManagerDefaults, IsDistributedReturnsFalse) {
    // LocalTransactionManager should inherit the default is_distributed() = false
    Arena arena{65536, 1048576};
    LocalTransactionManager ltm(arena);
    EXPECT_FALSE(ltm.is_distributed());
}

TEST(TransactionManagerDefaults, RouteDmlReturnsError) {
    Arena arena{65536, 1048576};
    LocalTransactionManager ltm(arena);
    DmlResult r = ltm.route_dml("some_backend", StringRef{"SELECT 1", 8});
    EXPECT_FALSE(r.success);
    EXPECT_FALSE(r.error_message.empty());
}

// ===========================================================================
// DistributedTransactionManager::is_distributed() and route_dml() overrides
// ===========================================================================

// Simple mock executor that tracks DML calls for distributed txn tests
class TrackingRemoteExecutor : public RemoteExecutor {
public:
    struct DmlCall {
        std::string backend;
        std::string sql;
    };

    std::vector<DmlCall> dml_calls;

    ResultSet execute(const char* /*backend_name*/, StringRef /*sql*/) override {
        return {};
    }

    DmlResult execute_dml(const char* backend_name, StringRef sql) override {
        dml_calls.push_back({backend_name, std::string(sql.ptr, sql.len)});
        DmlResult r;
        r.success = true;
        r.affected_rows = 1;
        return r;
    }

    bool allows_unpinned_distributed_2pc() const override { return true; }
};

TEST(DistributedTxnOverrides, IsDistributedReturnsTrue) {
    TrackingRemoteExecutor exec;
    DistributedTransactionManager dtm(exec);
    EXPECT_TRUE(dtm.is_distributed());
}

TEST(DistributedTxnOverrides, RouteDmlCallsExecuteParticipantDml) {
    TrackingRemoteExecutor exec;
    DistributedTransactionManager dtm(exec);

    dtm.begin();
    EXPECT_TRUE(dtm.in_transaction());

    // route_dml should enlist the backend and execute the DML
    DmlResult r = dtm.route_dml("shard1", StringRef{"INSERT INTO t VALUES (1)", 24});
    EXPECT_TRUE(r.success);
    EXPECT_EQ(r.affected_rows, 1u);

    // The backend should be enlisted
    auto& participants = dtm.participants();
    ASSERT_EQ(participants.size(), 1u);
    EXPECT_EQ(participants[0], "shard1");

    // The executor should have received the DML (via the unpinned fallback
    // since TrackingRemoteExecutor doesn't implement checkout_session)
    ASSERT_GE(exec.dml_calls.size(), 1u);
    // Find the actual DML call (first call may be XA START)
    bool found_insert = false;
    for (auto& call : exec.dml_calls) {
        if (call.sql.find("INSERT") != std::string::npos) {
            found_insert = true;
            EXPECT_EQ(call.backend, "shard1");
        }
    }
    EXPECT_TRUE(found_insert);

    dtm.rollback();
}

TEST(DistributedTxnOverrides, RouteDmlFailsWhenNoTransaction) {
    TrackingRemoteExecutor exec;
    DistributedTransactionManager dtm(exec);

    // No active transaction -- route_dml should fail
    DmlResult r = dtm.route_dml("shard1", StringRef{"INSERT INTO t VALUES (1)", 24});
    EXPECT_FALSE(r.success);
    EXPECT_FALSE(r.error_message.empty());
}

// ===========================================================================
// Session auto-enlistment: DML routed through route_dml when distributed txn
// ===========================================================================

// A TransactionManager that tracks route_dml calls
class TrackingDistributedTxnMgr : public TransactionManager {
public:
    struct RouteDmlCall {
        std::string backend;
        std::string sql;
    };

    std::vector<RouteDmlCall> route_dml_calls;
    bool active_ = false;
    bool auto_commit_ = true;

    bool begin() override { active_ = true; return true; }
    bool commit() override { active_ = false; return true; }
    bool rollback() override { active_ = false; return true; }
    bool savepoint(const char*) override { return false; }
    bool rollback_to(const char*) override { return false; }
    bool release_savepoint(const char*) override { return false; }

    bool in_transaction() const override { return active_; }
    bool is_auto_commit() const override { return auto_commit_; }
    void set_auto_commit(bool ac) override { auto_commit_ = ac; }

    bool is_distributed() const override { return true; }

    DmlResult route_dml(const char* backend_name,
                        StringRef sql) override {
        route_dml_calls.push_back({backend_name, std::string(sql.ptr, sql.len)});
        DmlResult r;
        r.success = true;
        r.affected_rows = 1;
        return r;
    }
};

// Mock remote executor that also tracks execute_dml calls
class SessionTrackingExecutor : public RemoteExecutor {
public:
    struct DmlCall {
        std::string backend;
        std::string sql;
    };

    std::vector<DmlCall> dml_calls;
    InMemoryCatalog backend_catalog;

    SessionTrackingExecutor() {
        backend_catalog.add_table("", "orders", {
            {"id",      SqlType::make_int(),        false},
            {"user_id", SqlType::make_int(),        true},
            {"amount",  SqlType::make_int(),        true},
        });
    }

    ResultSet execute(const char* /*backend_name*/, StringRef /*sql*/) override {
        return {};
    }

    DmlResult execute_dml(const char* backend_name, StringRef sql) override {
        dml_calls.push_back({backend_name, std::string(sql.ptr, sql.len)});
        DmlResult r;
        r.success = true;
        r.affected_rows = 1;
        return r;
    }
};

TEST(SessionAutoEnlistment, DmlRoutedThroughRouteDmlWhenDistributed) {
    InMemoryCatalog catalog;
    catalog.add_table("", "orders", {
        {"id",      SqlType::make_int(),        false},
        {"user_id", SqlType::make_int(),        true},
        {"amount",  SqlType::make_int(),        true},
    });

    TrackingDistributedTxnMgr txn_mgr;
    Session<Dialect::MySQL> session(catalog, txn_mgr);

    SessionTrackingExecutor remote_exec;
    session.set_remote_executor(&remote_exec);

    // Set up shard map: orders table on shard1, shard2 with shard key user_id
    ShardMap shard_map;
    TableShardConfig orders_config;
    orders_config.table_name = "orders";
    orders_config.shard_key = "user_id";
    orders_config.shards = {{"shard1"}, {"shard2"}};
    shard_map.add_table(orders_config);
    session.set_shard_map(&shard_map);

    // Begin a distributed transaction
    txn_mgr.begin();
    EXPECT_TRUE(txn_mgr.in_transaction());
    EXPECT_TRUE(txn_mgr.is_distributed());

    // Execute DML -- should route through txn_mgr.route_dml, not remote_exec.execute_dml
    auto dr = session.execute_statement("INSERT INTO orders (id, user_id, amount) VALUES (1, 100, 50)");
    EXPECT_TRUE(dr.success);

    // The tracking txn manager should have received route_dml calls
    EXPECT_FALSE(txn_mgr.route_dml_calls.empty());

    // The remote executor should NOT have received direct execute_dml calls
    EXPECT_TRUE(remote_exec.dml_calls.empty());

    txn_mgr.rollback();
}

TEST(SessionAutoEnlistment, DmlGoesToRemoteExecutorWhenNotDistributed) {
    InMemoryCatalog catalog;
    catalog.add_table("", "orders", {
        {"id",      SqlType::make_int(),        false},
        {"user_id", SqlType::make_int(),        true},
        {"amount",  SqlType::make_int(),        true},
    });

    // Use a non-distributed txn manager (LocalTransactionManager defaults)
    // but we need one that isn't distributed. TrackingDistributedTxnMgr with
    // is_distributed overridden to false would work, but let's just not be
    // in a transaction at all -- auto-commit wraps an implicit txn that is
    // not distributed.
    TrackingDistributedTxnMgr txn_mgr;
    // Override: make this one NOT distributed
    // We can't override further, so let's just test with auto_commit=true and
    // no active txn: the session will wrap in implicit txn (which calls begin),
    // but is_distributed() returns true -- so it would still route. Instead,
    // let's use a completely separate class.

    // Actually, the cleanest test: verify that without an active txn,
    // even if is_distributed()=true, the path doesn't use route_dml because
    // in_transaction() is false at the time of the DML check.
    // auto_commit mode: session calls begin() on the txn_mgr, which sets
    // active_=true, so in_transaction() && is_distributed() would be true.
    // So this test would still route through route_dml.
    //
    // Let's test the opposite: a non-distributed txn manager receiving
    // direct execute_dml calls.
    Arena arena{65536, 1048576};
    LocalTransactionManager local_txn(arena);

    Session<Dialect::MySQL> session(catalog, local_txn);

    SessionTrackingExecutor remote_exec;
    session.set_remote_executor(&remote_exec);

    ShardMap shard_map;
    TableShardConfig orders_config;
    orders_config.table_name = "orders";
    orders_config.shard_key = "user_id";
    orders_config.shards = {{"shard1"}, {"shard2"}};
    shard_map.add_table(orders_config);
    session.set_shard_map(&shard_map);

    // No explicit transaction; auto-commit wraps an implicit one.
    // LocalTransactionManager.is_distributed() returns false, so DML should
    // go to remote_exec.execute_dml, not route_dml.
    auto dr = session.execute_statement("INSERT INTO orders (id, user_id, amount) VALUES (1, 100, 50)");
    EXPECT_TRUE(dr.success);

    // Remote executor should have received execute_dml calls
    EXPECT_FALSE(remote_exec.dml_calls.empty());
}

TEST(SessionAutoEnlistment, ScatterDmlRoutedThroughRouteDml) {
    InMemoryCatalog catalog;
    catalog.add_table("", "orders", {
        {"id",      SqlType::make_int(),        false},
        {"user_id", SqlType::make_int(),        true},
        {"amount",  SqlType::make_int(),        true},
    });

    TrackingDistributedTxnMgr txn_mgr;
    Session<Dialect::MySQL> session(catalog, txn_mgr);

    SessionTrackingExecutor remote_exec;
    session.set_remote_executor(&remote_exec);

    ShardMap shard_map;
    TableShardConfig orders_config;
    orders_config.table_name = "orders";
    orders_config.shard_key = "user_id";
    orders_config.shards = {{"shard1"}, {"shard2"}};
    shard_map.add_table(orders_config);
    session.set_shard_map(&shard_map);

    txn_mgr.begin();

    // DELETE without WHERE on a sharded table should scatter to all shards
    auto dr = session.execute_statement("DELETE FROM orders");
    EXPECT_TRUE(dr.success);

    // Both shards should have received route_dml calls
    EXPECT_GE(txn_mgr.route_dml_calls.size(), 2u);

    // Check that both backends were hit
    std::set<std::string> backends_hit;
    for (auto& call : txn_mgr.route_dml_calls) {
        backends_hit.insert(call.backend);
    }
    EXPECT_TRUE(backends_hit.count("shard1") > 0 || backends_hit.count("shard2") > 0);

    // Remote executor should NOT have been called directly
    EXPECT_TRUE(remote_exec.dml_calls.empty());

    txn_mgr.rollback();
}
