// test_session.cpp — Session API end-to-end tests

#include <gtest/gtest.h>
#include "sql_engine/session.h"
#include "sql_engine/local_txn.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_engine/in_memory_catalog.h"
#include <cstring>
#include <string>

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
