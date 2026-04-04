// test_local_txn.cpp — Local transaction tests (begin/commit/rollback/savepoint)

#include <gtest/gtest.h>
#include "sql_engine/local_txn.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/parser.h"
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

class LocalTxnTest : public ::testing::Test {
protected:
    Arena data_arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Parser<Dialect::MySQL> parser;

    const TableInfo* users_table = nullptr;
    InMemoryMutableDataSource* users_source = nullptr;
    LocalTransactionManager* txn_mgr = nullptr;

    void SetUp() override {
        functions.register_builtins();

        catalog.add_table("", "users", {
            {"id",   SqlType::make_int(),        false},
            {"name", SqlType::make_varchar(255), true},
            {"age",  SqlType::make_int(),        true},
        });
        users_table = catalog.get_table(StringRef{"users", 5});

        std::vector<Row> initial = {
            build_row(data_arena, {value_int(1), value_string(arena_str(data_arena, "Alice")), value_int(25)}),
            build_row(data_arena, {value_int(2), value_string(arena_str(data_arena, "Bob")),   value_int(30)}),
            build_row(data_arena, {value_int(3), value_string(arena_str(data_arena, "Carol")), value_int(17)}),
        };
        users_source = new InMemoryMutableDataSource(users_table, data_arena, std::move(initial));

        txn_mgr = new LocalTransactionManager(data_arena);
        txn_mgr->register_source("users", users_source);
    }

    void TearDown() override {
        delete txn_mgr;
        delete users_source;
    }

    DmlResult run_dml(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) {
            DmlResult dr;
            dr.error_message = "parse error";
            return dr;
        }
        DmlPlanBuilder<Dialect::MySQL> dml_builder(catalog, parser.arena());
        PlanNode* plan = dml_builder.build(r.ast);
        if (!plan) {
            DmlResult dr;
            dr.error_message = "plan build error";
            return dr;
        }
        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_mutable_data_source("users", users_source);
        return executor.execute_dml(plan);
    }

    ResultSet run_select(const char* sql) {
        parser.reset();
        auto r = parser.parse(sql, std::strlen(sql));
        if (r.status != ParseResult::OK || !r.ast) return {};
        PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
        PlanNode* plan = builder.build(r.ast);
        if (!plan) return {};
        PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
        executor.add_mutable_data_source("users", users_source);
        return executor.execute(plan);
    }
};

// BEGIN + INSERT + COMMIT → data persists
TEST_F(LocalTxnTest, InsertCommit) {
    EXPECT_EQ(users_source->row_count(), 3u);
    EXPECT_TRUE(txn_mgr->begin());
    EXPECT_TRUE(txn_mgr->in_transaction());

    auto dr = run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(users_source->row_count(), 4u);

    EXPECT_TRUE(txn_mgr->commit());
    EXPECT_FALSE(txn_mgr->in_transaction());
    EXPECT_EQ(users_source->row_count(), 4u);
}

// BEGIN + INSERT + ROLLBACK → data reverted
TEST_F(LocalTxnTest, InsertRollback) {
    EXPECT_EQ(users_source->row_count(), 3u);
    EXPECT_TRUE(txn_mgr->begin());

    auto dr = run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(users_source->row_count(), 4u);

    EXPECT_TRUE(txn_mgr->rollback());
    EXPECT_FALSE(txn_mgr->in_transaction());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// BEGIN + UPDATE + ROLLBACK → original values restored
TEST_F(LocalTxnTest, UpdateRollback) {
    // Alice age=25 initially
    EXPECT_TRUE(txn_mgr->begin());

    auto dr = run_dml("UPDATE users SET age = 99 WHERE id = 1");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(dr.affected_rows, 1u);

    // Verify update took effect
    auto rs = run_select("SELECT * FROM users");
    bool found_99 = false;
    for (auto& row : rs.rows) {
        if (row.get(0).int_val == 1 && row.get(2).int_val == 99) found_99 = true;
    }
    EXPECT_TRUE(found_99);

    EXPECT_TRUE(txn_mgr->rollback());

    // After rollback, age should be 25 again
    rs = run_select("SELECT * FROM users");
    for (auto& row : rs.rows) {
        if (row.get(0).int_val == 1) {
            EXPECT_EQ(row.get(2).int_val, 25);
        }
    }
}

// BEGIN + DELETE + ROLLBACK → rows restored
TEST_F(LocalTxnTest, DeleteRollback) {
    EXPECT_EQ(users_source->row_count(), 3u);
    EXPECT_TRUE(txn_mgr->begin());

    auto dr = run_dml("DELETE FROM users WHERE id = 2");
    EXPECT_TRUE(dr.success);
    EXPECT_EQ(users_source->row_count(), 2u);

    EXPECT_TRUE(txn_mgr->rollback());
    EXPECT_EQ(users_source->row_count(), 3u);
}

// Multiple operations + ROLLBACK → all reverted
TEST_F(LocalTxnTest, MultipleOpsRollback) {
    EXPECT_TRUE(txn_mgr->begin());

    run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    run_dml("DELETE FROM users WHERE id = 1");
    run_dml("UPDATE users SET age = 99 WHERE id = 2");

    EXPECT_EQ(users_source->row_count(), 3u); // 3 + 1 - 1 = 3

    EXPECT_TRUE(txn_mgr->rollback());
    EXPECT_EQ(users_source->row_count(), 3u);

    // Verify original data restored
    auto rs = run_select("SELECT * FROM users");
    EXPECT_EQ(rs.row_count(), 3u);
    // Alice should exist with age 25
    bool found_alice = false;
    for (auto& row : rs.rows) {
        if (row.get(0).int_val == 1) {
            EXPECT_EQ(row.get(2).int_val, 25);
            found_alice = true;
        }
    }
    EXPECT_TRUE(found_alice);
}

// SAVEPOINT + ROLLBACK TO → partial revert
TEST_F(LocalTxnTest, SavepointRollbackTo) {
    EXPECT_TRUE(txn_mgr->begin());

    run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_EQ(users_source->row_count(), 4u);

    EXPECT_TRUE(txn_mgr->savepoint("sp1"));

    run_dml("INSERT INTO users (id, name, age) VALUES (5, 'Eve', 28)");
    EXPECT_EQ(users_source->row_count(), 5u);

    EXPECT_TRUE(txn_mgr->rollback_to("sp1"));
    EXPECT_EQ(users_source->row_count(), 4u); // Eve removed, Dave kept

    EXPECT_TRUE(txn_mgr->commit());
    EXPECT_EQ(users_source->row_count(), 4u);
}

// Auto-commit: INSERT without BEGIN → committed immediately
TEST_F(LocalTxnTest, AutoCommitInsert) {
    txn_mgr->set_auto_commit(true);
    EXPECT_TRUE(txn_mgr->is_auto_commit());
    EXPECT_FALSE(txn_mgr->in_transaction());

    // Direct insert (simulating auto-commit behavior from Session)
    txn_mgr->begin();
    auto dr = run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_TRUE(dr.success);
    txn_mgr->commit();

    EXPECT_EQ(users_source->row_count(), 4u);
    EXPECT_FALSE(txn_mgr->in_transaction());
}

// Release savepoint
TEST_F(LocalTxnTest, ReleaseSavepoint) {
    EXPECT_TRUE(txn_mgr->begin());
    EXPECT_TRUE(txn_mgr->savepoint("sp1"));
    EXPECT_TRUE(txn_mgr->release_savepoint("sp1"));
    // Rolling back to released savepoint should fail
    EXPECT_FALSE(txn_mgr->rollback_to("sp1"));
    EXPECT_TRUE(txn_mgr->commit());
}

// Rollback without begin should fail
TEST_F(LocalTxnTest, RollbackWithoutBegin) {
    EXPECT_FALSE(txn_mgr->in_transaction());
    EXPECT_FALSE(txn_mgr->rollback());
}

// Double begin: second begin re-snapshots
TEST_F(LocalTxnTest, DoubleBegin) {
    EXPECT_TRUE(txn_mgr->begin());
    run_dml("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 22)");
    EXPECT_EQ(users_source->row_count(), 4u);

    // Begin again — re-snapshots current state (4 rows)
    EXPECT_TRUE(txn_mgr->begin());
    run_dml("INSERT INTO users (id, name, age) VALUES (5, 'Eve', 28)");
    EXPECT_EQ(users_source->row_count(), 5u);

    EXPECT_TRUE(txn_mgr->rollback());
    // Should revert to state at second begin (4 rows)
    EXPECT_EQ(users_source->row_count(), 4u);
}
