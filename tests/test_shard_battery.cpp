#include <gtest/gtest.h>
#include "sql_engine/shard_map.h"
#include "sql_engine/distributed_planner.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/dml_plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/function_registry.h"
#include "sql_engine/remote_executor.h"
#include "sql_engine/mutable_data_source.h"
#include "sql_engine/session.h"
#include "sql_engine/local_txn.h"
#include "sql_parser/parser.h"

#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <memory>

using namespace sql_engine;
using namespace sql_parser;

namespace {

StringRef sref(const char* s) {
    return StringRef{s, static_cast<uint32_t>(std::strlen(s))};
}

void find_nodes(PlanNode* n, PlanNodeType t, std::vector<PlanNode*>& out) {
    if (!n) return;
    if (n->type == t) out.push_back(n);
    find_nodes(n->left, t, out);
    find_nodes(n->right, t, out);
    if (n->type == PlanNodeType::MERGE_SORT) {
        for (uint16_t i = 0; i < n->merge_sort.child_count; ++i)
            find_nodes(n->merge_sort.children[i], t, out);
    }
    if (n->type == PlanNodeType::MERGE_AGGREGATE) {
        for (uint16_t i = 0; i < n->merge_aggregate.child_count; ++i)
            find_nodes(n->merge_aggregate.children[i], t, out);
    }
}

TableShardConfig hash3() {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id";
    c.shards = {{"a"}, {"b"}, {"c"}};
    return c;
}

} // namespace

// =====================================================================
// ShardMap routing battery
// =====================================================================

TEST(ShardBatteryHash, SameValueSameShard100) {
    ShardMap map;
    map.add_table(hash3());
    for (int i = -50; i < 50; ++i) {
        size_t x = 0, y = 0;
        ASSERT_TRUE(map.try_shard_index_for_int(sref("users"), i, x));
        ASSERT_TRUE(map.try_shard_index_for_int(sref("users"), i, y));
        EXPECT_EQ(x, y);
        EXPECT_LT(x, 3u);
    }
}

TEST(ShardBatteryHash, StringIntMatchesIntWide) {
    ShardMap map;
    map.add_table(hash3());
    for (int i = -30; i < 30; ++i) {
        std::string s = std::to_string(i);
        size_t a = 0, b = 0;
        ASSERT_TRUE(map.try_shard_index_for_int(sref("users"), i, a));
        ASSERT_TRUE(map.try_shard_index_for_string(
            sref("users"), s.c_str(), static_cast<uint32_t>(s.size()), b));
        EXPECT_EQ(a, b) << i;
    }
}

TEST(ShardBatteryHash, HitsAllThreeShards) {
    ShardMap map;
    map.add_table(hash3());
    bool seen[3] = {};
    for (int i = 0; i < 64; ++i) {
        size_t x = 0;
        ASSERT_TRUE(map.try_shard_index_for_int(sref("users"), i, x));
        seen[x] = true;
    }
    EXPECT_TRUE(seen[0] && seen[1] && seen[2]);
}

TEST(ShardBatteryHash, UnknownTableFalse) {
    ShardMap map;
    size_t x = 7;
    EXPECT_FALSE(map.try_shard_index_for_int(sref("nope"), 1, x));
    EXPECT_EQ(map.shard_index_for_int(sref("nope"), 1), static_cast<size_t>(-1));
}

TEST(ShardBatteryHash, CaseInsensitiveTable) {
    ShardMap map;
    map.add_table(hash3());
    size_t a = 0, b = 0;
    ASSERT_TRUE(map.try_shard_index_for_int(sref("USERS"), 5, a));
    ASSERT_TRUE(map.try_shard_index_for_int(sref("users"), 5, b));
    EXPECT_EQ(a, b);
}

TEST(ShardBatteryHash, EmptyStringRoutes) {
    ShardMap map;
    map.add_table(hash3());
    size_t x = 9;
    EXPECT_TRUE(map.try_shard_index_for_string(sref("users"), "", 0, x));
    EXPECT_LT(x, 3u);
}

TEST(ShardBatteryHash, LongStringStable) {
    ShardMap map;
    map.add_table(hash3());
    const char* s = "the-quick-brown-fox-jumps-over-the-lazy-dog";
    uint32_t n = static_cast<uint32_t>(std::strlen(s));
    size_t a = 0, b = 0;
    ASSERT_TRUE(map.try_shard_index_for_string(sref("users"), s, n, a));
    ASSERT_TRUE(map.try_shard_index_for_string(sref("users"), s, n, b));
    EXPECT_EQ(a, b);
}

TEST(ShardBatteryRange, Bounds) {
    TableShardConfig c = hash3();
    c.strategy = RoutingStrategy::RANGE;
    c.ranges = {{0, 0}, {10, 1}, {100, 2}};
    ShardMap map;
    map.add_table(c);
    size_t x = 9;
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), -1, x));
    EXPECT_EQ(x, 0u);
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 0, x));
    EXPECT_EQ(x, 0u);
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 1, x));
    EXPECT_EQ(x, 1u);
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 10, x));
    EXPECT_EQ(x, 1u);
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 11, x));
    EXPECT_EQ(x, 2u);
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 9999, x));
    EXPECT_EQ(x, 2u);
}

TEST(ShardBatteryRange, CollectWindow) {
    TableShardConfig c = hash3();
    c.strategy = RoutingStrategy::RANGE;
    c.ranges = {{5, 0}, {10, 1}, {100, 2}};
    ShardMap map;
    map.add_table(c);
    std::vector<size_t> out;
    map.collect_int_range_shards(sref("users"), 6, 10, out);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 1u);
}

TEST(ShardBatteryRange, StringUnroutable) {
    TableShardConfig c = hash3();
    c.strategy = RoutingStrategy::RANGE;
    c.ranges = {{5, 0}, {10, 1}};
    ShardMap map;
    map.add_table(c);
    size_t x = 0;
    EXPECT_FALSE(map.try_shard_index_for_string(sref("users"), "3", 1, x));
}

TEST(ShardBatteryList, MappedAndMiss) {
    TableShardConfig c = hash3();
    c.strategy = RoutingStrategy::LIST;
    c.list = {{true, 1, "", 0}, {true, 2, "", 0}, {true, 9, "", 2}};
    ShardMap map;
    map.add_table(c);
    size_t x = 9;
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 1, x));
    EXPECT_EQ(x, 0u);
    EXPECT_TRUE(map.try_shard_index_for_int(sref("users"), 9, x));
    EXPECT_EQ(x, 2u);
    EXPECT_FALSE(map.try_shard_index_for_int(sref("users"), 3, x));
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 3), static_cast<size_t>(-1));
}

TEST(ShardBatteryList, BetweenTwoShards) {
    TableShardConfig c = hash3();
    c.strategy = RoutingStrategy::LIST;
    c.list = {{true, 1, "", 0}, {true, 5, "", 1}, {true, 9, "", 2}};
    ShardMap map;
    map.add_table(c);
    std::vector<size_t> out;
    map.collect_int_list_shards(sref("users"), 1, 5, out);
    ASSERT_EQ(out.size(), 2u);
}

TEST(ShardBatteryList, StringKeys) {
    TableShardConfig c = hash3();
    c.strategy = RoutingStrategy::LIST;
    c.list = {{false, 0, "east", 0}, {false, 0, "west", 1}};
    ShardMap map;
    map.add_table(c);
    size_t x = 9;
    EXPECT_TRUE(map.try_shard_index_for_string(sref("users"), "east", 4, x));
    EXPECT_EQ(x, 0u);
    EXPECT_FALSE(map.try_shard_index_for_string(sref("users"), "north", 5, x));
}

TEST(ShardBatteryComposite, HashTwoPartsStable) {
    TableShardConfig c;
    c.table_name = "kv";
    c.shard_key = "t+id";
    c.shards = {{"a"}, {"b"}, {"c"}};
    ShardMap map;
    map.add_table(c);
    ShardKeyPart p[] = {{true, 1, nullptr, 0}, {true, 2, nullptr, 0}};
    size_t a = 0, b = 0;
    ASSERT_TRUE(map.try_shard_index_for_parts(sref("kv"), p, 2, a));
    ASSERT_TRUE(map.try_shard_index_for_parts(sref("kv"), p, 2, b));
    EXPECT_EQ(a, b);
}

TEST(ShardBatteryComposite, RangeUsesFirstOnly) {
    TableShardConfig c;
    c.table_name = "kv";
    c.shard_key = "t+id";
    c.shards = {{"a"}, {"b"}};
    c.strategy = RoutingStrategy::RANGE;
    c.ranges = {{5, 0}, {100, 1}};
    ShardMap map;
    map.add_table(c);
    ShardKeyPart lo[] = {{true, 1, nullptr, 0}, {true, 99, nullptr, 0}};
    ShardKeyPart hi[] = {{true, 9, nullptr, 0}, {true, 0, nullptr, 0}};
    size_t a = 9, b = 9;
    ASSERT_TRUE(map.try_shard_index_for_parts(sref("kv"), lo, 2, a));
    ASSERT_TRUE(map.try_shard_index_for_parts(sref("kv"), hi, 2, b));
    EXPECT_EQ(a, 0u);
    EXPECT_EQ(b, 1u);
}

TEST(ShardBatteryComposite, GetKeysSplit) {
    TableShardConfig c;
    c.table_name = "kv";
    c.shard_key = "tenant_id+user_id";
    c.shards = {{"a"}, {"b"}};
    ShardMap map;
    map.add_table(c);
    const auto& keys = map.get_shard_keys(sref("kv"));
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "tenant_id");
    EXPECT_EQ(keys[1], "user_id");
}

TEST(ShardBatterySameRouting, IdenticalLayouts) {
    ShardMap map;
    TableShardConfig a = hash3();
    TableShardConfig b = hash3();
    b.table_name = "orders";
    b.shard_key = "user_id";
    map.add_table(a);
    map.add_table(b);
    EXPECT_TRUE(map.same_routing(sref("users"), sref("orders")));
}

TEST(ShardBatterySameRouting, DifferentShardCount) {
    ShardMap map;
    map.add_table(hash3());
    TableShardConfig b;
    b.table_name = "orders";
    b.shard_key = "id";
    b.shards = {{"a"}, {"b"}};
    map.add_table(b);
    EXPECT_FALSE(map.same_routing(sref("users"), sref("orders")));
}

// =====================================================================
// Planner prune battery
// =====================================================================

class PruneBattery : public ::testing::Test {
protected:
    InMemoryCatalog catalog;
    ShardMap shards;
    const TableInfo* users = nullptr;

    void SetUp() override {
        catalog.add_table("", "users", {
            {"id", SqlType::make_int(), false},
            {"name", SqlType::make_varchar(255), true},
            {"age", SqlType::make_int(), true},
        });
        users = catalog.get_table(sref("users"));
        shards.add_table({"users", "id", {{"s0"}, {"s1"}, {"s2"}}});
    }

    size_t remotes(const char* sql) {
        Parser<Dialect::MySQL> p;
        auto pr = p.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK) return 999;
        PlanBuilder<Dialect::MySQL> b(catalog, p.arena());
        PlanNode* plan = b.build(pr.ast);
        DistributedPlanner<Dialect::MySQL> dp(shards, catalog, p.arena());
        PlanNode* dist = dp.distribute(plan);
        if (!dist && dp.last_error()) return 0;
        std::vector<PlanNode*> out;
        find_nodes(dist, PlanNodeType::REMOTE_SCAN, out);
        return out.size();
    }

    void set_range() {
        TableShardConfig c;
        c.table_name = "users";
        c.shard_key = "id";
        c.shards = {{"s0"}, {"s1"}, {"s2"}};
        c.strategy = RoutingStrategy::RANGE;
        c.ranges = {{5, 0}, {10, 1}, {100000, 2}};
        shards.add_table(c);
    }

    void set_list() {
        TableShardConfig c;
        c.table_name = "users";
        c.shard_key = "id";
        c.shards = {{"s0"}, {"s1"}, {"s2"}};
        c.strategy = RoutingStrategy::LIST;
        c.list = {{true, 1, "", 0}, {true, 6, "", 1}, {true, 7, "", 1},
                  {true, 15, "", 2}};
        shards.add_table(c);
    }
};

TEST_F(PruneBattery, HashPointIsSingle) { EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 4"), 1u); }
TEST_F(PruneBattery, HashNoWhereIsThree) { EXPECT_EQ(remotes("SELECT * FROM users"), 3u); }
TEST_F(PruneBattery, HashNonKeyIsThree) { EXPECT_EQ(remotes("SELECT * FROM users WHERE age = 1"), 3u); }
TEST_F(PruneBattery, HashInTwoValues) {
    size_t n = remotes("SELECT * FROM users WHERE id IN (1, 2)");
    EXPECT_GE(n, 1u);
    EXPECT_LE(n, 2u);
}
TEST_F(PruneBattery, HashPlaceholderScatters) { EXPECT_EQ(remotes("SELECT * FROM users WHERE id = ?"), 3u); }
TEST_F(PruneBattery, RangeLe5) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id <= 5"), 1u); }
TEST_F(PruneBattery, RangeGt10) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id > 10"), 1u); }
TEST_F(PruneBattery, RangeBetween6And10) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id BETWEEN 6 AND 10"), 1u); }
TEST_F(PruneBattery, RangeGe1Le15) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id >= 1 AND id <= 15"), 3u); }
TEST_F(PruneBattery, RangeLt1) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id < 1"), 1u); }
TEST_F(PruneBattery, RangeEq3) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 3"), 1u); }
TEST_F(PruneBattery, RangeEq7) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 7"), 1u); }
TEST_F(PruneBattery, RangeEq20) { set_range(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 20"), 1u); }
TEST_F(PruneBattery, ListEq1) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 1"), 1u); }
TEST_F(PruneBattery, ListEq6) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 6"), 1u); }
TEST_F(PruneBattery, ListMissScatters) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 99"), 3u); }
TEST_F(PruneBattery, ListBetween67) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id BETWEEN 6 AND 7"), 1u); }
TEST_F(PruneBattery, ListBetweenAll) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id BETWEEN 1 AND 15"), 3u); }
TEST_F(PruneBattery, ListInMappedAndMiss) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id IN (6, 99)"), 1u); }
TEST_F(PruneBattery, ListInTwoShards) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id IN (6, 15)"), 2u); }
TEST_F(PruneBattery, ListOrTwoShards) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 1 OR id = 6"), 2u); }
TEST_F(PruneBattery, ListEmptyBetweenScatters) { set_list(); EXPECT_EQ(remotes("SELECT * FROM users WHERE id BETWEEN 2 AND 5"), 3u); }
TEST_F(PruneBattery, AndWithNonKeyStillPrunes) {
    EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 4 AND age > 0"), 1u);
}
TEST_F(PruneBattery, SelectStarCount) { EXPECT_GE(remotes("SELECT COUNT(*) FROM users"), 3u); }
TEST_F(PruneBattery, SelectStarCountPoint) { EXPECT_GE(remotes("SELECT COUNT(*) FROM users WHERE id = 4"), 1u); }
TEST_F(PruneBattery, OrderByScatter) { EXPECT_GE(remotes("SELECT * FROM users ORDER BY name"), 3u); }
TEST_F(PruneBattery, LimitScatter) { EXPECT_EQ(remotes("SELECT * FROM users LIMIT 5"), 3u); }
TEST_F(PruneBattery, DistinctScatter) { EXPECT_EQ(remotes("SELECT DISTINCT name FROM users"), 3u); }

TEST_F(PruneBattery, UnknownTableErrors) {
    catalog.add_table("", "ghost", {{"id", SqlType::make_int(), false}});
    Parser<Dialect::MySQL> p;
    auto pr = p.parse("SELECT * FROM ghost", 19);
    PlanBuilder<Dialect::MySQL> b(catalog, p.arena());
    PlanNode* plan = b.build(pr.ast);
    DistributedPlanner<Dialect::MySQL> dp(shards, catalog, p.arena());
    EXPECT_EQ(dp.distribute(plan), nullptr);
    ASSERT_NE(dp.last_error(), nullptr);
}

TEST_F(PruneBattery, CompositePartialScatters) {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id+age";
    c.shards = {{"s0"}, {"s1"}, {"s2"}};
    shards.add_table(c);
    EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 3"), 3u);
}

TEST_F(PruneBattery, CompositeBothKeysPrune) {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id+age";
    c.shards = {{"s0"}, {"s1"}, {"s2"}};
    shards.add_table(c);
    EXPECT_EQ(remotes("SELECT * FROM users WHERE id = 3 AND age = 17"), 1u);
}

TEST_F(PruneBattery, CompositeRangeFirstKey) {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id+name";
    c.shards = {{"s0"}, {"s1"}, {"s2"}};
    c.strategy = RoutingStrategy::RANGE;
    c.ranges = {{5, 0}, {10, 1}, {100000, 2}};
    shards.add_table(c);
    EXPECT_EQ(remotes("SELECT * FROM users WHERE id <= 5"), 1u);
    EXPECT_EQ(remotes("SELECT * FROM users WHERE id BETWEEN 6 AND 10"), 1u);
}

// =====================================================================
// DML battery (compact mock)
// =====================================================================

struct BatBackend {
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    Arena arena{65536, 1048576};
    std::map<std::string, InMemoryMutableDataSource*> srcs;
    std::vector<std::unique_ptr<MutableDataSource>> owned;
    BatBackend() { functions.register_builtins(); }
    void add_table(const char* n, std::initializer_list<ColumnDef> cols) {
        catalog.add_table("", n, cols);
        const TableInfo* ti = catalog.get_table(sref(n));
        auto* s = new InMemoryMutableDataSource(ti, arena);
        srcs[n] = s;
        owned.emplace_back(s);
    }
};

class BatExec : public RemoteExecutor {
public:
    void add_backend(const std::string& n) { b_[n] = std::make_unique<BatBackend>(); }
    BatBackend* get(const std::string& n) {
        auto it = b_.find(n);
        return it == b_.end() ? nullptr : it->second.get();
    }
    void add_users_all() {
        for (auto& kv : b_) {
            kv.second->add_table("users", {
                {"id", SqlType::make_int(), false},
                {"name", SqlType::make_varchar(255), true},
                {"age", SqlType::make_int(), true},
            });
        }
    }
    ResultSet execute(const char* name, StringRef sql) override {
        auto* bd = get(name);
        if (!bd) return ResultSet::fail("unknown backend");
        std::string s(sql.ptr, sql.len);
        if (s.size() > 11 && s.compare(s.size() - 11, 11, " FOR UPDATE") == 0)
            s.resize(s.size() - 11);
        if (s.rfind("INSERT", 0) == 0 || s.rfind("UPDATE", 0) == 0 || s.rfind("DELETE", 0) == 0) {
            run_dml(bd, s);
            return {};
        }
        Parser<Dialect::MySQL> p;
        auto pr = p.parse(s.c_str(), s.size());
        if (pr.status != ParseResult::OK || !pr.ast) return {};
        PlanBuilder<Dialect::MySQL> b(bd->catalog, p.arena());
        PlanNode* plan = b.build(pr.ast);
        if (!plan) return {};
        PlanExecutor<Dialect::MySQL> ex(bd->functions, bd->catalog, p.arena());
        for (auto& t : bd->srcs) ex.add_mutable_data_source(t.first.c_str(), t.second);
        ResultSet rs = ex.execute(plan);
        rs.ok = true;
        return rs;
    }
    DmlResult execute_dml(const char* name, StringRef sql) override {
        auto* bd = get(name);
        if (!bd) {
            DmlResult r;
            r.error_message = "unknown backend";
            return r;
        }
        return run_dml(bd, std::string(sql.ptr, sql.len));
    }
    size_t total(const char* table) {
        size_t n = 0;
        for (auto& kv : b_) {
            auto it = kv.second->srcs.find(table);
            if (it != kv.second->srcs.end()) n += it->second->row_count();
        }
        return n;
    }
    size_t on(const char* backend, const char* table) {
        auto* bd = get(backend);
        if (!bd) return 0;
        auto it = bd->srcs.find(table);
        return it == bd->srcs.end() ? 0 : it->second->row_count();
    }
private:
    std::map<std::string, std::unique_ptr<BatBackend>> b_;
    DmlResult run_dml(BatBackend* bd, const std::string& sql) {
        Parser<Dialect::MySQL> p;
        auto pr = p.parse(sql.c_str(), sql.size());
        if (pr.status != ParseResult::OK || !pr.ast) {
            DmlResult r;
            r.error_message = "parse error";
            return r;
        }
        DmlPlanBuilder<Dialect::MySQL> b(bd->catalog, p.arena());
        PlanNode* plan = b.build(pr.ast);
        if (!plan) {
            DmlResult r;
            r.error_message = "plan error";
            return r;
        }
        PlanExecutor<Dialect::MySQL> ex(bd->functions, bd->catalog, p.arena());
        for (auto& t : bd->srcs) ex.add_mutable_data_source(t.first.c_str(), t.second);
        return ex.execute_dml(plan);
    }
};

class DmlBattery : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    InMemoryCatalog catalog;
    FunctionRegistry<Dialect::MySQL> functions;
    ShardMap shards;
    BatExec exec;

    void SetUp() override {
        functions.register_builtins();
        catalog.add_table("", "users", {
            {"id", SqlType::make_int(), false},
            {"name", SqlType::make_varchar(255), true},
            {"age", SqlType::make_int(), true},
        });
        shards.add_table({"users", "id", {{"s0"}, {"s1"}, {"s2"}}});
        exec.add_backend("s0");
        exec.add_backend("s1");
        exec.add_backend("s2");
        exec.add_users_all();
    }

    const char* backend(int64_t id) {
        return shards.get_shards(sref("users"))[shards.shard_index_for_int(sref("users"), id)]
            .backend_name.c_str();
    }

    DmlResult dml(const char* sql) {
        Parser<Dialect::MySQL> p;
        auto pr = p.parse(sql, std::strlen(sql));
        if (pr.status != ParseResult::OK || !pr.ast) {
            DmlResult r;
            r.error_message = "parse";
            return r;
        }
        DmlPlanBuilder<Dialect::MySQL> b(catalog, p.arena());
        PlanNode* plan = b.build(pr.ast);
        DistributedPlanner<Dialect::MySQL> dp(shards, catalog, p.arena(), &exec, &functions);
        PlanNode* dist = dp.distribute_dml(plan);
        if (dp.last_error()) {
            DmlResult r;
            r.error_message = dp.last_error();
            return r;
        }
        DmlResult total;
        total.success = true;
        std::function<void(PlanNode*)> walk = [&](PlanNode* n) {
            if (!n) return;
            if (n->type == PlanNodeType::REMOTE_SCAN) {
                StringRef s{n->remote_scan.remote_sql, n->remote_scan.remote_sql_len};
                DmlResult r = exec.execute_dml(n->remote_scan.backend_name, s);
                if (!r.success) {
                    total.success = false;
                    total.error_message = r.error_message;
                }
                total.affected_rows += r.affected_rows;
                return;
            }
            walk(n->left);
            walk(n->right);
        };
        walk(dist);
        return total;
    }

    ResultSet sel(const char* sql) {
        Parser<Dialect::MySQL> p;
        auto pr = p.parse(sql, std::strlen(sql));
        PlanBuilder<Dialect::MySQL> b(catalog, p.arena());
        PlanNode* plan = b.build(pr.ast);
        DistributedPlanner<Dialect::MySQL> dp(shards, catalog, p.arena(), &exec, &functions);
        PlanNode* dist = dp.distribute(plan);
        PlanExecutor<Dialect::MySQL> ex(functions, catalog, p.arena());
        ex.set_remote_executor(&exec);
        return ex.execute(dist);
    }
};

TEST_F(DmlBattery, InsertThenSelect0) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (0, 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 0").row_count(), 1u);
}
TEST_F(DmlBattery, InsertThenSelect1) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (1, 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 1").row_count(), 1u);
}
TEST_F(DmlBattery, InsertThenSelect2) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (2, 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 2").row_count(), 1u);
}
TEST_F(DmlBattery, InsertThenSelect7) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (7, 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 7").row_count(), 1u);
}
TEST_F(DmlBattery, InsertThenSelect13) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (13, 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 13").row_count(), 1u);
}
TEST_F(DmlBattery, InsertThenSelectNeg3) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (-3, 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = -3").row_count(), 1u);
}
TEST_F(DmlBattery, InsertStringThenIntSelect) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES ('5', 'n', 1)").success);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 5").row_count(), 1u);
}
TEST_F(DmlBattery, InsertLandsOnHashedShard) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (8, 'n', 1)").success);
    EXPECT_EQ(exec.on(backend(8), "users"), 1u);
    EXPECT_EQ(exec.total("users"), 1u);
}
TEST_F(DmlBattery, MissingKeyErrors) {
    EXPECT_FALSE(dml("INSERT INTO users (name, age) VALUES ('n', 1)").success);
}
TEST_F(DmlBattery, NonLiteralErrors) {
    EXPECT_FALSE(dml("INSERT INTO users (id, name, age) VALUES (1+1, 'n', 1)").success);
}
TEST_F(DmlBattery, DeletePoint) {
    dml("INSERT INTO users (id, name, age) VALUES (4, 'n', 1)");
    EXPECT_TRUE(dml("DELETE FROM users WHERE id = 4").success);
    EXPECT_EQ(exec.total("users"), 0u);
}
TEST_F(DmlBattery, DeleteScatterNone) {
    dml("INSERT INTO users (id, name, age) VALUES (4, 'n', 1)");
    EXPECT_TRUE(dml("DELETE FROM users WHERE age = 99").success);
    EXPECT_EQ(exec.total("users"), 1u);
}
TEST_F(DmlBattery, UpdateNonKeyPoint) {
    dml("INSERT INTO users (id, name, age) VALUES (4, 'n', 1)");
    EXPECT_TRUE(dml("UPDATE users SET age = 9 WHERE id = 4").success);
    auto rs = sel("SELECT age FROM users WHERE id = 4");
    ASSERT_EQ(rs.row_count(), 1u);
    EXPECT_EQ(rs.rows[0].get(0).int_val, 9);
}
TEST_F(DmlBattery, UpdateMoveCrossShard) {
    dml("INSERT INTO users (id, name, age) VALUES (3, 'n', 1)");
    int64_t dest = 3;
    for (int64_t i = 4; i < 80; ++i) {
        if (std::strcmp(backend(i), backend(3)) != 0) { dest = i; break; }
    }
    ASSERT_NE(dest, 3);
    std::string sql = "UPDATE users SET id = " + std::to_string(dest) + " WHERE id = 3";
    EXPECT_TRUE(dml(sql.c_str()).success);
    EXPECT_EQ(exec.on(backend(3), "users"), 0u);
    EXPECT_EQ(exec.on(backend(dest), "users"), 1u);
}
TEST_F(DmlBattery, UpdateQualifiedMove) {
    dml("INSERT INTO users (id, name, age) VALUES (3, 'n', 1)");
    int64_t dest = 9;
    if (std::strcmp(backend(3), backend(9)) == 0) dest = 8;
    std::string sql = "UPDATE users SET users.id = " + std::to_string(dest) + " WHERE id = 3";
    EXPECT_TRUE(dml(sql.c_str()).success);
    EXPECT_EQ(sel(("SELECT name FROM users WHERE id = " + std::to_string(dest)).c_str()).row_count(), 1u);
}
TEST_F(DmlBattery, UpdateMoveNoRow) {
    dml("INSERT INTO users (id, name, age) VALUES (3, 'n', 1)");
    EXPECT_TRUE(dml("UPDATE users SET id = 9 WHERE id = 99").success);
    EXPECT_EQ(exec.total("users"), 1u);
}
TEST_F(DmlBattery, MultiInsertThenEachSelect) {
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (10, 'a', 1), (11, 'b', 1), (12, 'c', 1)").success);
    EXPECT_EQ(exec.total("users"), 3u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 10").row_count(), 1u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 11").row_count(), 1u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 12").row_count(), 1u);
}
TEST_F(DmlBattery, SessionUnknownTableErrors) {
    catalog.add_table("", "ghost", {{"id", SqlType::make_int(), false}});
    LocalTransactionManager txn(arena);
    Session<Dialect::MySQL> session(catalog, txn);
    session.set_remote_executor(&exec);
    session.set_shard_map(&shards);
    auto rs = session.execute_query("SELECT * FROM ghost");
    EXPECT_FALSE(rs.ok);
}
TEST_F(DmlBattery, SessionCountTwice) {
    dml("INSERT INTO users (id, name, age) VALUES (1, 'a', 2)");
    dml("INSERT INTO users (id, name, age) VALUES (2, 'b', 3)");
    LocalTransactionManager txn(arena);
    Session<Dialect::MySQL> session(catalog, txn);
    session.set_remote_executor(&exec);
    session.set_shard_map(&shards);
    auto a = session.execute_query("SELECT COUNT(*) FROM users");
    auto b = session.execute_query("SELECT COUNT(*) FROM users");
    ASSERT_EQ(a.row_count(), 1u);
    ASSERT_EQ(b.row_count(), 1u);
    EXPECT_EQ(a.rows[0].get(0).to_int64(), 2);
    EXPECT_EQ(b.rows[0].get(0).to_int64(), 2);
}
TEST_F(DmlBattery, RangeInsertSelect) {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id";
    c.shards = {{"s0"}, {"s1"}, {"s2"}};
    c.strategy = RoutingStrategy::RANGE;
    c.ranges = {{5, 0}, {10, 1}, {1000, 2}};
    shards.add_table(c);
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (3, 'lo', 1)").success);
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (7, 'mid', 1)").success);
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (20, 'hi', 1)").success);
    EXPECT_EQ(exec.on("s0", "users"), 1u);
    EXPECT_EQ(exec.on("s1", "users"), 1u);
    EXPECT_EQ(exec.on("s2", "users"), 1u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 3").row_count(), 1u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 7").row_count(), 1u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 20").row_count(), 1u);
}
TEST_F(DmlBattery, ListInsertUnmappedErrors) {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id";
    c.shards = {{"s0"}, {"s1"}, {"s2"}};
    c.strategy = RoutingStrategy::LIST;
    c.list = {{true, 1, "", 0}, {true, 2, "", 1}};
    shards.add_table(c);
    EXPECT_FALSE(dml("INSERT INTO users (id, name, age) VALUES (99, 'x', 1)").success);
    EXPECT_EQ(exec.total("users"), 0u);
}
TEST_F(DmlBattery, ListInsertMapped) {
    TableShardConfig c;
    c.table_name = "users";
    c.shard_key = "id";
    c.shards = {{"s0"}, {"s1"}, {"s2"}};
    c.strategy = RoutingStrategy::LIST;
    c.list = {{true, 1, "", 0}, {true, 2, "", 1}};
    shards.add_table(c);
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (1, 'x', 1)").success);
    EXPECT_EQ(exec.on("s0", "users"), 1u);
    EXPECT_EQ(sel("SELECT name FROM users WHERE id = 1").row_count(), 1u);
}

#define HASH_ROUNDTRIP(n) \
TEST_F(DmlBattery, InsertSelect_##n) { \
    EXPECT_TRUE(dml("INSERT INTO users (id, name, age) VALUES (" #n ", 'x', 1)").success); \
    EXPECT_EQ(sel("SELECT id FROM users WHERE id = " #n).row_count(), 1u); \
}

HASH_ROUNDTRIP(20)
HASH_ROUNDTRIP(21)
HASH_ROUNDTRIP(22)
HASH_ROUNDTRIP(23)
HASH_ROUNDTRIP(24)
HASH_ROUNDTRIP(25)
HASH_ROUNDTRIP(26)
HASH_ROUNDTRIP(27)
HASH_ROUNDTRIP(28)
HASH_ROUNDTRIP(29)
HASH_ROUNDTRIP(30)
HASH_ROUNDTRIP(31)
HASH_ROUNDTRIP(32)
HASH_ROUNDTRIP(33)
HASH_ROUNDTRIP(34)
HASH_ROUNDTRIP(35)
HASH_ROUNDTRIP(36)
HASH_ROUNDTRIP(37)
HASH_ROUNDTRIP(38)
HASH_ROUNDTRIP(39)
HASH_ROUNDTRIP(40)
HASH_ROUNDTRIP(41)
HASH_ROUNDTRIP(42)
HASH_ROUNDTRIP(43)
HASH_ROUNDTRIP(44)
HASH_ROUNDTRIP(45)
HASH_ROUNDTRIP(46)
HASH_ROUNDTRIP(47)
HASH_ROUNDTRIP(48)
HASH_ROUNDTRIP(49)
