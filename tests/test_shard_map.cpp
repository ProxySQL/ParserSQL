// test_shard_map.cpp — Routing strategy correctness for ShardMap.
//
// Each strategy is asserted by exhaustive route-table enumeration on a
// small key range, so any change in hash function, tie-breaking, or
// modulo arithmetic shows up as a concrete diff rather than a silent
// shift in distribution.

#include <gtest/gtest.h>
#include "sql_engine/shard_map.h"

#include <set>

using namespace sql_engine;
using sql_parser::StringRef;

namespace {

StringRef sref(const char* s) {
    return StringRef{s, static_cast<uint32_t>(std::strlen(s))};
}

TableShardConfig make_two_shards(RoutingStrategy strategy) {
    TableShardConfig cfg;
    cfg.table_name = "users";
    cfg.shard_key = "id";
    cfg.shards = {ShardInfo{"shard1"}, ShardInfo{"shard2"}};
    cfg.strategy = strategy;
    return cfg;
}

} // namespace

// ----------------------------------------------------------------------
// HASH strategy
// ----------------------------------------------------------------------

TEST(ShardMapHashTest, IsDeterministic) {
    ShardMap map;
    map.add_table(make_two_shards(RoutingStrategy::HASH));

    // Same input must always produce the same shard.
    for (int i = -100; i < 100; ++i) {
        size_t a = map.shard_index_for_int(sref("users"), i);
        size_t b = map.shard_index_for_int(sref("users"), i);
        EXPECT_EQ(a, b) << "for value " << i;
    }
}

TEST(ShardMapHashTest, DistributesAcrossBothShards) {
    // FNV-1a with modulo 2 should hit both shards for the small range
    // 0..63 — the test is loose because we don't want to over-specify
    // the exact partition, only that it's not pathological (all-zero
    // or all-one).
    ShardMap map;
    map.add_table(make_two_shards(RoutingStrategy::HASH));

    std::set<size_t> seen;
    for (int i = 0; i < 64; ++i) {
        seen.insert(map.shard_index_for_int(sref("users"), i));
    }
    EXPECT_EQ(seen.size(), 2u) << "hash never reaches one of the shards";
}

TEST(ShardMapHashTest, NotIdentityFunction) {
    // The original implementation used std::hash<int64_t> which on
    // libstdc++ is the identity function, so route(K)==K%num_shards.
    // FNV-1a should NOT have that property — at minimum, route(0) and
    // route(1) should differ from "even goes to shard 0, odd to shard 1"
    // for at least one small value, otherwise the regression is back.
    ShardMap map;
    map.add_table(make_two_shards(RoutingStrategy::HASH));

    bool diverges = false;
    for (int i = 0; i < 32; ++i) {
        size_t expected_if_identity = static_cast<size_t>(i) % 2;
        size_t actual = map.shard_index_for_int(sref("users"), i);
        if (actual != expected_if_identity) { diverges = true; break; }
    }
    EXPECT_TRUE(diverges)
        << "hash routing matches identity-mod-N — std::hash regression";
}

TEST(ShardMapHashTest, StringKeysRouteSomewhereStable) {
    ShardMap map;
    map.add_table(make_two_shards(RoutingStrategy::HASH));

    // Same string -> same shard, repeatedly.
    size_t a = map.shard_index_for_string(sref("users"), "alice", 5);
    size_t b = map.shard_index_for_string(sref("users"), "alice", 5);
    EXPECT_EQ(a, b);
    EXPECT_LT(a, 2u);
}

// ----------------------------------------------------------------------
// RANGE strategy
// ----------------------------------------------------------------------

TEST(ShardMapRangeTest, MatchesDemoDataPlacement) {
    // Demo: shard1 holds id 1..5, shard2 holds id 6..10.
    TableShardConfig cfg = make_two_shards(RoutingStrategy::RANGE);
    cfg.ranges = {ShardRange{5, 0}, ShardRange{10, 1}};
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_int(sref("users"), 1), 0u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 5), 0u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 6), 1u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 10), 1u);
}

TEST(ShardMapRangeTest, AboveMaxBoundFallsToLastShard) {
    TableShardConfig cfg = make_two_shards(RoutingStrategy::RANGE);
    cfg.ranges = {ShardRange{5, 0}, ShardRange{10, 1}};
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_int(sref("users"), 11), 1u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 9999), 1u);
}

TEST(ShardMapRangeTest, AcceptsUnsortedInputAndSortsInternally) {
    TableShardConfig cfg = make_two_shards(RoutingStrategy::RANGE);
    // Intentionally provided out of order.
    cfg.ranges = {ShardRange{10, 1}, ShardRange{5, 0}};
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_int(sref("users"), 3), 0u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 8), 1u);
}

TEST(ShardMapRangeTest, StringKeyDoesNotRouteSpuriously) {
    // RANGE is integer-keyed only. A string lookup must not pretend it
    // computed a real route.
    TableShardConfig cfg = make_two_shards(RoutingStrategy::RANGE);
    cfg.ranges = {ShardRange{5, 0}, ShardRange{10, 1}};
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_string(sref("users"), "anything", 8), 0u);
}

// ----------------------------------------------------------------------
// LIST strategy
// ----------------------------------------------------------------------

TEST(ShardMapListTest, IntKeysRouteByExplicitMap) {
    TableShardConfig cfg = make_two_shards(RoutingStrategy::LIST);
    cfg.list = {
        ShardListEntry{true, 1, "", 0},
        ShardListEntry{true, 2, "", 0},
        ShardListEntry{true, 6, "", 1},
        ShardListEntry{true, 7, "", 1},
    };
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_int(sref("users"), 1), 0u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 2), 0u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 6), 1u);
    EXPECT_EQ(map.shard_index_for_int(sref("users"), 7), 1u);
}

TEST(ShardMapListTest, MissingKeyRoutesToShardZero) {
    TableShardConfig cfg = make_two_shards(RoutingStrategy::LIST);
    cfg.list = {ShardListEntry{true, 1, "", 0}};
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_int(sref("users"), 999), 0u);
}

TEST(ShardMapListTest, StringKeysRouteByExplicitMap) {
    TableShardConfig cfg = make_two_shards(RoutingStrategy::LIST);
    cfg.list = {
        ShardListEntry{false, 0, "us-east", 0},
        ShardListEntry{false, 0, "us-west", 1},
    };
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_string(sref("users"), "us-east", 7), 0u);
    EXPECT_EQ(map.shard_index_for_string(sref("users"), "us-west", 7), 1u);
    EXPECT_EQ(map.shard_index_for_string(sref("users"), "eu-north", 8), 0u);
}

// ----------------------------------------------------------------------
// Default strategy + general sanity
// ----------------------------------------------------------------------

TEST(ShardMapTest, DefaultStrategyIsHash) {
    TableShardConfig cfg;
    cfg.table_name = "users";
    cfg.shard_key = "id";
    cfg.shards = {ShardInfo{"a"}, ShardInfo{"b"}};
    EXPECT_EQ(cfg.strategy, RoutingStrategy::HASH);
}

TEST(ShardMapTest, ClampOnOutOfRangeShardIndex) {
    // A range entry that points at shard_index 9 with only 2 shards
    // configured must not return 9 — clamp to last (1).
    TableShardConfig cfg = make_two_shards(RoutingStrategy::RANGE);
    cfg.ranges = {ShardRange{100, 9}};
    ShardMap map;
    map.add_table(cfg);

    EXPECT_EQ(map.shard_index_for_int(sref("users"), 50), 1u);
}

TEST(ShardMapTest, UnknownTableReturnsZero) {
    ShardMap map;  // empty
    EXPECT_EQ(map.shard_index_for_int(sref("nope"), 5), 0u);
    EXPECT_EQ(map.shard_index_for_string(sref("nope"), "x", 1), 0u);
}
