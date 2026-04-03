#include <gtest/gtest.h>
#include "sql_engine/functions/comparison.h"
#include "sql_parser/arena.h"

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Arena;
using sql_parser::StringRef;

class ComparisonTest : public ::testing::Test {
protected:
    Arena arena{4096};
};

// --- COALESCE ---

TEST_F(ComparisonTest, CoalesceFirstNonNull) {
    Value args[] = {value_null(), value_int(42), value_int(99)};
    auto r = fn_coalesce(args, 3, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(ComparisonTest, CoalesceFirstArg) {
    Value args[] = {value_int(1), value_int(2)};
    auto r = fn_coalesce(args, 2, arena);
    EXPECT_EQ(r.int_val, 1);
}

TEST_F(ComparisonTest, CoalesceAllNull) {
    Value args[] = {value_null(), value_null(), value_null()};
    auto r = fn_coalesce(args, 3, arena);
    EXPECT_TRUE(r.is_null());
}

TEST_F(ComparisonTest, CoalesceSingleNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_coalesce(args, 1, arena).is_null());
}

TEST_F(ComparisonTest, CoalesceSingleNonNull) {
    Value args[] = {value_int(5)};
    EXPECT_EQ(fn_coalesce(args, 1, arena).int_val, 5);
}

// --- NULLIF ---

TEST_F(ComparisonTest, NullifEqual) {
    Value args[] = {value_int(42), value_int(42)};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, NullifNotEqual) {
    Value args[] = {value_int(42), value_int(99)};
    auto r = fn_nullif(args, 2, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(ComparisonTest, NullifFirstNull) {
    Value args[] = {value_null(), value_int(42)};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, NullifSecondNull) {
    Value args[] = {value_int(42), value_null()};
    EXPECT_EQ(fn_nullif(args, 2, arena).int_val, 42);
}

TEST_F(ComparisonTest, NullifBothNull) {
    Value args[] = {value_null(), value_null()};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, NullifDifferentTypes) {
    Value args[] = {value_int(42), value_double(42.0)};
    // Different tags -> not equal -> return first arg
    auto r = fn_nullif(args, 2, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(ComparisonTest, NullifStrings) {
    const char* s = "hello";
    Value args[] = {value_string(StringRef{s, 5}), value_string(StringRef{s, 5})};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

// --- IFNULL ---

TEST_F(ComparisonTest, IfnullFirstNull) {
    Value args[] = {value_null(), value_int(42)};
    EXPECT_EQ(fn_ifnull(args, 2, arena).int_val, 42);
}

TEST_F(ComparisonTest, IfnullFirstNonNull) {
    Value args[] = {value_int(10), value_int(42)};
    EXPECT_EQ(fn_ifnull(args, 2, arena).int_val, 10);
}

// --- IF ---

TEST_F(ComparisonTest, IfTrue) {
    Value args[] = {value_bool(true), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 1);
}

TEST_F(ComparisonTest, IfFalse) {
    Value args[] = {value_bool(false), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 2);
}

TEST_F(ComparisonTest, IfNull) {
    Value args[] = {value_null(), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 2);
}

TEST_F(ComparisonTest, IfIntTruthy) {
    Value args[] = {value_int(42), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 1);
}

TEST_F(ComparisonTest, IfIntZero) {
    Value args[] = {value_int(0), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 2);
}

// --- LEAST ---

TEST_F(ComparisonTest, LeastBasic) {
    Value args[] = {value_int(3), value_int(1), value_int(2)};
    EXPECT_EQ(fn_least(args, 3, arena).int_val, 1);
}

TEST_F(ComparisonTest, LeastWithNull) {
    Value args[] = {value_null(), value_int(5), value_int(3)};
    EXPECT_EQ(fn_least(args, 3, arena).int_val, 3);
}

TEST_F(ComparisonTest, LeastAllNull) {
    Value args[] = {value_null(), value_null()};
    EXPECT_TRUE(fn_least(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, LeastNegative) {
    Value args[] = {value_int(-5), value_int(3), value_int(-10)};
    EXPECT_EQ(fn_least(args, 3, arena).int_val, -10);
}

TEST_F(ComparisonTest, LeastDoubles) {
    Value args[] = {value_double(3.14), value_double(2.71), value_double(1.41)};
    EXPECT_DOUBLE_EQ(fn_least(args, 3, arena).double_val, 1.41);
}

// --- GREATEST ---

TEST_F(ComparisonTest, GreatestBasic) {
    Value args[] = {value_int(3), value_int(1), value_int(2)};
    EXPECT_EQ(fn_greatest(args, 3, arena).int_val, 3);
}

TEST_F(ComparisonTest, GreatestWithNull) {
    Value args[] = {value_null(), value_int(5), value_int(3)};
    EXPECT_EQ(fn_greatest(args, 3, arena).int_val, 5);
}

TEST_F(ComparisonTest, GreatestAllNull) {
    Value args[] = {value_null(), value_null()};
    EXPECT_TRUE(fn_greatest(args, 2, arena).is_null());
}
