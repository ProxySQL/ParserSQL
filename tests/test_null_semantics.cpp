#include <gtest/gtest.h>
#include "sql_engine/null_semantics.h"

using namespace sql_engine;
using namespace sql_engine::null_semantics;

// --- propagate_null ---

TEST(NullSemanticsTest, PropagateNullBothNonNull) {
    Value result = value_null();
    EXPECT_FALSE(propagate_null(value_int(1), value_int(2), result));
}

TEST(NullSemanticsTest, PropagateNullLeftNull) {
    Value result = value_null();
    EXPECT_TRUE(propagate_null(value_null(), value_int(2), result));
    EXPECT_TRUE(result.is_null());
}

TEST(NullSemanticsTest, PropagateNullRightNull) {
    Value result = value_null();
    EXPECT_TRUE(propagate_null(value_int(1), value_null(), result));
    EXPECT_TRUE(result.is_null());
}

TEST(NullSemanticsTest, PropagateNullBothNull) {
    Value result = value_null();
    EXPECT_TRUE(propagate_null(value_null(), value_null(), result));
    EXPECT_TRUE(result.is_null());
}

// --- AND truth table (exhaustive 3x3) ---

struct ThreeValuedCase {
    Value left;
    Value right;
    bool expect_null;
    bool expect_val;  // only meaningful if expect_null is false
};

class AndTruthTable : public ::testing::TestWithParam<ThreeValuedCase> {};

TEST_P(AndTruthTable, Check) {
    auto [left, right, expect_null, expect_val] = GetParam();
    Value result = eval_and(left, right);
    if (expect_null) {
        EXPECT_TRUE(result.is_null());
    } else {
        EXPECT_FALSE(result.is_null());
        EXPECT_EQ(result.bool_val, expect_val);
    }
}

INSTANTIATE_TEST_SUITE_P(NullSemantics, AndTruthTable, ::testing::Values(
    // TRUE AND TRUE = TRUE
    ThreeValuedCase{value_bool(true),  value_bool(true),  false, true},
    // TRUE AND FALSE = FALSE
    ThreeValuedCase{value_bool(true),  value_bool(false), false, false},
    // TRUE AND NULL = NULL
    ThreeValuedCase{value_bool(true),  value_null(),      true,  false},
    // FALSE AND TRUE = FALSE
    ThreeValuedCase{value_bool(false), value_bool(true),  false, false},
    // FALSE AND FALSE = FALSE
    ThreeValuedCase{value_bool(false), value_bool(false), false, false},
    // FALSE AND NULL = FALSE
    ThreeValuedCase{value_bool(false), value_null(),      false, false},
    // NULL AND TRUE = NULL
    ThreeValuedCase{value_null(),      value_bool(true),  true,  false},
    // NULL AND FALSE = FALSE
    ThreeValuedCase{value_null(),      value_bool(false), false, false},
    // NULL AND NULL = NULL
    ThreeValuedCase{value_null(),      value_null(),      true,  false}
));

// --- OR truth table (exhaustive 3x3) ---

class OrTruthTable : public ::testing::TestWithParam<ThreeValuedCase> {};

TEST_P(OrTruthTable, Check) {
    auto [left, right, expect_null, expect_val] = GetParam();
    Value result = eval_or(left, right);
    if (expect_null) {
        EXPECT_TRUE(result.is_null());
    } else {
        EXPECT_FALSE(result.is_null());
        EXPECT_EQ(result.bool_val, expect_val);
    }
}

INSTANTIATE_TEST_SUITE_P(NullSemantics, OrTruthTable, ::testing::Values(
    // TRUE OR TRUE = TRUE
    ThreeValuedCase{value_bool(true),  value_bool(true),  false, true},
    // TRUE OR FALSE = TRUE
    ThreeValuedCase{value_bool(true),  value_bool(false), false, true},
    // TRUE OR NULL = TRUE
    ThreeValuedCase{value_bool(true),  value_null(),      false, true},
    // FALSE OR TRUE = TRUE
    ThreeValuedCase{value_bool(false), value_bool(true),  false, true},
    // FALSE OR FALSE = FALSE
    ThreeValuedCase{value_bool(false), value_bool(false), false, false},
    // FALSE OR NULL = NULL
    ThreeValuedCase{value_bool(false), value_null(),      true,  false},
    // NULL OR TRUE = TRUE
    ThreeValuedCase{value_null(),      value_bool(true),  false, true},
    // NULL OR FALSE = NULL
    ThreeValuedCase{value_null(),      value_bool(false), true,  false},
    // NULL OR NULL = NULL
    ThreeValuedCase{value_null(),      value_null(),      true,  false}
));

// --- NOT ---

TEST(NullSemanticsTest, NotTrue) {
    auto r = eval_not(value_bool(true));
    EXPECT_FALSE(r.is_null());
    EXPECT_FALSE(r.bool_val);
}

TEST(NullSemanticsTest, NotFalse) {
    auto r = eval_not(value_bool(false));
    EXPECT_FALSE(r.is_null());
    EXPECT_TRUE(r.bool_val);
}

TEST(NullSemanticsTest, NotNull) {
    auto r = eval_not(value_null());
    EXPECT_TRUE(r.is_null());
}
