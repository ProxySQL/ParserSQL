#include <gtest/gtest.h>
#include "sql_engine/function_registry.h"
#include "sql_engine/functions/arithmetic.h"
#include "sql_parser/arena.h"
#include <cmath>
#include <climits>

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Arena;

class ArithmeticTest : public ::testing::Test {
protected:
    Arena arena{4096};

    Value call1(SqlFunction fn, Value a) {
        Value args[] = {a};
        return fn(args, 1, arena);
    }
    Value call2(SqlFunction fn, Value a, Value b) {
        Value args[] = {a, b};
        return fn(args, 2, arena);
    }
};

// --- ABS ---

TEST_F(ArithmeticTest, AbsPositive) { EXPECT_EQ(call1(fn_abs, value_int(5)).int_val, 5); }
TEST_F(ArithmeticTest, AbsNegative) { EXPECT_EQ(call1(fn_abs, value_int(-5)).int_val, 5); }
TEST_F(ArithmeticTest, AbsZero) { EXPECT_EQ(call1(fn_abs, value_int(0)).int_val, 0); }
TEST_F(ArithmeticTest, AbsDouble) { EXPECT_DOUBLE_EQ(call1(fn_abs, value_double(-3.14)).double_val, 3.14); }
TEST_F(ArithmeticTest, AbsNull) { EXPECT_TRUE(call1(fn_abs, value_null()).is_null()); }
TEST_F(ArithmeticTest, AbsUint) { EXPECT_EQ(call1(fn_abs, value_uint(42)).uint_val, 42u); }

// --- CEIL ---

TEST_F(ArithmeticTest, CeilPositive) { EXPECT_DOUBLE_EQ(call1(fn_ceil, value_double(3.2)).double_val, 4.0); }
TEST_F(ArithmeticTest, CeilNegative) { EXPECT_DOUBLE_EQ(call1(fn_ceil, value_double(-3.2)).double_val, -3.0); }
TEST_F(ArithmeticTest, CeilInt) { EXPECT_EQ(call1(fn_ceil, value_int(5)).int_val, 5); }
TEST_F(ArithmeticTest, CeilNull) { EXPECT_TRUE(call1(fn_ceil, value_null()).is_null()); }

// --- FLOOR ---

TEST_F(ArithmeticTest, FloorPositive) { EXPECT_DOUBLE_EQ(call1(fn_floor, value_double(3.7)).double_val, 3.0); }
TEST_F(ArithmeticTest, FloorNegative) { EXPECT_DOUBLE_EQ(call1(fn_floor, value_double(-3.2)).double_val, -4.0); }
TEST_F(ArithmeticTest, FloorInt) { EXPECT_EQ(call1(fn_floor, value_int(5)).int_val, 5); }
TEST_F(ArithmeticTest, FloorNull) { EXPECT_TRUE(call1(fn_floor, value_null()).is_null()); }

// --- ROUND ---

TEST_F(ArithmeticTest, RoundNoDecimals) { EXPECT_DOUBLE_EQ(call1(fn_round, value_double(3.5)).double_val, 4.0); }
TEST_F(ArithmeticTest, RoundWithDecimals) {
    EXPECT_DOUBLE_EQ(call2(fn_round, value_double(3.14159), value_int(2)).double_val, 3.14);
}
TEST_F(ArithmeticTest, RoundNegDecimals) {
    EXPECT_DOUBLE_EQ(call2(fn_round, value_double(1234.0), value_int(-2)).double_val, 1200.0);
}
TEST_F(ArithmeticTest, RoundNull) { EXPECT_TRUE(call1(fn_round, value_null()).is_null()); }

// --- TRUNCATE ---

TEST_F(ArithmeticTest, TruncatePositive) {
    EXPECT_DOUBLE_EQ(call2(fn_truncate, value_double(3.789), value_int(2)).double_val, 3.78);
}
TEST_F(ArithmeticTest, TruncateNegative) {
    EXPECT_DOUBLE_EQ(call2(fn_truncate, value_double(-3.789), value_int(2)).double_val, -3.78);
}
TEST_F(ArithmeticTest, TruncateNull) {
    EXPECT_TRUE(call2(fn_truncate, value_null(), value_int(2)).is_null());
    EXPECT_TRUE(call2(fn_truncate, value_double(3.14), value_null()).is_null());
}

// --- MOD ---

TEST_F(ArithmeticTest, ModIntegers) { EXPECT_EQ(call2(fn_mod, value_int(10), value_int(3)).int_val, 1); }
TEST_F(ArithmeticTest, ModNegative) { EXPECT_EQ(call2(fn_mod, value_int(-10), value_int(3)).int_val, -1); }
TEST_F(ArithmeticTest, ModDouble) {
    EXPECT_NEAR(call2(fn_mod, value_double(10.5), value_double(3.0)).double_val, 1.5, 1e-9);
}
TEST_F(ArithmeticTest, ModByZero) { EXPECT_TRUE(call2(fn_mod, value_int(10), value_int(0)).is_null()); }
TEST_F(ArithmeticTest, ModNull) { EXPECT_TRUE(call2(fn_mod, value_null(), value_int(3)).is_null()); }

// --- POWER ---

TEST_F(ArithmeticTest, PowerBasic) { EXPECT_DOUBLE_EQ(call2(fn_power, value_int(2), value_int(3)).double_val, 8.0); }
TEST_F(ArithmeticTest, PowerFractional) { EXPECT_NEAR(call2(fn_power, value_double(4.0), value_double(0.5)).double_val, 2.0, 1e-9); }
TEST_F(ArithmeticTest, PowerZero) { EXPECT_DOUBLE_EQ(call2(fn_power, value_int(5), value_int(0)).double_val, 1.0); }
TEST_F(ArithmeticTest, PowerNull) { EXPECT_TRUE(call2(fn_power, value_null(), value_int(2)).is_null()); }

// --- SQRT ---

TEST_F(ArithmeticTest, SqrtPositive) { EXPECT_DOUBLE_EQ(call1(fn_sqrt, value_double(4.0)).double_val, 2.0); }
TEST_F(ArithmeticTest, SqrtZero) { EXPECT_DOUBLE_EQ(call1(fn_sqrt, value_double(0.0)).double_val, 0.0); }
TEST_F(ArithmeticTest, SqrtNegative) { EXPECT_TRUE(call1(fn_sqrt, value_double(-1.0)).is_null()); }
TEST_F(ArithmeticTest, SqrtNull) { EXPECT_TRUE(call1(fn_sqrt, value_null()).is_null()); }

// --- SIGN ---

TEST_F(ArithmeticTest, SignPositive) { EXPECT_EQ(call1(fn_sign, value_int(42)).int_val, 1); }
TEST_F(ArithmeticTest, SignNegative) { EXPECT_EQ(call1(fn_sign, value_int(-42)).int_val, -1); }
TEST_F(ArithmeticTest, SignZero) { EXPECT_EQ(call1(fn_sign, value_int(0)).int_val, 0); }
TEST_F(ArithmeticTest, SignDouble) { EXPECT_EQ(call1(fn_sign, value_double(-0.5)).int_val, -1); }
TEST_F(ArithmeticTest, SignNull) { EXPECT_TRUE(call1(fn_sign, value_null()).is_null()); }
