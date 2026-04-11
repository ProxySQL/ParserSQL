#include <gtest/gtest.h>
#include "sql_engine/types.h"
#include "sql_engine/value.h"

using namespace sql_engine;
using sql_parser::StringRef;

// --- SqlType tests ---

TEST(SqlTypeTest, MakeInt) {
    auto t = SqlType::make_int();
    EXPECT_EQ(t.kind, SqlType::INT);
    EXPECT_FALSE(t.is_unsigned);
    EXPECT_TRUE(t.is_numeric());
    EXPECT_FALSE(t.is_string());
    EXPECT_FALSE(t.is_temporal());
}

TEST(SqlTypeTest, MakeUnsignedBigint) {
    auto t = SqlType::make_bigint(true);
    EXPECT_EQ(t.kind, SqlType::BIGINT);
    EXPECT_TRUE(t.is_unsigned);
    EXPECT_TRUE(t.is_numeric());
}

TEST(SqlTypeTest, MakeVarchar) {
    auto t = SqlType::make_varchar(255);
    EXPECT_EQ(t.kind, SqlType::VARCHAR);
    EXPECT_EQ(t.precision, 255);
    EXPECT_TRUE(t.is_string());
    EXPECT_FALSE(t.is_numeric());
}

TEST(SqlTypeTest, MakeDecimal) {
    auto t = SqlType::make_decimal(10, 2);
    EXPECT_EQ(t.kind, SqlType::DECIMAL);
    EXPECT_EQ(t.precision, 10);
    EXPECT_EQ(t.scale, 2);
    EXPECT_TRUE(t.is_numeric());
}

TEST(SqlTypeTest, MakeDate) {
    auto t = SqlType::make_date();
    EXPECT_EQ(t.kind, SqlType::DATE);
    EXPECT_TRUE(t.is_temporal());
    EXPECT_FALSE(t.is_numeric());
}

TEST(SqlTypeTest, MakeTimestampWithTZ) {
    auto t = SqlType::make_timestamp(true);
    EXPECT_EQ(t.kind, SqlType::TIMESTAMP);
    EXPECT_TRUE(t.has_timezone);
    EXPECT_TRUE(t.is_temporal());
}

TEST(SqlTypeTest, Equality) {
    EXPECT_EQ(SqlType::make_int(), SqlType::make_int());
    EXPECT_NE(SqlType::make_int(), SqlType::make_bigint());
    EXPECT_NE(SqlType::make_int(), SqlType::make_int(true));
    EXPECT_EQ(SqlType::make_decimal(10, 2), SqlType::make_decimal(10, 2));
    EXPECT_NE(SqlType::make_decimal(10, 2), SqlType::make_decimal(10, 3));
}

TEST(SqlTypeTest, Categories) {
    // Numeric category
    EXPECT_TRUE(SqlType::make_bool().is_numeric());
    EXPECT_TRUE(SqlType::make_tinyint().is_numeric());
    EXPECT_TRUE(SqlType::make_float().is_numeric());
    EXPECT_TRUE(SqlType::make_double().is_numeric());
    EXPECT_TRUE(SqlType::make_decimal(10, 2).is_numeric());

    // String category
    EXPECT_TRUE(SqlType::make_char(10).is_string());
    EXPECT_TRUE(SqlType::make_varchar(255).is_string());
    EXPECT_TRUE(SqlType::make_text().is_string());
    EXPECT_TRUE(SqlType::make_blob().is_string());

    // Temporal category
    EXPECT_TRUE(SqlType::make_date().is_temporal());
    EXPECT_TRUE(SqlType::make_time().is_temporal());
    EXPECT_TRUE(SqlType::make_datetime().is_temporal());
    EXPECT_TRUE(SqlType::make_timestamp().is_temporal());

    // Structured
    EXPECT_TRUE(SqlType::make_json().is_structured());
}

// --- Value constructor + tag round-trip tests ---

TEST(ValueTest, Null) {
    auto v = value_null();
    EXPECT_EQ(v.tag, Value::TAG_NULL);
    EXPECT_TRUE(v.is_null());
    EXPECT_FALSE(v.is_numeric());
    EXPECT_FALSE(v.is_string());
    EXPECT_FALSE(v.is_temporal());
}

TEST(ValueTest, Bool) {
    auto t = value_bool(true);
    auto f = value_bool(false);
    EXPECT_EQ(t.tag, Value::TAG_BOOL);
    EXPECT_TRUE(t.bool_val);
    EXPECT_FALSE(t.is_null());
    EXPECT_TRUE(t.is_numeric());
    EXPECT_EQ(f.tag, Value::TAG_BOOL);
    EXPECT_FALSE(f.bool_val);
}

TEST(ValueTest, Int64) {
    auto v = value_int(42);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
    EXPECT_TRUE(v.is_numeric());
    EXPECT_FALSE(v.is_null());

    // Negative
    auto neg = value_int(-100);
    EXPECT_EQ(neg.int_val, -100);

    // Limits
    auto mx = value_int(INT64_MAX);
    EXPECT_EQ(mx.int_val, INT64_MAX);
    auto mn = value_int(INT64_MIN);
    EXPECT_EQ(mn.int_val, INT64_MIN);
}

TEST(ValueTest, Uint64) {
    auto v = value_uint(18446744073709551615ULL);
    EXPECT_EQ(v.tag, Value::TAG_UINT64);
    EXPECT_EQ(v.uint_val, UINT64_MAX);
    EXPECT_TRUE(v.is_numeric());
}

TEST(ValueTest, Double) {
    auto v = value_double(3.14);
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.14);
    EXPECT_TRUE(v.is_numeric());

    // Special values
    auto zero = value_double(0.0);
    EXPECT_DOUBLE_EQ(zero.double_val, 0.0);
    auto neg = value_double(-1.5);
    EXPECT_DOUBLE_EQ(neg.double_val, -1.5);
}

TEST(ValueTest, Decimal) {
    const char* dec = "123.45";
    auto v = value_decimal(StringRef{dec, 6});
    EXPECT_EQ(v.tag, Value::TAG_DECIMAL);
    EXPECT_EQ(v.str_val.len, 6u);
    EXPECT_TRUE(v.is_numeric());
}

TEST(ValueTest, String) {
    const char* s = "hello";
    auto v = value_string(StringRef{s, 5});
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(v.str_val.len, 5u);
    EXPECT_TRUE(v.is_string());
    EXPECT_FALSE(v.is_numeric());

    // Empty string
    auto empty = value_string(StringRef{nullptr, 0});
    EXPECT_EQ(empty.tag, Value::TAG_STRING);
    EXPECT_TRUE(empty.str_val.empty());
    EXPECT_FALSE(empty.is_null());
}

TEST(ValueTest, Bytes) {
    const char* b = "\x00\x01\x02";
    auto v = value_bytes(StringRef{b, 3});
    EXPECT_EQ(v.tag, Value::TAG_BYTES);
    EXPECT_EQ(v.str_val.len, 3u);
}

TEST(ValueTest, Date) {
    // 2024-01-15 = 19737 days since epoch
    auto v = value_date(19737);
    EXPECT_EQ(v.tag, Value::TAG_DATE);
    EXPECT_EQ(v.date_val, 19737);
    EXPECT_TRUE(v.is_temporal());
    EXPECT_FALSE(v.is_numeric());

    // Epoch
    auto epoch = value_date(0);
    EXPECT_EQ(epoch.date_val, 0);

    // Pre-epoch
    auto pre = value_date(-1);
    EXPECT_EQ(pre.date_val, -1);
}

TEST(ValueTest, Time) {
    // 12:30:45 = (12*3600 + 30*60 + 45) * 1000000 us
    int64_t us = (12LL * 3600 + 30 * 60 + 45) * 1000000LL;
    auto v = value_time(us);
    EXPECT_EQ(v.tag, Value::TAG_TIME);
    EXPECT_EQ(v.time_val, us);
    EXPECT_TRUE(v.is_temporal());
}

TEST(ValueTest, Datetime) {
    auto v = value_datetime(1705276800000000LL);
    EXPECT_EQ(v.tag, Value::TAG_DATETIME);
    EXPECT_EQ(v.datetime_val, 1705276800000000LL);
    EXPECT_TRUE(v.is_temporal());
}

TEST(ValueTest, Timestamp) {
    auto v = value_timestamp(1705276800000000LL);
    EXPECT_EQ(v.tag, Value::TAG_TIMESTAMP);
    EXPECT_EQ(v.timestamp_val, 1705276800000000LL);
    EXPECT_TRUE(v.is_temporal());
}

// TEST(ValueTest, Interval) removed along with the TAG_INTERVAL type.
// INTERVAL had no producer anywhere in the engine; see comment in
// include/sql_engine/value.h for the reasoning. Re-add this test when
// INTERVAL is re-introduced with an actual producer (e.g. PostgreSQL
// INTERVAL OID parsing).

TEST(ValueTest, Json) {
    const char* j = R"({"key": "value"})";
    auto v = value_json(StringRef{j, static_cast<uint32_t>(strlen(j))});
    EXPECT_EQ(v.tag, Value::TAG_JSON);
    EXPECT_FALSE(v.is_null());
}

TEST(ValueTest, ToDouble) {
    EXPECT_DOUBLE_EQ(value_bool(true).to_double(), 1.0);
    EXPECT_DOUBLE_EQ(value_bool(false).to_double(), 0.0);
    EXPECT_DOUBLE_EQ(value_int(42).to_double(), 42.0);
    EXPECT_DOUBLE_EQ(value_uint(100).to_double(), 100.0);
    EXPECT_DOUBLE_EQ(value_double(3.14).to_double(), 3.14);
    EXPECT_DOUBLE_EQ(value_null().to_double(), 0.0);
}

TEST(ValueTest, ToInt64) {
    EXPECT_EQ(value_bool(true).to_int64(), 1);
    EXPECT_EQ(value_bool(false).to_int64(), 0);
    EXPECT_EQ(value_int(42).to_int64(), 42);
    EXPECT_EQ(value_uint(100).to_int64(), 100);
    EXPECT_EQ(value_double(3.7).to_int64(), 3);
    EXPECT_EQ(value_null().to_int64(), 0);
}
