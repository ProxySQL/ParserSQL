#include <gtest/gtest.h>
#include "sql_engine/coercion.h"
#include "sql_engine/datetime_parse.h"
#include "sql_parser/arena.h"

using namespace sql_engine;
using sql_parser::Dialect;
using sql_parser::Arena;
using sql_parser::StringRef;

// ===== MySQL coercion =====

class MySQLCoercion : public ::testing::Test {
protected:
    Arena arena{4096};
};

TEST_F(MySQLCoercion, CanCoerceSameType) {
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::INT, SqlType::INT));
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::VARCHAR, SqlType::VARCHAR));
}

TEST_F(MySQLCoercion, CanCoerceNumericWithinCategory) {
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::TINYINT, SqlType::BIGINT));
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::INT, SqlType::DOUBLE));
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::FLOAT, SqlType::DECIMAL));
}

TEST_F(MySQLCoercion, CanCoerceStringToNumeric) {
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::VARCHAR, SqlType::INT));
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::VARCHAR, SqlType::DOUBLE));
}

TEST_F(MySQLCoercion, CanCoerceNumericToString) {
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::INT, SqlType::VARCHAR));
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::DOUBLE, SqlType::VARCHAR));
}

TEST_F(MySQLCoercion, CanCoerceStringToDate) {
    EXPECT_TRUE(CoercionRules<Dialect::MySQL>::can_coerce(SqlType::VARCHAR, SqlType::DATE));
}

TEST_F(MySQLCoercion, CommonTypeNumericPromotion) {
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::common_type(SqlType::INT, SqlType::BIGINT), SqlType::BIGINT);
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::common_type(SqlType::FLOAT, SqlType::DOUBLE), SqlType::DOUBLE);
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::common_type(SqlType::INT, SqlType::DOUBLE), SqlType::DOUBLE);
}

TEST_F(MySQLCoercion, CommonTypeStringAndNumeric) {
    // MySQL: string+numeric -> DOUBLE
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::common_type(SqlType::VARCHAR, SqlType::INT), SqlType::DOUBLE);
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::common_type(SqlType::INT, SqlType::VARCHAR), SqlType::DOUBLE);
}

TEST_F(MySQLCoercion, CoerceStringToInt) {
    const char* s = "42";
    auto v = value_string(StringRef{s, 2});
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_INT64, arena);
    EXPECT_EQ(r.tag, Value::TAG_INT64);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(MySQLCoercion, CoerceStringToIntLenient) {
    // MySQL: "42abc" -> 42 (truncates at first non-digit)
    const char* s = "42abc";
    auto v = value_string(StringRef{s, 5});
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_INT64, arena);
    EXPECT_EQ(r.tag, Value::TAG_INT64);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(MySQLCoercion, CoerceIntToString) {
    auto v = value_int(42);
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_STRING, arena);
    EXPECT_EQ(r.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(r.str_val.ptr, r.str_val.len), "42");
}

TEST_F(MySQLCoercion, CoerceIntToDouble) {
    auto v = value_int(42);
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_DOUBLE, arena);
    EXPECT_EQ(r.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(r.double_val, 42.0);
}

TEST_F(MySQLCoercion, CoerceNullPassthrough) {
    auto v = value_null();
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_INT64, arena);
    EXPECT_TRUE(r.is_null());
}

TEST_F(MySQLCoercion, CoerceSameTagNoop) {
    auto v = value_int(42);
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_INT64, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(MySQLCoercion, CoerceBoolToInt) {
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::coerce_value(value_bool(true), Value::TAG_INT64, arena).int_val, 1);
    EXPECT_EQ(CoercionRules<Dialect::MySQL>::coerce_value(value_bool(false), Value::TAG_INT64, arena).int_val, 0);
}

TEST_F(MySQLCoercion, CoerceDoubleToString) {
    auto v = value_double(3.14);
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_STRING, arena);
    EXPECT_EQ(r.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(r.str_val.ptr, r.str_val.len), "3.14");
}

// ===== PostgreSQL coercion =====

class PgSQLCoercion : public ::testing::Test {
protected:
    Arena arena{4096};
};

TEST_F(PgSQLCoercion, CanCoerceWithinNumericWidening) {
    // INT -> BIGINT: widening, allowed
    EXPECT_TRUE(CoercionRules<Dialect::PostgreSQL>::can_coerce(SqlType::INT, SqlType::BIGINT));
    EXPECT_TRUE(CoercionRules<Dialect::PostgreSQL>::can_coerce(SqlType::FLOAT, SqlType::DOUBLE));
}

TEST_F(PgSQLCoercion, CannotCoerceNumericNarrowing) {
    // BIGINT -> INT: narrowing, not allowed
    EXPECT_FALSE(CoercionRules<Dialect::PostgreSQL>::can_coerce(SqlType::BIGINT, SqlType::INT));
}

TEST_F(PgSQLCoercion, CannotCoerceStringToInt) {
    // PostgreSQL: implicit string->int NOT allowed
    EXPECT_FALSE(CoercionRules<Dialect::PostgreSQL>::can_coerce(SqlType::VARCHAR, SqlType::INT));
}

TEST_F(PgSQLCoercion, CannotCoerceIntToString) {
    EXPECT_FALSE(CoercionRules<Dialect::PostgreSQL>::can_coerce(SqlType::INT, SqlType::VARCHAR));
}

TEST_F(PgSQLCoercion, CommonTypeCrossCategoryReturnsUnknown) {
    EXPECT_EQ(CoercionRules<Dialect::PostgreSQL>::common_type(SqlType::VARCHAR, SqlType::INT), SqlType::UNKNOWN);
}

TEST_F(PgSQLCoercion, CoerceIntToDouble) {
    auto v = value_int(42);
    auto r = CoercionRules<Dialect::PostgreSQL>::coerce_value(v, Value::TAG_DOUBLE, arena);
    EXPECT_EQ(r.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(r.double_val, 42.0);
}

TEST_F(PgSQLCoercion, CoerceStringToIntFails) {
    const char* s = "42";
    auto v = value_string(StringRef{s, 2});
    auto r = CoercionRules<Dialect::PostgreSQL>::coerce_value(v, Value::TAG_INT64, arena);
    EXPECT_TRUE(r.is_null());  // strict: returns NULL (error)
}

TEST_F(PgSQLCoercion, CoerceIntToBoolFails) {
    auto v = value_int(1);
    auto r = CoercionRules<Dialect::PostgreSQL>::coerce_value(v, Value::TAG_BOOL, arena);
    EXPECT_TRUE(r.is_null());  // PostgreSQL: no implicit int->bool
}

// ----- String -> temporal coercion (MySQL lenient) -----

TEST_F(MySQLCoercion, CoerceStringToDateValue) {
    const char* s = "2026-04-10";
    auto v = value_string(StringRef{s, 10});
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_DATE, arena);
    EXPECT_EQ(r.tag, Value::TAG_DATE);
    EXPECT_EQ(r.date_val, datetime_parse::parse_date("2026-04-10"));
}

TEST_F(MySQLCoercion, CoerceStringToDatetimeValue) {
    const char* s = "2026-04-10 13:45:30";
    auto v = value_string(StringRef{s, 19});
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_DATETIME, arena);
    EXPECT_EQ(r.tag, Value::TAG_DATETIME);
    EXPECT_EQ(r.datetime_val, datetime_parse::parse_datetime("2026-04-10 13:45:30"));
}

TEST_F(MySQLCoercion, CoerceStringToTime) {
    const char* s = "13:45:30";
    auto v = value_string(StringRef{s, 8});
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_TIME, arena);
    EXPECT_EQ(r.tag, Value::TAG_TIME);
    EXPECT_EQ(r.time_val, datetime_parse::parse_time("13:45:30"));
}

TEST_F(MySQLCoercion, CoerceDateToDatetime) {
    auto v = value_date(datetime_parse::days_since_epoch(2026, 4, 10));
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_DATETIME, arena);
    EXPECT_EQ(r.tag, Value::TAG_DATETIME);
    int64_t expected = static_cast<int64_t>(datetime_parse::days_since_epoch(2026, 4, 10))
                       * 86400LL * 1000000LL;
    EXPECT_EQ(r.datetime_val, expected);
}

TEST_F(MySQLCoercion, CoerceDatetimeToDateTruncates) {
    auto v = value_datetime(datetime_parse::parse_datetime("2026-04-10 13:45:30"));
    auto r = CoercionRules<Dialect::MySQL>::coerce_value(v, Value::TAG_DATE, arena);
    EXPECT_EQ(r.tag, Value::TAG_DATE);
    EXPECT_EQ(r.date_val, datetime_parse::days_since_epoch(2026, 4, 10));
}

// ----- PostgreSQL: strict, string -> temporal is NOT implicit -----

TEST_F(PgSQLCoercion, StringToDateIsNotImplicit) {
    const char* s = "2026-04-10";
    auto v = value_string(StringRef{s, 10});
    auto r = CoercionRules<Dialect::PostgreSQL>::coerce_value(v, Value::TAG_DATE, arena);
    EXPECT_TRUE(r.is_null())
        << "PostgreSQL requires CAST('2026-04-10' AS date) -- implicit is NULL";
}

TEST_F(PgSQLCoercion, DateToTimestampIsImplicit) {
    auto v = value_date(datetime_parse::days_since_epoch(2026, 4, 10));
    auto r = CoercionRules<Dialect::PostgreSQL>::coerce_value(v, Value::TAG_TIMESTAMP, arena);
    EXPECT_EQ(r.tag, Value::TAG_TIMESTAMP);
    int64_t expected = static_cast<int64_t>(datetime_parse::days_since_epoch(2026, 4, 10))
                       * 86400LL * 1000000LL;
    EXPECT_EQ(r.timestamp_val, expected);
}
