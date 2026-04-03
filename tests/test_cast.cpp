#include <gtest/gtest.h>
#include "sql_engine/functions/cast.h"
#include "sql_parser/arena.h"
#include <string>

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Dialect;
using sql_parser::Arena;
using sql_parser::StringRef;

class CastTest : public ::testing::Test {
protected:
    Arena arena{4096};

    Value S(const char* s) { return value_string(StringRef{s, static_cast<uint32_t>(std::strlen(s))}); }
    std::string str(const Value& v) { return std::string(v.str_val.ptr, v.str_val.len); }
};

// ===== MySQL CAST =====

TEST_F(CastTest, MySQLStringToInt) {
    auto r = cast_value<Dialect::MySQL>(S("42"), SqlType::INT, arena);
    EXPECT_EQ(r.tag, Value::TAG_INT64);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(CastTest, MySQLStringToIntLenient) {
    auto r = cast_value<Dialect::MySQL>(S("42abc"), SqlType::INT, arena);
    EXPECT_EQ(r.tag, Value::TAG_INT64);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(CastTest, MySQLIntToString) {
    auto r = cast_value<Dialect::MySQL>(value_int(42), SqlType::VARCHAR, arena);
    EXPECT_EQ(r.tag, Value::TAG_STRING);
    EXPECT_EQ(str(r), "42");
}

TEST_F(CastTest, MySQLIntToDouble) {
    auto r = cast_value<Dialect::MySQL>(value_int(42), SqlType::DOUBLE, arena);
    EXPECT_EQ(r.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(r.double_val, 42.0);
}

TEST_F(CastTest, MySQLDoubleToInt) {
    auto r = cast_value<Dialect::MySQL>(value_double(3.7), SqlType::INT, arena);
    EXPECT_EQ(r.tag, Value::TAG_INT64);
    EXPECT_EQ(r.int_val, 3);
}

TEST_F(CastTest, MySQLBoolToInt) {
    EXPECT_EQ(cast_value<Dialect::MySQL>(value_bool(true), SqlType::INT, arena).int_val, 1);
    EXPECT_EQ(cast_value<Dialect::MySQL>(value_bool(false), SqlType::INT, arena).int_val, 0);
}

TEST_F(CastTest, MySQLNullPassthrough) {
    EXPECT_TRUE(cast_value<Dialect::MySQL>(value_null(), SqlType::INT, arena).is_null());
}

TEST_F(CastTest, MySQLSameTypNoop) {
    auto v = value_int(42);
    auto r = cast_value<Dialect::MySQL>(v, SqlType::BIGINT, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(CastTest, MySQLDoubleToString) {
    auto r = cast_value<Dialect::MySQL>(value_double(3.14), SqlType::VARCHAR, arena);
    EXPECT_EQ(str(r), "3.14");
}

TEST_F(CastTest, MySQLBoolToString) {
    auto r = cast_value<Dialect::MySQL>(value_bool(true), SqlType::VARCHAR, arena);
    EXPECT_EQ(str(r), "1");
}

// ===== PostgreSQL CAST =====

TEST_F(CastTest, PgSQLStringToIntStrict) {
    auto r = cast_value<Dialect::PostgreSQL>(S("42"), SqlType::INT, arena);
    EXPECT_EQ(r.tag, Value::TAG_INT64);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(CastTest, PgSQLStringToIntStrictFails) {
    // "42abc" should fail in strict mode
    auto r = cast_value<Dialect::PostgreSQL>(S("42abc"), SqlType::INT, arena);
    EXPECT_TRUE(r.is_null());
}

TEST_F(CastTest, PgSQLIntToString) {
    auto r = cast_value<Dialect::PostgreSQL>(value_int(42), SqlType::VARCHAR, arena);
    EXPECT_EQ(str(r), "42");
}

TEST_F(CastTest, PgSQLBoolToString) {
    EXPECT_EQ(str(cast_value<Dialect::PostgreSQL>(value_bool(true), SqlType::VARCHAR, arena)), "true");
    EXPECT_EQ(str(cast_value<Dialect::PostgreSQL>(value_bool(false), SqlType::VARCHAR, arena)), "false");
}

TEST_F(CastTest, PgSQLStringToBoolTrue) {
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(S("true"), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(S("t"), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(S("yes"), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(S("1"), SqlType::BOOLEAN, arena).bool_val);
}

TEST_F(CastTest, PgSQLStringToBoolFalse) {
    EXPECT_FALSE(cast_value<Dialect::PostgreSQL>(S("false"), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_FALSE(cast_value<Dialect::PostgreSQL>(S("f"), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_FALSE(cast_value<Dialect::PostgreSQL>(S("no"), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_FALSE(cast_value<Dialect::PostgreSQL>(S("0"), SqlType::BOOLEAN, arena).bool_val);
}

TEST_F(CastTest, PgSQLStringToBoolInvalid) {
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(S("maybe"), SqlType::BOOLEAN, arena).is_null());
}

TEST_F(CastTest, PgSQLIntToBool) {
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(value_int(1), SqlType::BOOLEAN, arena).bool_val);
    EXPECT_FALSE(cast_value<Dialect::PostgreSQL>(value_int(0), SqlType::BOOLEAN, arena).bool_val);
}

TEST_F(CastTest, PgSQLNullPassthrough) {
    EXPECT_TRUE(cast_value<Dialect::PostgreSQL>(value_null(), SqlType::INT, arena).is_null());
}

// ===== Unsupported target =====

TEST_F(CastTest, UnsupportedTarget) {
    EXPECT_TRUE(cast_value<Dialect::MySQL>(value_int(1), SqlType::UNKNOWN, arena).is_null());
}
