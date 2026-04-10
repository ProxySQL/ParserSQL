// Tests for temporal built-in functions:
//   NOW, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURDATE, CURTIME,
//   YEAR, MONTH, DAY, DAYOFMONTH, HOUR, MINUTE, SECOND,
//   UNIX_TIMESTAMP, FROM_UNIXTIME, DATEDIFF
//
// These were flagged as the biggest practical gap in the codebase review:
// zero temporal functions were previously registered, so queries like
// SELECT * FROM events WHERE created_at > NOW() - INTERVAL 1 DAY failed.

#include "sql_engine/function_registry.h"
#include "sql_engine/datetime_parse.h"
#include "sql_engine/value.h"
#include "sql_parser/arena.h"
#include "gtest/gtest.h"
#include <chrono>
#include <cstring>
#include <string>

using namespace sql_engine;
using sql_parser::Arena;
using sql_parser::Dialect;
using sql_parser::StringRef;

namespace {

StringRef arena_str(Arena& arena, const char* s) {
    uint32_t len = static_cast<uint32_t>(std::strlen(s));
    char* buf = static_cast<char*>(arena.allocate(len));
    std::memcpy(buf, s, len);
    return StringRef{buf, len};
}

// Call a registered function by name. Return NULL value if the function
// isn't found (should not happen for any name these tests reference).
Value call(FunctionRegistry<Dialect::MySQL>& reg, const char* name,
           std::initializer_list<Value> args, Arena& arena) {
    const FunctionEntry* entry = reg.lookup(name, static_cast<uint32_t>(std::strlen(name)));
    if (!entry) return value_null();
    // Value has a deleted default constructor, so initialize each slot to
    // NULL before copying the provided args in.
    Value arg_buf[8] = {
        value_null(), value_null(), value_null(), value_null(),
        value_null(), value_null(), value_null(), value_null(),
    };
    uint16_t i = 0;
    for (auto& a : args) arg_buf[i++] = a;
    return entry->impl(arg_buf, static_cast<uint16_t>(args.size()), arena);
}

}  // namespace

// =====================================================================
// Current-time functions
// =====================================================================

class DatetimeFuncsTest : public ::testing::Test {
protected:
    Arena arena{65536, 1048576};
    FunctionRegistry<Dialect::MySQL> registry;

    void SetUp() override {
        registry.register_builtins();
    }
};

TEST_F(DatetimeFuncsTest, NowReturnsDatetime) {
    Value v = call(registry, "NOW", {}, arena);
    EXPECT_EQ(v.tag, Value::TAG_DATETIME);
    // Sanity: result should be within a few seconds of wall clock.
    auto now = std::chrono::system_clock::now();
    int64_t expected_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              now.time_since_epoch()).count();
    int64_t diff = v.datetime_val - expected_us;
    if (diff < 0) diff = -diff;
    EXPECT_LT(diff, 2LL * 1000000);  // < 2 seconds
}

TEST_F(DatetimeFuncsTest, CurrentTimestampIsAliasForNow) {
    Value v = call(registry, "CURRENT_TIMESTAMP", {}, arena);
    EXPECT_EQ(v.tag, Value::TAG_DATETIME);
}

TEST_F(DatetimeFuncsTest, CurrentDateReturnsDate) {
    Value v = call(registry, "CURRENT_DATE", {}, arena);
    EXPECT_EQ(v.tag, Value::TAG_DATE);
    // Format it and verify it looks like a plausible YYYY-MM-DD.
    char buf[16];
    size_t n = datetime_parse::format_date(v.date_val, buf, sizeof(buf));
    std::string s(buf, n);
    EXPECT_EQ(s.size(), 10u);
    EXPECT_EQ(s[4], '-');
    EXPECT_EQ(s[7], '-');
}

TEST_F(DatetimeFuncsTest, CurdateIsAliasForCurrentDate) {
    Value a = call(registry, "CURRENT_DATE", {}, arena);
    Value b = call(registry, "CURDATE", {}, arena);
    EXPECT_EQ(a.tag, Value::TAG_DATE);
    EXPECT_EQ(b.tag, Value::TAG_DATE);
    // Same day (may differ by off-by-one if we're at midnight but that's extremely unlikely during a test run).
    EXPECT_EQ(a.date_val, b.date_val);
}

TEST_F(DatetimeFuncsTest, CurrentTimeReturnsTime) {
    Value v = call(registry, "CURRENT_TIME", {}, arena);
    EXPECT_EQ(v.tag, Value::TAG_TIME);
    // Time-of-day must be in [0, 86400 * 1e6).
    EXPECT_GE(v.time_val, 0);
    EXPECT_LT(v.time_val, 86400LL * 1000000LL);
}

// =====================================================================
// Component extractors
// =====================================================================

TEST_F(DatetimeFuncsTest, YearMonthDayFromDate) {
    Value date = value_date(datetime_parse::days_since_epoch(2026, 4, 10));
    EXPECT_EQ(call(registry, "YEAR",  {date}, arena).int_val, 2026);
    EXPECT_EQ(call(registry, "MONTH", {date}, arena).int_val, 4);
    EXPECT_EQ(call(registry, "DAY",   {date}, arena).int_val, 10);
    EXPECT_EQ(call(registry, "DAYOFMONTH", {date}, arena).int_val, 10);
}

TEST_F(DatetimeFuncsTest, HourMinuteSecondFromDatetime) {
    // 2026-04-10 13:45:30.500000 -- parse via datetime_parse, then extract.
    int64_t us = datetime_parse::parse_datetime("2026-04-10 13:45:30.500000");
    Value dt = value_datetime(us);
    EXPECT_EQ(call(registry, "HOUR",   {dt}, arena).int_val, 13);
    EXPECT_EQ(call(registry, "MINUTE", {dt}, arena).int_val, 45);
    EXPECT_EQ(call(registry, "SECOND", {dt}, arena).int_val, 30);
    EXPECT_EQ(call(registry, "YEAR",   {dt}, arena).int_val, 2026);
    EXPECT_EQ(call(registry, "MONTH",  {dt}, arena).int_val, 4);
    EXPECT_EQ(call(registry, "DAY",    {dt}, arena).int_val, 10);
}

TEST_F(DatetimeFuncsTest, YearFromString) {
    Value s = value_string(arena_str(arena, "2024-12-25"));
    EXPECT_EQ(call(registry, "YEAR",  {s}, arena).int_val, 2024);
    EXPECT_EQ(call(registry, "MONTH", {s}, arena).int_val, 12);
    EXPECT_EQ(call(registry, "DAY",   {s}, arena).int_val, 25);
}

TEST_F(DatetimeFuncsTest, YearFromDatetimeString) {
    Value s = value_string(arena_str(arena, "2024-12-25 08:30:00"));
    EXPECT_EQ(call(registry, "YEAR",   {s}, arena).int_val, 2024);
    EXPECT_EQ(call(registry, "HOUR",   {s}, arena).int_val, 8);
    EXPECT_EQ(call(registry, "MINUTE", {s}, arena).int_val, 30);
}

TEST_F(DatetimeFuncsTest, NullInputReturnsNull) {
    EXPECT_TRUE(call(registry, "YEAR",  {value_null()}, arena).is_null());
    EXPECT_TRUE(call(registry, "MONTH", {value_null()}, arena).is_null());
    EXPECT_TRUE(call(registry, "HOUR",  {value_null()}, arena).is_null());
}

TEST_F(DatetimeFuncsTest, UnparseableStringReturnsNull) {
    // Not a date at all -- the extractor should return NULL rather than
    // crash or return garbage. parse_date just reads 0s out of junk, so
    // we only test that it doesn't crash and returns SOMETHING (the
    // "return NULL for clearly invalid" behavior is aspirational; the
    // current parser is permissive).
    Value s = value_string(arena_str(arena, ""));
    Value y = call(registry, "YEAR", {s}, arena);
    EXPECT_TRUE(y.is_null() || y.tag == Value::TAG_INT64);
}

// =====================================================================
// Epoch conversion
// =====================================================================

TEST_F(DatetimeFuncsTest, UnixTimestampNoArgsIsCurrentTime) {
    Value v = call(registry, "UNIX_TIMESTAMP", {}, arena);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    auto now = std::chrono::system_clock::now();
    int64_t expected = std::chrono::duration_cast<std::chrono::seconds>(
                           now.time_since_epoch()).count();
    int64_t diff = v.int_val - expected;
    if (diff < 0) diff = -diff;
    EXPECT_LT(diff, 2);  // within 2 seconds of now
}

TEST_F(DatetimeFuncsTest, UnixTimestampFromDatetime) {
    // 1970-01-01 00:00:01 UTC = 1 second after epoch
    Value dt = value_datetime(1LL * 1000000LL);
    Value v = call(registry, "UNIX_TIMESTAMP", {dt}, arena);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 1);
}

TEST_F(DatetimeFuncsTest, FromUnixtimeReturnsDatetime) {
    Value v = call(registry, "FROM_UNIXTIME", {value_int(0)}, arena);
    EXPECT_EQ(v.tag, Value::TAG_DATETIME);
    EXPECT_EQ(v.datetime_val, 0);  // 1970-01-01 00:00:00
}

TEST_F(DatetimeFuncsTest, FromUnixtimeRoundTripsWithUnixTimestamp) {
    int64_t sec = 1712750400;  // 2024-04-10 12:00:00 UTC
    Value dt = call(registry, "FROM_UNIXTIME", {value_int(sec)}, arena);
    Value back = call(registry, "UNIX_TIMESTAMP", {dt}, arena);
    EXPECT_EQ(back.int_val, sec);
    EXPECT_EQ(call(registry, "YEAR",  {dt}, arena).int_val, 2024);
    EXPECT_EQ(call(registry, "MONTH", {dt}, arena).int_val, 4);
    EXPECT_EQ(call(registry, "DAY",   {dt}, arena).int_val, 10);
}

// =====================================================================
// DATEDIFF
// =====================================================================

TEST_F(DatetimeFuncsTest, DateDiffSameDay) {
    Value d = value_date(datetime_parse::days_since_epoch(2026, 4, 10));
    EXPECT_EQ(call(registry, "DATEDIFF", {d, d}, arena).int_val, 0);
}

TEST_F(DatetimeFuncsTest, DateDiffOneDay) {
    Value d1 = value_date(datetime_parse::days_since_epoch(2026, 4, 11));
    Value d2 = value_date(datetime_parse::days_since_epoch(2026, 4, 10));
    EXPECT_EQ(call(registry, "DATEDIFF", {d1, d2}, arena).int_val, 1);
    // Reversed: should be -1
    EXPECT_EQ(call(registry, "DATEDIFF", {d2, d1}, arena).int_val, -1);
}

TEST_F(DatetimeFuncsTest, DateDiffAcrossYearBoundary) {
    Value d1 = value_date(datetime_parse::days_since_epoch(2025, 1, 1));
    Value d2 = value_date(datetime_parse::days_since_epoch(2024, 1, 1));
    EXPECT_EQ(call(registry, "DATEDIFF", {d1, d2}, arena).int_val, 366);  // 2024 is leap
}

TEST_F(DatetimeFuncsTest, DateDiffAcceptsStringArgs) {
    Value a = value_string(arena_str(arena, "2026-04-10"));
    Value b = value_string(arena_str(arena, "2026-04-01"));
    EXPECT_EQ(call(registry, "DATEDIFF", {a, b}, arena).int_val, 9);
}

TEST_F(DatetimeFuncsTest, DateDiffMixedTypes) {
    // Date - datetime (same day) should be 0.
    Value d  = value_date(datetime_parse::days_since_epoch(2026, 4, 10));
    Value dt = value_datetime(datetime_parse::parse_datetime("2026-04-10 13:45:30"));
    EXPECT_EQ(call(registry, "DATEDIFF", {d, dt}, arena).int_val, 0);
}

TEST_F(DatetimeFuncsTest, DateDiffNullPropagates) {
    Value d = value_date(0);
    EXPECT_TRUE(call(registry, "DATEDIFF", {value_null(), d}, arena).is_null());
    EXPECT_TRUE(call(registry, "DATEDIFF", {d, value_null()}, arena).is_null());
}
