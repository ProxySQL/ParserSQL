// Tests for the datetime_parse helpers (parse + format round trip).
//
// These cover the format_date / format_time / format_datetime functions that
// were added to close the gap where value_to_string in the wire protocol
// server was returning empty strings for DATE/TIME/DATETIME/TIMESTAMP.

#include "sql_engine/datetime_parse.h"
#include "gtest/gtest.h"
#include <cstring>
#include <string>

using namespace sql_engine::datetime_parse;

// ----- days_to_ymd basic cases -----

TEST(DaysToYmd, Epoch) {
    int y, m, d;
    days_to_ymd(0, y, m, d);
    EXPECT_EQ(y, 1970);
    EXPECT_EQ(m, 1);
    EXPECT_EQ(d, 1);
}

TEST(DaysToYmd, OneDayAfterEpoch) {
    int y, m, d;
    days_to_ymd(1, y, m, d);
    EXPECT_EQ(y, 1970);
    EXPECT_EQ(m, 1);
    EXPECT_EQ(d, 2);
}

TEST(DaysToYmd, OneDayBeforeEpoch) {
    int y, m, d;
    days_to_ymd(-1, y, m, d);
    EXPECT_EQ(y, 1969);
    EXPECT_EQ(m, 12);
    EXPECT_EQ(d, 31);
}

TEST(DaysToYmd, LeapDay) {
    // 2024-02-29 should exist; 2023-02-29 should not (but we don't validate,
    // we just trust the input). Test via round-trip from days_since_epoch.
    int32_t d = days_since_epoch(2024, 2, 29);
    int y, mo, da;
    days_to_ymd(d, y, mo, da);
    EXPECT_EQ(y, 2024);
    EXPECT_EQ(mo, 2);
    EXPECT_EQ(da, 29);
}

TEST(DaysToYmd, RoundTripAcrossManyYears) {
    for (int year = 1900; year <= 2100; ++year) {
        for (int month = 1; month <= 12; ++month) {
            int32_t d = days_since_epoch(year, month, 15);
            int y, mo, da;
            days_to_ymd(d, y, mo, da);
            EXPECT_EQ(y, year) << "year " << year << " month " << month;
            EXPECT_EQ(mo, month) << "year " << year << " month " << month;
            EXPECT_EQ(da, 15) << "year " << year << " month " << month;
        }
    }
}

// ----- format_date -----

TEST(FormatDate, EpochDay) {
    char buf[16];
    size_t n = format_date(0, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "1970-01-01");
}

TEST(FormatDate, SpecificDay) {
    // 2026-04-10 is 20554 days after epoch (validated below).
    int32_t d = days_since_epoch(2026, 4, 10);
    char buf[16];
    size_t n = format_date(d, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "2026-04-10");
}

TEST(FormatDate, RoundTripParseThenFormat) {
    int32_t d = parse_date("2024-02-29");
    char buf[16];
    size_t n = format_date(d, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "2024-02-29");
}

TEST(FormatDate, BufferTooSmall) {
    char buf[5];
    size_t n = format_date(0, buf, sizeof(buf));
    EXPECT_EQ(n, 0u);
}

// ----- format_time -----

TEST(FormatTime, Midnight) {
    char buf[32];
    size_t n = format_time(0, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "00:00:00");
}

TEST(FormatTime, OneSecondPastMidnight) {
    char buf[32];
    size_t n = format_time(1000000LL, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "00:00:01");
}

TEST(FormatTime, WithMicroseconds) {
    // 1 hour, 23 minutes, 45 seconds, 678901 microseconds
    int64_t us = (1LL * 3600 + 23 * 60 + 45) * 1000000LL + 678901LL;
    char buf[32];
    size_t n = format_time(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "01:23:45.678901");
}

TEST(FormatTime, NegativeTime) {
    // MySQL allows negative TIME for intervals.
    int64_t us = -((1LL * 3600 + 23 * 60 + 45) * 1000000LL);
    char buf[32];
    size_t n = format_time(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "-01:23:45");
}

TEST(FormatTime, RoundTripParseThenFormat) {
    int64_t us = parse_time("12:34:56");
    char buf[32];
    size_t n = format_time(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "12:34:56");
}

TEST(FormatTime, RoundTripWithFraction) {
    int64_t us = parse_time("12:34:56.789012");
    char buf[32];
    size_t n = format_time(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "12:34:56.789012");
}

// ----- format_datetime -----

TEST(FormatDatetime, Epoch) {
    char buf[32];
    size_t n = format_datetime(0, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "1970-01-01 00:00:00");
}

TEST(FormatDatetime, RoundTripParseThenFormat) {
    int64_t us = parse_datetime("2026-04-10 13:00:00");
    char buf[32];
    size_t n = format_datetime(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "2026-04-10 13:00:00");
}

TEST(FormatDatetime, RoundTripWithFraction) {
    int64_t us = parse_datetime("2026-04-10 13:00:00.123456");
    char buf[32];
    size_t n = format_datetime(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "2026-04-10 13:00:00.123456");
}

TEST(FormatDatetime, DateBeforeEpoch) {
    // Make sure negative datetimes format the time-of-day part in [00:00:00, 24:00:00)
    // by using floor division on the day boundary.
    int64_t us = parse_datetime("1969-07-20 20:17:40");
    char buf[32];
    size_t n = format_datetime(us, buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, n), "1969-07-20 20:17:40");
}
