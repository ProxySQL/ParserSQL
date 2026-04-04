#include "sql_engine/datetime_parse.h"
#include <cstdlib>
#include <cstring>

namespace sql_engine {
namespace datetime_parse {

static bool is_leap_year(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

static int days_in_month(int y, int m) {
    static const int normal[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    if (m == 2 && is_leap_year(y)) return 29;
    if (m >= 1 && m <= 12) return normal[m];
    return 30;
}

int32_t days_since_epoch(int year, int month, int day) {
    // Count days from 1970-01-01 to year-month-day.
    // Positive for dates after epoch, negative for dates before.
    int32_t total = 0;

    if (year >= 1970) {
        for (int y = 1970; y < year; ++y) {
            total += is_leap_year(y) ? 366 : 365;
        }
        for (int m = 1; m < month; ++m) {
            total += days_in_month(year, m);
        }
        total += day - 1;
    } else {
        for (int y = year; y < 1970; ++y) {
            total -= is_leap_year(y) ? 366 : 365;
        }
        for (int m = 1; m < month; ++m) {
            total += days_in_month(year, m);
        }
        total += day - 1;
    }

    return total;
}

// Parse integer from string, advancing the pointer.
static int parse_int(const char*& s, int digits) {
    int v = 0;
    for (int i = 0; i < digits && *s >= '0' && *s <= '9'; ++i, ++s) {
        v = v * 10 + (*s - '0');
    }
    return v;
}

// Parse fractional microseconds from ".uuuuuu" part.
static int64_t parse_frac_us(const char*& s) {
    if (*s != '.') return 0;
    ++s; // skip '.'
    int64_t frac = 0;
    int digits = 0;
    while (*s >= '0' && *s <= '9' && digits < 6) {
        frac = frac * 10 + (*s - '0');
        ++s;
        ++digits;
    }
    // Pad to 6 digits
    for (int i = digits; i < 6; ++i) frac *= 10;
    // Skip remaining fractional digits
    while (*s >= '0' && *s <= '9') ++s;
    return frac;
}

int32_t parse_date(const char* s) {
    if (!s || !*s) return 0;
    const char* p = s;
    int year = parse_int(p, 4);
    if (*p == '-') ++p;
    int month = parse_int(p, 2);
    if (*p == '-') ++p;
    int day = parse_int(p, 2);
    return days_since_epoch(year, month, day);
}

int64_t parse_datetime(const char* s) {
    if (!s || !*s) return 0;
    const char* p = s;
    int year = parse_int(p, 4);
    if (*p == '-') ++p;
    int month = parse_int(p, 2);
    if (*p == '-') ++p;
    int day = parse_int(p, 2);
    if (*p == ' ' || *p == 'T') ++p;
    int hour = parse_int(p, 2);
    if (*p == ':') ++p;
    int minute = parse_int(p, 2);
    if (*p == ':') ++p;
    int second = parse_int(p, 2);
    int64_t frac = parse_frac_us(p);

    int32_t days = days_since_epoch(year, month, day);
    int64_t us = static_cast<int64_t>(days) * 86400LL * 1000000LL
               + static_cast<int64_t>(hour) * 3600LL * 1000000LL
               + static_cast<int64_t>(minute) * 60LL * 1000000LL
               + static_cast<int64_t>(second) * 1000000LL
               + frac;
    return us;
}

int64_t parse_time(const char* s) {
    if (!s || !*s) return 0;
    bool negative = false;
    const char* p = s;
    if (*p == '-') { negative = true; ++p; }
    int hour = parse_int(p, 3); // MySQL TIME can be > 24h
    if (*p == ':') ++p;
    int minute = parse_int(p, 2);
    if (*p == ':') ++p;
    int second = parse_int(p, 2);
    int64_t frac = parse_frac_us(p);

    int64_t us = static_cast<int64_t>(hour) * 3600LL * 1000000LL
               + static_cast<int64_t>(minute) * 60LL * 1000000LL
               + static_cast<int64_t>(second) * 1000000LL
               + frac;
    return negative ? -us : us;
}

} // namespace datetime_parse
} // namespace sql_engine
