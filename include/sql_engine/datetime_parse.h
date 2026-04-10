#ifndef SQL_ENGINE_DATETIME_PARSE_H
#define SQL_ENGINE_DATETIME_PARSE_H

#include <cstdint>
#include <cstddef>

namespace sql_engine {
namespace datetime_parse {

// Parse "YYYY-MM-DD" to days since 1970-01-01.
int32_t parse_date(const char* s);

// Parse "YYYY-MM-DD HH:MM:SS[.uuuuuu]" to microseconds since epoch.
int64_t parse_datetime(const char* s);

// Parse "HH:MM:SS[.uuuuuu]" to microseconds since midnight.
int64_t parse_time(const char* s);

// Calendar math: compute days since 1970-01-01 for the given date.
// Handles leap years correctly.
int32_t days_since_epoch(int year, int month, int day);

// Calendar math: decompose days since 1970-01-01 into year/month/day.
// Handles leap years and dates before the epoch correctly.
void days_to_ymd(int32_t days, int& year, int& month, int& day);

// Format days since epoch as "YYYY-MM-DD" into buf (needs >= 11 bytes including NUL).
// Returns number of bytes written (excluding NUL).
size_t format_date(int32_t days, char* buf, size_t buf_len);

// Format microseconds since midnight as "HH:MM:SS" or "HH:MM:SS.uuuuuu".
// Returns number of bytes written (excluding NUL).
size_t format_time(int64_t us_since_midnight, char* buf, size_t buf_len);

// Format microseconds since epoch as "YYYY-MM-DD HH:MM:SS[.uuuuuu]".
// Returns number of bytes written (excluding NUL).
size_t format_datetime(int64_t us_since_epoch, char* buf, size_t buf_len);

} // namespace datetime_parse
} // namespace sql_engine

#endif // SQL_ENGINE_DATETIME_PARSE_H
