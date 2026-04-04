#ifndef SQL_ENGINE_DATETIME_PARSE_H
#define SQL_ENGINE_DATETIME_PARSE_H

#include <cstdint>

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

} // namespace datetime_parse
} // namespace sql_engine

#endif // SQL_ENGINE_DATETIME_PARSE_H
