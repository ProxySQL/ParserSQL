#ifndef SQL_ENGINE_VALUE_H
#define SQL_ENGINE_VALUE_H

#include "sql_engine/types.h"
#include "sql_parser/common.h"  // StringRef
#include <cstdint>
#include <type_traits>

namespace sql_engine {

using sql_parser::StringRef;

struct Value {
    enum Tag : uint8_t {
        TAG_NULL = 0,
        TAG_BOOL,
        TAG_INT64,
        TAG_UINT64,
        TAG_DOUBLE,
        TAG_DECIMAL,    // stored as StringRef for now
        TAG_STRING,
        TAG_BYTES,
        TAG_DATE,       // days since 1970-01-01 (int32)
        TAG_TIME,       // microseconds since midnight (int64)
        TAG_DATETIME,   // microseconds since epoch (int64)
        TAG_TIMESTAMP,  // microseconds since epoch (int64)
        TAG_INTERVAL,   // months + microseconds
        TAG_JSON,       // stored as StringRef
    };

    Tag tag = TAG_NULL;

    union {
        bool bool_val;
        int64_t int_val;
        uint64_t uint_val;
        double double_val;
        StringRef str_val;      // TAG_STRING, TAG_DECIMAL, TAG_BYTES, TAG_JSON
        int32_t date_val;       // days since epoch
        int64_t time_val;       // microseconds since midnight
        int64_t datetime_val;   // microseconds since epoch
        int64_t timestamp_val;  // microseconds since epoch
        struct { int32_t months; int64_t microseconds; } interval_val;
    };

    bool is_null() const { return tag == TAG_NULL; }
    bool is_numeric() const { return tag >= TAG_BOOL && tag <= TAG_DECIMAL; }
    bool is_string() const { return tag == TAG_STRING; }
    bool is_temporal() const { return tag >= TAG_DATE && tag <= TAG_INTERVAL; }

    // Convert numeric value to double (for arithmetic). Returns 0.0 for non-numeric.
    double to_double() const {
        switch (tag) {
            case TAG_BOOL:   return bool_val ? 1.0 : 0.0;
            case TAG_INT64:  return static_cast<double>(int_val);
            case TAG_UINT64: return static_cast<double>(uint_val);
            case TAG_DOUBLE: return double_val;
            default:         return 0.0;
        }
    }

    // Convert numeric value to int64 (for integer ops). Returns 0 for non-numeric.
    int64_t to_int64() const {
        switch (tag) {
            case TAG_BOOL:   return bool_val ? 1 : 0;
            case TAG_INT64:  return int_val;
            case TAG_UINT64: return static_cast<int64_t>(uint_val);
            case TAG_DOUBLE: return static_cast<int64_t>(double_val);
            default:         return 0;
        }
    }
};

// Value constructors -- free functions matching the spec
inline Value value_null() {
    Value r{};
    r.tag = Value::TAG_NULL;
    return r;
}

inline Value value_bool(bool v) {
    Value r{};
    r.tag = Value::TAG_BOOL;
    r.bool_val = v;
    return r;
}

inline Value value_int(int64_t v) {
    Value r{};
    r.tag = Value::TAG_INT64;
    r.int_val = v;
    return r;
}

inline Value value_uint(uint64_t v) {
    Value r{};
    r.tag = Value::TAG_UINT64;
    r.uint_val = v;
    return r;
}

inline Value value_double(double v) {
    Value r{};
    r.tag = Value::TAG_DOUBLE;
    r.double_val = v;
    return r;
}

inline Value value_decimal(StringRef s) {
    Value r{};
    r.tag = Value::TAG_DECIMAL;
    r.str_val = s;
    return r;
}

inline Value value_string(StringRef s) {
    Value r{};
    r.tag = Value::TAG_STRING;
    r.str_val = s;
    return r;
}

inline Value value_bytes(StringRef s) {
    Value r{};
    r.tag = Value::TAG_BYTES;
    r.str_val = s;
    return r;
}

inline Value value_date(int32_t days) {
    Value r{};
    r.tag = Value::TAG_DATE;
    r.date_val = days;
    return r;
}

inline Value value_time(int64_t us) {
    Value r{};
    r.tag = Value::TAG_TIME;
    r.time_val = us;
    return r;
}

inline Value value_datetime(int64_t us) {
    Value r{};
    r.tag = Value::TAG_DATETIME;
    r.datetime_val = us;
    return r;
}

inline Value value_timestamp(int64_t us) {
    Value r{};
    r.tag = Value::TAG_TIMESTAMP;
    r.timestamp_val = us;
    return r;
}

inline Value value_interval(int32_t months, int64_t us) {
    Value r{};
    r.tag = Value::TAG_INTERVAL;
    r.interval_val = {months, us};
    return r;
}

inline Value value_json(StringRef s) {
    Value r{};
    r.tag = Value::TAG_JSON;
    r.str_val = s;
    return r;
}

} // namespace sql_engine

#endif // SQL_ENGINE_VALUE_H
