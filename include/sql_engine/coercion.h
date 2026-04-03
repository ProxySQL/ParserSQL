#ifndef SQL_ENGINE_COERCION_H
#define SQL_ENGINE_COERCION_H

#include "sql_engine/types.h"
#include "sql_engine/value.h"
#include "sql_parser/common.h"   // Dialect, StringRef
#include "sql_parser/arena.h"    // Arena
#include <cstdlib>
#include <cstdio>
#include <cstring>

namespace sql_engine {

using sql_parser::Dialect;
using sql_parser::Arena;

template <Dialect D>
struct CoercionRules {
    // Can `from` be implicitly coerced to `to`?
    static bool can_coerce(SqlType::Kind from, SqlType::Kind to);

    // What common type should two operands be promoted to?
    static SqlType::Kind common_type(SqlType::Kind left, SqlType::Kind right);

    // Perform the coercion. Returns new Value with target tag, or NULL on failure.
    static Value coerce_value(const Value& val, Value::Tag target, Arena& arena);
};

// ----- Helper: parse int from string (MySQL: lenient, stops at first non-digit) -----
namespace detail {

inline bool parse_int_lenient(const char* s, uint32_t len, int64_t& out) {
    if (len == 0) { out = 0; return true; }
    char* end = nullptr;
    char buf[64];
    uint32_t n = len < 63 ? len : 63;
    std::memcpy(buf, s, n);
    buf[n] = '\0';
    out = std::strtoll(buf, &end, 10);
    return end != buf;  // at least one char consumed
}

inline bool parse_int_strict(const char* s, uint32_t len, int64_t& out) {
    if (len == 0) return false;
    char* end = nullptr;
    char buf[64];
    uint32_t n = len < 63 ? len : 63;
    std::memcpy(buf, s, n);
    buf[n] = '\0';
    out = std::strtoll(buf, &end, 10);
    // Strict: entire string must be consumed (ignoring trailing whitespace)
    while (end && *end == ' ') ++end;
    return end == buf + n;
}

inline bool parse_double_lenient(const char* s, uint32_t len, double& out) {
    if (len == 0) { out = 0.0; return true; }
    char buf[128];
    uint32_t n = len < 127 ? len : 127;
    std::memcpy(buf, s, n);
    buf[n] = '\0';
    char* end = nullptr;
    out = std::strtod(buf, &end);
    return end != buf;
}

inline StringRef int_to_string(int64_t v, Arena& arena) {
    char buf[32];
    int n = std::snprintf(buf, sizeof(buf), "%lld", (long long)v);
    return arena.allocate_string(buf, static_cast<uint32_t>(n));
}

inline StringRef double_to_string(double v, Arena& arena) {
    char buf[64];
    int n = std::snprintf(buf, sizeof(buf), "%g", v);
    return arena.allocate_string(buf, static_cast<uint32_t>(n));
}

} // namespace detail

// ----- MySQL specialization (permissive) -----

template <>
inline bool CoercionRules<Dialect::MySQL>::can_coerce(SqlType::Kind from, SqlType::Kind to) {
    if (from == to) return true;
    if (from == SqlType::NULL_TYPE) return true;  // NULL coerces to anything

    // Numeric within-category: always
    SqlType probe_from{from}; SqlType probe_to{to};
    if (probe_from.is_numeric() && probe_to.is_numeric()) return true;

    // String <-> Numeric: MySQL allows
    if (probe_from.is_string() && probe_to.is_numeric()) return true;
    if (probe_from.is_numeric() && probe_to.is_string()) return true;

    // String <-> Temporal: MySQL allows (parse attempt)
    if (probe_from.is_string() && probe_to.is_temporal()) return true;
    if (probe_from.is_temporal() && probe_to.is_string()) return true;

    // Temporal within-category: allowed
    if (probe_from.is_temporal() && probe_to.is_temporal()) return true;

    return false;
}

template <>
inline SqlType::Kind CoercionRules<Dialect::MySQL>::common_type(SqlType::Kind left, SqlType::Kind right) {
    if (left == right) return left;

    SqlType pl{left}; SqlType pr{right};

    // Both numeric: promote up the hierarchy
    if (pl.is_numeric() && pr.is_numeric()) {
        // BOOL < TINYINT < ... < BIGINT < FLOAT < DOUBLE < DECIMAL
        // Use the wider kind
        return left > right ? left : right;
    }

    // One string, one numeric: MySQL promotes to numeric (DOUBLE)
    if ((pl.is_numeric() && pr.is_string()) || (pl.is_string() && pr.is_numeric())) {
        return SqlType::DOUBLE;
    }

    // Both temporal: promote to wider
    if (pl.is_temporal() && pr.is_temporal()) {
        return left > right ? left : right;
    }

    // Fallback: STRING
    return SqlType::VARCHAR;
}

template <>
inline Value CoercionRules<Dialect::MySQL>::coerce_value(const Value& val, Value::Tag target, Arena& arena) {
    if (val.tag == target) return val;
    if (val.is_null()) return value_null();

    switch (target) {
        case Value::TAG_INT64: {
            if (val.tag == Value::TAG_BOOL) return value_int(val.bool_val ? 1 : 0);
            if (val.tag == Value::TAG_UINT64) return value_int(static_cast<int64_t>(val.uint_val));
            if (val.tag == Value::TAG_DOUBLE) return value_int(static_cast<int64_t>(val.double_val));
            if (val.tag == Value::TAG_STRING || val.tag == Value::TAG_DECIMAL) {
                int64_t out;
                if (detail::parse_int_lenient(val.str_val.ptr, val.str_val.len, out))
                    return value_int(out);
            }
            return value_null();
        }
        case Value::TAG_DOUBLE: {
            if (val.tag == Value::TAG_BOOL) return value_double(val.bool_val ? 1.0 : 0.0);
            if (val.tag == Value::TAG_INT64) return value_double(static_cast<double>(val.int_val));
            if (val.tag == Value::TAG_UINT64) return value_double(static_cast<double>(val.uint_val));
            if (val.tag == Value::TAG_STRING || val.tag == Value::TAG_DECIMAL) {
                double out;
                if (detail::parse_double_lenient(val.str_val.ptr, val.str_val.len, out))
                    return value_double(out);
            }
            return value_null();
        }
        case Value::TAG_STRING: {
            if (val.tag == Value::TAG_INT64)
                return value_string(detail::int_to_string(val.int_val, arena));
            if (val.tag == Value::TAG_UINT64) {
                char buf[32];
                int n = std::snprintf(buf, sizeof(buf), "%llu", (unsigned long long)val.uint_val);
                return value_string(arena.allocate_string(buf, static_cast<uint32_t>(n)));
            }
            if (val.tag == Value::TAG_DOUBLE)
                return value_string(detail::double_to_string(val.double_val, arena));
            if (val.tag == Value::TAG_BOOL)
                return value_string(val.bool_val
                    ? arena.allocate_string("1", 1)
                    : arena.allocate_string("0", 1));
            return value_null();
        }
        case Value::TAG_BOOL: {
            if (val.tag == Value::TAG_INT64) return value_bool(val.int_val != 0);
            if (val.tag == Value::TAG_DOUBLE) return value_bool(val.double_val != 0.0);
            return value_null();
        }
        default:
            return value_null();
    }
}

// ----- PostgreSQL specialization (strict) -----

template <>
inline bool CoercionRules<Dialect::PostgreSQL>::can_coerce(SqlType::Kind from, SqlType::Kind to) {
    if (from == to) return true;
    if (from == SqlType::NULL_TYPE) return true;

    SqlType probe_from{from}; SqlType probe_to{to};

    // Within-category numeric promotions only
    if (probe_from.is_numeric() && probe_to.is_numeric()) {
        // Only allow widening: kind value must increase (narrower to wider)
        return to > from;
    }

    // Temporal within-category promotions
    if (probe_from.is_temporal() && probe_to.is_temporal()) {
        return to > from;
    }

    // No cross-category implicit coercion in PostgreSQL
    return false;
}

template <>
inline SqlType::Kind CoercionRules<Dialect::PostgreSQL>::common_type(SqlType::Kind left, SqlType::Kind right) {
    if (left == right) return left;

    SqlType pl{left}; SqlType pr{right};

    // Both numeric: promote to wider
    if (pl.is_numeric() && pr.is_numeric()) {
        return left > right ? left : right;
    }

    // Both temporal: promote to wider
    if (pl.is_temporal() && pr.is_temporal()) {
        return left > right ? left : right;
    }

    // Cross-category: return UNKNOWN (error)
    return SqlType::UNKNOWN;
}

template <>
inline Value CoercionRules<Dialect::PostgreSQL>::coerce_value(const Value& val, Value::Tag target, Arena& /*arena*/) {
    if (val.tag == target) return val;
    if (val.is_null()) return value_null();

    // PostgreSQL: only within-category promotions
    switch (target) {
        case Value::TAG_INT64: {
            if (val.tag == Value::TAG_BOOL) return value_int(val.bool_val ? 1 : 0);
            if (val.tag == Value::TAG_UINT64) return value_int(static_cast<int64_t>(val.uint_val));
            // String -> int: NOT allowed implicitly in PostgreSQL
            return value_null();
        }
        case Value::TAG_DOUBLE: {
            if (val.tag == Value::TAG_BOOL) return value_double(val.bool_val ? 1.0 : 0.0);
            if (val.tag == Value::TAG_INT64) return value_double(static_cast<double>(val.int_val));
            if (val.tag == Value::TAG_UINT64) return value_double(static_cast<double>(val.uint_val));
            // String -> double: NOT allowed implicitly
            return value_null();
        }
        case Value::TAG_BOOL: {
            // PostgreSQL does not implicitly convert int to bool
            return value_null();
        }
        default:
            return value_null();
    }
}

} // namespace sql_engine

#endif // SQL_ENGINE_COERCION_H
