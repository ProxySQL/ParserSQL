# SQL Engine Type System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the type system foundation for the composable SQL query engine -- SqlType metadata, Value tagged union, three-valued NULL logic, dialect-templated coercion, extensible function registry, and all P0 built-in functions (arithmetic, comparison, string, cast).

**Architecture:** Header-only library in `include/sql_engine/` that depends on the parser's `Arena`, `StringRef`, and `Dialect` but is otherwise independent. Dialect differences handled via `template <Dialect D>` specialization (same pattern as the parser). One `.cpp` file for `register_builtins()`.

**Tech Stack:** C++17, GNU Make, Google Test (already in `third_party/`), parser library (`libsqlparser.a`)

**Spec:** `docs/superpowers/specs/2026-04-03-type-system-design.md`

---

## Scope

This plan builds:
1. `SqlType` -- type metadata struct with Kind enum, precision, scale, flags
2. `Value` -- tagged union for runtime values (14 tags), constructors, helpers
3. `null_semantics` -- three-valued logic (AND/OR/NOT with NULL)
4. `CoercionRules<D>` -- dialect-templated type coercion (MySQL permissive, PostgreSQL strict)
5. `FunctionRegistry<D>` -- extensible function dispatch table
6. P0 built-in functions: arithmetic, comparison, string, cast
7. Comprehensive tests for every component

**Not in scope:** Expression evaluator (sub-project 2), row/tuple format (sub-project 4), catalog/schema (sub-project 3), aggregate functions, date/time functions beyond constructors, JSON functions.

---

## File Structure

```
include/sql_engine/
    types.h              -- SqlType struct with Kind enum
    value.h              -- Value tagged union + constructors + helpers
    null_semantics.h     -- NULL propagation, three-valued AND/OR/NOT
    coercion.h           -- CoercionRules<D> template (MySQL + PostgreSQL)
    function_registry.h  -- FunctionRegistry<D>, FunctionEntry, SqlFunction typedef
    functions/
        arithmetic.h     -- ABS, CEIL, FLOOR, ROUND, TRUNCATE, MOD, POWER, SQRT, SIGN
        comparison.h     -- LEAST, GREATEST, COALESCE, NULLIF, IFNULL, IF
        string.h         -- CONCAT, LENGTH, UPPER, LOWER, SUBSTRING, TRIM, etc.
        cast.h           -- CAST(x AS type) for all type pairs

src/sql_engine/
    function_registry.cpp -- register_builtins() implementations (explicit template instantiations)

tests/
    test_value.cpp           -- Value creation, tag checks, round-trip
    test_null_semantics.cpp  -- Three-valued logic truth tables
    test_coercion.cpp        -- Coercion matrix, both dialects
    test_arithmetic.cpp      -- P0 arithmetic functions + NULL + edge cases
    test_comparison.cpp      -- LEAST, GREATEST, COALESCE, NULLIF, IFNULL, IF
    test_string_funcs.cpp    -- All P0 string functions
    test_cast.cpp            -- CAST between type pairs, dialect differences
    test_registry.cpp        -- Lookup, case-insensitive, unknown function
```

---

### Task 1: Build System + SqlType + Value

**Files:**
- Create: `include/sql_engine/types.h`
- Create: `include/sql_engine/value.h`
- Create: `tests/test_value.cpp`
- Modify: `Makefile.new`

This task establishes the `include/sql_engine/` directory and the two foundational headers. `SqlType` is metadata (what a column can hold). `Value` is the runtime tagged union that the expression evaluator will operate on.

- [ ] **Step 1: Create `include/sql_engine/types.h`**

Create `include/sql_engine/types.h`:
```cpp
#ifndef SQL_ENGINE_TYPES_H
#define SQL_ENGINE_TYPES_H

#include <cstdint>

namespace sql_engine {

struct SqlType {
    enum Kind : uint8_t {
        // Numeric
        BOOLEAN,
        TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT,
        FLOAT, DOUBLE,
        DECIMAL,

        // String
        CHAR, VARCHAR,
        TEXT, MEDIUMTEXT, LONGTEXT,
        BINARY, VARBINARY, BLOB,

        // Temporal
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        INTERVAL,

        // Structured
        JSON, JSONB,
        ENUM,
        ARRAY,

        // Special
        NULL_TYPE,
        UNKNOWN
    };

    Kind kind = UNKNOWN;
    uint16_t precision = 0;
    uint16_t scale = 0;
    bool is_unsigned = false;
    bool has_timezone = false;

    // Convenience constructors
    static SqlType make_bool() { return {BOOLEAN}; }
    static SqlType make_tinyint(bool uns = false) { return {TINYINT, 0, 0, uns}; }
    static SqlType make_smallint(bool uns = false) { return {SMALLINT, 0, 0, uns}; }
    static SqlType make_int(bool uns = false) { return {INT, 0, 0, uns}; }
    static SqlType make_bigint(bool uns = false) { return {BIGINT, 0, 0, uns}; }
    static SqlType make_float() { return {FLOAT}; }
    static SqlType make_double() { return {DOUBLE}; }
    static SqlType make_decimal(uint16_t p, uint16_t s) { return {DECIMAL, p, s}; }
    static SqlType make_char(uint16_t len) { return {CHAR, len}; }
    static SqlType make_varchar(uint16_t len) { return {VARCHAR, len}; }
    static SqlType make_text() { return {TEXT}; }
    static SqlType make_blob() { return {BLOB}; }
    static SqlType make_date() { return {DATE}; }
    static SqlType make_time() { return {TIME}; }
    static SqlType make_datetime() { return {DATETIME}; }
    static SqlType make_timestamp(bool tz = false) { return {TIMESTAMP, 0, 0, false, tz}; }
    static SqlType make_json() { return {JSON}; }
    static SqlType make_null() { return {NULL_TYPE}; }

    // Category queries
    bool is_numeric() const {
        return kind >= BOOLEAN && kind <= DECIMAL;
    }
    bool is_string() const {
        return kind >= CHAR && kind <= BLOB;
    }
    bool is_temporal() const {
        return kind >= DATE && kind <= INTERVAL;
    }
    bool is_structured() const {
        return kind >= JSON && kind <= ARRAY;
    }

    bool operator==(const SqlType& o) const {
        return kind == o.kind && precision == o.precision &&
               scale == o.scale && is_unsigned == o.is_unsigned &&
               has_timezone == o.has_timezone;
    }
    bool operator!=(const SqlType& o) const { return !(*this == o); }
};

} // namespace sql_engine

#endif // SQL_ENGINE_TYPES_H
```

- [ ] **Step 2: Create `include/sql_engine/value.h`**

Create `include/sql_engine/value.h`:
```cpp
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
```

- [ ] **Step 3: Create `tests/test_value.cpp`**

Create `tests/test_value.cpp`:
```cpp
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

TEST(ValueTest, Interval) {
    auto v = value_interval(2, 3600000000LL);
    EXPECT_EQ(v.tag, Value::TAG_INTERVAL);
    EXPECT_EQ(v.interval_val.months, 2);
    EXPECT_EQ(v.interval_val.microseconds, 3600000000LL);
    EXPECT_TRUE(v.is_temporal());
}

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
```

- [ ] **Step 4: Update `Makefile.new`**

Add `tests/test_value.cpp` to the `TEST_SRCS` list. No new library sources yet (all headers are header-only at this point).

In `Makefile.new`, add `test_value.cpp` to the `TEST_SRCS` variable:
```makefile
TEST_SRCS = $(TEST_DIR)/test_main.cpp \
            $(TEST_DIR)/test_arena.cpp \
            $(TEST_DIR)/test_tokenizer.cpp \
            $(TEST_DIR)/test_classifier.cpp \
            $(TEST_DIR)/test_expression.cpp \
            $(TEST_DIR)/test_set.cpp \
            $(TEST_DIR)/test_select.cpp \
            $(TEST_DIR)/test_emitter.cpp \
            $(TEST_DIR)/test_stmt_cache.cpp \
            $(TEST_DIR)/test_insert.cpp \
            $(TEST_DIR)/test_update.cpp \
            $(TEST_DIR)/test_delete.cpp \
            $(TEST_DIR)/test_compound.cpp \
            $(TEST_DIR)/test_digest.cpp \
            $(TEST_DIR)/test_misc_stmts.cpp \
            $(TEST_DIR)/test_value.cpp
```

- [ ] **Step 5: Build and verify**

```bash
mkdir -p include/sql_engine
make -f Makefile.new clean && make -f Makefile.new test
```

All 430 existing parser tests plus the new Value tests must pass. Expect approximately 450+ tests total.

- [ ] **Step 6: Commit**

```bash
git add include/sql_engine/types.h include/sql_engine/value.h tests/test_value.cpp Makefile.new
git commit -m "Add SqlType and Value foundation for sql_engine type system"
```

---

### Task 2: NULL Semantics

**Files:**
- Create: `include/sql_engine/null_semantics.h`
- Create: `tests/test_null_semantics.cpp`
- Modify: `Makefile.new` (add test file)

Three-valued logic is critical to get right. Every binary operator must propagate NULL correctly, and AND/OR have special short-circuit behavior with NULL.

- [ ] **Step 1: Create `include/sql_engine/null_semantics.h`**

Create `include/sql_engine/null_semantics.h`:
```cpp
#ifndef SQL_ENGINE_NULL_SEMANTICS_H
#define SQL_ENGINE_NULL_SEMANTICS_H

#include "sql_engine/value.h"

namespace sql_engine {
namespace null_semantics {

// For most binary operators: if either operand is NULL, result is NULL.
// Returns true if NULL was propagated (caller should return `result` immediately).
inline bool propagate_null(const Value& left, const Value& right, Value& result) {
    if (left.is_null() || right.is_null()) {
        result = value_null();
        return true;
    }
    return false;
}

// Three-valued AND:
//   FALSE AND anything = FALSE
//   anything AND FALSE = FALSE
//   TRUE AND TRUE      = TRUE
//   NULL AND TRUE      = NULL
//   TRUE AND NULL      = NULL
//   NULL AND NULL      = NULL
inline Value eval_and(const Value& left, const Value& right) {
    // If either side is known FALSE, result is FALSE
    if (!left.is_null() && left.tag == Value::TAG_BOOL && !left.bool_val)
        return value_bool(false);
    if (!right.is_null() && right.tag == Value::TAG_BOOL && !right.bool_val)
        return value_bool(false);
    // If either side is NULL, result is NULL
    if (left.is_null() || right.is_null())
        return value_null();
    // Both are non-null booleans and neither is false, so both are true
    return value_bool(true);
}

// Three-valued OR:
//   TRUE OR anything = TRUE
//   anything OR TRUE = TRUE
//   FALSE OR FALSE   = FALSE
//   NULL OR FALSE    = NULL
//   FALSE OR NULL    = NULL
//   NULL OR NULL     = NULL
inline Value eval_or(const Value& left, const Value& right) {
    // If either side is known TRUE, result is TRUE
    if (!left.is_null() && left.tag == Value::TAG_BOOL && left.bool_val)
        return value_bool(true);
    if (!right.is_null() && right.tag == Value::TAG_BOOL && right.bool_val)
        return value_bool(true);
    // If either side is NULL, result is NULL
    if (left.is_null() || right.is_null())
        return value_null();
    // Both are non-null booleans and neither is true, so both are false
    return value_bool(false);
}

// NOT NULL = NULL, NOT TRUE = FALSE, NOT FALSE = TRUE
inline Value eval_not(const Value& val) {
    if (val.is_null()) return value_null();
    return value_bool(!val.bool_val);
}

} // namespace null_semantics
} // namespace sql_engine

#endif // SQL_ENGINE_NULL_SEMANTICS_H
```

- [ ] **Step 2: Create `tests/test_null_semantics.cpp`**

Create `tests/test_null_semantics.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/null_semantics.h"

using namespace sql_engine;
using namespace sql_engine::null_semantics;

// --- propagate_null ---

TEST(NullSemanticsTest, PropagateNullBothNonNull) {
    Value result;
    EXPECT_FALSE(propagate_null(value_int(1), value_int(2), result));
}

TEST(NullSemanticsTest, PropagateNullLeftNull) {
    Value result;
    EXPECT_TRUE(propagate_null(value_null(), value_int(2), result));
    EXPECT_TRUE(result.is_null());
}

TEST(NullSemanticsTest, PropagateNullRightNull) {
    Value result;
    EXPECT_TRUE(propagate_null(value_int(1), value_null(), result));
    EXPECT_TRUE(result.is_null());
}

TEST(NullSemanticsTest, PropagateNullBothNull) {
    Value result;
    EXPECT_TRUE(propagate_null(value_null(), value_null(), result));
    EXPECT_TRUE(result.is_null());
}

// --- AND truth table (exhaustive 3x3) ---

struct ThreeValuedCase {
    Value left;
    Value right;
    bool expect_null;
    bool expect_val;  // only meaningful if expect_null is false
};

class AndTruthTable : public ::testing::TestWithParam<ThreeValuedCase> {};

TEST_P(AndTruthTable, Check) {
    auto [left, right, expect_null, expect_val] = GetParam();
    Value result = eval_and(left, right);
    if (expect_null) {
        EXPECT_TRUE(result.is_null());
    } else {
        EXPECT_FALSE(result.is_null());
        EXPECT_EQ(result.bool_val, expect_val);
    }
}

INSTANTIATE_TEST_SUITE_P(NullSemantics, AndTruthTable, ::testing::Values(
    // TRUE AND TRUE = TRUE
    ThreeValuedCase{value_bool(true),  value_bool(true),  false, true},
    // TRUE AND FALSE = FALSE
    ThreeValuedCase{value_bool(true),  value_bool(false), false, false},
    // TRUE AND NULL = NULL
    ThreeValuedCase{value_bool(true),  value_null(),      true,  false},
    // FALSE AND TRUE = FALSE
    ThreeValuedCase{value_bool(false), value_bool(true),  false, false},
    // FALSE AND FALSE = FALSE
    ThreeValuedCase{value_bool(false), value_bool(false), false, false},
    // FALSE AND NULL = FALSE
    ThreeValuedCase{value_bool(false), value_null(),      false, false},
    // NULL AND TRUE = NULL
    ThreeValuedCase{value_null(),      value_bool(true),  true,  false},
    // NULL AND FALSE = FALSE
    ThreeValuedCase{value_null(),      value_bool(false), false, false},
    // NULL AND NULL = NULL
    ThreeValuedCase{value_null(),      value_null(),      true,  false}
));

// --- OR truth table (exhaustive 3x3) ---

class OrTruthTable : public ::testing::TestWithParam<ThreeValuedCase> {};

TEST_P(OrTruthTable, Check) {
    auto [left, right, expect_null, expect_val] = GetParam();
    Value result = eval_or(left, right);
    if (expect_null) {
        EXPECT_TRUE(result.is_null());
    } else {
        EXPECT_FALSE(result.is_null());
        EXPECT_EQ(result.bool_val, expect_val);
    }
}

INSTANTIATE_TEST_SUITE_P(NullSemantics, OrTruthTable, ::testing::Values(
    // TRUE OR TRUE = TRUE
    ThreeValuedCase{value_bool(true),  value_bool(true),  false, true},
    // TRUE OR FALSE = TRUE
    ThreeValuedCase{value_bool(true),  value_bool(false), false, true},
    // TRUE OR NULL = TRUE
    ThreeValuedCase{value_bool(true),  value_null(),      false, true},
    // FALSE OR TRUE = TRUE
    ThreeValuedCase{value_bool(false), value_bool(true),  false, true},
    // FALSE OR FALSE = FALSE
    ThreeValuedCase{value_bool(false), value_bool(false), false, false},
    // FALSE OR NULL = NULL
    ThreeValuedCase{value_bool(false), value_null(),      true,  false},
    // NULL OR TRUE = TRUE
    ThreeValuedCase{value_null(),      value_bool(true),  false, true},
    // NULL OR FALSE = NULL
    ThreeValuedCase{value_null(),      value_bool(false), true,  false},
    // NULL OR NULL = NULL
    ThreeValuedCase{value_null(),      value_null(),      true,  false}
));

// --- NOT ---

TEST(NullSemanticsTest, NotTrue) {
    auto r = eval_not(value_bool(true));
    EXPECT_FALSE(r.is_null());
    EXPECT_FALSE(r.bool_val);
}

TEST(NullSemanticsTest, NotFalse) {
    auto r = eval_not(value_bool(false));
    EXPECT_FALSE(r.is_null());
    EXPECT_TRUE(r.bool_val);
}

TEST(NullSemanticsTest, NotNull) {
    auto r = eval_not(value_null());
    EXPECT_TRUE(r.is_null());
}
```

- [ ] **Step 3: Update `Makefile.new`**

Add `$(TEST_DIR)/test_null_semantics.cpp` to `TEST_SRCS`.

- [ ] **Step 4: Build and verify**

```bash
make -f Makefile.new clean && make -f Makefile.new test
```

All previous tests plus new NULL semantics tests (including 18 parameterized AND/OR cases + 3 NOT + 4 propagate) must pass.

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/null_semantics.h tests/test_null_semantics.cpp Makefile.new
git commit -m "Add three-valued NULL semantics for sql_engine"
```

---

### Task 3: Type Coercion

**Files:**
- Create: `include/sql_engine/coercion.h`
- Create: `tests/test_coercion.cpp`
- Modify: `Makefile.new` (add test file)

Coercion rules differ significantly between MySQL (permissive) and PostgreSQL (strict). This is implemented as a template specialization on `Dialect`.

- [ ] **Step 1: Create `include/sql_engine/coercion.h`**

Create `include/sql_engine/coercion.h`:
```cpp
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
```

- [ ] **Step 2: Create `tests/test_coercion.cpp`**

Create `tests/test_coercion.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/coercion.h"
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
```

- [ ] **Step 3: Update `Makefile.new`**

Add `$(TEST_DIR)/test_coercion.cpp` to `TEST_SRCS`.

- [ ] **Step 4: Build and verify**

```bash
make -f Makefile.new clean && make -f Makefile.new test
```

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/coercion.h tests/test_coercion.cpp Makefile.new
git commit -m "Add dialect-templated type coercion rules (MySQL permissive, PostgreSQL strict)"
```

---

### Task 4: Function Registry + Arithmetic Functions

**Files:**
- Create: `include/sql_engine/function_registry.h`
- Create: `include/sql_engine/functions/arithmetic.h`
- Create: `tests/test_arithmetic.cpp`
- Modify: `Makefile.new` (add test file)

The function registry maps `(name, dialect)` to implementations. Arithmetic functions are the first category registered.

- [ ] **Step 1: Create `include/sql_engine/function_registry.h`**

Create `include/sql_engine/function_registry.h`:
```cpp
#ifndef SQL_ENGINE_FUNCTION_REGISTRY_H
#define SQL_ENGINE_FUNCTION_REGISTRY_H

#include "sql_engine/value.h"
#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include <cstdint>
#include <cstring>

namespace sql_engine {

using sql_parser::Dialect;
using sql_parser::Arena;

// Function signature: takes array of args, count, arena for allocations.
using SqlFunction = Value(*)(const Value* args, uint16_t arg_count, Arena& arena);

struct FunctionEntry {
    const char* name;       // uppercased canonical name
    uint32_t name_len;
    SqlFunction impl;
    uint8_t min_args;
    uint8_t max_args;       // 255 = variadic
};

template <Dialect D>
class FunctionRegistry {
public:
    static constexpr uint32_t MAX_FUNCTIONS = 256;

    void register_function(const FunctionEntry& entry) {
        if (count_ < MAX_FUNCTIONS) {
            entries_[count_++] = entry;
        }
    }

    const FunctionEntry* lookup(const char* name, uint32_t name_len) const {
        for (uint32_t i = 0; i < count_; ++i) {
            if (entries_[i].name_len == name_len &&
                ci_compare(entries_[i].name, name, name_len) == 0) {
                return &entries_[i];
            }
        }
        return nullptr;
    }

    // Register all built-in functions for this dialect.
    // Implemented in function_registry.cpp.
    void register_builtins();

    uint32_t size() const { return count_; }

private:
    FunctionEntry entries_[MAX_FUNCTIONS] = {};
    uint32_t count_ = 0;

    static int ci_compare(const char* a, const char* b, uint32_t len) {
        for (uint32_t i = 0; i < len; ++i) {
            char ca = a[i]; if (ca >= 'a' && ca <= 'z') ca -= 32;
            char cb = b[i]; if (cb >= 'a' && cb <= 'z') cb -= 32;
            if (ca != cb) return ca < cb ? -1 : 1;
        }
        return 0;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_FUNCTION_REGISTRY_H
```

- [ ] **Step 2: Create `include/sql_engine/functions/arithmetic.h`**

Create `include/sql_engine/functions/arithmetic.h`:
```cpp
#ifndef SQL_ENGINE_FUNCTIONS_ARITHMETIC_H
#define SQL_ENGINE_FUNCTIONS_ARITHMETIC_H

#include "sql_engine/value.h"
#include "sql_engine/null_semantics.h"
#include "sql_parser/arena.h"
#include <cmath>
#include <cstdlib>

namespace sql_engine {
namespace functions {

// ABS(x) -- absolute value
inline Value fn_abs(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    switch (args[0].tag) {
        case Value::TAG_INT64:  return value_int(args[0].int_val < 0 ? -args[0].int_val : args[0].int_val);
        case Value::TAG_DOUBLE: return value_double(std::fabs(args[0].double_val));
        case Value::TAG_UINT64: return args[0]; // already unsigned
        default: return value_double(std::fabs(args[0].to_double()));
    }
}

// CEIL(x) / CEILING(x)
inline Value fn_ceil(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    if (args[0].tag == Value::TAG_INT64 || args[0].tag == Value::TAG_UINT64) return args[0];
    return value_double(std::ceil(args[0].to_double()));
}

// FLOOR(x)
inline Value fn_floor(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    if (args[0].tag == Value::TAG_INT64 || args[0].tag == Value::TAG_UINT64) return args[0];
    return value_double(std::floor(args[0].to_double()));
}

// ROUND(x) or ROUND(x, d)
inline Value fn_round(const Value* args, uint16_t arg_count, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    double v = args[0].to_double();
    int decimals = 0;
    if (arg_count >= 2 && !args[1].is_null()) {
        decimals = static_cast<int>(args[1].to_int64());
    }
    if (decimals == 0) {
        return value_double(std::round(v));
    }
    double factor = std::pow(10.0, decimals);
    return value_double(std::round(v * factor) / factor);
}

// TRUNCATE(x, d)
inline Value fn_truncate(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    double v = args[0].to_double();
    int decimals = static_cast<int>(args[1].to_int64());
    double factor = std::pow(10.0, decimals);
    return value_double(std::trunc(v * factor) / factor);
}

// MOD(x, y) -- modulo
inline Value fn_mod(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    // Integer mod if both are integer types
    if (args[0].tag == Value::TAG_INT64 && args[1].tag == Value::TAG_INT64) {
        if (args[1].int_val == 0) return value_null(); // division by zero
        return value_int(args[0].int_val % args[1].int_val);
    }
    double b = args[1].to_double();
    if (b == 0.0) return value_null();
    return value_double(std::fmod(args[0].to_double(), b));
}

// POWER(x, y) / POW(x, y)
inline Value fn_power(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    return value_double(std::pow(args[0].to_double(), args[1].to_double()));
}

// SQRT(x)
inline Value fn_sqrt(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    double v = args[0].to_double();
    if (v < 0.0) return value_null(); // domain error
    return value_double(std::sqrt(v));
}

// SIGN(x) -- returns -1, 0, or 1
inline Value fn_sign(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    double v = args[0].to_double();
    if (v > 0.0) return value_int(1);
    if (v < 0.0) return value_int(-1);
    return value_int(0);
}

} // namespace functions
} // namespace sql_engine

#endif // SQL_ENGINE_FUNCTIONS_ARITHMETIC_H
```

- [ ] **Step 3: Create `tests/test_arithmetic.cpp`**

Create `tests/test_arithmetic.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/functions/arithmetic.h"
#include "sql_parser/arena.h"
#include <cmath>
#include <climits>

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Arena;

class ArithmeticTest : public ::testing::Test {
protected:
    Arena arena{4096};

    Value call1(SqlFunction fn, Value a) {
        Value args[] = {a};
        return fn(args, 1, arena);
    }
    Value call2(SqlFunction fn, Value a, Value b) {
        Value args[] = {a, b};
        return fn(args, 2, arena);
    }
};

// --- ABS ---

TEST_F(ArithmeticTest, AbsPositive) { EXPECT_EQ(call1(fn_abs, value_int(5)).int_val, 5); }
TEST_F(ArithmeticTest, AbsNegative) { EXPECT_EQ(call1(fn_abs, value_int(-5)).int_val, 5); }
TEST_F(ArithmeticTest, AbsZero) { EXPECT_EQ(call1(fn_abs, value_int(0)).int_val, 0); }
TEST_F(ArithmeticTest, AbsDouble) { EXPECT_DOUBLE_EQ(call1(fn_abs, value_double(-3.14)).double_val, 3.14); }
TEST_F(ArithmeticTest, AbsNull) { EXPECT_TRUE(call1(fn_abs, value_null()).is_null()); }
TEST_F(ArithmeticTest, AbsUint) { EXPECT_EQ(call1(fn_abs, value_uint(42)).uint_val, 42u); }

// --- CEIL ---

TEST_F(ArithmeticTest, CeilPositive) { EXPECT_DOUBLE_EQ(call1(fn_ceil, value_double(3.2)).double_val, 4.0); }
TEST_F(ArithmeticTest, CeilNegative) { EXPECT_DOUBLE_EQ(call1(fn_ceil, value_double(-3.2)).double_val, -3.0); }
TEST_F(ArithmeticTest, CeilInt) { EXPECT_EQ(call1(fn_ceil, value_int(5)).int_val, 5); }
TEST_F(ArithmeticTest, CeilNull) { EXPECT_TRUE(call1(fn_ceil, value_null()).is_null()); }

// --- FLOOR ---

TEST_F(ArithmeticTest, FloorPositive) { EXPECT_DOUBLE_EQ(call1(fn_floor, value_double(3.7)).double_val, 3.0); }
TEST_F(ArithmeticTest, FloorNegative) { EXPECT_DOUBLE_EQ(call1(fn_floor, value_double(-3.2)).double_val, -4.0); }
TEST_F(ArithmeticTest, FloorInt) { EXPECT_EQ(call1(fn_floor, value_int(5)).int_val, 5); }
TEST_F(ArithmeticTest, FloorNull) { EXPECT_TRUE(call1(fn_floor, value_null()).is_null()); }

// --- ROUND ---

TEST_F(ArithmeticTest, RoundNoDecimals) { EXPECT_DOUBLE_EQ(call1(fn_round, value_double(3.5)).double_val, 4.0); }
TEST_F(ArithmeticTest, RoundWithDecimals) {
    EXPECT_DOUBLE_EQ(call2(fn_round, value_double(3.14159), value_int(2)).double_val, 3.14);
}
TEST_F(ArithmeticTest, RoundNegDecimals) {
    EXPECT_DOUBLE_EQ(call2(fn_round, value_double(1234.0), value_int(-2)).double_val, 1200.0);
}
TEST_F(ArithmeticTest, RoundNull) { EXPECT_TRUE(call1(fn_round, value_null()).is_null()); }

// --- TRUNCATE ---

TEST_F(ArithmeticTest, TruncatePositive) {
    EXPECT_DOUBLE_EQ(call2(fn_truncate, value_double(3.789), value_int(2)).double_val, 3.78);
}
TEST_F(ArithmeticTest, TruncateNegative) {
    EXPECT_DOUBLE_EQ(call2(fn_truncate, value_double(-3.789), value_int(2)).double_val, -3.78);
}
TEST_F(ArithmeticTest, TruncateNull) {
    EXPECT_TRUE(call2(fn_truncate, value_null(), value_int(2)).is_null());
    EXPECT_TRUE(call2(fn_truncate, value_double(3.14), value_null()).is_null());
}

// --- MOD ---

TEST_F(ArithmeticTest, ModIntegers) { EXPECT_EQ(call2(fn_mod, value_int(10), value_int(3)).int_val, 1); }
TEST_F(ArithmeticTest, ModNegative) { EXPECT_EQ(call2(fn_mod, value_int(-10), value_int(3)).int_val, -1); }
TEST_F(ArithmeticTest, ModDouble) {
    EXPECT_NEAR(call2(fn_mod, value_double(10.5), value_double(3.0)).double_val, 1.5, 1e-9);
}
TEST_F(ArithmeticTest, ModByZero) { EXPECT_TRUE(call2(fn_mod, value_int(10), value_int(0)).is_null()); }
TEST_F(ArithmeticTest, ModNull) { EXPECT_TRUE(call2(fn_mod, value_null(), value_int(3)).is_null()); }

// --- POWER ---

TEST_F(ArithmeticTest, PowerBasic) { EXPECT_DOUBLE_EQ(call2(fn_power, value_int(2), value_int(3)).double_val, 8.0); }
TEST_F(ArithmeticTest, PowerFractional) { EXPECT_NEAR(call2(fn_power, value_double(4.0), value_double(0.5)).double_val, 2.0, 1e-9); }
TEST_F(ArithmeticTest, PowerZero) { EXPECT_DOUBLE_EQ(call2(fn_power, value_int(5), value_int(0)).double_val, 1.0); }
TEST_F(ArithmeticTest, PowerNull) { EXPECT_TRUE(call2(fn_power, value_null(), value_int(2)).is_null()); }

// --- SQRT ---

TEST_F(ArithmeticTest, SqrtPositive) { EXPECT_DOUBLE_EQ(call1(fn_sqrt, value_double(4.0)).double_val, 2.0); }
TEST_F(ArithmeticTest, SqrtZero) { EXPECT_DOUBLE_EQ(call1(fn_sqrt, value_double(0.0)).double_val, 0.0); }
TEST_F(ArithmeticTest, SqrtNegative) { EXPECT_TRUE(call1(fn_sqrt, value_double(-1.0)).is_null()); }
TEST_F(ArithmeticTest, SqrtNull) { EXPECT_TRUE(call1(fn_sqrt, value_null()).is_null()); }

// --- SIGN ---

TEST_F(ArithmeticTest, SignPositive) { EXPECT_EQ(call1(fn_sign, value_int(42)).int_val, 1); }
TEST_F(ArithmeticTest, SignNegative) { EXPECT_EQ(call1(fn_sign, value_int(-42)).int_val, -1); }
TEST_F(ArithmeticTest, SignZero) { EXPECT_EQ(call1(fn_sign, value_int(0)).int_val, 0); }
TEST_F(ArithmeticTest, SignDouble) { EXPECT_EQ(call1(fn_sign, value_double(-0.5)).int_val, -1); }
TEST_F(ArithmeticTest, SignNull) { EXPECT_TRUE(call1(fn_sign, value_null()).is_null()); }
```

- [ ] **Step 4: Update `Makefile.new`**

Add `$(TEST_DIR)/test_arithmetic.cpp` to `TEST_SRCS`.

- [ ] **Step 5: Build and verify**

```bash
make -f Makefile.new clean && make -f Makefile.new test
```

- [ ] **Step 6: Commit**

```bash
git add include/sql_engine/function_registry.h include/sql_engine/functions/arithmetic.h tests/test_arithmetic.cpp Makefile.new
git commit -m "Add function registry and P0 arithmetic functions (ABS, CEIL, FLOOR, ROUND, etc.)"
```

---

### Task 5: Comparison Functions

**Files:**
- Create: `include/sql_engine/functions/comparison.h`
- Create: `tests/test_comparison.cpp`
- Modify: `Makefile.new` (add test file)

Comparison functions include `LEAST`, `GREATEST`, `COALESCE`, `NULLIF`, `IFNULL` (MySQL), and `IF` (MySQL). These have special NULL handling that differs from standard binary operators.

- [ ] **Step 1: Create `include/sql_engine/functions/comparison.h`**

Create `include/sql_engine/functions/comparison.h`:
```cpp
#ifndef SQL_ENGINE_FUNCTIONS_COMPARISON_H
#define SQL_ENGINE_FUNCTIONS_COMPARISON_H

#include "sql_engine/value.h"
#include "sql_parser/arena.h"

namespace sql_engine {
namespace functions {

// COALESCE(x, y, z, ...) -- first non-NULL argument
inline Value fn_coalesce(const Value* args, uint16_t arg_count, Arena& /*arena*/) {
    for (uint16_t i = 0; i < arg_count; ++i) {
        if (!args[i].is_null()) return args[i];
    }
    return value_null();
}

// NULLIF(x, y) -- returns NULL if x == y (same value), else x
inline Value fn_nullif(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null() && args[1].is_null()) return value_null();
    if (args[0].is_null() || args[1].is_null()) return args[0];
    // Compare: both must be same tag and same value
    if (args[0].tag != args[1].tag) return args[0];
    switch (args[0].tag) {
        case Value::TAG_BOOL:
            return args[0].bool_val == args[1].bool_val ? value_null() : args[0];
        case Value::TAG_INT64:
            return args[0].int_val == args[1].int_val ? value_null() : args[0];
        case Value::TAG_UINT64:
            return args[0].uint_val == args[1].uint_val ? value_null() : args[0];
        case Value::TAG_DOUBLE:
            return args[0].double_val == args[1].double_val ? value_null() : args[0];
        case Value::TAG_STRING:
        case Value::TAG_DECIMAL:
        case Value::TAG_JSON:
            return args[0].str_val == args[1].str_val ? value_null() : args[0];
        default:
            return args[0];
    }
}

// IFNULL(x, y) -- MySQL: returns y if x is NULL, else x. Same as COALESCE(x, y).
inline Value fn_ifnull(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    return args[0].is_null() ? args[1] : args[0];
}

// IF(cond, then_val, else_val) -- MySQL: ternary
inline Value fn_if(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return args[2];
    // Truthy: non-zero for numeric, non-empty for string
    bool truthy = false;
    switch (args[0].tag) {
        case Value::TAG_BOOL:   truthy = args[0].bool_val; break;
        case Value::TAG_INT64:  truthy = args[0].int_val != 0; break;
        case Value::TAG_UINT64: truthy = args[0].uint_val != 0; break;
        case Value::TAG_DOUBLE: truthy = args[0].double_val != 0.0; break;
        default: truthy = true; break; // non-null non-numeric = truthy
    }
    return truthy ? args[1] : args[2];
}

// Helper: compare two numeric values. Returns <0, 0, >0.
inline int compare_values(const Value& a, const Value& b) {
    double da = a.to_double();
    double db = b.to_double();
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

// LEAST(x, y, ...) -- smallest non-NULL value. Returns NULL if all args are NULL.
inline Value fn_least(const Value* args, uint16_t arg_count, Arena& /*arena*/) {
    const Value* best = nullptr;
    for (uint16_t i = 0; i < arg_count; ++i) {
        if (args[i].is_null()) continue;
        if (!best || compare_values(args[i], *best) < 0) {
            best = &args[i];
        }
    }
    return best ? *best : value_null();
}

// GREATEST(x, y, ...) -- largest non-NULL value. Returns NULL if all args are NULL.
inline Value fn_greatest(const Value* args, uint16_t arg_count, Arena& /*arena*/) {
    const Value* best = nullptr;
    for (uint16_t i = 0; i < arg_count; ++i) {
        if (args[i].is_null()) continue;
        if (!best || compare_values(args[i], *best) > 0) {
            best = &args[i];
        }
    }
    return best ? *best : value_null();
}

} // namespace functions
} // namespace sql_engine

#endif // SQL_ENGINE_FUNCTIONS_COMPARISON_H
```

- [ ] **Step 2: Create `tests/test_comparison.cpp`**

Create `tests/test_comparison.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/functions/comparison.h"
#include "sql_parser/arena.h"

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Arena;
using sql_parser::StringRef;

class ComparisonTest : public ::testing::Test {
protected:
    Arena arena{4096};
};

// --- COALESCE ---

TEST_F(ComparisonTest, CoalesceFirstNonNull) {
    Value args[] = {value_null(), value_int(42), value_int(99)};
    auto r = fn_coalesce(args, 3, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(ComparisonTest, CoalesceFirstArg) {
    Value args[] = {value_int(1), value_int(2)};
    auto r = fn_coalesce(args, 2, arena);
    EXPECT_EQ(r.int_val, 1);
}

TEST_F(ComparisonTest, CoalesceAllNull) {
    Value args[] = {value_null(), value_null(), value_null()};
    auto r = fn_coalesce(args, 3, arena);
    EXPECT_TRUE(r.is_null());
}

TEST_F(ComparisonTest, CoalesceSingleNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_coalesce(args, 1, arena).is_null());
}

TEST_F(ComparisonTest, CoalesceSingleNonNull) {
    Value args[] = {value_int(5)};
    EXPECT_EQ(fn_coalesce(args, 1, arena).int_val, 5);
}

// --- NULLIF ---

TEST_F(ComparisonTest, NullifEqual) {
    Value args[] = {value_int(42), value_int(42)};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, NullifNotEqual) {
    Value args[] = {value_int(42), value_int(99)};
    auto r = fn_nullif(args, 2, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(ComparisonTest, NullifFirstNull) {
    Value args[] = {value_null(), value_int(42)};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, NullifSecondNull) {
    Value args[] = {value_int(42), value_null()};
    EXPECT_EQ(fn_nullif(args, 2, arena).int_val, 42);
}

TEST_F(ComparisonTest, NullifBothNull) {
    Value args[] = {value_null(), value_null()};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, NullifDifferentTypes) {
    Value args[] = {value_int(42), value_double(42.0)};
    // Different tags -> not equal -> return first arg
    auto r = fn_nullif(args, 2, arena);
    EXPECT_EQ(r.int_val, 42);
}

TEST_F(ComparisonTest, NullifStrings) {
    const char* s = "hello";
    Value args[] = {value_string(StringRef{s, 5}), value_string(StringRef{s, 5})};
    EXPECT_TRUE(fn_nullif(args, 2, arena).is_null());
}

// --- IFNULL ---

TEST_F(ComparisonTest, IfnullFirstNull) {
    Value args[] = {value_null(), value_int(42)};
    EXPECT_EQ(fn_ifnull(args, 2, arena).int_val, 42);
}

TEST_F(ComparisonTest, IfnullFirstNonNull) {
    Value args[] = {value_int(10), value_int(42)};
    EXPECT_EQ(fn_ifnull(args, 2, arena).int_val, 10);
}

// --- IF ---

TEST_F(ComparisonTest, IfTrue) {
    Value args[] = {value_bool(true), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 1);
}

TEST_F(ComparisonTest, IfFalse) {
    Value args[] = {value_bool(false), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 2);
}

TEST_F(ComparisonTest, IfNull) {
    Value args[] = {value_null(), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 2);
}

TEST_F(ComparisonTest, IfIntTruthy) {
    Value args[] = {value_int(42), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 1);
}

TEST_F(ComparisonTest, IfIntZero) {
    Value args[] = {value_int(0), value_int(1), value_int(2)};
    EXPECT_EQ(fn_if(args, 3, arena).int_val, 2);
}

// --- LEAST ---

TEST_F(ComparisonTest, LeastBasic) {
    Value args[] = {value_int(3), value_int(1), value_int(2)};
    EXPECT_EQ(fn_least(args, 3, arena).int_val, 1);
}

TEST_F(ComparisonTest, LeastWithNull) {
    Value args[] = {value_null(), value_int(5), value_int(3)};
    EXPECT_EQ(fn_least(args, 3, arena).int_val, 3);
}

TEST_F(ComparisonTest, LeastAllNull) {
    Value args[] = {value_null(), value_null()};
    EXPECT_TRUE(fn_least(args, 2, arena).is_null());
}

TEST_F(ComparisonTest, LeastNegative) {
    Value args[] = {value_int(-5), value_int(3), value_int(-10)};
    EXPECT_EQ(fn_least(args, 3, arena).int_val, -10);
}

TEST_F(ComparisonTest, LeastDoubles) {
    Value args[] = {value_double(3.14), value_double(2.71), value_double(1.41)};
    EXPECT_DOUBLE_EQ(fn_least(args, 3, arena).double_val, 1.41);
}

// --- GREATEST ---

TEST_F(ComparisonTest, GreatestBasic) {
    Value args[] = {value_int(3), value_int(1), value_int(2)};
    EXPECT_EQ(fn_greatest(args, 3, arena).int_val, 3);
}

TEST_F(ComparisonTest, GreatestWithNull) {
    Value args[] = {value_null(), value_int(5), value_int(3)};
    EXPECT_EQ(fn_greatest(args, 3, arena).int_val, 5);
}

TEST_F(ComparisonTest, GreatestAllNull) {
    Value args[] = {value_null(), value_null()};
    EXPECT_TRUE(fn_greatest(args, 2, arena).is_null());
}
```

- [ ] **Step 3: Update `Makefile.new`**

Add `$(TEST_DIR)/test_comparison.cpp` to `TEST_SRCS`.

- [ ] **Step 4: Build and verify**

```bash
make -f Makefile.new clean && make -f Makefile.new test
```

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/functions/comparison.h tests/test_comparison.cpp Makefile.new
git commit -m "Add comparison functions (COALESCE, NULLIF, IFNULL, IF, LEAST, GREATEST)"
```

---

### Task 6: String Functions

**Files:**
- Create: `include/sql_engine/functions/string.h`
- Create: `tests/test_string_funcs.cpp`
- Modify: `Makefile.new` (add test file)

String functions allocate their results from the Arena. They follow the pattern: NULL arg in = NULL out (with exceptions for CONCAT_WS).

- [ ] **Step 1: Create `include/sql_engine/functions/string.h`**

Create `include/sql_engine/functions/string.h`:
```cpp
#ifndef SQL_ENGINE_FUNCTIONS_STRING_H
#define SQL_ENGINE_FUNCTIONS_STRING_H

#include "sql_engine/value.h"
#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include <cstring>
#include <algorithm>

namespace sql_engine {
namespace functions {

// CONCAT(s1, s2, ...) -- NULL if any arg is NULL (MySQL behavior)
inline Value fn_concat(const Value* args, uint16_t arg_count, Arena& arena) {
    // Check for NULL args
    uint32_t total_len = 0;
    for (uint16_t i = 0; i < arg_count; ++i) {
        if (args[i].is_null()) return value_null();
        total_len += args[i].str_val.len;
    }
    if (total_len == 0) return value_string(StringRef{nullptr, 0});
    char* buf = static_cast<char*>(arena.allocate(total_len));
    if (!buf) return value_null();
    uint32_t pos = 0;
    for (uint16_t i = 0; i < arg_count; ++i) {
        if (args[i].str_val.len > 0) {
            std::memcpy(buf + pos, args[i].str_val.ptr, args[i].str_val.len);
            pos += args[i].str_val.len;
        }
    }
    return value_string(StringRef{buf, total_len});
}

// CONCAT_WS(separator, s1, s2, ...) -- skips NULL args (does NOT return NULL)
inline Value fn_concat_ws(const Value* args, uint16_t arg_count, Arena& arena) {
    if (arg_count < 1 || args[0].is_null()) return value_null();
    StringRef sep = args[0].str_val;

    // First pass: count total length
    uint32_t total_len = 0;
    uint16_t non_null_count = 0;
    for (uint16_t i = 1; i < arg_count; ++i) {
        if (args[i].is_null()) continue;
        if (non_null_count > 0) total_len += sep.len;
        total_len += args[i].str_val.len;
        non_null_count++;
    }
    if (non_null_count == 0) return value_string(StringRef{nullptr, 0});
    char* buf = static_cast<char*>(arena.allocate(total_len));
    if (!buf) return value_null();
    uint32_t pos = 0;
    bool first = true;
    for (uint16_t i = 1; i < arg_count; ++i) {
        if (args[i].is_null()) continue;
        if (!first && sep.len > 0) {
            std::memcpy(buf + pos, sep.ptr, sep.len);
            pos += sep.len;
        }
        first = false;
        if (args[i].str_val.len > 0) {
            std::memcpy(buf + pos, args[i].str_val.ptr, args[i].str_val.len);
            pos += args[i].str_val.len;
        }
    }
    return value_string(StringRef{buf, total_len});
}

// LENGTH(s) / CHAR_LENGTH(s) -- byte length (for now, same as char length for ASCII)
inline Value fn_length(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    return value_int(static_cast<int64_t>(args[0].str_val.len));
}

// UPPER(s) / UCASE(s)
inline Value fn_upper(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null()) return value_null();
    uint32_t len = args[0].str_val.len;
    if (len == 0) return value_string(StringRef{nullptr, 0});
    char* buf = static_cast<char*>(arena.allocate(len));
    if (!buf) return value_null();
    for (uint32_t i = 0; i < len; ++i) {
        char c = args[0].str_val.ptr[i];
        buf[i] = (c >= 'a' && c <= 'z') ? c - 32 : c;
    }
    return value_string(StringRef{buf, len});
}

// LOWER(s) / LCASE(s)
inline Value fn_lower(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null()) return value_null();
    uint32_t len = args[0].str_val.len;
    if (len == 0) return value_string(StringRef{nullptr, 0});
    char* buf = static_cast<char*>(arena.allocate(len));
    if (!buf) return value_null();
    for (uint32_t i = 0; i < len; ++i) {
        char c = args[0].str_val.ptr[i];
        buf[i] = (c >= 'A' && c <= 'Z') ? c + 32 : c;
    }
    return value_string(StringRef{buf, len});
}

// SUBSTRING(s, pos) or SUBSTRING(s, pos, len) -- 1-based position
inline Value fn_substring(const Value* args, uint16_t arg_count, Arena& arena) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    int64_t pos = args[1].to_int64();
    int64_t slen = static_cast<int64_t>(args[0].str_val.len);

    // Convert 1-based to 0-based. Negative pos counts from end.
    int64_t start;
    if (pos > 0) {
        start = pos - 1;
    } else if (pos < 0) {
        start = slen + pos;
    } else {
        start = 0; // pos=0 is treated as before string start in MySQL
    }
    if (start < 0) start = 0;
    if (start >= slen) return value_string(StringRef{nullptr, 0});

    int64_t extract_len = slen - start;
    if (arg_count >= 3 && !args[2].is_null()) {
        int64_t requested = args[2].to_int64();
        if (requested < 0) return value_string(StringRef{nullptr, 0});
        if (requested < extract_len) extract_len = requested;
    }

    return value_string(arena.allocate_string(
        args[0].str_val.ptr + start, static_cast<uint32_t>(extract_len)));
}

// TRIM(s) -- remove leading and trailing spaces
inline Value fn_trim(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    const char* p = args[0].str_val.ptr;
    uint32_t len = args[0].str_val.len;
    uint32_t start = 0;
    while (start < len && p[start] == ' ') ++start;
    uint32_t end = len;
    while (end > start && p[end - 1] == ' ') --end;
    return value_string(StringRef{p + start, end - start});
}

// LTRIM(s)
inline Value fn_ltrim(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    const char* p = args[0].str_val.ptr;
    uint32_t len = args[0].str_val.len;
    uint32_t start = 0;
    while (start < len && p[start] == ' ') ++start;
    return value_string(StringRef{p + start, len - start});
}

// RTRIM(s)
inline Value fn_rtrim(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null()) return value_null();
    const char* p = args[0].str_val.ptr;
    uint32_t len = args[0].str_val.len;
    while (len > 0 && p[len - 1] == ' ') --len;
    return value_string(StringRef{p, len});
}

// REPLACE(s, from, to)
inline Value fn_replace(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null() || args[1].is_null() || args[2].is_null()) return value_null();
    const char* src = args[0].str_val.ptr;
    uint32_t src_len = args[0].str_val.len;
    const char* from = args[1].str_val.ptr;
    uint32_t from_len = args[1].str_val.len;
    const char* to = args[2].str_val.ptr;
    uint32_t to_len = args[2].str_val.len;

    if (from_len == 0) return args[0]; // empty search string: return original

    // Count occurrences to size buffer
    uint32_t count = 0;
    for (uint32_t i = 0; i + from_len <= src_len; ++i) {
        if (std::memcmp(src + i, from, from_len) == 0) { ++count; i += from_len - 1; }
    }
    if (count == 0) return args[0];

    uint32_t new_len = src_len - (count * from_len) + (count * to_len);
    char* buf = static_cast<char*>(arena.allocate(new_len));
    if (!buf) return value_null();
    uint32_t pos = 0;
    for (uint32_t i = 0; i < src_len; ) {
        if (i + from_len <= src_len && std::memcmp(src + i, from, from_len) == 0) {
            std::memcpy(buf + pos, to, to_len);
            pos += to_len;
            i += from_len;
        } else {
            buf[pos++] = src[i++];
        }
    }
    return value_string(StringRef{buf, new_len});
}

// REVERSE(s)
inline Value fn_reverse(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null()) return value_null();
    uint32_t len = args[0].str_val.len;
    if (len == 0) return value_string(StringRef{nullptr, 0});
    char* buf = static_cast<char*>(arena.allocate(len));
    if (!buf) return value_null();
    for (uint32_t i = 0; i < len; ++i) {
        buf[i] = args[0].str_val.ptr[len - 1 - i];
    }
    return value_string(StringRef{buf, len});
}

// LEFT(s, n)
inline Value fn_left(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    int64_t n = args[1].to_int64();
    if (n <= 0) return value_string(StringRef{nullptr, 0});
    uint32_t take = static_cast<uint32_t>(n);
    if (take > args[0].str_val.len) take = args[0].str_val.len;
    return value_string(StringRef{args[0].str_val.ptr, take});
}

// RIGHT(s, n)
inline Value fn_right(const Value* args, uint16_t /*arg_count*/, Arena& /*arena*/) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    int64_t n = args[1].to_int64();
    if (n <= 0) return value_string(StringRef{nullptr, 0});
    uint32_t take = static_cast<uint32_t>(n);
    uint32_t len = args[0].str_val.len;
    if (take > len) take = len;
    return value_string(StringRef{args[0].str_val.ptr + len - take, take});
}

// LPAD(s, len, pad)
inline Value fn_lpad(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null() || args[1].is_null() || args[2].is_null()) return value_null();
    int64_t target = args[1].to_int64();
    if (target <= 0) return value_string(StringRef{nullptr, 0});
    uint32_t tlen = static_cast<uint32_t>(target);
    uint32_t slen = args[0].str_val.len;
    if (slen >= tlen) {
        // Truncate to target length
        return value_string(StringRef{args[0].str_val.ptr, tlen});
    }
    char* buf = static_cast<char*>(arena.allocate(tlen));
    if (!buf) return value_null();
    uint32_t pad_needed = tlen - slen;
    uint32_t pad_len = args[2].str_val.len;
    if (pad_len == 0) return value_string(StringRef{nullptr, 0});
    for (uint32_t i = 0; i < pad_needed; ++i) {
        buf[i] = args[2].str_val.ptr[i % pad_len];
    }
    std::memcpy(buf + pad_needed, args[0].str_val.ptr, slen);
    return value_string(StringRef{buf, tlen});
}

// RPAD(s, len, pad)
inline Value fn_rpad(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null() || args[1].is_null() || args[2].is_null()) return value_null();
    int64_t target = args[1].to_int64();
    if (target <= 0) return value_string(StringRef{nullptr, 0});
    uint32_t tlen = static_cast<uint32_t>(target);
    uint32_t slen = args[0].str_val.len;
    if (slen >= tlen) {
        return value_string(StringRef{args[0].str_val.ptr, tlen});
    }
    char* buf = static_cast<char*>(arena.allocate(tlen));
    if (!buf) return value_null();
    std::memcpy(buf, args[0].str_val.ptr, slen);
    uint32_t pad_len = args[2].str_val.len;
    if (pad_len == 0) return value_string(StringRef{nullptr, 0});
    for (uint32_t i = slen; i < tlen; ++i) {
        buf[i] = args[2].str_val.ptr[(i - slen) % pad_len];
    }
    return value_string(StringRef{buf, tlen});
}

// REPEAT(s, n)
inline Value fn_repeat(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null() || args[1].is_null()) return value_null();
    int64_t n = args[1].to_int64();
    if (n <= 0) return value_string(StringRef{nullptr, 0});
    uint32_t slen = args[0].str_val.len;
    uint32_t total = slen * static_cast<uint32_t>(n);
    if (slen == 0) return value_string(StringRef{nullptr, 0});
    char* buf = static_cast<char*>(arena.allocate(total));
    if (!buf) return value_null();
    for (int64_t i = 0; i < n; ++i) {
        std::memcpy(buf + i * slen, args[0].str_val.ptr, slen);
    }
    return value_string(StringRef{buf, total});
}

// SPACE(n) -- returns n spaces
inline Value fn_space(const Value* args, uint16_t /*arg_count*/, Arena& arena) {
    if (args[0].is_null()) return value_null();
    int64_t n = args[0].to_int64();
    if (n <= 0) return value_string(StringRef{nullptr, 0});
    uint32_t len = static_cast<uint32_t>(n);
    char* buf = static_cast<char*>(arena.allocate(len));
    if (!buf) return value_null();
    std::memset(buf, ' ', len);
    return value_string(StringRef{buf, len});
}

} // namespace functions
} // namespace sql_engine

#endif // SQL_ENGINE_FUNCTIONS_STRING_H
```

- [ ] **Step 2: Create `tests/test_string_funcs.cpp`**

Create `tests/test_string_funcs.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/functions/string.h"
#include "sql_parser/arena.h"
#include <string>

using namespace sql_engine;
using namespace sql_engine::functions;
using sql_parser::Arena;
using sql_parser::StringRef;

class StringFuncTest : public ::testing::Test {
protected:
    Arena arena{4096};

    Value S(const char* s) { return value_string(StringRef{s, static_cast<uint32_t>(std::strlen(s))}); }
    std::string str(const Value& v) { return std::string(v.str_val.ptr, v.str_val.len); }
};

// --- CONCAT ---

TEST_F(StringFuncTest, ConcatTwo) {
    Value args[] = {S("hello"), S(" world")};
    EXPECT_EQ(str(fn_concat(args, 2, arena)), "hello world");
}

TEST_F(StringFuncTest, ConcatThree) {
    Value args[] = {S("a"), S("b"), S("c")};
    EXPECT_EQ(str(fn_concat(args, 3, arena)), "abc");
}

TEST_F(StringFuncTest, ConcatNull) {
    Value args[] = {S("hello"), value_null()};
    EXPECT_TRUE(fn_concat(args, 2, arena).is_null());
}

TEST_F(StringFuncTest, ConcatEmpty) {
    Value args[] = {S(""), S("")};
    auto r = fn_concat(args, 2, arena);
    EXPECT_EQ(r.str_val.len, 0u);
}

// --- CONCAT_WS ---

TEST_F(StringFuncTest, ConcatWsBasic) {
    Value args[] = {S(","), S("a"), S("b"), S("c")};
    EXPECT_EQ(str(fn_concat_ws(args, 4, arena)), "a,b,c");
}

TEST_F(StringFuncTest, ConcatWsSkipsNull) {
    Value args[] = {S(","), S("a"), value_null(), S("c")};
    EXPECT_EQ(str(fn_concat_ws(args, 4, arena)), "a,c");
}

TEST_F(StringFuncTest, ConcatWsNullSeparator) {
    Value args[] = {value_null(), S("a"), S("b")};
    EXPECT_TRUE(fn_concat_ws(args, 3, arena).is_null());
}

// --- LENGTH ---

TEST_F(StringFuncTest, LengthBasic) {
    Value args[] = {S("hello")};
    EXPECT_EQ(fn_length(args, 1, arena).int_val, 5);
}

TEST_F(StringFuncTest, LengthEmpty) {
    Value args[] = {S("")};
    EXPECT_EQ(fn_length(args, 1, arena).int_val, 0);
}

TEST_F(StringFuncTest, LengthNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_length(args, 1, arena).is_null());
}

// --- UPPER / LOWER ---

TEST_F(StringFuncTest, UpperBasic) {
    Value args[] = {S("hello")};
    EXPECT_EQ(str(fn_upper(args, 1, arena)), "HELLO");
}

TEST_F(StringFuncTest, LowerBasic) {
    Value args[] = {S("HELLO")};
    EXPECT_EQ(str(fn_lower(args, 1, arena)), "hello");
}

TEST_F(StringFuncTest, UpperNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_upper(args, 1, arena).is_null());
}

TEST_F(StringFuncTest, LowerMixed) {
    Value args[] = {S("HeLLo WoRLd")};
    EXPECT_EQ(str(fn_lower(args, 1, arena)), "hello world");
}

// --- SUBSTRING ---

TEST_F(StringFuncTest, SubstringFrom) {
    Value args[] = {S("hello world"), value_int(7)};
    EXPECT_EQ(str(fn_substring(args, 2, arena)), "world");
}

TEST_F(StringFuncTest, SubstringFromLen) {
    Value args[] = {S("hello world"), value_int(1), value_int(5)};
    EXPECT_EQ(str(fn_substring(args, 3, arena)), "hello");
}

TEST_F(StringFuncTest, SubstringNegative) {
    Value args[] = {S("hello"), value_int(-3)};
    EXPECT_EQ(str(fn_substring(args, 2, arena)), "llo");
}

TEST_F(StringFuncTest, SubstringNull) {
    Value args[] = {value_null(), value_int(1)};
    EXPECT_TRUE(fn_substring(args, 2, arena).is_null());
}

TEST_F(StringFuncTest, SubstringBeyondEnd) {
    Value args[] = {S("hi"), value_int(10)};
    EXPECT_EQ(fn_substring(args, 2, arena).str_val.len, 0u);
}

// --- TRIM / LTRIM / RTRIM ---

TEST_F(StringFuncTest, TrimBoth) {
    Value args[] = {S("  hello  ")};
    EXPECT_EQ(str(fn_trim(args, 1, arena)), "hello");
}

TEST_F(StringFuncTest, LtrimBasic) {
    Value args[] = {S("  hello  ")};
    EXPECT_EQ(str(fn_ltrim(args, 1, arena)), "hello  ");
}

TEST_F(StringFuncTest, RtrimBasic) {
    Value args[] = {S("  hello  ")};
    EXPECT_EQ(str(fn_rtrim(args, 1, arena)), "  hello");
}

TEST_F(StringFuncTest, TrimNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_trim(args, 1, arena).is_null());
}

TEST_F(StringFuncTest, TrimAllSpaces) {
    Value args[] = {S("   ")};
    EXPECT_EQ(fn_trim(args, 1, arena).str_val.len, 0u);
}

// --- REPLACE ---

TEST_F(StringFuncTest, ReplaceBasic) {
    Value args[] = {S("hello world"), S("world"), S("earth")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "hello earth");
}

TEST_F(StringFuncTest, ReplaceMultiple) {
    Value args[] = {S("aaa"), S("a"), S("bb")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "bbbbbb");
}

TEST_F(StringFuncTest, ReplaceNoMatch) {
    Value args[] = {S("hello"), S("xyz"), S("abc")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "hello");
}

TEST_F(StringFuncTest, ReplaceNull) {
    Value args[] = {value_null(), S("a"), S("b")};
    EXPECT_TRUE(fn_replace(args, 3, arena).is_null());
}

TEST_F(StringFuncTest, ReplaceEmptySearch) {
    Value args[] = {S("hello"), S(""), S("x")};
    EXPECT_EQ(str(fn_replace(args, 3, arena)), "hello");
}

// --- REVERSE ---

TEST_F(StringFuncTest, ReverseBasic) {
    Value args[] = {S("hello")};
    EXPECT_EQ(str(fn_reverse(args, 1, arena)), "olleh");
}

TEST_F(StringFuncTest, ReverseEmpty) {
    Value args[] = {S("")};
    EXPECT_EQ(fn_reverse(args, 1, arena).str_val.len, 0u);
}

TEST_F(StringFuncTest, ReverseNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_reverse(args, 1, arena).is_null());
}

// --- LEFT / RIGHT ---

TEST_F(StringFuncTest, LeftBasic) {
    Value args[] = {S("hello"), value_int(3)};
    EXPECT_EQ(str(fn_left(args, 2, arena)), "hel");
}

TEST_F(StringFuncTest, LeftBeyondLen) {
    Value args[] = {S("hi"), value_int(10)};
    EXPECT_EQ(str(fn_left(args, 2, arena)), "hi");
}

TEST_F(StringFuncTest, RightBasic) {
    Value args[] = {S("hello"), value_int(3)};
    EXPECT_EQ(str(fn_right(args, 2, arena)), "llo");
}

TEST_F(StringFuncTest, LeftZero) {
    Value args[] = {S("hello"), value_int(0)};
    EXPECT_EQ(fn_left(args, 2, arena).str_val.len, 0u);
}

// --- LPAD / RPAD ---

TEST_F(StringFuncTest, LpadBasic) {
    Value args[] = {S("hi"), value_int(5), S("*")};
    EXPECT_EQ(str(fn_lpad(args, 3, arena)), "***hi");
}

TEST_F(StringFuncTest, LpadCyclicPad) {
    Value args[] = {S("hi"), value_int(7), S("abc")};
    EXPECT_EQ(str(fn_lpad(args, 3, arena)), "abcabhi");
}

TEST_F(StringFuncTest, LpadTruncate) {
    Value args[] = {S("hello"), value_int(3), S("*")};
    EXPECT_EQ(str(fn_lpad(args, 3, arena)), "hel");
}

TEST_F(StringFuncTest, RpadBasic) {
    Value args[] = {S("hi"), value_int(5), S("*")};
    EXPECT_EQ(str(fn_rpad(args, 3, arena)), "hi***");
}

TEST_F(StringFuncTest, RpadNull) {
    Value args[] = {value_null(), value_int(5), S("*")};
    EXPECT_TRUE(fn_rpad(args, 3, arena).is_null());
}

// --- REPEAT ---

TEST_F(StringFuncTest, RepeatBasic) {
    Value args[] = {S("ab"), value_int(3)};
    EXPECT_EQ(str(fn_repeat(args, 2, arena)), "ababab");
}

TEST_F(StringFuncTest, RepeatZero) {
    Value args[] = {S("ab"), value_int(0)};
    EXPECT_EQ(fn_repeat(args, 2, arena).str_val.len, 0u);
}

TEST_F(StringFuncTest, RepeatNull) {
    Value args[] = {value_null(), value_int(3)};
    EXPECT_TRUE(fn_repeat(args, 2, arena).is_null());
}

// --- SPACE ---

TEST_F(StringFuncTest, SpaceBasic) {
    Value args[] = {value_int(5)};
    EXPECT_EQ(str(fn_space(args, 1, arena)), "     ");
}

TEST_F(StringFuncTest, SpaceZero) {
    Value args[] = {value_int(0)};
    EXPECT_EQ(fn_space(args, 1, arena).str_val.len, 0u);
}

TEST_F(StringFuncTest, SpaceNull) {
    Value args[] = {value_null()};
    EXPECT_TRUE(fn_space(args, 1, arena).is_null());
}
```

- [ ] **Step 3: Update `Makefile.new`**

Add `$(TEST_DIR)/test_string_funcs.cpp` to `TEST_SRCS`.

- [ ] **Step 4: Build and verify**

```bash
make -f Makefile.new clean && make -f Makefile.new test
```

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/functions/string.h tests/test_string_funcs.cpp Makefile.new
git commit -m "Add P0 string functions (CONCAT, UPPER, LOWER, SUBSTRING, TRIM, REPLACE, etc.)"
```

---

### Task 7: Cast Functions

**Files:**
- Create: `include/sql_engine/functions/cast.h`
- Create: `tests/test_cast.cpp`
- Modify: `Makefile.new` (add test file)

`CAST(x AS type)` handles explicit type conversion between all supported type pairs. MySQL is lenient (partial parses succeed), PostgreSQL is strict (malformed input returns NULL/error).

- [ ] **Step 1: Create `include/sql_engine/functions/cast.h`**

Create `include/sql_engine/functions/cast.h`:
```cpp
#ifndef SQL_ENGINE_FUNCTIONS_CAST_H
#define SQL_ENGINE_FUNCTIONS_CAST_H

#include "sql_engine/types.h"
#include "sql_engine/value.h"
#include "sql_engine/coercion.h"
#include "sql_parser/common.h"
#include "sql_parser/arena.h"
#include <cstdio>
#include <cstring>

namespace sql_engine {
namespace functions {

// Cast a Value to a target SqlType::Kind.
// Explicit casts are more permissive than implicit coercion -- even PostgreSQL
// allows explicit CAST(string AS int).
template <sql_parser::Dialect D>
Value cast_value(const Value& val, SqlType::Kind target, sql_parser::Arena& arena) {
    if (val.is_null()) return value_null();

    // Map SqlType::Kind to Value::Tag for the target
    Value::Tag target_tag;
    switch (target) {
        case SqlType::BOOLEAN:   target_tag = Value::TAG_BOOL; break;
        case SqlType::TINYINT:
        case SqlType::SMALLINT:
        case SqlType::MEDIUMINT:
        case SqlType::INT:
        case SqlType::BIGINT:    target_tag = Value::TAG_INT64; break;
        case SqlType::FLOAT:
        case SqlType::DOUBLE:    target_tag = Value::TAG_DOUBLE; break;
        case SqlType::DECIMAL:   target_tag = Value::TAG_DECIMAL; break;
        case SqlType::CHAR:
        case SqlType::VARCHAR:
        case SqlType::TEXT:
        case SqlType::MEDIUMTEXT:
        case SqlType::LONGTEXT:  target_tag = Value::TAG_STRING; break;
        case SqlType::DATE:      target_tag = Value::TAG_DATE; break;
        case SqlType::TIME:      target_tag = Value::TAG_TIME; break;
        case SqlType::DATETIME:  target_tag = Value::TAG_DATETIME; break;
        case SqlType::TIMESTAMP: target_tag = Value::TAG_TIMESTAMP; break;
        default:                 return value_null(); // unsupported cast target
    }

    if (val.tag == target_tag) return val;

    // For explicit casts, use MySQL-style coercion (lenient) even for Pg,
    // but validate strictly for PostgreSQL.
    if constexpr (D == sql_parser::Dialect::MySQL) {
        return CoercionRules<sql_parser::Dialect::MySQL>::coerce_value(val, target_tag, arena);
    } else {
        // PostgreSQL: explicit casts allow string->int etc., but strictly
        switch (target_tag) {
            case Value::TAG_INT64: {
                if (val.tag == Value::TAG_BOOL) return value_int(val.bool_val ? 1 : 0);
                if (val.tag == Value::TAG_UINT64) return value_int(static_cast<int64_t>(val.uint_val));
                if (val.tag == Value::TAG_DOUBLE) return value_int(static_cast<int64_t>(val.double_val));
                if (val.tag == Value::TAG_STRING || val.tag == Value::TAG_DECIMAL) {
                    int64_t out;
                    if (detail::parse_int_strict(val.str_val.ptr, val.str_val.len, out))
                        return value_int(out);
                    return value_null(); // parse error
                }
                return value_null();
            }
            case Value::TAG_DOUBLE: {
                if (val.tag == Value::TAG_BOOL) return value_double(val.bool_val ? 1.0 : 0.0);
                if (val.tag == Value::TAG_INT64) return value_double(static_cast<double>(val.int_val));
                if (val.tag == Value::TAG_UINT64) return value_double(static_cast<double>(val.uint_val));
                if (val.tag == Value::TAG_STRING) {
                    double out;
                    if (detail::parse_double_lenient(val.str_val.ptr, val.str_val.len, out))
                        return value_double(out);
                    return value_null();
                }
                return value_null();
            }
            case Value::TAG_STRING: {
                if (val.tag == Value::TAG_INT64)
                    return value_string(detail::int_to_string(val.int_val, arena));
                if (val.tag == Value::TAG_DOUBLE)
                    return value_string(detail::double_to_string(val.double_val, arena));
                if (val.tag == Value::TAG_BOOL)
                    return value_string(val.bool_val
                        ? arena.allocate_string("true", 4)
                        : arena.allocate_string("false", 5));
                return value_null();
            }
            case Value::TAG_BOOL: {
                if (val.tag == Value::TAG_INT64) return value_bool(val.int_val != 0);
                if (val.tag == Value::TAG_STRING) {
                    // PostgreSQL: 'true'/'t'/'1'/'yes'/'on' -> true
                    if (val.str_val.equals_ci("true", 4) || val.str_val.equals_ci("t", 1) ||
                        val.str_val.equals_ci("1", 1) || val.str_val.equals_ci("yes", 3))
                        return value_bool(true);
                    if (val.str_val.equals_ci("false", 5) || val.str_val.equals_ci("f", 1) ||
                        val.str_val.equals_ci("0", 1) || val.str_val.equals_ci("no", 2))
                        return value_bool(false);
                    return value_null();
                }
                return value_null();
            }
            default:
                return value_null();
        }
    }
}

} // namespace functions
} // namespace sql_engine

#endif // SQL_ENGINE_FUNCTIONS_CAST_H
```

- [ ] **Step 2: Create `tests/test_cast.cpp`**

Create `tests/test_cast.cpp`:
```cpp
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
```

- [ ] **Step 3: Update `Makefile.new`**

Add `$(TEST_DIR)/test_cast.cpp` to `TEST_SRCS`.

- [ ] **Step 4: Build and verify**

```bash
make -f Makefile.new clean && make -f Makefile.new test
```

- [ ] **Step 5: Commit**

```bash
git add include/sql_engine/functions/cast.h tests/test_cast.cpp Makefile.new
git commit -m "Add CAST functions with MySQL lenient and PostgreSQL strict parsing"
```

---

### Task 8: Integration + function_registry.cpp

**Files:**
- Create: `src/sql_engine/function_registry.cpp`
- Create: `tests/test_registry.cpp`
- Modify: `Makefile.new` (add library source and test file)

This task wires everything together: `register_builtins()` populates the registry with all P0 functions, and integration tests verify end-to-end lookup and dispatch.

- [ ] **Step 1: Create `src/sql_engine/function_registry.cpp`**

Create `src/sql_engine/function_registry.cpp`:
```cpp
#include "sql_engine/function_registry.h"
#include "sql_engine/functions/arithmetic.h"
#include "sql_engine/functions/comparison.h"
#include "sql_engine/functions/string.h"
#include "sql_parser/common.h"

namespace sql_engine {

using sql_parser::Dialect;

namespace {

// Helper to construct a FunctionEntry inline
FunctionEntry entry(const char* name, SqlFunction fn, uint8_t min_args, uint8_t max_args) {
    return FunctionEntry{name, static_cast<uint32_t>(std::strlen(name)), fn, min_args, max_args};
}

template <Dialect D>
void register_arithmetic(FunctionRegistry<D>& reg) {
    reg.register_function(entry("ABS",      functions::fn_abs,      1, 1));
    reg.register_function(entry("CEIL",     functions::fn_ceil,     1, 1));
    reg.register_function(entry("CEILING",  functions::fn_ceil,     1, 1));
    reg.register_function(entry("FLOOR",    functions::fn_floor,    1, 1));
    reg.register_function(entry("ROUND",    functions::fn_round,    1, 2));
    reg.register_function(entry("TRUNCATE", functions::fn_truncate, 2, 2));
    reg.register_function(entry("MOD",      functions::fn_mod,      2, 2));
    reg.register_function(entry("POWER",    functions::fn_power,    2, 2));
    reg.register_function(entry("POW",      functions::fn_power,    2, 2));
    reg.register_function(entry("SQRT",     functions::fn_sqrt,     1, 1));
    reg.register_function(entry("SIGN",     functions::fn_sign,     1, 1));
}

template <Dialect D>
void register_comparison(FunctionRegistry<D>& reg) {
    reg.register_function(entry("COALESCE", functions::fn_coalesce, 1, 255));
    reg.register_function(entry("NULLIF",   functions::fn_nullif,   2, 2));
    reg.register_function(entry("LEAST",    functions::fn_least,    1, 255));
    reg.register_function(entry("GREATEST", functions::fn_greatest, 1, 255));
}

template <Dialect D>
void register_string(FunctionRegistry<D>& reg) {
    reg.register_function(entry("CONCAT",      functions::fn_concat,      1, 255));
    reg.register_function(entry("CONCAT_WS",   functions::fn_concat_ws,   2, 255));
    reg.register_function(entry("LENGTH",      functions::fn_length,      1, 1));
    reg.register_function(entry("CHAR_LENGTH",  functions::fn_length,     1, 1));
    reg.register_function(entry("UPPER",       functions::fn_upper,       1, 1));
    reg.register_function(entry("UCASE",       functions::fn_upper,       1, 1));
    reg.register_function(entry("LOWER",       functions::fn_lower,       1, 1));
    reg.register_function(entry("LCASE",       functions::fn_lower,       1, 1));
    reg.register_function(entry("SUBSTRING",   functions::fn_substring,   2, 3));
    reg.register_function(entry("SUBSTR",      functions::fn_substring,   2, 3));
    reg.register_function(entry("TRIM",        functions::fn_trim,        1, 1));
    reg.register_function(entry("LTRIM",       functions::fn_ltrim,       1, 1));
    reg.register_function(entry("RTRIM",       functions::fn_rtrim,       1, 1));
    reg.register_function(entry("REPLACE",     functions::fn_replace,     3, 3));
    reg.register_function(entry("REVERSE",     functions::fn_reverse,     1, 1));
    reg.register_function(entry("LEFT",        functions::fn_left,        2, 2));
    reg.register_function(entry("RIGHT",       functions::fn_right,       2, 2));
    reg.register_function(entry("LPAD",        functions::fn_lpad,        3, 3));
    reg.register_function(entry("RPAD",        functions::fn_rpad,        3, 3));
    reg.register_function(entry("REPEAT",      functions::fn_repeat,      2, 2));
    reg.register_function(entry("SPACE",       functions::fn_space,       1, 1));
}

} // anonymous namespace

// ----- MySQL register_builtins -----

template <>
void FunctionRegistry<Dialect::MySQL>::register_builtins() {
    register_arithmetic(*this);
    register_comparison(*this);
    register_string(*this);

    // MySQL-specific
    register_function(entry("IFNULL", functions::fn_ifnull, 2, 2));
    register_function(entry("IF",     functions::fn_if,     3, 3));
}

// ----- PostgreSQL register_builtins -----

template <>
void FunctionRegistry<Dialect::PostgreSQL>::register_builtins() {
    register_arithmetic(*this);
    register_comparison(*this);
    register_string(*this);
    // No IFNULL or IF in PostgreSQL
}

// Explicit template instantiations
template class FunctionRegistry<Dialect::MySQL>;
template class FunctionRegistry<Dialect::PostgreSQL>;

} // namespace sql_engine
```

- [ ] **Step 2: Update `Makefile.new` for new library source**

Add `src/sql_engine/function_registry.cpp` to the build. Modify the library target and sources:

```makefile
# Add sql_engine sources
ENGINE_SRC_DIR = $(PROJECT_ROOT)/src/sql_engine
ENGINE_SRCS = $(ENGINE_SRC_DIR)/function_registry.cpp
ENGINE_OBJS = $(ENGINE_SRCS:.cpp=.o)

# Update LIB_TARGET to include engine objects or create a separate lib
# Simplest: add engine objects to the link step for tests

$(ENGINE_SRC_DIR)/%.o: $(ENGINE_SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@
```

Update the test target link line to include `$(ENGINE_OBJS)`:

```makefile
$(TEST_TARGET): $(TEST_OBJS) $(GTEST_OBJ) $(LIB_TARGET) $(ENGINE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(TEST_OBJS) $(GTEST_OBJ) $(ENGINE_OBJS) -L$(PROJECT_ROOT) -lsqlparser -lpthread
```

Add `$(TEST_DIR)/test_registry.cpp` to `TEST_SRCS`.

Add `$(ENGINE_OBJS)` to the `clean` target.

- [ ] **Step 3: Create `tests/test_registry.cpp`**

Create `tests/test_registry.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/function_registry.h"
#include "sql_engine/value.h"
#include "sql_parser/arena.h"

using namespace sql_engine;
using sql_parser::Dialect;
using sql_parser::Arena;

class RegistryTest : public ::testing::Test {
protected:
    Arena arena{4096};
};

// --- MySQL registry ---

TEST_F(RegistryTest, MySQLBuiltinsRegistered) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_GT(reg.size(), 0u);
}

TEST_F(RegistryTest, MySQLLookupAbs) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("ABS", 3);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->min_args, 1);
    EXPECT_EQ(entry->max_args, 1);
}

TEST_F(RegistryTest, MySQLLookupCaseInsensitive) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("abs", 3), nullptr);
    EXPECT_NE(reg.lookup("Abs", 3), nullptr);
    EXPECT_NE(reg.lookup("ABS", 3), nullptr);
}

TEST_F(RegistryTest, MySQLLookupUnknownReturnsNull) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_EQ(reg.lookup("DOES_NOT_EXIST", 14), nullptr);
}

TEST_F(RegistryTest, MySQLHasIfnull) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("IFNULL", 6), nullptr);
}

TEST_F(RegistryTest, MySQLHasIf) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("IF", 2), nullptr);
}

// --- PostgreSQL registry ---

TEST_F(RegistryTest, PgSQLBuiltinsRegistered) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_GT(reg.size(), 0u);
}

TEST_F(RegistryTest, PgSQLLookupAbs) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_NE(reg.lookup("ABS", 3), nullptr);
}

TEST_F(RegistryTest, PgSQLNoIfnull) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_EQ(reg.lookup("IFNULL", 6), nullptr);
}

TEST_F(RegistryTest, PgSQLNoIf) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    EXPECT_EQ(reg.lookup("IF", 2), nullptr);
}

// --- Dispatch through registry ---

TEST_F(RegistryTest, MySQLDispatchAbsViaRegistry) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("ABS", 3);
    ASSERT_NE(entry, nullptr);

    Value args[] = {value_int(-42)};
    Value result = entry->impl(args, 1, arena);
    EXPECT_EQ(result.int_val, 42);
}

TEST_F(RegistryTest, MySQLDispatchConcatViaRegistry) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("CONCAT", 6);
    ASSERT_NE(entry, nullptr);

    Value args[] = {
        value_string(sql_parser::StringRef{"hello", 5}),
        value_string(sql_parser::StringRef{" world", 6})
    };
    Value result = entry->impl(args, 2, arena);
    EXPECT_EQ(std::string(result.str_val.ptr, result.str_val.len), "hello world");
}

TEST_F(RegistryTest, PgSQLDispatchCoalesceViaRegistry) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();
    auto* entry = reg.lookup("COALESCE", 8);
    ASSERT_NE(entry, nullptr);

    Value args[] = {value_null(), value_int(42), value_int(99)};
    Value result = entry->impl(args, 3, arena);
    EXPECT_EQ(result.int_val, 42);
}

// --- Lookup all P0 functions ---

TEST_F(RegistryTest, MySQLAllP0FunctionsPresent) {
    FunctionRegistry<Dialect::MySQL> reg;
    reg.register_builtins();

    const char* names[] = {
        "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE", "MOD",
        "POWER", "POW", "SQRT", "SIGN",
        "COALESCE", "NULLIF", "LEAST", "GREATEST", "IFNULL", "IF",
        "CONCAT", "CONCAT_WS", "LENGTH", "CHAR_LENGTH", "UPPER", "UCASE",
        "LOWER", "LCASE", "SUBSTRING", "SUBSTR", "TRIM", "LTRIM", "RTRIM",
        "REPLACE", "REVERSE", "LEFT", "RIGHT", "LPAD", "RPAD", "REPEAT", "SPACE"
    };
    for (const char* name : names) {
        EXPECT_NE(reg.lookup(name, static_cast<uint32_t>(std::strlen(name))), nullptr)
            << "Missing function: " << name;
    }
}

TEST_F(RegistryTest, PgSQLAllP0FunctionsPresent) {
    FunctionRegistry<Dialect::PostgreSQL> reg;
    reg.register_builtins();

    // Same as MySQL minus IFNULL and IF
    const char* names[] = {
        "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE", "MOD",
        "POWER", "POW", "SQRT", "SIGN",
        "COALESCE", "NULLIF", "LEAST", "GREATEST",
        "CONCAT", "CONCAT_WS", "LENGTH", "CHAR_LENGTH", "UPPER", "UCASE",
        "LOWER", "LCASE", "SUBSTRING", "SUBSTR", "TRIM", "LTRIM", "RTRIM",
        "REPLACE", "REVERSE", "LEFT", "RIGHT", "LPAD", "RPAD", "REPEAT", "SPACE"
    };
    for (const char* name : names) {
        EXPECT_NE(reg.lookup(name, static_cast<uint32_t>(std::strlen(name))), nullptr)
            << "Missing function: " << name;
    }
}
```

- [ ] **Step 4: Create `src/sql_engine/` directory and build**

```bash
mkdir -p src/sql_engine
make -f Makefile.new clean && make -f Makefile.new test
```

All tests must pass: 430 original parser tests + all new type system tests.

- [ ] **Step 5: Verify final test count**

```bash
./run_tests --gtest_list_tests 2>/dev/null | grep -c "  "
```

Expected: approximately 530+ tests total (430 parser + ~100 type system).

- [ ] **Step 6: Commit**

```bash
git add src/sql_engine/function_registry.cpp tests/test_registry.cpp Makefile.new
git commit -m "Add function registry with register_builtins() for MySQL and PostgreSQL P0 functions"
```
