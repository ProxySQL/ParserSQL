# SQL Engine Type System — Design Specification

## Overview

The type system is the foundation of the composable SQL query engine library. It defines how SQL values are represented in memory, how types are classified, how coercion works across dialects, how NULL is handled, and how SQL functions are registered and dispatched.

This is sub-project 1 of the query engine. Everything else (expression evaluator, logical plan, optimizer, executor) depends on it.

### Goals

- **Unified value representation** covering all MySQL and PostgreSQL types
- **Dialect-specific type coercion** via compile-time templating (MySQL permissive, PostgreSQL strict)
- **Correct three-valued NULL logic** baked into the foundation
- **Extensible function registry** organized by category, implemented incrementally
- **Performance-conscious** — compact Value type, zero-copy strings, arena-compatible

### Constraints

- C++17 floor (same as parser)
- Uses the parser's `Arena`, `StringRef`, and `Dialect` enum
- Lives in `include/sql_engine/` — separate library from the parser, depends on it
- Header-only where practical (same pattern as parser)

### Non-Goals (deferred to later sub-projects)

- Expression evaluator (sub-project 2)
- Row/tuple format (sub-project 4)
- Catalog / schema metadata (sub-project 3)
- Aggregate functions (need execution engine)
- Date/time functions beyond basic constructors (P1, after core proves itself)
- JSON functions (P3)

---

## SqlType — Type Metadata

`SqlType` describes what a column can hold. It is metadata, not a runtime value.

```cpp
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
        DATETIME,       // MySQL DATETIME / PgSQL TIMESTAMP WITHOUT TIME ZONE
        TIMESTAMP,      // MySQL TIMESTAMP / PgSQL TIMESTAMP WITH TIME ZONE
        INTERVAL,       // PostgreSQL only

        // Structured
        JSON, JSONB,
        ENUM,
        ARRAY,          // PostgreSQL only

        // Special
        NULL_TYPE,
        UNKNOWN
    };

    Kind kind = UNKNOWN;
    uint16_t precision = 0;  // DECIMAL(precision,scale), CHAR(n), VARCHAR(n)
    uint16_t scale = 0;      // DECIMAL scale
    bool is_unsigned = false; // MySQL integer unsigned flag
    bool has_timezone = false;// TIMESTAMP WITH/WITHOUT TIME ZONE

    // Convenience constructors
    static SqlType make_int() { return {INT}; }
    static SqlType make_varchar(uint16_t len) { return {VARCHAR, len}; }
    static SqlType make_decimal(uint16_t p, uint16_t s) { return {DECIMAL, p, s}; }
    // etc.
};
```

### Type categories (for coercion grouping)

- **Numeric:** BOOLEAN, TINYINT–BIGINT, FLOAT, DOUBLE, DECIMAL
- **String:** CHAR, VARCHAR, TEXT variants, BINARY, VARBINARY, BLOB
- **Temporal:** DATE, TIME, DATETIME, TIMESTAMP, INTERVAL
- **Structured:** JSON, JSONB, ENUM, ARRAY

---

## Value — Runtime Representation

`Value` is what the expression evaluator operates on. Every intermediate result is a `Value`.

```cpp
struct Value {
    enum Tag : uint8_t {
        TAG_NULL = 0,
        TAG_BOOL,
        TAG_INT64,       // all signed integers widen to int64
        TAG_UINT64,      // MySQL unsigned BIGINT
        TAG_DOUBLE,      // FLOAT and DOUBLE
        TAG_DECIMAL,     // stored as string for now, proper int128 later
        TAG_STRING,      // CHAR, VARCHAR, TEXT
        TAG_BYTES,       // BINARY, VARBINARY, BLOB
        TAG_DATE,        // days since 1970-01-01 (int32)
        TAG_TIME,        // microseconds since midnight (int64)
        TAG_DATETIME,    // microseconds since epoch (int64), no timezone
        TAG_TIMESTAMP,   // microseconds since epoch (int64), timezone context
        TAG_INTERVAL,    // months + microseconds
        TAG_JSON,        // stored as string, parsed on demand
    };

    Tag tag = TAG_NULL;

    union {
        bool bool_val;
        int64_t int_val;
        uint64_t uint_val;
        double double_val;
        StringRef str_val;       // zero-copy into row buffer or arena-allocated
        int32_t date_val;        // days since epoch
        int64_t time_val;        // microseconds since midnight
        int64_t datetime_val;    // microseconds since epoch
        int64_t timestamp_val;   // microseconds since epoch
        struct { int32_t months; int64_t microseconds; } interval_val;
    };

    bool is_null() const { return tag == TAG_NULL; }
    bool is_numeric() const { return tag >= TAG_BOOL && tag <= TAG_DECIMAL; }
    bool is_string() const { return tag == TAG_STRING; }
    bool is_temporal() const { return tag >= TAG_DATE && tag <= TAG_INTERVAL; }
};
```

### Design decisions

1. **All integers widen to int64/uint64.** Runtime values don't carry TINYINT vs INT distinction — that's in SqlType. Simplifies the evaluator to one code path for all integer arithmetic.

2. **Dates as integers.** DATE = days since epoch (int32). DATETIME/TIMESTAMP = microseconds since epoch (int64). Makes date arithmetic trivial: `date + 1 day` = `date_val + 1`.

3. **StringRef for strings.** Zero-copy from row buffers. New strings (from CONCAT, etc.) allocated from arena.

4. **NULL is a tag value.** `tag == TAG_NULL` means the value is NULL. No separate null bitmap at the Value level (that's the Row's job, if it wants one for columnar storage).

5. **DECIMAL deferred.** Stored as string initially. Proper fixed-point (int128-based scaled integer) added later without changing Value layout — just add conversion logic.

### Value constructors

```cpp
inline Value value_null() { return Value{Value::TAG_NULL, {}}; }
inline Value value_bool(bool v) { Value r; r.tag = Value::TAG_BOOL; r.bool_val = v; return r; }
inline Value value_int(int64_t v) { Value r; r.tag = Value::TAG_INT64; r.int_val = v; return r; }
inline Value value_uint(uint64_t v) { Value r; r.tag = Value::TAG_UINT64; r.uint_val = v; return r; }
inline Value value_double(double v) { Value r; r.tag = Value::TAG_DOUBLE; r.double_val = v; return r; }
inline Value value_string(StringRef s) { Value r; r.tag = Value::TAG_STRING; r.str_val = s; return r; }
inline Value value_date(int32_t days) { Value r; r.tag = Value::TAG_DATE; r.date_val = days; return r; }
inline Value value_datetime(int64_t us) { Value r; r.tag = Value::TAG_DATETIME; r.datetime_val = us; return r; }
inline Value value_timestamp(int64_t us) { Value r; r.tag = Value::TAG_TIMESTAMP; r.timestamp_val = us; return r; }
inline Value value_time(int64_t us) { Value r; r.tag = Value::TAG_TIME; r.time_val = us; return r; }
inline Value value_interval(int32_t months, int64_t us) {
    Value r; r.tag = Value::TAG_INTERVAL; r.interval_val = {months, us}; return r;
}
```

---

## Type Coercion

When an operator encounters two values of different types, coercion rules determine what happens. MySQL and PostgreSQL differ significantly.

### Coercion interface

```cpp
template <Dialect D>
struct CoercionRules {
    // Can `from` be implicitly coerced to `to`?
    static bool can_coerce(SqlType::Kind from, SqlType::Kind to);

    // What common type should two operands be promoted to?
    static SqlType::Kind common_type(SqlType::Kind left, SqlType::Kind right);

    // Perform the coercion (returns new Value with target type, or NULL on failure)
    static Value coerce_value(const Value& val, SqlType::Kind target, Arena& arena);
};
```

### Promotion hierarchy

When coercion IS allowed, narrower types promote to wider:

```
BOOL → INT64 → UINT64 → DOUBLE → DECIMAL → STRING
                                      ↑
DATE → DATETIME → TIMESTAMP ─────────┘
```

### MySQL coercion (permissive)

- String to int: `'42' → 42`, `'42abc' → 42` (truncates at first non-digit)
- String to date: `'2024-01-15' → DATE` (attempts parse)
- Int to string: `42 → '42'`
- Most cross-category implicit conversions allowed
- Comparison between string and int: string is coerced to int

### PostgreSQL coercion (strict)

- String to int: ERROR (requires explicit `::integer` cast)
- Most cross-category implicit conversions disallowed
- Within same category: promotions allowed (INT → BIGINT, FLOAT → DOUBLE)
- Explicit casts via `CAST(x AS type)` or `x::type` always allowed

### Implementation

Separate specialization per dialect using `if constexpr` or template specialization:

```cpp
template <>
struct CoercionRules<Dialect::MySQL> {
    static bool can_coerce(SqlType::Kind from, SqlType::Kind to) {
        // MySQL: almost everything can coerce to everything
        return true; // simplified; real implementation has a matrix
    }
    // ...
};

template <>
struct CoercionRules<Dialect::PostgreSQL> {
    static bool can_coerce(SqlType::Kind from, SqlType::Kind to) {
        // PostgreSQL: only within same category or explicit promotion path
        // ...
    }
    // ...
};
```

---

## NULL Semantics

SQL uses three-valued logic. NULL is not a value — it represents "unknown."

### Rules

| Expression | Result |
|---|---|
| `NULL = NULL` | NULL |
| `NULL = 1` | NULL |
| `NULL AND FALSE` | FALSE (short-circuit) |
| `NULL AND TRUE` | NULL |
| `NULL OR TRUE` | TRUE (short-circuit) |
| `NULL OR FALSE` | NULL |
| `NOT NULL` | NULL |
| `NULL + 1` | NULL |
| `NULL > 5` | NULL |

### Special forms that handle NULL explicitly

| Expression | Result |
|---|---|
| `x IS NULL` | TRUE or FALSE (never NULL) |
| `x IS NOT NULL` | TRUE or FALSE |
| `COALESCE(x, y, z)` | first non-NULL |
| `NULLIF(x, y)` | NULL if x=y, else x |
| `IFNULL(x, y)` | MySQL: y if x is NULL |
| `x IN (1, NULL)` | TRUE if x=1, NULL if x not in non-NULL values |

### Implementation helpers

```cpp
namespace null_semantics {

// Most binary operators: NULL in → NULL out
inline bool propagate_null(const Value& left, const Value& right, Value& result) {
    if (left.is_null() || right.is_null()) {
        result = value_null();
        return true;  // caller should return immediately
    }
    return false;  // not null, proceed with operation
}

// AND: special NULL handling
inline Value eval_and(const Value& left, const Value& right);

// OR: special NULL handling
inline Value eval_or(const Value& left, const Value& right);

// NOT: NULL → NULL
inline Value eval_not(const Value& val);

} // namespace null_semantics
```

---

## Function Registry

Maps `(function_name, dialect) → implementation`. Extensible — functions are registered at startup, can be added by users of the library.

### Function signature

```cpp
using SqlFunction = Value(*)(const Value* args, uint16_t arg_count, Arena& arena);
```

All functions take an array of `Value` arguments and return a `Value`. Arena is provided for functions that need to allocate (e.g., string functions).

### Registry

```cpp
struct FunctionEntry {
    const char* name;          // uppercased canonical name
    SqlFunction impl;
    uint8_t min_args;
    uint8_t max_args;          // 255 = variadic
};

template <Dialect D>
class FunctionRegistry {
public:
    void register_function(const FunctionEntry& entry);
    const FunctionEntry* lookup(const char* name, uint32_t name_len) const;

    // Register all built-in functions for this dialect
    void register_builtins();
};
```

### Built-in function categories (implementation priority)

**P0 — built with this sub-project:**

Arithmetic: `ABS`, `CEIL`/`CEILING`, `FLOOR`, `ROUND`, `TRUNCATE`, `MOD`, `POWER`/`POW`, `SQRT`, `SIGN`

Comparison: `LEAST`, `GREATEST`, `COALESCE`, `NULLIF`, `IFNULL` (MySQL), `IF` (MySQL)

String: `CONCAT`, `CONCAT_WS`, `LENGTH`/`CHAR_LENGTH`, `UPPER`/`UCASE`, `LOWER`/`LCASE`, `SUBSTRING`/`SUBSTR`, `TRIM`/`LTRIM`/`RTRIM`, `REPLACE`, `REVERSE`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPEAT`, `SPACE`

Cast: `CAST(x AS type)`, implicit coercion

**P1 — second sub-project (after expression evaluator):**

Date/Time: `NOW`, `CURDATE`, `CURTIME`, `DATE`, `TIME`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DATE_ADD`/`DATE_SUB`, `DATEDIFF`, `TIMESTAMPDIFF`, `DATE_FORMAT` (MySQL), `TO_CHAR` (PgSQL), `EXTRACT`

**P2 — needs execution engine:**

Aggregate: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT` (MySQL), `STRING_AGG` (PgSQL), `ARRAY_AGG` (PgSQL)

**P3 — lower priority:**

JSON: `JSON_EXTRACT`, `->`, `->>`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_SET`, `JSON_REPLACE`

Math: `LOG`, `LN`, `EXP`, `SIN`, `COS`, `TAN`, `RAND`/`RANDOM`

---

## File Organization

```
include/sql_engine/
    types.h              — SqlType struct
    value.h              — Value tagged union + constructors
    coercion.h           — CoercionRules<D> template
    null_semantics.h     — NULL propagation helpers
    function_registry.h  — FunctionRegistry<D> + FunctionEntry
    functions/
        arithmetic.h     — P0 arithmetic functions
        comparison.h     — P0 comparison functions
        string.h         — P0 string functions
        cast.h           — CAST + implicit coercion execution

src/sql_engine/
    function_registry.cpp — register_builtins() implementations

tests/
    test_value.cpp           — Value creation, tag checks, constructors
    test_coercion.cpp        — Coercion for both dialects (permissive vs strict)
    test_null_semantics.cpp  — Three-valued logic, AND/OR short-circuit
    test_arithmetic.cpp      — All P0 arithmetic functions + NULL
    test_comparison.cpp      — Comparison operators + cross-type coercion
    test_string.cpp          — All P0 string functions
    test_cast.cpp            — CAST between all type pairs
```

---

## Testing Strategy

The type system is where subtle bugs hide. Testing must be thorough:

1. **Value round-trip tests** — create a value, check tag and payload
2. **Coercion matrix tests** — for every pair of types, verify can_coerce and common_type for both dialects
3. **NULL propagation tests** — every operator with NULL inputs
4. **Function tests** — every P0 function with: normal args, NULL args, edge cases (empty string, zero, MAX_INT, MIN_INT, negative numbers)
5. **MySQL vs PostgreSQL divergence tests** — cases where the two dialects give different results for the same input (e.g., `'42abc' + 0` → 42 in MySQL, error in PgSQL)
6. **Date arithmetic tests** — days-since-epoch round-trips, edge cases (leap years, epoch boundaries)

---

## Performance Targets

| Operation | Target |
|---|---|
| Value construction | <5ns |
| NULL check | <1ns (single byte compare) |
| Integer arithmetic | <5ns |
| String comparison | <50ns (for typical lengths) |
| Type coercion (int→double) | <10ns |
| Function lookup | <50ns (hash table, same as keyword lookup) |
