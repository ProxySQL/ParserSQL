# SQL Engine Expression Evaluator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the expression evaluator that connects the SQL parser AST to the query engine type system. Takes any expression AST node, resolves column references via callback, and returns a `Value` -- the first time parser and engine connect end-to-end.

**Architecture:** Header-only templates in `include/sql_engine/` that depend on the parser's AST (`AstNode`, `NodeType`, `Arena`) and the type system (`Value`, `CoercionRules<D>`, `null_semantics`, `FunctionRegistry<D>`). Dialect differences handled via `template <Dialect D>` specialization. No exceptions -- errors return `value_null()`.

**Tech Stack:** C++17, GNU Make, Google Test (already in `third_party/`), parser library (`libsqlparser.a`), type system headers from sub-project 1.

**Spec:** `docs/superpowers/specs/2026-04-03-expression-evaluator-design.md`

**Depends on:** Sub-project 1 (type system) -- all files in `include/sql_engine/` are already built.

---

## Scope

This plan builds:
1. `match_like<D>()` -- LIKE pattern matcher with dialect-aware case sensitivity
2. `tag_to_kind()` / `kind_to_tag()` -- mapping helpers between `Value::Tag` and `SqlType::Kind`
3. `evaluate_expression<D>()` -- the core evaluator that switches on `NodeType` and recursively evaluates
4. Parser fix: `flags = 1` for simple CASE in `parse_case()`
5. Comprehensive tests: unit tests for LIKE, unit tests for each node type, integration tests parsing real SQL

**Not in scope:** Aggregate functions, subquery evaluation, window functions, row/tuple values, ORDER BY/LIMIT execution, date/time arithmetic.

---

## File Structure

```
include/sql_engine/
    like.h               -- match_like<D>() LIKE pattern matcher
    tag_kind_map.h       -- Value::Tag <-> SqlType::Kind conversion helpers
    expression_eval.h    -- evaluate_expression<D>() template function

include/sql_parser/
    expression_parser.h  -- one-line fix: flags = 1 for simple CASE

tests/
    test_like.cpp                 -- LIKE pattern matching tests
    test_expression_eval.cpp      -- Unit tests for each node type
    test_eval_integration.cpp     -- End-to-end: parse SQL -> evaluate -> verify
```

---

### Task 1: LIKE Pattern Matcher + Tag-Kind Mapping + Parser Fix

**Files:**
- Create: `include/sql_engine/like.h`
- Create: `include/sql_engine/tag_kind_map.h`
- Create: `tests/test_like.cpp`
- Modify: `include/sql_parser/expression_parser.h` (one-line change)
- Modify: `Makefile.new`

This task builds the LIKE matcher (needed by binary operator dispatch), the tag/kind mapping helpers (needed by coercion in the evaluator), and fixes the parser's CASE/WHEN disambiguation.

- [ ] **Step 1: Create `include/sql_engine/tag_kind_map.h`**

This header provides bidirectional conversion between `Value::Tag` (runtime) and `SqlType::Kind` (type system). The evaluator needs this because `CoercionRules<D>::common_type()` operates on `SqlType::Kind` while runtime values carry `Value::Tag`.

Create `include/sql_engine/tag_kind_map.h`:
```cpp
#ifndef SQL_ENGINE_TAG_KIND_MAP_H
#define SQL_ENGINE_TAG_KIND_MAP_H

#include "sql_engine/types.h"
#include "sql_engine/value.h"

namespace sql_engine {

// Convert a runtime Value::Tag to the corresponding SqlType::Kind.
// Used before calling CoercionRules<D>::common_type().
inline SqlType::Kind tag_to_kind(Value::Tag tag) {
    switch (tag) {
        case Value::TAG_NULL:      return SqlType::NULL_TYPE;
        case Value::TAG_BOOL:      return SqlType::BOOLEAN;
        case Value::TAG_INT64:     return SqlType::BIGINT;
        case Value::TAG_UINT64:    return SqlType::BIGINT;
        case Value::TAG_DOUBLE:    return SqlType::DOUBLE;
        case Value::TAG_DECIMAL:   return SqlType::DECIMAL;
        case Value::TAG_STRING:    return SqlType::VARCHAR;
        case Value::TAG_BYTES:     return SqlType::VARBINARY;
        case Value::TAG_DATE:      return SqlType::DATE;
        case Value::TAG_TIME:      return SqlType::TIME;
        case Value::TAG_DATETIME:  return SqlType::DATETIME;
        case Value::TAG_TIMESTAMP: return SqlType::TIMESTAMP;
        case Value::TAG_INTERVAL:  return SqlType::INTERVAL;
        case Value::TAG_JSON:      return SqlType::JSON;
        default:                   return SqlType::UNKNOWN;
    }
}

// Convert a SqlType::Kind back to a Value::Tag for coercion targets.
// Used after common_type() returns the promotion target.
inline Value::Tag kind_to_tag(SqlType::Kind kind) {
    switch (kind) {
        case SqlType::BOOLEAN:                                return Value::TAG_BOOL;
        case SqlType::TINYINT:
        case SqlType::SMALLINT:
        case SqlType::MEDIUMINT:
        case SqlType::INT:
        case SqlType::BIGINT:                                 return Value::TAG_INT64;
        case SqlType::FLOAT:
        case SqlType::DOUBLE:                                 return Value::TAG_DOUBLE;
        case SqlType::DECIMAL:                                return Value::TAG_DECIMAL;
        case SqlType::CHAR:
        case SqlType::VARCHAR:
        case SqlType::TEXT:
        case SqlType::MEDIUMTEXT:
        case SqlType::LONGTEXT:                               return Value::TAG_STRING;
        case SqlType::BINARY:
        case SqlType::VARBINARY:
        case SqlType::BLOB:                                   return Value::TAG_BYTES;
        case SqlType::DATE:                                   return Value::TAG_DATE;
        case SqlType::TIME:                                   return Value::TAG_TIME;
        case SqlType::DATETIME:                               return Value::TAG_DATETIME;
        case SqlType::TIMESTAMP:                              return Value::TAG_TIMESTAMP;
        case SqlType::INTERVAL:                               return Value::TAG_INTERVAL;
        case SqlType::JSON:
        case SqlType::JSONB:                                  return Value::TAG_JSON;
        default:                                              return Value::TAG_NULL;
    }
}

} // namespace sql_engine

#endif // SQL_ENGINE_TAG_KIND_MAP_H
```

- [ ] **Step 2: Create `include/sql_engine/like.h`**

The LIKE pattern matcher uses an iterative two-pointer approach. `%` matches zero or more characters, `_` matches exactly one character, the escape character (default `\`) makes the next character literal. MySQL is case-insensitive by default; PostgreSQL is case-sensitive.

Create `include/sql_engine/like.h`:
```cpp
#ifndef SQL_ENGINE_LIKE_H
#define SQL_ENGINE_LIKE_H

#include "sql_parser/common.h"
#include <cctype>

namespace sql_engine {

using sql_parser::Dialect;
using sql_parser::StringRef;

namespace detail {

inline char to_lower(char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c + 32) : c;
}

} // namespace detail

// Match a string against a SQL LIKE pattern.
//
// Template parameter D controls case sensitivity:
//   MySQL:      case-insensitive by default
//   PostgreSQL: case-sensitive (use ILIKE for insensitive, not handled here)
//
// Pattern characters:
//   %  -- matches zero or more characters
//   _  -- matches exactly one character
//   escape_char -- next character is literal (default '\')
//
// Algorithm: iterative with backtracking via saved positions for '%'.
// O(n*m) worst case, O(n+m) typical.
template <Dialect D>
bool match_like(StringRef text, StringRef pattern, char escape_char = '\\') {
    constexpr bool case_insensitive = (D == Dialect::MySQL);

    uint32_t ti = 0;  // text index
    uint32_t pi = 0;  // pattern index

    // Saved positions for '%' backtracking
    uint32_t star_pi = UINT32_MAX;  // pattern position after last '%'
    uint32_t star_ti = UINT32_MAX;  // text position when last '%' was hit

    while (ti < text.len) {
        if (pi < pattern.len) {
            char pc = pattern.ptr[pi];

            // Check escape character
            if (pc == escape_char && pi + 1 < pattern.len) {
                // Next character is literal
                pi++;
                pc = pattern.ptr[pi];
                char tc = text.ptr[ti];
                if (case_insensitive) {
                    tc = detail::to_lower(tc);
                    pc = detail::to_lower(pc);
                }
                if (tc == pc) {
                    ti++;
                    pi++;
                    continue;
                }
                // Fall through to backtrack
            } else if (pc == '%') {
                // Save backtrack position
                star_pi = pi + 1;
                star_ti = ti;
                pi++;
                continue;
            } else if (pc == '_') {
                // Match exactly one character
                ti++;
                pi++;
                continue;
            } else {
                // Literal character match
                char tc = text.ptr[ti];
                if (case_insensitive) {
                    tc = detail::to_lower(tc);
                    pc = detail::to_lower(pc);
                }
                if (tc == pc) {
                    ti++;
                    pi++;
                    continue;
                }
                // Fall through to backtrack
            }
        }

        // Mismatch or pattern exhausted: try backtracking to last '%'
        if (star_pi != UINT32_MAX) {
            pi = star_pi;
            star_ti++;
            ti = star_ti;
            continue;
        }

        // No '%' to backtrack to: match fails
        return false;
    }

    // Text consumed: skip any remaining '%' in pattern
    while (pi < pattern.len && pattern.ptr[pi] == '%') {
        pi++;
    }

    return pi == pattern.len;
}

} // namespace sql_engine

#endif // SQL_ENGINE_LIKE_H
```

- [ ] **Step 3: Modify parser's `expression_parser.h` -- set `flags = 1` for simple CASE**

In `include/sql_parser/expression_parser.h`, in the `parse_case()` method, add `node->flags = 1;` inside the `if (tok_.peek().type != TokenType::TK_WHEN)` block. This lets the evaluator distinguish simple CASE from searched CASE without ambiguity.

Find this code in `parse_case()`:
```cpp
        if (tok_.peek().type != TokenType::TK_WHEN) {
            AstNode* case_expr = parse();
            if (case_expr) node->add_child(case_expr);
        }
```

Change it to:
```cpp
        if (tok_.peek().type != TokenType::TK_WHEN) {
            node->flags = 1;  // simple CASE (has case_expr)
            AstNode* case_expr = parse();
            if (case_expr) node->add_child(case_expr);
        }
```

- [ ] **Step 4: Create `tests/test_like.cpp`**

Create `tests/test_like.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/like.h"

using namespace sql_engine;
using sql_parser::Dialect;
using sql_parser::StringRef;

// Helper to make StringRef from string literal
static StringRef S(const char* s) {
    return StringRef{s, static_cast<uint32_t>(std::strlen(s))};
}

// ===== MySQL (case-insensitive) =====

class LikeMySQLTest : public ::testing::Test {};

TEST_F(LikeMySQLTest, ExactMatch) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hello")));
}

TEST_F(LikeMySQLTest, ExactMatchCaseInsensitive) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("Hello"), S("hello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("HELLO"), S("hello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("HELLO")));
}

TEST_F(LikeMySQLTest, NoMatch) {
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("world")));
}

TEST_F(LikeMySQLTest, PercentPrefix) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%llo")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%hello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%o")));
}

TEST_F(LikeMySQLTest, PercentSuffix) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hel%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hello%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("h%")));
}

TEST_F(LikeMySQLTest, PercentBoth) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%ell%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%hello%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%%")));
}

TEST_F(LikeMySQLTest, PercentOnly) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("anything"), S("%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("%")));
}

TEST_F(LikeMySQLTest, Underscore) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("hell_")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("_ello")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("_____")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("____")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("______")));
}

TEST_F(LikeMySQLTest, UnderscoreAndPercent) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("_ell%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello"), S("%ll_")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("hello world"), S("hello_world")));
}

TEST_F(LikeMySQLTest, EscapeCharacter) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("100%"), S("100\\%")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("100x"), S("100\\%")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("a_b"), S("a\\_b")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("axb"), S("a\\_b")));
}

TEST_F(LikeMySQLTest, CustomEscapeCharacter) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("100%"), S("100#%"), '#'));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("100x"), S("100#%"), '#'));
}

TEST_F(LikeMySQLTest, EmptyString) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("%")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S(""), S("_")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S(""), S("a")));
}

TEST_F(LikeMySQLTest, EmptyPattern) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S(""), S("")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("hello"), S("")));
}

TEST_F(LikeMySQLTest, MultiplePercents) {
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("abcdef"), S("%b%d%f")));
    EXPECT_TRUE(match_like<Dialect::MySQL>(S("abcdef"), S("%b%e%")));
    EXPECT_FALSE(match_like<Dialect::MySQL>(S("abcdef"), S("%z%")));
}

// ===== PostgreSQL (case-sensitive) =====

class LikePgSQLTest : public ::testing::Test {};

TEST_F(LikePgSQLTest, ExactMatch) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hello")));
}

TEST_F(LikePgSQLTest, CaseSensitive) {
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("Hello"), S("hello")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("HELLO"), S("hello")));
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hello")));
}

TEST_F(LikePgSQLTest, PercentPrefix) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("%llo")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("hello"), S("%LLO")));
}

TEST_F(LikePgSQLTest, PercentSuffix) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hel%")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("hello"), S("HEL%")));
}

TEST_F(LikePgSQLTest, Underscore) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("hell_")));
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("hello"), S("_ello")));
}

TEST_F(LikePgSQLTest, EscapeCharacter) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S("100%"), S("100\\%")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S("100x"), S("100\\%")));
}

TEST_F(LikePgSQLTest, EmptyStringEdgeCases) {
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S(""), S("")));
    EXPECT_TRUE(match_like<Dialect::PostgreSQL>(S(""), S("%")));
    EXPECT_FALSE(match_like<Dialect::PostgreSQL>(S(""), S("_")));
}
```

- [ ] **Step 5: Update `Makefile.new`**

Add `test_like.cpp` to the `TEST_SRCS` list in `Makefile.new`. Find the line:
```
            $(TEST_DIR)/test_registry.cpp
```
Add after it:
```
            $(TEST_DIR)/test_like.cpp
```

- [ ] **Step 6: Build and verify**

Run:
```bash
make -f Makefile.new test 2>&1 | tail -20
```

All existing tests must still pass, plus the new LIKE tests.

---

### Task 2: Core evaluate_expression -- Literals, Columns, Unary

**Files:**
- Create: `include/sql_engine/expression_eval.h`
- Create: `tests/test_expression_eval.cpp`
- Modify: `Makefile.new`

This task builds the skeleton `evaluate_expression<D>()` function with the main `switch` on `NodeType`. It handles all leaf nodes (literals, columns, asterisk, placeholder), unary operators (`-` and `NOT`), the `NODE_EXPRESSION` wrapper, and deferred node types.

- [ ] **Step 1: Create `include/sql_engine/expression_eval.h`**

This file contains the full `evaluate_expression<D>()` function. Task 2 implements the leaf nodes and unary ops. Tasks 3 and 4 extend this same file with binary operators and special forms.

Create `include/sql_engine/expression_eval.h`:
```cpp
#ifndef SQL_ENGINE_EXPRESSION_EVAL_H
#define SQL_ENGINE_EXPRESSION_EVAL_H

#include "sql_engine/value.h"
#include "sql_engine/types.h"
#include "sql_engine/null_semantics.h"
#include "sql_engine/coercion.h"
#include "sql_engine/tag_kind_map.h"
#include "sql_engine/like.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include <functional>
#include <cstdlib>
#include <cstring>

namespace sql_engine {

using sql_parser::Dialect;
using sql_parser::Arena;
using sql_parser::AstNode;
using sql_parser::NodeType;
using sql_parser::StringRef;

namespace detail {

// Count children of an AST node.
inline uint32_t child_count(const AstNode* node) {
    uint32_t n = 0;
    for (const AstNode* c = node->first_child; c; c = c->next_sibling) ++n;
    return n;
}

// Get the Nth child (0-based). Returns nullptr if out of bounds.
inline const AstNode* nth_child(const AstNode* node, uint32_t index) {
    const AstNode* c = node->first_child;
    for (uint32_t i = 0; c && i < index; ++i) c = c->next_sibling;
    return c;
}

// Parse a null-terminated-ish StringRef as int64 via strtoll.
inline int64_t parse_int_from_ref(StringRef ref) {
    if (ref.len == 0) return 0;
    char buf[64];
    uint32_t n = ref.len < 63 ? ref.len : 63;
    std::memcpy(buf, ref.ptr, n);
    buf[n] = '\0';
    return std::strtoll(buf, nullptr, 10);
}

// Parse a StringRef as double via strtod.
inline double parse_double_from_ref(StringRef ref) {
    if (ref.len == 0) return 0.0;
    char buf[128];
    uint32_t n = ref.len < 127 ? ref.len : 127;
    std::memcpy(buf, ref.ptr, n);
    buf[n] = '\0';
    return std::strtod(buf, nullptr);
}

// Case-insensitive comparison of StringRef against a C-string literal.
inline bool ref_equals_ci(StringRef ref, const char* s, uint32_t slen) {
    return ref.equals_ci(s, slen);
}

} // namespace detail

// ============================================================
// evaluate_expression<D>()
//
// Evaluates a single AST expression node recursively.
// Returns value_null() on error or unsupported nodes.
// ============================================================

template <Dialect D>
Value evaluate_expression(const AstNode* expr,
                          const std::function<Value(StringRef)>& resolve,
                          FunctionRegistry<D>& functions,
                          Arena& arena) {
    if (!expr) return value_null();

    switch (expr->type) {

    // ---- Leaf nodes ----

    case NodeType::NODE_LITERAL_INT: {
        // Check for TRUE/FALSE keywords (parser stores them as NODE_LITERAL_INT)
        StringRef val = expr->value();
        if (detail::ref_equals_ci(val, "TRUE", 4))
            return value_bool(true);
        if (detail::ref_equals_ci(val, "FALSE", 5))
            return value_bool(false);
        return value_int(detail::parse_int_from_ref(val));
    }

    case NodeType::NODE_LITERAL_FLOAT: {
        return value_double(detail::parse_double_from_ref(expr->value()));
    }

    case NodeType::NODE_LITERAL_STRING: {
        return value_string(expr->value());
    }

    case NodeType::NODE_LITERAL_NULL: {
        return value_null();
    }

    case NodeType::NODE_COLUMN_REF: {
        return resolve(expr->value());
    }

    case NodeType::NODE_IDENTIFIER: {
        return resolve(expr->value());
    }

    case NodeType::NODE_ASTERISK: {
        return value_string(StringRef{"*", 1});
    }

    case NodeType::NODE_PLACEHOLDER: {
        return value_null();  // unresolved placeholder
    }

    // ---- Qualified name: combine table.column, call resolve ----

    case NodeType::NODE_QUALIFIED_NAME: {
        const AstNode* tbl = detail::nth_child(expr, 0);
        const AstNode* col = detail::nth_child(expr, 1);
        if (!tbl || !col) return value_null();
        // Build "table.column" string in arena
        StringRef t = tbl->value();
        StringRef c = col->value();
        uint32_t total = t.len + 1 + c.len;
        char* buf = static_cast<char*>(arena.allocate(total));
        if (!buf) return value_null();
        std::memcpy(buf, t.ptr, t.len);
        buf[t.len] = '.';
        std::memcpy(buf + t.len + 1, c.ptr, c.len);
        return resolve(StringRef{buf, total});
    }

    // ---- Wrapper: unwrap and evaluate first child ----

    case NodeType::NODE_EXPRESSION: {
        return evaluate_expression<D>(expr->first_child, resolve, functions, arena);
    }

    // ---- Unary operators ----

    case NodeType::NODE_UNARY_OP: {
        StringRef op = expr->value();
        const AstNode* operand_node = expr->first_child;
        if (!operand_node) return value_null();
        Value operand = evaluate_expression<D>(operand_node, resolve, functions, arena);
        if (op.len == 1 && op.ptr[0] == '-') {
            // Unary minus
            if (operand.is_null()) return value_null();
            if (operand.tag == Value::TAG_INT64)
                return value_int(-operand.int_val);
            if (operand.tag == Value::TAG_DOUBLE)
                return value_double(-operand.double_val);
            if (operand.tag == Value::TAG_UINT64)
                return value_int(-static_cast<int64_t>(operand.uint_val));
            return value_null();
        }
        if (detail::ref_equals_ci(op, "NOT", 3)) {
            // Three-valued NOT: NULL->NULL, TRUE->FALSE, FALSE->TRUE
            // The operand might not be a bool yet. Coerce truthy check:
            if (operand.is_null()) return value_null();
            if (operand.tag == Value::TAG_BOOL)
                return null_semantics::eval_not(operand);
            // For numeric: 0 is false, non-zero is true
            if (operand.tag == Value::TAG_INT64)
                return value_bool(operand.int_val == 0);
            if (operand.tag == Value::TAG_DOUBLE)
                return value_bool(operand.double_val == 0.0);
            return value_bool(false);  // non-null non-numeric = truthy, NOT = false
        }
        return value_null();
    }

    // ---- Binary operators (Task 3 adds the full implementation) ----

    case NodeType::NODE_BINARY_OP: {
        StringRef op = expr->value();
        const AstNode* left_node = detail::nth_child(expr, 0);
        const AstNode* right_node = detail::nth_child(expr, 1);
        if (!left_node || !right_node) return value_null();

        // --- Short-circuit: AND ---
        if (detail::ref_equals_ci(op, "AND", 3)) {
            Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
            // If left is FALSE -> FALSE immediately
            if (!left_val.is_null() && left_val.tag == Value::TAG_BOOL && !left_val.bool_val)
                return value_bool(false);
            Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
            return null_semantics::eval_and(left_val, right_val);
        }

        // --- Short-circuit: OR ---
        if (detail::ref_equals_ci(op, "OR", 2)) {
            Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
            // If left is TRUE -> TRUE immediately
            if (!left_val.is_null() && left_val.tag == Value::TAG_BOOL && left_val.bool_val)
                return value_bool(true);
            Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
            return null_semantics::eval_or(left_val, right_val);
        }

        // --- IS / IS NOT (never return NULL) ---
        if (detail::ref_equals_ci(op, "IS", 2)) {
            Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
            Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
            // IS TRUE: left is truthy and not null
            if (!right_val.is_null() && right_val.tag == Value::TAG_BOOL && right_val.bool_val) {
                // IS TRUE
                if (left_val.is_null()) return value_bool(false);
                if (left_val.tag == Value::TAG_BOOL) return value_bool(left_val.bool_val);
                if (left_val.tag == Value::TAG_INT64) return value_bool(left_val.int_val != 0);
                if (left_val.tag == Value::TAG_DOUBLE) return value_bool(left_val.double_val != 0.0);
                return value_bool(true);  // non-null = truthy
            }
            // IS FALSE: left is falsy and not null
            if (!right_val.is_null() && right_val.tag == Value::TAG_BOOL && !right_val.bool_val) {
                if (left_val.is_null()) return value_bool(false);
                if (left_val.tag == Value::TAG_BOOL) return value_bool(!left_val.bool_val);
                if (left_val.tag == Value::TAG_INT64) return value_bool(left_val.int_val == 0);
                if (left_val.tag == Value::TAG_DOUBLE) return value_bool(left_val.double_val == 0.0);
                return value_bool(false);  // non-null non-numeric = truthy, so not false
            }
            return value_null();
        }
        if (detail::ref_equals_ci(op, "IS NOT", 6)) {
            Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
            Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
            // IS NOT TRUE = NOT (IS TRUE)
            if (!right_val.is_null() && right_val.tag == Value::TAG_BOOL && right_val.bool_val) {
                if (left_val.is_null()) return value_bool(true);
                if (left_val.tag == Value::TAG_BOOL) return value_bool(!left_val.bool_val);
                if (left_val.tag == Value::TAG_INT64) return value_bool(left_val.int_val == 0);
                if (left_val.tag == Value::TAG_DOUBLE) return value_bool(left_val.double_val == 0.0);
                return value_bool(false);
            }
            // IS NOT FALSE = NOT (IS FALSE)
            if (!right_val.is_null() && right_val.tag == Value::TAG_BOOL && !right_val.bool_val) {
                if (left_val.is_null()) return value_bool(true);
                if (left_val.tag == Value::TAG_BOOL) return value_bool(left_val.bool_val);
                if (left_val.tag == Value::TAG_INT64) return value_bool(left_val.int_val != 0);
                if (left_val.tag == Value::TAG_DOUBLE) return value_bool(left_val.double_val != 0.0);
                return value_bool(true);
            }
            return value_null();
        }

        // --- LIKE ---
        if (detail::ref_equals_ci(op, "LIKE", 4)) {
            Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
            Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
            if (left_val.is_null() || right_val.is_null()) return value_null();
            // Coerce both to strings if not already
            if (left_val.tag != Value::TAG_STRING)
                left_val = CoercionRules<D>::coerce_value(left_val, Value::TAG_STRING, arena);
            if (right_val.tag != Value::TAG_STRING)
                right_val = CoercionRules<D>::coerce_value(right_val, Value::TAG_STRING, arena);
            if (left_val.is_null() || right_val.is_null()) return value_null();
            return value_bool(match_like<D>(left_val.str_val, right_val.str_val));
        }

        // --- || : PostgreSQL = concat, MySQL = OR ---
        if (op.len == 2 && op.ptr[0] == '|' && op.ptr[1] == '|') {
            if constexpr (D == Dialect::PostgreSQL) {
                // String concatenation
                Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
                Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
                if (left_val.is_null() || right_val.is_null()) return value_null();
                // Coerce to string
                if (left_val.tag != Value::TAG_STRING)
                    left_val = CoercionRules<D>::coerce_value(left_val, Value::TAG_STRING, arena);
                if (right_val.tag != Value::TAG_STRING)
                    right_val = CoercionRules<D>::coerce_value(right_val, Value::TAG_STRING, arena);
                if (left_val.is_null() || right_val.is_null()) return value_null();
                uint32_t total = left_val.str_val.len + right_val.str_val.len;
                if (total == 0) return value_string(StringRef{nullptr, 0});
                char* buf = static_cast<char*>(arena.allocate(total));
                if (!buf) return value_null();
                std::memcpy(buf, left_val.str_val.ptr, left_val.str_val.len);
                std::memcpy(buf + left_val.str_val.len, right_val.str_val.ptr, right_val.str_val.len);
                return value_string(StringRef{buf, total});
            } else {
                // MySQL: || is OR
                Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
                if (!left_val.is_null() && left_val.tag == Value::TAG_BOOL && left_val.bool_val)
                    return value_bool(true);
                Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);
                return null_semantics::eval_or(left_val, right_val);
            }
        }

        // --- Standard binary: evaluate both sides, null-propagate, coerce, apply ---
        Value left_val = evaluate_expression<D>(left_node, resolve, functions, arena);
        Value right_val = evaluate_expression<D>(right_node, resolve, functions, arena);

        // NULL propagation
        if (left_val.is_null() || right_val.is_null()) return value_null();

        // Map to SqlType::Kind for coercion
        SqlType::Kind left_kind = tag_to_kind(left_val.tag);
        SqlType::Kind right_kind = tag_to_kind(right_val.tag);
        SqlType::Kind common = CoercionRules<D>::common_type(left_kind, right_kind);
        if (common == SqlType::UNKNOWN) return value_null();
        Value::Tag target_tag = kind_to_tag(common);

        // Coerce both to common type
        Value lv = CoercionRules<D>::coerce_value(left_val, target_tag, arena);
        Value rv = CoercionRules<D>::coerce_value(right_val, target_tag, arena);
        if (lv.is_null() || rv.is_null()) return value_null();

        // --- Arithmetic operators ---
        if (op.len == 1) {
            switch (op.ptr[0]) {
                case '+':
                    if (lv.tag == Value::TAG_INT64) return value_int(lv.int_val + rv.int_val);
                    if (lv.tag == Value::TAG_DOUBLE) return value_double(lv.double_val + rv.double_val);
                    return value_null();
                case '-':
                    if (lv.tag == Value::TAG_INT64) return value_int(lv.int_val - rv.int_val);
                    if (lv.tag == Value::TAG_DOUBLE) return value_double(lv.double_val - rv.double_val);
                    return value_null();
                case '*':
                    if (lv.tag == Value::TAG_INT64) return value_int(lv.int_val * rv.int_val);
                    if (lv.tag == Value::TAG_DOUBLE) return value_double(lv.double_val * rv.double_val);
                    return value_null();
                case '/':
                    if (lv.tag == Value::TAG_INT64) {
                        if (rv.int_val == 0) return value_null();
                        return value_int(lv.int_val / rv.int_val);
                    }
                    if (lv.tag == Value::TAG_DOUBLE) {
                        if (rv.double_val == 0.0) return value_null();
                        return value_double(lv.double_val / rv.double_val);
                    }
                    return value_null();
                case '%':
                    if (lv.tag == Value::TAG_INT64) {
                        if (rv.int_val == 0) return value_null();
                        return value_int(lv.int_val % rv.int_val);
                    }
                    if (lv.tag == Value::TAG_DOUBLE) {
                        if (rv.double_val == 0.0) return value_null();
                        return value_double(std::fmod(lv.double_val, rv.double_val));
                    }
                    return value_null();
                default:
                    break;
            }
        }

        // --- Comparison operators ---
        // =
        if (op.len == 1 && op.ptr[0] == '=') {
            if (lv.tag == Value::TAG_INT64)  return value_bool(lv.int_val == rv.int_val);
            if (lv.tag == Value::TAG_DOUBLE) return value_bool(lv.double_val == rv.double_val);
            if (lv.tag == Value::TAG_STRING) return value_bool(lv.str_val == rv.str_val);
            if (lv.tag == Value::TAG_BOOL)   return value_bool(lv.bool_val == rv.bool_val);
            return value_null();
        }
        // <> or !=
        if ((op.len == 2 && op.ptr[0] == '<' && op.ptr[1] == '>') ||
            (op.len == 2 && op.ptr[0] == '!' && op.ptr[1] == '=')) {
            if (lv.tag == Value::TAG_INT64)  return value_bool(lv.int_val != rv.int_val);
            if (lv.tag == Value::TAG_DOUBLE) return value_bool(lv.double_val != rv.double_val);
            if (lv.tag == Value::TAG_STRING) return value_bool(lv.str_val != rv.str_val);
            if (lv.tag == Value::TAG_BOOL)   return value_bool(lv.bool_val != rv.bool_val);
            return value_null();
        }
        // <
        if (op.len == 1 && op.ptr[0] == '<') {
            if (lv.tag == Value::TAG_INT64)  return value_bool(lv.int_val < rv.int_val);
            if (lv.tag == Value::TAG_DOUBLE) return value_bool(lv.double_val < rv.double_val);
            if (lv.tag == Value::TAG_STRING) {
                int cmp = std::memcmp(lv.str_val.ptr, rv.str_val.ptr,
                    lv.str_val.len < rv.str_val.len ? lv.str_val.len : rv.str_val.len);
                if (cmp == 0) return value_bool(lv.str_val.len < rv.str_val.len);
                return value_bool(cmp < 0);
            }
            return value_null();
        }
        // >
        if (op.len == 1 && op.ptr[0] == '>') {
            if (lv.tag == Value::TAG_INT64)  return value_bool(lv.int_val > rv.int_val);
            if (lv.tag == Value::TAG_DOUBLE) return value_bool(lv.double_val > rv.double_val);
            if (lv.tag == Value::TAG_STRING) {
                int cmp = std::memcmp(lv.str_val.ptr, rv.str_val.ptr,
                    lv.str_val.len < rv.str_val.len ? lv.str_val.len : rv.str_val.len);
                if (cmp == 0) return value_bool(lv.str_val.len > rv.str_val.len);
                return value_bool(cmp > 0);
            }
            return value_null();
        }
        // <=
        if (op.len == 2 && op.ptr[0] == '<' && op.ptr[1] == '=') {
            if (lv.tag == Value::TAG_INT64)  return value_bool(lv.int_val <= rv.int_val);
            if (lv.tag == Value::TAG_DOUBLE) return value_bool(lv.double_val <= rv.double_val);
            if (lv.tag == Value::TAG_STRING) {
                int cmp = std::memcmp(lv.str_val.ptr, rv.str_val.ptr,
                    lv.str_val.len < rv.str_val.len ? lv.str_val.len : rv.str_val.len);
                if (cmp == 0) return value_bool(lv.str_val.len <= rv.str_val.len);
                return value_bool(cmp < 0);
            }
            return value_null();
        }
        // >=
        if (op.len == 2 && op.ptr[0] == '>' && op.ptr[1] == '=') {
            if (lv.tag == Value::TAG_INT64)  return value_bool(lv.int_val >= rv.int_val);
            if (lv.tag == Value::TAG_DOUBLE) return value_bool(lv.double_val >= rv.double_val);
            if (lv.tag == Value::TAG_STRING) {
                int cmp = std::memcmp(lv.str_val.ptr, rv.str_val.ptr,
                    lv.str_val.len < rv.str_val.len ? lv.str_val.len : rv.str_val.len);
                if (cmp == 0) return value_bool(lv.str_val.len >= rv.str_val.len);
                return value_bool(cmp >= 0);
            }
            return value_null();
        }

        return value_null();  // unknown operator
    }

    // ---- IS NULL / IS NOT NULL (never return NULL) ----

    case NodeType::NODE_IS_NULL: {
        Value child = evaluate_expression<D>(expr->first_child, resolve, functions, arena);
        return value_bool(child.is_null());
    }

    case NodeType::NODE_IS_NOT_NULL: {
        Value child = evaluate_expression<D>(expr->first_child, resolve, functions, arena);
        return value_bool(!child.is_null());
    }

    // ---- BETWEEN: expr >= low AND expr <= high ----

    case NodeType::NODE_BETWEEN: {
        const AstNode* expr_node = detail::nth_child(expr, 0);
        const AstNode* low_node  = detail::nth_child(expr, 1);
        const AstNode* high_node = detail::nth_child(expr, 2);
        if (!expr_node || !low_node || !high_node) return value_null();

        Value val  = evaluate_expression<D>(expr_node, resolve, functions, arena);
        Value low  = evaluate_expression<D>(low_node,  resolve, functions, arena);
        Value high = evaluate_expression<D>(high_node, resolve, functions, arena);

        // NULL propagation
        if (val.is_null() || low.is_null() || high.is_null()) return value_null();

        // Coerce all three to common type
        SqlType::Kind k1 = tag_to_kind(val.tag);
        SqlType::Kind k2 = tag_to_kind(low.tag);
        SqlType::Kind k3 = tag_to_kind(high.tag);
        SqlType::Kind common = CoercionRules<D>::common_type(
            CoercionRules<D>::common_type(k1, k2), k3);
        if (common == SqlType::UNKNOWN) return value_null();
        Value::Tag target = kind_to_tag(common);

        Value v = CoercionRules<D>::coerce_value(val,  target, arena);
        Value l = CoercionRules<D>::coerce_value(low,  target, arena);
        Value h = CoercionRules<D>::coerce_value(high, target, arena);
        if (v.is_null() || l.is_null() || h.is_null()) return value_null();

        // Compare: v >= l AND v <= h
        bool ge = false, le = false;
        if (v.tag == Value::TAG_INT64) {
            ge = v.int_val >= l.int_val;
            le = v.int_val <= h.int_val;
        } else if (v.tag == Value::TAG_DOUBLE) {
            ge = v.double_val >= l.double_val;
            le = v.double_val <= h.double_val;
        } else if (v.tag == Value::TAG_STRING) {
            auto scmp = [](StringRef a, StringRef b) -> int {
                uint32_t minlen = a.len < b.len ? a.len : b.len;
                int r = std::memcmp(a.ptr, b.ptr, minlen);
                if (r != 0) return r;
                return (a.len < b.len) ? -1 : (a.len > b.len) ? 1 : 0;
            };
            ge = scmp(v.str_val, l.str_val) >= 0;
            le = scmp(v.str_val, h.str_val) <= 0;
        } else {
            return value_null();
        }
        return value_bool(ge && le);
    }

    // ---- IN list ----

    case NodeType::NODE_IN_LIST: {
        const AstNode* expr_node = expr->first_child;
        if (!expr_node) return value_null();

        Value val = evaluate_expression<D>(expr_node, resolve, functions, arena);
        if (val.is_null()) return value_null();

        bool found = false;
        bool has_null = false;

        for (const AstNode* item = expr_node->next_sibling; item; item = item->next_sibling) {
            Value item_val = evaluate_expression<D>(item, resolve, functions, arena);
            if (item_val.is_null()) {
                has_null = true;
                continue;
            }
            // Coerce and compare
            SqlType::Kind lk = tag_to_kind(val.tag);
            SqlType::Kind rk = tag_to_kind(item_val.tag);
            SqlType::Kind common = CoercionRules<D>::common_type(lk, rk);
            if (common == SqlType::UNKNOWN) continue;
            Value::Tag target = kind_to_tag(common);
            Value lv = CoercionRules<D>::coerce_value(val,      target, arena);
            Value rv = CoercionRules<D>::coerce_value(item_val, target, arena);
            if (lv.is_null() || rv.is_null()) { has_null = true; continue; }

            bool eq = false;
            if (lv.tag == Value::TAG_INT64)  eq = lv.int_val == rv.int_val;
            else if (lv.tag == Value::TAG_DOUBLE) eq = lv.double_val == rv.double_val;
            else if (lv.tag == Value::TAG_STRING) eq = lv.str_val == rv.str_val;
            else if (lv.tag == Value::TAG_BOOL) eq = lv.bool_val == rv.bool_val;

            if (eq) { found = true; break; }
        }

        if (found) return value_bool(true);
        if (has_null) return value_null();
        return value_bool(false);
    }

    // ---- CASE/WHEN ----

    case NodeType::NODE_CASE_WHEN: {
        uint32_t count = detail::child_count(expr);
        bool is_simple = (expr->flags == 1);

        if (is_simple) {
            // Simple CASE: children = [case_expr, when1, then1, when2, then2, ..., else?]
            const AstNode* case_node = expr->first_child;
            if (!case_node) return value_null();
            Value case_val = evaluate_expression<D>(case_node, resolve, functions, arena);

            const AstNode* child = case_node->next_sibling;
            uint32_t remaining = count - 1;  // excluding case_expr

            while (child && child->next_sibling) {
                Value when_val = evaluate_expression<D>(child, resolve, functions, arena);
                const AstNode* then_node = child->next_sibling;

                // Compare case_val = when_val
                bool match = false;
                if (!case_val.is_null() && !when_val.is_null()) {
                    SqlType::Kind lk = tag_to_kind(case_val.tag);
                    SqlType::Kind rk = tag_to_kind(when_val.tag);
                    SqlType::Kind common = CoercionRules<D>::common_type(lk, rk);
                    if (common != SqlType::UNKNOWN) {
                        Value::Tag target = kind_to_tag(common);
                        Value lv = CoercionRules<D>::coerce_value(case_val, target, arena);
                        Value rv = CoercionRules<D>::coerce_value(when_val, target, arena);
                        if (!lv.is_null() && !rv.is_null()) {
                            if (lv.tag == Value::TAG_INT64)  match = lv.int_val == rv.int_val;
                            else if (lv.tag == Value::TAG_DOUBLE) match = lv.double_val == rv.double_val;
                            else if (lv.tag == Value::TAG_STRING) match = lv.str_val == rv.str_val;
                            else if (lv.tag == Value::TAG_BOOL)   match = lv.bool_val == rv.bool_val;
                        }
                    }
                }

                if (match) {
                    return evaluate_expression<D>(then_node, resolve, functions, arena);
                }

                child = then_node->next_sibling;
                remaining -= 2;
            }

            // Check for ELSE (one remaining child)
            if (child && remaining == 1) {
                return evaluate_expression<D>(child, resolve, functions, arena);
            }
            return value_null();
        } else {
            // Searched CASE: children = [when1, then1, when2, then2, ..., else?]
            const AstNode* child = expr->first_child;
            uint32_t remaining = count;

            while (child && child->next_sibling) {
                Value when_val = evaluate_expression<D>(child, resolve, functions, arena);
                const AstNode* then_node = child->next_sibling;

                // Evaluate WHEN condition as boolean
                bool is_true = false;
                if (!when_val.is_null()) {
                    if (when_val.tag == Value::TAG_BOOL) is_true = when_val.bool_val;
                    else if (when_val.tag == Value::TAG_INT64) is_true = when_val.int_val != 0;
                    else if (when_val.tag == Value::TAG_DOUBLE) is_true = when_val.double_val != 0.0;
                    else is_true = true;  // non-null non-numeric = truthy
                }

                if (is_true) {
                    return evaluate_expression<D>(then_node, resolve, functions, arena);
                }

                child = then_node->next_sibling;
                remaining -= 2;
            }

            // Check for ELSE (one remaining child)
            if (child && remaining % 2 == 1) {
                return evaluate_expression<D>(child, resolve, functions, arena);
            }
            return value_null();
        }
    }

    // ---- Function calls ----

    case NodeType::NODE_FUNCTION_CALL: {
        StringRef name = expr->value();
        const FunctionEntry* entry = functions.lookup(name.ptr, name.len);
        if (!entry) return value_null();

        // Evaluate arguments
        uint32_t argc = detail::child_count(expr);
        constexpr uint32_t MAX_ARGS = 64;
        Value args[MAX_ARGS];
        uint32_t i = 0;
        for (const AstNode* arg = expr->first_child; arg && i < MAX_ARGS;
             arg = arg->next_sibling, ++i) {
            args[i] = evaluate_expression<D>(arg, resolve, functions, arena);
        }
        return entry->impl(args, static_cast<uint16_t>(i), arena);
    }

    // ---- Deferred node types (return value_null) ----

    case NodeType::NODE_SUBQUERY:          return value_null();  // requires full executor
    case NodeType::NODE_TUPLE:             return value_null();  // requires row/tuple value type
    case NodeType::NODE_ARRAY_CONSTRUCTOR: return value_null();  // requires array value type
    case NodeType::NODE_ARRAY_SUBSCRIPT:   return value_null();  // requires array support
    case NodeType::NODE_FIELD_ACCESS:      return value_null();  // requires composite type

    default:
        return value_null();  // unknown node type
    }
}

} // namespace sql_engine

#endif // SQL_ENGINE_EXPRESSION_EVAL_H
```

- [ ] **Step 2: Create `tests/test_expression_eval.cpp`**

This test file tests each node type by constructing AST nodes directly (not via the parser). This isolates the evaluator from parser behavior.

Create `tests/test_expression_eval.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include <string>

using namespace sql_engine;
using namespace sql_parser;

// Test fixture with arena, registry, and a simple column resolver.
class ExprEvalTest : public ::testing::Test {
protected:
    Arena arena{4096};
    FunctionRegistry<Dialect::MySQL> mysql_funcs;
    FunctionRegistry<Dialect::PostgreSQL> pg_funcs;

    void SetUp() override {
        mysql_funcs.register_builtins();
        pg_funcs.register_builtins();
    }

    // Default resolver: resolve "x" -> 10, "y" -> 20, "name" -> "hello"
    std::function<Value(StringRef)> resolver = [](StringRef name) -> Value {
        if (name.equals_ci("x", 1)) return value_int(10);
        if (name.equals_ci("y", 1)) return value_int(20);
        if (name.equals_ci("name", 4)) return value_string(StringRef{"hello", 5});
        if (name.equals_ci("flag", 4)) return value_bool(true);
        if (name.equals_ci("nothing", 7)) return value_null();
        if (name.equals_ci("t.col", 5)) return value_int(42);
        return value_null();
    };

    // Helper: make a leaf node
    AstNode* leaf(NodeType type, const char* val) {
        return make_node(arena, type, StringRef{val, static_cast<uint32_t>(std::strlen(val))});
    }

    // Helper: make a node with children
    AstNode* node_with_children(NodeType type, const char* val,
                                std::initializer_list<AstNode*> children) {
        AstNode* n = make_node(arena, type,
            val ? StringRef{val, static_cast<uint32_t>(std::strlen(val))} : StringRef{});
        for (AstNode* c : children) n->add_child(c);
        return n;
    }

    // Helper: evaluate with MySQL dialect
    Value eval_mysql(AstNode* node) {
        return evaluate_expression<Dialect::MySQL>(node, resolver, mysql_funcs, arena);
    }

    // Helper: evaluate with PostgreSQL dialect
    Value eval_pg(AstNode* node) {
        return evaluate_expression<Dialect::PostgreSQL>(node, resolver, pg_funcs, arena);
    }
};

// ===== Leaf Nodes =====

TEST_F(ExprEvalTest, LiteralInt) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "42"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, LiteralIntNegative) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "-7"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, -7);
}

TEST_F(ExprEvalTest, LiteralIntTrue) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "TRUE"));
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, LiteralIntFalse) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_INT, "FALSE"));
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

TEST_F(ExprEvalTest, LiteralFloat) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_FLOAT, "3.14"));
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.14);
}

TEST_F(ExprEvalTest, LiteralString) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_STRING, "hello"));
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello");
}

TEST_F(ExprEvalTest, LiteralNull) {
    auto v = eval_mysql(leaf(NodeType::NODE_LITERAL_NULL, "NULL"));
    EXPECT_TRUE(v.is_null());
}

TEST_F(ExprEvalTest, ColumnRef) {
    auto v = eval_mysql(leaf(NodeType::NODE_COLUMN_REF, "x"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 10);
}

TEST_F(ExprEvalTest, ColumnRefUnknown) {
    auto v = eval_mysql(leaf(NodeType::NODE_COLUMN_REF, "unknown_col"));
    EXPECT_TRUE(v.is_null());
}

TEST_F(ExprEvalTest, Identifier) {
    auto v = eval_mysql(leaf(NodeType::NODE_IDENTIFIER, "y"));
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 20);
}

TEST_F(ExprEvalTest, Asterisk) {
    auto v = eval_mysql(leaf(NodeType::NODE_ASTERISK, "*"));
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "*");
}

TEST_F(ExprEvalTest, Placeholder) {
    auto v = eval_mysql(leaf(NodeType::NODE_PLACEHOLDER, "?"));
    EXPECT_TRUE(v.is_null());
}

TEST_F(ExprEvalTest, QualifiedName) {
    AstNode* tbl = leaf(NodeType::NODE_IDENTIFIER, "t");
    AstNode* col = leaf(NodeType::NODE_IDENTIFIER, "col");
    AstNode* qn = node_with_children(NodeType::NODE_QUALIFIED_NAME, nullptr, {tbl, col});
    auto v = eval_mysql(qn);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, ExpressionWrapper) {
    AstNode* inner = leaf(NodeType::NODE_LITERAL_INT, "99");
    AstNode* wrapper = node_with_children(NodeType::NODE_EXPRESSION, nullptr, {inner});
    auto v = eval_mysql(wrapper);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 99);
}

TEST_F(ExprEvalTest, NullExpr) {
    EXPECT_TRUE(eval_mysql(nullptr).is_null());
}

// ===== Unary Operators =====

TEST_F(ExprEvalTest, UnaryMinus) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "42");
    AstNode* neg = node_with_children(NodeType::NODE_UNARY_OP, "-", {child});
    auto v = eval_mysql(neg);
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, -42);
}

TEST_F(ExprEvalTest, UnaryMinusDouble) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_FLOAT, "3.14");
    AstNode* neg = node_with_children(NodeType::NODE_UNARY_OP, "-", {child});
    auto v = eval_mysql(neg);
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, -3.14);
}

TEST_F(ExprEvalTest, UnaryMinusNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* neg = node_with_children(NodeType::NODE_UNARY_OP, "-", {child});
    EXPECT_TRUE(eval_mysql(neg).is_null());
}

TEST_F(ExprEvalTest, UnaryNotTrue) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    auto v = eval_mysql(not_node);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

TEST_F(ExprEvalTest, UnaryNotFalse) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    auto v = eval_mysql(not_node);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, UnaryNotNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    EXPECT_TRUE(eval_mysql(not_node).is_null());
}

TEST_F(ExprEvalTest, UnaryNotInt) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "0");
    AstNode* not_node = node_with_children(NodeType::NODE_UNARY_OP, "NOT", {child});
    auto v = eval_mysql(not_node);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);  // NOT 0 = TRUE
}

// ===== Binary Arithmetic =====

TEST_F(ExprEvalTest, Add) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "+", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(ExprEvalTest, SubtractDouble) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_FLOAT, "10.5");
    AstNode* r = leaf(NodeType::NODE_LITERAL_FLOAT, "3.2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "-", {l, r});
    auto v = eval_mysql(op);
    EXPECT_NEAR(v.double_val, 7.3, 1e-9);
}

TEST_F(ExprEvalTest, Multiply) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "6");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "7");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "*", {l, r});
    EXPECT_EQ(eval_mysql(op).int_val, 42);
}

TEST_F(ExprEvalTest, Divide) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "/", {l, r});
    EXPECT_EQ(eval_mysql(op).int_val, 3);  // integer division
}

TEST_F(ExprEvalTest, DivisionByZeroReturnsNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "0");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "/", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, Modulo) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "%", {l, r});
    EXPECT_EQ(eval_mysql(op).int_val, 1);
}

TEST_F(ExprEvalTest, ArithmeticNullPropagation) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "+", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, IntDoubleCoercion) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_FLOAT, "2.5");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "+", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.5);
}

// ===== Comparison =====

TEST_F(ExprEvalTest, Equal) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "=", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, NotEqual) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<>", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LessThan) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, GreaterThan) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, ">", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, ComparisonNullPropagation) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "=", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, StringComparison) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "abc");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "def");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "<", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

// ===== Logical =====

TEST_F(ExprEvalTest, AndTrueTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, AndFalseShortCircuit) {
    // FALSE AND <anything> = FALSE, right side should not matter
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, AndNullTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "AND", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

TEST_F(ExprEvalTest, OrFalseTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, OrTrueShortCircuit) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, OrNullFalse) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "OR", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

// ===== IS / IS NOT =====

TEST_F(ExprEvalTest, IsTrueOnTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsTrueOnFalse) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsTrueOnNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, IsNotFalseOnTrue) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "IS NOT", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

// ===== LIKE =====

TEST_F(ExprEvalTest, LikeMatch) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "hel%");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "LIKE", {l, r});
    EXPECT_TRUE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LikeNoMatch) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "xyz%");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "LIKE", {l, r});
    EXPECT_FALSE(eval_mysql(op).bool_val);
}

TEST_F(ExprEvalTest, LikeNull) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, "%");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "LIKE", {l, r});
    EXPECT_TRUE(eval_mysql(op).is_null());
}

// ===== || : concat in PgSQL, OR in MySQL =====

TEST_F(ExprEvalTest, DoublePipePgConcat) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* r = leaf(NodeType::NODE_LITERAL_STRING, " world");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "||", {l, r});
    auto v = eval_pg(op);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello world");
}

TEST_F(ExprEvalTest, DoublePipeMySQLOr) {
    AstNode* l = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* r = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* op = node_with_children(NodeType::NODE_BINARY_OP, "||", {l, r});
    auto v = eval_mysql(op);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

// ===== IS NULL / IS NOT NULL =====

TEST_F(ExprEvalTest, IsNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* n = node_with_children(NodeType::NODE_IS_NULL, nullptr, {child});
    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(ExprEvalTest, IsNullOnNonNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_IS_NULL, nullptr, {child});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, IsNotNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_IS_NOT_NULL, nullptr, {child});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, IsNotNullOnNull) {
    AstNode* child = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* n = node_with_children(NodeType::NODE_IS_NOT_NULL, nullptr, {child});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

// ===== BETWEEN =====

TEST_F(ExprEvalTest, BetweenInRange) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "5");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, BetweenOutOfRange) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "15");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, BetweenBoundaryInclusive) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, BetweenNull) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* low  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* high = leaf(NodeType::NODE_LITERAL_INT, "10");
    AstNode* n = node_with_children(NodeType::NODE_BETWEEN, nullptr, {expr, low, high});
    EXPECT_TRUE(eval_mysql(n).is_null());
}

// ===== IN list =====

TEST_F(ExprEvalTest, InListMatch) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* v2   = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* v3   = leaf(NodeType::NODE_LITERAL_INT, "3");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1, v2, v3});
    EXPECT_TRUE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, InListNoMatch) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "5");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* v2   = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1, v2});
    EXPECT_FALSE(eval_mysql(n).bool_val);
}

TEST_F(ExprEvalTest, InListNullExpr) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1});
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, InListNullInValues) {
    AstNode* expr = leaf(NodeType::NODE_LITERAL_INT, "5");
    AstNode* v1   = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* v2   = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* n = node_with_children(NodeType::NODE_IN_LIST, nullptr, {expr, v1, v2});
    // No match, but has NULL -> result is NULL
    EXPECT_TRUE(eval_mysql(n).is_null());
}

// ===== CASE/WHEN =====

TEST_F(ExprEvalTest, SearchedCaseWhen) {
    // CASE WHEN TRUE THEN 1 ELSE 2 END  (flags=0: searched)
    AstNode* when_cond = leaf(NodeType::NODE_LITERAL_INT, "TRUE");
    AstNode* then_val  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* else_val  = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {when_cond, then_val, else_val});
    n->flags = 0;
    auto v = eval_mysql(n);
    EXPECT_EQ(v.int_val, 1);
}

TEST_F(ExprEvalTest, SearchedCaseElse) {
    // CASE WHEN FALSE THEN 1 ELSE 2 END
    AstNode* when_cond = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* then_val  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* else_val  = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {when_cond, then_val, else_val});
    n->flags = 0;
    auto v = eval_mysql(n);
    EXPECT_EQ(v.int_val, 2);
}

TEST_F(ExprEvalTest, SearchedCaseNoMatch) {
    // CASE WHEN FALSE THEN 1 END -> NULL
    AstNode* when_cond = leaf(NodeType::NODE_LITERAL_INT, "FALSE");
    AstNode* then_val  = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {when_cond, then_val});
    n->flags = 0;
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, SimpleCaseWhen) {
    // CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END
    AstNode* case_expr = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* when1 = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* then1 = leaf(NodeType::NODE_LITERAL_STRING, "one");
    AstNode* when2 = leaf(NodeType::NODE_LITERAL_INT, "2");
    AstNode* then2 = leaf(NodeType::NODE_LITERAL_STRING, "two");
    AstNode* else_val = leaf(NodeType::NODE_LITERAL_STRING, "other");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {case_expr, when1, then1, when2, then2, else_val});
    n->flags = 1;  // simple CASE
    auto v = eval_mysql(n);
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "one");
}

TEST_F(ExprEvalTest, SimpleCaseElse) {
    // CASE 99 WHEN 1 THEN 'one' ELSE 'other' END
    AstNode* case_expr = leaf(NodeType::NODE_LITERAL_INT, "99");
    AstNode* when1 = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* then1 = leaf(NodeType::NODE_LITERAL_STRING, "one");
    AstNode* else_val = leaf(NodeType::NODE_LITERAL_STRING, "other");
    AstNode* n = node_with_children(NodeType::NODE_CASE_WHEN, nullptr,
                                     {case_expr, when1, then1, else_val});
    n->flags = 1;
    auto v = eval_mysql(n);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "other");
}

// ===== Function Calls =====

TEST_F(ExprEvalTest, FunctionCallAbs) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_INT, "-42");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "ABS", {arg});
    auto v = eval_mysql(fn);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, FunctionCallUpper) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_STRING, "hello");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "UPPER", {arg});
    auto v = eval_mysql(fn);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "HELLO");
}

TEST_F(ExprEvalTest, FunctionCallCoalesce) {
    AstNode* a1 = leaf(NodeType::NODE_LITERAL_NULL, "NULL");
    AstNode* a2 = leaf(NodeType::NODE_LITERAL_INT, "42");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "COALESCE", {a1, a2});
    auto v = eval_mysql(fn);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(ExprEvalTest, FunctionCallUnknown) {
    AstNode* arg = leaf(NodeType::NODE_LITERAL_INT, "1");
    AstNode* fn = node_with_children(NodeType::NODE_FUNCTION_CALL, "NOEXIST", {arg});
    EXPECT_TRUE(eval_mysql(fn).is_null());
}

// ===== Deferred Nodes =====

TEST_F(ExprEvalTest, SubqueryReturnsNull) {
    AstNode* n = make_node(arena, NodeType::NODE_SUBQUERY);
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, TupleReturnsNull) {
    AstNode* n = make_node(arena, NodeType::NODE_TUPLE);
    EXPECT_TRUE(eval_mysql(n).is_null());
}

TEST_F(ExprEvalTest, ArrayConstructorReturnsNull) {
    AstNode* n = make_node(arena, NodeType::NODE_ARRAY_CONSTRUCTOR);
    EXPECT_TRUE(eval_mysql(n).is_null());
}
```

- [ ] **Step 3: Update `Makefile.new`**

Add `test_expression_eval.cpp` to the `TEST_SRCS` list. Find the line with `test_like.cpp` (added in Task 1) and add after it:
```
            $(TEST_DIR)/test_expression_eval.cpp
```

- [ ] **Step 4: Build and verify**

Run:
```bash
make -f Makefile.new test 2>&1 | tail -30
```

All tests must pass.

---

### Task 3: Binary Operators -- Arithmetic, Comparison, Logical

**Files:**
- None new. This task is already implemented in `expression_eval.h` from Task 2.

The full `NODE_BINARY_OP` handling is included in the `expression_eval.h` created in Task 2 because the switch-dispatch structure naturally houses all operators together. Task 2's implementation already includes:

- Arithmetic: `+`, `-`, `*`, `/`, `%` with int64/double coercion
- Comparison: `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=` with coercion
- Logical: `AND` (short-circuit), `OR` (short-circuit) via `null_semantics`
- IS / IS NOT: IS TRUE, IS FALSE, IS NOT TRUE, IS NOT FALSE (never return NULL)
- LIKE: delegates to `match_like<D>()`
- `||`: PostgreSQL concat vs MySQL OR
- Division by zero returns NULL
- NULL propagation on all standard binary operators

Tests for all of these are in `test_expression_eval.cpp` from Task 2. No additional files needed.

- [ ] **Verify**: Confirm all binary operator tests pass.

---

### Task 4: Special Forms -- IS NULL, BETWEEN, IN, CASE/WHEN, Function Calls

**Files:**
- None new. This task is already implemented in `expression_eval.h` from Task 2.

The full handling for these special forms is included in the `expression_eval.h` created in Task 2:

- `NODE_IS_NULL` / `NODE_IS_NOT_NULL`: evaluate child, return `value_bool(child.is_null())`
- `NODE_BETWEEN`: three children, coerce to common type, check `>=` low AND `<=` high
- `NODE_IN_LIST`: N children, first is expression, rest are values; handles NULL propagation correctly
- `NODE_CASE_WHEN`: uses `flags` field (0=searched, 1=simple) to disambiguate
- `NODE_FUNCTION_CALL`: lookup in `FunctionRegistry<D>`, evaluate args, call

Tests for all of these are in `test_expression_eval.cpp` from Task 2. No additional files needed.

- [ ] **Verify**: Confirm all special form tests pass.

---

### Task 5: Integration Tests -- Parse SQL -> Evaluate

**Files:**
- Create: `tests/test_eval_integration.cpp`
- Modify: `Makefile.new`

This is the milestone: parser and engine connect end-to-end. Parse real SQL strings, navigate the AST to find the expression node, evaluate it, and verify the result.

- [ ] **Step 1: Create `tests/test_eval_integration.cpp`**

Create `tests/test_eval_integration.cpp`:
```cpp
#include <gtest/gtest.h>
#include "sql_engine/expression_eval.h"
#include "sql_engine/function_registry.h"
#include "sql_parser/parser.h"
#include "sql_parser/common.h"
#include <string>

using namespace sql_engine;
using namespace sql_parser;

// Integration test fixture: parse SQL, navigate to expression, evaluate.
class EvalIntegrationTest : public ::testing::Test {
protected:
    Parser<Dialect::MySQL> mysql_parser;
    FunctionRegistry<Dialect::MySQL> mysql_funcs;

    Parser<Dialect::PostgreSQL> pg_parser;
    FunctionRegistry<Dialect::PostgreSQL> pg_funcs;

    // No columns needed for these tests -- resolver always returns NULL.
    std::function<Value(StringRef)> no_columns = [](StringRef) -> Value {
        return value_null();
    };

    void SetUp() override {
        mysql_funcs.register_builtins();
        pg_funcs.register_builtins();
    }

    // Parse a SELECT, navigate to the first select item's expression, evaluate.
    // SELECT <expr>  ->  AST: SELECT_STMT -> SELECT_ITEM_LIST -> SELECT_ITEM -> expr
    Value eval_select_mysql(const char* sql) {
        auto r = mysql_parser.parse(sql, std::strlen(sql));
        EXPECT_EQ(r.status, ParseResult::OK) << "Failed to parse: " << sql;
        if (!r.ast) return value_null();

        // Navigate: SELECT_STMT -> SELECT_ITEM_LIST -> first SELECT_ITEM -> first child (expression)
        const AstNode* item_list = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
        if (!item_list || !item_list->first_child) return value_null();
        const AstNode* first_item = item_list->first_child;

        // The select item's first child is the expression
        const AstNode* expr = first_item->first_child;
        if (!expr) return value_null();

        Value result = evaluate_expression<Dialect::MySQL>(
            expr, no_columns, mysql_funcs, mysql_parser.arena());
        mysql_parser.reset();
        return result;
    }

    Value eval_select_pg(const char* sql) {
        auto r = pg_parser.parse(sql, std::strlen(sql));
        EXPECT_EQ(r.status, ParseResult::OK) << "Failed to parse: " << sql;
        if (!r.ast) return value_null();

        const AstNode* item_list = find_child(r.ast, NodeType::NODE_SELECT_ITEM_LIST);
        if (!item_list || !item_list->first_child) return value_null();
        const AstNode* first_item = item_list->first_child;
        const AstNode* expr = first_item->first_child;
        if (!expr) return value_null();

        Value result = evaluate_expression<Dialect::PostgreSQL>(
            expr, no_columns, pg_funcs, pg_parser.arena());
        pg_parser.reset();
        return result;
    }

    // Helper to find a child node by type
    static const AstNode* find_child(const AstNode* node, NodeType type) {
        for (const AstNode* c = node->first_child; c; c = c->next_sibling) {
            if (c->type == type) return c;
        }
        return nullptr;
    }
};

// ===== Literal Evaluation =====

TEST_F(EvalIntegrationTest, SelectInteger) {
    auto v = eval_select_mysql("SELECT 42");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectFloat) {
    auto v = eval_select_mysql("SELECT 3.14");
    EXPECT_EQ(v.tag, Value::TAG_DOUBLE);
    EXPECT_DOUBLE_EQ(v.double_val, 3.14);
}

TEST_F(EvalIntegrationTest, SelectString) {
    auto v = eval_select_mysql("SELECT 'hello'");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello");
}

TEST_F(EvalIntegrationTest, SelectNull) {
    auto v = eval_select_mysql("SELECT NULL");
    EXPECT_TRUE(v.is_null());
}

TEST_F(EvalIntegrationTest, SelectTrue) {
    auto v = eval_select_mysql("SELECT TRUE");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectFalse) {
    auto v = eval_select_mysql("SELECT FALSE");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

// ===== Arithmetic =====

TEST_F(EvalIntegrationTest, SelectOnePlusTwo) {
    auto v = eval_select_mysql("SELECT 1 + 2");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(EvalIntegrationTest, SelectArithmeticPrecedence) {
    auto v = eval_select_mysql("SELECT 2 + 3 * 4");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 14);
}

TEST_F(EvalIntegrationTest, SelectDivisionByZero) {
    auto v = eval_select_mysql("SELECT 1 / 0");
    EXPECT_TRUE(v.is_null());
}

TEST_F(EvalIntegrationTest, SelectModulo) {
    auto v = eval_select_mysql("SELECT 10 % 3");
    EXPECT_EQ(v.int_val, 1);
}

// ===== Comparison =====

TEST_F(EvalIntegrationTest, SelectComparison) {
    auto v = eval_select_mysql("SELECT 1 < 2");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectEqualityTrue) {
    auto v = eval_select_mysql("SELECT 42 = 42");
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectEqualityFalse) {
    auto v = eval_select_mysql("SELECT 1 = 2");
    EXPECT_FALSE(v.bool_val);
}

// ===== Function Calls =====

TEST_F(EvalIntegrationTest, SelectUpperFunction) {
    auto v = eval_select_mysql("SELECT UPPER('hello')");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "HELLO");
}

TEST_F(EvalIntegrationTest, SelectCoalesceFunction) {
    auto v = eval_select_mysql("SELECT COALESCE(NULL, 42)");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectCoalesceMultipleNulls) {
    auto v = eval_select_mysql("SELECT COALESCE(NULL, NULL, 42)");
    EXPECT_EQ(v.int_val, 42);
}

TEST_F(EvalIntegrationTest, SelectAbsFunction) {
    auto v = eval_select_mysql("SELECT ABS(-7)");
    EXPECT_EQ(v.int_val, 7);
}

TEST_F(EvalIntegrationTest, SelectLengthFunction) {
    auto v = eval_select_mysql("SELECT LENGTH('hello')");
    EXPECT_EQ(v.int_val, 5);
}

// ===== CASE/WHEN =====

TEST_F(EvalIntegrationTest, SelectSearchedCase) {
    auto v = eval_select_mysql("SELECT CASE WHEN 1 > 2 THEN 'a' ELSE 'b' END");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "b");
}

TEST_F(EvalIntegrationTest, SelectSimpleCase) {
    auto v = eval_select_mysql("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "one");
}

TEST_F(EvalIntegrationTest, SelectSimpleCaseSecondBranch) {
    auto v = eval_select_mysql("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "two");
}

TEST_F(EvalIntegrationTest, SelectSimpleCaseElse) {
    auto v = eval_select_mysql("SELECT CASE 99 WHEN 1 THEN 'one' ELSE 'other' END");
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "other");
}

// ===== IN =====

TEST_F(EvalIntegrationTest, SelectInList) {
    auto v = eval_select_mysql("SELECT 1 IN (1, 2, 3)");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectNotInList) {
    auto v = eval_select_mysql("SELECT 5 IN (1, 2, 3)");
    EXPECT_FALSE(v.bool_val);
}

// ===== BETWEEN =====

TEST_F(EvalIntegrationTest, SelectBetween) {
    auto v = eval_select_mysql("SELECT 5 BETWEEN 1 AND 10");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectNotBetween) {
    auto v = eval_select_mysql("SELECT 15 BETWEEN 1 AND 10");
    EXPECT_FALSE(v.bool_val);
}

// ===== IS NULL =====

TEST_F(EvalIntegrationTest, SelectIsNull) {
    auto v = eval_select_mysql("SELECT NULL IS NULL");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectIsNotNull) {
    auto v = eval_select_mysql("SELECT 1 IS NOT NULL");
    EXPECT_TRUE(v.bool_val);
}

// ===== LIKE =====

TEST_F(EvalIntegrationTest, SelectLike) {
    auto v = eval_select_mysql("SELECT 'test' LIKE 't%'");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectLikeNoMatch) {
    auto v = eval_select_mysql("SELECT 'test' LIKE 'x%'");
    EXPECT_FALSE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectLikeUnderscore) {
    auto v = eval_select_mysql("SELECT 'test' LIKE 'tes_'");
    EXPECT_TRUE(v.bool_val);
}

// ===== Logical Operators =====

TEST_F(EvalIntegrationTest, SelectAndOr) {
    auto v = eval_select_mysql("SELECT TRUE AND FALSE");
    EXPECT_FALSE(v.bool_val);

    v = eval_select_mysql("SELECT TRUE OR FALSE");
    EXPECT_TRUE(v.bool_val);
}

TEST_F(EvalIntegrationTest, SelectNotExpression) {
    auto v = eval_select_mysql("SELECT NOT TRUE");
    EXPECT_EQ(v.tag, Value::TAG_BOOL);
    EXPECT_FALSE(v.bool_val);
}

// ===== Unary Minus =====

TEST_F(EvalIntegrationTest, SelectUnaryMinus) {
    auto v = eval_select_mysql("SELECT -42");
    EXPECT_EQ(v.tag, Value::TAG_INT64);
    EXPECT_EQ(v.int_val, -42);
}

// ===== PostgreSQL-specific =====

TEST_F(EvalIntegrationTest, PgSelectConcat) {
    auto v = eval_select_pg("SELECT 'hello' || ' world'");
    EXPECT_EQ(v.tag, Value::TAG_STRING);
    EXPECT_EQ(std::string(v.str_val.ptr, v.str_val.len), "hello world");
}

TEST_F(EvalIntegrationTest, PgSelectCaseSensitiveLike) {
    auto v = eval_select_pg("SELECT 'Hello' LIKE 'hello'");
    EXPECT_FALSE(v.bool_val);  // PostgreSQL is case-sensitive
}

// ===== Complex Expressions =====

TEST_F(EvalIntegrationTest, NestedArithmetic) {
    auto v = eval_select_mysql("SELECT (1 + 2) * (3 + 4)");
    EXPECT_EQ(v.int_val, 21);
}

TEST_F(EvalIntegrationTest, NestedFunctionCalls) {
    auto v = eval_select_mysql("SELECT ABS(-1) + ABS(-2)");
    EXPECT_EQ(v.int_val, 3);
}

TEST_F(EvalIntegrationTest, NullArithmeticPropagation) {
    auto v = eval_select_mysql("SELECT NULL + 1");
    EXPECT_TRUE(v.is_null());
}
```

- [ ] **Step 2: Update `Makefile.new`**

Add `test_eval_integration.cpp` to the `TEST_SRCS` list. Find the line with `test_expression_eval.cpp` (added in Task 2) and add after it:
```
            $(TEST_DIR)/test_eval_integration.cpp
```

- [ ] **Step 3: Build and run full test suite**

Run:
```bash
make -f Makefile.new test 2>&1 | tail -40
```

All tests must pass -- this is the milestone where parser and engine connect end-to-end.

- [ ] **Step 4: Verify test counts**

Expected new test count:
- `test_like.cpp`: ~20 tests
- `test_expression_eval.cpp`: ~55 tests
- `test_eval_integration.cpp`: ~35 tests
- Total new: ~110 tests

---

## Summary of Changes

| File | Action | Task |
|---|---|---|
| `include/sql_engine/tag_kind_map.h` | Create | 1 |
| `include/sql_engine/like.h` | Create | 1 |
| `include/sql_parser/expression_parser.h` | Modify (1 line) | 1 |
| `tests/test_like.cpp` | Create | 1 |
| `include/sql_engine/expression_eval.h` | Create | 2 |
| `tests/test_expression_eval.cpp` | Create | 2 |
| `tests/test_eval_integration.cpp` | Create | 5 |
| `Makefile.new` | Modify | 1, 2, 5 |

## Dependencies Between Tasks

```
Task 1 (LIKE + tag_kind_map + parser fix)
   |
   v
Task 2 (expression_eval.h + unit tests)  -- includes Task 3 and Task 4 content
   |
   v
Task 5 (integration tests)
```

Tasks 3 and 4 are verification-only steps since the full `expression_eval.h` is created in Task 2 with all operator and special form handling. This avoids partial-file creation and the complexity of extending a header across multiple tasks.
