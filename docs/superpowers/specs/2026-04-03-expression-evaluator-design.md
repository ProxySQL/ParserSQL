# SQL Engine Expression Evaluator — Design Specification

## Overview

The expression evaluator connects the SQL parser to the query engine. It takes a parser AST expression node, resolves column references via a caller-provided callback, and returns a `Value` using the type system from sub-project 1.

This is sub-project 2 of the query engine. It depends on sub-project 1 (type system) and the SQL parser.

### Goals

- **Evaluate any SQL expression** from the parser's AST: literals, column refs, arithmetic, comparison, logical operators, function calls, LIKE, IS NULL, BETWEEN, IN, CASE/WHEN
- **Dialect-aware** via `CoercionRules<D>` for type promotion and `FunctionRegistry<D>` for function dispatch
- **Correct NULL handling** via three-valued logic throughout
- **Column resolution via callback** — no row format imposed on the caller
- **Integration milestone** — first time parser and engine connect end-to-end

### Constraints

- C++17
- Uses parser's AST (`AstNode`, `NodeType`) and arena
- Uses type system's `Value`, `CoercionRules<D>`, `null_semantics`, `FunctionRegistry<D>`
- Header-only (`expression_eval.h`, `like.h`)
- No exceptions — errors return `value_null()`

### Non-Goals (deferred)

- Row/tuple representation (sub-project 4)
- Aggregate function evaluation (needs executor with row-group state)
- Subquery evaluation (needs full executor — `IN (SELECT ...)` returns NULL for now)
- Window functions
- ORDER BY / LIMIT execution

---

## Core Interface

```cpp
template <Dialect D>
Value evaluate_expression(const AstNode* expr,
                          const std::function<Value(StringRef)>& resolve,
                          FunctionRegistry<D>& functions,
                          Arena& arena);
```

**Parameters:**
- `expr` — AST expression node from the parser
- `resolve` — callback that maps column names to values. Called when the evaluator hits `NODE_COLUMN_REF` or `NODE_QUALIFIED_NAME`. For qualified names (`table.column`), the callback receives the full qualified string.
- `functions` — function registry with built-in functions registered
- `arena` — for allocating intermediate string results

**Returns:** A `Value`. Returns `value_null()` on error or unsupported node types.

---

## AST Node Dispatch

The evaluator switches on `expr->type`:

### Leaf nodes (no recursion)

| NodeType | Action |
|---|---|
| `NODE_LITERAL_INT` | Check for `"TRUE"`/`"FALSE"` text → `value_bool()`. Otherwise parse via `strtoll` → `value_int()` |
| `NODE_LITERAL_FLOAT` | Parse `expr->value()` string to double via `strtod` |
| `NODE_LITERAL_STRING` | `value_string(expr->value())` |
| `NODE_LITERAL_NULL` | `value_null()` |
| `NODE_COLUMN_REF` | `resolve(expr->value())` |
| `NODE_ASTERISK` | `value_string(StringRef{"*", 1})` (for `COUNT(*)`) |
| `NODE_PLACEHOLDER` | `value_null()` (unresolved placeholder) |
| `NODE_IDENTIFIER` | `resolve(expr->value())` (column or keyword-as-value) |

### Deferred node types (return value_null)

These node types are produced by the expression parser but require features beyond the expression evaluator's scope. They return `value_null()` with a clear comment for future implementation:

| NodeType | Reason deferred |
|---|---|
| `NODE_SUBQUERY` | Requires full executor |
| `NODE_TUPLE` | Row constructor — requires row/tuple value type |
| `NODE_ARRAY_CONSTRUCTOR` | ARRAY[...] — requires array value type |
| `NODE_ARRAY_SUBSCRIPT` | expr[index] — requires array support |
| `NODE_FIELD_ACCESS` | (expr).field — requires composite type support |
| `NODE_EXPRESSION` | Wrapper node — unwrap and evaluate first child |

Note: `NODE_EXPRESSION` should be handled by unwrapping (evaluating its first child), not by returning NULL.

### Qualified name

`NODE_QUALIFIED_NAME` has two children (table, column). Combine into `"table.column"` and call `resolve()`.

### Binary operators

`NODE_BINARY_OP` has value = operator text, two children (left, right).

```
1. If operator is AND/OR: use short-circuit evaluation (see below)
2. If operator is IS/IS NOT: handle IS TRUE/FALSE/NULL (see below)
3. Evaluate left child → left_val
4. Evaluate right child → right_val
5. NULL propagation: if either NULL → return value_null()
6. Map Value::Tag → SqlType::Kind for coercion lookup
7. Find common type: CoercionRules<D>::common_type(left_kind, right_kind)
8. Map result SqlType::Kind back to Value::Tag
9. Coerce both operands: CoercionRules<D>::coerce_value(val, target_tag, arena)
10. Apply operator → return result
```

**Value::Tag ↔ SqlType::Kind mapping:** The evaluator needs a helper to convert between these enums since `CoercionRules` operates on `SqlType::Kind` while runtime values carry `Value::Tag`. The mapping is straightforward (TAG_INT64→INT, TAG_DOUBLE→DOUBLE, TAG_STRING→VARCHAR, etc.).

**Short-circuit for AND/OR:**
- `AND`: if left is FALSE → return FALSE without evaluating right. If left is NULL → evaluate right; if right is FALSE → FALSE, else NULL.
- `OR`: if left is TRUE → return TRUE without evaluating right. If left is NULL → evaluate right; if right is TRUE → TRUE, else NULL.

**Arithmetic operators** (`+`, `-`, `*`, `/`, `%`, `DIV`, `MOD`):
- Operate on coerced numeric values (int64 or double)
- Division by zero → NULL
- `%` / `MOD` → integer remainder
- `DIV` → integer division (truncate toward zero)
- Note: `DIV` and `MOD` are MySQL keywords. If the parser doesn't currently produce them as `NODE_BINARY_OP` infix operators (they may not be in `infix_precedence`), the evaluator should handle them gracefully. They may arrive as function calls instead — the function registry already has `MOD` registered.

**Comparison operators** (`=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`):
- Compare coerced values
- Return `value_bool(result)`

**IS / IS NOT operators** (`IS`, `IS NOT`):
The parser produces `NODE_BINARY_OP` with value `"IS"` or `"IS NOT"` for `IS TRUE`, `IS FALSE`, `IS NOT TRUE`, `IS NOT FALSE`. The right child is a `NODE_LITERAL_INT` with text `"TRUE"` or `"FALSE"`.

- `expr IS TRUE` → evaluate expr; if it's truthy and not NULL → TRUE, else FALSE
- `expr IS FALSE` → evaluate expr; if it's falsy and not NULL → TRUE, else FALSE
- `expr IS NOT TRUE` → NOT (expr IS TRUE)
- `expr IS NOT FALSE` → NOT (expr IS FALSE)
- These NEVER return NULL — they always return TRUE or FALSE.

Note: `IS NULL` and `IS NOT NULL` are handled via separate `NODE_IS_NULL`/`NODE_IS_NOT_NULL` node types, not through this path.

**String operators:**
- `||` in PostgreSQL: string concatenation
- `||` in MySQL: logical OR
- `LIKE`: delegate to `match_like<D>()`

### Unary operators

`NODE_UNARY_OP` has value = operator text, one child.

| Operator | Action |
|---|---|
| `-` | Negate: int → `-int_val`, double → `-double_val`. NULL → NULL. |
| `NOT` | `null_semantics::eval_not(child_val)` |

Note: The parser does not produce `NODE_UNARY_OP` for unary `+` — it discards it and returns the inner expression directly. So no `+` case is needed.

### IS NULL / IS NOT NULL

`NODE_IS_NULL` / `NODE_IS_NOT_NULL` have one child.

- Evaluate child → `value_bool(child.is_null())` or `value_bool(!child.is_null())`
- **Never returns NULL** — IS NULL always returns TRUE or FALSE

### BETWEEN

`NODE_BETWEEN` has three children: expr, low, high.

- Evaluate all three
- Equivalent to `expr >= low AND expr <= high`
- Uses coercion for comparison
- NULL propagation: if any is NULL, follows standard comparison NULL rules

### IN list

`NODE_IN_LIST` has N children: first is the expression, rest are values.

- Evaluate the expression
- If expression is NULL → return NULL
- Evaluate each value, compare with `=`
- If any match → TRUE
- If no match but any comparison was NULL → NULL
- If no match and no NULLs → FALSE

### CASE/WHEN

`NODE_CASE_WHEN` children are: [case_expr], when1, then1, when2, then2, ..., [else_expr].

**Disambiguation rule:** The parser's `parse_case()` adds the case expression as the first child ONLY when the token after CASE is not WHEN. This means:
- If the child count is **even** → searched CASE (pairs of WHEN/THEN, optionally +1 ELSE makes odd, but each pair is 2)
- If the child count is **odd** → either simple CASE (case_expr + pairs) or searched CASE with ELSE

**Practical approach:** Count children. Try to interpret as searched CASE first (children are WHEN/THEN pairs + optional ELSE). Evaluate the first child as a WHEN condition. If the CASE statement doesn't match any when conditions and we have the wrong form, fall back. In practice, the parser's CASE/WHEN structure is:
- Searched: `[condition1, value1, condition2, value2, ..., else_value]` — odd count with ELSE, even without
- Simple: `[case_expr, match1, value1, match2, value2, ..., else_value]` — even count with ELSE, odd without

**Simplest correct implementation:** Store a flag in `NODE_CASE_WHEN`'s `flags` field to distinguish the forms. The parser sets `flags = 1` for simple CASE, `flags = 0` for searched CASE. This requires a one-line change to the parser's `parse_case()` method and eliminates all ambiguity.

**Searched CASE** (`flags == 0`):
```
Children: when1, then1, when2, then2, ..., [else_expr]
For each WHEN/THEN pair:
  evaluate WHEN condition
  if TRUE → evaluate and return THEN value
If remaining child exists → it's ELSE, evaluate and return
If no match and no ELSE → NULL
```

**Simple CASE** (`flags == 1`):
```
Children: case_expr, when1, then1, when2, then2, ..., [else_expr]
Evaluate case_expr
For each WHEN/THEN pair (starting from second child):
  evaluate WHEN value
  if case_expr = WHEN value → evaluate and return THEN
If remaining child → ELSE
If no match → NULL
```

### Function calls

`NODE_FUNCTION_CALL` has value = function name, children = arguments.

```
1. Lookup function in FunctionRegistry<D> by name
2. If not found → return value_null()
3. Evaluate each argument child into Value array
4. Call function(args, arg_count, arena)
5. Return result
```

### Subquery (deferred)

`NODE_SUBQUERY` → return `value_null()`. Full subquery evaluation requires the executor.

---

## LIKE Pattern Matching

```cpp
template <Dialect D>
bool match_like(StringRef text, StringRef pattern, char escape_char = '\\');
```

**Pattern rules:**
- `%` matches zero or more characters
- `_` matches exactly one character
- Escape character (default `\`) makes the next character literal

**Dialect differences:**
- MySQL: case-insensitive by default
- PostgreSQL: case-sensitive (`ILIKE` for insensitive)

**Algorithm:** Iterative two-pointer approach. O(n*m) worst case, O(n) typical. No regex.

---

## Error Handling

The evaluator does not throw exceptions. Error cases:

| Error | Behavior |
|---|---|
| Unknown node type | Return `value_null()` |
| Division by zero | Return `value_null()` |
| Function not found | Return `value_null()` |
| Type coercion failure | Return `value_null()` |
| Integer overflow | Wrap (int64 arithmetic) |
| Invalid literal parse | `value_int(0)` for MySQL, `value_null()` for PostgreSQL |

---

## File Organization

```
include/sql_engine/
    expression_eval.h    — evaluate_expression<D>() template function
    like.h               — match_like<D>() pattern matcher

tests/
    test_expression_eval.cpp      — Unit tests: each node type
    test_like.cpp                 — LIKE pattern matching tests
    test_eval_integration.cpp     — End-to-end: parse SQL → evaluate → check result
```

---

## Testing Strategy

### Unit tests (test_expression_eval.cpp)

- **Literals:** INT, FLOAT, STRING, NULL, BOOL → correct Value
- **Arithmetic:** `1 + 2`, `10 / 3`, `10 % 3`, `10 DIV 3`, division by zero, NULL + 1
- **Comparison:** `1 = 1`, `1 < 2`, `'a' > 'b'`, cross-type (`1 = '1'` MySQL vs PgSQL)
- **Logical:** AND/OR/NOT truth tables with NULL, short-circuit verification
- **IS NULL / IS NOT NULL:** NULL and non-NULL inputs
- **BETWEEN:** normal, NULL boundary, NULL expression
- **IN list:** match, no match, NULL in list, NULL expression
- **CASE/WHEN:** searched and simple forms, NULL handling, no ELSE
- **Function calls:** known function, unknown function, NULL args
- **Column resolution:** callback called with correct names, qualified names

### LIKE tests (test_like.cpp)

- Exact match, prefix `%`, suffix `%`, wildcard `_`
- Case sensitivity per dialect
- Escape characters
- Empty string / empty pattern edge cases

### Integration tests (test_eval_integration.cpp)

Parse SQL → navigate to expression AST node → evaluate → verify result:

- `SELECT 1 + 2` → 3
- `SELECT UPPER('hello')` → `'HELLO'`
- `SELECT COALESCE(NULL, NULL, 42)` → 42
- `SELECT CASE WHEN 1 > 2 THEN 'a' ELSE 'b' END` → `'b'`
- `SELECT 1 IN (1, 2, 3)` → true
- `SELECT 5 BETWEEN 1 AND 10` → true
- `SELECT NULL IS NULL` → true
- `SELECT 'test' LIKE 't%'` → true

---

## Performance Targets

| Operation | Target |
|---|---|
| Literal evaluation | <10ns |
| Binary arithmetic (int + int) | <15ns |
| Comparison (int = int) | <15ns |
| NULL check + propagation | <5ns |
| Function call (simple, e.g., ABS) | <30ns |
| LIKE simple pattern | <100ns |
| CASE/WHEN (3 branches) | <50ns |
| Full expression: `price * qty > 100` | <50ns (excluding column resolution) |
