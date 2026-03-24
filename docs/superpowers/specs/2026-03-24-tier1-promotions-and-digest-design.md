# Tier 1 Promotions, UNION Support & Query Digest — Design Specification

## Overview

Extends the SQL parser with full Tier 1 deep parsing for INSERT, UPDATE, and DELETE (both MySQL and PostgreSQL dialects), adds UNION/INTERSECT/EXCEPT compound query support with recursive nesting, and introduces AST-based query digest/normalization for query rules matching.

### Goals

- **INSERT Tier 1:** Full AST for INSERT/REPLACE with VALUES, SELECT, SET, ON DUPLICATE KEY UPDATE (MySQL), ON CONFLICT (PostgreSQL), RETURNING (PostgreSQL).
- **UPDATE Tier 1:** Full AST with multi-table JOIN (MySQL), FROM (PostgreSQL), ORDER BY/LIMIT (MySQL), RETURNING (PostgreSQL).
- **DELETE Tier 1:** Full AST with multi-table (MySQL both forms), USING (PostgreSQL), ORDER BY/LIMIT (MySQL), RETURNING (PostgreSQL).
- **Compound queries:** UNION [ALL], INTERSECT [ALL], EXCEPT [ALL] with parenthesized nesting and precedence (INTERSECT binds tighter).
- **Query digest:** AST-based normalization (literals → `?`, IN list collapsing, keyword uppercasing) + 64-bit hash. Works for all statement types including Tier 2 (token-level fallback).

### Constraints

- Same as original spec: C++17 floor, both dialects, sub-microsecond targets, arena allocation, header-only parsers.
- All new parsers follow the established pattern: `XxxParser<D>` header-only template, uses `ExpressionParser<D>`, integrated via `parser.cpp`.
- Emitter extended for all new node types + digest mode.

### Prerequisite Refactoring: Shared Table Reference Parsing

The existing `SelectParser<D>` has `parse_from_clause()`, `parse_table_reference()`, `parse_join()`, and `parse_optional_alias()` as private methods. These are needed by `InsertParser` (for INSERT ... SELECT), `UpdateParser` (for MySQL multi-table UPDATE), and `DeleteParser` (for MySQL multi-table DELETE and PostgreSQL USING).

**Solution:** Extract these methods into a shared `TableRefParser<D>` utility class that takes a `Tokenizer<D>&` and `Arena&`. All parsers (SelectParser, InsertParser, UpdateParser, DeleteParser) instantiate it internally. SelectParser's private methods are replaced with calls to TableRefParser.

```
include/sql_parser/
    table_ref_parser.h    — shared FROM/JOIN/table reference parsing
```

This refactoring is a prerequisite for Plans 7-9 and should be done as the first task of Plan 7.

### Classifier Updates

The classifier switch in `Parser<D>::classify_and_dispatch()` must be updated:

- `TK_INSERT` → `parse_insert()` (was `extract_insert()`)
- `TK_UPDATE` → `parse_update()` (was `extract_update()`)
- `TK_DELETE` → `parse_delete()` (was `extract_delete()`)
- `TK_REPLACE` → `parse_insert()` with a REPLACE flag (was `extract_replace()`)
- `TK_SELECT` → compound query aware `parse_select()` (existing, updated)

### is_alias_start() Update

The `is_alias_start()` blocklist in SelectParser must be updated to include new clause-starting keywords: `TK_RETURNING`, `TK_INTERSECT`, `TK_EXCEPT`, `TK_CONFLICT`, `TK_DO`, `TK_NOTHING`, `TK_DUPLICATE`.

---

## New NodeType Additions

```cpp
// INSERT nodes
NODE_INSERT_STMT,
NODE_INSERT_COLUMNS,       // (col1, col2, ...)
NODE_VALUES_CLAUSE,        // VALUES keyword wrapper
NODE_VALUES_ROW,           // single (val1, val2, ...) row
NODE_INSERT_SET_CLAUSE,    // MySQL INSERT ... SET col=val form
NODE_ON_DUPLICATE_KEY,     // MySQL ON DUPLICATE KEY UPDATE
NODE_ON_CONFLICT,          // PostgreSQL ON CONFLICT
NODE_CONFLICT_TARGET,      // PostgreSQL conflict target (cols or ON CONSTRAINT)
NODE_CONFLICT_ACTION,      // DO UPDATE SET ... or DO NOTHING
NODE_RETURNING_CLAUSE,     // PostgreSQL RETURNING expr_list

// UPDATE nodes
NODE_UPDATE_STMT,
NODE_UPDATE_SET_CLAUSE,    // SET col=expr, col=expr in UPDATE context
NODE_UPDATE_SET_ITEM,      // single col=expr pair

// DELETE nodes
NODE_DELETE_STMT,
NODE_DELETE_USING_CLAUSE,  // PostgreSQL USING for join-like deletes

// Compound query nodes
NODE_COMPOUND_QUERY,       // root for UNION/INTERSECT/EXCEPT
NODE_SET_OPERATION,        // operator (UNION, INTERSECT, EXCEPT) with ALL flag

// Statement options (shared)
NODE_STMT_OPTIONS,         // LOW_PRIORITY, IGNORE, QUICK, DELAYED, etc.
```

---

## INSERT Deep Parser

### MySQL Syntax

```sql
INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO] table_name
    [(col1, col2, ...)]
    { VALUES (row1), (row2), ... | SELECT ... | SET col=val, ... }
    [ON DUPLICATE KEY UPDATE col=expr, col=expr, ...]

REPLACE [LOW_PRIORITY | DELAYED] [INTO] table_name
    [(col1, col2, ...)]
    { VALUES (row1), (row2), ... | SELECT ... | SET col=val, ... }
```

### PostgreSQL Syntax

```sql
INSERT INTO table_name [(col1, col2, ...)]
    { VALUES (row1), (row2), ... | SELECT ... | DEFAULT VALUES }
    [ON CONFLICT [(col1, col2, ...)] | [ON CONSTRAINT name]
        { DO UPDATE SET col=expr [, ...] [WHERE ...] | DO NOTHING }]
    [RETURNING expr_list]
```

### AST Structure

```
NODE_INSERT_STMT
  ├── NODE_STMT_OPTIONS (LOW_PRIORITY, IGNORE, etc.)
  ├── NODE_TABLE_REF (table name, optional schema)
  ├── NODE_INSERT_COLUMNS (col1, col2, ...)
  ├── NODE_VALUES_CLAUSE
  │     ├── NODE_VALUES_ROW (val1, val2, ...)
  │     └── NODE_VALUES_ROW (val1, val2, ...)
  │   OR
  ├── NODE_SELECT_STMT (INSERT ... SELECT)
  │   OR
  ├── NODE_INSERT_SET_CLAUSE (MySQL SET col=val)
  │     ├── NODE_UPDATE_SET_ITEM (col = expr)
  │     └── NODE_UPDATE_SET_ITEM (col = expr)
  ├── NODE_ON_DUPLICATE_KEY (MySQL)
  │     ├── NODE_UPDATE_SET_ITEM (col = expr)
  │     └── NODE_UPDATE_SET_ITEM (col = expr)
  │   OR
  ├── NODE_ON_CONFLICT (PostgreSQL)
  │     ├── NODE_CONFLICT_TARGET (cols or ON CONSTRAINT name)
  │     └── NODE_CONFLICT_ACTION (DO UPDATE SET ... WHERE ... or DO NOTHING)
  └── NODE_RETURNING_CLAUSE (PostgreSQL)
        ├── expression
        └── expression
```

The parser reuses `ExpressionParser` for all value expressions and `SelectParser` for `INSERT ... SELECT`. The `RETURNING` clause uses the same item-list parsing as SELECT's select item list.

---

## UPDATE Deep Parser

### MySQL Syntax

```sql
UPDATE [LOW_PRIORITY] [IGNORE] table_references
    SET col=expr [, col=expr, ...]
    [WHERE condition]
    [ORDER BY ...]
    [LIMIT count]
```

`table_references` can include JOINs — same grammar as SELECT's FROM clause.

### PostgreSQL Syntax

```sql
UPDATE [ONLY] table_name [[AS] alias]
    SET col=expr [, col=expr, ...]
    [FROM from_list]
    [WHERE condition]
    [RETURNING expr_list]
```

### AST Structure

```
NODE_UPDATE_STMT
  ├── NODE_STMT_OPTIONS (LOW_PRIORITY, IGNORE)
  ├── NODE_TABLE_REF (target table — for both dialects, the primary table being updated)
  ├── NODE_FROM_CLAUSE (MySQL: additional JOINed table refs; PostgreSQL: FROM join source)
  │     (distinguished by position: always comes after SET clause for PostgreSQL,
  │      comes as part of the initial table refs for MySQL)
  │     (MySQL multi-table: flags field has FLAG_UPDATE_TARGET_TABLES = 0x01)
  ├── NODE_UPDATE_SET_CLAUSE
  │     ├── NODE_UPDATE_SET_ITEM (col = expr)
  │     └── NODE_UPDATE_SET_ITEM (col = expr)
  ├── NODE_WHERE_CLAUSE
  ├── NODE_ORDER_BY_CLAUSE (MySQL only)
  ├── NODE_LIMIT_CLAUSE (MySQL only)
  └── NODE_RETURNING_CLAUSE (PostgreSQL)
```

For MySQL multi-table UPDATE, the table references (with JOINs) reuse the shared `TableRefParser` methods. For MySQL, the JOINed tables appear as children of the first `NODE_FROM_CLAUSE` (before SET). For PostgreSQL, the single target table is a `NODE_TABLE_REF`, and the optional `FROM` clause (after SET, before WHERE) is a separate `NODE_FROM_CLAUSE` child. The emitter checks the statement type to determine emission order.

---

## DELETE Deep Parser

### MySQL Syntax

```sql
-- Single-table:
DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM table_name
    [WHERE condition]
    [ORDER BY ...]
    [LIMIT count]

-- Multi-table form 1:
DELETE [LOW_PRIORITY] [QUICK] [IGNORE] t1, t2
    FROM table_references
    [WHERE condition]

-- Multi-table form 2:
DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM t1, t2
    USING table_references
    [WHERE condition]
```

### PostgreSQL Syntax

```sql
DELETE FROM [ONLY] table_name [[AS] alias]
    [USING using_list]
    [WHERE condition]
    [RETURNING expr_list]
```

### AST Structure

```
NODE_DELETE_STMT
  ├── NODE_STMT_OPTIONS (LOW_PRIORITY, QUICK, IGNORE)
  ├── NODE_TABLE_REF (target table(s))
  ├── NODE_FROM_CLAUSE (multi-table MySQL: source tables with JOINs)
  ├── NODE_DELETE_USING_CLAUSE (MySQL USING or PostgreSQL USING)
  ├── NODE_WHERE_CLAUSE
  ├── NODE_ORDER_BY_CLAUSE (MySQL single-table only)
  ├── NODE_LIMIT_CLAUSE (MySQL single-table only)
  └── NODE_RETURNING_CLAUSE (PostgreSQL)
```

---

## Compound Query Parser (UNION/INTERSECT/EXCEPT)

### Syntax

```sql
select_stmt { UNION | INTERSECT | EXCEPT } [ALL] select_stmt
    [{ UNION | INTERSECT | EXCEPT } [ALL] select_stmt ...]
    [ORDER BY ...] [LIMIT ...]

-- With parenthesized nesting:
(SELECT ...) UNION ALL (SELECT ... INTERSECT SELECT ...) ORDER BY ... LIMIT ...
```

### Precedence

Per SQL standard: INTERSECT binds tighter than UNION and EXCEPT. So:

```sql
SELECT 1 UNION SELECT 2 INTERSECT SELECT 3
-- Parses as: SELECT 1 UNION (SELECT 2 INTERSECT SELECT 3)
```

Implemented via precedence levels:
- INTERSECT: higher precedence
- UNION, EXCEPT: lower precedence (same level, left-associative)

### AST Structure

```
NODE_COMPOUND_QUERY
  ├── NODE_SET_OPERATION (value="UNION ALL")
  │     ├── NODE_SELECT_STMT (left)
  │     └── NODE_SELECT_STMT (right)
  ├── NODE_ORDER_BY_CLAUSE (applies to whole compound)
  └── NODE_LIMIT_CLAUSE (applies to whole compound)
```

For nested compounds:
```
NODE_COMPOUND_QUERY
  └── NODE_SET_OPERATION (value="UNION")
        ├── NODE_SELECT_STMT (left)
        └── NODE_SET_OPERATION (value="INTERSECT")
              ├── NODE_SELECT_STMT
              └── NODE_SELECT_STMT
```

### Integration

A new `CompoundQueryParser<D>` class sits above `SelectParser<D>`. The `parse_select()` method in `Parser<D>` is updated to call `CompoundQueryParser` instead of `SelectParser` directly.

`CompoundQueryParser` works as follows:
1. Parse the first operand: if `(`, consume it, parse inner compound recursively, expect `)`. Otherwise, call `SelectParser::parse()` for a single SELECT.
2. Check for set operator (UNION/INTERSECT/EXCEPT). If none, return the single SELECT as-is.
3. If found, enter a Pratt-like precedence loop: parse the operator, parse the next operand, build `NODE_SET_OPERATION` nodes respecting INTERSECT > UNION/EXCEPT precedence.
4. After the compound, parse optional trailing ORDER BY / LIMIT (applies to whole result).
5. Wrap in `NODE_COMPOUND_QUERY` and return.

This layering means `SelectParser` is unchanged — it still parses a single SELECT statement. The compound logic is entirely in `CompoundQueryParser`, which is a separate header-only template.

```
include/sql_parser/
    compound_query_parser.h   — UNION/INTERSECT/EXCEPT with precedence
```

---

## Query Digest / Normalization

### API

```cpp
template <Dialect D>
class Digest {
public:
    Digest(Arena& arena);

    // From a parsed AST (Tier 1)
    DigestResult compute(const AstNode* ast);

    // From raw SQL (works for any statement, falls back to token-level for Tier 2)
    DigestResult compute(const char* sql, size_t len);
};

struct DigestResult {
    StringRef normalized;  // "SELECT * FROM t WHERE id = ?"
    uint64_t hash;         // 64-bit hash
};
```

### Normalization Rules

1. **Literals → `?`:** Replace `NODE_LITERAL_INT`, `NODE_LITERAL_FLOAT`, `NODE_LITERAL_STRING` with `?`
2. **IN list collapsing:** `IN (?, ?, ?)` → `IN (?)` (ProxySQL convention — multiple values produce the same digest)
3. **Keyword uppercasing:** All SQL keywords emitted in uppercase canonical form
4. **Whitespace normalization:** Single space between tokens, no leading/trailing
5. **Comment stripping:** Comments already stripped by tokenizer, so this is free
6. **Backtick/quote stripping:** Identifiers emitted without quotes in digest (optional, configurable)

### Token-Level Fallback (Tier 2)

For statements without a full AST (Tier 2 or parse failures), the digest works at the token level:

1. Tokenize the input
2. Walk tokens, emitting each:
   - Keywords → uppercase
   - Identifiers → as-is
   - Literals (TK_INTEGER, TK_FLOAT, TK_STRING) → `?`
   - Operators/punctuation → as-is
3. Collapse consecutive `?` in IN/VALUES lists
4. Hash the result

This ensures digest works for ALL queries, even those the parser doesn't deeply understand.

### Hash Function

64-bit FNV-1a — simple, fast, no external dependency, good distribution. Computed incrementally as the normalized string is built (no second pass).

---

## Emitter Extensions

New `emit_*` methods for each new node type, following the same pattern as existing SET/SELECT emission:

- `emit_insert_stmt`, `emit_values_clause`, `emit_values_row`, `emit_on_duplicate_key`, `emit_on_conflict`, `emit_returning`
- `emit_update_stmt`, `emit_update_set_clause`, `emit_update_set_item`
- `emit_delete_stmt`, `emit_delete_using`
- `emit_compound_query`, `emit_set_operation`

The `RETURNING` emitter is shared across INSERT/UPDATE/DELETE.

**Digest mode** is a constructor flag on the emitter:

```cpp
enum class EmitMode : uint8_t { NORMAL, DIGEST };

Emitter(Arena& arena, EmitMode mode = EmitMode::NORMAL,
        const ParamBindings* bindings = nullptr);
```

In digest mode, the following methods change behavior:
- `emit_value()` / `emit_string_literal()` — for literal nodes (`NODE_LITERAL_INT`, `NODE_LITERAL_FLOAT`, `NODE_LITERAL_STRING`), emit `?` instead of actual value
- `emit_in_list()` — emit `IN (?)` regardless of how many values, collapsing the list
- `emit_values_row()` — emit `(?, ?, ...)` matching column count but with `?` for all values
- `emit_placeholder()` — emit `?` (same as normal mode, already a placeholder)
- All keyword text emitted in uppercase (e.g., `SELECT`, `FROM`, `WHERE`)
- `emit_alias()` — skip aliases in digest mode (aliases don't affect query semantics for routing)

Methods that do NOT change in digest mode: structural emission (FROM, JOIN, WHERE, GROUP BY, ORDER BY, LIMIT, etc.) remains identical since the query structure matters for digest grouping.

---

## New Token Additions

```cpp
// Needed for new syntax:
TK_DELAYED,
TK_HIGH_PRIORITY,
TK_DUPLICATE,
TK_KEY,
TK_CONFLICT,
TK_DO,
TK_NOTHING,
TK_RETURNING,
TK_ONLY,        // already exists in enum, verify in keyword tables
TK_EXCEPT,
TK_INTERSECT,
TK_CONSTRAINT,
// Note: DEFAULT VALUES uses existing TK_DEFAULT + TK_VALUES (two-token approach, no compound token needed)
// Note: TK_UNION and TK_OF already exist from Plan 3
```

---

## Implementation Plans (separate)

This spec should be implemented across 5 plans:

1. **Plan 7: Shared table ref parser + INSERT deep parser** — Extract TableRefParser from SelectParser, then INSERT/REPLACE with all syntax, emitter, tests. Closes #5.
2. **Plan 8: UPDATE deep parser** — full UPDATE syntax, emitter, tests. Closes #6.
3. **Plan 9: DELETE deep parser** — full DELETE syntax, emitter, tests. Closes #7.
4. **Plan 10: Compound queries** — CompoundQueryParser with UNION/INTERSECT/EXCEPT nesting, emitter, tests. Closes #8.
5. **Plan 11: Query digest** — Digest module with both AST and token-level modes, tests. Closes #9.

**Dependencies:** Plan 7 must come first (extracts shared TableRefParser). Plans 8-9 depend on Plan 7's TableRefParser but are independent of each other. Plan 10 is independent of 7-9. Plan 11 benefits from all prior plans being complete but works with Tier 2 token-level fallback.

---

## Performance Targets

| Operation | Target |
|---|---|
| INSERT parse (simple VALUES) | <500ns |
| INSERT parse (multi-row + ON DUPLICATE KEY) | <2us |
| UPDATE parse (simple) | <500ns |
| DELETE parse (simple) | <300ns |
| Compound UNION (2 simple SELECTs) | <1us |
| Query digest (simple SELECT) | <500ns |
| Query digest (token-level, Tier 2) | <200ns |
