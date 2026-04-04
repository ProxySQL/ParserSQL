# SQL Engine Row Format & Logical Plan — Design Specification

## Overview

This spec covers two tightly coupled components: the in-memory Row format and the Logical Plan tree. The Row defines how data flows between operators. The Logical Plan defines the relational algebra tree that the optimizer and executor will operate on.

Sub-project 4+5 of the query engine. Depends on: type system (sub-project 1), expression evaluator (sub-project 2), catalog (sub-project 3).

### Goals

- **Row struct** — fixed-width array of `Value` objects, arena-allocated, indexed by ordinal
- **Logical plan nodes** — relational algebra: Scan, Filter, Project, Join, Aggregate, Sort, Limit, Distinct, Union
- **Plan builder** — mechanical translation from parser AST to logical plan tree (no optimization)
- **SELECT-only** for now — INSERT/UPDATE/DELETE plan generation deferred to executor

### Constraints

- C++17, arena-allocated plan nodes
- Plan nodes hold pointers to parser AST expression nodes (not copies)
- No optimization in the plan builder — that's sub-project 6

---

## Row Format

```cpp
struct Row {
    Value* values;          // array indexed by ordinal
    uint16_t column_count;

    Value get(uint16_t ordinal) const { return values[ordinal]; }
    void set(uint16_t ordinal, Value v) { values[ordinal] = v; }
    bool is_null(uint16_t ordinal) const { return values[ordinal].is_null(); }
};
```

No separate null bitmap — `Value::tag == TAG_NULL` serves as the null indicator.

**Allocation:**

```cpp
inline Row make_row(Arena& arena, uint16_t column_count) {
    Value* vals = static_cast<Value*>(arena.allocate(sizeof(Value) * column_count));
    for (uint16_t i = 0; i < column_count; ++i) vals[i] = value_null();
    return Row{vals, column_count};
}
```

Rows are arena-allocated. Freed on arena reset after query completes.

**Schema:** A Row's column metadata is the `TableInfo` from the Catalog (or a `ProjectInfo` for computed columns). No new schema struct needed.

---

## Logical Plan Nodes

### Node types

```cpp
enum class PlanNodeType : uint8_t {
    SCAN,       // read from data source
    FILTER,     // WHERE / HAVING condition
    PROJECT,    // SELECT expression list
    JOIN,       // JOIN two sources
    AGGREGATE,  // GROUP BY + aggregate functions
    SORT,       // ORDER BY
    LIMIT,      // LIMIT + OFFSET
    DISTINCT,   // remove duplicates
    SET_OP,     // UNION / INTERSECT / EXCEPT
};
```

### PlanNode struct

```cpp
struct PlanNode {
    PlanNodeType type;
    PlanNode* left = nullptr;    // primary child (or left of join/union)
    PlanNode* right = nullptr;   // right of join/union (null for unary ops)

    union {
        struct {
            const TableInfo* table;
        } scan;

        struct {
            const AstNode* expr;        // WHERE/HAVING expression AST
        } filter;

        struct {
            const AstNode** exprs;      // SELECT expression list (AST nodes)
            const AstNode** aliases;    // alias AST nodes (parallel array, nullable entries)
            uint16_t count;
        } project;

        struct {
            uint8_t join_type;          // INNER=0, LEFT=1, RIGHT=2, FULL=3, CROSS=4
            const AstNode* condition;   // ON expression AST (null for CROSS/NATURAL)
        } join;

        struct {
            const AstNode** group_by;   // GROUP BY expression list
            uint16_t group_count;
            const AstNode** agg_exprs;  // aggregate expressions (COUNT, SUM, etc.)
            uint16_t agg_count;
        } aggregate;

        struct {
            const AstNode** keys;       // ORDER BY key expressions
            uint8_t* directions;        // 0=ASC, 1=DESC (parallel array)
            uint16_t count;
        } sort;

        struct {
            int64_t count;
            int64_t offset;
        } limit;

        struct {
            uint8_t op;                 // 0=UNION, 1=INTERSECT, 2=EXCEPT
            bool all;                   // UNION ALL vs UNION
        } set_op;
    };
};
```

**Design decisions:**

1. **AST expression pointers, not copies.** Plan nodes reference the parser's AST nodes directly. The AST lives in the arena for the query's lifetime. Avoids duplication.

2. **Arena-allocated.** `PlanNode` is allocated from the arena via `make_plan_node(arena, type)`.

3. **Union for node-specific data.** Compact — each PlanNode is ~48 bytes regardless of type.

---

## Plan Builder — AST to Logical Plan

```cpp
template <Dialect D>
class PlanBuilder {
public:
    PlanBuilder(const Catalog& catalog, Arena& arena);

    // Build a logical plan from a parsed SELECT statement AST
    PlanNode* build(const AstNode* stmt_ast);

private:
    const Catalog& catalog_;
    Arena& arena_;

    PlanNode* build_select(const AstNode* select_ast);
    PlanNode* build_from(const AstNode* from_clause);
    PlanNode* build_join(const AstNode* join_clause, PlanNode* left);
    PlanNode* build_compound(const AstNode* compound_ast);
};
```

### Translation rules for SELECT

The builder walks the SELECT AST children and builds the plan bottom-up:

```
SQL:    SELECT DISTINCT name, COUNT(*)
        FROM users u JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        GROUP BY name
        HAVING COUNT(*) > 5
        ORDER BY name
        LIMIT 10 OFFSET 5

Plan:   Limit(10, 5)
          └── Sort([name ASC])
                └── Distinct
                      └── Project([name, COUNT(*)])
                            └── Filter(COUNT(*) > 5)      ← HAVING
                                  └── Aggregate([name], [COUNT(*)])
                                        └── Filter(u.active = 1)  ← WHERE
                                              └── Join(INNER, u.id = o.user_id)
                                                    ├── Scan(users)
                                                    └── Scan(orders)
```

### Build order

1. Start with FROM → `Scan` nodes (one per table)
2. If JOINs → wrap in `Join` nodes
3. If WHERE → wrap in `Filter`
4. If GROUP BY → wrap in `Aggregate`
5. If HAVING → wrap in `Filter` (above Aggregate)
6. SELECT list → wrap in `Project`
7. If DISTINCT → wrap in `Distinct`
8. If ORDER BY → wrap in `Sort`
9. If LIMIT → wrap in `Limit`

### FROM clause translation

- Single table: `Scan(table)` — look up table in Catalog
- Multiple tables (comma join): `Join(CROSS, Scan(t1), Scan(t2))`
- Explicit JOIN: `Join(type, Scan(t1), Scan(t2), condition)`
- Multiple JOINs: left-deep tree of `Join` nodes
- Subquery in FROM: defer (return `Scan` with null table for now)

### Compound queries

`NODE_COMPOUND_QUERY` → `SetOp` node wrapping two child plans:

```
SELECT ... UNION ALL SELECT ...
→ SetOp(UNION, all=true, build(left), build(right))
```

Trailing ORDER BY / LIMIT on compounds → `Sort` / `Limit` above the `SetOp`.

### No FROM clause

`SELECT 1 + 2` (no FROM) → `Project` with no child (leaf node). The executor generates a single empty row.

---

## File Organization

```
include/sql_engine/
    row.h                — Row struct, make_row()
    plan_node.h          — PlanNodeType enum, PlanNode struct, make_plan_node()
    plan_builder.h       — PlanBuilder<D> template

tests/
    test_row.cpp         — Row creation, get/set, null
    test_plan_builder.cpp — AST → plan for various SELECT shapes
```

---

## Testing Strategy

### Row tests
- Create row, set/get values
- NULL checks
- Arena allocation

### Plan builder tests

Parse real SQL → build plan → walk tree → verify structure:

| SQL | Expected plan shape |
|---|---|
| `SELECT * FROM users` | Scan(users) |
| `SELECT name FROM users` | Project → Scan |
| `SELECT * FROM users WHERE id = 1` | Filter → Scan |
| `SELECT name, age FROM users WHERE age > 18` | Project → Filter → Scan |
| `SELECT * FROM users ORDER BY name LIMIT 10` | Limit → Sort → Scan |
| `SELECT status, COUNT(*) FROM users GROUP BY status` | Project → Aggregate → Scan |
| `SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5` | Project → Filter → Aggregate → Scan |
| `SELECT * FROM users u JOIN orders o ON u.id = o.user_id` | Join(Scan, Scan) |
| `SELECT DISTINCT name FROM users` | Distinct → Project → Scan |
| `SELECT * FROM t1 UNION ALL SELECT * FROM t2` | SetOp(Scan, Scan) |
| `SELECT 1 + 2` | Project (no child — leaf) |

Each test verifies: correct PlanNodeType at each level, correct table in Scan, correct expression pointer in Filter.

---

## Performance Targets

| Operation | Target |
|---|---|
| make_row (10 columns) | <50ns |
| Row get/set | <5ns |
| Plan builder (simple SELECT WHERE) | <500ns |
| Plan builder (complex JOIN + GROUP BY) | <2us |
