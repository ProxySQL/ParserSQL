# Subquery Execution — Design Specification

## Overview

Enable subquery evaluation in the expression evaluator and executor. Currently `NODE_SUBQUERY` returns NULL. After this sub-project, `WHERE id IN (SELECT ...)`, `WHERE EXISTS (SELECT ...)`, scalar subqueries, and correlated subqueries work correctly.

Sub-project 11. Depends on: executor (sub-project 7), backend connections (sub-project 10).

### Goals

- **Uncorrelated subqueries** — `WHERE id IN (SELECT user_id FROM orders)`, `WHERE EXISTS (SELECT 1 FROM ...)`, `SELECT (SELECT MAX(age) FROM users)`
- **Correlated subqueries** — `WHERE age > (SELECT AVG(age) FROM users WHERE dept = outer.dept)`
- **Subqueries in FROM** — `SELECT * FROM (SELECT ...) AS t` (derived tables)
- **Subqueries in expressions** — `SELECT CASE WHEN (SELECT COUNT(*) FROM orders) > 0 THEN 'yes' ELSE 'no' END`
- **Distributed subqueries** — subquery on different backend than outer query

### Constraints

- C++17
- Reuse existing executor pipeline for inner queries
- Arena-allocated intermediate results
- No materialized CTEs (WITH clause) — deferred

---

## Subquery Types

### 1. Scalar subquery

Returns a single value. Used anywhere an expression is expected.

```sql
SELECT name, (SELECT MAX(total) FROM orders WHERE orders.user_id = users.id) AS max_order
FROM users
```

Execution: run inner query, verify it returns exactly 0 or 1 row. If 0 rows → NULL. If 1 row → the single value. If >1 row → error.

### 2. EXISTS subquery

Returns TRUE if inner query returns at least one row, FALSE otherwise.

```sql
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
```

Execution: run inner query with implicit LIMIT 1 (optimization — only need to check if any row exists). Return value_bool(has_row).

### 3. IN subquery

Returns TRUE if outer value matches any row from inner query.

```sql
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
```

Execution: materialize inner query into a set. For each outer row, check if the value is in the set. Optimization: use a hash set for O(1) lookup.

### 4. Correlated subquery

The inner query references columns from the outer query. Must be re-executed for each outer row.

```sql
SELECT * FROM users u
WHERE age > (SELECT AVG(age) FROM users WHERE dept = u.dept)
```

Execution: for each outer row, bind the outer columns into the inner query's resolver, execute inner query, use result.

### 5. Derived table (FROM subquery)

A subquery in the FROM clause that acts as a virtual table.

```sql
SELECT t.name FROM (SELECT name, age FROM users WHERE age > 18) AS t
```

Execution: execute inner query, materialize results as a DataSource, Scan from it.

---

## Architecture

### SubqueryExecutor

A new component that the expression evaluator calls when it encounters NODE_SUBQUERY:

```cpp
template <Dialect D>
class SubqueryExecutor {
public:
    SubqueryExecutor(PlanExecutor<D>& executor,
                     PlanBuilder<D>& builder,
                     Optimizer<D>& optimizer,
                     Arena& arena);

    // Execute a subquery AST, return result
    Value execute_scalar(const AstNode* subquery_ast,
                          const std::function<Value(StringRef)>& outer_resolve);

    bool execute_exists(const AstNode* subquery_ast,
                         const std::function<Value(StringRef)>& outer_resolve);

    ResultSet execute_set(const AstNode* subquery_ast,
                           const std::function<Value(StringRef)>& outer_resolve);
};
```

### Integration with expression evaluator

The expression evaluator's `evaluate_expression()` currently returns `value_null()` for `NODE_SUBQUERY`. It needs access to a `SubqueryExecutor`:

```cpp
template <Dialect D>
Value evaluate_expression(const AstNode* expr,
                          const std::function<Value(StringRef)>& resolve,
                          FunctionRegistry<D>& functions,
                          Arena& arena,
                          SubqueryExecutor<D>* subquery_exec = nullptr);  // NEW optional param
```

When `NODE_SUBQUERY` is encountered and `subquery_exec != nullptr`, call the appropriate method. The subquery type is determined by context (the parent node — IN_LIST, EXISTS check, or scalar position).

### Integration with plan builder

Derived tables in FROM: the plan builder recognizes subquery AST nodes in the FROM clause and creates a special DERIVED_SCAN plan node:

```cpp
struct {
    PlanNode* inner_plan;    // the subquery's plan
    const char* alias;
} derived_scan;
```

### Integration with executor

The DerivedScanOperator:
1. On open(): execute the inner plan, materialize into a vector of Rows
2. On next(): yield rows from the materialized result

### Correlated subquery handling

For correlated subqueries, the inner query's resolver needs access to the outer row. The SubqueryExecutor creates a combined resolver:

```cpp
auto combined_resolve = [&outer_resolve, &inner_resolve](StringRef name) -> Value {
    // Try inner first (inner table columns take precedence)
    Value v = inner_resolve(name);
    if (/* found in inner */) return v;
    // Fall back to outer
    return outer_resolve(name);
};
```

The inner query is re-executed for each outer row (naive but correct). Optimization (caching, decorrelation) deferred.

---

## Distributed Subqueries

When the outer query and subquery reference tables on different backends:

```sql
-- users on backend_a, orders on backend_b
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
```

The distributed planner recognizes the subquery and:
1. Executes the subquery against backend_b: `SELECT user_id FROM orders`
2. Materializes the result locally
3. Rewrites the outer query: `SELECT * FROM users WHERE id IN (1, 2, 3, ...)` with the materialized values
4. Sends the rewritten query to backend_a

For correlated cross-backend subqueries, the engine falls back to row-by-row execution (fetch outer rows, execute inner per row). This is slow but correct.

---

## File Organization

```
include/sql_engine/
    subquery_executor.h          -- SubqueryExecutor<D>
    operators/
        derived_scan_op.h        -- DerivedScanOperator

    (modify) expression_eval.h   -- Add SubqueryExecutor* parameter
    (modify) plan_node.h         -- Add DERIVED_SCAN node type
    (modify) plan_builder.h      -- Handle subquery in FROM
    (modify) plan_executor.h     -- Build DerivedScanOperator
    (modify) distributed_planner.h -- Distributed subquery handling

tests/
    test_subquery.cpp            -- All subquery types
```

---

## Testing Strategy

- Scalar subquery: `SELECT (SELECT MAX(age) FROM users)` → correct value
- Scalar subquery returning 0 rows → NULL
- EXISTS: `WHERE EXISTS (SELECT 1 FROM orders WHERE ...)` → correct filtering
- NOT EXISTS → correct filtering
- IN subquery: `WHERE id IN (SELECT user_id FROM orders)` → correct set membership
- NOT IN subquery → correct
- Correlated scalar: inner references outer column → re-executed per row
- Derived table: `FROM (SELECT ...) AS t` → works as table source
- Nested subquery: subquery within subquery
- NULL handling: IN with NULLs in subquery result
- Distributed: subquery on different backend than outer query

---

## Performance Targets

| Operation | Target |
|---|---|
| Uncorrelated IN subquery (100 values) | <500us (materialization + hash set build) |
| EXISTS subquery | <100us (stops after first row) |
| Scalar subquery | <200us |
| Correlated subquery (100 outer rows) | <50ms (100 inner executions) |
| Derived table (100 rows) | <200us (materialization) |
