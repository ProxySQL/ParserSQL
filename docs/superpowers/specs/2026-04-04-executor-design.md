# SQL Engine Executor — Design Specification

## Overview

The executor runs logical plans against data sources and produces result sets. It uses the Volcano/iterator model: each plan node becomes an operator with open/next/close methods. Rows are pulled on demand from root to leaves.

Sub-project 7 of the query engine. Depends on: type system, expression evaluator, catalog, row format, logical plan.

### Goals

- **Volcano iterator model** — open/next/close operator interface
- **9 operator types** — Scan, Filter, Project, Join (nested loop), Aggregate, Sort, Limit, Distinct, SetOp
- **DataSource abstraction** — composable data input (in-memory, cached results, remote)
- **PlanExecutor** — builds operator tree from logical plan, executes, returns ResultSet
- **End-to-end milestone** — SQL string → parse → plan → execute → result rows

### Constraints

- C++17, arena-compatible where possible
- Operators own their state, arena used for row allocation
- Materialized ResultSet (all rows in memory) — streaming deferred
- Uses expression evaluator for WHERE/HAVING/Project expressions
- Uses catalog for column resolution

### Non-Goals

- Optimizer (separate sub-project, added after executor works)
- INSERT/UPDATE/DELETE execution (needs writable storage)
- Subquery execution
- Index-based scans
- Streaming/cursor output
- Vectorized execution

---

## Operator Interface

```cpp
class Operator {
public:
    virtual ~Operator() = default;
    virtual void open() = 0;
    virtual bool next(Row& out) = 0;   // returns false when exhausted
    virtual void close() = 0;
};
```

Execution flow:
```
operator->open();
Row row;
while (operator->next(row)) {
    // process row
}
operator->close();
```

---

## DataSource Interface

```cpp
class DataSource {
public:
    virtual ~DataSource() = default;
    virtual const TableInfo* table_info() const = 0;
    virtual void open() = 0;
    virtual bool next(Row& out) = 0;
    virtual void close() = 0;
};
```

### InMemoryDataSource

Reference implementation: yields rows from a `std::vector<Row>`.

```cpp
class InMemoryDataSource : public DataSource {
public:
    InMemoryDataSource(const TableInfo* table, std::vector<Row> rows);
    const TableInfo* table_info() const override;
    void open() override;
    bool next(Row& out) override;
    void close() override;
private:
    const TableInfo* table_;
    std::vector<Row> rows_;
    size_t cursor_ = 0;
};
```

---

## Operator Types

### ScanOperator

Wraps a DataSource. Yields all rows from the source.

```cpp
class ScanOperator : public Operator {
    DataSource* source_;
};
```

### FilterOperator

Evaluates a WHERE/HAVING expression for each input row. Skips rows that don't match.

```cpp
class FilterOperator : public Operator {
    Operator* child_;
    const AstNode* expr_;           // WHERE expression AST
    // + evaluator context (functions, resolver, arena)
};
```

`next()` calls `child_->next()` in a loop, evaluates `expr_` for each row. Returns the first row where the expression evaluates to a truthy value (not NULL, not FALSE).

### ProjectOperator

Evaluates a list of expressions, produces a new row with computed columns.

```cpp
class ProjectOperator : public Operator {
    Operator* child_;               // null if no FROM (e.g., SELECT 1+2)
    const AstNode** exprs_;         // expression list
    uint16_t expr_count_;
};
```

If `child_` is null, produces one row (evaluates expressions with no input row).

### NestedLoopJoinOperator

For each row from the left child, scans all rows from the right child. Emits combined rows where the join condition matches.

```cpp
class NestedLoopJoinOperator : public Operator {
    Operator* left_;
    Operator* right_;
    uint8_t join_type_;             // INNER, LEFT, RIGHT, FULL, CROSS
    const AstNode* condition_;      // ON expression (null for CROSS)
};
```

For LEFT JOIN: if no right row matches, emit left row + NULLs for right columns.
For CROSS JOIN: no condition check, emit all combinations.

Right side is materialized on first `open()` (stored in a vector) since it's scanned multiple times.

### AggregateOperator

Buffers all input rows, groups by key, computes aggregate functions.

```cpp
class AggregateOperator : public Operator {
    Operator* child_;
    const AstNode** group_by_exprs_;
    uint16_t group_count_;
    const AstNode** agg_exprs_;
    uint16_t agg_count_;
};
```

On `open()`: consume all child rows, build groups (hash map keyed by group-by values).
On `next()`: yield one row per group with computed aggregates.

**Aggregate state per group:**
- COUNT: increment counter
- SUM: accumulate value
- AVG: accumulate sum + count
- MIN/MAX: track extreme value

Detects aggregate function calls in the expression AST by checking for `NODE_FUNCTION_CALL` with names COUNT/SUM/AVG/MIN/MAX.

### SortOperator

Buffers all input, sorts by key(s), yields in order.

```cpp
class SortOperator : public Operator {
    Operator* child_;
    const AstNode** keys_;
    uint8_t* directions_;           // 0=ASC, 1=DESC
    uint16_t key_count_;
};
```

Uses `std::sort` with a custom comparator that evaluates key expressions.

### LimitOperator

Counts rows, skips offset, stops at count.

```cpp
class LimitOperator : public Operator {
    Operator* child_;
    int64_t count_;
    int64_t offset_;
    int64_t emitted_ = 0;
    int64_t skipped_ = 0;
};
```

### DistinctOperator

Tracks seen row values. Skips duplicates.

```cpp
class DistinctOperator : public Operator {
    Operator* child_;
    // hash set of seen row value combinations
};
```

Uses a hash set keyed by a hash of all column values in the row.

### SetOpOperator

UNION: yield all rows from left, then all from right.
UNION ALL: same but skip deduplication.
INTERSECT: yield rows that appear in both (hash-based).
EXCEPT: yield rows from left that don't appear in right (hash-based).

```cpp
class SetOpOperator : public Operator {
    Operator* left_;
    Operator* right_;
    uint8_t op_;        // UNION=0, INTERSECT=1, EXCEPT=2
    bool all_;
};
```

---

## PlanExecutor

Converts a logical plan tree into an operator tree and executes it.

```cpp
template <Dialect D>
class PlanExecutor {
public:
    PlanExecutor(FunctionRegistry<D>& functions,
                 const Catalog& catalog,
                 Arena& arena);

    void add_data_source(const char* table_name, DataSource* source);

    ResultSet execute(PlanNode* plan);

private:
    Operator* build_operator(PlanNode* node);

    FunctionRegistry<D>& functions_;
    const Catalog& catalog_;
    Arena& arena_;
    std::unordered_map<std::string, DataSource*> sources_;
};
```

`build_operator` recursively walks the plan tree:
- SCAN → ScanOperator(find data source by table name)
- FILTER → FilterOperator(build child, expr)
- PROJECT → ProjectOperator(build child, exprs)
- JOIN → NestedLoopJoinOperator(build left, build right, condition)
- AGGREGATE → AggregateOperator(build child, group_by, agg_exprs)
- SORT → SortOperator(build child, keys, directions)
- LIMIT → LimitOperator(build child, count, offset)
- DISTINCT → DistinctOperator(build child)
- SET_OP → SetOpOperator(build left, build right, op, all)

---

## ResultSet

```cpp
struct ResultSet {
    std::vector<Row> rows;
    std::vector<std::string> column_names;
    uint16_t column_count = 0;

    size_t row_count() const { return rows.size(); }
    bool empty() const { return rows.empty(); }
};
```

All result rows are materialized in memory. The arena used for row Value storage must outlive the ResultSet.

---

## File Organization

```
include/sql_engine/
    operator.h           — Operator base class
    data_source.h        — DataSource interface + InMemoryDataSource
    result_set.h         — ResultSet struct
    operators/
        scan_op.h
        filter_op.h
        project_op.h
        join_op.h
        aggregate_op.h
        sort_op.h
        limit_op.h
        distinct_op.h
        set_op_op.h
    plan_executor.h      — PlanExecutor<D>

tests/
    test_operators.cpp       — Unit tests per operator
    test_plan_executor.cpp   — End-to-end SQL → results
```

---

## Testing Strategy

### Operator unit tests

Each operator tested in isolation with hand-built inputs:

- **ScanOperator:** yields all rows, empty source, reopen
- **FilterOperator:** keeps matching, filters all, NULL in condition
- **ProjectOperator:** column subset, computed expression, no-FROM single row
- **JoinOperator:** inner match, inner no-match, left join NULLs, cross join
- **AggregateOperator:** COUNT(*), SUM, AVG, GROUP BY with multiple groups, no-group aggregate
- **SortOperator:** ASC, DESC, multi-key, stable, single row
- **LimitOperator:** count only, offset+count, offset beyond data, zero limit
- **DistinctOperator:** removes dupes, all unique, all same
- **SetOpOperator:** UNION ALL, UNION (dedup), INTERSECT, EXCEPT

### End-to-end integration tests

Full pipeline: SQL → parse → plan → execute → verify result rows:

| SQL | Expected |
|---|---|
| `SELECT * FROM users` | All rows |
| `SELECT name FROM users WHERE age > 18` | Filtered + projected |
| `SELECT * FROM users ORDER BY age DESC LIMIT 2` | Sorted + limited |
| `SELECT dept, COUNT(*) FROM users GROUP BY dept` | Aggregated |
| `SELECT * FROM users u JOIN orders o ON u.id = o.user_id` | Joined |
| `SELECT name FROM users WHERE name LIKE 'A%'` | LIKE filter |
| `SELECT 1 + 2` | Single row: [3] |
| `SELECT DISTINCT status FROM users` | Deduplicated |
| `SELECT * FROM t1 UNION ALL SELECT * FROM t2` | Combined rows |

---

## Performance Targets

| Operation | Target |
|---|---|
| ScanOperator::next() | <20ns per row (pointer increment) |
| FilterOperator::next() | <100ns per row (expression evaluation) |
| ProjectOperator::next() | <200ns per row (N expression evaluations) |
| LimitOperator::next() | <10ns per row (counter check) |
| SortOperator (1000 rows, 1 key) | <100us total |
| AggregateOperator (1000 rows, 10 groups) | <200us total |
| Full pipeline: simple SELECT WHERE (100 rows) | <50us total |
