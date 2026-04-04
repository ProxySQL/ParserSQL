# Distributed Query Planner — Design Specification

## Overview

The distributed planner decomposes logical plans across multiple backends. Given a shard map (which tables live where), it rewrites the plan to push operations to remote backends and merge results locally. This is the core of a Vitess-like distributed query engine.

Sub-project 8 of the query engine. Depends on: logical plan, optimizer, executor, emitter.

### Goals

- **Shard map** — configurable table-to-backend mapping, supports both unsharded and multi-shard tables
- **Full query decomposition** — handles cross-backend joins, distributed aggregation (with partial aggregates), distributed sort+limit (merge-sort), distributed distinct
- **Remote SQL generation** — produces SQL strings to send to backends using the existing emitter
- **RemoteExecutor interface** — abstract backend communication, mock in tests
- **3 new operators** — RemoteScan, MergeAggregate, MergeSort
- **Correctness-preserving** — distributed execution produces identical results to local execution

### Constraints

- C++17, arena-allocated
- No actual networking — uses RemoteExecutor interface (implementations deferred)
- No shard-key-aware routing (all shards queried for sharded tables)
- No distributed transactions

### Non-Goals

- Wire protocol server (separate sub-project)
- Network transport / connection pooling
- Shard key routing (skip irrelevant shards based on WHERE predicates)
- Distributed writes (INSERT/UPDATE/DELETE across backends)

---

## Shard Map

```cpp
struct ShardInfo {
    std::string backend_name;
};

struct TableShardConfig {
    std::string table_name;
    std::string shard_key;         // empty if unsharded
    std::vector<ShardInfo> shards; // 1 if unsharded, N if sharded
};

class ShardMap {
public:
    void add_table(const TableShardConfig& config);
    bool is_sharded(StringRef table_name) const;
    const std::vector<ShardInfo>& get_shards(StringRef table_name) const;
    StringRef get_shard_key(StringRef table_name) const;
};
```

---

## New Plan Node Types

```cpp
enum class PlanNodeType : uint8_t {
    // ... existing 9 types ...
    REMOTE_SCAN,        // fetch from remote backend via SQL
    MERGE_AGGREGATE,    // merge partial aggregates from N sources
    MERGE_SORT,         // merge N pre-sorted streams
};
```

### REMOTE_SCAN

```cpp
struct {
    const char* backend_name;
    const char* remote_sql;
    uint16_t remote_sql_len;
    const TableInfo* table;       // expected result schema
} remote_scan;
```

### MERGE_AGGREGATE

Sits above N children (RemoteScans), each returning partial aggregates. Merges them by group key.

Children are connected via a plan node array (left = first child, additional children linked via next pointer or stored in a side array).

Merge operations per aggregate:

| Original | Remote sends | Local merge |
|---|---|---|
| COUNT(*) | COUNT(*) | SUM of counts |
| COUNT(col) | COUNT(col) | SUM of counts |
| SUM(col) | SUM(col) | SUM of sums |
| AVG(col) | SUM(col), COUNT(col) | SUM(sums) / SUM(counts) |
| MIN(col) | MIN(col) | MIN of mins |
| MAX(col) | MAX(col) | MAX of maxes |

### MERGE_SORT

Sits above N children (RemoteScans), each returning pre-sorted results. Performs N-way merge to produce globally sorted output.

```cpp
struct {
    const AstNode** keys;
    uint8_t* directions;
    uint16_t key_count;
} merge_sort;
```

---

## Distributed Planner

```cpp
template <Dialect D>
class DistributedPlanner {
public:
    DistributedPlanner(const ShardMap& shards, const Catalog& catalog, Arena& arena);

    // Rewrite a logical plan for distributed execution
    PlanNode* distribute(PlanNode* plan);

private:
    const ShardMap& shards_;
    const Catalog& catalog_;
    Arena& arena_;

    PlanNode* distribute_scan(PlanNode* scan_node);
    PlanNode* distribute_join(PlanNode* join_node);
    PlanNode* distribute_aggregate(PlanNode* agg_node, PlanNode* source);
    PlanNode* distribute_sort_limit(PlanNode* sort_or_limit, PlanNode* source);
    PlanNode* distribute_distinct(PlanNode* distinct_node, PlanNode* source);
};
```

### Decomposition Cases

**Case 1: Single table, unsharded**

The entire sub-plan that touches one unsharded table is converted to a single RemoteScan. Filters, projections, sort, and limit are all pushed into the remote SQL.

```
Filter(age > 18) → Scan(users)    [users → backend_a]

→ RemoteScan(backend_a, "SELECT * FROM users WHERE age > 18")
```

**Case 2: Single table, sharded (N backends)**

Each shard gets its own RemoteScan with the same query. Results are combined with a local UNION ALL (SetOp node).

```
Scan(users)    [users → shard_1, shard_2, shard_3]

→ SetOp(UNION ALL)
    ├── RemoteScan(shard_1, "SELECT * FROM users")
    ├── RemoteScan(shard_2, "SELECT * FROM users")
    └── RemoteScan(shard_3, "SELECT * FROM users")
```

WHERE conditions are pushed into each remote query.

**Case 3: Aggregation on sharded table**

Each shard computes partial aggregates. A MergeAggregate node combines them locally.

```
Aggregate(GROUP BY dept, COUNT(*), AVG(sal))
  └── Scan(users, 3 shards)

→ MergeAggregate(GROUP BY dept, SUM_OF_COUNTS, SUM_OVER_SUM)
    ├── RemoteScan(shard_1, "SELECT dept, COUNT(*), SUM(sal), COUNT(sal) FROM users GROUP BY dept")
    ├── RemoteScan(shard_2, same)
    └── RemoteScan(shard_3, same)
```

AVG is decomposed: remote sends SUM + COUNT, local computes SUM/COUNT.

**Case 4: ORDER BY + LIMIT on sharded table**

Each shard returns its top-N sorted. MergeSort produces globally sorted output. Outer Limit takes final top-N.

```
Limit(10) → Sort(name ASC) → Scan(users, 3 shards)

→ Limit(10) → MergeSort(name ASC)
    ├── RemoteScan(shard_1, "SELECT * FROM users ORDER BY name LIMIT 10")
    ├── RemoteScan(shard_2, same)
    └── RemoteScan(shard_3, same)
```

**Case 5: Cross-backend JOIN**

Tables on different backends. Each side is fetched remotely, join is performed locally. Filters are pushed to the appropriate remote side.

```
Join(u.id = o.user_id)
  ├── Filter(age > 18) → Scan(users)   [backend_a]
  └── Scan(orders)                       [backend_b]

→ Local: NestedLoopJoin(u.id = o.user_id)
    ├── RemoteScan(backend_a, "SELECT * FROM users WHERE age > 18")
    └── RemoteScan(backend_b, "SELECT * FROM orders")
```

**Case 6: DISTINCT on sharded table**

Each shard computes local DISTINCT. Local DISTINCT deduplicates across shards.

```
Distinct → Scan(users, 3 shards)

→ Distinct
    └── SetOp(UNION ALL)
          ├── RemoteScan(shard_1, "SELECT DISTINCT dept FROM users")
          ├── RemoteScan(shard_2, same)
          └── RemoteScan(shard_3, same)
```

---

## Remote SQL Generation

```cpp
template <Dialect D>
class RemoteQueryBuilder {
public:
    RemoteQueryBuilder(Arena& arena);

    // Build SQL string from plan components
    StringRef build_select(const TableInfo* table,
                           const AstNode* where_expr,      // nullable
                           const AstNode** project_exprs,   // nullable
                           uint16_t project_count,
                           const AstNode** group_by,        // nullable
                           uint16_t group_count,
                           const AstNode** order_keys,      // nullable
                           uint8_t* order_dirs,
                           uint16_t order_count,
                           int64_t limit,                    // -1 = no limit
                           bool distinct);
};
```

Uses `StringBuilder` (existing) and `Emitter<D>` (for expression AST nodes) to produce SQL strings. The generated SQL is arena-allocated.

---

## Remote Executor Interface

```cpp
class RemoteExecutor {
public:
    virtual ~RemoteExecutor() = default;
    virtual ResultSet execute(const char* backend_name, StringRef sql) = 0;
};
```

For tests: `MockRemoteExecutor` — pre-configured with per-backend data. When `execute()` is called, it parses the incoming SQL using our parser, executes against in-memory data using our executor, and returns the results. This validates that the remote SQL the planner generates is correct.

---

## New Operators

### RemoteScanOperator

```cpp
class RemoteScanOperator : public Operator {
    RemoteExecutor* executor_;
    const char* backend_name_;
    StringRef remote_sql_;
    ResultSet results_;     // materialized on open()
    size_t cursor_ = 0;
};
```

On `open()`: call `executor_->execute(backend_name_, remote_sql_)` and store the ResultSet. On `next()`: yield rows from the stored results.

### MergeAggregateOperator

Takes N child operators (each returning partial aggregates), merges by group key.

On `open()`: consume all rows from all children, build merge map keyed by group-by values. For each group, combine partial aggregates (SUM counts, SUM sums, MIN of mins, etc.).

On `next()`: yield one row per merged group.

### MergeSortOperator

Takes N child operators (each returning pre-sorted rows), performs N-way merge.

On `open()`: open all children, peek at first row from each.

On `next()`: compare head rows from all children, yield the smallest (or largest for DESC), advance that child.

Uses a min-heap for efficient N-way merge: O(log N) per row.

---

## File Organization

```
include/sql_engine/
    shard_map.h                  — ShardMap, ShardInfo, TableShardConfig
    distributed_planner.h        — DistributedPlanner<D>
    remote_query_builder.h       — SQL generation for remote sub-plans
    remote_executor.h            — RemoteExecutor interface
    operators/
        remote_scan_op.h         — RemoteScanOperator
        merge_aggregate_op.h     — MergeAggregateOperator
        merge_sort_op.h          — MergeSortOperator

tests/
    test_distributed_planner.cpp — All 6 decomposition cases + correctness
```

---

## Testing Strategy

### MockRemoteExecutor

The mock uses our own engine to execute remote queries:

```cpp
class MockRemoteExecutor : public RemoteExecutor {
    // Maps backend_name → (Catalog + DataSource)
    // When execute() called: parse SQL, build plan, execute locally, return results
};
```

This validates both the planner's decomposition AND the generated SQL's correctness.

### Decomposition tests

For each of the 6 cases:
1. Set up shard map
2. Parse SQL → build logical plan → optimize → distribute
3. Walk the distributed plan → verify node types (RemoteScan, MergeAggregate, etc.)
4. Verify remote SQL strings contain expected clauses (WHERE, GROUP BY, ORDER BY, LIMIT)

### Correctness tests

For each case:
1. Execute the query locally (all data in one place) → get reference results
2. Execute the distributed plan (data split across mock backends) → get distributed results
3. Compare: results must be identical (same rows, same order for ORDER BY queries)

### Test data setup

Create a "users" table with ~20 rows, split across 3 mock backends. Include variety: different ages, departments, names for meaningful filter/group/sort testing.

---

## Performance Targets

| Operation | Target |
|---|---|
| Distribute simple unsharded query | <5us |
| Distribute sharded query (3 shards) | <20us |
| Remote SQL generation | <2us |
| MergeSort (3 streams × 100 rows) | <100us |
| MergeAggregate (3 streams × 10 groups) | <50us |
