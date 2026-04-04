# SQL Engine Optimizer — Design Specification

## Overview

Rule-based query optimizer that transforms logical plans into more efficient logical plans. Takes a plan tree, returns a better plan tree. Same type in, same type out.

Sub-project 6 of the query engine. Depends on: logical plan (sub-project 5), expression evaluator (sub-project 2), catalog (sub-project 3).

### Goals

- **Four rewrite rules:** predicate pushdown, projection pruning, constant folding, limit pushdown
- **Fixed rule sequence** — rules applied in order, one pass each
- **Correctness-preserving** — optimized plan produces identical results to unoptimized
- **Architected for extension** — cost-based optimization can be added later without rewriting

### Constraints

- C++17, arena-allocated new nodes
- Rules operate on PlanNode trees (same types as plan builder output)
- No iterative fixed-point — single pass per rule
- No table statistics needed (rule-based, not cost-based)

### Non-Goals

- Cost-based optimization (future sub-project)
- Join reordering (needs cost model)
- Index selection (needs index metadata in catalog)
- Subquery decorrelation

---

## Interface

```cpp
template <Dialect D>
class Optimizer {
public:
    Optimizer(const Catalog& catalog);

    PlanNode* optimize(PlanNode* plan, Arena& arena);

private:
    const Catalog& catalog_;
};
```

Internally applies rules in sequence:

```cpp
PlanNode* optimize(PlanNode* plan, Arena& arena) {
    plan = predicate_pushdown(plan, catalog_, arena);
    plan = projection_pruning(plan, catalog_, arena);
    plan = constant_folding(plan, catalog_, arena);
    plan = limit_pushdown(plan, catalog_, arena);
    return plan;
}
```

Each rule is a standalone function:

```cpp
using RewriteRule = PlanNode*(*)(PlanNode* node, const Catalog& catalog, Arena& arena);
```

---

## Rule 1: Predicate Pushdown

Move Filter nodes below Join nodes when the filter condition only references columns from one side of the join.

### Before

```
Filter(a.x > 10)
  └── Join(ON a.id = b.id)
        ├── Scan(a)
        └── Scan(b)
```

### After

```
Join(ON a.id = b.id)
  ├── Filter(a.x > 10)
  │     └── Scan(a)
  └── Scan(b)
```

### Algorithm

1. Walk the tree top-down
2. When encountering Filter above Join:
   - Analyze the filter expression: which tables does it reference?
   - If it references only left-side tables → push filter to left child
   - If it references only right-side tables → push filter to right child
   - If it references both sides → leave in place (can't push)
3. For compound conditions (AND): split into individual predicates, push each independently
4. Recurse into children

### Table reference analysis

To determine which tables an expression references, walk the AST expression looking for `NODE_COLUMN_REF` and `NODE_QUALIFIED_NAME` nodes. For qualified names (`a.x`), the table prefix is known. For unqualified names (`x`), look up in the catalog to find which table the column belongs to.

---

## Rule 2: Projection Pruning

If the query only needs a subset of columns, annotate or transform the plan to avoid carrying unused columns through the pipeline.

### Approach

Walk the plan tree top-down, tracking which columns are needed by each node:

1. Start from the root (Project node) — its expression list tells us which columns are needed
2. Filter nodes add their expression's column references to the needed set
3. Join conditions add their column references
4. Sort keys add their column references
5. Aggregate group-by and aggregate expressions add their column references

If a Scan node produces columns that no ancestor needs, insert a Project node immediately above the Scan to strip unused columns.

### Implementation

```cpp
PlanNode* projection_pruning(PlanNode* plan, const Catalog& catalog, Arena& arena);
```

Collect column references from all expressions in the plan. For each Scan, compare needed columns against available columns. If fewer are needed, insert a slimming Project.

---

## Rule 3: Constant Folding

Evaluate expressions that don't reference any columns at plan time.

### Examples

- `10 + 8` → `18`
- `UPPER('hello')` → `'HELLO'`
- `1 > 2` → `FALSE`
- `COALESCE(NULL, 42)` → `42`

### Algorithm

1. Walk all expression AST nodes in the plan (Filter, Project, Sort, etc.)
2. For each expression, check if it references any columns (no `NODE_COLUMN_REF`, no `NODE_QUALIFIED_NAME`)
3. If it's purely constant, evaluate it using the expression evaluator
4. Replace the expression with a new literal AST node containing the result

### Implementation

```cpp
PlanNode* constant_folding(PlanNode* plan, const Catalog& catalog, Arena& arena);
```

Uses `evaluate_expression<D>()` with a null resolver (no columns available — if it tries to resolve a column, it's not a constant expression).

The expression evaluator already handles this — we just need to replace the AST node with a literal node after evaluation.

---

## Rule 4: Limit Pushdown

Push Limit nodes past Filter nodes toward Scan nodes, enabling early termination.

### Before

```
Limit(10)
  └── Filter(active = 1)
        └── Scan(users)
```

### After

```
Limit(10)
  └── Filter(active = 1)
        └── Limit(10)
              └── Scan(users)
```

The inner Limit allows the scan to stop after 10 candidates (before filtering). The outer Limit ensures exactly 10 results after filtering.

### Constraints — when NOT to push

Do NOT push limit past:
- **Sort** — sort needs all rows before it can produce ordered output
- **Aggregate** — aggregation needs all rows to compute correct groups
- **Distinct** — needs all rows to determine uniqueness
- **Join** — limit on one side would produce incorrect join results

Only push past **Filter** nodes (and only when there's no Sort/Aggregate above).

### Implementation

```cpp
PlanNode* limit_pushdown(PlanNode* plan, const Catalog& catalog, Arena& arena);
```

Walk the tree. When seeing Limit → Filter → child, insert a new Limit(same count) between Filter and child. The extra Limit is a hint for early termination, not a correctness requirement.

---

## File Organization

```
include/sql_engine/
    optimizer.h                   — Optimizer<D> class
    rules/
        predicate_pushdown.h      — Push filters below joins
        projection_pruning.h      — Drop unused columns early
        constant_folding.h        — Evaluate constants at plan time
        limit_pushdown.h          — Push limits toward scans

tests/
    test_optimizer.cpp            — Tests for each rule + combined + correctness
```

---

## Testing Strategy

### Per-rule tests

Each rule is tested independently: build a specific plan shape, apply the rule, verify the transformed shape.

**Predicate pushdown:**
- Filter above Join → pushed to correct side
- Filter referencing both sides → stays in place
- AND with mixed predicates → split and push individually
- No Join → no change

**Projection pruning:**
- SELECT subset of columns → Project inserted above Scan
- SELECT * → no pruning needed
- Columns used in WHERE but not SELECT → still kept

**Constant folding:**
- `10 + 8` → literal 18
- `UPPER('hello')` → literal 'HELLO'
- `col + 1` → not folded (has column reference)
- Mixed: `col > 10 + 8` → `col > 18` (partial folding)

**Limit pushdown:**
- Limit → Filter → Scan → inner Limit inserted
- Limit → Sort → Scan → no change (Sort blocks pushdown)
- Limit → Aggregate → Scan → no change

### Combined optimization test

Apply all rules to a complex query and verify the final plan shape.

### Correctness test

Parse SQL → build plan → execute WITHOUT optimizer → get results.
Parse SQL → build plan → optimize → execute WITH optimizer → get results.
Verify both result sets are identical.

This is the critical test — the optimizer must never change query semantics.

---

## Performance Targets

| Operation | Target |
|---|---|
| Optimizer (simple query, 4 rules) | <10us |
| Optimizer (complex query with joins) | <50us |
| Predicate pushdown (single join) | <5us |
| Constant folding (5 constants) | <2us |
