# Issue 07: Integrate CTE Handling Into The Main `Session` Query Path

## Priority

`P2`

## Problem

CTE execution exists through `PlanExecutor::execute_with_cte()`, but the main `Session::execute_query()` path does not use it. `PlanBuilder::build_cte()` currently just builds the main query and ignores the CTE materialization path.

## Evidence

- [include/sql_engine/plan_executor.h](/data/rene/ParserSQL/include/sql_engine/plan_executor.h:105)
- [include/sql_engine/session.h](/data/rene/ParserSQL/include/sql_engine/session.h:83)
- [include/sql_engine/plan_builder.h](/data/rene/ParserSQL/include/sql_engine/plan_builder.h:396)
- [tests/test_cte.cpp](/data/rene/ParserSQL/tests/test_cte.cpp:70)

## Risk

- `WITH` queries behave differently depending on entry path
- CTE tests validate executor-special behavior rather than the main user API

## Desired Outcome

`Session::execute_query()` should execute CTE-wrapped queries through the same high-level API that users already rely on, with planner/executor responsibilities clearly defined.

## Scope

- Decide whether CTE materialization lives in `Session`, `PlanExecutor`, or a new orchestration layer
- Make the main query path CTE-aware
- Preserve existing non-CTE behavior and plan caching rules

## Acceptance Criteria

- `Session::execute_query()` can run current non-recursive CTE queries
- Existing CTE tests can be migrated or expanded to exercise the main path
- CTE support remains intentionally limited until recursive support is explicitly tackled

## Status

Resolved 2026-04-18.

## Resolution Notes

`Session::execute_query` now detects `NODE_CTE` at the AST root and routes those queries through `PlanExecutor::execute_with_cte`. CTE materialisation runs per-call: each CTE definition is built and executed, then registered into the catalog as a synthetic in-memory table; the main `SELECT` then runs against those synthetic tables.

CTE queries deliberately bypass the plan cache. `PlanBuilder::build_cte` only builds the main `SELECT` — caching that plan would silently return wrong results because it would not re-materialise the CTE rows. Per-call re-parse + re-materialise is the trade-off for correctness; recursive CTE optimisation remains explicitly out of scope.

`PlanExecutor::execute_with_cte` was extended to apply the executor's distribute callback to (a) the inner SELECT for each CTE definition and (b) the main query. CTE queries against sharded tables now go through the distributed planner the same way non-CTE queries do; the materialised CTE rows themselves live locally.

### Test Coverage

`tests/test_session.cpp` adds four cases that exercise CTEs through the user API:

- `CteSimple` — `WITH … SELECT *`
- `CteWithAggregation` — `WITH … GROUP BY` then outer filter
- `CteMultipleDefinitions` — two CTEs, one consumed
- `CteFilteredAfterMaterialisation` — outer `WHERE` against the materialised CTE

The existing `tests/test_cte.cpp` cases (which call `execute_with_cte` directly) are unchanged and still pass.

### Future Work

- Recursive `WITH RECURSIVE` (still deferred — see issue 06 for parser-side coverage gaps).
- Plan-cache awareness for CTE queries (cache the AST + build pipeline so we re-materialise without re-parsing).
- Push CTE materialisation across the boundary into ProxySQL when this engine is embedded — large-result CTEs may want to live on a backend rather than be pulled local.

