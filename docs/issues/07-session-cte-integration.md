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

