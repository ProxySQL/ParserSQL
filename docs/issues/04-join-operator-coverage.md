# Issue 04: Close Join Execution Coverage Gaps Or Reject Unsupported Joins Earlier

## Priority

`P1`

## Status

Resolved 2026-04-18 (commit 07d09cb).

## Problem

The parser and planner can represent a broader join surface than the executor can run. Execution currently rejects `RIGHT` and `FULL` joins at runtime, and hash join only supports `INNER` and `LEFT`.

## Evidence

- [include/sql_engine/operators/join_op.h](/data/rene/ParserSQL/include/sql_engine/operators/join_op.h:33)
- [include/sql_engine/operators/hash_join_op.h](/data/rene/ParserSQL/include/sql_engine/operators/hash_join_op.h:37)

## Risk

- Runtime failures after successful parse/plan
- Confusing user-facing capability boundary
- Incomplete planner/executor contract

## Desired Outcome

Either implement the missing join forms end-to-end, or reject them consistently earlier with explicit error surfaces.

## Scope

- Decide whether to implement or early-reject `RIGHT` and `FULL`
- Align planner, executor, and tests around the same contract
- Preserve existing `INNER`, `LEFT`, and `CROSS` semantics

## Acceptance Criteria

- Unsupported joins do not make it deep into execution silently
- Supported joins are documented and tested consistently

## Resolution Notes

- Extended `NestedLoopJoinOperator` to implement `RIGHT` and `FULL` join semantics instead of rejecting them at runtime.
- Added right-side match tracking so unmatched right rows are emitted correctly for `RIGHT` and `FULL`.
- Fixed nested-loop join predicate resolution for qualified names such as `users.id` and `orders.user_id`.
- Added operator-level and plan-executor coverage for `RIGHT` and `FULL` joins.

## Verification

- `rm -f tests/test_operators.o tests/test_plan_executor.o run_tests && make run_tests && ./run_tests --gtest_filter="JoinOpTest.RightJoinIncludesUnmatchedRightRows:JoinOpTest.FullJoinIncludesUnmatchedRowsFromBothSides:HashJoinTest.RightJoinIncludesUnmatchedRightRows:HashJoinTest.FullJoinIncludesUnmatchedRowsFromBothSides" --gtest_brief=1`
- `make clean && make run_tests && ./run_tests --gtest_brief=1`
