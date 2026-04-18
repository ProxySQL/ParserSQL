# Issue 02: Make 2PC Phase Timeouts Deterministic

## Priority

`P1`

## Status

Resolved 2026-04-18 (commit ebefb9f).

## Problem

Per-phase timeout handling in `DistributedTransactionManager` is currently documented as best-effort. The timeout setter may run as a separate statement from the XA/PREPARE/COMMIT statement it is intended to bound, which means the timeout can miss the actual protected operation.

## Evidence

- [include/sql_engine/distributed_txn.h](/data/rene/ParserSQL/include/sql_engine/distributed_txn.h:59)

## Risk

- Coordinator hangs or long stalls during prepare/commit on unhealthy backends
- False sense of safety from a timeout API that is not deterministic

## Desired Outcome

Timeout semantics should be deterministic for the statement they claim to bound. If deterministic behavior cannot be guaranteed for a backend/executor combination, the API should either reject the configuration or document and surface the weaker behavior explicitly.

## Scope

- Decide the timeout model by backend and executor type
- Remove or sharply constrain ambiguous “best-effort” behavior
- Add tests around timeout application sequencing if feasible

## Acceptance Criteria

- The effective timeout behavior is explicit and reliable
- API behavior is consistent across supported 2PC executors
- Documentation and tests match the final semantics

## Resolution Notes

- PostgreSQL phase timeouts now use `SET statement_timeout = <ms>` immediately before `PREPARE TRANSACTION` and `COMMIT/ROLLBACK PREPARED` on the participant session.
- MySQL no longer emits `SET SESSION max_execution_time` for XA phase SQL, because that setting does not bound XA control statements.
- Enlistment no longer injects timeout SQL before `BEGIN` / `XA START`.

## Verification

- `make clean && make run_tests && ./run_tests --gtest_filter="DistributedTxnTimeout.*" --gtest_brief=1`
- `./run_tests --gtest_brief=1`
