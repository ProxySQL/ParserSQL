# Issue 01: Distributed 2PC Must Require Safe Session Pinning

## Priority

`P0`

## Status

Resolved 2026-04-18 (commit ebefb9f).

## Problem

`DistributedTransactionManager` currently falls back to unpinned `execute_dml()` calls when `checkout_session()` returns `nullptr`. That fallback is explicitly documented as unsafe for pooled real-backend 2PC because phase 1 and phase 2 may run on different physical connections.

## Evidence

- [include/sql_engine/remote_executor.h](/data/rene/ParserSQL/include/sql_engine/remote_executor.h:25)
- [include/sql_engine/distributed_txn.h](/data/rene/ParserSQL/include/sql_engine/distributed_txn.h:110)
- [include/sql_engine/distributed_txn.h](/data/rene/ParserSQL/include/sql_engine/distributed_txn.h:141)

## Risk

- Silent correctness failure in distributed transactions
- Broken XA / `PREPARE TRANSACTION` behavior when executors are pooled
- Hard-to-debug commit/rollback divergence across participants

## Desired Outcome

Distributed 2PC must fail closed unless the executor can prove one of these is true:

1. It provides pinned sessions via `checkout_session()`
2. It explicitly declares that unpinned fallback is safe for its execution model

## Scope

- Add an executor capability check to the `RemoteExecutor` API
- Update `DistributedTransactionManager` to reject unsafe fallback paths
- Keep single-connection executors and test mocks usable by explicitly opting them into legacy-safe fallback
- Update tests that currently assume all `nullptr` session paths are acceptable

## Acceptance Criteria

- A pooled executor without pinned session support cannot enlist or route DML for distributed 2PC
- Executors that guarantee single-connection-per-backend behavior can still opt into the fallback path
- Existing pinned-session behavior remains unchanged
- Regression tests cover both safe and unsafe fallback cases

## Verification

- Targeted unit tests in `tests/test_distributed_txn.cpp`
- Targeted unit tests in `tests/test_session.cpp` if route behavior changes
