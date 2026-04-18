# Distributed 2PC Safe Pinning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent distributed 2PC from silently using unsafe unpinned fallback paths unless the executor explicitly declares that fallback safe.

**Architecture:** Extend the `RemoteExecutor` contract with an explicit capability for legacy-safe unpinned 2PC fallback. `DistributedTransactionManager` will keep using pinned sessions when available, but it will reject `nullptr` session fallback unless the executor opts in. Single-connection executors and test mocks can opt in; pooled executors will remain pinned-session-only.

**Tech Stack:** C++17, GoogleTest, existing `RemoteExecutor`/`DistributedTransactionManager` interfaces

---

### Task 1: Add the executor capability flag

**Files:**
- Modify: `include/sql_engine/remote_executor.h`
- Modify: `include/sql_engine/mysql_remote_executor.h`
- Modify: `include/sql_engine/pgsql_remote_executor.h`
- Modify: `include/sql_engine/multi_remote_executor.h`
- Modify: `src/sql_engine/multi_remote_executor.cpp`

- [ ] **Step 1: Add the failing tests first**

Add tests that assert distributed 2PC fallback is rejected for an executor that returns `nullptr` from `checkout_session()` and does not explicitly opt in.

- [ ] **Step 2: Run targeted tests to verify failure**

Run: `./run_tests --gtest_filter="DistributedTxnOverrides.*:DistributedTxn.*" --gtest_brief=1`

Expected: at least one new test fails because the current code still routes through the legacy fallback.

- [ ] **Step 3: Add the API hook**

Update `RemoteExecutor` with a default capability method:

```cpp
virtual bool allows_unpinned_distributed_2pc() const { return false; }
```

Override it in single-connection executors and wrappers that are safe without pinning:

```cpp
bool allows_unpinned_distributed_2pc() const override { return true; }
```

- [ ] **Step 4: Run compile-targeted tests**

Run: `make test`

Expected: build succeeds even if the new regression tests still fail.

### Task 2: Harden distributed transaction fallback

**Files:**
- Modify: `include/sql_engine/distributed_txn.h`
- Test: `tests/test_distributed_txn.cpp`
- Test: `tests/test_session.cpp`

- [ ] **Step 1: Update the fallback contract**

Change the `nullptr` session path from implicit fallback to guarded fallback:

```cpp
if (!session) {
    if (!executor_.allows_unpinned_distributed_2pc()) {
        return false;
    }
    // legacy-safe single-connection fallback
}
```

Apply the same rule in `execute_participant_dml()` so `route_dml()` cannot bypass the safety check.

- [ ] **Step 2: Add or update regression tests**

Cover:

- executor with no sessions and no opt-in => enlist fails
- executor with no sessions and explicit opt-in => legacy fallback still works
- pinned-session executor path remains valid

- [ ] **Step 3: Run targeted tests**

Run: `./run_tests --gtest_filter="DistributedTxn.*:DistributedTxnOverrides.*" --gtest_brief=1`

Expected: all targeted distributed transaction tests pass.

### Task 3: Align comments and route expectations

**Files:**
- Modify: `include/sql_engine/remote_executor.h`
- Modify: `include/sql_engine/distributed_txn.h`
- Modify: `tests/test_session.cpp`

- [ ] **Step 1: Update comments to match the new contract**

Replace comments that describe `nullptr` as an acceptable generic fallback with language that distinguishes:

- pinned-session path
- explicit legacy-safe fallback
- rejected unsafe fallback

- [ ] **Step 2: Update tests that currently assume every `nullptr` path is allowed**

For example, tests using `TrackingRemoteExecutor` should explicitly opt in if they intend to validate the legacy-safe fallback.

- [ ] **Step 3: Run focused session tests**

Run: `./run_tests --gtest_filter="DistributedTxnOverrides.*:Session*" --gtest_brief=1`

Expected: updated transaction and session routing tests pass.

### Task 4: Final verification

**Files:**
- Modify: `include/sql_engine/remote_executor.h`
- Modify: `include/sql_engine/distributed_txn.h`
- Modify: `include/sql_engine/mysql_remote_executor.h`
- Modify: `include/sql_engine/pgsql_remote_executor.h`
- Modify: `include/sql_engine/multi_remote_executor.h`
- Modify: `src/sql_engine/multi_remote_executor.cpp`
- Modify: `tests/test_distributed_txn.cpp`
- Modify: `tests/test_session.cpp`

- [ ] **Step 1: Run the full targeted verification set**

Run:

```bash
./run_tests --gtest_filter="DistributedTxn.*:DistributedTxnOverrides.*:Session*" --gtest_brief=1
```

Expected: all matching tests pass.

- [ ] **Step 2: Run the full suite**

Run:

```bash
./run_tests --gtest_brief=1
```

Expected: no regressions; backend-dependent tests may still skip if local services are unavailable.

- [ ] **Step 3: Commit**

```bash
git add include/sql_engine/remote_executor.h \
        include/sql_engine/distributed_txn.h \
        include/sql_engine/mysql_remote_executor.h \
        include/sql_engine/pgsql_remote_executor.h \
        include/sql_engine/multi_remote_executor.h \
        src/sql_engine/multi_remote_executor.cpp \
        tests/test_distributed_txn.cpp \
        tests/test_session.cpp \
        docs/issues/README.md \
        docs/issues/01-distributed-2pc-safe-session-pinning.md \
        docs/superpowers/specs/2026-04-15-implementation-gap-backlog-design.md \
        docs/superpowers/plans/2026-04-15-distributed-2pc-safe-pinning.md
git commit -m "fix: fail closed on unsafe distributed 2pc fallback"
```
