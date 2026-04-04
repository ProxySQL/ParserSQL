# Transactions — Design Specification

## Overview

Add transaction support: BEGIN/COMMIT/ROLLBACK for single-backend operations, and distributed two-phase commit (2PC) for cross-backend transactions. This enables correct multi-statement execution with atomicity guarantees.

Sub-project 12. Depends on: backend connections (sub-project 10), DML execution (sub-project 9).

### Goals

- **Transaction interface** — BEGIN, COMMIT, ROLLBACK abstraction
- **Single-backend transactions** — forward BEGIN/COMMIT/ROLLBACK to the backend, maintain session affinity
- **Distributed 2PC** — two-phase commit for operations spanning multiple backends
- **Savepoints** — SAVEPOINT / ROLLBACK TO / RELEASE for nested transaction points
- **Auto-commit management** — track and manage auto-commit state per session
- **Local transactions** — for InMemoryMutableDataSource (undo log for ROLLBACK)

### Constraints

- C++17
- 2PC requires XA transaction support on MySQL backends (`XA START`, `XA END`, `XA PREPARE`, `XA COMMIT`)
- PostgreSQL 2PC requires `PREPARE TRANSACTION` / `COMMIT PREPARED`
- Savepoints for distributed transactions are complex — initial implementation supports single-backend savepoints only
- No MVCC isolation for local storage (deferred)

---

## Transaction Manager

```cpp
class TransactionManager {
public:
    virtual ~TransactionManager() = default;

    // Transaction lifecycle
    virtual bool begin() = 0;
    virtual bool commit() = 0;
    virtual bool rollback() = 0;

    // Savepoints
    virtual bool savepoint(const char* name) = 0;
    virtual bool rollback_to(const char* name) = 0;
    virtual bool release_savepoint(const char* name) = 0;

    // State
    virtual bool in_transaction() const = 0;
    virtual bool is_auto_commit() const = 0;
    virtual void set_auto_commit(bool ac) = 0;
};
```

---

## Single-Backend Transactions

For queries that touch only one backend, transactions are simple pass-through:

```cpp
class SingleBackendTransactionManager : public TransactionManager {
public:
    SingleBackendTransactionManager(RemoteExecutor& executor, const char* backend_name);

    bool begin() override {
        return executor_.execute_dml(backend_, "BEGIN").success;
    }
    bool commit() override {
        return executor_.execute_dml(backend_, "COMMIT").success;
    }
    bool rollback() override {
        return executor_.execute_dml(backend_, "ROLLBACK").success;
    }
    // ... savepoints similarly forwarded
};
```

**Session affinity:** Once a transaction starts on a backend, all subsequent queries in that transaction must go to the same backend connection. The transaction manager tracks which backend owns the current transaction.

---

## Distributed Transactions (2PC)

When a transaction spans multiple backends (e.g., INSERT to shard_1 + INSERT to shard_2), we need atomicity across backends.

### MySQL XA Protocol

```
XA START 'txn_id'           -- begin on each participant
... execute DML ...
XA END 'txn_id'             -- mark end of work
XA PREPARE 'txn_id'         -- phase 1: prepare (vote)
XA COMMIT 'txn_id'          -- phase 2: commit (if all prepared)
XA ROLLBACK 'txn_id'        -- phase 2: rollback (if any failed)
```

### PostgreSQL Two-Phase Commit

```sql
BEGIN;
... execute DML ...
PREPARE TRANSACTION 'txn_id';   -- phase 1
COMMIT PREPARED 'txn_id';       -- phase 2: commit
ROLLBACK PREPARED 'txn_id';     -- phase 2: rollback
```

### DistributedTransactionManager

```cpp
class DistributedTransactionManager : public TransactionManager {
public:
    DistributedTransactionManager(RemoteExecutor& executor,
                                   const ShardMap& shards);

    bool begin() override;
    bool commit() override;    // 2PC: prepare all, then commit all
    bool rollback() override;  // rollback all participants

    // Track which backends are participating
    void enlist_backend(const char* backend_name);

private:
    RemoteExecutor& executor_;
    const ShardMap& shards_;

    std::string txn_id_;                              // unique transaction ID
    std::vector<std::string> participants_;            // enlisted backends
    std::unordered_map<std::string, bool> prepared_;   // phase 1 results

    std::string generate_txn_id();

    // 2PC phases
    bool phase1_prepare();    // XA PREPARE / PREPARE TRANSACTION on all participants
    bool phase2_commit();     // XA COMMIT / COMMIT PREPARED on all participants
    void phase2_rollback();   // XA ROLLBACK / ROLLBACK PREPARED on all participants
};
```

### 2PC Flow

```
begin():
    1. Generate unique txn_id
    2. Set state = ACTIVE

(DML operations happen — each one calls enlist_backend())

commit():
    1. For each participant: send XA END + XA PREPARE (phase 1)
    2. If ALL participants return success:
         For each participant: send XA COMMIT (phase 2)
         Return success
    3. If ANY participant fails:
         For each prepared participant: send XA ROLLBACK
         Return failure

rollback():
    1. For each participant: send XA ROLLBACK (or ROLLBACK if not yet prepared)
```

### Failure handling

- If a participant fails during phase 1 (PREPARE): rollback all participants. Transaction fails.
- If a participant fails during phase 2 (COMMIT): this is the classic 2PC problem. The prepared-but-not-committed transactions remain in limbo. A recovery mechanism (periodic check + resolve) is needed for production. For the prototype, log the error and return failure.
- Transaction ID generation: `"parsersql_" + timestamp + "_" + random_suffix` for uniqueness.

---

## Local Transactions

For InMemoryMutableDataSource, transactions use an undo log:

```cpp
class LocalTransactionManager : public TransactionManager {
    struct UndoEntry {
        enum Op { INSERT, DELETE, UPDATE };
        Op op;
        std::string table_name;
        Row old_row;      // for UPDATE/DELETE: the original row
        Row new_row;      // for INSERT/UPDATE: the new row
    };

    std::vector<UndoEntry> undo_log_;
    bool in_txn_ = false;

    bool begin() override { undo_log_.clear(); in_txn_ = true; return true; }
    bool commit() override { undo_log_.clear(); in_txn_ = false; return true; }
    bool rollback() override {
        // Apply undo log in reverse
        for (auto it = undo_log_.rbegin(); it != undo_log_.rend(); ++it) {
            // Reverse each operation
        }
        undo_log_.clear();
        in_txn_ = false;
        return true;
    }
};
```

The MutableDataSource records undo entries for each write operation when a transaction is active.

---

## Integration with Engine

### Session concept

A `Session` ties together a transaction manager, the executor, and query state:

```cpp
template <Dialect D>
class Session {
public:
    Session(PlanExecutor<D>& executor,
            TransactionManager& txn_mgr);

    // Execute any SQL statement
    ResultSet execute_query(const char* sql, size_t len);
    DmlResult execute_dml(const char* sql, size_t len);

    // Transaction control
    bool begin();
    bool commit();
    bool rollback();
    bool savepoint(const char* name);

    // Auto-commit: if true, each statement is its own transaction
    void set_auto_commit(bool ac);

private:
    PlanExecutor<D>& executor_;
    TransactionManager& txn_mgr_;
    Parser<D> parser_;
    PlanBuilder<D> builder_;
    Optimizer<D> optimizer_;
    FunctionRegistry<D> functions_;
    bool auto_commit_ = true;
};
```

The Session is the highest-level API — give it SQL, get results. It manages the full pipeline internally.

---

## File Organization

```
include/sql_engine/
    transaction_manager.h            -- TransactionManager interface
    single_backend_txn.h             -- SingleBackendTransactionManager
    distributed_txn.h                -- DistributedTransactionManager (2PC)
    local_txn.h                      -- LocalTransactionManager (undo log)
    session.h                        -- Session<D> high-level API

    (modify) mutable_data_source.h   -- Hook for undo log recording
    (modify) plan_executor.h         -- Transaction-aware DML execution

tests/
    test_local_txn.cpp               -- Local transaction tests (begin/commit/rollback)
    test_single_backend_txn.cpp      -- Single-backend transaction against real MySQL/PgSQL
    test_distributed_txn.cpp         -- 2PC against real backends
    test_session.cpp                 -- Session API end-to-end tests
```

---

## Testing Strategy

### Local transactions (test_local_txn.cpp)
- BEGIN + INSERT + COMMIT → data persists
- BEGIN + INSERT + ROLLBACK → data reverted
- BEGIN + UPDATE + ROLLBACK → original values restored
- BEGIN + DELETE + ROLLBACK → rows restored
- BEGIN + multiple operations + ROLLBACK → all reverted in order
- Savepoint: BEGIN + INSERT + SAVEPOINT + INSERT + ROLLBACK TO → second insert reverted
- Auto-commit: INSERT without BEGIN → immediately committed

### Single-backend (test_single_backend_txn.cpp)
- SKIP_IF_NO_MYSQL / SKIP_IF_NO_PGSQL
- BEGIN + INSERT + COMMIT → visible on backend
- BEGIN + INSERT + ROLLBACK → not visible
- Session affinity: all queries in transaction go to same connection

### Distributed 2PC (test_distributed_txn.cpp)
- SKIP_IF_NO_MYSQL
- BEGIN + INSERT to shard_1 + INSERT to shard_2 + COMMIT → both visible
- BEGIN + INSERT to shard_1 + INSERT to shard_2 + ROLLBACK → neither visible
- 2PC prepare failure simulation → both rolled back

### Session (test_session.cpp)
- Execute SELECT via Session → correct results
- Execute INSERT via Session → affected rows
- BEGIN/COMMIT via Session → transaction state correct
- Auto-commit mode → each statement committed independently
- Mixed: SELECT + DML in same session

---

## Performance Targets

| Operation | Target |
|---|---|
| BEGIN (local) | <1us |
| COMMIT (local, 10 operations) | <5us (clear undo log) |
| ROLLBACK (local, 10 operations) | <50us (apply undo) |
| BEGIN (single backend) | <1ms (network round-trip) |
| 2PC COMMIT (2 backends) | <10ms (4 round-trips: END + PREPARE + COMMIT × 2) |
