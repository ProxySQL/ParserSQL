# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance hand-written recursive descent SQL parser **and distributed query engine** for ProxySQL. Supports MySQL and PostgreSQL dialects via compile-time templating (`Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`). Designed for sub-microsecond parser latency on the proxy hot path.

The project has four major subsystems:

1. **Parser** (`include/sql_parser/`) — tokenizer + classifier + deep parsers + emitter + digest + statement cache
2. **Query engine** (`include/sql_engine/`) — plan builder + optimizer + 15 operators + type/value system + expression evaluator + catalog + function registry
3. **Distributed layer** (`include/sql_engine/`) — shard map, distributed planner, remote executors (MySQL + PgSQL), connection pooling, parallel shard I/O, session abstraction
4. **Transaction layer** (`include/sql_engine/`) — local / single-backend / distributed 2PC transaction managers, durable WAL, crash recovery, auto-compaction

## Build Commands

```bash
make all                # Build library + run all 1160 tests
make lib                # Build only libsqlparser.a
make test               # Build + run tests
make bench              # Build + run benchmarks
make bench-compare      # Run comparison vs libpg_query (requires libpg_query built)
make build-corpus-test  # Build corpus test harness
make build-sqlengine    # Build interactive SQL engine CLI
make mysql-server       # Build MySQL wire-protocol server
make engine-stress      # Build direct-API stress harness
make bench-distributed  # Build distributed benchmark tool
make clean              # Remove all build artifacts
```

For release benchmarks: `sed 's/-g -O2/-O3/' Makefile > /tmp/Makefile.release && make -f /tmp/Makefile.release bench`.

Full benchmark + corpus report: `bash scripts/run_benchmarks.sh report.md`.

## Parser Architecture

### Three-layer pipeline

1. **Tokenizer** (`tokenizer.h`) — Zero-copy pull-based iterator, dialect-templated. Keyword lookup via sorted-array binary search. Produces `Token{type, StringRef, offset}`.
2. **Classifier** (`parser.cpp:classify_and_dispatch()`) — Switch on first token. Routes to Tier 1 deep parser or Tier 2 extractor.
3. **Statement parsers** — Each Tier 1 statement has its own header-only template class (`SelectParser<D>`, `SetParser<D>`, `InsertParser<D>`, `UpdateParser<D>`, `DeleteParser<D>`, `CompoundQueryParser<D>`).

### Key types

- `Arena` — Block-chained bump allocator. 64KB default, 1MB max. O(1) reset.
- `StringRef` — `{const char* ptr, uint32_t len}`. Zero-copy view into input buffer. Trivially copyable.
- `AstNode` — 32 bytes. Intrusive linked list (first_child + next_sibling). Arena-allocated.
- `ParseResult` — Status (OK/PARTIAL/ERROR) + stmt_type + ast + table_name/schema_name + remaining (for multi-statement).

### Namespace

Everything parser-side is in `namespace sql_parser`. All templates are parameterized on `Dialect D`.

### Adding a new deep parser

1. Create `include/sql_parser/xxx_parser.h` — header-only template following `SetParser<D>` pattern.
2. Add node types to `NodeType` enum in `common.h`.
3. Add tokens to `token.h` and both keyword tables (sorted!).
4. Add `parse_xxx()` method to `parser.h` and implement in `parser.cpp`.
5. Update `classify_and_dispatch()` switch to route to new parser.
6. Add emit methods to `emitter.h`.
7. Add `is_keyword_as_identifier()` entries in `expression_parser.h` for new keywords that can also be identifiers.
8. Update `is_alias_start()` blocklist in `table_ref_parser.h` for clause-starting keywords.
9. Write tests in `tests/test_xxx.cpp`, add to `Makefile` TEST_SRCS.

### Expression parsing

`ExpressionParser<D>` uses Pratt parsing (precedence climbing). Used by all Tier 1 parsers for WHERE conditions, SET values, function args, etc. Handles: literals, identifiers, binary/unary ops, IS NULL, BETWEEN, IN, NOT IN/BETWEEN/LIKE, CASE/WHEN, function calls, subqueries, ARRAY constructors, tuple constructors, field access, window functions (OVER), array subscripts.

### Table reference parsing

`TableRefParser<D>` is a shared utility. Used by SELECT (FROM), UPDATE (MySQL multi-table), DELETE (MySQL multi-table), INSERT (for INSERT...SELECT). Handles: simple tables, qualified names, aliases, JOINs (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL), subqueries in FROM.

### Emitter

`Emitter<D>` walks AST and produces SQL text into arena-backed `StringBuilder`. Supports:
- Normal mode: faithful round-trip reconstruction
- Digest mode (`EmitMode::DIGEST`): literals→`?`, IN collapsing, keyword uppercasing
- Bindings mode: materializes `?` placeholders with bound parameter values

## Query Engine Architecture

Everything engine-side is in `namespace sql_engine`. Templates are parameterized on `Dialect D` where dialect-specific behavior applies (coercion rules, `||` semantics, LIKE matching, 0-vs-1-based array indexing).

### Pipeline

```
Input SQL → parse → plan → optimize → [distribute] → execute → ResultSet
```

1. **Type System** (`types.h`, `value.h`) — `SqlType` describes column types (30+ kinds). `Value` is a 14-tag discriminated union for runtime values (null, bool, int64, uint64, double, decimal, string, bytes, date, time, datetime, timestamp, interval, json).
2. **Expression Evaluator** (`expression_eval.h`) — Recursively evaluates AST expression nodes against a row. Handles arithmetic, comparisons, boolean logic (three-valued), BETWEEN, IN, LIKE, CASE/WHEN, function calls, array subscripts. Uses `CoercionRules<D>` for type promotion and `null_semantics` for NULL propagation.
3. **Catalog** (`catalog.h`, `in_memory_catalog.h`) — Abstract interface for table/column metadata. `InMemoryCatalog` is the hash-map implementation. `CatalogResolver` creates column-resolve callbacks from catalog + table + row.
4. **Plan Builder** (`plan_builder.h`) — Translates a SELECT AST into a `PlanNode` tree: FROM (Scan/Join) → WHERE (Filter) → GROUP BY (Aggregate) → HAVING (Filter) → SELECT list (Project) → window → DISTINCT → ORDER BY (Sort) → LIMIT. `DmlPlanBuilder` handles INSERT/UPDATE/DELETE.
5. **Optimizer** (`optimizer.h`, `rules/*.h`) — Predicate pushdown, limit pushdown, constant folding.
6. **Distributed Planner** (`distributed_planner.h`) — If `ShardMap` is configured, rewrites the plan: shard-key routing, scatter/gather, distributed merge-aggregate, distributed merge-sort, cross-shard join materialization, cross-shard subquery rewriting.
7. **Executor** (`plan_executor.h`) — Converts a `PlanNode` tree into an `Operator` tree (Volcano model: open/next/close). Pulls rows through the tree and collects them into a `ResultSet`.

### Key engine types

- `Value` — 14-tag discriminated union. Constructors: `value_null()`, `value_int(i)`, `value_string(s)`, etc.
- `SqlType` — Column type descriptor with `Kind` enum, precision, scale, unsigned flag, timezone flag.
- `Row` — `{Value* values, uint16_t column_count}`. Arena-allocated via `make_row()`.
- `PlanNode` — Arena-allocated union node. 15 `PlanNodeType` values including SCAN, FILTER, PROJECT, JOIN, HASH_JOIN, AGGREGATE, SORT, LIMIT, DISTINCT, SET_OP, WINDOW, REMOTE_SCAN, MERGE_AGGREGATE, MERGE_SORT, INSERT_PLAN/UPDATE_PLAN/DELETE_PLAN.
- `Operator` — Abstract base class with `open()`, `next(Row&)`, `close()`. 15 implementations in `operators/`.
- `ResultSet` — `{vector<Row> rows, vector<string> column_names, uint16_t column_count}`.
- `Session<D>` (`session.h`) — High-level API: parse + plan + optimize + distribute + execute, plus plan cache, transactions, sharding.
- `DataSource` / `MutableDataSource` — Storage interface for scans (`next(Row&)`) and DML (`insert`/`delete_where`/`update_where`).

### How to add a new operator

1. Create `include/sql_engine/operators/xxx_op.h` — implement `Operator` (open/next/close).
2. Add a new `PlanNodeType` enum value in `plan_node.h` and a corresponding union member in `PlanNode`.
3. Add `build_xxx()` in `PlanExecutor` and wire it into `build_operator()` switch.
4. Add translation logic in `PlanBuilder::build_select()` (or `DmlPlanBuilder` for DML).
5. Include the new header in `plan_executor.h`.
6. Write tests in `tests/test_operators.cpp` or a dedicated test file.

### How to add a new SQL function

1. Write the function in the appropriate file under `include/sql_engine/functions/` (`arithmetic.h`, `string.h`, `comparison.h`, `datetime.h`, or `cast.h`). Signature: `Value fn(const Value* args, uint16_t arg_count, Arena& arena)`.
2. Register it in `src/sql_engine/function_registry.cpp` inside `register_builtins()`:
   ```cpp
   register_function({"MYFUNC", 6, my_func_impl, 1, 2});
   //                  name, name_len, impl, min_args, max_args (255=variadic)
   ```
3. Write tests in `tests/test_string_funcs.cpp`, `tests/test_arithmetic.cpp`, `tests/test_datetime_funcs.cpp`, or `tests/test_registry.cpp`.

### How to implement a custom DataSource

```cpp
class MySource : public DataSource {
    const TableInfo* table_info() const override;
    void open() override;
    bool next(Row& out) override;
    void close() override;
};
```

For DML, implement `MutableDataSource` (adds `insert`, `delete_where`, `update_where`). Register: `executor.add_data_source("t", &src);` / `executor.add_mutable_data_source("t", &src);`.

### How to implement a custom Catalog

```cpp
class MyCatalog : public Catalog {
    const TableInfo* get_table(StringRef name) const override;
    const TableInfo* get_table(StringRef schema, StringRef table) const override;
    const ColumnInfo* get_column(const TableInfo* table, StringRef column_name) const override;
};
```

`TableInfo` must outlive any queries that reference it. `ColumnInfo::ordinal` must match the column position in rows returned by the corresponding `DataSource`.

## Distributed Execution

### Shard map (`shard_map.h`)

Tables are either **unsharded** (pinned to one backend) or **sharded** on a column across N backends:

```cpp
ShardMap shards;
shards.add_unsharded_table("config", "primary");
shards.add_sharded_table("users", "id", {"shard1", "shard2"});
```

### Remote executors

- `RemoteExecutor` — abstract interface. `execute(backend, sql)` returns a `ResultSet`, `execute_dml(backend, sql)` returns a `DmlResult`, `checkout_session(backend)` returns a pinned `RemoteSession` (for 2PC).
- `MySQLRemoteExecutor` — single persistent connection per backend (simple).
- `PgSQLRemoteExecutor` — same for PostgreSQL.
- `MultiRemoteExecutor` — multi-dialect wrapper.
- `ThreadSafeMultiRemoteExecutor` — pooled, thread-safe. Uses `ConnectionPool` per backend. The production path. Also provides pinned `PooledMySQLSession` for 2PC participants.

### Connection pool (`connection_pool.h`)

Per-backend pool with:
- `ConnectionGuard` — RAII checkout/checkin with poisoning on error paths
- Health pings, reconnection on failure
- Configurable connect/read/write timeouts
- Optional SSL/TLS (`ssl_mode`, `ssl_ca`, `ssl_cert`, `ssl_key`)

### Session (`session.h`)

The high-level API. Holds references to `Catalog`, `TransactionManager`, optionally `RemoteExecutor`, `ShardMap`, local/mutable data sources. Provides `execute_query()`, `execute_statement()`, plan cache (LRU bounded).

`Session::execute_statement()` auto-routes DML through the transaction's pinned sessions when a distributed 2PC transaction is active. Sharded DML gets rewritten by `DistributedPlanner::distribute_dml()` into `REMOTE_SCAN` nodes.

### Distributed operators

- `RemoteScanOperator` — issues a SQL string to a backend, yields rows.
- `MergeAggregateOperator` — merges partial aggregates from shards (SUM-of-counts, SUM-of-sums, MIN-of-mins, MAX-of-maxes, AVG-from-sum-count).
- `MergeSortOperator` — k-way merge of sorted shard streams.
- `HashJoinOperator` — builds a hash table on one side, probes with the other. Used for cross-shard joins.

### Adding distributed support for a new operator

1. In `distributed_planner.h::distribute_node()`, add a case that recognizes when the operator can be pushed down vs. must stay on the coordinator.
2. If it can be pushed, rewrite into `REMOTE_SCAN` + emit the SQL via `RemoteQueryBuilder`.
3. If it must stay, optionally add a distributed merge variant (like `MERGE_AGGREGATE` / `MERGE_SORT`) that consumes partial results from shards.

## Transactions

### Three transaction managers

All implement the abstract `TransactionManager` (`transaction_manager.h`): `begin()`, `commit()`, `rollback()`, `savepoint()`, `rollback_to()`, `release_savepoint()`, `in_transaction()`, `is_auto_commit()`, `set_auto_commit()`, `is_distributed()`, `route_dml()`.

- **`LocalTransactionManager`** (`local_txn.h`) — in-memory undo log for local data sources.
- **`SingleBackendTransactionManager`** (`single_backend_txn.h`) — forwards BEGIN/COMMIT/ROLLBACK to one backend.
- **`DistributedTransactionManager`** (`distributed_txn.h`) — 2PC coordinator. Per-participant pinned sessions (required for XA / PREPARE TRANSACTION). Optional durable WAL. Per-phase statement timeouts. Auto-compaction. Strict-mode commit.

### 2PC protocol by dialect

- **MySQL:** `XA START 'txn_id'` → `XA END` → `XA PREPARE` (phase 1) → `XA COMMIT` (phase 2)
- **PostgreSQL:** `BEGIN` → `PREPARE TRANSACTION 'txn_id'` (phase 1) → `COMMIT PREPARED 'txn_id'` (phase 2)

### Durable WAL (`durable_txn_log.h`)

Text-format, tab-separated, fsync per record:

```
COMMIT\t<txn_id>\t<participant1>,<participant2>,...
ROLLBACK\t<txn_id>\t<participant1>,<participant2>,...
COMPLETE\t<txn_id>
```

Writes the phase-2 **decision** before dispatching to participants. The `COMPLETE` record is appended after phase 2 succeeds. In-doubt = DECISION without matching COMPLETE. `compact()` atomically rewrites the log keeping only in-doubt entries.

Auto-compaction: `set_auto_compact_threshold(N)` fires `compact()` after every N successful completions.

### Crash recovery (`transaction_recovery.h`)

At startup, call `TransactionRecovery::recover()` to drive in-doubt txns to completion. Idempotent — recognizes "already resolved" errors (MySQL `XAER_NOTA`, PostgreSQL "does not exist"/"not found"). Safe to re-run.

### Session auto-enlistment

`Session::execute_statement()` checks `txn_mgr_.in_transaction() && txn_mgr_.is_distributed()`. If both true and the table is remote, DML is routed through `txn_mgr_.route_dml(backend, sql)` instead of directly through the executor. This keeps the DML on the pinned 2PC connection.

### Adding a new transaction manager

Subclass `TransactionManager`. Provide `begin()/commit()/rollback()` semantics; implement `route_dml()` if the manager should intercept DML; override `is_distributed()` to opt into `Session` auto-enlistment.

## Tests

Google Test. 1,160 tests across 50 test files. Validated against 86K+ external queries from 9 corpora (99.92% fully parsed).

Run a single test: `./run_tests --gtest_filter="*WindowFunc*"`.

### Test files by component

**Parser:**
`test_tokenizer.cpp`, `test_classifier.cpp`, `test_expression.cpp`, `test_select.cpp`, `test_insert.cpp`, `test_update.cpp`, `test_delete.cpp`, `test_set.cpp`, `test_compound.cpp`, `test_emitter.cpp`, `test_digest.cpp`, `test_stmt_cache.cpp`, `test_arena.cpp`, `test_misc_stmts.cpp`, `test_star_modifiers.cpp`

**Engine (core):**
`test_value.cpp`, `test_row.cpp`, `test_coercion.cpp`, `test_null_semantics.cpp`, `test_like.cpp`, `test_expression_eval.cpp`, `test_eval_integration.cpp`, `test_catalog.cpp`, `test_registry.cpp`, `test_arithmetic.cpp`, `test_comparison.cpp`, `test_cast.cpp`, `test_string_funcs.cpp`, `test_datetime_format.cpp`, `test_datetime_funcs.cpp`, `test_result_set.cpp`

**Engine (operators & planner):**
`test_operators.cpp`, `test_plan_builder.cpp`, `test_plan_executor.cpp`, `test_optimizer.cpp`, `test_cte.cpp`, `test_window.cpp`, `test_subquery.cpp`, `test_dml.cpp`

**Distributed:**
`test_distributed_dml.cpp`, `test_distributed_planner.cpp`, `test_distributed_real.cpp`

**Transactions:**
`test_local_txn.cpp`, `test_single_backend_txn.cpp`, `test_distributed_txn.cpp`

**Integration (auto-skip without live backend):**
`test_mysql_executor.cpp`, `test_pgsql_executor.cpp`, `test_session.cpp`, `test_ssl_config.cpp`

## Benchmarks

Google Benchmark. Parser + engine + distributed + comparison benchmarks.
- `bench/bench_parser.cpp` — single-thread + multi-thread + percentile
- `bench/bench_engine.cpp` — expression eval, plan building, pipeline, operators (filter, join, sort, aggregate)
- `bench/bench_comparison.cpp` — vs libpg_query and sqlparser-rs
- `tools/bench_distributed.cpp` — distributed query benchmark with pipeline breakdown

Full benchmark report (release -O3, full corpus, Markdown output):

```bash
bash scripts/run_benchmarks.sh report.md
```

## Tools

| Tool | Source | Purpose |
|---|---|---|
| `sqlengine` | `tools/sqlengine.cpp` | Interactive SQL CLI; stdin/REPL/one-shot; backends + sharding |
| `mysql_server` | `tools/mysql_server.cpp` | MySQL wire-protocol server fronted by the engine |
| `corpus_test` | `tests/corpus_test.cpp` | Parse SQL from stdin/files, report OK/PARTIAL/ERROR |
| `engine_stress_test` | `tools/engine_stress_test.cpp` | Direct-API engine stress harness |
| `bench_distributed` | `tools/bench_distributed.cpp` | Distributed-query benchmark |

### `sqlengine` usage

```bash
# In-memory (expression eval, no backends)
echo "SELECT 1 + 2, UPPER('hello'), COALESCE(NULL, 42)" | ./sqlengine

# Interactive mode
./sqlengine

# Single MySQL backend
./sqlengine --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1"

# Multiple backends with sharding
./sqlengine \
  --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
  --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
  --shard "users:id:shard1,shard2"

# With SSL/TLS
./sqlengine --backend "mysql://root:pass@host:3306/db?name=s1&ssl_mode=REQUIRED&ssl_ca=/etc/ssl/ca.pem"
```

## Corpus testing

`corpus_test` reads SQL from stdin/files (one query per line), parses each, reports OK/PARTIAL/ERROR counts with error categorization.

Usage: `./corpus_test <mysql|pgsql> [files...]` (stdin if no files).

The full corpus (86,467 queries from PostgreSQL regression, MySQL MTR, CockroachDB, Vitess, TiDB, sqlparser-rs, SQLGlot) is downloaded on demand by `scripts/run_benchmarks.sh`. Not committed to the repo.
