# ParserSQL — High-Performance SQL Parser & Distributed Query Engine

A high-performance, hand-written recursive descent SQL parser **and distributed query engine** for [ProxySQL](https://github.com/sysown/proxysql). Parses MySQL and PostgreSQL with sub-microsecond latency, then plans and executes queries against local data sources or remote shards — with connection pooling, sharding, window functions, CTEs, 2PC distributed transactions, and a MySQL-wire protocol server.

Dialect selection is compile-time (`Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`) — zero runtime overhead.

## What it does

- **Parse** MySQL or PostgreSQL SQL into a compact arena-allocated AST.
- **Plan** queries into logical plans with predicate pushdown, limit pushdown, constant folding.
- **Execute** plans through a Volcano-model operator tree: scan, filter, project, join (nested loop + hash), aggregate, window, sort, limit, distinct, set ops, CTEs.
- **Distribute** queries across shards: shard-key routing, scatter/gather, distributed merge-aggregate, distributed sort, parallel shard I/O, hash-join across shards.
- **Remote backends** via libmysqlclient and libpq, with thread-safe connection pools, health checks, and per-phase statement timeouts.
- **Transactions** — local undo-log, single-backend DML, and 2PC (XA on MySQL, PREPARE TRANSACTION on PostgreSQL) with a durable WAL, crash recovery, and auto-compaction.
- **Serve MySQL clients** — ship a wire-protocol server (`mysql_server`) that speaks the MySQL protocol directly.
- **Validate** against 86K+ queries from 9 external corpora (PostgreSQL regression, MySQL MTR, CockroachDB, Vitess, TiDB, sqlparser-rs, SQLGlot).

## Performance

### Parser (sub-microsecond on modern hardware)

| Operation | Latency |
|---|---|
| Classify BEGIN | 29 ns |
| Parse SET | 107 ns |
| Parse simple SELECT | 178 ns |
| Parse complex SELECT (JOINs + GROUP BY) | 1.04 µs |
| Parse INSERT | ~200 ns |
| Emit SELECT (round-trip) | 256 ns |
| Arena reset | 4.6 ns |

### Vs. other parsers

| Parser | Simple SELECT | Complex SELECT |
|---|---|---|
| **ParserSQL** | **178 ns** | **1.04 µs** |
| libpg_query | ~4× slower | ~3–4× slower |
| sqlparser-rs (Rust) | ~25× slower | ~20× slower |

### Corpus coverage (86,467 external queries, April 2026)

| Corpus | Queries | OK Rate |
|---|---|---|
| PostgreSQL regression | 55,553 | **99.92%** |
| MySQL MTR | 2,270 | 99.91% |
| CockroachDB parser testdata | 17,429 | **99.96%** |
| SQLGlot | 1,450 | 99.10% |
| sqlparser-rs (MySQL + PG + common) | 2,431 | **100%** |
| Vitess | 2,291 | **100%** |
| TiDB | 5,043 | 99.88% |
| **Total** | **86,467** | **99.92% OK** / 0.02% error |

See [`docs/benchmarks/latest.md`](docs/benchmarks/latest.md) for the full report, and [`REPRODUCING.md`](docs/benchmarks/REPRODUCING.md) for reproduction instructions.

## Quick Start

### Build

```bash
make all                # Build library + run all 1,160 tests
make lib                # Just the static library
make bench              # Benchmarks (-O2); use scripts/run_benchmarks.sh for -O3 report
make build-sqlengine    # Interactive SQL CLI
make build-corpus-test  # Corpus validation harness
```

### Interactive CLI (`sqlengine`)

```bash
# In-memory expression evaluation (no backend)
echo "SELECT 1 + 2, UPPER('hello'), COALESCE(NULL, 42)" | ./sqlengine

# Against a MySQL backend
./sqlengine --backend "mysql://root:pass@127.0.0.1:3306/mydb?name=primary"

# Sharded across two backends
./sqlengine \
  --backend "mysql://root:pass@host1:3306/db?name=shard1" \
  --backend "mysql://root:pass@host2:3306/db?name=shard2" \
  --shard "users:id:shard1,shard2"

# With SSL/TLS
./sqlengine --backend "mysql://root:pass@host:3306/db?name=s1&ssl_mode=REQUIRED&ssl_ca=/etc/ssl/ca.pem"
```

### Parse + emit

```cpp
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

Parser<Dialect::MySQL> parser;
auto result = parser.parse("SELECT * FROM users WHERE id = 1", 32);

// result.ast has the full AST
// result.stmt_type == StmtType::SELECT

Emitter<Dialect::MySQL> emitter(parser.arena());
emitter.emit(result.ast);
StringRef sql = emitter.result();    // "SELECT * FROM users WHERE id = 1"

// Query digest for fingerprinting
Digest<Dialect::MySQL> digest(parser.arena());
DigestResult dr = digest.compute(result.ast);
// dr.normalized = "SELECT * FROM users WHERE id = ?"
// dr.hash       = 0x... (64-bit FNV-1a)

parser.reset();    // O(1) arena rewind — reuse memory
```

### Full pipeline — parse, plan, execute (local data source)

```cpp
#include "sql_parser/parser.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/in_memory_catalog.h"

using namespace sql_parser;
using namespace sql_engine;

InMemoryCatalog catalog;
catalog.add_table("", "users", {
    {"id",   SqlType::make_int(),        false},
    {"name", SqlType::make_varchar(255), true},
    {"age",  SqlType::make_int(),        true},
});

const TableInfo* table = catalog.get_table(StringRef{"users", 5});
InMemoryDataSource source(table, /* rows */);

FunctionRegistry<Dialect::MySQL> functions;
functions.register_builtins();

Parser<Dialect::MySQL> parser;
auto pr = parser.parse("SELECT name, age FROM users WHERE age > 21", 43);

PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
PlanNode* plan = builder.build(pr.ast);

PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
executor.add_data_source("users", &source);
ResultSet rs = executor.execute(plan);
```

### Sharded execution via `Session`

```cpp
#include "sql_engine/session.h"
#include "sql_engine/thread_safe_executor.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/local_txn.h"

// Backends (connection-pooled, thread-safe)
ThreadSafeMultiRemoteExecutor executor;
executor.add_backend({.name = "shard1", .host = "h1", .port = 3306, ...});
executor.add_backend({.name = "shard2", .host = "h2", .port = 3306, ...});

// Sharding policy: "users" is sharded on "id" across shard1, shard2
ShardMap shards;
shards.add_sharded_table("users", "id", {"shard1", "shard2"});

// Catalog, transactions, session
InMemoryCatalog catalog;  /* ... add_table(...) ... */
LocalTransactionManager txn;
Session<Dialect::MySQL> session(catalog, txn);
session.set_remote_executor(&executor);
session.set_shard_map(&shards);

// Execute — routes to one shard if WHERE has shard key, otherwise scatters
auto rs = session.execute_query("SELECT * FROM users WHERE id = 42");

// DML too
auto dml = session.execute_statement("UPDATE users SET age = age + 1 WHERE country = 'US'");
```

### 2PC distributed transaction

```cpp
#include "sql_engine/distributed_txn.h"
#include "sql_engine/durable_txn_log.h"

// Durable WAL for crash recovery
DurableTransactionLog log;
log.open("/var/lib/proxysql/txn.log");

DistributedTransactionManager dtxn(executor, DistributedTransactionManager::BackendDialect::MYSQL);
dtxn.set_durable_log(&log);
dtxn.set_require_durable_log(true);             // fail commit if WAL write fails
dtxn.set_phase_statement_timeout_ms(30000);     // bound 2PC per-phase duration
dtxn.set_auto_compact_threshold(1000);          // compact WAL after every 1000 completions

Session<Dialect::MySQL> session(catalog, dtxn);
session.set_remote_executor(&executor);
session.set_shard_map(&shards);

session.begin();
session.execute_statement("UPDATE accounts SET balance = balance - 100 WHERE id = 1");  // shard1
session.execute_statement("UPDATE accounts SET balance = balance + 100 WHERE id = 2");  // shard2
session.commit();   // Phase 1 PREPARE + Phase 2 COMMIT across both shards
```

### Startup recovery

```cpp
// At startup, resolve any in-doubt transactions left over from a prior crash
TransactionRecovery recovery(executor, log, BackendDialect::MYSQL);
auto report = recovery.recover();
// report.recovered_commit, recovered_rollback, still_in_doubt, ...
```

## Architecture

```
                    SQL bytes
                        │
                        ▼
                 ┌──────────────┐
                 │  Tokenizer   │  Zero-copy pull iterator, dialect-templated
                 │  <Dialect D> │  Binary-search keyword lookup (~130 keywords)
                 └──────┬───────┘
                        ▼
                 ┌──────────────┐
                 │  Classifier  │  Switch on first token → Tier 1 or Tier 2
                 └──────┬───────┘
                        │
          ┌─── Tier 1 (deep parse: SELECT/INSERT/UPDATE/DELETE/SET/...)
          │           │
          │           ▼
          │   ┌──────────────┐
          │   │ Full AST in  │
          │   │ arena (32 B  │
          │   │ nodes)       │
          │   └──────┬───────┘
          │          │
          │          ▼
          │   ┌──────────────┐
          │   │ Plan Builder │  AST → logical plan
          │   └──────┬───────┘
          │          │
          │          ▼
          │   ┌──────────────┐
          │   │   Optimizer  │  Predicate / limit pushdown, constant folding
          │   └──────┬───────┘
          │          │
          │          ▼
          │   ┌──────────────┐
          │   │  Distributed │  If ShardMap configured:
          │   │    Planner   │   - Shard-key routing (single shard)
          │   │              │   - Scatter to all shards + merge
          │   │              │   - Distributed aggregate/sort
          │   │              │   - Cross-shard join materialization
          │   └──────┬───────┘
          │          │
          │          ▼
          │   ┌──────────────┐
          │   │Plan Executor │  Volcano model: open/next/close
          │   │  (Operators) │  Local + RemoteScan via ThreadSafeMulti
          │   └──────┬───────┘
          │          │
          │          ▼
          │      ResultSet
          │
          └─── Tier 2 (lightweight): DDL, transactions, SHOW, GRANT, USE ...
```

### Execution layer

- **Volcano model** — every operator implements `open() / next(Row&) / close()`; rows are pulled top-down one at a time.
- **15 operators:** scan, filter, project, nested-loop join (5 join types), hash join, aggregate, sort, limit, distinct, set-op, CTE derived-scan, window, merge-aggregate (distributed), merge-sort (distributed), remote-scan.
- **Type system** — 30+ `SqlType::Kind` values; 14-tag runtime `Value` (null, bool, int64, uint64, double, decimal, string, bytes, date, time, datetime, timestamp, interval, json).
- **Three-valued logic** — proper NULL propagation in AND/OR/NOT, comparisons, aggregates.

### Distributed layer

- **Shard map** — tables can be unsharded (pinned to one backend) or sharded on a column across N backends. Shard-key routing when WHERE filters on the key; scatter/gather otherwise.
- **Thread-safe connection pool** — per-backend pool with checkout/checkin RAII guards, health pings, reconnection on failure, configurable connect/read/write timeouts, optional SSL/TLS.
- **Pinned sessions** — 2PC participants use sticky connections (required for XA / PREPARE TRANSACTION).
- **Parallel shard I/O** — optional thread pool for parallel remote-scan open, useful for scatter queries with many shards.
- **Plan cache** — bounded LRU of (SQL → parsed plan) per Session.

### Transaction layer

- **`LocalTransactionManager`** — in-memory transactions for local data sources (undo log, savepoints).
- **`SingleBackendTransactionManager`** — BEGIN/COMMIT/ROLLBACK forwarded to one backend.
- **`DistributedTransactionManager`** — 2PC coordinator. Per-participant sticky session. Optional durable WAL for crash recovery. Per-phase statement timeouts. Auto-compaction. Strict mode (fail commit if WAL write fails).
- **`TransactionRecovery`** — drives in-doubt transactions to completion on startup. Idempotent (safe to re-run).
- **Session-level 2PC enlistment** — `Session::execute_statement()` automatically routes DML through the distributed transaction's pinned sessions when a distributed txn is active.

### Key design decisions

- **Arena allocator** — 64 KB bump allocator per parser, O(1) reset. All AST nodes and plan nodes live in the arena. No per-node new/delete.
- **Zero-copy `StringRef`** — tokens point into the original input buffer.
- **32-byte `AstNode`** — half a cache line; intrusive linked list (first_child + next_sibling).
- **Compile-time dialect dispatch** — `if constexpr` for MySQL vs PostgreSQL differences. Zero runtime overhead.
- **Header-only parsers & operators** — maximum inlining. Only `arena.cpp`, `parser.cpp`, a few engine `.cpp` files compile separately.

## Features

### Parser

- **Tier 1 deep parse:** SELECT, INSERT, UPDATE, DELETE, SET, REPLACE, EXPLAIN, CALL, DO, LOAD DATA
- **Compound queries:** UNION / INTERSECT / EXCEPT with SQL-standard precedence and parenthesized nesting
- **CTEs:** `WITH ... [RECURSIVE] AS (...)` — non-recursive materialized, recursive planned
- **Window functions:** ROW_NUMBER, RANK, DENSE_RANK, SUM/COUNT/AVG/MIN/MAX OVER (PARTITION BY ... ORDER BY ...)
- **Subqueries:** scalar, IN, EXISTS, correlated — in SELECT/WHERE/FROM/HAVING
- **Multi-table DML:** `UPDATE t1 JOIN t2 ON ... SET ...`, `DELETE t1 FROM t1 JOIN t2 ON ...` (MySQL multi-target forms; PostgreSQL FROM/USING)
- **BigQuery extensions:** `SELECT * EXCEPT(col1, col2)`, `SELECT * REPLACE(expr AS col)`
- **Array / tuple / field-access expressions:** `ARRAY[1,2,3][2]`, `(row).field`, `ROW(...)`
- **Query reconstruction:** Parse → modify AST → emit valid SQL (round-trip)
- **Digest:** Normalize for fingerprinting (literals → `?`, IN-list collapse, keyword upper-case) + 64-bit FNV-1a hash
- **Prepared-statement cache:** LRU keyed by SQL text; one parser arena per cached plan
- **Tier 2 classification** for all other statements (DDL, transactions, SHOW, GRANT, ...)

### Query engine

- **Plan builder** — FROM → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT → window → CTE
- **Optimizer rules** — predicate pushdown, limit pushdown, constant folding
- **Operators (15)** — scan, filter, project, nested-loop join, hash join, aggregate, window, sort, limit, distinct, set-op, derived-scan (CTE materialization), merge-aggregate (distributed), merge-sort (distributed), remote-scan
- **~45 built-in functions** — arithmetic (ABS, CEIL, FLOOR, ROUND, MOD, POWER, SQRT, LOG, LN, EXP, SIGN, TRUNCATE, RAND, GREATEST, LEAST), string (UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, LTRIM, RTRIM, REPLACE, REVERSE, LEFT, RIGHT, LPAD, RPAD, REPEAT), comparison (COALESCE, NULLIF, IF, IFNULL), datetime (NOW, CURRENT_DATE, CURRENT_TIME, DATE, EXTRACT, DATE_TRUNC, DATE_FORMAT, TIMESTAMPDIFF, ...), type (CAST)
- **Catalog interface** — `Catalog` + `InMemoryCatalog`; plug in your own
- **DataSource interface** — `DataSource` + `MutableDataSource`; plug in your own storage

### Distributed execution

- **Shard routing** — shard-key lookups go to one backend; scatter queries go to all
- **Distributed aggregation** — per-shard partial aggregates + coordinator merge (COUNT+SUM+MIN+MAX + AVG from SUM/COUNT)
- **Distributed sort** — per-shard sort + coordinator merge
- **Cross-shard joins** — hash-join coordinator; materialized subquery cache
- **Cross-shard DML** — scatter INSERT/UPDATE/DELETE when no shard key; single-shard when key present
- **Cross-shard INSERT ... SELECT** — source materialized, rows routed by destination shard key

### Transactions

- **Local** — undo-log, savepoints, auto-commit
- **Single-backend** — BEGIN/COMMIT/ROLLBACK forwarded to one backend
- **Distributed 2PC** — XA (MySQL) or PREPARE TRANSACTION (PostgreSQL); per-phase statement timeouts; per-participant pinned sessions
- **Durable WAL** — write-ahead log of phase-2 decisions, atomic rewrite compaction, in-doubt scan
- **Crash recovery** — `TransactionRecovery` drives in-doubt txns to completion on startup (idempotent)
- **Auto-enlistment** — `Session::execute_statement()` automatically routes DML through pinned sessions inside a distributed txn

### Backends & connectivity

- **MySQL** — libmysqlclient with pooled and single-connection paths, UTF-8, configurable timeouts
- **PostgreSQL** — libpq with statement_timeout, UTC-normalized TIMESTAMPTZ handling
- **SSL/TLS** — `ssl_mode`, `ssl_ca`, `ssl_cert`, `ssl_key` configurable per backend for both dialects
- **Connection pool** — thread-safe with health checks, reconnection, RAII `ConnectionGuard`
- **MySQL wire-protocol server** — `mysql_server` speaks the MySQL protocol; backends are ParserSQL engines

### Thread-safety

- One `Parser` instance per thread (zero shared state, no locks)
- `ThreadSafeMultiRemoteExecutor` safe across threads
- `DistributedTransactionManager` designed for one coordinator per transaction

## Tools

| Tool | Build | Purpose |
|---|---|---|
| `sqlengine` | `make build-sqlengine` | Interactive SQL CLI; stdin, one-shot, or REPL; optional backends and sharding |
| `mysql_server` | `make build-mysql-server` | MySQL wire-protocol server fronted by the ParserSQL engine |
| `corpus_test` | `make build-corpus-test` | Read SQL from stdin/files, parse each, report OK/PARTIAL/ERROR |
| `engine_stress_test` | `make build-engine-stress` | Direct-API engine stress test |
| `bench_distributed` | `make build-bench-distributed` | Distributed query benchmark + pipeline breakdown |
| `run_bench` | `make bench` | Google-Benchmark micro-benchmarks |
| `run_tests` | `make test` | 1,160 Google-Test unit tests |

## Testing

- **1,160 unit tests** (Google Test, 50 test files)
- **86,467 external corpus queries** validated via `scripts/run_benchmarks.sh`
- **CI** — runs unit tests + a corpus-subset on every push/PR
- **Integration tests** (MySQL/PgSQL) auto-skip when no live backend is reachable

Run a single test:

```bash
./run_tests --gtest_filter="*WindowFunc*"
```

## File layout (abridged)

```
include/sql_parser/           Parser (header-only templates)
    parser.h                   Public Parser<D> API
    tokenizer.h                Pull-based, dialect-templated
    {select,insert,update,delete,set,compound_query,table_ref,expression,...}_parser.h
    emitter.h  digest.h  stmt_cache.h  ...

include/sql_engine/           Engine
    session.h                  High-level SQL API (parse+plan+optimize+distribute+execute)
    plan_builder.h  plan_executor.h  dml_plan_builder.h  distributed_planner.h  optimizer.h
    catalog.h  in_memory_catalog.h  data_source.h  mutable_data_source.h
    types.h  value.h  row.h  result_set.h  coercion.h  null_semantics.h
    operators/                 15 operator implementations
    functions/                 Built-in SQL functions
    rules/                     Optimizer rules
    backend_config.h  connection_pool.h  thread_safe_executor.h
    mysql_remote_executor.{h,cpp}  pgsql_remote_executor.{h,cpp}  multi_remote_executor.h
    shard_map.h  remote_session.h  remote_query_builder.h  subquery_executor.h
    transaction_manager.h  local_txn.h  single_backend_txn.h  distributed_txn.h
    durable_txn_log.h  transaction_recovery.h
    datetime_parse.h  thread_pool.h  engine_limits.h

src/sql_parser/               arena.cpp, parser.cpp
src/sql_engine/               function_registry.cpp, in_memory_catalog.cpp,
                              datetime_parse.cpp, mysql_remote_executor.cpp,
                              pgsql_remote_executor.cpp, multi_remote_executor.cpp

tools/    sqlengine.cpp  mysql_server.cpp  engine_stress_test.cpp  bench_distributed.cpp
tests/    1,160 Google-Test tests across 50 files
bench/    bench_parser.cpp  bench_engine.cpp  bench_comparison.cpp
scripts/  run_benchmarks.sh  run_comparison.sh
docs/benchmarks/  latest.md  comparison.md  distributed_comparison.md  REPRODUCING.md
```

## Building

```bash
make all                   # library + tests (debug -O2)
make lib                   # just libsqlparser.a

# Release benchmarks
sed 's/-g -O2/-O3/' Makefile > /tmp/Makefile.release
make -f /tmp/Makefile.release bench

# Comparison vs. libpg_query (requires third_party/libpg_query built)
cd third_party/libpg_query && make && cd ../..
make bench-compare

# Full benchmark report (builds release, runs bench + corpora, writes Markdown)
bash scripts/run_benchmarks.sh report.md
```

Requires `g++` or `clang++` with C++17 support. For MySQL/PgSQL backends, `libmysqlclient-dev` and `libpq-dev`. Google Test and Google Benchmark are vendored.

## License

See [LICENSE](LICENSE).
