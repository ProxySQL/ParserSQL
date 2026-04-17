# `sqlengine` — Interactive SQL Engine CLI

`sqlengine` is the primary interactive entry point to the SQL engine. It is the binary you reach for when you want to **try the engine** — evaluate an expression, run a query against a real backend, exercise sharding, or step through a 2PC transaction by hand.

It is the most useful tool in the repo for demos, blog post screencasts, smoke tests after a build, and answering "wait, does X actually work end-to-end?".

> Source: `tools/sqlengine.cpp` (one file, ~436 lines).
> Build: `make build-sqlengine`. Output: `./sqlengine`.

---

## 1. The two modes

`sqlengine` has exactly two modes; the mode is decided by whether you pass any `--backend` flag.

| Mode             | Trigger                | What it does                                                                  |
| ---------------- | ---------------------- | ----------------------------------------------------------------------------- |
| **In-memory**    | no `--backend`         | Uses `InMemoryCatalog` + `LocalTransactionManager`. Evaluates literal expressions and constant queries. No tables, no remote I/O. |
| **Backend-connected** | one or more `--backend` | Wires up a `ThreadSafeMultiRemoteExecutor` against the listed MySQL/PostgreSQL backends. Optionally adds a `ShardMap` from `--shard` flags. Real distributed query execution. |

Either mode runs the same REPL.

---

## 2. Invocation

```text
./sqlengine [OPTIONS]

Options:
  --backend URL    Add a backend (mysql://... or pgsql://... or postgres://... or postgresql://...)
  --shard SPEC     Add a shard config (table:key:shard1,shard2,...)
  --help           Show built-in help
```

### Backend URL syntax

Parsed by `parse_backend_url` in `src/sql_engine/tool_config_parser.cpp`. Accepted schemes: `mysql`, `pgsql`, `postgres`, `postgresql`.

```text
mysql://USER[:PASSWORD]@HOST[:PORT]/DATABASE?KEY=VALUE&...
```

Required query parameter: `name=` — the logical name used by `--shard` and by the WAL.

Optional query parameters: `ssl_mode`, `ssl_ca`, `ssl_cert`, `ssl_key`.

Example:

```text
mysql://root:test@127.0.0.1:13306/testdb?name=shard1
pgsql://app:secret@db1:5432/orders?name=primary&ssl_mode=REQUIRED&ssl_ca=/etc/ssl/ca.pem
```

### Shard spec syntax

```text
TABLE:SHARD_KEY:BACKEND1,BACKEND2,...
```

Backend names refer to the `name=` value from the backend URLs. A table with one backend is unsharded but pinned. Two or more backends turns on hash-based sharding by `SHARD_KEY`.

Example:

```text
--shard "users:id:shard1,shard2,shard3"
```

### Multiple flags

`--backend` and `--shard` are repeatable. Order does not matter — backends are registered first, then shards.

---

## 3. REPL behaviour

`sqlengine` reads SQL from stdin. It auto-detects whether stdin is a TTY:

- **Interactive** (TTY): prints a banner, lists connected backends, prompts with `sql> `, exits on `Ctrl+D`, `quit`, `exit`, or `\q`.
- **Piped** (not a TTY): silent — reads to EOF, prints results inline. Good for one-shot demos and scripted tests.

### Statement parsing rules

These are not power-user details — they are rough edges to know about:

- **One statement per line.** Multi-line queries are not supported. A trailing `;` is stripped.
- **Empty lines are skipped.**
- **Line-leading comments are skipped:** `-- ...` and `/* ...`. Inline comments inside a statement are passed through to the parser.
- **Quit tokens:** `quit`, `exit`, `\q`. (No `\help`, no `\d`, no other meta-commands.)

### Output format

Queries (SELECT, SHOW, DESCRIBE, EXPLAIN) print a MySQL-style table, plus a row count and elapsed time:

```text
+----+-----------+
| id | name      |
+----+-----------+
|  1 | alice     |
|  2 | bob       |
+----+-----------+
2 rows in set (0.003 sec)
```

DML statements (INSERT, UPDATE, DELETE, BEGIN, COMMIT, …) print one of:

```text
Query OK, 1 row affected (0.012 sec)
ERROR: <message>
```

Parse errors are reported inline with the message from the parser:

```text
ERROR: parse error — unexpected token ',' (0.000 sec)
```

---

## 4. Two important behaviours that are not in `--help`

### 4.1 Automatic schema discovery from the first backend

When you start in backend-connected mode **with at least one `--shard`**, `sqlengine` queries each sharded table's first backend with `SHOW COLUMNS FROM <table>` and registers the result in the local `InMemoryCatalog`. This is what lets queries against sharded tables type-check and plan.

Caveats — flag these in any demo:

- Discovery uses `SHOW COLUMNS` (MySQL syntax). Against a PostgreSQL backend it will silently fail and the table will not appear in the catalog.
- Type mapping is intentionally rough: anything containing `int` becomes `INT`, anything containing `decimal` becomes `DECIMAL(10,2)`, anything containing `date` becomes `DATE`, everything else falls back to `VARCHAR(255)`. Fine for demos; not a reflection of a column's true type.
- Discovery only runs for tables named in a `--shard` flag. Unsharded tables are not auto-registered. (You can still query them via REMOTE_SCAN passthrough; they just won't have catalog metadata locally.)

### 4.2 The dialect is hard-coded to MySQL

The session is `Session<Dialect::MySQL>`. The MySQL keyword tables, `||` semantics, LIKE rules, and 0-vs-1-based array indexing apply regardless of which backend you are talking to.

This means:

- You can connect to a PostgreSQL backend and queries will be sent to it, but they will be *parsed and rewritten* with MySQL grammar first. PostgreSQL-specific syntax (`PREPARE TRANSACTION`, `RETURNING`, `::` casts, `'string' || 'string'` for concat in some configurations) may not parse.
- Cross-dialect setups in `--backend` are technically allowed but the practical sweet spot today is MySQL backends.
- There is no `--dialect` flag yet.

---

## 5. What you can actually do — recipe book

Each recipe is meant to be runnable as-is. Replace ports / hosts / credentials as needed.

### 5.1 In-memory expression evaluation (no backends)

```bash
echo "SELECT 1 + 2, UPPER('hello'), COALESCE(NULL, 42)" | ./sqlengine
```

Demonstrates: parser, expression evaluator, function registry, three-valued NULL logic. Zero infrastructure.

### 5.2 Interactive REPL, in-memory

```bash
./sqlengine
```

```text
sql> SELECT 1 + 2 AS x, UPPER('hi')
sql> SELECT CASE WHEN 1 < 2 THEN 'yes' ELSE 'no' END
sql> SELECT NOW(), CURRENT_DATE
sql> \q
```

### 5.3 Single backend (passthrough)

```bash
./sqlengine \
  --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1"
```

Then in the REPL:

```text
sql> SELECT 1 + 1
sql> SELECT version()
```

Useful smoke test that the executor and connection pool can talk to a real backend.

### 5.4 Sharded query with scatter/gather

Two backends, one sharded table:

```bash
./sqlengine \
  --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
  --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
  --shard "users:id:shard1,shard2"
```

```text
sql> SELECT id, name FROM users WHERE id = 42      -- single-shard route
sql> SELECT COUNT(*) FROM users                    -- scatter + MERGE_AGGREGATE
sql> SELECT name FROM users ORDER BY id LIMIT 10   -- scatter + MERGE_SORT
```

### 5.5 Cross-shard JOIN

With two sharded tables on the same backends:

```bash
./sqlengine \
  --backend "mysql://...@shard1...?name=shard1" \
  --backend "mysql://...@shard2...?name=shard2" \
  --shard "users:id:shard1,shard2" \
  --shard "orders:user_id:shard1,shard2"
```

```text
sql> SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name
```

The planner emits scatter scans, builds a hash table on one side via `HashJoinOperator`, and aggregates with `MERGE_AGGREGATE`.

### 5.6 SSL/TLS to a backend

```bash
./sqlengine --backend "mysql://app:secret@db1:3306/orders?name=primary&ssl_mode=REQUIRED&ssl_ca=/etc/ssl/ca.pem&ssl_cert=/etc/ssl/client.crt&ssl_key=/etc/ssl/client.key"
```

### 5.7 Local transaction (single backend)

```text
sql> BEGIN
sql> INSERT INTO t VALUES (1)
sql> SELECT * FROM t
sql> ROLLBACK
sql> SELECT * FROM t   -- empty
```

> Note: `sqlengine` instantiates a `LocalTransactionManager`, not `SingleBackendTransactionManager` or `DistributedTransactionManager`. So today, transaction *semantics* in `sqlengine` follow the local manager — useful for exercising `BEGIN/COMMIT/ROLLBACK/SAVEPOINT` against in-memory data, but **not** the right tool for a 2PC demo. See §6.

---

## 6. What `sqlengine` does **not** do today

These are real gaps to know before you film a demo or ship a deck:

- **No 2PC demos out of the box.** The transaction manager is `LocalTransactionManager`. To exercise the `DistributedTransactionManager` end-to-end you need a small custom harness, or `bench_distributed`, or the integration tests under `tests/test_distributed_real.cpp` and `tests/test_distributed_txn.cpp`.
- **No multi-line statements.** Each statement must fit on one line.
- **No `\` meta-commands** beyond `\q`. No `\d`, no `\h`, no `\timing` toggle (timing is always on).
- **No `--dialect` flag.** Always parses as MySQL.
- **No persistent history file.** Use `rlwrap ./sqlengine` if you want readline-style history and editing.
- **Schema discovery is MySQL-only and intentionally lossy.** See §4.1.
- **No prepared statements over the wire** — the prepared-statement *cache* is on the parser side, but `EXECUTE`/`DEALLOCATE` are Tier 2 extracted statements and not executed against backends.

---

## 7. Companion tools (for context)

`sqlengine` is the interactive front-end. Other tools in `tools/` cover paths it doesn't:

| Tool                  | Source                              | When to reach for it                                                        |
| --------------------- | ----------------------------------- | --------------------------------------------------------------------------- |
| `mysql_server`        | `tools/mysql_server.cpp`            | A MySQL wire-protocol server fronted by the engine. Connect any MySQL client (`mysql` CLI, your app) and the engine handles the query. |
| `bench_distributed`   | `tools/bench_distributed.cpp`       | Throughput / latency benchmarking of distributed queries. Pipeline breakdown. |
| `engine_stress_test`  | `tools/engine_stress_test.cpp`      | Multi-threaded direct-API stress harness. No client protocol overhead.       |
| `corpus_test`         | `tests/corpus_test.cpp`             | Parse SQL from stdin/files and report OK/PARTIAL/ERROR. Used for the 86K corpus run. |

All four share the same backend / shard configuration syntax via `tool_config_parser` (in the working tree).

---

## 8. Where to look in the source

- `tools/sqlengine.cpp` — the whole tool, top to bottom. Worth reading once.
- `include/sql_engine/session.h` — the `Session<D>` class that ties parser + plan + optimize + distribute + execute together. `sqlengine` is a thin REPL on top of this.
- `include/sql_engine/multi_remote_executor.h`, `connection_pool.h` — the backend connection layer.
- `include/sql_engine/tool_config_parser.h` — the URL / shard parsing (working tree).
- `include/sql_engine/in_memory_catalog.h`, `catalog.h` — the catalog into which `sqlengine` registers auto-discovered schemas.

---

## 9. Suggested "first 10 minutes" path

If a new contributor or a viewer asks "show me what this thing does", in this order:

1. `make build-sqlengine`
2. `echo "SELECT 1 + 2, UPPER('hi')" | ./sqlengine` — proves the engine works in 5 seconds with no setup.
3. `./sqlengine` — interactive REPL, run a `CASE WHEN`, a `COALESCE`, a `NOW()`.
4. Spin up one MySQL backend; run §5.3 — proves real backend integration.
5. Spin up two MySQL backends; run §5.4 — the *distributed query* moment, where the project's value becomes visible.

Steps 1–3 cost nothing and already make a watchable demo. Steps 4–5 are the headline.
