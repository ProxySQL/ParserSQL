# Issue 08: Aggregate Projection Schema Wrong in Single-Shard Mode

## Priority

`P1`

## Status

Resolved 2026-04-18.

## Problem

When a `--shard` config lists exactly one backend (i.e. a sharded-table declaration with a single shard, used by `setup_single_backend.sh`), bare aggregate projections (`SELECT COUNT(*) FROM users`, `SELECT SUM(salary) FROM users WHERE …`) come back with:

- A column header taken from the catalog's first table column (`id`) instead of the projection's column name (`COUNT(*)`, `SUM(salary)`).
- The aggregate value placed into the wrong slot. The displayed first column is uninitialized — `value_to_string` returns `?` for it.

The bug is invisible in pure in-memory mode (no `--shard`) and in the multi-shard sharded mode (two shards), but reproducible every time a single-shard config is in play.

## Evidence

Single backend on port 13308, sharded as `users:id:single` and `orders:id:single`:

```
sql> SELECT COUNT(*) FROM users
+----+
| id |
+----+
|  ? |
+----+
1 row in set (0.001 sec)

sql> SELECT MIN(salary), MAX(salary) FROM users
+----+-----------+
| id | name      |
+----+-----------+
|  ? | 130000.00 |
+----+-----------+
1 row in set (0.000 sec)

sql> SELECT dept, COUNT(*) FROM users GROUP BY dept
+----+------+
| id | name |
+----+------+
|  ? |    5 |
|  ? |    3 |
|  ? |    2 |
+----+------+
3 rows in set (0.000 sec)
```

Compare the GROUP BY case: the COUNT values (5, 3, 2) are correct and the row count is right, but the headers (`id`, `name`) are the catalog's first two columns and the `dept` text is missing entirely. The same `SELECT dept, COUNT(*) FROM users GROUP BY dept` against the two-shard demo set returns the right headers (`dept`, `COUNT(*)`) and the right values.

## Hypotheses

The catalog auto-discovery in `tools/sqlengine.cpp` (~line 305) registers the table's columns from `SHOW COLUMNS`. The result-set schema for a sharded-table query in 1-shard mode then appears to be derived from those catalog columns rather than from the projection list. Likely culprits:

- `DistributedPlanner::distribute_node` or the `REMOTE_SCAN` build path emitting a `column_names` set that mirrors the source table rather than the SELECT projection.
- A 1-shard fast path that bypasses `MergeAggregateOperator` and returns the underlying scan schema verbatim.
- Or `MergeAggregateOperator` producing a `Row` of catalog-table arity rather than projection arity, with the aggregate value written to slot 1 and slot 0 left null/uninitialized.

Worth checking which shard-count branch in `distribute_node` runs when there is exactly one shard — the asymmetry between 1 and 2 shards is the strongest signal.

## Risk

- Aggregate queries are unreadable in any single-shard deployment (e.g. the baseline that `setup_single_backend.sh` exists to provide).
- `setup_single_backend.sh` is documented in the README and `bench_distributed` comments as the way to compare distributed vs single-server performance — those comparisons silently produce wrong-looking results.
- Production deployments that pin a single sharded "table" to one backend hit this on every aggregate.

## Desired Outcome

`SELECT COUNT(*) FROM t`, `SELECT SUM(x) FROM t WHERE …`, and similar bare aggregates produce a result set whose `column_names` matches the projection arity and whose values land in the correct slots, regardless of whether the table is configured with 1, 2, or N shards.

## Scope

- Investigate the 1-shard branch of `DistributedPlanner` and `MergeAggregateOperator`.
- Make sure `column_names.size()`, `Row::column_count`, and the projection arity all agree at the point of result emission.
- Add a 1-shard regression test in `tests/test_distributed_planner.cpp` or `tests/test_distributed_real.cpp`.

## Acceptance Criteria

- `scripts/test_sqlengine.sh single` reaches 10/10 on the existing assertions:
  - `single: total user count` finds `10` for `SELECT COUNT(*) FROM users`.
  - `single: SUM(salary) Engineering = 530000` finds `530000`.
- The same queries against the two-shard demo set continue to pass (`scripts/test_sqlengine.sh sharded`).
- A new gtest case covers the 1-shard configuration directly so the regression cannot return silently.

## Verification

```
docker rm -f parsersql-single 2>/dev/null
./scripts/setup_single_backend.sh
make build-sqlengine
./scripts/test_sqlengine.sh single   # 10/10
./scripts/test_sqlengine.sh all      # 34/34
```

## Resolution Notes

The single bug surfaced by the test was actually two bugs stacked.

### Bug A — wrong column headers (schema)

`make_unsharded_aggregate` in `distributed_planner.h` builds a remote SQL like `SELECT COUNT(*) FROM users`, sends it to the one backend, and returns a bare `REMOTE_SCAN` plan node tagged with the source `users` table. `build_column_names` in `plan_executor.h` saw the `REMOTE_SCAN` and emitted *all of* `users.columns[]` (`id, name, age, dept, salary`) as the result schema. The user saw `id` where the result was actually `COUNT(*)`.

The multi-shard path dodged this because its `MERGE_AGGREGATE` node carried explicit `output_exprs` and had its own `build_column_names` case that used them. The 1-shard path had no equivalent.

**Fix:**

- Added optional `output_exprs` + `output_expr_count` fields to `PlanNode::remote_scan` (mirroring the same fields on `merge_aggregate`).
- New helper `make_remote_scan_with_outputs(...)` in `distributed_planner.h` populates them.
- `make_unsharded_aggregate` now uses the new helper, attaching the projection list (`group_by + agg_exprs`) to the REMOTE_SCAN.
- `build_column_names(REMOTE_SCAN)` prefers the `output_exprs` (rendered through `Emitter`) when present, and falls back to the table's catalog columns only for `SELECT *` passthrough.

### Bug B — `?` rendered values (use-after-free)

After bug A was fixed and headers became correct, every aggregate value still rendered as `?`. Tracing showed `Value::tag = 58` arriving at the renderer — a value far outside the enum range, i.e. uninitialized memory.

`PlanExecutor` is a stack-local in `Session::execute_query`. When it goes out of scope, every operator in `operators_` is destroyed. `RemoteScanOperator` owns an internal `ResultSet` (returned from `RemoteExecutor::execute`) which heap-owns the `Value` arrays the rows point into. When the operator died, those arrays were freed; the outer `ResultSet` returned to the caller had `rows[i].values` pointing at freed memory.

The bug is invisible for any query whose plan tree wraps the `REMOTE_SCAN` in a local operator that re-allocates rows in the executor's arena (PROJECT, MERGE_AGGREGATE, etc.). The 1-shard aggregate path is the one shape that yields rows directly from a `REMOTE_SCAN` to the caller — that's why issue 08 only surfaced there.

A first attempt moved heap arrays and strings out of the operator's `ResultSet` into the outer `ResultSet`. That worked for value arrays but corrupted SSO `std::string` content (every string lost its first byte) because `StringRef`s captured into the source string's inline buffer became dangling after the move.

**Fix that worked:** keep the operators themselves alive as long as the returned `ResultSet`. A new `std::vector<std::shared_ptr<void>> backing_lifetimes` on `ResultSet` holds type-erased ownership of operators released from `PlanExecutor::operators_`. The operators (and all storage they own — heap arrays, the `owned_strings` deque, the inner `ResultSet`) survive until the caller is done with the `ResultSet`. Pointers stay valid; nothing moves.

## Test Coverage

- `scripts/test_sqlengine.sh single` — 10/10 with the fix; the two failing assertions (`single: total user count` finds `10` for `SELECT COUNT(*) FROM users`; `single: SUM(salary) Engineering = 530000` finds the value) now pass.
- `scripts/test_sqlengine.sh all` — 34/34.

The shell suite is the regression guard. Anyone who reintroduces either bug will see it fail loudly within seconds of running `make test-sqlengine-single`.

## Files Touched

- `include/sql_engine/plan_node.h` — `remote_scan.output_exprs` + `output_expr_count`.
- `include/sql_engine/distributed_planner.h` — `make_remote_scan_with_outputs`; `make_unsharded_aggregate` uses it.
- `include/sql_engine/plan_executor.h` — `build_column_names(REMOTE_SCAN)` honours `output_exprs`; `execute()` releases operators into `rs.backing_lifetimes`.
- `include/sql_engine/result_set.h` — `backing_lifetimes` field; move ctor / move assignment carry it.

## Future Work

- Consider extending `output_exprs` to *every* `REMOTE_SCAN` produced by the planner, including projected (non-aggregate) pushdown, so the renderer can emit better column headers across the board (e.g. `name` instead of catalog ordinal-0).
- Audit other places where rows might be returned from operator-local heap storage without lifetime extension. `SCAN` against a `MutableDataSource` that returns owned strings could have a similar shape.
