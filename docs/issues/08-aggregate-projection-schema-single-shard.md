# Issue 08: Aggregate Projection Schema Wrong in Single-Shard Mode

## Priority

`P1`

## Status

Open. Surfaced by `scripts/test_sqlengine.sh single` on 2026-04-18.

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
./scripts/test_sqlengine.sh single   # expect 10/10
make test                            # full unit suite
./scripts/test_sqlengine.sh all      # expect all-green
```
