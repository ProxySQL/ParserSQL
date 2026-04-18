# Issue 09: Single-Shard Route by Shard Key Misdirects for Some Keys

## Priority

`P0` — correctness bug that returns wrong (empty) results without error.

## Status

Resolved 2026-04-18.

## Problem

When a query selects on a sharded table with an equality predicate on the shard key (`SELECT … FROM t WHERE shard_key = K`), the planner is supposed to route the query to the single shard that owns key `K`. For some integer values of `K`, the route lands on the wrong shard and the query returns zero rows even though the row exists on a different shard.

This is a silent correctness failure: no error is raised; the user just sees an empty result set.

## Evidence

Two-shard demo set (`scripts/start_sharding_demo.sh`):

- Shard1 (port 13306) holds `users` rows with `id IN (1, 2, 3, 4, 5)` (Alice, Bob, Carol, Dave, Eve).
- Shard2 (port 13307) holds `users` rows with `id IN (6, 7, 8, 9, 10)` (Frank, Grace, Heidi, Ivan, Judy).

```
sql> SELECT name FROM users WHERE id = 5
+------+
| name |
+------+
+------+
0 rows in set (0.020 sec)         -- WRONG: Eve should be returned

sql> SELECT name FROM users WHERE id = 7
+------+
| name |
+------+
| Grace|
+------+
1 row in set (0.025 sec)          -- correct
```

Scatter queries on the same table return 10 rows, so both shards are reachable and connectivity is fine. The bug is specifically in the *route* decision for a single-shard equality predicate.

## Hypotheses

The shard map in `include/sql_engine/shard_map.h` uses `std::hash<int64_t>` modulo the shard count. For `id = 5`:

- If `hash(5) % 2 == 1`, the route lands on shard2 (Frank…Judy), where `id=5` does not exist → empty.
- For `id = 7`, the same hash also lands on shard2, where `id=7` exists → correct.

Two failure modes are possible and either explains the observation:

1. **Distribution is genuinely unbalanced for small keys.** Both `5` and `7` hash to the same shard. This is plausible for low-bit hash patterns. The planner is correct but the routing decision is wrong because the *data placement* in the demo doesn't follow the same hash. (Demo data is loaded by-range — 1–5 to shard1, 6–10 to shard2 — not by hash.)
2. **The planner ignores the shard key.** It might always pick the second backend (or some other deterministic non-hash choice) when there is a shard-key predicate, leaving multi-shard scatter as the only correctness path.

If hypothesis 1 is the actual cause, then the *demo data setup* is the thing that is misaligned with the engine's routing — but this still produces wrong results for any user who follows the documented demo path, which is itself a bug worth fixing (e.g., make `start_sharding_demo.sh` load by hash, or document the expected key→shard mapping, or change the routing function).

If hypothesis 2 is the cause, the bug is in `DistributedPlanner` or `ShardMap::shard_index_for_int`.

Triage steps:

1. Print the route choice for `id IN {1..10}` from `ShardMap::shard_index_for_int` directly.
2. Confirm whether `DistributedPlanner` actually consults the shard key for the route — the key path is `distribute_node` for a `FILTER(SCAN(t)) WHERE t.shard_key = literal`.
3. Decide whether the demo data setup or the routing function (or both) needs to change.

## Risk

- Wrong results returned silently. No error, no warning, no log line.
- Any user who tries the documented `./scripts/start_sharding_demo.sh` flow and runs an equality query on the shard key sees this bug. It is in the front of the user's first impression.
- All single-shard route paths are suspect until this is understood.

## Desired Outcome

For every value `K` of an integer shard key, `SELECT … FROM t WHERE shard_key = K` either:

- routes to the unique shard that owns `K` (matching the data placement), and returns the same rows as a full scatter would have, or
- if the engine decides it cannot prove ownership, falls back to scatter (correct, just slower).

The "route to the wrong shard and return empty" mode must not exist.

## Scope

- Audit `ShardMap::shard_index_for_int` and the route path in `DistributedPlanner::distribute_node`.
- Decide on the contract between data placement and shard-routing function. Either:
  - Document that data must be loaded with the same hash the router uses (and update `start_sharding_demo.sh` to do so), or
  - Switch the router to a placement function that the demo's range-based loading already matches.
- Add gtest cases in `tests/test_distributed_planner.cpp` that, for each id in 1..10, assert the routed shard equals the shard that actually contains the row (using a mock executor that records which backend received the SQL).
- Re-enable the two failing assertions in `scripts/test_sqlengine.sh sharded` (`route to shard1 (Eve)`, `route returns one row`).

## Acceptance Criteria

- For every `id IN (1..10)` against the demo two-shard setup, `SELECT name FROM users WHERE id = ?` returns exactly the row that lives on the shard the data was loaded into.
- `scripts/test_sqlengine.sh sharded` reaches 16/16 on the existing assertions.
- A new gtest case checks shard-key route correctness for at least 10 values per shard count {2, 3, 4} so the regression cannot return silently.

## Verification

```
docker rm -f parsersql-shard1 parsersql-shard2 2>/dev/null
./scripts/start_sharding_demo.sh
make build-sqlengine
./scripts/test_sqlengine.sh sharded   # 16/16
make test                             # full unit suite
```

## Resolution Notes

Both root causes addressed.

**Hash function.** Replaced `std::hash<int64_t>` (libstdc++ identity → `K mod num_shards`) with FNV-1a 64-bit applied to all 8 bytes of the key. Deterministic across compilers, doesn't have the small-key collision pattern, regression-guarded by `ShardMapHashTest.NotIdentityFunction`.

**Routing strategy.** Added `RoutingStrategy` enum to `TableShardConfig` with three values:

- `HASH` (default) — FNV-1a + modulo. Backward compatible; the existing `--shard "table:key:b1,b2"` syntax keeps producing this.
- `RANGE` — ordered `(upper_inclusive, shard_index)` entries; first match wins; values exceeding the largest bound route to the last entry's shard (matches the MySQL `PARTITION BY RANGE LESS THAN MAXVALUE` idiom). Integer-keyed only today.
- `LIST` — explicit `value -> shard_index` map. Supports both int and string keys. Misses fall back to shard 0.

**Demo aligned with routing.** `scripts/start_sharding_demo.sh` loads users 1-5 to shard1 and 6-10 to shard2; `scripts/run_sharding_demo.sh` and `scripts/test_sqlengine.sh` now declare `--shard "users:id:range:5=shard1,10=shard2"` (and the analogous `orders` line) so the routing matches the data placement. The two previously failing route assertions in `scripts/test_sqlengine.sh sharded` (`route to shard1 (Eve)`, `route returns one row`) pass without further change.

**ProxySQL alignment.** The strategy enum is the deliberate seam for a future `PROXYSQL_RULES` value that delegates to ProxySQL's `mysql_query_rules` table. The existing planner code paths in `distributed_planner.h` (`literal_to_shard_index`) call through `ShardMap` unchanged, so adding a new strategy is a one-place addition rather than a cross-cutting refactor.

**Shard-spec syntax extension.** `parse_shard_spec` in `src/sql_engine/tool_config_parser.cpp` accepts an optional strategy qualifier:

```
table:key:backend1,backend2                       # HASH (implicit, back-compat)
table:key:hash:backend1,backend2                  # HASH (explicit)
table:key:range:5=shard1,10=shard2                # RANGE
table:key:list:1=shard1,6=shard2                  # LIST (int keys)
table:key:list:us-east=a,us-west=b                # LIST (string keys)
```

A repeated backend in `range:` or `list:` is interned once into `cfg.shards`.

## Test Coverage

- `tests/test_shard_map.cpp` — 14 cases covering HASH determinism, FNV-1a divergence from identity (regression guard), RANGE matching demo placement, RANGE above-max fallback, RANGE accepts unsorted input and sorts internally, RANGE rejects spurious string lookups, LIST int + string keys, LIST miss fallback, default-strategy is HASH, out-of-range shard_index clamping, unknown table.
- `tests/test_ssl_config.cpp` — 11 added cases for the new `--shard` syntax (HASH default, explicit hash/range/list, malformed entries, non-int range bound, empty body rejection, backend interning).
- `scripts/test_sqlengine.sh sharded` — 16/16 with the new range routing.

## Future Work

- Add a `PROXYSQL_RULES` strategy that consults `mysql_query_rules`-shaped data when the engine is embedded inside ProxySQL.
- Allow RANGE on string keys (lexicographic upper bounds) if a real use case arises.
- Allow multiple key columns per table for composite sharding.
- Allow a "shard not found" sentinel so unrouteable values can be made an error rather than silently routed.
