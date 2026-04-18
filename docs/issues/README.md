# Local Issue Backlog

This directory is the local working backlog for implementation gaps identified from the current codebase audit. Priority reflects correctness and production risk first, then maintainability, then feature breadth.

## Priority Order

### P0

1. [Distributed 2PC must require safe session pinning](01-distributed-2pc-safe-session-pinning.md) — resolved 2026-04-18 (commit ebefb9f)
9. [Single-shard route by shard key misdirects for some keys](09-shard-key-route-misdirection.md) — resolved 2026-04-18 (commit f0f2915 — FNV-1a hash + RoutingStrategy enum: HASH/RANGE/LIST)

### P1

2. [Make 2PC phase timeouts deterministic rather than best-effort](02-distributed-2pc-deterministic-phase-timeouts.md) — resolved 2026-04-18 (commit ebefb9f, bundled with issue 01)
3. [Extract shared backend and shard configuration parsing](03-shared-backend-config-parsing.md) — resolved 2026-04-18 (commit f0f2915, bundled with issue 09)
4. [Close join execution coverage gaps or reject unsupported joins earlier](04-join-operator-coverage.md) — resolved 2026-04-18 (commit 07d09cb)
8. [Aggregate projection schema wrong in single-shard mode](08-aggregate-projection-schema-single-shard.md) — resolved 2026-04-18 (commit ab8b7fb — REMOTE_SCAN.output_exprs + ResultSet.backing_lifetimes; surfaced two stacked bugs: schema and use-after-free)

### P2

5. [Tighten expression and type semantics](05-expression-and-type-semantics.md) — partial; arrays/tuples/field-access landed (commit a90d147), CHAR_LENGTH UTF-8 + PostgreSQL string casts landed (commit 9f090e5); non-literal arrays through planner, decimal int128, broader decimal semantics still open
6. [Close parser gaps around MySQL `SELECT ... INTO` and recursive CTE handling](06-parser-gaps-select-into-and-recursive-cte.md)
7. [Integrate CTE handling into the main `Session` query path](07-session-cte-integration.md) — resolved 2026-04-18

## Notes

- CTE work is intentionally held at `P2` per current priority guidance.
- All `P0` and original `P1` items resolved as of 2026-04-18. Issues 08 and 09 (surfaced by `scripts/test_sqlengine.sh`) also resolved the same day.
- Issue 05 is partial; remaining work continues per the compound-value spec and tracked in this README.
- Open: 06, 07, and the remaining items in 05.
- Each issue file includes evidence, scope, acceptance criteria, suggested verification, and resolution notes once closed.
