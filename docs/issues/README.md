# Local Issue Backlog

This directory is the local working backlog for implementation gaps identified from the current codebase audit. Priority reflects correctness and production risk first, then maintainability, then feature breadth.

## Priority Order

### P0

1. [Distributed 2PC must require safe session pinning](01-distributed-2pc-safe-session-pinning.md) — implemented in current working tree
9. [Single-shard route by shard key misdirects for some keys](09-shard-key-route-misdirection.md) — resolved 2026-04-18 (FNV-1a hash + RoutingStrategy enum: HASH/RANGE/LIST)

### P1

2. [Make 2PC phase timeouts deterministic rather than best-effort](02-distributed-2pc-deterministic-phase-timeouts.md) — implemented in current working tree
3. [Extract shared backend and shard configuration parsing](03-shared-backend-config-parsing.md) — implemented in current working tree
4. [Close join execution coverage gaps or reject unsupported joins earlier](04-join-operator-coverage.md) — implemented in current working tree
8. [Aggregate projection schema wrong in single-shard mode](08-aggregate-projection-schema-single-shard.md) — resolved 2026-04-18 (REMOTE_SCAN.output_exprs + ResultSet.backing_lifetimes; surfaced two stacked bugs: schema and use-after-free)

### P2

5. [Tighten expression and type semantics](05-expression-and-type-semantics.md)
6. [Close parser gaps around MySQL `SELECT ... INTO` and recursive CTE handling](06-parser-gaps-select-into-and-recursive-cte.md)
7. [Integrate CTE handling into the main `Session` query path](07-session-cte-integration.md)

## Notes

- CTE work is intentionally held at `P2` per current priority guidance.
- `P0` (issue 01) plus all original `P1` items (02–04) are implemented in the working tree.
- Issues 08 and 09 were surfaced by `scripts/test_sqlengine.sh` and remain open.
- Each issue file includes evidence, scope, acceptance criteria, and suggested verification.
