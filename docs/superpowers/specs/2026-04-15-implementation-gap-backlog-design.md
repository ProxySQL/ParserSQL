# Implementation Gap Backlog Design

**Goal:** Create a local, detailed issue backlog for the known implementation gaps and start execution from the highest-priority correctness issue.

**Scope decision:** The implementation gaps are too broad to execute as one plan. They are decomposed into local issues in `docs/issues/`, with immediate execution limited to the first `P0` item.

## Backlog Structure

- Use `docs/issues/README.md` as the prioritized index
- Use one Markdown file per issue for problem statement, evidence, scope, acceptance criteria, and verification
- Keep the issue docs local-first so work can proceed without GitHub issue setup

## Priority

1. `P0`: distributed 2PC must require safe session pinning
2. `P1`: deterministic 2PC phase timeouts
3. `P1`: shared backend and shard config parsing
4. `P1`: join execution coverage / early rejection alignment
5. `P2`: expression and type semantic gaps
6. `P2`: parser gaps around `SELECT ... INTO` and recursive CTE handling
7. `P2`: CTE integration into the main `Session` path

CTE work is explicitly held at `P2` for now.

## First Execution Target

The first implementation target is distributed 2PC safety. The current code explicitly allows an unpinned fallback path even though the same code comments state that this can silently corrupt pooled real-backend 2PC behavior. That is the highest-risk correctness issue and should fail closed.

## Intended Change Shape

- Extend the remote executor contract so executors can declare whether unpinned distributed 2PC fallback is safe
- Keep pinned-session executors working as-is
- Keep single-connection executors and selected mocks usable by explicit opt-in, not implicit fallback
- Update distributed transaction and session tests to match the hardened contract

## Non-Goals For This Pass

- No attempt to solve all backlog items in one change
- No large transaction subsystem rewrite
- No CTE redesign in this phase
