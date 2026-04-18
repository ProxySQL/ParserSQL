# Current State

## Documentation Inventory

The repository already has useful documentation, but it is spread across several audiences:

- `README.md` is the public overview and quick-start document.
- `CLAUDE.md` is the most detailed architecture guide today; it is accurate in broad strokes, but it is written for coding agents and maintainers rather than new contributors.
- `AGENTS.md` covers contributor workflow and repository conventions.
- `docs/benchmarks/` contains benchmark outputs and reproduction notes.
- `docs/superpowers/specs/` and `docs/superpowers/plans/` preserve design intent and implementation plans from earlier work.

## Implementation Snapshot

As of April 15, 2026, the codebase is a real four-layer system rather than just a parser prototype:

1. Parser in `include/sql_parser/` and `src/sql_parser/`
2. Query engine in `include/sql_engine/`
3. Distributed execution and remote backends in `include/sql_engine/` and `src/sql_engine/`
4. Transaction management, including 2PC, durable WAL, and recovery

Operational entry points exist for interactive use and experiments: `sqlengine`, `mysql_server`, `bench_distributed`, `engine_stress_test`, and `corpus_test`.

Fresh verification on April 15, 2026:

- `./run_tests --gtest_brief=1`
- Result: 1,197 tests ran, 1,160 passed, 37 skipped because live MySQL/PostgreSQL backends were not available locally

## Strengths

- Clear subsystem boundaries: parser, engine, distributed layer, and transactions are easy to identify from the directory layout.
- Strong unit-test signal: 1,160 passing tests plus CI across Linux and macOS.
- Useful performance discipline: benchmark tooling, published benchmark reports, and corpus validation are already part of the repository workflow.
- Good internal architecture notes: `CLAUDE.md` gives maintainers practical file-level guidance for extending the system.

## Weaknesses and Risks

- Public docs had drifted from the `Makefile`; several tool build targets were named incorrectly until this update.
- Documentation is fragmented. The most detailed design knowledge lives in `CLAUDE.md` and historical spec/plan files, not in one current contributor-facing document.
- Several critical components are large, concentrated files or headers, especially `include/sql_engine/distributed_planner.h`, `include/sql_engine/plan_executor.h`, `src/sql_parser/parser.cpp`, and `tools/mysql_server.cpp`.
- Backend URL parsing and related setup logic are duplicated across `tools/sqlengine.cpp`, `tools/mysql_server.cpp`, `tools/bench_distributed.cpp`, `tools/engine_stress_test.cpp`, and mirrored again in `tests/test_ssl_config.cpp`.
- Some remote/distributed verification paths depend on live services, so local default test runs still skip meaningful backend coverage.

## What Is Missing

- A contributor-oriented local setup guide for running MySQL and PostgreSQL integration paths with the existing `scripts/`.
- One authoritative architecture/status document before this file; maintainers had to reconstruct “current truth” from README, CLAUDE, code comments, and old plans.
- A documented list of known limitations and non-goals for the parser, executor, and distributed transaction path.
- A prioritized roadmap tying the current implementation to the next engineering milestone.

## Recommended Next Step

The highest-leverage next step is to consolidate backend/tool configuration into one shared module and document one supported local integration workflow around it.

Why this should go first:

- It removes copy-pasted parsing/setup logic from four tools and one test helper.
- It reduces the chance that SSL, backend naming, or shard parsing diverges between entry points.
- It creates a stable base for stronger end-to-end tests and clearer contributor setup docs.

Suggested scope for that next phase:

1. Extract backend URL and shard parsing into a shared utility under `include/sql_engine/` or `tools/`.
2. Update `sqlengine`, `mysql_server`, `bench_distributed`, `engine_stress_test`, and `tests/test_ssl_config.cpp` to use the shared code.
3. Add a short “local backend test workflow” doc that uses the existing `scripts/start_test_backends.sh` and related helpers.
4. Add one smoke-level verification path that exercises a live backend with the shared configuration code.
