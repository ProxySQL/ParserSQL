# PostgreSQL grammar continuation implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for independent expression and DDL work. Parent owns query integration and final verification.

**Goal:** Extend the native parser against the remaining PostgreSQL corpus, prioritizing SELECT composition and common object DDL.
**Architecture:** Preserve the current tokenizer/arena/AST architecture; add validated productions and explicit nodes where existing AST shapes cannot preserve semantics. Unsupported local engine behavior must be rejected before execution. Use the pinned PostgreSQL parser only as a development oracle.
**Tech stack:** C++17, GoogleTest, Python compatibility harness, PostgreSQL 18.4/libpg_query pinned source.
**Spec:** Continues the user-approved native grammar work documented in 2026-09-26-pg-grammar-gaps.md and the user's request to continue filling remaining gaps.

## Global constraints

- Continue branch feat/postgresql-coverage-and-ast-apis and update PR #67 with detailed commits.
- Do not commit benchmarks, benchmark README/.gitignore edits, or generated binaries.
- Preserve MySQL behavior, appended enum values, and the 48-byte AST layout.
- No runtime PostgreSQL dependency or unvalidated opaque-tail acceptance.
- Base 91880dc: 43,373 deep parses of 51,415 pinned corpus statements; preserve prior accepted coverage.

## Review focus

- Nested joins retain association and ON/USING ownership through emission.
- Keywords used as names and quoted names remain distinguishable from grammar.
- New syntax operands stay traversable; syntax constants do not become bind values.
- Malformed separators/clauses and exhausted arenas cannot produce a truncated successful AST.
- Planner guards reject new semantics rather than executing a simplified statement.

## Tasks

- [x] Expressions: tests/test_pg_expression_continuation.cpp; expression_parser.h and owned shared markers. Start with failing positive/negative fixtures for VARIADIC calls, array slices, ILIKE/SIMILAR TO/ESCAPE, POSITION/OVERLAY and IS JSON. Examples: SELECT a[:3], b[1:]; SELECT POSITION('a' IN s); SELECT OVERLAY(s PLACING 'x' FROM 2 FOR 1); SELECT f(VARIADIC ARRAY[1,2]); SELECT doc IS JSON OBJECT WITH UNIQUE KEYS. Compare emitted PostgreSQL AST, reject malformed bounds/operands and guard local execution.
- [x] DDL: tests/test_pg_object_ddl.cpp; pg_ddl_parser.h. Start with ALTER FUNCTION f(int) PARALLEL SAFE, CREATE DOMAIN d AS int CHECK(VALUE>0), CREATE TYPE t AS ENUM('a','b'), CREATE TYPE t AS(a int,b text), CREATE SEQUENCE s START WITH 3, ALTER SEQUENCE s RESTART WITH 4. Add routine options/ownership/schema/renaming, common ALTER domain/type/sequence and validated remaining DROP families. Keep names/types/expressions structural. Verify positive/negative/oracle cases.
- [x] Query composition: tests/test_pg_query_continuation.cpp; table_ref_parser.h, select_parser.h, compound_query_parser.h and pg_query_clauses.h. Tests first for parenthesized/nested joins, TABLESAMPLE/REPEATABLE, ROWS FROM, ORDER BY USING operators, row-lock strength/OF/wait policies and SELECT INTO. Preserve EXPLAIN options and inner completion while validating new query forms inside EXPLAIN. Preserve existing simple-join execution representation where possible; use appended guarded nodes for richer forms. Validate PostgreSQL AST equivalence and malformed syntax.
- [x] Integration: register tests, run independent cross-reviews, force full build and corpus runner, focused ASan/UBSan, oracle comparisons and Python harness. Replay all 51,415 statements and inspect losses/status transitions. Commit implementation first, refresh correctness snapshot against that commit, verify full compatibility workflow, commit snapshot and push/update PR #67. Report concrete remaining grammar variants without claiming complete PostgreSQL parity.

Implementation workers must not commit, run full make or change shared files outside their owned markers. Parent consolidates commits and verifies the final tree.

## Verified outcome

Implementation commit: `c3e1714`. The refreshed PostgreSQL 18.4 corpus records
45,810 of 51,415 statements as DEEP_SUPPORTED (89.1%), up 2,437 with no lost
supported cases. Final forced build: 1,455 tests passed and 37 external-backend
tests skipped. All 27 new tests passed ASan/UBSan; 87 Python harness tests passed.
The full compatibility workflow verified the refreshed baseline and 8,681 CI
cases. Independent reviews closed all findings. Benchmarks remain uncommitted.
