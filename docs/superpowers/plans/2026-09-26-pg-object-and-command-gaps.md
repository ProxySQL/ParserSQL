# PostgreSQL object and command gaps implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for independent grammar tasks; root owns integration and commits.

**Goal:** Continue native PostgreSQL coverage, prioritizing aggregate/operator definitions, roles/comments, cursors and prepared statements.
**Architecture:** Extend validated productions with structured arena AST nodes. Reuse existing DDL clause/list/name/type nodes; introduce a guarded command root for non-DDL commands. No arbitrary SQL tail nodes or PostgreSQL runtime dependency.
**Tech stack:** C++17, GoogleTest, pinned PostgreSQL 18.4/libpg_query development oracle.
**Spec:** Continues the approved native grammar design and the user's request to fill the remaining gaps.

## Global constraints

- Existing branch feat/postgresql-coverage-and-ast-apis; update PR #67 with detailed commits.
- Exclude benchmarks, benchmark-only README/.gitignore changes and binaries.
- Preserve MySQL behavior, existing enum values and the 48-byte AST layout.
- Baseline 854f752: 45,810 / 51,415 deeply parsed statements; inspect every lost case.
- Validate positive/negative grammar, raw-AST round trips, allocation failures and execution guards.

## Review focus

- Operator names and routine signatures use the correct PostgreSQL identifier/type categories.
- Names, literals, options and query bodies remain traversable and retain meaning through emission.
- Optional clauses cannot absorb malformed tokens or silently disappear on allocation failure.
- PostgreSQL-only commands never run through unsupported local engine paths.
- Source corpus is unchanged; measured improvements must reflect actual grammar support.

## Tasks

- [x] Definitions: pg_ddl_parser.h and tests/test_pg_definitions.cpp. Add failing fixtures, then CREATE AGGREGATE/OPERATOR, operator classes/families and matching ALTER/DROP identities. Use local gram.y and oracle to validate production boundaries and quoted names. Split focused helpers if useful without changing unrelated grammar. Root owns shared dispatch/enums/emitter.
- [x] Session commands: new pg_session_parser.h and tests/test_pg_session_commands.cpp. Add failing fixtures for DECLARE/FETCH/MOVE/CLOSE, PostgreSQL PREPARE/EXECUTE/DEALLOCATE, and bounded LISTEN/NOTIFY/UNLISTEN/DISCARD/CHECKPOINT. Expose handles(first) and parse(first), preserve query bodies and complete-input state. Coordinate new statement enums with root; no edits to root-owned parser.cpp or shared AST files.
- [x] Administration: root-owned pg_admin_parser.h and tests/test_pg_admin_commands.cpp. Add CREATE/ALTER/DROP ROLE/USER/GROUP with role options/settings, COMMENT/SECURITY LABEL object identities and literal/null values. Reuse existing AST structures, test malformed options and signatures, and oracle-check corpus variants.
- [ ] Integration: root-owned shared dispatch, enum names/maps, emitter/guards, Makefile and docs. Independent review, forced full build/corpus executable, ASan/UBSan, full corpus replay and Python harness. Detailed implementation commit; regenerate and verify correctness snapshot; detailed snapshot commit; push and update PR #67.

## Verified implementation results

- Full forced build followed by a library rebuild and full suite after the final reviewed header fix: 1,472 passed, 37 external-backend skips; corpus executable built.
- ASan/UBSan: all 17 new grammar tests passed, including constrained-arena checks.
- Compatibility harness: 87 tests passed.
- Unchanged PostgreSQL 18.4 source corpus: 48,313 / 51,415 deeply parsed (94.0%), an increase of 2,503 with zero lost deep cases.
- All 2,503 newly supported statements retain exactly equivalent PostgreSQL raw ASTs after SQL emission, excluding source-position fields only.
- Independent review addressed identifier categories, integer bounds, qualified type names and ordered variadic aliases. Uncommon equivalent type spellings, definition-value `%TYPE` and modifying CTE cursor bodies remain documented limitations.
- Snapshot refresh, verification and publication follow the implementation commit.
