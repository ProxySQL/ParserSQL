# PostgreSQL policy, statistics and foreign-object grammar plan

> **For agentic workers:** Use superpowers:subagent-driven-development for sequential grammar implementation and independent review; root owns integration and publication.

**Goal:** Continue the approved native PostgreSQL grammar expansion with policies, extended statistics and foreign objects.
**Architecture:** Extend validated PgDdlParser productions, reusing structural clause/list/name/type/expression nodes and existing execution guards. No opaque SQL tails or PostgreSQL runtime fallback.
**Tech stack:** C++17, GoogleTest, pinned PostgreSQL 18.4 source grammar and development oracle.
**Spec:** Continuation of the user's approved native grammar design and explicit request to continue filling remaining gaps.

## Global constraints

- Use existing branch feat/postgresql-coverage-and-ast-apis and PR #67; detailed commits, no benchmark files or benchmark-only README/.gitignore edits.
- Preserve MySQL behavior, existing enum values and the 48-byte AST layout.
- Production parsing stays native; PostgreSQL/libpg_query is only a development oracle.
- Baseline 398450a: 48,313 / 51,415 deeply supported; inspect every lost case.
- Keep names, options, expressions and literals traversable; allocation failure cannot silently omit operands.
- Root owns Makefile, statement-type mapping, documentation, corpus refresh and commits. Implementers do not commit or spawn subagents.

## Review focus

- Keyword categories and quoted identifiers follow PostgreSQL grammar.
- Parenthesized statistics expressions preserve raw-AST meaning through emission.
- Foreign OPTIONS distinguish CREATE string pairs from ALTER ADD/SET/DROP actions.
- Table constraints, partition forms and column options cannot absorb malformed input.
- Successful constrained-arena parses retain all required AST children; unsupported engine/parameterization paths remain guarded.

### Task 1: Policies and extended statistics

**Files:** Modify include/sql_parser/pg_ddl_parser.h; create tests/test_pg_policy_statistics.cpp.
**Interfaces:** Existing PgDdlParser CREATE/ALTER/DROP dispatch, clause/list nodes, expression callback, object-identity helpers. No new public enum/node types required.

- [x] Add failing tests for CREATE POLICY (AS, FOR, TO, USING, WITH CHECK), ALTER POLICY (roles/predicates/rename), DROP POLICY; CREATE STATISTICS (optional name, IF NOT EXISTS, kinds, columns/functions/parenthesized expressions, FROM relations), ALTER STATISTICS target/owner/schema/rename and DROP STATISTICS.
- [x] Check exact productions in /tmp/parsersql-coverage/postgresql/postgresql-18.4/src/backend/parser/gram.y, not documentation simplifications. Implement scoped helpers with validated options and keyword categories.
- [x] Positive and malformed fixtures include quoted names, clause order, missing operands, expressions/subqueries, target integer bounds, multiple relations, no-name statistics and semicolons.
- [x] Verify traversal/editing/emission and existing execution/parameterization rejection; sweep constrained arenas and compare successful node counts.
- [x] Build focused tests with a fresh parser object; validate relevant upstream cases and every positive/negative fixture using /tmp/parsersql-coverage/libpg_query/target/libpg_query.a. Strip only source-position fields when comparing raw AST JSON.
- [x] Report test-first evidence, commands/results, scope and limitations. Root runs full suite after integration. No commits, Makefile edits, benchmark edits or subagents.

### Task 2: Foreign tables, wrappers, servers and mappings

**Files:** Modify include/sql_parser/pg_ddl_parser.h; create tests/test_pg_foreign_ddl.cpp.
**Interfaces:** Builds on task 1 after review; existing CREATE/ALTER/DROP dispatch and structured table/column/constraint productions. Root handles IMPORT statement classification if introduced.

- [x] Add failing fixtures for CREATE/ALTER/DROP FOREIGN DATA WRAPPER, SERVER and USER MAPPING, CREATE/ALTER/DROP FOREIGN TABLE, and IMPORT FOREIGN SCHEMA (ALL, LIMIT TO, EXCEPT).
- [x] Implement exact pinned PG18 grammar for generic option string pairs and ALTER ADD/SET/DROP options; preserve names, handler/validator, type/version, roles and option literals structurally.
- [x] Reuse table/partition/constraint helpers for foreign tables where grammar permits, including column OPTIONS and INHERITS, partition bounds, server and generic options. Keep ordinary tables' acceptance boundaries intact. No blanket table-parser rewrite.
- [x] Cover positive/negative keyword, quoting, option ordering, partition/column/table constraint and identity cases. Validate allocation failure, traversal/emission and unsupported execution.
- [x] Build focused tests with a fresh parser object; compare relevant corpus and positive/negative fixtures with pinned raw parser. Document genuine remaining positive gaps rather than accepting arbitrary tails.
- [x] Report test-first evidence and commands/results. No commits, Makefile edits, benchmark edits or subagents.

### Task 3: Integration and publication

**Files:** Makefile, tools/pg_compat/statement_type_map.cpp and tests/pg_compat/statement_type_cases.cpp if needed, docs/postgresql-analysis-features.md, correctness snapshot files.

- [x] Register test files and add necessary accurate statement mappings. Review each task's diff independently and fix substantive findings.
- [x] Forced full build/test and corpus executable; ASan/UBSan for new suites; Python compatibility harness; full replay with zero unexplained regressions.
- [x] Verify raw-AST equivalence for every newly supported corpus statement; update native coverage docs and remaining examples.
- [x] Detailed implementation commit, refresh correctness snapshot, compare all refreshed results with reviewed replay, verify full corpus/CI cases, detailed snapshot commit.
- [x] Push existing branch and update PR #67. Preserve all benchmark-only local changes.

## Verification results

- Forced full build and corpus executable, then library rebuild and full suite after review fixes: 1,483 passed, 37 external-backend tests skipped.
- New grammar suites under ASan/UBSan: 11/11 passed, including constrained arenas.
- Compatibility harness: 87 tests passed.
- Same PostgreSQL 18.4 source corpus: 49,120 / 51,415 deeply supported (95.5%), +807 with no previously supported cases lost.
- Every newly supported statement reconstructs to the same PostgreSQL raw AST, excluding source-position fields only (807/807).
- All 298 policy/statistics and 615 of 616 foreign-object original corpus statements roundtrip exactly; the remaining foreign case requires the shared ALTER CONSTRAINT production.
- Independent review approved all five foreign-grammar fixes. The refreshed snapshot matches every reviewed replay row; full pinned-corpus verification and all 5,615 generated CI cases pass. Implementation commit: `19d77ca`. Publish the verified snapshot with the implementation on PR #67.
