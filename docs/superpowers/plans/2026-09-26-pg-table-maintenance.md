# PostgreSQL table constraints and maintenance grammar continuation

**Goal:** Continue the approved native grammar expansion with table constraints/actions and maintenance commands.
**Architecture:** Extend validated native productions and reuse structural AST nodes. Table work and session-command work have independent file ownership and run in parallel; root owns shared integration and final review/publication.
**Source:** Pinned PostgreSQL 18.4 gram.y at /tmp/parsersql-coverage/postgresql/postgresql-18.4/src/backend/parser/gram.y and development-only libpg_query oracle at /tmp/parsersql-coverage/libpg_query/target/libpg_query.a.
**Authorization:** Continues the approved design on feat/postgresql-coverage-and-ast-apis, publishing detailed commits to existing PR #67.

## Constraints

- Native parsing only: no opaque SQL tails, PostgreSQL runtime dependency or fallback.
- Preserve MySQL, existing enum values and the 48-byte AST layout.
- Retain names, expressions, options and operands as traversable AST children; allocation failure must not silently omit them.
- Baseline 0391d69: 49,120 / 51,415 deep; inspect every lost supported case. Keep corpus source/extraction policy unchanged.
- No benchmark sources, results or benchmark-only README/.gitignore changes in commits.
- Implementers own only their specified parser header/new test file. Root owns Makefile, common enums, compatibility mapping, docs and commits. No subagents or commits from implementers.

## Task 1: Table constraints and ALTER actions

Own include/sql_parser/pg_ddl_parser.h and new tests/test_pg_table_constraints.cpp.

- [x] Add failing fixtures, then implement native EXCLUDE table constraints with validated index elements/operators, INCLUDE/storage/tablespace/predicate/constraint attributes; temporal UNIQUE/PRIMARY KEY WITHOUT OVERLAPS and FOREIGN KEY PERIOD forms supported by the pinned grammar.
- [x] Expand ALTER CONSTRAINT (deferrability, enforcement and inheritance attributes), ALTER COLUMN identity/sequence changes including RESTART, SET EXPRESSION AS and REPLICA IDENTITY. Add ALTER INDEX storage/options and relevant table/index/view/materialized-view common command dispatch needed by the corpus without accepting arbitrary tails or standalone identity commands in comma lists.
- [x] Reuse the existing typed_table_elements production for CREATE TABLE OF and PARTITION OF column options, and add ALTER OF/NOT OF, if supported by a small validated extension; no table-parser rewrite. Keep unrelated legacy OIDS/Unicode gaps explicit.
- [x] Check exact grammar and raw-parser validation, including keyword categories, ordering, duplicate attributes, modifier bounds and CREATE versus ALTER distinctions. Keep changes scoped to these table productions; report other positive gaps.
- [x] Test malformed controls, AST traversal/editing/emission, unsupported execution/parameterization and constrained arenas. Build fresh focused parser/tests; raw-AST-check relevant original corpus SQL and every positive/negative fixture. Strip only position fields.
- [x] Write task-1-report.md in this plan's scratch workspace with scope, red/green evidence, exact commands/results and remaining limitations. Signal header stability. Root owns full build, sanitizers and final corpus validation.

## Task 2: Maintenance commands

Own include/sql_parser/pg_session_parser.h and new tests/test_pg_maintenance.cpp.

- [x] Add failing tests, then native REINDEX, CLUSTER (modern and legacy), REFRESH MATERIALIZED VIEW and LOCK grammar. Use PgSessionParser handles/parse and existing NODE_PG_COMMAND_STMT roots; root appends StmtType::REINDEX, CLUSTER, REFRESH_MATERIALIZED_VIEW, while LOCK already exists.
- [x] Preserve options, relation names/lists, qualified and ONLY/star forms where exact grammar permits, index names, concurrency, lock modes/NOWAIT and WITH [NO] DATA. Follow pinned grammar rather than server semantic validation; do not whitelist only currently recognized generic option names if raw grammar allows more.
- [x] Positive/malformed tests cover option name categories/values, optional names, clause order, legacy CLUSTER, qualified names, relation lists and terminal input. Verify reconstruction/traversal, local engine/parameterization rejection, MySQL unchanged and allocation failure.
- [x] Build fresh focused tests and compare relevant corpus SQL plus all fixtures with pinned raw parser. Write task-2-report.md with commands/results/red-green evidence/limitations. No shared file edits; root provides enums before compilation.

## Task 3: Integration and publication

- [x] Register tests and append/map new statement types with regression fixtures.
- [x] Independently review complete batch and fix substantive findings with regression tests.
- [x] Forced full build/test and corpus executable; ASan/UBSan new suites; Python harness; full corpus replay and raw-AST equivalence for every newly supported statement.
- [x] Update coverage docs; detailed implementation commit; refresh correctness snapshot; compare every row with reviewed replay and verify full corpus/CI cases; detailed snapshot commit.
- Publication: push the two verified commits on the same branch and update PR #67; leave benchmark changes uncommitted.

## Verified implementation outcome

The final forced library rebuild and full test suite passed: 1,494 tests passed,
37 external-database tests skipped; the corpus executable built successfully.
ASan/UBSan with leak detection passed all 11 new tests. The compatibility Python
harness passed all 87 tests.

The unchanged 51,415-statement PostgreSQL source corpus now has 49,845 deeply
supported statements (96.9%), up 725 from 49,120, with zero lost supported cases.
All 725 newly supported statements reconstruct to equivalent PostgreSQL raw ASTs
after removing source-position fields only. Gains: 356 ALTER, 149 CREATE TABLE,
117 REINDEX, 39 LOCK, 36 REFRESH MATERIALIZED VIEW and 28 CLUSTER statements.

Independent review corrected view constraint-renaming and ungrouped EXCLUDE
expression boundaries; 854 generated boundary probes then agreed with the raw
parser. SQL value functions and COLLATION FOR were added to the reused index
production. Author oracle fixtures cover 52 positive/67 negative table cases and
52 positive/61 negative maintenance cases; 988 maintenance keyword probes agree.

Remaining selected table gaps include legacy OIDS syntax, flag-only CREATE TABLE
options, repeated column-level enforcement attributes and Unicode identifiers.
Broader remaining families include rules, publications, extensions, function
bodies and other object options. This corpus result is not full grammar parity.

Snapshot validation: all 51,415 refreshed rows match the reviewed replay, and the
full pinned compatibility workflow passed, including all 5,125 generated CI
cases. The implementation commit is 1381df1; the snapshot is committed separately.
