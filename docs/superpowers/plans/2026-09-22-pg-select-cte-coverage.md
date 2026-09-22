# PostgreSQL SELECT and CTE coverage increment

Continue the user-approved grammar expansion on PR #67. Execute inline, retain
MySQL behavior and the 48-byte AST layout, and exclude all benchmark files from
commits. Preserve syntax in ASTs and emitted SQL; reject newly parsed features
in the local engine unless its existing implementation supports them.

1. Add failing PostgreSQL expression tests for qualified/quoted function names,
   DISTINCT/ALL aggregates, aggregate ORDER BY with direction/null placement,
   WITHIN GROUP, composition with FILTER/OVER, malformed argument boundaries,
   and parameterization of aggregate sort constants (which are not ordinals).
2. Implement expression parsing and emission with node-specific flags and an
   appended aggregate order node. Retain ordinary function argument children.
3. Add failing CTE tests for output columns, VALUES/TABLE/compound bodies,
   nested WITH, materialization hints, and strict delimiters. Preserve the body
   as the first definition child and explicit columns as a separate child.
   Add CTE emission and guard unsupported execution. DML CTEs remain deferred
   if their integration requires changes beyond this query-focused increment.
4. Force rebuild and run full C++ tests, corpus harness, and PostgreSQL oracle
   comparisons. Inspect transitions before refreshing correctness snapshots.
5. Obtain an independent code review, document behavior and remaining gaps,
   make detailed commits, push the branch, and update PR #67.

Review focus: quoted names, call versus column ambiguity, missing delimiters,
aggregate order constants versus SELECT ordinals, CTE child layout and engine
materialization behavior. Do not equate complete parsing with AST equivalence.

## Progress and review

- Qualified calls, aggregate modifiers/order and CTE composition implemented.
  Focused tests first demonstrated the missing syntax, then passed.
- Ruling: extend the shared PostgreSQL subquery callback and its entry checks
  to support VALUES/TABLE/WITH in derived tables and scalar subqueries. This
  avoids different grammars for the same query in CTE and derived contexts.
- Review exposed malformed EXISTS null dereference, quoted CTE name emission,
  incomplete operand acceptance in nested queries, duplicate bare WITH, ALL
  WITHIN GROUP acceptance, and aggregate star restrictions. Added regressions
  and corrected each against pinned PostgreSQL grammar.
- Ruling: add NULLS FIRST/LAST in compound-query ordering because shared
  subquery parsing must preserve ordering clauses. Unsupported local execution
  remains explicitly rejected.
- Review demonstrated physical-table shadowing when unsupported CTE bodies were
  skipped during execution. Reject VALUES/TABLE query bodies and nested CTE
  materialization before execution, with an executor regression test.
- Final corpus replay: 27,014 complete parses (+771), no previously complete
  case lost. Gains: 638 SELECT, 123 EXPLAIN, nine UPDATE and one INSERT. Another
  79 trailing and 42 type-mismatch results now report explicit errors; these
  remain unsupported queries, not lost complete parses.
- Forced full rebuild: 1,381 passing C++ tests, 37 backend skips; corpus harness
  built. Focused sanitizer run: 52 passing tests. All 24 new original/emitted
  PostgreSQL AST pairs match after source locations are removed.
- Independent final review: no outstanding findings after 20 targeted grammar
  checks and three CTE physical-table shadowing reproductions.
- Correctness snapshot refresh and PR publication follow the grammar commit.
