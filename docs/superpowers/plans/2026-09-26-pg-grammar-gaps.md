# Remaining PostgreSQL grammar gaps

The user authorized filling the remaining gaps in ParserSQL's own grammar.
Continue on feat/postgresql-coverage-and-ast-apis and update PR #67. Keep detailed
commits and leave all benchmark work uncommitted. Baseline: de06d72, 27,014 of
51,415 pinned PostgreSQL 18.4 statements completely parsed.

## Implementation groups

1. Expressions/types: special EXTRACT and SUBSTRING syntax, AT TIME ZONE,
   interval literals and qualifiers, qualified OPERATOR and type modifiers.
   Preserve structural operands and source spelling; verify precedence,
   malformed input and equivalent PostgreSQL ASTs after SQL emission.
2. Query clauses: GROUPING SETS/ROLLUP/CUBE, both LIMIT/OFFSET orders and FETCH,
   WITH ORDINALITY and typed table-function output columns. Expressions remain
   AST children; syntax modifiers must not become bind parameters.
3. DML/CTEs: MERGE branches/actions/RETURNING, WITH preceding DML and modifying
   CTE bodies, SEARCH/CYCLE, and remaining practical INSERT variants. Distinguish
   query subexpressions from DML bodies; reject unsupported local execution.
4. DDL/utility: structure common CREATE/ALTER/DROP tables, views, functions,
   indexes and triggers, privilege and maintenance forms. Do not claim complete
   parsing by storing an unvalidated tail or skipping arbitrary tokens.
5. SQL/JSON and XML: add explicit productions for structured constructors,
   query/value functions, JSON_TABLE/XMLTABLE and serialization options. Keep
   default expressions traversable and preserve server-defined syntax.
6. Integration: test first in each group, obtain independent review, force the
   full C++ rebuild, run sanitizers and pinned oracle AST comparisons, inspect
   every compatibility transition, refresh correctness fixtures, then commit
   and publish the PR update. Report remaining variants honestly.

## Coordination and review

Use the subagent-development skill for independent grammar implementations.
Each group owns its parser headers and tests. Shared AST/emitter/planner edits
must be confined to distinct marked sections; no global rewrites. The parent
owns integration, query clauses and final corpus validation. Existing MySQL
behavior and the 48-byte AST layout remain unchanged. Append node/statement
kinds. No production PostgreSQL dependency or parser fallback.

Review focus: syntax rejection versus semantic validation; SQL emission
preserving names, precedence and options; valid nesting; parameterization
contexts; local execution refusing unsupported semantics before side effects.
Full PostgreSQL grammar parity must not be claimed from corpus counts alone.

## Completed verification

Implemented all five grammar groups with focused malformed-input and AST tests.
Independent reviews covered DML/DDL and expressions/query clauses; confirmed
findings were fixed and rechecked. ASan/UBSan passes all 47 new grammar tests,
including arena-exhaustion regressions. The forced full rebuild passes 1,428
tests with 37 backend-dependent tests skipped. The compatibility harness passes
87 tests, and 215 representative PostgreSQL 18.4 AST round trips match after
removing source-position fields.

Pinned corpus replay: 43,373/51,415 deeply parsed, versus 27,014 before this
phase; zero baseline regressions. All 1,274 former classification-only cases
now have complete ASTs. Reviewed 5,919 changes between unsupported categories:
5,165 trailing-input to error, 723 type-mismatch to error, and 31 trailing-input
to partial. These reflect stricter productions rather than lost accepted SQL;
unsupported CREATE/ALTER and expression variants remain documented limitations.
The generated correctness snapshot is refreshed separately after the code commit
so its parser revision identifies the implementation being measured.
